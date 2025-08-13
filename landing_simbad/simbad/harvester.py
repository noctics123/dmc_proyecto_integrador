# landing_simbad/simbad/harvester.py
from __future__ import annotations
import os, json, time, random, io
from typing import Dict, Any, List, Tuple, Iterable
from datetime import datetime, date
import requests
import pandas as pd
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from google.cloud import storage
import logging

BASE    = "https://apis.sb.gob.do/estadisticas/v2/"
ENDPT   = "carteras/creditos"

def month_iter(y0: int, m0: int, y1: int, m1: int) -> Iterable[str]:
    y, m = y0, m0
    while (y < y1) or (y == y1 and m <= m1):
        yield f"{y:04d}-{m:02d}"
        m += 1
        if m == 13:
            y, m = y+1, 1

def _expand_params(params: Dict[str, Any]) -> List[Tuple[str,str]]:
    out: List[Tuple[str,str]] = []
    for k,v in params.items():
        if v is None:
            continue
        if isinstance(v, (list, tuple)):
            for item in v:
                out.append((k, str(item)))
        else:
            out.append((k, str(v)))
    return out

def new_session_with_retries(total=12, backoff=1.0, jitter=0.5) -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=total, connect=total, read=total, status=total,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=40, pool_maxsize=40)
    sess.mount("https://", adapter); sess.mount("http://", adapter)
    sess.headers.update({
        "Ocp-Apim-Subscription-Key": os.getenv("SB_API_KEY", ""),
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/123 Safari/537.36",
        "Accept": "application/json",
    })
    sess._rnd_jitter = jitter
    return sess

def fetch_month(sess: requests.Session, tipo_entidad: str, periodo: str, registros_pag: int = 10000, attempts: int = 3) -> pd.DataFrame:
    url = BASE + ENDPT
    for attempt in range(1, attempts+1):
        frames, pagina = [], 1
        while True:
            params: Dict[str, Any] = {
                "periodoInicial": periodo,
                "periodoFinal":   periodo,
                "tipoEntidad":    [tipo_entidad],
                "paginas":        pagina,
                "registros":      registros_pag,
            }
            try:
                r = sess.get(url, params=_expand_params(params), timeout=(30, 180))
            except requests.exceptions.RequestException:
                time.sleep((2**attempt) + random.random()*sess._rnd_jitter)
                frames=[]; break

            if r.status_code in (429, 500, 502, 503, 504):
                ra = r.headers.get("Retry-After")
                try:
                    wait = float(ra) if ra and ra.strip().isdigit() else 5.0
                except Exception:
                    wait = 5.0
                time.sleep(wait + random.random()*sess._rnd_jitter)
                continue

            if r.status_code == 204:
                break
            if r.status_code == 400:
                return pd.DataFrame()

            try:
                r.raise_for_status()
                data = r.json()
            except Exception:
                frames=[]; break

            if not isinstance(data, list) or not data:
                break

            frames.append(pd.DataFrame(data))

            xpag = r.headers.get("x-pagination")
            if not xpag:
                break
            try:
                if not json.loads(xpag).get("HasNext", False):
                    break
            except Exception:
                break

            pagina += 1
            time.sleep(0.2 + random.random()*sess._rnd_jitter)

        if frames:
            return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()

def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False, encoding="utf-8")
    return buf.getvalue().encode("utf-8")

def upload_csv_to_gcs(df: pd.DataFrame, bucket: str, path: str) -> None:
    client = storage.Client()
    blob = client.bucket(bucket).blob(path)
    blob.upload_from_string(_to_csv_bytes(df), content_type="text/csv")

def _shard_months(months: list[str]) -> list[str]:
    try:
        idx = int(os.getenv("CLOUD_RUN_TASK_INDEX", "0"))
        cnt = max(int(os.getenv("CLOUD_RUN_TASK_COUNT", "1")), 1)
        return [m for i, m in enumerate(months) if i % cnt == idx]
    except Exception:
        return months

def run_harvest(cutoff_date: date | None = None) -> dict:
    bucket   = os.environ["GCS_BUCKET"]
    prefix   = os.getenv("LANDING_PREFIX", "landing").rstrip("/")
    dataset  = os.getenv("SB_DATASET", "simbad_carteras_aayp_hipotecarios")
    tipo_ent = os.getenv("SB_TIPO_ENTIDAD", "AAyP")
    cartera  = os.getenv("SB_CARTERA_OBJ", "Créditos Hipotecarios")
    start_y  = int(os.getenv("SB_START_YEAR", "2012"))
    keep_m   = os.getenv("SB_KEEP_MONTHLY", "false").lower() == "true"
    make_con = os.getenv("SB_MAKE_CONSOLIDATED", "false").lower() == "true"

    if cutoff_date is None:
        cutoff_date = date.today()
    end_y, end_m = cutoff_date.year, cutoff_date.month
    dt_str = cutoff_date.isoformat()

    assert os.getenv("SB_API_KEY"), "SB_API_KEY no configurado"
    assert bucket, "GCS_BUCKET no configurado"

    sess = new_session_with_retries()
    months_all = list(month_iter(start_y, 1, end_y, end_m))
    months = _shard_months(months_all)
    logging.info("Procesando %s de %s meses (task_index=%s)", len(months), len(months_all), os.getenv("CLOUD_RUN_TASK_INDEX","0"))

    consolidated = []
    written = 0
    for periodo in months:
        df = fetch_month(sess, tipo_ent, periodo)
        if df.empty:
            empty = pd.DataFrame()
            monthly_key = f"{prefix}/{dataset}/monthly/{periodo}/carteras_{tipo_ent}_hipotecarios_{periodo}.csv"
            upload_csv_to_gcs(empty, bucket, monthly_key)
            logging.info("%s: 0 filas (CSV vacío) -> gs://%s/%s", periodo, bucket, monthly_key)
            continue

        if "tipoCartera" in df.columns:
            before = len(df)
            df = df[df["tipoCartera"] == cartera]
            logging.info("%s: %s -> %s filas tras filtro '%s'", periodo, before, len(df), cartera)

        monthly_key = f"{prefix}/{dataset}/monthly/{periodo}/carteras_{tipo_ent}_hipotecarios_{periodo}.csv"
        upload_csv_to_gcs(df, bucket, monthly_key)
        logging.info("Guardado mensual -> gs://%s/%s", bucket, monthly_key)
        written += len(df)

        if make_con:
            consolidated.append(df)

    out = {
        "processed_months": len(months),
        "rows_written": written,
        "dataset": dataset,
        "bucket": bucket,
        "prefix": prefix,
        "cutoff_date": dt_str,
        "consolidated": False,
    }

    if make_con and consolidated:
        df_full = pd.concat(consolidated, ignore_index=True)
        start_str = f"{start_y}-01"
        end_str   = f"{end_y}-{end_m:02d}"
        full_key  = f"{prefix}/{dataset}/dt={dt_str}/{dataset}_{start_str}_a_{end_str}_full.csv"
        upload_csv_to_gcs(df_full, bucket, full_key)
        logging.info("Consolidado (%s filas) -> gs://%s/%s", len(df_full), bucket, full_key)
        out["consolidated"] = True
        out["consolidated_rows"] = int(len(df_full))
        out["consolidated_path"] = f"gs://{bucket}/{full_key}"

    if not keep_m:
        logging.info("SB_KEEP_MONTHLY=false -> mensual guardado (borrado omitido por seguridad)")

    return out
