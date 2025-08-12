# landing_run/simbad/simbad.py
import os, json, time, random, tempfile, datetime as dt
from pathlib import Path
from typing import Dict, Any, List, Tuple, Union
import pandas as pd
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from google.cloud import storage
import logging

logger = logging.getLogger(__name__)

BASE    = "https://apis.sb.gob.do/estadisticas/v2/"
ENDPT   = "carteras/creditos"

# envs (se validan en runtime desde el servicio)
SB_API_KEY      = os.getenv("SB_API_KEY")
SB_TIPO_ENTIDAD = os.getenv("SB_TIPO_ENTIDAD", "AAyP")
SB_START_YEAR   = int(os.getenv("SB_START_YEAR", "2012"))
SB_DATASET      = os.getenv("SB_DATASET", "simbad_carteras_aayp_hipotecarios")
SB_KEEP_MONTHLY = os.getenv("SB_KEEP_MONTHLY", "false").lower() == "true"

BUCKET      = os.getenv("GCS_BUCKET")
BASE_PREFIX = os.getenv("LANDING_PREFIX")

HEADERS = {
    "Ocp-Apim-Subscription-Key": SB_API_KEY or "",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0",
}

def _now_year_month():
    today = dt.date.today()
    return today.year, today.month

def _expand_params(d: Dict[str, Union[str,int,List[str]]]) -> List[Tuple[str,str]]:
    out: List[Tuple[str,str]] = []
    for k,v in d.items():
        if v is None: 
            continue
        if isinstance(v, (list,tuple)):
            for item in v:
                out.append((k, str(item)))
        else:
            out.append((k, str(v)))
    return out

def _new_session(total=12, backoff=1.0, jitter=0.5) -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=total, connect=total, read=total, status=total,
        backoff_factor=backoff,
        status_forcelist=(429,500,502,503,504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=40, pool_maxsize=40)
    sess.mount("https://", adapter); sess.mount("http://", adapter)
    sess.headers.update(HEADERS)
    sess._rnd_jitter = jitter
    return sess

def _save_df_to_gcs_csv(df: pd.DataFrame, dataset: str, date_str: str, filename: str) -> str:
    """gs://<bucket>/<BASE_PREFIX>/<dataset>/dt=<date_str>/<filename>"""
    if not BUCKET or not BASE_PREFIX:
        raise RuntimeError("Faltan GCS_BUCKET o LANDING_PREFIX en el entorno")
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    object_name = f"{BASE_PREFIX}/{dataset}/dt={date_str}/{filename}"
    blob = bucket.blob(object_name)
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=True) as tmp:
        df.to_csv(tmp.name, index=False, encoding="utf-8")
        blob.upload_from_filename(tmp.name, content_type="text/csv")
    path = f"gs://{BUCKET}/{object_name}"
    logger.info(f"[WRITE] {path}")
    return path

def _fetch_month(sess: requests.Session, periodo: str, registros_pag=10000, attempts=3) -> pd.DataFrame:
    """Descarga un mes para SB_TIPO_ENTIDAD; maneja paginación + reintentos."""
    url = BASE + ENDPT
    for attempt in range(1, attempts+1):
        frames, pagina = [], 1
        while True:
            params = {
                "periodoInicial": periodo,
                "periodoFinal":   periodo,
                "tipoEntidad":    [SB_TIPO_ENTIDAD],
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
                wait = float(ra) if (ra and ra.isdigit()) else 5.0
                time.sleep(wait + random.random()*sess._rnd_jitter)
                continue
            if r.status_code == 204: break
            if r.status_code == 400: return pd.DataFrame()

            try:
                r.raise_for_status()
                data = r.json()
            except Exception:
                frames=[]; break

            if not isinstance(data, list) or not data:
                break

            frames.append(pd.DataFrame(data))

            xpag = r.headers.get("x-pagination")
            if not xpag: break
            try:
                if not json.loads(xpag).get("HasNext", False): break
            except Exception:
                break
            pagina += 1
            time.sleep(0.2 + random.random()*sess._rnd_jitter)

        if frames:
            return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()

def run_simbad_carteras(run_date_iso: str, delete_monthlies: bool = False):
    """
    Baja 2012-01 → (año/mes actual), filtra 'Créditos Hipotecarios',
    sube a GCS (partición dt=run_date_iso):
      - CSV mensuales si SB_KEEP_MONTHLY=True.
      - CSV consolidado (siempre).
    """
    if not SB_API_KEY:
        raise RuntimeError("SB_API_KEY es requerido")
    y_end, m_end = _now_year_month()
    sess = _new_session()

    monthly_paths, dfs = [], []
    for y in range(SB_START_YEAR, y_end + 1):
        for m in range(1, 13):
            if (y == y_end and m > m_end): break
            periodo = f"{y:04d}-{m:02d}"
            logger.info(f"[SIMBAD] {periodo} …")
            df = _fetch_month(sess, periodo)
            if df.empty:
                logger.info(f"[SIMBAD] {periodo}: 0 filas")
                continue
            if "tipoCartera" in df.columns:
                df = df[df["tipoCartera"] == "Créditos Hipotecarios"]
            if df.empty:
                logger.info(f"[SIMBAD] {periodo}: 0 tras filtro 'Créditos Hipotecarios'")
                continue
            dfs.append(df)
            if SB_KEEP_MONTHLY:
                fname = f"carteras_AAyP_hipotecarios_{periodo}.csv"
                p = _save_df_to_gcs_csv(df, SB_DATASET, run_date_iso, fname)
                monthly_paths.append(p)

    if not dfs:
        return {"saved": [], "message": "Sin filas", "date_partition": f"dt={run_date_iso}"}

    df_all = pd.concat(dfs, ignore_index=True)
    start_str = f"{SB_START_YEAR}-01"
    end_str   = f"{y_end}-{m_end:02d}"
    final_name = f"carteras_AAyP_hipotecarios_{start_str}_a_{end_str}_full.csv"
    final_path = _save_df_to_gcs_csv(df_all, SB_DATASET, run_date_iso, final_name)

    # delete mensuales si se pidió y existen
    if delete_monthlies and monthly_paths:
        client = storage.Client()
        bkt = client.bucket(BUCKET)
        for uri in monthly_paths:
            _, _, rest = uri.partition(f"gs://{BUCKET}/")
            try:
                bkt.blob(rest).delete()
                logger.info(f"[DELETE] {uri}")
            except Exception as e:
                logger.warning(f"No se pudo borrar {uri}: {e}")

    return {"saved": [*monthly_paths, final_path],
            "rows_final": len(df_all),
            "date_partition": f"dt={run_date_iso}"}

