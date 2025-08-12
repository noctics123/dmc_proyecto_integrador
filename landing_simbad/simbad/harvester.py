# landing_simbad/simbad/harvester.py
import os
import io
import json
import time
import logging
import datetime as dt
from typing import List, Tuple

import pandas as pd
import requests
from google.cloud import storage
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger("simbad.harvester")


API_BASE = "https://apis.sb.gob.do/estadisticas/v2/carteras/creditos"
MONTHLY_DIR = "monthly"  # subcarpeta opcional para CSV por mes


def _requests_session(api_key: str, timeout: int = 15) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Ocp-Apim-Subscription-Key": api_key,
        "User-Agent": "simbad-harvester/1.0 (+cloud-run)"
    })
    retry = Retry(
        total=8, connect=5, read=5,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.request_timeout = timeout
    return s


def _month_iter(start_year: int) -> List[Tuple[int, int]]:
    """Devuelve [(YYYY, MM), ...] desde start_year-01 hasta el mes actual."""
    today = dt.date.today()
    months = []
    y, m = start_year, 1
    while (y < today.year) or (y == today.year and m <= today.month):
        months.append((y, m))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return months


def _fetch_month_df(sess: requests.Session, y: int, m: int, tipo_entidad: str) -> pd.DataFrame:
    """Descarga un mes, pagina y devuelve DataFrame bruto (sin filtrar)."""
    periodo = f"{y:04d}-{m:02d}"
    params = {
        "periodoInicial": periodo,
        "periodoFinal": periodo,
        "tipoEntidad": tipo_entidad,
        "paginas": 1,
        "registros": 10000,
    }
    dfs = []
    page = 1

    while True:
        params["paginas"] = page
        r = sess.get(API_BASE, params=params, timeout=getattr(sess, "request_timeout", 15))
        r.raise_for_status()

        # Chequear si hay contenido
        if r.status_code == 204 or not r.text.strip():
            break

        # Parse JSON
        try:
            payload = r.json()
        except Exception:
            # Si devuelve HTML por mantenimiento u otro, paramos este mes
            log.warning("Respuesta no JSON para %s: %s...", periodo, r.headers.get("content-type"))
            break

        # Data
        if isinstance(payload, list) and payload:
            dfs.append(pd.DataFrame(payload))
        elif isinstance(payload, dict) and payload.get("Data"):
            # Por si algún endpoint devuelve {Data:[...]}
            dfs.append(pd.DataFrame(payload["Data"]))
        else:
            # Puede que sea 200 sin body válido → salimos
            break

        # Paginación
        xp = r.headers.get("x-pagination")
        if not xp:
            break
        try:
            meta = json.loads(xp)
            has_next = meta.get("HasNext", False)
        except Exception:
            has_next = False

        if not has_next:
            break
        page += 1
        # pequeño respiro anti-rate-limit
        time.sleep(0.2)

    if dfs:
        out = pd.concat(dfs, ignore_index=True)
        out["__periodo"] = periodo  # guardamos el período
        return out
    return pd.DataFrame()


def _filter_hipotecarios(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # Columna esperada "tipoCartera" en la API v2
    if "tipoCartera" in df.columns:
        df = df[df["tipoCartera"].astype(str).str.lower() == "créditos hipotecarios".lower()]
    # Normaliza nombre de periodo
    if "periodo" in df.columns:
        # asegurar YYYY-MM
        df["periodo"] = df["periodo"].astype(str)
    else:
        df["periodo"] = df["__periodo"]
    return df


def _upload_csv_to_gcs(df: pd.DataFrame, bucket: str, object_name: str) -> str:
    client = storage.Client()
    b = client.bucket(bucket)
    blob = b.blob(object_name)

    # CSV en memoria (para no escribir disco)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    blob.upload_from_string(buf.getvalue(), content_type="text/csv")
    path = f"gs://{bucket}/{object_name}"
    log.info("[WRITE] %s (%d filas)", path, len(df))
    return path


def run_harvest(
    api_key: str,
    tipo_entidad: str,
    start_year: int,
    bucket: str,
    prefix: str,
    dataset: str,
    keep_monthly: bool,
    run_date: str,
) -> dict:
    """
    Descarga 2012→mes actual, filtra 'Créditos Hipotecarios',
    sube CSVs mensuales (si keep_monthly) y un consolidado final por dt=run_date.
    """
    if not all([api_key, bucket, prefix, dataset]):
        raise ValueError("Faltan parámetros requeridos (api_key, bucket, prefix, dataset)")

    sess = _requests_session(api_key)
    months = _month_iter(start_year)
    saved_paths = []
    all_pieces = []

    log.info("=== SIMBAD harvest: tipoEntidad=%s, desde=%d, keep_monthly=%s ===",
             tipo_entidad, start_year, keep_monthly)

    for (y, m) in months:
        periodo = f"{y:04d}-{m:02d}"
        log.info("⏬ Descargando %s…", periodo)
        try:
            raw_df = _fetch_month_df(sess, y, m, tipo_entidad)
        except requests.HTTPError as e:
            log.warning("HTTP %s en %s: %s", e.response.status_code if e.response else "ERR", periodo, str(e))
            continue
        except Exception as e:
            log.warning("Error en %s: %s", periodo, str(e))
            continue

        if raw_df.empty:
            log.info("Sin datos en %s", periodo)
            continue

        df = _filter_hipotecarios(raw_df)
        if df.empty:
            log.info("Sin filas de 'Créditos Hipotecarios' en %s", periodo)
            continue

        all_pieces.append(df)

        if keep_monthly:
            obj = f"{prefix}/{dataset}/{MONTHLY_DIR}/periodo={periodo}/carteras_{tipo_entidad}_hipotecarios_{periodo}.csv"
            saved_paths.append(_upload_csv_to_gcs(df, bucket, obj))

        # respiro ligero para no golpear API
        time.sleep(0.1)

    if not all_pieces:
        return {"saved": saved_paths, "consolidated": None, "rows": 0}

    full = pd.concat(all_pieces, ignore_index=True)

    # Orden y columnas recomendadas
    preferred = [
        "periodo","tipoCredito","tipoEntidad","entidad","sectorEconomico","region","provincia",
        "moneda","tipoCartera","actividad","sector","persona","facilidad","residencia",
        "administracionYPropiedad","genero","tipoCliente","clasificacionEntidad",
        "cantidadPlasticos","cantidadCredito","deuda","tasaPorDeuda","deudaCapital",
        "deudaVencida","deudaVencidaDe31A90Dias","valorDesembolso","valorGarantia",
        "valorProvisionCapitalYRendimiento"
    ]
    cols = [c for c in preferred if c in full.columns] + [c for c in full.columns if c not in preferred]
    full = full[cols]

    timestamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    consolidated_obj = (
        f"{prefix}/{dataset}/dt={run_date}/"
        f"consolidado_{tipo_entidad}_hipotecarios_{months[0][0]}_{months[-1][0]}_{timestamp}.csv"
    )
    consolidated_path = _upload_csv_to_gcs(full, bucket, consolidated_obj)

    return {
        "saved": saved_paths,
        "consolidated": consolidated_path,
        "rows": len(full),
        "from": f"{months[0][0]}-{months[0][1]:02d}",
        "to": f"{months[-1][0]}-{months[-1][1]:02d}",
    }
