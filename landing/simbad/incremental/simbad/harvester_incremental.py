# landing/simbad/incremental/simbad/harvester_incremental.py
import os
import io
import json
import time
import logging
import datetime as dt
from typing import List, Tuple, Optional

import pandas as pd
import requests
from google.cloud import storage
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger("simbad.harvester_incremental")

API_BASE = "https://apis.sb.gob.do/estadisticas/v2/carteras/creditos"


def _requests_session(api_key: str, timeout: int = 15) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Ocp-Apim-Subscription-Key": api_key,
        "User-Agent": "simbad-incremental-harvester/1.0 (+cloud-run)"
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


def _get_last_available_periods(lookback_months: int = 3) -> List[Tuple[int, int]]:
    """
    Devuelve los últimos N meses incluyendo el actual.
    Por defecto 3 meses para asegurar que capturamos datos nuevos/actualizados.
    """
    today = dt.date.today()
    months = []

    # Empezar desde hace lookback_months
    current_date = today.replace(day=1)  # Primer día del mes actual

    for i in range(lookback_months):
        if i > 0:
            # Retroceder un mes
            if current_date.month == 1:
                current_date = current_date.replace(year=current_date.year - 1, month=12)
            else:
                current_date = current_date.replace(month=current_date.month - 1)

        months.append((current_date.year, current_date.month))

    # Devolver en orden cronológico (más antiguo primero)
    return list(reversed(months))


def _get_latest_data_period(bucket: str, prefix: str, dataset: str) -> Optional[str]:
    """
    Busca en GCS cuál fue el último período cargado exitosamente.
    Retorna YYYY-MM o None si no encuentra datos.
    """
    try:
        client = storage.Client()
        b = client.bucket(bucket)

        # Buscar archivos consolidados más recientes
        blobs = b.list_blobs(prefix=f"{prefix}/{dataset}/")

        latest_period = None
        for blob in blobs:
            # Buscar archivos que contengan "consolidado" y extraer período
            if "consolidado" in blob.name and ".csv" in blob.name:
                # Extraer fecha del path dt=YYYY-MM-DD
                parts = blob.name.split("/")
                for part in parts:
                    if part.startswith("dt="):
                        date_str = part[3:]  # Remover "dt="
                        period = date_str[:7]  # YYYY-MM-DD -> YYYY-MM
                        if not latest_period or period > latest_period:
                            latest_period = period

        return latest_period

    except Exception as e:
        log.warning("Error al buscar último período en GCS: %s", str(e))
        return None


def _fetch_month_df(sess: requests.Session, y: int, m: int, tipo_entidad: str) -> pd.DataFrame:
    """Descarga un mes específico, con paginación."""
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
            log.warning("Respuesta no JSON para %s: %s...", periodo, r.headers.get("content-type"))
            break

        # Data
        if isinstance(payload, list) and payload:
            dfs.append(pd.DataFrame(payload))
        elif isinstance(payload, dict) and payload.get("Data"):
            dfs.append(pd.DataFrame(payload["Data"]))
        else:
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
        time.sleep(0.2)

    if dfs:
        out = pd.concat(dfs, ignore_index=True)
        out["__periodo"] = periodo
        return out
    return pd.DataFrame()


def _filter_hipotecarios(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra solo créditos hipotecarios."""
    if df.empty:
        return df

    if "tipoCartera" in df.columns:
        df = df[df["tipoCartera"].astype(str).str.lower() == "créditos hipotecarios".lower()]

    if "periodo" in df.columns:
        df["periodo"] = df["periodo"].astype(str)
    else:
        df["periodo"] = df["__periodo"]

    return df


def _upload_csv_to_gcs(df: pd.DataFrame, bucket: str, object_name: str) -> str:
    """Sube DataFrame como CSV a GCS."""
    client = storage.Client()
    b = client.bucket(bucket)
    blob = b.blob(object_name)

    buf = io.StringIO()
    df.to_csv(buf, index=False)
    blob.upload_from_string(buf.getvalue(), content_type="text/csv")
    path = f"gs://{bucket}/{object_name}"
    log.info("[WRITE] %s (%d filas)", path, len(df))
    return path


def run_incremental_harvest(
    api_key: str,
    tipo_entidad: str,
    bucket: str,
    prefix: str,
    dataset: str,
    run_date: str,
    lookback_months: int = 3,
    force_periods: Optional[List[str]] = None
) -> dict:
    """
    Carga incremental inteligente de SIMBAD.

    Args:
        api_key: Clave API de SIMBAD
        tipo_entidad: Tipo de entidad (AAyP, etc.)
        bucket: Bucket de GCS
        prefix: Prefijo en GCS
        dataset: Nombre del dataset
        run_date: Fecha de ejecución (YYYY-MM-DD)
        lookback_months: Cuántos meses hacia atrás revisar (default: 3)
        force_periods: Lista de períodos específicos a cargar (YYYY-MM)

    Returns:
        Dict con resultados de la carga
    """
    if not all([api_key, bucket, prefix, dataset]):
        raise ValueError("Faltan parámetros requeridos")

    sess = _requests_session(api_key)
    saved_paths = []
    all_pieces = []

    # Determinar qué períodos cargar
    if force_periods:
        periods_to_load = [(int(p[:4]), int(p[5:7])) for p in force_periods]
        log.info("=== SIMBAD INCREMENTAL: Carga forzada de períodos %s ===", force_periods)
    else:
        # Carga inteligente: últimos N meses
        periods_to_load = _get_last_available_periods(lookback_months)
        last_loaded = _get_latest_data_period(bucket, prefix, dataset)

        log.info("=== SIMBAD INCREMENTAL: Último período en GCS: %s ===", last_loaded or "Ninguno")
        log.info("=== Cargando últimos %d meses: %s ===",
                lookback_months,
                [f"{y:04d}-{m:02d}" for y, m in periods_to_load])

    # Cargar períodos determinados
    for (y, m) in periods_to_load:
        periodo = f"{y:04d}-{m:02d}"
        log.info("⏬ Descargando %s (incremental)…", periodo)

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
            log.info("Sin créditos hipotecarios en %s", periodo)
            continue

        all_pieces.append(df)

        # Guardar archivo individual por período
        obj = f"{prefix}/{dataset}/incremental/periodo={periodo}/carteras_{tipo_entidad}_hipotecarios_{periodo}.csv"
        saved_paths.append(_upload_csv_to_gcs(df, bucket, obj))

        time.sleep(0.1)  # Rate limiting

    if not all_pieces:
        return {
            "type": "incremental",
            "saved": saved_paths,
            "consolidated": None,
            "rows": 0,
            "periods_loaded": 0
        }

    # Crear consolidado incremental
    full = pd.concat(all_pieces, ignore_index=True)

    # Ordenar columnas como en histórico
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

    # Archivo consolidado incremental
    timestamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    consolidated_obj = (
        f"{prefix}/{dataset}/incremental/dt={run_date}/"
        f"incremental_{tipo_entidad}_hipotecarios_{len(periods_to_load)}months_{timestamp}.csv"
    )
    consolidated_path = _upload_csv_to_gcs(full, bucket, consolidated_obj)

    return {
        "type": "incremental",
        "saved": saved_paths,
        "consolidated": consolidated_path,
        "rows": len(full),
        "periods_loaded": len(periods_to_load),
        "from": f"{periods_to_load[0][0]}-{periods_to_load[0][1]:02d}",
        "to": f"{periods_to_load[-1][0]}-{periods_to_load[-1][1]:02d}",
        "lookback_months": lookback_months
    }