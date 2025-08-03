# landing_run/main.py
import os
import tempfile
import datetime as dt
from typing import Optional, Dict, Any, List
import pandas as pd
import logging
from fastapi import FastAPI, Body, HTTPException
from google.cloud import storage
try:
    from scrapers.inflacion import extract_inflacion_12m
    from scrapers.tipo_cambio import extract_tipo_cambio
    from scrapers.desempleo import extract_desempleo_imf
except ImportError as e:
    logging.error(f"Failed to import scraper modules: {str(e)}")
    raise

# Configure logging
from google.cloud import logging as gcp_logging
logging_client = gcp_logging.Client()
logger = logging_client.logger("macro-scraper")
logging.basicConfig(level=logging.INFO)

# ========= CONFIG =========
BUCKET = os.getenv("GCS_BUCKET", "dmc_proy_storage")
BASE_PREFIX = os.getenv("LANDING_PREFIX", "landing/macroeconomics")
POWERBI_RESOURCE_KEY = os.getenv("POWERBI_RESOURCE_KEY", "d2d3b042-b343-4f05-85cb-be05eb64dd22")
PORT = int(os.getenv("PORT", "8080"))
# =========================

app = FastAPI(title="Macroeconomics Scraper", version="1.0.0")

# ---------- Utils ----------
def _normalize_date(run_date: Optional[str]) -> str:
    if run_date:
        try:
            d = dt.date.fromisoformat(run_date)
        except ValueError:
            logger.error(f"Invalid run_date format: {run_date}")
            raise HTTPException(status_code=400, detail=f"run_date debe ser YYYY-MM-DD; recibido: {run_date}")
    else:
        d = dt.date.today()
    return d.isoformat()

def _save_df_to_gcs(df: pd.DataFrame, dataset: str, date_str: str, filename: str) -> str:
    try:
        object_name = f"{BASE_PREFIX}/{dataset}/dt={date_str}/{filename}"
        client = storage.Client()
        bkt = client.bucket(BUCKET)
        blob = bkt.blob(object_name)

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=True) as tmp:
            df.to_csv(tmp.name, index=False, encoding="utf-8")
            blob.upload_from_filename(tmp.name, content_type="text/csv")

        path = f"gs://{BUCKET}/{object_name}"
        logger.info(f"[WRITE] Saved to {path}")
        return path
    except Exception as e:
        logger.error(f"Failed to save to GCS: {str(e)}")
        raise

# ---------- Pipeline ----------
def run_pipeline(run_date: Optional[str] = None) -> Dict[str, Any]:
    date_str = _normalize_date(run_date)
    saved: List[str] = []
    logger.info(f"Starting pipeline for date: {date_str}")

    # 1) Inflación 12m
    try:
        logger.info("Extracting inflacion_12m")
        df_infl = extract_inflacion_12m()
        logger.info(f"Inflacion_12m rows: {len(df_infl)}")
        if not df_infl.empty:
            saved.append(_save_df_to_gcs(df_infl, "inflacion_12m", date_str, "inflacion_12m.csv"))
        else:
            logger.warning("Inflacion DataFrame is empty")
    except Exception as e:
        logger.error(f"Error extracting inflacion_12m: {str(e)}")

    # 2) Tipo de cambio
    try:
        logger.info("Extracting tipo_cambio")
        df_tc = extract_tipo_cambio()
        logger.info(f"Tipo_cambio rows: {len(df_tc)}")
        if not df_tc.empty:
            saved.append(_save_df_to_gcs(df_tc, "tipo_cambio", date_str, "tipo_cambio.csv"))
        else:
            logger.warning("Tipo de cambio DataFrame is empty")
    except Exception as e:
        logger.error(f"Error extracting tipo_cambio: {str(e)}")

    # 3) Desempleo (IMF)
    try:
        logger.info("Extracting desempleo_imf")
        df_des = extract_desempleo_imf()
        logger.info(f"Desempleo_imf rows: {len(df_des)}")
        if not df_des.empty:
            saved.append(_save_df_to_gcs(df_des, "desempleo_imf", date_str, "desempleo_imf.csv"))
        else:
            logger.warning("Desempleo DataFrame is empty")
    except Exception as e:
        logger.error(f"Error extracting desempleo_imf: {str(e)}")

    logger.info(f"Pipeline completed. Saved files: {saved}")
    return {"saved": saved, "date_partition": f"dt={date_str}"}

# ---------- Endpoints ----------
@app.get("/healthz")
def healthz():
    logger.info("Health check endpoint called")
    return {
        "status": "ok",
        "bucket": BUCKET,
        "base_prefix": BASE_PREFIX,
    }

@app.post("/run")
def run(body: Dict[str, Any] = Body(default={})):
    logger.info(f"Run endpoint called with body: {body}")
    return {"ok": True, **run_pipeline(body.get("run_date"))}

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting uvicorn server on port {PORT}")
    uvicorn.run("landing_run.main:app", host="0.0.0.0", port=PORT, log_level="info")
