# landing_run/main.py
import os, tempfile, datetime as dt
from typing import Optional, Dict, Any, List

import pandas as pd
import requests  # <- pega tu scraping real en las funciones extract_*
from fastapi import FastAPI, Body, HTTPException
from google.cloud import storage

# ========= CONFIG =========
BUCKET = os.getenv("GCS_BUCKET", "dmc_proy_storage")
# Nuevo default acorde a tu estructura
BASE_PREFIX = os.getenv("LANDING_PREFIX", "landing/macroeconomics")
# =========================

app = FastAPI(title="Macroeconomics Scraper", version="1.0.0")

# ---------- Utils ----------
def _normalize_date(run_date: Optional[str]) -> str:
    """
    Devuelve YYYY-MM-DD. Si run_date viene, valida formato ISO.
    """
    if run_date:
        try:
            d = dt.date.fromisoformat(run_date)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"run_date debe ser YYYY-MM-DD; recibido: {run_date}")
    else:
        d = dt.date.today()
    return d.isoformat()

def _save_df_to_gcs(df: pd.DataFrame, dataset: str, date_str: str, filename: str) -> str:
    """
    Guarda df como CSV en: gs://<bucket>/<BASE_PREFIX>/<dataset>/dt=<date_str>/<filename>
    """
    object_name = f"{BASE_PREFIX}/{dataset}/dt={date_str}/{filename}"
    client = storage.Client()
    bkt = client.bucket(BUCKET)
    blob = bkt.blob(object_name)

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=True) as tmp:
        df.to_csv(tmp.name, index=False, encoding="utf-8")
        blob.upload_from_filename(tmp.name, content_type="text/csv")

    path = f"gs://{BUCKET}/{object_name}"
    print(f"[WRITE] {path}", flush=True)
    return path

# ---------- Extracciones (reemplaza con tu lógica real) ----------
def extract_inflacion_12m() -> pd.DataFrame:
    # TODO: pega aquí tu scraping real y retorna un DataFrame
    return pd.DataFrame([{"pais": "Peru", "fecha": "2025-08", "inflacion12m": 3.1}])

def extract_tipo_cambio() -> pd.DataFrame:
    # TODO: pega tu extracción real
    return pd.DataFrame([{"fecha": "2025-08-01", "tc_compra": 3.72, "tc_venta": 3.74}])

def extract_desempleo_imf() -> pd.DataFrame:
    # TODO: pega tu extracción real
    return pd.DataFrame([{"pais": "Peru", "anio": 2024, "tasa_desempleo": 7.4}])

# ---------- Pipeline ----------
def run_pipeline(run_date: Optional[str] = None) -> Dict[str, Any]:
    date_str = _normalize_date(run_date)
    saved: List[str] = []

    # 1) Inflación 12m
    df_infl = extract_inflacion_12m()
    if not df_infl.empty:
        saved.append(_save_df_to_gcs(df_infl, "inflacion_12m", date_str, "inflacion_12m.csv"))

    # 2) Tipo de cambio
    df_tc = extract_tipo_cambio()
    if not df_tc.empty:
        saved.append(_save_df_to_gcs(df_tc, "tipo_cambio", date_str, "tipo_cambio.csv"))

    # 3) Desempleo (IMF)
    df_des = extract_desempleo_imf()
    if not df_des.empty:
        saved.append(_save_df_to_gcs(df_des, "desempleo_imf", date_str, "desempleo_imf.csv"))

    return {"saved": saved, "date_partition": f"dt={date_str}"}

# ---------- Endpoints ----------
@app.get("/healthz")
def healthz():
    return {
        "status": "ok",
        "bucket": BUCKET,
        "base_prefix": BASE_PREFIX,
    }

@app.post("/run")
def run(body: Dict[str, Any] = Body(default={})):
    return {"ok": True, **run_pipeline(body.get("run_date"))}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
