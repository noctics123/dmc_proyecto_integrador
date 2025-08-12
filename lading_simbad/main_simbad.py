import os
import datetime as dt
from typing import Optional, Dict, Any
from fastapi import FastAPI, Body, HTTPException
from simbad.simbad import run_simbad_carteras  # lógica de extracción
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="SIMBAD Harvester", version="1.0.0")

BUCKET = os.getenv("GCS_BUCKET")
BASE_PREFIX = os.getenv("LANDING_PREFIX")
SB_API_KEY = os.getenv("SB_API_KEY")  # requerido por simbad.simbad

def _require_env():
    missing = []
    if not BUCKET: missing.append("GCS_BUCKET")
    if not BASE_PREFIX: missing.append("LANDING_PREFIX")
    if not SB_API_KEY: missing.append("SB_API_KEY")
    if missing:
        msg = f"Missing env vars: {', '.join(missing)}"
        logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)

def _normalize_date(run_date: Optional[str]) -> str:
    if run_date:
        try:
            return dt.date.fromisoformat(run_date).isoformat()
        except ValueError:
            raise HTTPException(status_code=400, detail=f"run_date debe ser YYYY-MM-DD; recibido: {run_date}")
    return dt.date.today().isoformat()

@app.get("/healthz")
def healthz():
    _require_env()
    return {"status":"ok","bucket":BUCKET,"prefix":BASE_PREFIX}

@app.post("/run")
def run(body: Optional[Dict[str, Any]] = Body(default=None)):
    _require_env()
    date_str = _normalize_date(body.get("run_date") if body else None)
    delete_monthlies = bool(body.get("delete_monthlies", False)) if body else False
    return {"ok": True, **run_simbad_carteras(date_str, delete_monthlies)}
