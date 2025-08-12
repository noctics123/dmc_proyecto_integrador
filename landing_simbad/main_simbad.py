import os, datetime as dt
from typing import Optional, Dict, Any
from fastapi import FastAPI, Body, HTTPException

# si tu módulo con la lógica está en landing_run/simbad/simbad.py
# copialo a landing_simbad/simbad/simbad.py o ajusta el import.
from simbad.simbad import run_simbad_carteras

app = FastAPI(title="SIMBAD Harvester", version="1.0.0")

def _require_env():
    missing = []
    for k in ["GCS_BUCKET", "LANDING_PREFIX", "SB_API_KEY"]:
        if not os.getenv(k): missing.append(k)
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing env vars: {', '.join(missing)}")

def _normalize_date(s: Optional[str]) -> str:
    if s:
        try: return dt.date.fromisoformat(s).isoformat()
        except ValueError: raise HTTPException(status_code=400, detail="run_date debe ser YYYY-MM-DD")
    return dt.date.today().isoformat()

@app.get("/healthz")
def healthz():
    _require_env()
    return {"status":"ok"}

@app.post("/run")
def run(body: Optional[Dict[str, Any]] = Body(default=None)):
    _require_env()
    date_str = _normalize_date(body.get("run_date") if body else None)
    delete_monthlies = bool(body.get("delete_monthlies", False)) if body else False
    return {"ok": True, **run_simbad_carteras(date_str, delete_monthlies)}
