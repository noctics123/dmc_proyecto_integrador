# landing_simbad/main_simbad.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, date
import os
import logging

try:
    from google.cloud import logging as cloud_logging
    client = cloud_logging.Client()
    client.setup_logging()
except Exception:
    pass

from simbad.harvester import run_harvest

app = FastAPI(title="SIMBAD Harvester", version="1.0.0")

class RunRequest(BaseModel):
    run_date: str | None = None
    make_consolidated: bool | None = None

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/run")
def run_endpoint(req: RunRequest):
    api_key = os.getenv("SB_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="SB_API_KEY ausente (Secret Manager)")
    if req.run_date:
        try:
            cutoff = datetime.strptime(req.run_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=422, detail="run_date debe ser YYYY-MM-DD")
    else:
        cutoff = date.today()
    if req.make_consolidated is not None:
        os.environ["SB_MAKE_CONSOLIDATED"] = "true" if req.make_consolidated else "false"
    try:
        summary = run_harvest(cutoff_date=cutoff)
        return {"status": "ok", **summary}
    except Exception as e:
        logging.exception("Fallo en ejecución /run")
        raise HTTPException(status_code=500, detail=str(e))
