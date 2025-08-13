# landing_simbad/main_simbad.py
import os
import logging
import datetime as dt
from fastapi import FastAPI, Body, HTTPException
from simbad.harvester import run_harvest

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("simbad")

app = FastAPI(title="SIMBAD Harvester", version="1.0.0")

def _normalize_date(run_date: str | None) -> str:
    if run_date:
        try:
            return dt.date.fromisoformat(run_date).isoformat()
        except ValueError:
            raise HTTPException(status_code=400, detail="run_date debe ser YYYY-MM-DD")
    return dt.date.today().isoformat()

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.post("/run")
def run(body: dict = Body(default=None)):
    try:
        run_date = _normalize_date(body.get("run_date") if body else None)
        bucket = os.getenv("GCS_BUCKET", "")
        prefix = os.getenv("LANDING_PREFIX", "")
        api_key = os.getenv("SB_API_KEY", "")
        tipo_entidad = os.getenv("SB_TIPO_ENTIDAD", "AAyP")
        start_year = int(os.getenv("SB_START_YEAR", "2012"))
        dataset = os.getenv("SB_DATASET", "simbad_carteras_aayp_hipotecarios")
        keep_m = os.getenv("SB_KEEP_MONTHLY", "false").lower() == "true"

        if not bucket or not prefix or not api_key:
            raise HTTPException(status_code=500, detail="Faltan env vars: GCS_BUCKET, LANDING_PREFIX o SB_API_KEY")

        res = run_harvest(
            api_key=api_key,
            tipo_entidad=tipo_entidad,
            start_year=start_year,
            bucket=bucket,
            prefix=prefix,
            dataset=dataset,
            keep_monthly=keep_m,
            run_date=run_date,
        )
        return {"ok": True, "date_partition": f"dt={run_date}", **res}
    except HTTPException:
        raise
    except Exception as e:
        log.exception("run failed")
        raise HTTPException(status_code=500, detail=str(e))