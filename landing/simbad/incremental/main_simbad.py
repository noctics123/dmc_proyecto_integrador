# landing/simbad/incremental/main_simbad.py
import os
import logging
import datetime as dt
from fastapi import FastAPI, Body, HTTPException
from simbad.harvester_incremental import run_incremental_harvest

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("simbad")

app = FastAPI(title="SIMBAD Incremental Harvester", version="1.0.0")

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

        # Para incremental: lookback_months desde env o default 3
        lookback_months = int(os.getenv("SB_LOOKBACK_MONTHS", "3"))

        res = run_incremental_harvest(
            api_key=api_key,
            tipo_entidad=tipo_entidad,
            bucket=bucket,
            prefix=prefix,
            dataset=dataset,
            run_date=run_date,
            lookback_months=lookback_months
        )
        return {"ok": True, "date_partition": f"dt={run_date}", **res}
    except HTTPException:
        raise
    except Exception as e:
        log.exception("run failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/run/force-periods")
def run_force_periods(body: dict = Body(default=None)):
    """
    Fuerza la carga de períodos específicos.
    Body: {"periods": ["2024-12", "2025-01"], "run_date": "2025-01-15"}
    """
    try:
        if not body or "periods" not in body:
            raise HTTPException(status_code=400, detail="Falta campo 'periods' con lista de YYYY-MM")

        periods = body["periods"]
        run_date = _normalize_date(body.get("run_date"))

        bucket = os.getenv("GCS_BUCKET", "")
        prefix = os.getenv("LANDING_PREFIX", "")
        api_key = os.getenv("SB_API_KEY", "")
        tipo_entidad = os.getenv("SB_TIPO_ENTIDAD", "AAyP")
        dataset = os.getenv("SB_DATASET", "simbad_carteras_aayp_hipotecarios")

        if not bucket or not prefix or not api_key:
            raise HTTPException(status_code=500, detail="Faltan env vars: GCS_BUCKET, LANDING_PREFIX o SB_API_KEY")

        res = run_incremental_harvest(
            api_key=api_key,
            tipo_entidad=tipo_entidad,
            bucket=bucket,
            prefix=prefix,
            dataset=dataset,
            run_date=run_date,
            force_periods=periods
        )
        return {"ok": True, "date_partition": f"dt={run_date}", "forced_periods": periods, **res}
    except HTTPException:
        raise
    except Exception as e:
        log.exception("run force periods failed")
        raise HTTPException(status_code=500, detail=str(e))