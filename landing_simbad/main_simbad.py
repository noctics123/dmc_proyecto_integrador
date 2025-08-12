# landing_simbad/main_simbad.py
import os
import logging
from fastapi import FastAPI, Body, HTTPException
import pandas as pd

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("simbad")

app = FastAPI(title="SIMBAD Harvester", version="1.0.0")

# --- healthz para que Cloud Run verifique que el contenedor arrancó
@app.get("/healthz")
def healthz():
    return {"status": "ok"}

# --- endpoint para ejecutar la corrida bajo demanda (evita hacer trabajo en import)
@app.post("/run")
def run(body: dict = Body(default=None)):
    try:
        # lee envs (con defaults seguros para no crashear)
        bucket = os.getenv("GCS_BUCKET", "")
        prefix = os.getenv("LANDING_PREFIX", "")
        api_key = os.getenv("SB_API_KEY", "")
        tipo_entidad = os.getenv("SB_TIPO_ENTIDAD", "AAyP")
        start_year = int(os.getenv("SB_START_YEAR", "2012"))
        dataset = os.getenv("SB_DATASET", "simbad_carteras_aayp_hipotecarios")
        keep_m = os.getenv("SB_KEEP_MONTHLY", "false").lower() == "true"

        # TODO: aquí llama a tu función de extracción real
        # from simbad.harvester import run_harvest
        # res_paths = run_harvest(api_key, tipo_entidad, start_year, bucket, prefix, dataset, keep_m)

        log.info("Harvester would run with: %s %s %s", tipo_entidad, start_year, dataset)
        res_paths = []  # placeholder
        return {"ok": True, "saved": res_paths}
    except Exception as e:
        log.exception("run failed")
        raise HTTPException(status_code=500, detail=str(e))
