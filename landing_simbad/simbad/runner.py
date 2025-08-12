# landing_simbad/simbad/runner.py
import os, datetime as dt
from simbad.harvester import run_harvest

def main():
    run_date = dt.date.today().isoformat()
    res = run_harvest(
        api_key=os.environ["SB_API_KEY"],
        tipo_entidad=os.getenv("SB_TIPO_ENTIDAD", "AAyP"),
        start_year=int(os.getenv("SB_START_YEAR", "2012")),
        bucket=os.environ["GCS_BUCKET"],
        prefix=os.environ["LANDING_PREFIX"],
        dataset=os.getenv("SB_DATASET", "simbad_carteras_aayp_hipotecarios"),
        keep_monthly=os.getenv("SB_KEEP_MONTHLY", "false").lower() == "true",
        run_date=run_date,
    )
    print({"ok": True, "date_partition": f"dt={run_date}", **res})

if __name__ == "__main__":
    main()
    
