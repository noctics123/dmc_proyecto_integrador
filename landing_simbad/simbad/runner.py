# landing_simbad/simbad/runner.py
import os
from datetime import datetime, date
from .harvester import run_harvest

def main():
    run_date_env = os.getenv("RUN_DATE")
    cutoff = date.today()
    if run_date_env:
        cutoff = datetime.strptime(run_date_env, "%Y-%m-%d").date()
    summary = run_harvest(cutoff_date=cutoff)
    print(summary)

if __name__ == "__main__":
    main()
