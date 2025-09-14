# landing/simbad/incremental/simbad/runner_incremental.py
import os
import datetime as dt
from simbad.harvester_incremental import run_incremental_harvest

def main():
    """Entry point para Cloud Run Job incremental."""
    run_date = dt.date.today().isoformat()

    # Variables de entorno
    bucket = os.environ["GCS_BUCKET"]
    prefix = os.environ["LANDING_PREFIX"]
    api_key = os.environ["SB_API_KEY"]
    tipo_entidad = os.getenv("SB_TIPO_ENTIDAD", "AAyP")
    dataset = os.getenv("SB_DATASET", "simbad_carteras_aayp_hipotecarios")
    lookback_months = int(os.getenv("SB_LOOKBACK_MONTHS", "3"))

    print(f"🚀 SIMBAD Incremental Job iniciado - {run_date}")
    print(f"📅 Lookback: {lookback_months} meses")
    print(f"🎯 Dataset: {dataset}")
    print(f"📍 Destino: gs://{bucket}/{prefix}/{dataset}/incremental/")

    try:
        res = run_incremental_harvest(
            api_key=api_key,
            tipo_entidad=tipo_entidad,
            bucket=bucket,
            prefix=prefix,
            dataset=dataset,
            run_date=run_date,
            lookback_months=lookback_months
        )

        print("✅ Carga incremental completada exitosamente:")
        print(f"   - Períodos cargados: {res.get('periods_loaded', 0)}")
        print(f"   - Filas procesadas: {res.get('rows', 0)}")
        print(f"   - Rango: {res.get('from', 'N/A')} → {res.get('to', 'N/A')}")
        print(f"   - Consolidado: {res.get('consolidated', 'N/A')}")

        result = {"ok": True, "date_partition": f"dt={run_date}", **res}
        print(f"📊 Resultado final: {result}")

    except Exception as e:
        print(f"❌ Error en carga incremental: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()