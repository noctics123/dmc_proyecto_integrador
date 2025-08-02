# landing_run/main.py
import os
import tempfile
import datetime as dt
from typing import Optional, Dict, Any, List
import json
import pandas as pd
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import when, col, broadcast
from fastapi import FastAPI, Body, HTTPException
from google.cloud import storage

# ========= CONFIG =========
BUCKET = os.getenv("GCS_BUCKET", "dmc_proy_storage")
BASE_PREFIX = os.getenv("LANDING_PREFIX", "landing/macroeconomics")
POWERBI_RESOURCE_KEY = os.getenv("POWERBI_RESOURCE_KEY", "d2d3b042-b343-4f05-85cb-be05eb64dd22")
# =========================

app = FastAPI(title="Macroeconomics Scraper", version="1.0.0")

# ---------- Utils ----------
def _normalize_date(run_date: Optional[str]) -> str:
    if run_date:
        try:
            d = dt.date.fromisoformat(run_date)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"run_date debe ser YYYY-MM-DD; recibido: {run_date}")
    else:
        d = dt.date.today()
    return d.isoformat()

def _save_df_to_gcs(df: pd.DataFrame, dataset: str, date_str: str, filename: str) -> str:
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

# ---------- Extraction Functions ----------
def extract_desempleo_imf() -> pd.DataFrame:
    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("WorldBankData360Monthly")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .getOrCreate())

    # Define schema
    schema = StructType([
        StructField("OBS_VALUE", DoubleType(), True),
        StructField("TIME_PERIOD", StringType(), True),
        StructField("FREQ", StringType(), True),
        StructField("REF_AREA", StringType(), True)
    ])

    # Country mapping (abridged for brevity; use full mapping from notebook)
    country_mapping = {
        "DOM": "Dominican Republic",
        "MAR": "Morocco",
        "AGO": "Angola",
        # ... add full mapping from notebook
    }

    # API endpoint and parameters
    url = "https://data360api.worldbank.org/data360/data"
    params_base = {
        "DATABASE_ID": "IMF_IFS",
        "INDICATOR": "IMF_IFS_LUR",
        "timePeriodFrom": "1949-01",
        "timePeriodTo": "2024-12",
        "FREQ": "M"
    }

    # Fetch data
    all_data = []
    skip = 0
    while True:
        params = params_base.copy()
        params["skip"] = skip
        response = requests.get(url, params=params)
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            break
        data = response.json()
        values = data.get("value", [])
        if not values:
            break
        all_data.extend(values)
        skip += 1000
        print(f"Fetched batch with {len(values)} records, total: {len(all_data)}")

    # Process OBS_VALUE to float
    for item in all_data:
        try:
            if item.get("OBS_VALUE") is not None:
                item["OBS_VALUE"] = float(item["OBS_VALUE"])
        except (ValueError, TypeError):
            item["OBS_VALUE"] = None

    # Create Spark DataFrame
    df_spark = spark.createDataFrame(all_data, schema=schema)

    # Add Freq Name and country mapping
    df_spark = df_spark.withColumn("Freq Name",
                                   when(df_spark.FREQ == "A", "Anual")
                                   .when(df_spark.FREQ == "M", "Mensual")
                                   .when(df_spark.FREQ == "Q", "Trimestral")
                                   .otherwise("Desconocido"))
    country_map_df = spark.createDataFrame(list(country_mapping.items()), ["REF_AREA", "País"])
    df_spark = df_spark.join(broadcast(country_map_df), "REF_AREA", "left_outer")

    # Select columns
    df_spark = df_spark.select(
        col("TIME_PERIOD").alias("Año"),
        col("FREQ").alias("Periodicidad"),
        col("Freq Name").alias("Freq Name"),
        col("OBS_VALUE").alias("Tasa"),
        col("REF_AREA").alias("País_ID"),
        col("País")
    )

    # Convert to Pandas
    df = df_spark.toPandas()
    spark.stop()
    return df

def extract_inflacion_12m() -> pd.DataFrame:
    QUERY_URL = "https://wabi-us-east2-api.analysis.windows.net/public/reports/querydata?synchronous=true"
    HEADERS = {
        "Content-Type": "application/json;charset=UTF-8",
        "X-PowerBI-ResourceKey": POWERBI_RESOURCE_KEY
    }
    payload_txt = r"""
    { "version": "1.0.0",
      "queries": [
        { "Query": { "Commands": [ { "SemanticQueryDataShapeCommand": {
            "Query": {
              "Version": 2,
              "From": [
                { "Name":"i1","Entity":"inflacion_mensual","Type":0 },
                { "Name":"c", "Entity":"combustibles","Type":0 },
                { "Name":"c1","Entity":"combustible_weeks","Type":0 },
                { "Name":"c2","Entity":"combustible_months","Type":0 }
              ],
              "Select":[
                { "Aggregation": {
                    "Expression": {
                      "Column": { "Expression":{ "SourceRef":{ "Source":"i1" } },
                                   "Property":"inflacion" } },
                    "Function": 0 },
                  "Name":"Sum(inflacion_mensual.inflacion)" },
                { "Column": {
                    "Expression":{ "SourceRef":{ "Source":"i1" } },
                    "Property":"Fecha" },
                  "Name":"inflacion_mensual.Fecha.Variación.Date Hierarchy.Year" }
              ],
              "Where":[
                { "Condition":{ "In":{
                    "Expressions":[{ "Column":{
                        "Expression":{ "SourceRef":{ "Source":"c" } },
                        "Property":"combustible" }}],
                    "Values":[[ { "Literal":{ "Value":"'Gasolina Premium'" } } ]] } } },
                { "Condition":{ "Comparison":{
                    "ComparisonKind":2,
                    "Left":{ "Column":{
                        "Expression":{ "SourceRef":{ "Source":"c" } },
                        "Property":"Fecha" }},
                    "Right":{ "Literal":{ "Value":"datetime'2018-01-01T00:00:00'" }}}}},
                { "Condition":{ "In":{
                    "Expressions":[{ "Column":{
                        "Expression":{ "SourceRef":{ "Source":"c1" } },
                        "Property":"year" }}],
                    "Values":[[ { "Literal":{ "Value":"'2025'" } } ]] } } },
                { "Condition":{ "In":{
                    "Expressions":[{ "Column":{
                        "Expression":{ "SourceRef":{ "Source":"c2" } },
                        "Property":"month" }}],
                    "Values":[[ { "Literal":{ "Value":"'AGOSTO'" } } ]] } } },
                { "Condition":{ "In":{
                    "Expressions":[{ "Column":{
                        "Expression":{ "SourceRef":{ "Source":"c1" } },
                        "Property":"Fecha" }}],
                    "Values":[[ { "Literal":{ "Value":"'26 DE JULIO AL 1 DE AGOSTO DEL 2025'" } } ]] } } }
              ]
            },
            "Binding": {
              "Primary":{ "Groupings":[{ "Projections":[0,1] }] },
              "DataReduction":{ "DataVolume":4, "Primary":{ "BinnedLineSample":{} }},
              "Version":1 },
            "ExecutionMetricsKind":1
        } } ] },
          "ApplicationContext":{ "DatasetId":4966303 }
        }
      ],
      "cancelQueries":[],
      "modelId":4966303
    }
    """
    payload = json.loads(payload_txt)
    r = requests.post(QUERY_URL, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    dsr = r.json()["results"][0]["result"]["data"]["dsr"]
    ph0 = dsr["DS"][0]["PH"][0]
    dm_key = next(k for k in ph0 if k.startswith("DM"))
    rows = ph0[dm_key]
    df = pd.DataFrame({
        "Fecha": pd.to_datetime([r["C"][0] for r in rows], unit="ms"),
        "Inflacion_12m_pct": [float(r["C"][1]) for r in rows]
    }).sort_values("Fecha").assign(Año=lambda d: d["Fecha"].dt.year)
    return df

def extract_tipo_cambio() -> pd.DataFrame:
    QUERY_URL = "https://wabi-us-east2-api.analysis.windows.net/public/reports/querydata?synchronous=true"
    HEADERS = {
        "Content-Type": "application/json;charset=UTF-8",
        "X-PowerBI-ResourceKey": POWERBI_RESOURCE_KEY
    }
    payload_txt = r"""
    { "version": "1.0.0",
      "queries": [
        { "Query": { "Commands": [ { "SemanticQueryDataShapeCommand": {
            "Query": {
              "Version": 2,
              "From": [ { "Name":"t1","Entity":"tasa_de_cambio","Type":0 } ],
              "Select": [
                { "Column": {
                    "Expression": { "SourceRef": { "Source":"t1" } },
                    "Property": "fecha"
                  },
                  "Name": "tasa_de_cambio.fecha"
                },
                { "Aggregation": {
                    "Expression": {
                      "Column": {
                        "Expression": { "SourceRef": { "Source":"t1" } },
                        "Property": "DOLAR ESTADOUNIDENSE"
                      }
                    },
                    "Function": 0
                  },
                  "Name": "Sum(tasa_de_cambio.DOLAR ESTADOUNIDENSE)"
                }
              ],
              "Where": [
                { "Condition": {
                    "Comparison": {
                      "ComparisonKind": 2,
                      "Left":  {
                        "Column": {
                          "Expression": { "SourceRef": { "Source":"t1" } },
                          "Property":"fecha"
                        }
                      },
                      "Right": { "Literal": { "Value":"datetime'2004-01-02T00:00:00'" } }
                    }
                  }
                }
              ]
            },
            "Binding": {
              "Primary": { "Groupings": [ { "Projections":[0,1] } ] },
              "DataReduction": { "DataVolume": 4, "Primary":{ "BinnedLineSample":{} } },
              "Version": 1
            },
            "ExecutionMetricsKind": 1
        } } ] },
          "ApplicationContext": {
            "DatasetId":"8d32b3d9-8f14-4cff-97bc-77275eeeb6ea",
            "Sources":[
              { "ReportId":"83be7f47-135f-4864-a502-96463364f0f8",
                "VisualId":"9518be824f665035ee0a" }
            ]
          }
        }
      ],
      "cancelQueries": [],
      "modelId": 4966303
    }
    """
    payload = json.loads(payload_txt)
    resp = requests.post(QUERY_URL, headers=HEADERS, json=payload, timeout=30)
    resp.raise_for_status()
    dsr = resp.json()["results"][0]["result"]["data"]["dsr"]
    ph0 = dsr["DS"][0]["PH"][0]
    dm_key = next(k for k in ph0 if k.startswith("DM"))
    rows = ph0[dm_key]
    fechas, valores = [], []
    for r in rows:
        c = r["C"]
        if len(c) >= 2:
            ts, val = (c[0], c[1]) if c[0] > 1e11 else (c[1], c[0]) if c[1] > 1e11 else (None, None)
            if ts is not None:
                fechas.append(ts)
                valores.append(val)
    df = pd.DataFrame({
        "Fecha": pd.to_datetime(fechas, unit="ms", errors="coerce"),
        "Tipo_Cambio_Dolar": pd.to_numeric(valores, errors="coerce")
    }).dropna().sort_values("Fecha")
    return df

# ---------- Pipeline ----------
def run_pipeline(run_date: Optional[str] = None) -> Dict[str, Any]:
    date_str = _normalize_date(run_date)
    saved: List[str] = []

    # 1) Inflación 12m
    try:
        df_infl = extract_inflacion_12m()
        if not df_infl.empty:
            saved.append(_save_df_to_gcs(df_infl, "inflacion_12m", date_str, "inflacion_12m.csv"))
        else:
            print("Warning: Inflacion DataFrame is empty")
    except Exception as e:
        print(f"Error extracting inflacion_12m: {e}")

    # 2) Tipo de cambio
    try:
        df_tc = extract_tipo_cambio()
        if not df_tc.empty:
            saved.append(_save_df_to_gcs(df_tc, "tipo_cambio", date_str, "tipo_cambio.csv"))
        else:
            print("Warning: Tipo de cambio DataFrame is empty")
    except Exception as e:
        print(f"Error extracting tipo_cambio: {e}")

    # 3) Desempleo (IMF)
    try:
        df_des = extract_desempleo_imf()
        if not df_des.empty:
            saved.append(_save_df_to_gcs(df_des, "desempleo_imf", date_str, "desempleo_imf.csv"))
        else:
            print("Warning: Desempleo DataFrame is empty")
    except Exception as e:
        print(f"Error extracting desempleo_imf: {e}")

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
