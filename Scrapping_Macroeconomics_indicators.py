#!/usr/bin/env python
# coding: utf-8

# <a href="https://colab.research.google.com/github/noctics123/dmc_pi_ruben_advde/blob/main/Scrapping_Macroeconomics_indicators.ipynb" target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>

# In[16]:


pip install requests pandas


# In[17]:


get_ipython().system('pip install requests')


# ## **INDICADOR DESEMPLEO**

# In[19]:


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import when, col, create_map, lit
import requests
import json
from pyspark.sql.functions import broadcast

# Initialize Spark session
builder = SparkSession.builder \
    .appName("WorldBankData360Monthly") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()
spark = builder

# Define the schema with only the required fields
schema = StructType([
    StructField("OBS_VALUE", DoubleType(), True),  # Tasa
    StructField("TIME_PERIOD", StringType(), True),  # Año
    StructField("FREQ", StringType(), True),  # Periodicidad code
    StructField("REF_AREA", StringType(), True)  # País ID
])

# Country code to country name mapping
country_mapping = {
    "DOM": "Dominican Republic",
    "MAR": "Morocco",
    "AGO": "Angola",
    "ABW": "Aruba",
    "AFG": "Afghanistan",
    "ALB": "Albania",
    "ARE": "United Arab Emirates",
    "ARG": "Argentina",
    "ARM": "Armenia",
    "ATG": "Antigua and Barbuda",
    "AUS": "Australia",
    "AUT": "Austria",
    "AZE": "Azerbaijan",
    "BDI": "Burundi",
    "BEL": "Belgium",
    "BEN": "Benin",
    "BFA": "Burkina Faso",
    "BGD": "Bangladesh",
    "BGR": "Bulgaria",
    "BHR": "Bahrain",
    "BHS": "Bahamas, The",
    "BIH": "Bosnia and Herzegovina",
    "BLR": "Belarus",
    "BLZ": "Belize",
    "BOL": "Bolivia",
    "BRA": "Brazil",
    "BRB": "Barbados",
    "BRN": "Brunei Darussalam",
    "BTN": "Bhutan",
    "BWA": "Botswana",
    "CAN": "Canada",
    "CHE": "Switzerland",
    "CHL": "Chile",
    "CHN": "China",
    "CIV": "Cote d'Ivoire",
    "CMR": "Cameroon",
    "COD": "Congo, Dem. Rep.",
    "COG": "Congo, Rep.",
    "COL": "Colombia",
    "COM": "Comoros",
    "CPV": "Cabo Verde",
    "CRI": "Costa Rica",
    "CUW": "Curacao",
    "CYP": "Cyprus",
    "CZE": "Czechia",
    "DEU": "Germany",
    "DJI": "Djibouti",
    "DMA": "Dominica",
    "DNK": "Denmark",
    "DZA": "Algeria",
    "ECU": "Ecuador",
    "EGY": "Egypt, Arab Rep.",
    "ESP": "Spain",
    "EST": "Estonia",
    "ETH": "Ethiopia",
    "FIN": "Finland",
    "FJI": "Fiji",
    "FRA": "France",
    "FSM": "Micronesia, Fed. Sts.",
    "GAB": "Gabon",
    "GBR": "United Kingdom",
    "GEO": "Georgia",
    "GHA": "Ghana",
    "GIN": "Guinea",
    "GMB": "Gambia, The",
    "GNB": "Guinea-Bissau",
    "GNQ": "Equatorial Guinea",
    "GRC": "Greece",
    "GRD": "Grenada",
    "GTM": "Guatemala",
    "GUY": "Guyana",
    "HKG": "Hong Kong SAR, China",
    "HND": "Honduras",
    "HRV": "Croatia",
    "HTI": "Haiti",
    "HUN": "Hungary",
    "IDN": "Indonesia",
    "IND": "India",
    "IRL": "Ireland",
    "IRN": "Iran, Islamic Rep.",
    "IRQ": "Iraq",
    "ISL": "Iceland",
    "ISR": "Israel",
    "ITA": "Italy",
    "JAM": "Jamaica",
    "JOR": "Jordan",
    "JPN": "Japan",
    "KAZ": "Kazakhstan",
    "KEN": "Kenya",
    "KGZ": "Kyrgyz Republic",
    "KHM": "Cambodia",
    "KIR": "Kiribati",
    "KNA": "St. Kitts and Nevis",
    "KOR": "Korea, Rep.",
    "KWT": "Kuwait",
    "LAO": "Lao PDR",
    "LBN": "Lebanon",
    "LBR": "Liberia",
    "LBY": "Libya",
    "LCA": "St. Lucia",
    "LKA": "Sri Lanka",
    "LSO": "Lesotho",
    "LTU": "Lithuania",
    "LUX": "Luxembourg",
    "LVA": "Latvia",
    "MAC": "Macao SAR, China",
    "MDA": "Moldova",
    "MDG": "Madagascar",
    "MDV": "Maldives",
    "MEX": "Mexico",
    "MHL": "Marshall Islands",
    "MKD": "North Macedonia",
    "MLI": "Mali",
    "MLT": "Malta",
    "MMR": "Myanmar",
    "MNE": "Montenegro",
    "MNG": "Mongolia",
    "MOZ": "Mozambique",
    "MRT": "Mauritania",
    "MUS": "Mauritius",
    "MWI": "Malawi",
    "MYS": "Malaysia",
    "NAM": "Namibia",
    "NER": "Niger",
    "NGA": "Nigeria",
    "NIC": "Nicaragua",
    "NLD": "Netherlands",
    "NOR": "Norway",
    "NPL": "Nepal",
    "NZL": "New Zealand",
    "OMN": "Oman",
    "PAK": "Pakistan",
    "PAN": "Panama",
    "PER": "Peru",
    "PHL": "Philippines",
    "PLW": "Palau",
    "PNG": "Papua New Guinea",
    "POL": "Poland",
    "PRT": "Portugal",
    "PRY": "Paraguay",
    "PSE": "West Bank and Gaza",
    "QAT": "Qatar",
    "ROU": "Romania",
    "RUS": "Russian Federation",
    "RWA": "Rwanda",
    "SAU": "Saudi Arabia",
    "SDN": "Sudan",
    "SEN": "Senegal",
    "SGP": "Singapore",
    "SLB": "Solomon Islands",
    "SLE": "Sierra Leone",
    "SLV": "El Salvador",
    "SMR": "San Marino",
    "SOM": "Somalia",
    "SRB": "Serbia",
    "STP": "Sao Tome and Principe",
    "SUR": "Suriname",
    "SVK": "Slovak Republic",
    "SVN": "Slovenia",
    "SWE": "Sweden",
    "SWZ": "Eswatini",
    "SYC": "Seychelles",
    "SYR": "Syrian Arab Republic",
    "TCD": "Chad",
    "TGO": "Togo",
    "THA": "Thailand",
    "TJK": "Tajikistan",
    "TLS": "Timor-Leste",
    "TON": "Tonga",
    "TTO": "Trinidad and Tobago",
    "TUN": "Tunisia",
    "TUR": "Turkiye",
    "TUV": "Tuvalu",
    "TZA": "Tanzania",
    "UGA": "Uganda",
    "UKR": "Ukraine",
    "URY": "Uruguay",
    "USA": "United States",
    "UZB": "Uzbekistan",
    "VCT": "St. Vincent and the Grenadines",
    "VEN": "Venezuela, RB",
    "VNM": "Viet Nam",
    "VUT": "Vanuatu",
    "YEM": "Yemen, Rep.",
    "ZAF": "South Africa",
    "ZMB": "Zambia",
    "ZWE": "Zimbabwe"
}

# API endpoint and base parameters (filtered for monthly data)
url = "https://data360api.worldbank.org/data360/data"
params_base = {
    "DATABASE_ID": "IMF_IFS",
    "INDICATOR": "IMF_IFS_LUR",
    "timePeriodFrom": "1949-01",
    "timePeriodTo": "2024-12",
    "FREQ": "M"  # Filter for monthly data
}

# Initialize an empty list to store all data
all_data = []

# Pagination loop
skip = 0
while True:
    params = params_base.copy()
    params["skip"] = skip

    # Make the API request
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        values = data.get("value", [])
        if not values:  # Break if no more data is returned
            break
        all_data.extend(values)
        skip += 1000  # Increment skip for the next batch
        print(f"Fetched batch with {len(values)} records, total so far: {len(all_data)}")
        if skip == 0:  # Print raw data from the first batch for debugging
            print("Sample raw data from first batch:", json.dumps(values[0], indent=2))
    else:
        print(f"Error: {response.status_code} - {response.text}")
        break

# Pre-process data to convert OBS_VALUE to float, handling potential errors
print(f"Total records before processing: {len(all_data)}")
for i, item in enumerate(all_data):
    try:
        if item.get("OBS_VALUE") is not None:
            item["OBS_VALUE"] = float(item["OBS_VALUE"])
    except (ValueError, TypeError) as e:
        print(f"Error converting OBS_VALUE at index {i}: {e}, setting to None")
        item["OBS_VALUE"] = None  # Set to None if conversion fails
print(f"Total records after processing: {len(all_data)}")

# Convert the data to a Spark DataFrame with the defined schema
try:
    df_spark = spark.createDataFrame(all_data, schema=schema)
    print("DataFrame created successfully.")
except Exception as e:
    print(f"Error creating DataFrame: {e}")
    raise

# Add Freq Name column by mapping FREQ codes to full names
df_spark = df_spark.withColumn("Freq Name",
                              when(df_spark.FREQ == "A", "Anual")
                              .when(df_spark.FREQ == "M", "Mensual")
                              .when(df_spark.FREQ == "Q", "Trimestral")
                              .otherwise("Desconocido"))

# Create a mapping DataFrame for country codes to names
country_map_df = spark.createDataFrame(list(country_mapping.items()), ["REF_AREA", "País"])

# Join the main DataFrame with the country mapping DataFrame
df_spark = df_spark.join(broadcast(country_map_df), "REF_AREA", "left_outer")

# Select and rename columns to match your requirements
df_spark = df_spark.select(
    df_spark.TIME_PERIOD.alias("Año"),
    df_spark.FREQ.alias("Periodicidad"),
    df_spark["Freq Name"].alias("Freq Name"),
    df_spark.OBS_VALUE.alias("Tasa"),
    df_spark.REF_AREA.alias("País_ID"),
    df_spark.País.alias("País")
)

# Count the total number of rows
try:
    row_count = df_spark.count()
    print(f"Total number of rows in the DataFrame: {row_count}")
except Exception as e:
    print(f"Error counting rows: {e}")
    raise

# Write the DataFrame to a Parquet file
parquet_path = "parquet/unemployment_data_monthly"
df_spark.write \
    .mode("overwrite") \
    .partitionBy("Año") \
    .parquet(parquet_path)
print(f"Parquet file saved to {parquet_path}")

# Read the Parquet file back
try:
    df_read = spark.read.parquet(parquet_path)
    print("Parquet file read successfully.")
    df_read.printSchema()
    df_read.show()
except Exception as e:
    print(f"Error reading Parquet file: {e}")
    raise

# Stop the Spark session
#spark.stop()


# ╔══════════════════════════════════════════════════════════════╗
# ║  EXTRA:  guardar un único CSV en /content/unemployment.csv   ║
# ╚══════════════════════════════════════════════════════════════╝
csv_out = "/content/unemployment_data_monthly.csv"

(df_spark
    .coalesce(1)                       # fuerza un solo part‑0000.csv
    .write
    .option("header", "true")          # incluye encabezados
    .mode("overwrite")
    .csv("/content/_tmp_unemployment_csv"))

# Mueve el único part‑*.csv al nombre final
import glob, shutil, os
part_file = glob.glob("/content/_tmp_unemployment_csv/part-*.csv")[0]
shutil.move(part_file, csv_out)
shutil.rmtree("/content/_tmp_unemployment_csv")   # limpia temporal
print(f"✅ CSV guardado → {csv_out}")

# (Opcional) inspeccionar las primeras filas con pandas
import pandas as pd
print(pd.read_csv(csv_out).head())


# ## **INDICADOR INFLACION**
# 
# ---
# 
# 

# In[ ]:


pip install pyspark


# In[ ]:


pip install requests pandas python-dateutil


# In[15]:


# ╔════════════════════════════════════════════════════════╗
# ║  Inflación promedio 12 m (Power BI) → CSV en /content/ ║
# ╚════════════════════════════════════════════════════════╝
import json, requests, pandas as pd

# 1⃣  URL y cabeceras comunes
QUERY_URL = "https://wabi-us-east2-api.analysis.windows.net/public/reports/querydata?synchronous=true"
HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "X-PowerBI-ResourceKey": "d2d3b042-b343-4f05-85cb-be05eb64dd22"
}

# 2⃣  Pega aquí TU payload completo  (empieza { y termina })
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
          "DataReduction":{ "DataVolume":4,
                            "Primary":{ "BinnedLineSample":{} }},
          "Version":1 },
        "ExecutionMetricsKind":1
    } } ] },
      "ApplicationContext":{ "DatasetId":4966303 }
    }
  ],
  "cancelQueries":[],
  "modelId":4966303
}
""".strip()

payload = json.loads(payload_txt)   # convierte cadena → dict

# 3⃣  Llamada POST
r = requests.post(QUERY_URL, headers=HEADERS, json=payload, timeout=30)
r.raise_for_status()
dsr = r.json()["results"][0]["result"]["data"]["dsr"]

# 4⃣  Extraer matriz de datos
ph0    = dsr["DS"][0]["PH"][0]
dm_key = next(k for k in ph0 if k.startswith("DM"))
rows   = ph0[dm_key]
print("Filas:", len(rows))

# 5⃣  DataFrame con conversión de tipos
df = (
    pd.DataFrame(
        {
            "Fecha": pd.to_datetime([r["C"][0] for r in rows], unit="ms"),
            "Inflacion_12m_pct": [float(r["C"][1]) for r in rows],
        }
    )
    .sort_values("Fecha")
    .assign(Año=lambda d: d["Fecha"].dt.year)
)

# 6⃣  Guardar CSV en /content/
csv_out = "/content/inflacion_rd_12m.csv"
df.to_csv(csv_out, index=False, encoding="utf-8")
print("✅ CSV generado →", csv_out)
df.head()


# ### **INDICADOR TASA DE CAMBIO**

# In[14]:


import json, requests, pandas as pd, pathlib, re

# ── 1. Guarda aquí tu payload completo (sin escapes) ──
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
""".strip()

# ── 2. Guardar y validar ──
payload_path = pathlib.Path("/content/payload_tcd.json")
payload_path.write_text(payload_txt, "utf-8")
payload = json.loads(payload_txt)           # ← no debe fallar
print("✅ payload guardado:", payload_path)

# ── 3. Endpoint y cabeceras ──
QUERY_URL = "https://wabi-us-east2-api.analysis.windows.net/public/reports/querydata?synchronous=true"
HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "X-PowerBI-ResourceKey": "d2d3b042-b343-4f05-85cb-be05eb64dd22"
}

# ── 4. POST al servicio ──
resp = requests.post(QUERY_URL, headers=HEADERS, json=payload, timeout=30)
resp.raise_for_status()

dsr  = resp.json()["results"][0]["result"]["data"]["dsr"]
ph0  = dsr["DS"][0]["PH"][0]
dm_key = next(k for k in ph0 if k.startswith("DM"))
rows = ph0[dm_key]
print(f"Filas descargadas: {len(rows)} ({dm_key})")

# ── 5. Mapear columnas con detección automática ──
fechas, valores = [], []

for r in rows:
    c = r["C"]
    if len(c) == 0:
        continue
    elif len(c) == 1:
        # Si solo llega un valor descartamos (no se puede saber cuál es)
        continue
    else:                       # len(c) ≥ 2
        # Identifica qué índice es timestamp (número grande en ms)
        if c[0] > 1e11:
            ts, val = c[0], c[1]
        elif c[1] > 1e11:
            ts, val = c[1], c[0]
        else:
            continue            # ninguno parece timestamp
    fechas.append(ts)
    valores.append(val)

df = (
    pd.DataFrame(
        {
            "Fecha":  pd.to_datetime(fechas, unit="ms", errors="coerce"),
            "Tipo_Cambio_Dolar": pd.to_numeric(valores, errors="coerce")
        }
    )
    .dropna()
    .sort_values("Fecha")
)

# ── 6. Guardar CSV ──
csv_out = "/content/tipo_cambio_dolar.csv"
df.to_csv(csv_out, index=False, encoding="utf-8")
print(f"✅ CSV generado: {csv_out} — filas finales: {len(df)}")
df.head()

