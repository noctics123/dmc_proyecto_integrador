import os
import tempfile
import datetime as dt
from typing import Optional, Dict, Any, List
import json
import pandas as pd
import requests
from fastapi import FastAPI, Body, HTTPException
from google.cloud import storage
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ========= CONFIG =========
BUCKET = os.getenv("GCS_BUCKET", "dae-integrador-2025")
BASE_PREFIX = os.getenv("LANDING_PREFIX", "lakehouse/landing/macroeconomics")
QUERY_URL = "https://wabi-us-east2-api.analysis.windows.net/public/reports/querydata?synchronous=true"
HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "X-PowerBI-ResourceKey": "d2d3b042-b343-4f05-85cb-be05eb64dd22"
}
# =========================

app = FastAPI(title="Macroeconomics Scraper", version="1.0.0")

# ---------- Utils ----------
def _normalize_date(run_date: Optional[str]) -> str:
    """
    Devuelve YYYY-MM-DD. Si run_date viene, valida formato ISO.
    """
    if run_date:
        try:
            d = dt.date.fromisoformat(run_date)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"run_date debe ser YYYY-MM-DD; recibido: {run_date}")
    else:
        d = dt.date.today()
    return d.isoformat()

def _save_df_to_gcs(df: pd.DataFrame, dataset: str, date_str: str, filename: str) -> str:
    """
    Guarda df como CSV en: gs://<bucket>/<BASE_PREFIX>/<dataset>/dt=<date_str>/<filename>
    """
    try:
        object_name = f"{BASE_PREFIX}/{dataset}/dt={date_str}/{filename}"
        client = storage.Client()
        bkt = client.bucket(BUCKET)
        blob = bkt.blob(object_name)

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=True) as tmp:
            df.to_csv(tmp.name, index=False, encoding="utf-8")
            blob.upload_from_filename(tmp.name, content_type="text/csv")

        path = f"gs://{BUCKET}/{object_name}"
        logger.info(f"[WRITE] {path}")
        return path
    except Exception as e:
        logger.error(f"Error saving to GCS: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to save to GCS: {str(e)}")

# ---------- Extracciones ----------
def extract_inflacion_12m(run_date: str) -> pd.DataFrame:
    """
    Scrape 12-month inflation data from PowerBI API for Dominican Republic.
    """
    try:
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

        payload = json.loads(payload_txt)
        r = requests.post(QUERY_URL, headers=HEADERS, json=payload, timeout=30)
        r.raise_for_status()
        dsr = r.json()["results"][0]["result"]["data"]["dsr"]
        ph0 = dsr["DS"][0]["PH"][0]
        dm_key = next(k for k in ph0 if k.startswith("DM"))
        rows = ph0[dm_key]

        df = pd.DataFrame(
            {
                "Fecha": pd.to_datetime([r["C"][0] for r in rows], unit="ms"),
                "inflacion": [float(r["C"][1]) for r in rows],
                "pais": "Dominican Republic"
            }
        ).sort_values("Fecha").assign(Fecha=lambda x: x["Fecha"].dt.strftime("%Y-%m"))

        logger.info(f"Extracted {len(df)} inflation records")
        return df
    except Exception as e:
        logger.error(f"Error extracting inflation data: {str(e)}")
        return pd.DataFrame()

def extract_tipo_cambio(run_date: str) -> pd.DataFrame:
    """
    Scrape exchange rate data from PowerBI API for USD.
    """
    try:
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
                if c[0] > 1e11:
                    ts, val = c[0], c[1]
                elif c[1] > 1e11:
                    ts, val = c[1], c[0]
                else:
                    continue
                fechas.append(ts)
                valores.append(val)

        df = pd.DataFrame(
            {
                "fecha": pd.to_datetime(fechas, unit="ms", errors="coerce"),
                "tc_venta": pd.to_numeric(valores, errors="coerce")
            }
        ).dropna().sort_values("fecha")
        df["tc_compra"] = df["tc_venta"] * 0.995  # Approximate tc_compra as per original example
        df["fecha"] = df["fecha"].dt.strftime("%Y-%m-%d")

        logger.info(f"Extracted {len(df)} exchange rate records")
        return df
    except Exception as e:
        logger.error(f"Error extracting exchange rate data: {str(e)}")
        return pd.DataFrame()

def extract_desempleo_imf(run_date: str) -> pd.DataFrame:
    """
    Scrape unemployment data from WorldBank API.
    """
    try:
        url = "https://data360api.worldbank.org/data360/data"
        params_base = {
            "DATABASE_ID": "IMF_IFS",
            "INDICATOR": "IMF_IFS_LUR",
            "timePeriodFrom": "1949-01",
            "timePeriodTo": "2024-12",
            "FREQ": "M"
        }
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

        all_data = []
        skip = 0
        while True:
            params = params_base.copy()
            params["skip"] = skip
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                values = data.get("value", [])
                if not values:
                    break
                all_data.extend(values)
                skip += 1000
                logger.info(f"Fetched {len(values)} unemployment records, total: {len(all_data)}")
            else:
                logger.error(f"Error fetching unemployment data: {response.status_code} - {response.text}")
                return pd.DataFrame()

        for item in all_data:
            try:
                if item.get("OBS_VALUE") is not None:
                    item["OBS_VALUE"] = float(item["OBS_VALUE"])
            except (ValueError, TypeError) as e:
                logger.warning(f"Error converting OBS_VALUE: {e}, setting to None")
                item["OBS_VALUE"] = None

        df = pd.DataFrame(all_data)[["OBS_VALUE", "TIME_PERIOD", "FREQ", "REF_AREA"]]
        df = df.rename(columns={
            "OBS_VALUE": "tasa_desempleo",
            "TIME_PERIOD": "anio",
            "FREQ": "periodicidad",
            "REF_AREA": "pais_id"
        })
        df["pais"] = df["pais_id"].map(country_mapping)
        df["periodicidad"] = df["periodicidad"].map({"A": "Anual", "M": "Mensual", "Q": "Trimestral"}).fillna("Desconocido")

        logger.info(f"Extracted {len(df)} unemployment records")
        return df
    except Exception as e:
        logger.error(f"Error extracting unemployment data: {str(e)}")
        return pd.DataFrame()

# ---------- Pipeline ----------
def run_pipeline(run_date: Optional[str] = None) -> Dict[str, Any]:
    date_str = _normalize_date(run_date)
    saved = []

    try:
        # 1) Inflación 12m
        df_infl = extract_inflacion_12m(date_str)
        if not df_infl.empty:
            saved.append(_save_df_to_gcs(df_infl, "inflacion_12m", date_str, "inflacion_12m.csv"))

        # 2) Tipo de cambio
        df_tc = extract_tipo_cambio(date_str)
        if not df_tc.empty:
            saved.append(_save_df_to_gcs(df_tc, "tipo_cambio", date_str, "tipo_cambio.csv"))

        # 3) Desempleo (IMF)
        df_des = extract_desempleo_imf(date_str)
        if not df_des.empty:
            saved.append(_save_df_to_gcs(df_des, "desempleo_imf", date_str, "desempleo_imf.csv"))

        return {"saved": saved, "date_partition": f"dt={date_str}"}
    except Exception as e:
        logger.error(f"Pipeline error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Pipeline failed: {str(e)}")

# ---------- Endpoints ----------
@app.get("/healthz")
def healthz():
    return {
        "status": "ok",
        "bucket": BUCKET,
        "base_prefix": BASE_PREFIX,
    }

@app.post("/run")
def run(body: Optional[Dict[str, Any]] = Body(default=None)):
    run_date = body.get("run_date") if body else None
    return {"ok": True, **run_pipeline(run_date)}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
