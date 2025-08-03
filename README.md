# Manual: Desplegar un Scraper FastAPI en Google Cloud Run + Scheduler

## ✨ Objetivo

Desplegar una API en FastAPI que scrapea datos, los guarda en GCS y se ejecuta automáticamente con Cloud Scheduler.

---

## 🔧 Requisitos

- Cuenta en Google Cloud Platform con facturación activa
- Servicios habilitados:
  ```bash
  gcloud services enable run.googleapis.com \
      cloudbuild.googleapis.com \
      cloudscheduler.googleapis.com \
      storage.googleapis.com
  ```
- Herramientas instaladas:
  - `gcloud CLI`
  - `Docker` (si haces build local opcional)

---

## 📝 Paso 1: Preparar el Proyecto

### Archivos necesarios:

#### `main.py`

Contiene toda la lógica de scraping, GCS y API con FastAPI. (Ya está implementado)

#### `requirements.txt`

```txt
fastapi
uvicorn[standard]
pandas
requests
google-cloud-storage
```

#### `Dockerfile`

```Dockerfile
FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

ENV PORT 8080
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
```

---

## 🚁 Paso 2: Crear Bucket en GCS

```bash
gsutil mb -l us-west2 gs://dmc_proy_storage
```

---

## 🚀 Paso 3: Desplegar a Cloud Run

```bash
SERVICE_NAME="landing-scraper"
REGION="us-west2"
PROJECT_ID="<tu-proyecto-id>"

gcloud run deploy $SERVICE_NAME \
  --source . \
  --region $REGION \
  --allow-unauthenticated \
  --set-env-vars GCS_BUCKET=dmc_proy_storage,LANDING_PREFIX=landing/macroeconomics
```

Al terminar verás la URL: `https://<region>.run.app`.

---

## ⏰ Paso 4: Crear Job en Cloud Scheduler

```bash
gcloud scheduler jobs create http landing_scraper_trigger \
  --location=us-west2 \
  --schedule="0 6 * * *" \
  --uri="https://<CLOUD-RUN-URL>/run" \
  --http-method=POST \
  --message-body="{}"
```

---

## ✅ Paso 5: Probar manualmente

Haz una petición POST a:

```
https://<CLOUD-RUN-URL>/run
```

Desde navegador, Postman o `curl`.

---

## 📃 Paso 6: Verificar archivos en GCS

Archivos guardados en:

```
gs://dmc_proy_storage/landing/macroeconomics/<dataset>/dt=<YYYY-MM-DD>/*.csv
```

---

## 🔍 Paso 7: Logs y monitoreo

```bash
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=landing-scraper" --limit=50
```

---

## 🏑 Opcional: CI/CD con GitHub

- Subir el código a GitHub
- Conectar repo a Cloud Build
- Automatizar el deploy con cada `push` o `tag`

---

## 🌍 Enlaces últiles

- Cloud Run: [https://console.cloud.google.com/run](https://console.cloud.google.com/run)
- Cloud Scheduler: [https://console.cloud.google.com/cloudscheduler](https://console.cloud.google.com/cloudscheduler)
- Cloud Storage: [https://console.cloud.google.com/storage](https://console.cloud.google.com/storage)
- Logs: [https://console.cloud.google.com/logs](https://console.cloud.google.com/logs)

---

> Manual creado para recrear scraper de macroeconomía desde cero en Google Cloud.

