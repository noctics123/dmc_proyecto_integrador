# DMC Proyecto Integrador - Data Pipeline Platform

## 🎯 Descripción del Proyecto

Plataforma completa de data engineering para webscraping y procesamiento de datos macroeconómicos y bancarios de República Dominicana, con despliegue automatizado en Google Cloud Platform.

## 🏗️ Arquitectura del Sistema

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Landing Zone  │ → │   Bronze Layer   │ → │  Silver Layer   │
│   (Web Scraping)│    │   (Raw Parquet)  │    │ (Cleaned Data)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
┌─────────────────┐    ┌──────────────────┐            │
│   Gold Layer    │ ← │  Business Logic  │ ←──────────┘
│ (Analytics Ready)│    │   & Aggregation  │
└─────────────────┘    └──────────────────┘
```

### Diferenciación de Tecnologías por Caso de Uso:

- **BigQuery**: Cargas mensuales regulares (eficiente, bajo costo)
- **DataProc**: Reprocesos históricos masivos (flexibilidad total)
- **Cloud Run**: Servicios de scraping (serverless, escalable)
- **Web App**: Gestión y monitoreo para usuarios no técnicos

## 📊 Fuentes de Datos

### 1. Datos Macroeconómicos
- **Inflación 12m**: API PowerBI - República Dominicana
- **Tipo de Cambio USD**: API PowerBI - Banco Central
- **Desempleo Mundial**: WorldBank API - IMF

### 2. Datos Bancarios SIMBAD
- **Carteras de Créditos**: API SIMBAD - Superintendencia de Bancos
- **Período**: 2012-presente
- **Frecuencia**: Mensual
- **Tipo**: Hipotecarios AAyP (Asalariado Privado)

## 🚀 Estructura del Repositorio

```
dmc_proyecto_integrador/
├── landing_run/              # Scraper macroeconomía (FUNCIONAL)
├── landing_simbad/           # Scraper SIMBAD (FUNCIONAL)
├── etl_spark/               # Pipeline ETL Spark
│   ├── bronze/              # Landing → Bronze jobs
│   ├── silver/              # Bronze → Silver transformations
│   ├── gold/                # Silver → Gold aggregations
│   └── common/              # Utilidades compartidas
├── sql/                     # Stored Procedures & Queries
│   ├── sp_bronze_create.sql # External tables bronze
│   ├── sp_silver_upsert.sql # MERGE incremental silver
│   ├── sp_gold_create.sql   # Métricas gold agregadas
│   └── config/              # Configuraciones GCP
├── notebooks/               # Jupyter notebooks desarrollo
├── web_app/                 # Management UI
│   ├── backend/             # FastAPI backend
│   ├── frontend/            # React frontend
│   └── gcp_integration/     # SDK wrappers
└── docs/                    # Documentación
```

## ⚙️ Pipeline de Datos

### Flujo Principal:
1. **Landing** (Cloud Run): Scraping automático mensual
2. **Bronze** (DataProc/BigQuery): Ingesta raw a Parquet
3. **Silver** (BigQuery): Limpieza y deduplicación
4. **Gold** (BigQuery): Métricas de negocio agregadas

### Orquestación:
- **Cloud Scheduler**: Trigger día 20 de cada mes, 12:00 AM PET
- **Cloud Workflows**: Secuenciación de jobs ETL
- **Cloud Build**: CI/CD automático desde GitHub

## 🛠️ Configuración de Infraestructura GCP

### Proyecto GCP: `proyecto-integrador-dae-2025`

### Recursos Configurados:
- **BigQuery Datasets**: `bronze`, `silver_clean`, `gold`, `dataset_dae`, `supply`
- **Cloud Storage**: `dae-integrador-2025` (lakehouse structure)
- **DataProc Cluster**: `cluster-integrador-2025` (us-central1-a)
  - Master: n2-standard-2 (1 instance)
  - Workers: n2-standard-2 (4 instances)
  - Components: Jupyter, Zeppelin, Trino, Delta

### Secret Manager:
- `sb_api_key`: API key para SIMBAD

## 📅 Cronograma de Ejecución

### Mensual (Día 20):
```
12:00 AM - Landing scrapers (macroeconomía + SIMBAD)
02:00 AM - Bronze ETL (DataProc/BigQuery)
04:00 AM - Silver ETL (BigQuery MERGE)
06:00 AM - Gold ETL (BigQuery aggregations)
```

### Bajo Demanda:
- **Reprocesos históricos**: DataProc cluster
- **Análisis exploratorio**: Jupyter notebooks
- **Deployments**: CI/CD triggers

## 🌐 Web App de Gestión

### Funcionalidades para Usuarios No Técnicos:
- **Dashboard**: Estado de pipelines en tiempo real
- **Deploy Queue**: Gestión visual de despliegues
- **Configuración**: APIs, schedules, parámetros ETL
- **Monitoreo**: Data quality, costos, performance
- **Logs**: Visualización de errores y progreso

### Roles de Usuario:
- **Admin**: Acceso completo + gestión usuarios
- **Data Engineer**: Configuraciones técnicas + debugging
- **Business Analyst**: Solo lectura + configs básicas
- **Viewer**: Dashboard y reportes únicamente

## 🔧 Comandos de Desarrollo

### Backend (FastAPI):
```bash
cd web_app/backend
pip install -r requirements.txt
uvicorn main:app --reload
```

### Frontend (React):
```bash
cd web_app/frontend
npm install
npm start
```

### ETL Jobs (DataProc):
```bash
# Bronze job
gcloud dataproc jobs submit pyspark \\
  gs://dae-integrador-2025/jobs/etl_spark/bronze/land_simbad_bronze.py \\
  --cluster=cluster-integrador-2025 \\
  --region=us-central1

# Silver job  
gcloud dataproc jobs submit pyspark \\
  gs://dae-integrador-2025/jobs/etl_spark/silver/silver_simbad_job.py \\
  --cluster=cluster-integrador-2025 \\
  --region=us-central1 \\
  --args="proyecto-integrador-dae-2025,dae-integrador-2025"
```

## 📊 Métricas de Calidad

### Data Quality Checks:
- **Freshness**: Datos < 24h desde scraping
- **Completeness**: Records esperados vs reales
- **Accuracy**: Validaciones de negocio (rangos, formatos)
- **Schema**: Consistencia de tipos de datos

### Alertas Configuradas:
- **Pipeline failures**: Slack/email notifications
- **Data quality degradation**: Automated anomaly detection
- **Cost spikes**: Budget threshold alerts
- **Performance issues**: SLA violation alerts

## 🚀 Próximos Desarrollos

### Fase 1 - Pipeline Completion (Semanas 1-2):
- [ ] Integrar scripts ETL existentes
- [ ] Automatizar stored procedures SQL
- [ ] Configurar orquestación completa

### Fase 2 - Web App MVP (Semanas 3-6):
- [ ] Backend API con GCP integration
- [ ] Frontend dashboard básico
- [ ] Deploy interface con logs en tiempo real

### Fase 3 - Features Avanzadas (Semanas 7-10):
- [ ] Advanced monitoring y alertas
- [ ] User management y roles
- [ ] Cost optimization y performance tuning

## 📝 Contribución

1. Fork del repositorio
2. Feature branch: `git checkout -b feature/nueva-funcionalidad`
3. Commit cambios: `git commit -am 'Add nueva funcionalidad'`
4. Push branch: `git push origin feature/nueva-funcionalidad`
5. Submit Pull Request

## 📄 Licencia

Este proyecto es propiedad de la Universidad Tecnológica del Perú - Proyecto Integrador DMC.

## 🔗 Enlaces Útiles

- [DataProc Cluster Console](https://console.cloud.google.com/dataproc/clusters/detail/us-central1/cluster-integrador-2025)
- [BigQuery Datasets](https://console.cloud.google.com/bigquery?project=proyecto-integrador-dae-2025)
- [Cloud Storage Bucket](https://console.cloud.google.com/storage/browser/dae-integrador-2025)
- [Cloud Build Triggers](https://console.cloud.google.com/cloud-build/triggers)
- [Cloud Scheduler Jobs](https://console.cloud.google.com/cloudscheduler)