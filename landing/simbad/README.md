# SIMBAD Data Loaders

Carga de datos de la Superintendencia de Bancos de República Dominicana (SIMBAD) optimizada para diferentes tipos de uso.

## 📁 Estructura

```
📁 simbad/
├── 📁 historical/          # Carga histórica completa (2012-presente)
│   ├── main_simbad.py      # FastAPI service
│   ├── Dockerfile
│   ├── cloudbuild.yaml     # Deploy service
│   ├── simbad/
│   │   └── harvester.py    # Lógica de carga histórica
│   └── README.md
└── 📁 incremental/         # Carga incremental (últimos períodos)
    ├── main_simbad.py      # FastAPI service
    ├── Dockerfile
    ├── cloudbuild.yaml     # Deploy service
    ├── cloudbuild.job.yaml # Deploy job programado
    ├── simbad/
    │   ├── harvester_incremental.py  # Lógica incremental
    │   └── runner_incremental.py     # Entry point para job
    └── README.md
```

## 🎯 Casos de Uso

### 1. **Historical** - Carga completa una sola vez
- **Cuándo**: Inicialización del sistema, migración, recuperación completa
- **Tiempo**: 45-90 minutos
- **Datos**: 2012-presente (13+ años)
- **Recurso**: 2 CPU, 2GB RAM, timeout 2h

### 2. **Incremental** - Actualizaciones periódicas
- **Cuándo**: Mensual, programado, actualizaciones regulares
- **Tiempo**: 2-5 minutos
- **Datos**: Últimos 3 meses (configurable)
- **Recurso**: 1 CPU, 1GB RAM, timeout 30min

## 🚀 Deployment

### Historical Service
```bash
gcloud builds submit --config=landing/simbad/historical/cloudbuild.yaml
```

### Incremental Service
```bash
gcloud builds submit --config=landing/simbad/incremental/cloudbuild.yaml
```

### Incremental Job (recomendado para scheduler)
```bash
gcloud builds submit --config=landing/simbad/incremental/cloudbuild.job.yaml
```

## 📊 Data Output

### Historical
```
gs://bucket/prefix/dataset/dt=YYYY-MM-DD/
└── consolidado_AAyP_hipotecarios_2012_2025_timestamp.csv
```

### Incremental
```
gs://bucket/prefix/dataset/
├── incremental/
│   ├── periodo=YYYY-MM/
│   │   └── carteras_AAyP_hipotecarios_YYYY-MM.csv
│   └── dt=YYYY-MM-DD/
│       └── incremental_AAyP_hipotecarios_3months_timestamp.csv
```

## ⚙️ Configuración

### Variables comunes
- `GCS_BUCKET`: Bucket de destino
- `LANDING_PREFIX`: Prefijo en GCS
- `SB_API_KEY`: API key de SIMBAD
- `SB_TIPO_ENTIDAD`: Tipo de entidad (default: AAyP)
- `SB_DATASET`: Nombre del dataset

### Específicas incremental
- `SB_LOOKBACK_MONTHS`: Meses hacia atrás (default: 3)

### Específicas historical
- `SB_START_YEAR`: Año inicial (default: 2012)
- `SB_KEEP_MONTHLY`: Archivos mensuales (default: false)

## 🔄 Flujo Recomendado

1. **Setup inicial**: Ejecutar `historical` una sola vez
2. **Mantenimiento**: Configurar `incremental` con Cloud Scheduler
3. **Reprocesamiento**: Usar `incremental/run/force-periods` para períodos específicos

## 📈 Monitoring

Ambos servicios incluyen endpoints de health check y logging detallado para monitoreo en Cloud Run/Cloud Logging.