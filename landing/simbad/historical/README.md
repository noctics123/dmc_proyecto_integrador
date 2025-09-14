# SIMBAD Historical Data Loader

## Propósito
Carga histórica completa de datos SIMBAD desde 2012 hasta el presente. **Ejecutar solo una vez** para inicializar el sistema con todos los datos históricos.

## Cuándo usar
- **Inicialización**: Primera vez que se configura el pipeline de datos
- **Recuperación**: Cuando se necesita reprocesar toda la información histórica
- **Migración**: Al cambiar de estructura de datos o bucket

## Configuración

### Variables de entorno requeridas:
- `GCS_BUCKET`: Bucket de destino (ej: `dae-integrador-2025`)
- `LANDING_PREFIX`: Prefijo en GCS (ej: `lakehouse/landing/simbad`)
- `SB_API_KEY`: API key de SIMBAD

### Variables opcionales:
- `SB_TIPO_ENTIDAD`: Tipo de entidad (default: `AAyP`)
- `SB_START_YEAR`: Año inicial (default: `2012`)
- `SB_DATASET`: Nombre del dataset (default: `simbad_carteras_aayp_hipotecarios`)
- `SB_KEEP_MONTHLY`: Guardar archivos mensuales (default: `false`)

## Endpoints
- `GET /healthz`: Health check
- `POST /run`: Ejecuta carga histórica completa

## Output
```
gs://bucket/prefix/dataset/dt=YYYY-MM-DD/
└── consolidado_AAyP_hipotecarios_2012_2025_timestamp.csv
```

## Tiempo estimado
⏱️ **45-90 minutos** (dependiendo del volumen de datos)

## ⚠️ Importante
- Esta carga descarga **13+ años de datos**
- Solo ejecutar cuando sea necesario reprocesar toda la información
- Para actualizaciones regulares, usar la versión **incremental**