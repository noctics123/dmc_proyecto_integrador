# SIMBAD Incremental Data Loader

## Propósito
Carga incremental inteligente que actualiza solo los últimos períodos. Diseñado para **ejecución periódica** (mensual/programada) para mantener los datos actualizados.

## Cuándo usar
- **Actualización mensual**: Para capturar nuevos datos del mes actual
- **Reprocesamiento**: Para actualizar datos de meses recientes que pudieron cambiar
- **Mantenimiento**: Ejecución programada via Cloud Scheduler

## Características principales

### 🧠 Carga inteligente
- Detecta automáticamente el último período cargado en GCS
- Carga solo los últimos N meses (default: 3)
- Evita descargar toda la historia como el loader histórico

### ⚡ Optimizado para velocidad
- **Tiempo estimado**: 2-5 minutos
- Solo descarga datos recientes
- Perfecto para Cloud Scheduler

### 🎯 Flexibilidad
- Configurar períodos de retrospectiva via `SB_LOOKBACK_MONTHS`
- Forzar períodos específicos via endpoint `/run/force-periods`

## Configuración

### Variables de entorno requeridas:
- `GCS_BUCKET`: Bucket de destino
- `LANDING_PREFIX`: Prefijo en GCS
- `SB_API_KEY`: API key de SIMBAD

### Variables opcionales:
- `SB_TIPO_ENTIDAD`: Tipo de entidad (default: `AAyP`)
- `SB_DATASET`: Nombre del dataset (default: `simbad_carteras_aayp_hipotecarios`)
- `SB_LOOKBACK_MONTHS`: Meses hacia atrás (default: `3`)

## Endpoints

### `POST /run`
Carga incremental automática de últimos períodos.

**Body (opcional):**
```json
{
  "run_date": "2025-01-15"
}
```

### `POST /run/force-periods`
Fuerza carga de períodos específicos.

**Body:**
```json
{
  "periods": ["2024-12", "2025-01"],
  "run_date": "2025-01-15"
}
```

## Output Structure
```
gs://bucket/prefix/dataset/
├── incremental/
│   ├── periodo=2024-12/
│   │   └── carteras_AAyP_hipotecarios_2024-12.csv
│   ├── periodo=2025-01/
│   │   └── carteras_AAyP_hipotecarios_2025-01.csv
│   └── dt=2025-01-15/
│       └── incremental_AAyP_hipotecarios_3months_timestamp.csv
```

## Casos de uso

### 1. Ejecución mensual programada
```bash
# Cloud Scheduler ejecuta cada mes
curl -X POST https://simbad-incremental-service/run
```

### 2. Reprocesar períodos específicos
```bash
curl -X POST https://simbad-incremental-service/run/force-periods \
  -H "Content-Type: application/json" \
  -d '{"periods": ["2024-11", "2024-12"]}'
```

### 3. Configuración para más retrospectiva
```bash
# Configurar para revisar últimos 6 meses
export SB_LOOKBACK_MONTHS=6
```

## Ventajas vs Historical

| Aspecto | Incremental | Historical |
|---------|-------------|------------|
| **Tiempo** | 2-5 min | 45-90 min |
| **Datos** | Últimos 3 meses | 2012-presente |
| **Uso** | Periódico | Una vez |
| **Recursos** | Mínimos | Intensivos |
| **Scheduler** | ✅ Ideal | ❌ No recomendado |