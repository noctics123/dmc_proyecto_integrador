# Lakehouse Processing Pipeline

Pipeline de procesamiento de datos para arquitectura lakehouse con DataProc y PySpark.

## 📁 Estructura

```
lakehouse_processing/
├── 📁 notebooks/                   # Notebooks de DataProc (Jupyter)
│   ├── bronze_simbad_ingestion.ipynb          # Landing → Bronze SIMBAD
│   ├── bronze_macroeconomics_ingestion.ipynb  # Landing → Bronze Macroeconomía
│   ├── silver_data_cleaning.ipynb             # Bronze → Silver (limpieza)
│   └── gold_metrics_aggregation.ipynb         # Silver → Gold (métricas)
│
└── README.md                       # Documentación del pipeline DataProc
```

## 🎯 Capas del Lakehouse

### **Landing Layer**
- **Formato**: CSV raw data
- **Fuentes**: SIMBAD API, PowerBI Macroeconomía
- **Estructura**: `gs://bucket/lakehouse/landing/{source}/dt=YYYY-MM-DD/`

### **Bronze Layer**
- **Formato**: Parquet (sin transformaciones)
- **Propósito**: Ingesta raw con esquema estable
- **Particionado**: Por año/mes (SIMBAD), por dt_captura (Macro)
- **Estructura**: `gs://bucket/lakehouse/bronze/{source}/anio=*/mes=*/`

### **Silver Layer**
- **Formato**: Parquet (datos limpios)
- **Propósito**: Deduplicación, normalización, validación
- **Estructura**: `gs://bucket/lakehouse/silver/{source}/`

### **Gold Layer**
- **Formato**: Parquet (métricas de negocio)
- **Propósito**: KPIs, agregaciones, métricas finales
- **Estructura**: `gs://bucket/lakehouse/gold/{metric_type}/`

## 🚀 Uso en DataProc

### Configuración del Cluster
```bash
# Cluster actual configurado con:
- Jupyter habilitado
- ZEPPELIN, TRINO, DELTA disponibles
- Ubicación: us-central1
- Nombre: cluster-integrador-2025
```

### Ejecutar Notebooks
1. **Acceder a Jupyter**:
   - Consola GCP → DataProc → Clusters → Web Interfaces
   - Seleccionar Jupyter

2. **Ubicación de Notebooks**:
   - Los notebooks están en esta carpeta del repositorio
   - Copiar a DataProc o ejecutar directamente

3. **Orden de Ejecución**:
   ```
   1. bronze_simbad_ingestion.ipynb      # Si hay datos SIMBAD nuevos
   2. bronze_macroeconomics_ingestion.ipynb  # Para datos macro
   3. silver_data_cleaning.ipynb         # Limpieza de datos
   4. gold_metrics_aggregation.ipynb     # Métricas finales
   ```

## 📊 Datasets Procesados

### SIMBAD (Superintendencia de Bancos RD)
- **Fuente**: `landing/simbad/simbad_carteras_aayp_hipotecarios/`
- **Contenido**: Créditos hipotecarios AAyP (2012-presente)
- **Volumen**: ~676K filas por carga completa
- **Frecuencia**: Incremental (mensual via landing)

### Macroeconomía
- **Fuente**: `landing/macroeconomics/`
- **Datasets**:
  - `desempleo_imf` - Datos de desempleo FMI
  - `inflacion_12m` - Inflación anualizada
  - `tipo_cambio` - Tasas de cambio
- **Frecuencia**: Diaria (via PowerBI scraper)

## ⚙️ Configuración

### Variables Principales (config.py)
```python
BUCKET = "dae-integrador-2025"
REGION = "us-west2"
CLUSTER = "cluster-integrador-2025"
```

### Esquemas y Particionado
- **SIMBAD**: Particionado por `anio`/`mes` basado en campo `periodo`
- **Macroeconomía**: Particionado por `dt_captura` (fecha de carga)

## 🔧 Desarrollo

### Añadir Nuevo Dataset
1. Actualizar `config.py` con nueva configuración
2. Crear notebook específico en `notebooks/`
3. Definir esquema y particionado apropiado
4. Implementar validaciones de calidad

### Debugging
- Logs disponibles en DataProc cluster
- Verificar datos en cada capa antes de procesar siguiente
- Usar `.count()` y `.show()` para validar transformaciones

## 📈 Monitoreo

### Métricas Clave
- **Filas procesadas** por capa
- **Tiempo de ejecución** por notebook
- **Calidad de datos** (nulls, duplicados)
- **Cobertura temporal** (períodos procesados)

### Validaciones
- Verificar particiones creadas correctamente
- Confirmar esquemas consistentes entre capas
- Validar rangos de fechas y valores numéricos