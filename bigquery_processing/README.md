# BigQuery Processing Pipeline

Pipeline de transformación de datos usando BigQuery para capas Silver y Gold del lakehouse.

## 📊 Arquitectura Actual

### **Flujo de Datos**
```
DataProc (Notebooks) → Bronze (External Tables) → BigQuery Silver → BigQuery Gold
```

### **Datasets en BigQuery**
- **bronze**: Tablas externas que apuntan a Parquet en GCS
- **silver_clean**: Datos limpios y normalizados
- **gold**: Métricas agregadas y KPIs de negocio

## 📁 Estructura del Repositorio

```
bigquery_processing/
├── 📁 bronze_to_silver/           # Transformaciones Bronze → Silver
│   ├── simbad_cleaning.sql        # Limpieza de datos SIMBAD
│   └── macroeconomics_cleaning.sql # Normalización datos macro
│
├── 📁 silver_to_gold/             # Transformaciones Silver → Gold
│   ├── simbad_aggregations.sql    # KPIs y métricas SIMBAD
│   └── combined_metrics.sql       # Métricas combinadas con macro
│
├── 📁 stored_procedures/          # Stored Procedures con prefijo sp_
│   ├── sp_process_bronze_to_silver.sql
│   ├── sp_process_silver_to_gold.sql
│   └── sp_full_pipeline_refresh.sql
│
├── 📁 schemas/                    # Definiciones de esquemas
│   ├── bronze_external_tables.sql
│   ├── silver_tables.sql
│   └── gold_tables.sql
│
└── 📁 documentation/              # Documentación técnica
    ├── data_lineage.md
    ├── incremental_loading.md
    └── scheduling_guide.md
```

## 🔄 Patrones de Carga Identificados

### **Estado Actual - Análisis**
- **Silver**: Una sola carga (dt_captura = 2025-08-12) con 110K registros
- **Gold**: Datos desde 2012-07 hasta 2025-06 con 4,965 registros
- **Patrón**: Parece ser carga completa (full load), no incremental

### **Comportamiento por Capa**

#### **Bronze Layer (DataProc)**
- ✅ **Incremental**: Detecta último dt= automáticamente
- ✅ **Eficiente**: Solo procesa nuevos archivos CSV→Parquet

#### **Silver Layer (BigQuery)**
- ❓ **A verificar**: ¿Reprocesa todo bronze o solo nuevos dt_captura?
- 📊 **Evidencia**: Solo un dt_captura sugiere carga completa

#### **Gold Layer (BigQuery)**
- ❓ **A verificar**: ¿Recalcula todas las métricas o solo períodos nuevos?
- 📊 **Distribución**: 32 registros consistentes por mes (agregación por provincia)

## ⚡ Recomendaciones de Optimización

### **1. Implementar Carga Incremental Real**
```sql
-- Ejemplo: Solo procesar nuevos dt_captura
WHERE dt_captura > (SELECT MAX(dt_captura) FROM silver_clean.simbad_hipotecarios)
```

### **2. Particionado Inteligente**
- **Silver**: Particionar por dt_captura (carga)
- **Gold**: Particionar por periodo_date (período de datos)

### **3. Stored Procedures Inteligentes**
- `sp_incremental_silver_refresh()` - Solo nuevos datos
- `sp_full_silver_rebuild()` - Reconstrucción completa
- `sp_gold_update_period(start_date, end_date)` - Actualización selectiva

## 🎯 Próximos Pasos

1. **Extraer queries actuales** de BigQuery scheduled queries/jobs
2. **Crear stored procedures** con lógica incremental
3. **Implementar checkpoints** para detectar último procesamiento
4. **Configurar scheduling** optimizado en Cloud Scheduler
5. **Documentar lineage** y dependencias entre capas

## 📈 Métricas Clave

### **Volúmenes Actuales**
- **Bronze SIMBAD**: ~676K filas (histórico completo)
- **Silver SIMBAD**: 110K filas (limpio)
- **Gold SIMBAD**: 4.9K filas (agregado por provincia/período)

### **Frecuencias Objetivo**
- **Bronze**: Diario (incremental por DataProc)
- **Silver**: Diario (incremental por BigQuery)
- **Gold**: Diario (solo períodos nuevos/actualizados)