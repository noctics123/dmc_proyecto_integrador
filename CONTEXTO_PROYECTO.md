# DMC Proyecto Integrador - Contexto del Proyecto

## 📋 Resumen Ejecutivo

Este es un proyecto de pipeline de datos (ETL) completo implementado en GCP con una web app como portal de gestión. La arquitectura incluye múltiples servicios de Google Cloud para procesar datos desde landing hasta capas gold (bronze -> silver -> gold).

## 🏗️ Arquitectura del Sistema

### Servicios GCP Utilizados
- **Cloud Storage**: Almacenamiento de datos raw y procesados
- **DataProc**: Clusters de Spark para procesamiento de datos históricos
- **Cloud Build**: CI/CD para despliegues automatizados
- **Cloud Run**: Hosting del backend API
- **BigQuery**: Data warehouse (capas bronze, silver, gold)
- **Notebooks**: Entornos de análisis de datos
- **PySpark**: Motor de procesamiento distribuido

### Pipeline de Datos
1. **Landing**: Scrapers que recolectan datos de SIMBAD y indicadores macroeconómicos
2. **Bronze**: Ingesta de datos raw en formato Parquet
3. **Silver**: Limpieza, deduplicación y normalización de datos
4. **Gold**: Agregaciones y métricas de negocio listas para BI

## 🌐 Web App - Portal de Gestión

### Stack Tecnológico
- **Frontend**: React 18 + TypeScript + Ant Design
- **Backend**: FastAPI (Python)
- **Estado**: TanStack React Query para gestión de estado async
- **Routing**: React Router DOM

### Estructura del Proyecto
```
web_app/
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   ├── pages/
│   │   ├── hooks/
│   │   └── services/
│   └── package.json
└── backend/
    ├── main.py
    ├── gcp_integration/
    ├── models/
    └── requirements.txt
```

## ✅ Problemas RESUELTOS (Enero 2025)

### 1. ✅ Pestañas Implementadas
- **Deployments** (`/deployments`): ✅ Implementada con agrupación por capas
  - Vista organizada por Landing → Bronze → Silver → Gold
  - Servicios GCP detallados por capa con estados reales
  - Interfaz para despliegues con logs integrados
  
- **Monitoring** (`/monitoring`): ✅ Implementada con logs y seguimiento
  - Historial de ejecuciones con filtros
  - Logs detallados por componente
  - Seguimiento de problemas y errores
  - Vista de stored procedures por capa

### 2. ✅ Progreso Dinámico Corregido
- Eliminado progreso hardcodeado en `PipelineStatus.tsx`
- Implementado cálculo dinámico basado en tiempo y tipo de componente
- Integración con APIs para progreso real (`useJobProgress` hook)
- Actualizaciones cada 5 segundos en tiempo real

### 3. ✅ Sistema de Logs y Monitoreo
- Logs en tiempo real con colores y niveles (INFO, WARNING, ERROR, SUCCESS)
- Modal de logs detallados con información de ejecución
- API endpoints para obtener logs: `/api/logs/{jobId}`
- Hooks personalizados para polling automático

### 4. ✅ Arquitectura por Capas Documentada
- **Landing**: Cloud Build, Cloud Run, Cloud Storage, Cloud Scheduler
- **Bronze**: DataProc, BigQuery External Tables, PySpark Scripts
- **Silver**: BigQuery Datasets, Stored Procedures (`sp_silver_merge`)
- **Gold**: Métricas de negocio, Stored Procedures (`sp_gold_indicators`)

## 📊 Stored Procedures Identificados

### Nomenclatura Implementada: `sp_[capa]_[proceso]`

#### Bronze Layer
- `sp_bronze_simbad`: Landing → Bronze conversion para datos SIMBAD
- `sp_bronze_macro`: ETL para indicadores macroeconómicos

#### Silver Layer  
- `sp_silver_merge`: MERGE incremental con deduplicación
- `sp_silver_quality`: Validaciones de calidad de datos

#### Gold Layer
- `sp_gold_indicators`: Cálculo de métricas de riesgo (PD, tasa mora, cobertura)
- `sp_gold_external`: Integración con datos externos (inflación, tipo cambio)

## 🎯 Funcionalidades Implementadas

### ✅ Interfaz Amigable para Personal No Técnico
- Descripciones claras en lenguaje de negocio
- Iconos y colores intuitivos por tipo de servicio
- Estados visuales fáciles de interpretar
- Tooltips explicativos para detalles técnicos
- Métricas resumidas en cards de fácil lectura

### ✅ Sistema de Seguimiento
- Historial completo de ejecuciones con filtros
- Logs detallados paso a paso con timestamps
- Identificación clara de errores con soluciones sugeridas
- Progreso en tiempo real con descripciones de etapa actual

## 🔧 Tecnologías y Dependencias

### Frontend
- React Query: Manejo de estado async y caché
- Ant Design: Componentes UI
- Recharts: Gráficos y visualizaciones
- Axios: Cliente HTTP

### Backend
- FastAPI: Framework web async
- GCP Client Libraries: Integración con servicios
- Uvicorn: Servidor ASGI

## 💡 Notas de Implementación

- El proyecto usa tanto datos mock como reales dependiendo del modo
- Existe configuración para DEMO_MODE en variables de entorno
- La aplicación está preparada para polling automático cada 15-30 segundos
- Hay manejo básico de errores pero se puede mejorar

## 🚀 Estado Actual

El proyecto tiene la estructura base sólida pero necesita completar la implementación de las funcionalidades core de monitoreo y despliegue para ser totalmente funcional como portal de gestión.

---
*Archivo generado automáticamente para mantener contexto del proyecto*