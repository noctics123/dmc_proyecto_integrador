"""
Simplified FastAPI Backend for DMC Pipeline Management
Works without GCP dependencies for local demo
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import asyncio
from typing import Dict, Any

app = FastAPI(
    title="DMC Data Pipeline Management API (Demo)",
    description="Simplified API for local demo without GCP dependencies",
    version="1.0.0-demo"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mock data
mock_pipeline_status = {
    "dataproc": {
        "component": "dataproc",
        "component_type": "dataproc",
        "state": "stopped",
        "last_run": "2025-01-09T09:15:00Z",
        "metrics": {
            "worker_count": 4,
            "master_machine_type": "n2-standard-2",
            "cluster_uuid": "ed10ff14-bd3b-46f2-a14d-a9c04ae72a9b"
        }
    },
    "bigquery_bronze": {
        "component": "bigquery_bronze",
        "component_type": "bronze",
        "state": "healthy",
        "last_run": "2025-01-10T08:35:00Z",
        "metrics": {
            "table_count": 4,
            "dataset_size_mb": 1250
        }
    },
    "bigquery_silver": {
        "component": "bigquery_silver_clean",
        "component_type": "silver",
        "state": "running",
        "last_run": "2025-01-10T08:48:00Z",
        "metrics": {
            "table_count": 1,
            "dataset_size_mb": 890
        }
    },
    "landing_simbad": {
        "component": "landing_simbad",
        "component_type": "landing",
        "state": "healthy",
        "last_run": "2025-01-10T08:30:00Z",
        "metrics": {
            "records_processed": 15234
        }
    },
    "landing_macroeconomics": {
        "component": "landing_macroeconomics",
        "component_type": "landing",
        "state": "healthy",
        "last_run": "2025-01-10T08:28:00Z",
        "metrics": {
            "records_processed": 3456
        }
    }
}

mock_data_quality = {
    "datasets": {
        "bronze.simbad_bronze_parquet_ext": {
            "freshness_hours": 2.5,
            "record_count": 15234,
            "completeness_pct": 98.5,
            "accuracy_score": 96.2,
            "schema_violations": 0
        },
        "silver_clean.simbad_hipotecarios": {
            "freshness_hours": 3.2,
            "record_count": 14892,
            "completeness_pct": 99.1,
            "accuracy_score": 97.8,
            "schema_violations": 0
        },
        "gold.simbad_gold": {
            "freshness_hours": 24.1,
            "record_count": 2156,
            "completeness_pct": 95.2,
            "accuracy_score": 98.5,
            "schema_violations": 0
        }
    },
    "overall_score": 96.8,
    "issues": [
        "gold.simbad_gold: Data is 24.1 hours old",
        "bronze.simbad_bronze_parquet_ext: Completeness below 99%"
    ],
    "last_updated": datetime.utcnow().isoformat()
}

mock_cost_metrics = {
    "total_monthly_cost": 125.50,
    "cost_by_service": {
        "BigQuery": 45.20,
        "Cloud Storage": 12.30,
        "DataProc": 55.00,
        "Cloud Run": 8.50,
        "Cloud Build": 4.50
    },
    "cost_trend": [
        {"date": "2025-01-01", "cost": 120.00},
        {"date": "2025-01-02", "cost": 123.00},
        {"date": "2025-01-03", "cost": 125.50}
    ],
    "budget_utilization_pct": 62.75
}

mock_performance_metrics = {
    "avg_execution_time": {
        "bronze_etl": 12.5,
        "silver_etl": 8.2,
        "gold_etl": 5.1,
        "landing_scrapers": 15.3
    },
    "success_rate": {
        "bronze_etl": 98.5,
        "silver_etl": 99.2,
        "gold_etl": 97.8,
        "landing_scrapers": 95.1
    },
    "throughput": {
        "bronze_etl": 85000,
        "silver_etl": 92000,
        "gold_etl": 15000,
        "landing_scrapers": 5000
    },
    "resource_utilization": {
        "dataproc_cpu": 65.5,
        "dataproc_memory": 72.1,
        "bigquery_slots": 45.8,
        "cloud_run_cpu": 35.2
    }
}

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "DMC Data Pipeline Management API (Demo Mode)",
        "status": "running",
        "mode": "demo",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "mode": "demo"
    }

# Pipeline Management Endpoints
@app.get("/api/pipelines/status")
async def get_pipeline_status():
    """Get status of all pipeline components"""
    # Simulate some delay
    await asyncio.sleep(0.1)
    return mock_pipeline_status

@app.get("/api/pipelines/{component}/status")
async def get_component_status(component: str):
    """Get status of specific pipeline component"""
    await asyncio.sleep(0.1)
    
    if component in mock_pipeline_status:
        return mock_pipeline_status[component]
    else:
        return {
            "component": component,
            "component_type": "unknown",
            "state": "unknown",
            "error_message": f"Component {component} not found"
        }

@app.post("/api/deploy/{component}")
async def trigger_deployment(component: str):
    """Trigger deployment of specific component"""
    await asyncio.sleep(0.2)
    
    job_id = f"demo-job-{int(datetime.utcnow().timestamp())}"
    
    return {
        "job_id": job_id,
        "status": "started",
        "component": component,
        "message": f"Demo deployment triggered for {component}"
    }

@app.get("/api/logs/{job_id}")
async def get_deployment_logs(job_id: str, lines: int = 100):
    """Get deployment logs"""
    await asyncio.sleep(0.1)
    
    mock_logs = [
        f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] Starting deployment...",
        f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] Building container image...",
        f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] Pushing to registry...",
        f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] Deploying to Cloud Run...",
        f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] Deployment completed successfully!",
    ]
    
    return {
        "job_id": job_id,
        "lines": mock_logs[-lines:] if lines < len(mock_logs) else mock_logs,
        "is_complete": True,
        "timestamp": datetime.utcnow().isoformat()
    }

# Monitoring Endpoints
@app.get("/api/monitoring/data-quality")
async def get_data_quality_metrics():
    """Get data quality metrics"""
    await asyncio.sleep(0.1)
    return mock_data_quality

@app.get("/api/monitoring/costs")
async def get_cost_metrics():
    """Get cost metrics"""
    await asyncio.sleep(0.1)
    return mock_cost_metrics

@app.get("/api/monitoring/performance")
async def get_performance_metrics():
    """Get performance metrics"""
    await asyncio.sleep(0.1)
    return mock_performance_metrics

# DataProc Management
@app.post("/api/dataproc/cluster/start")
async def start_dataproc_cluster():
    """Start DataProc cluster"""
    await asyncio.sleep(0.5)  # Simulate longer operation
    
    # Update mock status
    mock_pipeline_status["dataproc"]["state"] = "running"
    mock_pipeline_status["dataproc"]["last_run"] = datetime.utcnow().isoformat()
    
    return {
        "status": "starting",
        "cluster_name": "cluster-integrador-2025",
        "message": "Demo: DataProc cluster start initiated"
    }

@app.post("/api/dataproc/cluster/stop")
async def stop_dataproc_cluster():
    """Stop DataProc cluster"""
    await asyncio.sleep(0.5)
    
    # Update mock status
    mock_pipeline_status["dataproc"]["state"] = "stopped"
    
    return {
        "status": "stopping",
        "cluster_name": "cluster-integrador-2025",
        "message": "Demo: DataProc cluster stop initiated"
    }

@app.get("/api/dataproc/cluster/status")
async def get_dataproc_status():
    """Get DataProc cluster status"""
    await asyncio.sleep(0.1)
    
    return {
        "cluster_name": "cluster-integrador-2025",
        "state": mock_pipeline_status["dataproc"]["state"],
        "metrics": mock_pipeline_status["dataproc"]["metrics"],
        "last_run": mock_pipeline_status["dataproc"]["last_run"],
        "mode": "demo"
    }

if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting DMC Pipeline API in DEMO mode...")
    print("📍 API will be available at: http://localhost:8000")
    print("📚 Documentation at: http://localhost:8000/docs")
    print("🔗 Frontend should connect to: http://localhost:3000")
    
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)