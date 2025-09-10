"""
FastAPI Backend for DMC Data Pipeline Management
Main application entry point
"""

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import os
from typing import Dict, List, Optional

from .gcp_integration.pipeline_manager import PipelineManager
from .gcp_integration.monitoring import MonitoringService
from .models.api_models import (
    PipelineStatus, DeploymentRequest, ConfigUpdate,
    DataQualityMetrics, LogsResponse
)

# Initialize GCP services
pipeline_manager = PipelineManager()
monitoring_service = MonitoringService()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management"""
    # Startup
    await pipeline_manager.initialize()
    await monitoring_service.initialize()
    yield
    # Shutdown
    await pipeline_manager.cleanup()
    await monitoring_service.cleanup()

app = FastAPI(
    title="DMC Data Pipeline Management API",
    description="API for managing data pipelines, deployments, and monitoring",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "DMC Data Pipeline Management API", "status": "running"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": "2025-01-01T00:00:00Z"}

# Pipeline Management Endpoints
@app.get("/api/pipelines/status", response_model=Dict[str, PipelineStatus])
async def get_pipeline_status():
    """Get status of all pipeline components"""
    try:
        return await pipeline_manager.get_all_status()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pipelines/{component}/status", response_model=PipelineStatus)
async def get_component_status(component: str):
    """Get status of specific pipeline component"""
    try:
        return await pipeline_manager.get_component_status(component)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Component {component} not found")

@app.post("/api/deploy/{component}")
async def trigger_deployment(
    component: str, 
    request: DeploymentRequest,
    background_tasks: BackgroundTasks
):
    """Trigger deployment of specific component"""
    try:
        job_id = await pipeline_manager.trigger_deployment(
            component, request.branch, request.config
        )
        background_tasks.add_task(
            pipeline_manager.monitor_deployment, job_id
        )
        return {"job_id": job_id, "status": "started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/logs/{job_id}", response_model=LogsResponse)
async def get_deployment_logs(job_id: str, lines: Optional[int] = 100):
    """Get real-time deployment logs"""
    try:
        return await monitoring_service.get_deployment_logs(job_id, lines)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

# Configuration Management
@app.put("/api/config/{component}")
async def update_component_config(component: str, config: ConfigUpdate):
    """Update component configuration"""
    try:
        await pipeline_manager.update_config(component, config.config)
        return {"status": "updated", "component": component}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/config/{component}")
async def get_component_config(component: str):
    """Get current component configuration"""
    try:
        return await pipeline_manager.get_config(component)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

# Monitoring and Data Quality
@app.get("/api/monitoring/data-quality", response_model=DataQualityMetrics)
async def get_data_quality_metrics():
    """Get data quality metrics across all datasets"""
    try:
        return await monitoring_service.get_data_quality_metrics()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/monitoring/costs")
async def get_cost_metrics():
    """Get GCP cost metrics for data pipeline"""
    try:
        return await monitoring_service.get_cost_metrics()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/monitoring/performance")
async def get_performance_metrics():
    """Get pipeline performance metrics"""
    try:
        return await monitoring_service.get_performance_metrics()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# DataProc Management
@app.post("/api/dataproc/cluster/start")
async def start_dataproc_cluster():
    """Start DataProc cluster for historical processing"""
    try:
        result = await pipeline_manager.start_dataproc_cluster()
        return {"status": "starting", "cluster_name": result["cluster_name"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/dataproc/cluster/stop")
async def stop_dataproc_cluster():
    """Stop DataProc cluster to save costs"""
    try:
        result = await pipeline_manager.stop_dataproc_cluster()
        return {"status": "stopping", "cluster_name": result["cluster_name"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dataproc/cluster/status")
async def get_dataproc_status():
    """Get current DataProc cluster status"""
    try:
        return await pipeline_manager.get_dataproc_status()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)