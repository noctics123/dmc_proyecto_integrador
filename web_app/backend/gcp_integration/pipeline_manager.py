"""
GCP Pipeline Manager
Handles DataProc, Cloud Build, BigQuery, and other GCP services
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import json

from google.cloud import dataproc_v1
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import build_v1
from google.cloud import logging as cloud_logging
from google.auth import default

from ..models.api_models import (
    PipelineStatus, PipelineState, ComponentType, 
    DataProcConfig, DeploymentState
)

logger = logging.getLogger(__name__)

class PipelineManager:
    """Manages GCP pipeline components and deployments"""
    
    def __init__(self, project_id: str = "proyecto-integrador-dae-2025"):
        self.project_id = project_id
        self.region = "us-central1"
        self.cluster_name = "cluster-integrador-2025"
        
        # Initialize clients
        self.dataproc_client = None
        self.bigquery_client = None
        self.storage_client = None
        self.build_client = None
        self.logging_client = None
        
    async def initialize(self):
        """Initialize GCP clients"""
        try:
            # Get default credentials
            credentials, project = default()
            
            # Initialize clients
            self.dataproc_client = dataproc_v1.ClusterControllerClient()
            self.bigquery_client = bigquery.Client(project=self.project_id, credentials=credentials)
            self.storage_client = storage.Client(project=self.project_id, credentials=credentials)
            self.build_client = build_v1.CloudBuildClient()
            self.logging_client = cloud_logging.Client(project=self.project_id, credentials=credentials)
            
            logger.info("GCP clients initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize GCP clients: {e}")
            raise
    
    async def cleanup(self):
        """Clean up resources"""
        # Close any connections if needed
        pass
    
    async def get_all_status(self) -> Dict[str, PipelineStatus]:
        """Get status of all pipeline components"""
        try:
            components = {}
            
            # Get DataProc status
            components["dataproc"] = await self.get_dataproc_component_status()
            
            # Get BigQuery datasets status
            for dataset in ["bronze", "silver_clean", "gold"]:
                components[f"bigquery_{dataset}"] = await self.get_bigquery_dataset_status(dataset)
            
            # Get landing scrapers status
            components["landing_macroeconomics"] = await self.get_cloud_run_status("landing-macroeconomics")
            components["landing_simbad"] = await self.get_cloud_run_status("landing-simbad")
            
            return components
        except Exception as e:
            logger.error(f"Failed to get pipeline status: {e}")
            raise
    
    async def get_component_status(self, component: str) -> PipelineStatus:
        """Get status of specific component"""
        if component == "dataproc":
            return await self.get_dataproc_component_status()
        elif component.startswith("bigquery_"):
            dataset = component.replace("bigquery_", "")
            return await self.get_bigquery_dataset_status(dataset)
        elif component.startswith("landing_"):
            service = component.replace("landing_", "")
            return await self.get_cloud_run_status(service)
        else:
            raise ValueError(f"Unknown component: {component}")
    
    async def get_dataproc_component_status(self) -> PipelineStatus:
        """Get DataProc cluster status"""
        try:
            cluster_path = self.dataproc_client.cluster_path(
                self.project_id, self.region, self.cluster_name
            )
            
            cluster = self.dataproc_client.get_cluster(
                request={"cluster_name": cluster_path}
            )
            
            # Map DataProc states to our pipeline states
            state_mapping = {
                dataproc_v1.ClusterStatus.State.RUNNING: PipelineState.HEALTHY,
                dataproc_v1.ClusterStatus.State.CREATING: PipelineState.RUNNING,
                dataproc_v1.ClusterStatus.State.ERROR: PipelineState.FAILED,
                dataproc_v1.ClusterStatus.State.DELETING: PipelineState.STOPPED,
                dataproc_v1.ClusterStatus.State.STOPPED: PipelineState.STOPPED,
            }
            
            state = state_mapping.get(cluster.status.state, PipelineState.UNKNOWN)
            
            # Get cluster metrics
            metrics = {
                "worker_count": cluster.config.worker_config.num_instances,
                "master_machine_type": cluster.config.master_config.machine_type_uri.split("/")[-1],
                "worker_machine_type": cluster.config.worker_config.machine_type_uri.split("/")[-1],
                "cluster_uuid": cluster.cluster_uuid,
            }
            
            return PipelineStatus(
                component="dataproc",
                component_type=ComponentType.DATAPROC,
                state=state,
                last_run=datetime.fromtimestamp(cluster.status.state_start_time.timestamp()) if cluster.status.state_start_time else None,
                metrics=metrics
            )
            
        except Exception as e:
            logger.error(f"Failed to get DataProc status: {e}")
            return PipelineStatus(
                component="dataproc",
                component_type=ComponentType.DATAPROC,
                state=PipelineState.UNKNOWN,
                error_message=str(e)
            )
    
    async def get_bigquery_dataset_status(self, dataset_name: str) -> PipelineStatus:
        """Get BigQuery dataset status"""
        try:
            dataset_id = f"{self.project_id}.{dataset_name}"
            dataset = self.bigquery_client.get_dataset(dataset_id)
            
            # Get table count and last modified
            tables = list(self.bigquery_client.list_tables(dataset))
            table_count = len(tables)
            
            last_modified = None
            if tables:
                # Get the most recently modified table
                latest_table = max(tables, key=lambda t: t.modified or datetime.min)
                last_modified = latest_table.modified
            
            metrics = {
                "table_count": table_count,
                "dataset_size_mb": dataset.location,
                "creation_time": dataset.created.isoformat() if dataset.created else None,
            }
            
            return PipelineStatus(
                component=f"bigquery_{dataset_name}",
                component_type=ComponentType.SILVER if "silver" in dataset_name else 
                             ComponentType.GOLD if "gold" in dataset_name else ComponentType.BRONZE,
                state=PipelineState.HEALTHY,
                last_run=last_modified,
                metrics=metrics
            )
            
        except Exception as e:
            logger.error(f"Failed to get BigQuery dataset status for {dataset_name}: {e}")
            return PipelineStatus(
                component=f"bigquery_{dataset_name}",
                component_type=ComponentType.SILVER if "silver" in dataset_name else 
                             ComponentType.GOLD if "gold" in dataset_name else ComponentType.BRONZE,
                state=PipelineState.UNKNOWN,
                error_message=str(e)
            )
    
    async def get_cloud_run_status(self, service_name: str) -> PipelineStatus:
        """Get Cloud Run service status (simplified)"""
        # This would need Cloud Run client implementation
        # For now, return a placeholder
        return PipelineStatus(
            component=f"landing_{service_name}",
            component_type=ComponentType.LANDING,
            state=PipelineState.HEALTHY,
            metrics={"service_name": service_name}
        )
    
    async def trigger_deployment(self, component: str, branch: str = "main", config: Optional[Dict] = None) -> str:
        """Trigger deployment of specific component"""
        try:
            # Map component to trigger name
            trigger_mapping = {
                "etl_pipeline": "etl-pipeline-main",
                "web_app": "webapp-deployment",
                "landing_macroeconomics": "landing-run-trigger",
                "landing_simbad": "landing-simbad-trigger",
            }
            
            trigger_name = trigger_mapping.get(component)
            if not trigger_name:
                raise ValueError(f"Unknown component: {component}")
            
            # Create build request
            source = build_v1.RepoSource(
                project_id=self.project_id,
                repo_name="dmc_proyecto_integrador",  # Update with actual repo name
                branch_name=branch
            )
            
            build_request = build_v1.Build(source=source)
            
            # Trigger build
            operation = self.build_client.create_build(
                project_id=self.project_id,
                build=build_request
            )
            
            build_id = operation.metadata.build.id
            logger.info(f"Triggered deployment for {component}, build ID: {build_id}")
            
            return build_id
            
        except Exception as e:
            logger.error(f"Failed to trigger deployment for {component}: {e}")
            raise
    
    async def start_dataproc_cluster(self) -> Dict[str, str]:
        """Start DataProc cluster"""
        try:
            cluster_path = self.dataproc_client.cluster_path(
                self.project_id, self.region, self.cluster_name
            )
            
            operation = self.dataproc_client.start_cluster(
                request={"cluster_name": cluster_path}
            )
            
            logger.info(f"Started DataProc cluster: {self.cluster_name}")
            return {"cluster_name": self.cluster_name, "operation_name": operation.name}
            
        except Exception as e:
            logger.error(f"Failed to start DataProc cluster: {e}")
            raise
    
    async def stop_dataproc_cluster(self) -> Dict[str, str]:
        """Stop DataProc cluster"""
        try:
            cluster_path = self.dataproc_client.cluster_path(
                self.project_id, self.region, self.cluster_name
            )
            
            operation = self.dataproc_client.stop_cluster(
                request={"cluster_name": cluster_path}
            )
            
            logger.info(f"Stopped DataProc cluster: {self.cluster_name}")
            return {"cluster_name": self.cluster_name, "operation_name": operation.name}
            
        except Exception as e:
            logger.error(f"Failed to stop DataProc cluster: {e}")
            raise
    
    async def get_dataproc_status(self) -> Dict[str, Any]:
        """Get detailed DataProc cluster status"""
        status = await self.get_dataproc_component_status()
        return {
            "cluster_name": self.cluster_name,
            "state": status.state,
            "metrics": status.metrics,
            "last_run": status.last_run.isoformat() if status.last_run else None,
            "error_message": status.error_message
        }
    
    async def update_config(self, component: str, config: Dict[str, Any]):
        """Update component configuration"""
        # This would implement configuration updates
        # For now, just log
        logger.info(f"Updating config for {component}: {config}")
        
    async def get_config(self, component: str) -> Dict[str, Any]:
        """Get current component configuration"""
        # This would return actual configuration
        # For now, return placeholder
        return {"component": component, "config": {}}
    
    async def monitor_deployment(self, job_id: str):
        """Monitor deployment progress in background"""
        try:
            # This would implement deployment monitoring
            logger.info(f"Monitoring deployment {job_id}")
            
            # Simulate monitoring
            await asyncio.sleep(5)
            logger.info(f"Deployment {job_id} completed")
            
        except Exception as e:
            logger.error(f"Failed to monitor deployment {job_id}: {e}")