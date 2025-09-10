"""
Monitoring Service for GCP Pipeline
Handles data quality, performance metrics, and cost tracking
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import asyncio

from google.cloud import bigquery
from google.cloud import monitoring_v3
from google.cloud import logging as cloud_logging
from google.auth import default

from ..models.api_models import DataQualityMetrics, DatasetMetrics

logger = logging.getLogger(__name__)

class MonitoringService:
    """Monitors data quality, costs, and performance"""
    
    def __init__(self, project_id: str = "proyecto-integrador-dae-2025"):
        self.project_id = project_id
        self.bigquery_client = None
        self.monitoring_client = None
        self.logging_client = None
        
    async def initialize(self):
        """Initialize monitoring clients"""
        try:
            credentials, project = default()
            
            self.bigquery_client = bigquery.Client(project=self.project_id, credentials=credentials)
            self.monitoring_client = monitoring_v3.MetricServiceClient()
            self.logging_client = cloud_logging.Client(project=self.project_id, credentials=credentials)
            
            logger.info("Monitoring clients initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize monitoring clients: {e}")
            raise
    
    async def cleanup(self):
        """Clean up monitoring resources"""
        pass
    
    async def get_data_quality_metrics(self) -> DataQualityMetrics:
        """Get comprehensive data quality metrics"""
        try:
            datasets = {}
            issues = []
            
            # Check each dataset
            dataset_configs = [
                ("bronze", "simbad_bronze_parquet_ext"),
                ("silver_clean", "simbad_hipotecarios"),
                ("gold", "simbad_gold"),
            ]
            
            for dataset_name, table_name in dataset_configs:
                try:
                    metrics = await self._get_table_quality_metrics(dataset_name, table_name)
                    datasets[f"{dataset_name}.{table_name}"] = metrics
                    
                    # Check for issues
                    if metrics["freshness_hours"] > 48:  # Data older than 2 days
                        issues.append(f"{dataset_name}.{table_name}: Data is {metrics['freshness_hours']:.1f} hours old")
                    
                    if metrics["completeness_pct"] < 95:  # Less than 95% complete
                        issues.append(f"{dataset_name}.{table_name}: Only {metrics['completeness_pct']:.1f}% complete")
                        
                except Exception as e:
                    logger.error(f"Failed to get metrics for {dataset_name}.{table_name}: {e}")
                    issues.append(f"{dataset_name}.{table_name}: Failed to retrieve metrics - {str(e)}")
            
            # Calculate overall score
            if datasets:
                completeness_scores = [d.get("completeness_pct", 0) for d in datasets.values()]
                freshness_scores = [min(100, max(0, 100 - d.get("freshness_hours", 0) / 24 * 10)) for d in datasets.values()]
                overall_score = (sum(completeness_scores) + sum(freshness_scores)) / (2 * len(datasets))
            else:
                overall_score = 0
            
            return DataQualityMetrics(
                datasets=datasets,
                overall_score=overall_score,
                issues=issues,
                last_updated=datetime.utcnow()
            )
            
        except Exception as e:
            logger.error(f"Failed to get data quality metrics: {e}")
            raise
    
    async def _get_table_quality_metrics(self, dataset_name: str, table_name: str) -> Dict[str, Any]:
        """Get quality metrics for a specific table"""
        try:
            table_ref = f"{self.project_id}.{dataset_name}.{table_name}"
            
            # Get table metadata
            table = self.bigquery_client.get_table(table_ref)
            
            # Calculate freshness (hours since last modification)
            if table.modified:
                freshness_hours = (datetime.utcnow() - table.modified.replace(tzinfo=None)).total_seconds() / 3600
            else:
                freshness_hours = float('inf')
            
            # Get record count
            count_query = f"SELECT COUNT(*) as record_count FROM `{table_ref}`"
            count_result = list(self.bigquery_client.query(count_query))
            record_count = count_result[0].record_count if count_result else 0
            
            # Calculate completeness (non-null values in key fields)
            if dataset_name == "silver_clean" and table_name == "simbad_hipotecarios":
                completeness_query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(periodo) as periodo_non_null,
                    COUNT(entidad) as entidad_non_null,
                    COUNT(deudaCapital) as deuda_capital_non_null
                FROM `{table_ref}`
                """
                completeness_result = list(self.bigquery_client.query(completeness_query))
                if completeness_result:
                    row = completeness_result[0]
                    if row.total_records > 0:
                        completeness_pct = min(
                            row.periodo_non_null / row.total_records * 100,
                            row.entidad_non_null / row.total_records * 100,
                            row.deuda_capital_non_null / row.total_records * 100
                        )
                    else:
                        completeness_pct = 0
                else:
                    completeness_pct = 0
            else:
                # Simple completeness check for other tables
                completeness_pct = 100 if record_count > 0 else 0
            
            # Accuracy score (simplified - could be enhanced with business rules)
            accuracy_score = 95.0  # Placeholder
            
            # Schema violations (simplified)
            schema_violations = 0  # Placeholder
            
            return {
                "freshness_hours": freshness_hours,
                "record_count": record_count,
                "completeness_pct": completeness_pct,
                "accuracy_score": accuracy_score,
                "schema_violations": schema_violations,
                "table_size_mb": table.num_bytes / (1024 * 1024) if table.num_bytes else 0,
                "last_modified": table.modified.isoformat() if table.modified else None,
            }
            
        except Exception as e:
            logger.error(f"Failed to get table metrics for {dataset_name}.{table_name}: {e}")
            return {
                "freshness_hours": float('inf'),
                "record_count": 0,
                "completeness_pct": 0,
                "accuracy_score": 0,
                "schema_violations": 1,
                "error": str(e)
            }
    
    async def get_cost_metrics(self) -> Dict[str, Any]:
        """Get GCP cost metrics for the pipeline"""
        try:
            # This would require Billing API integration
            # For now, return mock data structure
            return {
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
                "budget_utilization_pct": 62.75,
                "projected_monthly_cost": 155.20
            }
        except Exception as e:
            logger.error(f"Failed to get cost metrics: {e}")
            raise
    
    async def get_performance_metrics(self) -> Dict[str, Any]:
        """Get pipeline performance metrics"""
        try:
            # This would query Cloud Monitoring for actual metrics
            # For now, return mock data
            return {
                "avg_execution_time": {
                    "bronze_etl": 12.5,  # minutes
                    "silver_etl": 8.2,
                    "gold_etl": 5.1,
                    "landing_scrapers": 15.3
                },
                "success_rate": {
                    "bronze_etl": 98.5,  # percentage
                    "silver_etl": 99.2,
                    "gold_etl": 97.8,
                    "landing_scrapers": 95.1
                },
                "throughput": {
                    "bronze_etl": 85000,  # records/hour
                    "silver_etl": 92000,
                    "gold_etl": 15000,
                    "landing_scrapers": 5000
                },
                "resource_utilization": {
                    "dataproc_cpu": 65.5,  # percentage
                    "dataproc_memory": 72.1,
                    "bigquery_slots": 45.8,
                    "cloud_run_cpu": 35.2
                }
            }
        except Exception as e:
            logger.error(f"Failed to get performance metrics: {e}")
            raise
    
    async def get_deployment_logs(self, job_id: str, lines: int = 100) -> Dict[str, Any]:
        """Get deployment logs for a specific job"""
        try:
            # Query Cloud Logging for build logs
            filter_str = f'resource.type="build" AND jsonPayload.buildId="{job_id}"'
            
            entries = self.logging_client.list_entries(
                filter_=filter_str,
                order_by=cloud_logging.DESCENDING,
                max_results=lines
            )
            
            log_lines = []
            is_complete = False
            
            for entry in entries:
                timestamp = entry.timestamp.strftime("%Y-%m-%d %H:%M:%S")
                message = str(entry.payload)
                log_lines.append(f"[{timestamp}] {message}")
                
                # Check if build is complete
                if "BUILD_SUCCESS" in message or "BUILD_FAILURE" in message:
                    is_complete = True
            
            # Reverse to show chronological order
            log_lines.reverse()
            
            return {
                "job_id": job_id,
                "lines": log_lines,
                "is_complete": is_complete,
                "timestamp": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Failed to get deployment logs for {job_id}: {e}")
            return {
                "job_id": job_id,
                "lines": [f"Error retrieving logs: {str(e)}"],
                "is_complete": True,
                "timestamp": datetime.utcnow()
            }
    
    async def create_alert_rule(self, name: str, condition: str, threshold: float, 
                              notification_channels: List[str]) -> bool:
        """Create monitoring alert rule"""
        try:
            # This would create actual Cloud Monitoring alert policies
            logger.info(f"Creating alert rule: {name} with threshold {threshold}")
            return True
        except Exception as e:
            logger.error(f"Failed to create alert rule {name}: {e}")
            return False
    
    async def get_pipeline_health_score(self) -> float:
        """Calculate overall pipeline health score"""
        try:
            quality_metrics = await self.get_data_quality_metrics()
            performance_metrics = await self.get_performance_metrics()
            
            # Weight different aspects
            quality_weight = 0.4
            performance_weight = 0.3
            availability_weight = 0.3
            
            quality_score = quality_metrics.overall_score
            
            # Calculate performance score from success rates
            success_rates = list(performance_metrics["success_rate"].values())
            performance_score = sum(success_rates) / len(success_rates) if success_rates else 0
            
            # Simple availability score (would need actual uptime data)
            availability_score = 98.5  # Placeholder
            
            overall_score = (
                quality_score * quality_weight +
                performance_score * performance_weight +
                availability_score * availability_weight
            )
            
            return min(100, max(0, overall_score))
            
        except Exception as e:
            logger.error(f"Failed to calculate pipeline health score: {e}")
            return 0