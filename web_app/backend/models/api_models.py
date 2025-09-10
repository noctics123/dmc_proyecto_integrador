"""
Pydantic models for API requests and responses
"""

from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
from datetime import datetime
from enum import Enum

class PipelineState(str, Enum):
    """Pipeline component states"""
    HEALTHY = "healthy"
    RUNNING = "running" 
    FAILED = "failed"
    STOPPED = "stopped"
    UNKNOWN = "unknown"

class DeploymentState(str, Enum):
    """Deployment states"""
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"

class ComponentType(str, Enum):
    """Pipeline component types"""
    LANDING = "landing"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    DATAPROC = "dataproc"
    SCHEDULER = "scheduler"

class PipelineStatus(BaseModel):
    """Status of a pipeline component"""
    component: str
    component_type: ComponentType
    state: PipelineState
    last_run: Optional[datetime] = None
    next_scheduled: Optional[datetime] = None
    last_success: Optional[datetime] = None
    error_message: Optional[str] = None
    metrics: Dict[str, Any] = Field(default_factory=dict)

class DeploymentRequest(BaseModel):
    """Request to trigger a deployment"""
    branch: str = "main"
    config: Optional[Dict[str, Any]] = None
    force: bool = False

class ConfigUpdate(BaseModel):
    """Configuration update request"""
    config: Dict[str, Any]
    validate: bool = True

class LogsResponse(BaseModel):
    """Deployment logs response"""
    job_id: str
    lines: List[str]
    is_complete: bool
    timestamp: datetime

class DataQualityMetrics(BaseModel):
    """Data quality metrics across datasets"""
    datasets: Dict[str, Dict[str, Any]]
    overall_score: float
    issues: List[str]
    last_updated: datetime

class DatasetMetrics(BaseModel):
    """Metrics for a specific dataset"""
    name: str
    freshness_hours: float
    record_count: int
    expected_count: Optional[int] = None
    completeness_pct: float
    accuracy_score: float
    schema_violations: int

class CostMetrics(BaseModel):
    """GCP cost metrics"""
    total_monthly_cost: float
    cost_by_service: Dict[str, float]
    cost_trend: List[Dict[str, Any]]
    budget_utilization_pct: float

class PerformanceMetrics(BaseModel):
    """Pipeline performance metrics"""
    avg_execution_time: Dict[str, float]  # by component
    success_rate: Dict[str, float]        # by component  
    throughput: Dict[str, float]          # records/hour by component
    resource_utilization: Dict[str, float]

class SchedulerConfig(BaseModel):
    """Cloud Scheduler configuration"""
    name: str
    schedule: str  # cron expression
    timezone: str
    description: Optional[str] = None
    enabled: bool = True
    target_config: Dict[str, Any]

class DataProcConfig(BaseModel):
    """DataProc cluster configuration"""
    cluster_name: str
    zone: str
    master_machine_type: str
    worker_machine_type: str
    num_workers: int
    disk_size_gb: int
    auto_scaling: bool = False
    max_workers: Optional[int] = None

class SecretConfig(BaseModel):
    """Secret Manager configuration"""
    secret_name: str
    secret_value: Optional[str] = None  # Don't expose in responses
    description: Optional[str] = None

class AlertRule(BaseModel):
    """Monitoring alert rule"""
    name: str
    condition: str
    threshold: float
    notification_channels: List[str]
    enabled: bool = True

class DeploymentHistory(BaseModel):
    """Deployment history entry"""
    job_id: str
    component: str
    branch: str
    state: DeploymentState
    started_at: datetime
    completed_at: Optional[datetime] = None
    triggered_by: str
    commit_sha: Optional[str] = None
    error_message: Optional[str] = None