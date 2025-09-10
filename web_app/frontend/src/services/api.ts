// API service for DMC Pipeline Management
import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';
const DEMO_MODE = process.env.REACT_APP_DEMO_MODE === 'true';

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 10000,
});

// Dynamic mock data generator
const generateDynamicMockData = () => {
  const now = new Date();
  const randomVariation = () => Math.floor(Math.random() * 20) - 10; // -10 to +10 variation
  
  return {
    pipelineStatus: {
      dataproc: {
        component: 'dataproc',
        component_type: 'dataproc',
        state: Math.random() > 0.7 ? 'running' : 'stopped',
        last_run: new Date(now.getTime() - Math.random() * 3600000).toISOString(), // Random within last hour
        metrics: {
          worker_count: 4,
          master_machine_type: 'n2-standard-2',
          cluster_uuid: 'ed10ff14-bd3b-46f2-a14d-a9c04ae72a9b',
          progress_percentage: Math.random() > 0.5 ? Math.floor(Math.random() * 100) : undefined
        }
      },
      bigquery_bronze: {
        component: 'bigquery_bronze',
        component_type: 'bronze',
        state: Math.random() > 0.8 ? 'running' : 'healthy',
        last_run: new Date(now.getTime() - Math.random() * 7200000).toISOString(),
        metrics: {
          table_count: 4,
          dataset_size_mb: 1250 + randomVariation() * 5,
          progress_percentage: Math.random() > 0.7 ? Math.floor(Math.random() * 100) : undefined
        }
      },
      bigquery_silver: {
        component: 'bigquery_silver_clean',
        component_type: 'silver',
        state: Math.random() > 0.6 ? 'running' : 'healthy',
        last_run: new Date(now.getTime() - Math.random() * 1800000).toISOString(),
        metrics: {
          table_count: 1,
          dataset_size_mb: 890 + randomVariation() * 3,
          progress_percentage: Math.random() > 0.6 ? Math.floor(Math.random() * 100) : undefined
        }
      },
      landing_simbad: {
        component: 'landing_simbad',
        component_type: 'landing',
        state: Math.random() > 0.9 ? 'running' : 'healthy',
        last_run: new Date(now.getTime() - Math.random() * 3600000).toISOString(),
        metrics: {
          records_processed: 15234 + randomVariation() * 100,
          progress_percentage: Math.random() > 0.8 ? Math.floor(Math.random() * 100) : undefined
        }
      },
      landing_macroeconomics: {
        component: 'landing_macroeconomics',
        component_type: 'landing',
        state: Math.random() > 0.9 ? 'running' : 'healthy',
        last_run: new Date(now.getTime() - Math.random() * 3600000).toISOString(),
        metrics: {
          records_processed: 3456 + randomVariation() * 20,
          progress_percentage: Math.random() > 0.8 ? Math.floor(Math.random() * 100) : undefined
        }
      }
  },
  dataQuality: {
    datasets: {
      'bronze.simbad_bronze_parquet_ext': {
        freshness_hours: 2.5,
        record_count: 15234,
        completeness_pct: 98.5,
        accuracy_score: 96.2,
        schema_violations: 0
      },
      'silver_clean.simbad_hipotecarios': {
        freshness_hours: 3.2,
        record_count: 14892,
        completeness_pct: 99.1,
        accuracy_score: 97.8,
        schema_violations: 0
      },
      'gold.simbad_gold': {
        freshness_hours: 24.1,
        record_count: 2156,
        completeness_pct: 95.2,
        accuracy_score: 98.5,
        schema_violations: 0
      }
    },
    overall_score: 96.8,
    issues: [
      'gold.simbad_gold: Data is 24.1 hours old',
      'bronze.simbad_bronze_parquet_ext: Completeness below 99%'
    ],
    last_updated: new Date().toISOString()
    },
    costMetrics: {
      total_monthly_cost: 125.50 + randomVariation(),
    cost_by_service: {
      'BigQuery': 45.20,
      'Cloud Storage': 12.30,
      'DataProc': 55.00,
      'Cloud Run': 8.50,
      'Cloud Build': 4.50
    },
    cost_trend: [
      { date: '2025-01-01', cost: 120.00 },
      { date: '2025-01-02', cost: 123.00 },
      { date: '2025-01-03', cost: 125.50 }
    ],
      budget_utilization_pct: 62.75 + randomVariation()
    },
    performanceMetrics: {
    avg_execution_time: {
      bronze_etl: 12.5,
      silver_etl: 8.2,
      gold_etl: 5.1,
      landing_scrapers: 15.3
    },
    success_rate: {
      bronze_etl: 98.5,
      silver_etl: 99.2,
      gold_etl: 97.8,
      landing_scrapers: 95.1
    },
    throughput: {
      bronze_etl: 85000,
      silver_etl: 92000,
      gold_etl: 15000,
      landing_scrapers: 5000
    },
    resource_utilization: {
      dataproc_cpu: 65.5,
      dataproc_memory: 72.1,
      bigquery_slots: 45.8,
      cloud_run_cpu: 35.2 + randomVariation()
    }
  };
};

// Cache the mock data and regenerate periodically
let mockDataCache = generateDynamicMockData();
let lastCacheUpdate = Date.now();

const getMockData = () => {
  const now = Date.now();
  // Regenerate mock data every 10 seconds to simulate real-time updates
  if (now - lastCacheUpdate > 10000) {
    mockDataCache = generateDynamicMockData();
    lastCacheUpdate = now;
  }
  return mockDataCache;
};

// API functions
export const pipelineAPI = {
  // Get all pipeline status
  getAllStatus: async () => {
    if (DEMO_MODE) {
      return { data: getMockData().pipelineStatus };
    }
    try {
      const response = await api.get('/api/pipelines/status');
      return response.data;
    } catch (error) {
      console.warn('API not available, using dynamic mock data');
      return getMockData().pipelineStatus;
    }
  },

  // Get component status
  getComponentStatus: async (component: string) => {
    if (DEMO_MODE) {
      const mockData = getMockData();
      return { data: mockData.pipelineStatus[component as keyof typeof mockData.pipelineStatus] };
    }
    try {
      const response = await api.get(`/api/pipelines/${component}/status`);
      return response.data;
    } catch (error) {
      console.warn('API not available, using dynamic mock data');
      const mockData = getMockData();
      return mockData.pipelineStatus[component as keyof typeof mockData.pipelineStatus];
    }
  },

  // Trigger deployment
  triggerDeployment: async (component: string, branch: string = 'main') => {
    if (DEMO_MODE) {
      return { data: { job_id: 'mock-job-' + Date.now(), status: 'started' } };
    }
    try {
      const response = await api.post(`/api/deploy/${component}`, { branch });
      return response.data;
    } catch (error) {
      console.warn('API not available, simulating deployment');
      return { job_id: 'mock-job-' + Date.now(), status: 'started' };
    }
  },

  // DataProc operations
  startDataproc: async () => {
    if (DEMO_MODE) {
      return { data: { status: 'starting', cluster_name: 'cluster-integrador-2025' } };
    }
    try {
      const response = await api.post('/api/dataproc/cluster/start');
      return response.data;
    } catch (error) {
      console.warn('API not available, simulating DataProc start');
      return { status: 'starting', cluster_name: 'cluster-integrador-2025' };
    }
  },

  stopDataproc: async () => {
    if (DEMO_MODE) {
      return { data: { status: 'stopping', cluster_name: 'cluster-integrador-2025' } };
    }
    try {
      const response = await api.post('/api/dataproc/cluster/stop');
      return response.data;
    } catch (error) {
      console.warn('API not available, simulating DataProc stop');
      return { status: 'stopping', cluster_name: 'cluster-integrador-2025' };
    }
  }
};

export const monitoringAPI = {
  // Get data quality metrics
  getDataQuality: async () => {
    if (DEMO_MODE) {
      return { data: getMockData().dataQuality };
    }
    try {
      const response = await api.get('/api/monitoring/data-quality');
      return response.data;
    } catch (error) {
      console.warn('API not available, using dynamic mock data');
      return getMockData().dataQuality;
    }
  },

  // Get cost metrics
  getCostMetrics: async () => {
    if (DEMO_MODE) {
      return { data: getMockData().costMetrics };
    }
    try {
      const response = await api.get('/api/monitoring/costs');
      return response.data;
    } catch (error) {
      console.warn('API not available, using dynamic mock data');
      return getMockData().costMetrics;
    }
  },

  // Get performance metrics
  getPerformanceMetrics: async () => {
    if (DEMO_MODE) {
      return { data: getMockData().performanceMetrics };
    }
    try {
      const response = await api.get('/api/monitoring/performance');
      return response.data;
    } catch (error) {
      console.warn('API not available, using dynamic mock data');
      return getMockData().performanceMetrics;
    }
  },

  // Get deployment logs in real-time
  getDeploymentLogs: async (jobId: string, lines: number = 100) => {
    if (DEMO_MODE) {
      return { 
        data: {
          logs: [
            { timestamp: '10:30:15', level: 'INFO', message: 'Iniciando despliegue de servicios...' },
            { timestamp: '10:30:16', level: 'INFO', message: 'Verificando configuración Cloud Build...' },
            { timestamp: '10:30:18', level: 'INFO', message: 'Construyendo imagen Docker...' },
            { timestamp: '10:30:35', level: 'SUCCESS', message: 'Imagen construida exitosamente' },
            { timestamp: '10:30:50', level: 'INFO', message: 'Servicio desplegado correctamente' }
          ],
          hasMore: false
        }
      };
    }
    try {
      const response = await api.get(`/api/logs/${jobId}?lines=${lines}`);
      return response.data;
    } catch (error) {
      console.warn('Logs API not available');
      return { logs: [], hasMore: false };
    }
  },

  // Get real-time job progress
  getJobProgress: async (jobId: string) => {
    if (DEMO_MODE) {
      // Simulate progressive completion for jobs
      const baseProgress = Math.floor(Math.random() * 100);
      const steps = [
        'Iniciando...', 'Preparando datos...', 'Procesando...', 
        'Validando...', 'Finalizando...', 'Completando...'
      ];
      const stepIndex = Math.floor((baseProgress / 100) * steps.length);
      return { 
        data: {
          progress_percentage: baseProgress,
          current_step: steps[Math.min(stepIndex, steps.length - 1)],
          estimated_completion: new Date(Date.now() + (100 - baseProgress) * 3000).toISOString()
        }
      };
    }
    try {
      const response = await api.get(`/api/jobs/${jobId}/progress`);
      return response.data;
    } catch (error) {
      console.warn('Job progress API not available');
      return { progress_percentage: 0, current_step: 'Unknown', estimated_completion: null };
    }
  }
};

export default api;