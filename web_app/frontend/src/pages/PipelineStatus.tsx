import React from 'react';
import { Card, Row, Col, Tag, Button, Space, Descriptions, Progress, Timeline, Spin, Alert } from 'antd';
import { 
  PlayCircleOutlined, 
  StopOutlined, 
  ReloadOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
  ClockCircleOutlined,
  SyncOutlined,
} from '@ant-design/icons';
import { usePipelineStatus } from '../hooks/usePipelineData';

const PipelineStatus: React.FC = () => {
  const { data: pipelineData, isLoading, error, isFetching, dataUpdatedAt } = usePipelineStatus();

  // Component mapping for display names and descriptions
  const componentDisplayInfo = {
    landing_simbad: {
      name: 'Landing - SIMBAD',
      description: 'SIMBAD banking data scraper',
      nextRun: '2025-02-20 00:00:00'
    },
    landing_macroeconomics: {
      name: 'Landing - Macroeconomics', 
      description: 'Macroeconomic indicators scraper',
      nextRun: '2025-02-20 00:00:00'
    },
    bigquery_bronze: {
      name: 'Bronze ETL',
      description: 'Raw data processing to Bronze layer',
      nextRun: '2025-02-20 02:00:00'
    },
    bigquery_silver: {
      name: 'Silver ETL',
      description: 'Data cleaning and deduplication', 
      nextRun: '2025-02-20 04:00:00'
    },
    bigquery_gold: {
      name: 'Gold ETL',
      description: 'Business metrics and indicators',
      nextRun: '2025-02-20 06:00:00'
    },
    dataproc: {
      name: 'DataProc Cluster',
      description: 'Spark processing cluster for historical reprocessing',
      nextRun: 'On demand'
    }
  };

  // Transform API data to component format
  const components = pipelineData ? Object.entries(pipelineData).map(([key, value]: [string, any]) => {
    const info = componentDisplayInfo[key as keyof typeof componentDisplayInfo];
    return {
      id: key,
      name: info?.name || key,
      status: value.state,
      lastRun: new Date(value.last_run).toLocaleString(),
      nextRun: info?.nextRun || 'TBD',
      duration: value.state === 'running' ? 'In progress...' : 'N/A',
      records: value.metrics?.records_processed?.toLocaleString() || 
               value.metrics?.table_count?.toString() || 'N/A',
      description: info?.description || 'Pipeline component',
      metrics: value.metrics
    };
  }) : [];

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'healthy':
        return <CheckCircleOutlined style={{ color: '#52c41a' }} />;
      case 'running':
        return <ClockCircleOutlined style={{ color: '#1890ff' }} />;
      case 'failed':
        return <ExclamationCircleOutlined style={{ color: '#ff4d4f' }} />;
      default:
        return <ClockCircleOutlined style={{ color: '#d9d9d9' }} />;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'healthy': return 'green';
      case 'running': return 'blue';
      case 'failed': return 'red';
      case 'stopped': return 'default';
      default: return 'default';
    }
  };

  const handleStart = (componentName: string) => {
    console.log(`Starting ${componentName}`);
    // API call to start component
  };

  const handleStop = (componentName: string) => {
    console.log(`Stopping ${componentName}`);
    // API call to stop component
  };

  const handleRestart = (componentName: string) => {
    console.log(`Restarting ${componentName}`);
    // API call to restart component
  };

  // Calcular progreso dinámico basado en métricas reales
  const calculateDynamicProgress = (component: any) => {
    const now = new Date().getTime();
    const lastRun = new Date(component.last_run).getTime();
    const timeSinceStart = now - lastRun;
    
    // Estimaciones basadas en el tipo de componente y tiempo transcurrido
    const estimatedTimes = {
      'landing_simbad': 3 * 60 * 1000, // 3 minutos
      'landing_macroeconomics': 2 * 60 * 1000, // 2 minutos
      'bigquery_bronze': 15 * 60 * 1000, // 15 minutos
      'bigquery_silver': 10 * 60 * 1000, // 10 minutos
      'bigquery_gold': 5 * 60 * 1000, // 5 minutos
      'dataproc': 20 * 60 * 1000, // 20 minutos
    };

    const estimatedTime = estimatedTimes[component.component as keyof typeof estimatedTimes] || 10 * 60 * 1000;
    const progress = Math.min(95, Math.round((timeSinceStart / estimatedTime) * 100));
    
    return progress;
  };

  // Describir la etapa actual del progreso
  const getProgressDescription = (componentId: string, percent: number) => {
    if (percent < 10) return 'Iniciando...';
    if (percent < 30) return 'Preparando datos...';
    if (percent < 60) return 'Procesando...';
    if (percent < 85) return 'Finalizando...';
    if (percent < 95) return 'Limpieza...';
    return 'Casi completo...';
  };

  const pipelineFlow = [
    {
      color: 'green',
      children: (
        <div>
          <strong>Landing Scrapers</strong>
          <div>SIMBAD + Macroeconomics data collection</div>
          <div style={{ color: '#666', fontSize: '12px' }}>Monthly: Day 20 at 00:00</div>
        </div>
      ),
    },
    {
      color: 'blue',
      children: (
        <div>
          <strong>Bronze Processing</strong>
          <div>Raw data ingestion to Parquet format</div>
          <div style={{ color: '#666', fontSize: '12px' }}>Triggered after landing completion</div>
        </div>
      ),
    },
    {
      color: 'orange',
      children: (
        <div>
          <strong>Silver Processing</strong>
          <div>Data cleaning, deduplication, and normalization</div>
          <div style={{ color: '#666', fontSize: '12px' }}>BigQuery MERGE operations</div>
        </div>
      ),
    },
    {
      color: 'purple',
      children: (
        <div>
          <strong>Gold Processing</strong>
          <div>Business metrics and analytical indicators</div>
          <div style={{ color: '#666', fontSize: '12px' }}>Final aggregations ready for BI</div>
        </div>
      ),
    },
  ];

  if (isLoading) {
    return (
      <div style={{ padding: '0 24px', display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '400px' }}>
        <Spin size="large" />
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ padding: '0 24px' }}>
        <Alert
          message="Error al cargar datos del pipeline"
          description="No se pudo conectar con el backend. Verifica que el servidor esté ejecutándose."
          type="error"
          showIcon
        />
      </div>
    );
  }

  return (
    <div style={{ padding: '0 24px' }}>
      <Row gutter={[16, 16]}>
        <Col span={16}>
          <Card 
            title={
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <span>Pipeline Components</span>
                <Space>
                  {isFetching && <SyncOutlined spin />}
                  <span style={{ fontSize: '12px', color: '#666' }}>
                    Última actualización: {new Date(dataUpdatedAt).toLocaleTimeString()}
                  </span>
                </Space>
              </div>
            } 
            bordered={false}
          >
            <Space direction="vertical" size="middle" style={{ width: '100%' }}>
              {components.map((component, index) => (
                <Card key={index} size="small" style={{ backgroundColor: '#fafafa' }}>
                  <Row align="middle">
                    <Col span={16}>
                      <Space size="middle">
                        {getStatusIcon(component.status)}
                        <div>
                          <div style={{ fontWeight: 'bold', fontSize: '16px' }}>
                            {component.name}
                          </div>
                          <div style={{ color: '#666', fontSize: '14px' }}>
                            {component.description}
                          </div>
                        </div>
                      </Space>
                    </Col>
                    <Col span={4} style={{ textAlign: 'center' }}>
                      <Tag color={getStatusColor(component.status)}>
                        {component.status.toUpperCase()}
                      </Tag>
                    </Col>
                    <Col span={4} style={{ textAlign: 'right' }}>
                      <Space>
                        {component.status === 'stopped' && (
                          <Button 
                            type="primary" 
                            size="small" 
                            icon={<PlayCircleOutlined />}
                            onClick={() => handleStart(component.name)}
                          >
                            Start
                          </Button>
                        )}
                        {component.status === 'running' && (
                          <Button 
                            danger 
                            size="small" 
                            icon={<StopOutlined />}
                            onClick={() => handleStop(component.name)}
                          >
                            Stop
                          </Button>
                        )}
                        {(component.status === 'healthy' || component.status === 'failed') && (
                          <Button 
                            size="small" 
                            icon={<ReloadOutlined />}
                            onClick={() => handleRestart(component.name)}
                          >
                            Restart
                          </Button>
                        )}
                      </Space>
                    </Col>
                  </Row>
                  
                  <Row style={{ marginTop: 16 }}>
                    <Col span={24}>
                      <Descriptions size="small" column={4}>
                        <Descriptions.Item label="Last Run">
                          {component.lastRun}
                        </Descriptions.Item>
                        <Descriptions.Item label="Duration">
                          {component.duration}
                        </Descriptions.Item>
                        <Descriptions.Item label="Records">
                          {component.records}
                        </Descriptions.Item>
                        <Descriptions.Item label="Next Run">
                          {component.nextRun}
                        </Descriptions.Item>
                      </Descriptions>
                    </Col>
                  </Row>
                  
                  {component.status === 'running' && (
                    <Progress 
                      percent={component.metrics?.progress_percentage || calculateDynamicProgress(component)}
                      size="small" 
                      status="active"
                      style={{ marginTop: 8 }}
                      format={(percent) => `${percent}% - ${getProgressDescription(component.id, percent || 0)}`}
                    />
                  )}
                </Card>
              ))}
            </Space>
          </Card>
        </Col>
        
        <Col span={8}>
          <Space direction="vertical" size="middle" style={{ width: '100%' }}>
            <Card title="Pipeline Flow" bordered={false}>
              <Timeline items={pipelineFlow} />
            </Card>
            
            <Card title="System Health" bordered={false}>
              <Space direction="vertical" style={{ width: '100%' }}>
                <div>
                  <div style={{ marginBottom: 8 }}>
                    <span>Overall Pipeline Health</span>
                    <span style={{ float: 'right', fontWeight: 'bold', color: '#52c41a' }}>
                      98.5%
                    </span>
                  </div>
                  <Progress percent={98.5} size="small" />
                </div>
                
                <div>
                  <div style={{ marginBottom: 8 }}>
                    <span>Data Freshness</span>
                    <span style={{ float: 'right', fontWeight: 'bold', color: '#1890ff' }}>
                      2h ago
                    </span>
                  </div>
                  <Progress percent={85} size="small" />
                </div>
                
                <div>
                  <div style={{ marginBottom: 8 }}>
                    <span>Error Rate</span>
                    <span style={{ float: 'right', fontWeight: 'bold', color: '#52c41a' }}>
                      0.2%
                    </span>
                  </div>
                  <Progress percent={0.2} size="small" status="exception" />
                </div>
              </Space>
            </Card>
            
            <Card title="Quick Actions" bordered={false}>
              <Space direction="vertical" style={{ width: '100%' }}>
                <Button type="primary" block>
                  Trigger Full Pipeline
                </Button>
                <Button block>
                  Run Data Quality Checks
                </Button>
                <Button block>
                  View Deployment Logs
                </Button>
                <Button danger block>
                  Emergency Stop All
                </Button>
              </Space>
            </Card>
          </Space>
        </Col>
      </Row>
    </div>
  );
};

export default PipelineStatus;