import React from 'react';
import { Card, Row, Col, Tag, Button, Space, Descriptions, Progress, Timeline } from 'antd';
import { 
  PlayCircleOutlined, 
  StopOutlined, 
  ReloadOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
  ClockCircleOutlined,
} from '@ant-design/icons';

const PipelineStatus: React.FC = () => {
  const components = [
    {
      name: 'Landing - SIMBAD',
      status: 'healthy',
      lastRun: '2025-01-10 08:30:00',
      nextRun: '2025-02-20 00:00:00',
      duration: '2m 15s',
      records: '15,234',
      description: 'SIMBAD banking data scraper',
    },
    {
      name: 'Landing - Macroeconomics',
      status: 'healthy',
      lastRun: '2025-01-10 08:28:00',
      nextRun: '2025-02-20 00:00:00',
      duration: '1m 45s',
      records: '3,456',
      description: 'Macroeconomic indicators scraper',
    },
    {
      name: 'Bronze ETL',
      status: 'running',
      lastRun: '2025-01-10 08:35:00',
      nextRun: '2025-02-20 02:00:00',
      duration: '12m 45s (in progress)',
      records: '15,234',
      description: 'Raw data processing to Bronze layer',
    },
    {
      name: 'Silver ETL',
      status: 'pending',
      lastRun: '2025-01-09 08:48:00',
      nextRun: '2025-02-20 04:00:00',
      duration: '8m 12s',
      records: '14,892',
      description: 'Data cleaning and deduplication',
    },
    {
      name: 'Gold ETL',
      status: 'pending',
      lastRun: '2025-01-09 08:56:00',
      nextRun: '2025-02-20 06:00:00',
      duration: '5m 30s',
      records: '2,156',
      description: 'Business metrics and indicators',
    },
    {
      name: 'DataProc Cluster',
      status: 'stopped',
      lastRun: '2025-01-09 09:15:00',
      nextRun: 'On demand',
      duration: 'N/A',
      records: 'N/A',
      description: 'Spark processing cluster for historical reprocessing',
    },
  ];

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

  return (
    <div style={{ padding: '0 24px' }}>
      <Row gutter={[16, 16]}>
        <Col span={16}>
          <Card title="Pipeline Components" bordered={false}>
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
                      percent={65} 
                      size="small" 
                      status="active"
                      style={{ marginTop: 8 }}
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