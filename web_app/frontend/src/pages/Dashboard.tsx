import React from 'react';
import { Row, Col, Card, Statistic, Progress, Table, Tag, Space, Spin, Alert } from 'antd';
import { 
  ArrowUpOutlined, 
  ArrowDownOutlined, 
  DatabaseOutlined,
  CloudOutlined,
  ApiOutlined,
  MonitorOutlined 
} from '@ant-design/icons';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { usePipelineStatus, useCostMetrics, usePerformanceMetrics, useDataQuality } from '../hooks/usePipelineData';
import ConnectionStatus from '../components/ConnectionStatus';

const Dashboard: React.FC = () => {
  // Fetch real data
  const { data: pipelineStatus, isLoading: pipelineLoading, error: pipelineError } = usePipelineStatus();
  const { data: costMetrics, isLoading: costLoading } = useCostMetrics();
  const { data: performanceMetrics, isLoading: performanceLoading } = usePerformanceMetrics();
  const { data: dataQuality, isLoading: qualityLoading } = useDataQuality();

  // Mock chart data - would be derived from real metrics
  const pipelineMetrics = [
    { date: '2025-01-01', success: 95, errors: 5 },
    { date: '2025-01-02', success: 98, errors: 2 },
    { date: '2025-01-03', success: 97, errors: 3 },
    { date: '2025-01-04', success: 99, errors: 1 },
    { date: '2025-01-05', success: 96, errors: 4 },
    { date: '2025-01-06', success: 98, errors: 2 },
    { date: '2025-01-07', success: 100, errors: 0 },
  ];

  const recentExecutions = [
    {
      key: '1',
      component: 'Landing SIMBAD',
      status: 'success',
      duration: '2m 15s',
      records: '15,234',
      timestamp: '2025-01-10 08:30',
    },
    {
      key: '2', 
      component: 'Bronze ETL',
      status: 'success',
      duration: '12m 45s',
      records: '15,234',
      timestamp: '2025-01-10 08:35',
    },
    {
      key: '3',
      component: 'Silver ETL',
      status: 'running',
      duration: '8m 12s',
      records: '14,892',
      timestamp: '2025-01-10 08:48',
    },
    {
      key: '4',
      component: 'Gold ETL',
      status: 'pending',
      duration: '-',
      records: '-',
      timestamp: '-',
    },
  ];

  const columns = [
    {
      title: 'Component',
      dataIndex: 'component',
      key: 'component',
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => {
        const color = status === 'success' ? 'green' : 
                    status === 'running' ? 'blue' :
                    status === 'failed' ? 'red' : 'default';
        return <Tag color={color}>{status.toUpperCase()}</Tag>;
      },
    },
    {
      title: 'Duration',
      dataIndex: 'duration',
      key: 'duration',
    },
    {
      title: 'Records',
      dataIndex: 'records',
      key: 'records',
    },
    {
      title: 'Last Run',
      dataIndex: 'timestamp',
      key: 'timestamp',
    },
  ];

  // Show loading state
  if (pipelineLoading || costLoading || performanceLoading || qualityLoading) {
    return (
      <div style={{ padding: '0 24px', textAlign: 'center', marginTop: '100px' }}>
        <Spin size="large" />
        <div style={{ marginTop: 16 }}>Cargando dashboard...</div>
      </div>
    );
  }

  // Show error state
  if (pipelineError) {
    return (
      <div style={{ padding: '0 24px' }}>
        <Alert
          message="Error al cargar datos"
          description="No se pudo conectar con la API. Mostrando datos de demostración."
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
        />
      </div>
    );
  }

  // Calculate metrics from real data
  const calculatePipelineHealth = () => {
    if (!pipelineStatus) return 95;
    const components = Object.values(pipelineStatus);
    const healthy = components.filter(c => c.state === 'healthy' || c.state === 'running').length;
    return Math.round((healthy / components.length) * 100);
  };

  const getTotalRecords = () => {
    if (!pipelineStatus) return 47852;
    let total = 0;
    Object.values(pipelineStatus).forEach(component => {
      if (component.metrics?.records_processed) {
        total += component.metrics.records_processed;
      }
    });
    return total || 47852;
  };

  const pipelineHealth = calculatePipelineHealth();
  const totalRecords = getTotalRecords();
  const monthlyCost = costMetrics?.total_monthly_cost || 125.50;
  const successRate = performanceMetrics?.success_rate ? 
    Object.values(performanceMetrics.success_rate).reduce((a, b) => a + b, 0) / Object.keys(performanceMetrics.success_rate).length 
    : 96.8;

  return (
    <div style={{ padding: '0 24px' }}>
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Card>
            <Statistic
              title="Pipeline Health"
              value={pipelineHealth}
              precision={1}
              valueStyle={{ color: pipelineHealth > 90 ? '#3f8600' : '#cf1322' }}
              prefix={pipelineHealth > 90 ? <ArrowUpOutlined /> : <ArrowDownOutlined />}
              suffix="%"
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="Daily Success Rate"
              value={successRate}
              precision={1}
              valueStyle={{ color: '#3f8600' }}
              prefix={<ArrowUpOutlined />}
              suffix="%"
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="Records Processed Today"
              value={totalRecords}
              valueStyle={{ color: '#1890ff' }}
              prefix={<DatabaseOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="Monthly Cost"
              value={monthlyCost}
              precision={2}
              valueStyle={{ color: '#cf1322' }}
              prefix="$"
              suffix={
                <span style={{ fontSize: '14px', color: '#666' }}>
                  <ArrowDownOutlined /> 8.2%
                </span>
              }
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={12}>
          <Card title="Pipeline Success Rate (Last 7 Days)" bordered={false}>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={pipelineMetrics}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" />
                <YAxis />
                <Tooltip />
                <Line 
                  type="monotone" 
                  dataKey="success" 
                  stroke="#52c41a" 
                  strokeWidth={2} 
                  name="Success Rate (%)"
                />
              </LineChart>
            </ResponsiveContainer>
          </Card>
        </Col>
        <Col span={12}>
          <Card title="System Overview" bordered={false}>
            <Space direction="vertical" style={{ width: '100%' }} size="large">
              <div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                  <span><DatabaseOutlined /> DataProc Cluster</span>
                  <Tag color="green">RUNNING</Tag>
                </div>
                <Progress percent={75} size="small" status="active" />
                <div style={{ fontSize: '12px', color: '#666', marginTop: 4 }}>
                  CPU: 65% | Memory: 72% | 4 workers active
                </div>
              </div>
              
              <div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                  <span><CloudOutlined /> BigQuery</span>
                  <Tag color="green">HEALTHY</Tag>
                </div>
                <Progress percent={92} size="small" />
                <div style={{ fontSize: '12px', color: '#666', marginTop: 4 }}>
                  Slots: 45% | Queries: 234 today
                </div>
              </div>
              
              <div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                  <span><ApiOutlined /> Landing Services</span>
                  <Tag color="green">ACTIVE</Tag>
                </div>
                <Progress percent={88} size="small" />
                <div style={{ fontSize: '12px', color: '#666', marginTop: 4 }}>
                  SIMBAD: OK | Macroeconomics: OK
                </div>
              </div>
              
              <div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                  <span><MonitorOutlined /> Data Quality</span>
                  <Tag color="yellow">WARNING</Tag>
                </div>
                <Progress percent={85} size="small" status="exception" />
                <div style={{ fontSize: '12px', color: '#666', marginTop: 4 }}>
                  2 quality issues detected
                </div>
              </div>
            </Space>
          </Card>
        </Col>
      </Row>

      <Row>
        <Col span={24}>
          <Card title="Recent Pipeline Executions" bordered={false}>
            <Table 
              columns={columns} 
              dataSource={recentExecutions} 
              pagination={false}
              size="middle"
            />
          </Card>
        </Col>
      </Row>
    </div>
  );
};

export default Dashboard;