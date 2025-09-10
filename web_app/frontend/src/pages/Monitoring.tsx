import React, { useState } from 'react';
import { 
  Card, 
  Row, 
  Col, 
  Table, 
  Tag, 
  Space, 
  Button,
  Modal,
  Tabs,
  Timeline,
  Progress,
  Alert,
  Statistic,
  List,
  Typography,
  Divider,
  Badge,
  Tooltip,
  Select,
  DatePicker,
  Input
} from 'antd';
import { 
  EyeOutlined,
  BugOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
  ClockCircleOutlined,
  WarningOutlined,
  DatabaseOutlined,
  CloudOutlined,
  ApiOutlined,
  SettingOutlined,
  ReloadOutlined,
  DownloadOutlined,
  FilterOutlined,
  SearchOutlined
} from '@ant-design/icons';
import { usePipelineStatus, useDataQuality } from '../hooks/usePipelineData';

const { Title, Text, Paragraph } = Typography;
const { TabPane } = Tabs;
const { RangePicker } = DatePicker;

interface ExecutionHistory {
  id: string;
  layer: string;
  component: string;
  startTime: string;
  endTime: string;
  duration: string;
  status: 'success' | 'failed' | 'running' | 'warning';
  recordsProcessed: number;
  errorDetails?: string;
  logs: LogEntry[];
}

interface LogEntry {
  timestamp: string;
  level: 'INFO' | 'WARNING' | 'ERROR' | 'SUCCESS' | 'DEBUG';
  component: string;
  message: string;
  details?: string;
}

const Monitoring: React.FC = () => {
  const { data: pipelineStatus, isLoading } = usePipelineStatus();
  const { data: dataQuality } = useDataQuality();
  const [selectedExecution, setSelectedExecution] = useState<ExecutionHistory | null>(null);
  const [logsModalVisible, setLogsModalVisible] = useState(false);
  const [activeTab, setActiveTab] = useState('executions');
  const [filterStatus, setFilterStatus] = useState<string>('all');
  const [filterLayer, setFilterLayer] = useState<string>('all');

  // Historia de ejecuciones simulada basada en la arquitectura real
  const executionHistory: ExecutionHistory[] = [
    {
      id: 'exec_001',
      layer: 'landing',
      component: 'SIMBAD Scraper',
      startTime: '2025-01-10 08:30:00',
      endTime: '2025-01-10 08:32:15',
      duration: '2m 15s',
      status: 'success',
      recordsProcessed: 15234,
      logs: [
        { timestamp: '08:30:00', level: 'INFO', component: 'scraper', message: 'Iniciando scraping SIMBAD...', details: 'Conectando a https://sib.gob.do' },
        { timestamp: '08:30:05', level: 'INFO', component: 'scraper', message: 'Autenticación exitosa', details: 'Usuario: etl_user' },
        { timestamp: '08:30:10', level: 'INFO', component: 'scraper', message: 'Descargando datos hipotecarios...', details: 'Período: 2024-12' },
        { timestamp: '08:31:45', level: 'INFO', component: 'storage', message: 'Guardando en Cloud Storage...', details: 'gs://dae-integrador-2025/lakehouse/landing/simbad/' },
        { timestamp: '08:32:15', level: 'SUCCESS', component: 'scraper', message: 'Scraping completado exitosamente', details: '15,234 registros procesados' }
      ]
    },
    {
      id: 'exec_002',
      layer: 'bronze',
      component: 'DataProc Bronze ETL',
      startTime: '2025-01-10 08:35:00',
      endTime: '2025-01-10 08:47:45',
      duration: '12m 45s',
      status: 'success',
      recordsProcessed: 15234,
      logs: [
        { timestamp: '08:35:00', level: 'INFO', component: 'dataproc', message: 'Iniciando cluster DataProc...', details: 'cluster-integrador-2025' },
        { timestamp: '08:35:30', level: 'INFO', component: 'dataproc', message: 'Cluster activo con 4 workers', details: 'Master: n2-standard-2, Workers: 4x n2-standard-2' },
        { timestamp: '08:36:00', level: 'INFO', component: 'pyspark', message: 'Ejecutando land_simbad_bronze.py...', details: 'Transformando CSV a Parquet' },
        { timestamp: '08:45:00', level: 'INFO', component: 'bigquery', message: 'Creando tablas externas...', details: 'simbad_bronze_parquet_ext' },
        { timestamp: '08:47:45', level: 'SUCCESS', component: 'etl', message: 'ETL Bronze completado', details: '15,234 registros convertidos a Parquet' }
      ]
    },
    {
      id: 'exec_003',
      layer: 'silver',
      component: 'Silver MERGE Process',
      startTime: '2025-01-10 08:48:00',
      endTime: '2025-01-10 08:56:12',
      duration: '8m 12s',
      status: 'running',
      recordsProcessed: 14892,
      logs: [
        { timestamp: '08:48:00', level: 'INFO', component: 'bigquery', message: 'Iniciando proceso MERGE Silver...', details: 'sp_silver_merge' },
        { timestamp: '08:48:05', level: 'INFO', component: 'bigquery', message: 'Verificando datos incrementales...', details: 'Período >= 202412' },
        { timestamp: '08:50:30', level: 'INFO', component: 'bigquery', message: 'Aplicando deduplicación...', details: 'ROW_NUMBER() por clave natural' },
        { timestamp: '08:55:00', level: 'WARNING', component: 'bigquery', message: '342 registros duplicados encontrados', details: 'Aplicando regla: dt_captura DESC' },
        { timestamp: '08:56:00', level: 'INFO', component: 'bigquery', message: 'MERGE en progreso...', details: '89% completado' }
      ]
    },
    {
      id: 'exec_004',
      layer: 'silver',
      component: 'Data Quality Validation',
      startTime: '2025-01-09 14:20:00',
      endTime: '2025-01-09 14:22:30',
      duration: '2m 30s',
      status: 'failed',
      recordsProcessed: 0,
      errorDetails: 'Schema validation failed: Missing column "periodo_date" in 3 records',
      logs: [
        { timestamp: '14:20:00', level: 'INFO', component: 'validator', message: 'Iniciando validación de calidad...', details: 'Checks: completeness, accuracy, schema' },
        { timestamp: '14:20:15', level: 'INFO', component: 'validator', message: 'Validando completeness...', details: '✓ 98.5% completeness' },
        { timestamp: '14:21:00', level: 'WARNING', component: 'validator', message: 'Accuracy check warning', details: '96.2% accuracy score' },
        { timestamp: '14:21:30', level: 'ERROR', component: 'validator', message: 'Schema validation failed', details: 'Missing periodo_date in 3 records' },
        { timestamp: '14:22:30', level: 'ERROR', component: 'validator', message: 'Validación terminada con errores', details: 'Fix required before Gold processing' }
      ]
    },
    {
      id: 'exec_005',
      layer: 'landing',
      component: 'Macroeconomics Scraper',
      startTime: '2025-01-10 08:28:00',
      endTime: '2025-01-10 08:33:45',
      duration: '5m 45s',
      status: 'warning',
      recordsProcessed: 3456,
      logs: [
        { timestamp: '08:28:00', level: 'INFO', component: 'macro-scraper', message: 'Conectando a APIs externas...', details: 'IMF + BCRD endpoints' },
        { timestamp: '08:28:30', level: 'INFO', component: 'macro-scraper', message: 'Descargando datos de inflación...', details: 'IMF API: 12-month inflation data' },
        { timestamp: '08:30:00', level: 'WARNING', component: 'macro-scraper', message: 'Timeout en BCRD API', details: 'Reintentando en 30s...' },
        { timestamp: '08:30:30', level: 'INFO', component: 'macro-scraper', message: 'Conexión BCRD restablecida', details: 'Descargando tipo de cambio...' },
        { timestamp: '08:33:45', level: 'SUCCESS', component: 'macro-scraper', message: 'Datos macro descargados', details: '3,456 registros con warnings' }
      ]
    }
  ];

  const currentIssues = [
    {
      id: 'issue_001',
      severity: 'high',
      component: 'Silver Layer',
      message: 'Schema validation errors detected',
      description: '3 registros sin campo periodo_date están bloqueando el procesamiento Gold',
      solution: 'Corregir ETL Bronze para asegurar todos los campos requeridos',
      timestamp: '2025-01-09 14:22:30'
    },
    {
      id: 'issue_002', 
      severity: 'medium',
      component: 'BCRD API',
      message: 'Timeouts intermitentes',
      description: 'API de tipo de cambio BCRD presenta latencia alta ocasionalmente',
      solution: 'Implementar retry con backoff exponencial',
      timestamp: '2025-01-10 08:30:00'
    },
    {
      id: 'issue_003',
      severity: 'low',
      component: 'DataProc Cluster',
      message: 'Uso de memoria sub-óptimo',
      description: 'Workers usando solo 60% memoria disponible',
      solution: 'Ajustar configuración Spark para mejor utilización',
      timestamp: '2025-01-10 08:40:00'
    }
  ];

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'success': return 'green';
      case 'failed': return 'red';
      case 'running': return 'blue';
      case 'warning': return 'orange';
      default: return 'default';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'success': return <CheckCircleOutlined />;
      case 'failed': return <ExclamationCircleOutlined />;
      case 'running': return <ClockCircleOutlined />;
      case 'warning': return <WarningOutlined />;
      default: return <ClockCircleOutlined />;
    }
  };

  const getSeverityColor = (severity: string) => {
    switch (severity) {
      case 'high': return 'red';
      case 'medium': return 'orange';
      case 'low': return 'blue';
      default: return 'default';
    }
  };

  const getLogLevelColor = (level: string) => {
    switch (level) {
      case 'ERROR': return '#ff4d4f';
      case 'WARNING': return '#faad14';
      case 'SUCCESS': return '#52c41a';
      case 'INFO': return '#1890ff';
      case 'DEBUG': return '#666';
      default: return '#666';
    }
  };

  const showExecutionDetails = (execution: ExecutionHistory) => {
    setSelectedExecution(execution);
    setLogsModalVisible(true);
  };

  const filteredExecutions = executionHistory.filter(exec => {
    const statusMatch = filterStatus === 'all' || exec.status === filterStatus;
    const layerMatch = filterLayer === 'all' || exec.layer === filterLayer;
    return statusMatch && layerMatch;
  });

  const executionColumns = [
    {
      title: 'Hora Inicio',
      dataIndex: 'startTime',
      key: 'startTime',
      width: 120,
      render: (time: string) => time.split(' ')[1]
    },
    {
      title: 'Capa',
      dataIndex: 'layer',
      key: 'layer',
      width: 80,
      render: (layer: string) => (
        <Tag color={layer === 'landing' ? 'green' : layer === 'bronze' ? 'volcano' : layer === 'silver' ? 'geekblue' : 'purple'}>
          {layer.toUpperCase()}
        </Tag>
      )
    },
    {
      title: 'Componente',
      dataIndex: 'component',
      key: 'component',
      ellipsis: true
    },
    {
      title: 'Estado',
      dataIndex: 'status',
      key: 'status',
      width: 90,
      render: (status: string) => (
        <Tag color={getStatusColor(status)} icon={getStatusIcon(status)}>
          {status.toUpperCase()}
        </Tag>
      )
    },
    {
      title: 'Duración',
      dataIndex: 'duration',
      key: 'duration',
      width: 90
    },
    {
      title: 'Registros',
      dataIndex: 'recordsProcessed',
      key: 'recordsProcessed',
      width: 100,
      render: (count: number) => count.toLocaleString()
    },
    {
      title: 'Acciones',
      key: 'actions',
      width: 100,
      render: (_, record: ExecutionHistory) => (
        <Space>
          <Button 
            type="text" 
            icon={<EyeOutlined />} 
            onClick={() => showExecutionDetails(record)}
            title="Ver logs detallados"
          />
          {record.status === 'failed' && (
            <Button 
              type="text" 
              icon={<BugOutlined />} 
              danger
              title="Ver errores"
            />
          )}
        </Space>
      )
    }
  ];

  return (
    <div style={{ padding: '0 24px' }}>
      <Row gutter={[0, 16]}>
        <Col span={24}>
          <Card>
            <Title level={3} style={{ margin: 0 }}>
              🔍 Monitoreo y Logs del Sistema
            </Title>
            <Paragraph type="secondary" style={{ marginTop: 8 }}>
              Seguimiento detallado de ejecuciones, logs, errores y métricas en tiempo real
            </Paragraph>
          </Card>
        </Col>
      </Row>

      {/* Métricas rápidas */}
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Card>
            <Statistic
              title="Ejecuciones Hoy"
              value={executionHistory.length}
              prefix={<PlayCircleOutlined />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="Tasa de Éxito"
              value={92.5}
              precision={1}
              suffix="%"
              prefix={<CheckCircleOutlined />}
              valueStyle={{ color: '#52c41a' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="Errores Activos"
              value={currentIssues.filter(i => i.severity === 'high').length}
              prefix={<BugOutlined />}
              valueStyle={{ color: '#ff4d4f' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="Tiempo Promedio"
              value="8.2"
              suffix="min"
              prefix={<ClockCircleOutlined />}
              valueStyle={{ color: '#722ed1' }}
            />
          </Card>
        </Col>
      </Row>

      <Card>
        <Tabs activeKey={activeTab} onChange={setActiveTab}>
          <TabPane tab="📋 Historial de Ejecuciones" key="executions">
            <Space style={{ marginBottom: 16 }}>
              <Select
                placeholder="Filtrar por estado"
                style={{ width: 150 }}
                value={filterStatus}
                onChange={setFilterStatus}
              >
                <Select.Option value="all">Todos</Select.Option>
                <Select.Option value="success">Exitosos</Select.Option>
                <Select.Option value="failed">Fallidos</Select.Option>
                <Select.Option value="running">En Curso</Select.Option>
                <Select.Option value="warning">Con Warnings</Select.Option>
              </Select>

              <Select
                placeholder="Filtrar por capa"
                style={{ width: 120 }}
                value={filterLayer}
                onChange={setFilterLayer}
              >
                <Select.Option value="all">Todas</Select.Option>
                <Select.Option value="landing">Landing</Select.Option>
                <Select.Option value="bronze">Bronze</Select.Option>
                <Select.Option value="silver">Silver</Select.Option>
                <Select.Option value="gold">Gold</Select.Option>
              </Select>

              <Button icon={<ReloadOutlined />}>Refrescar</Button>
              <Button icon={<DownloadOutlined />}>Exportar</Button>
            </Space>

            <Table
              columns={executionColumns}
              dataSource={filteredExecutions}
              rowKey="id"
              size="middle"
              pagination={{ pageSize: 10 }}
            />
          </TabPane>

          <TabPane tab="🚨 Problemas Actuales" key="issues">
            <List
              dataSource={currentIssues}
              renderItem={(issue) => (
                <List.Item
                  actions={[
                    <Button type="link">Ver Detalles</Button>,
                    <Button type="link">Resolver</Button>
                  ]}
                >
                  <List.Item.Meta
                    avatar={
                      <Badge 
                        status={getSeverityColor(issue.severity) as any} 
                        dot
                      >
                        <BugOutlined style={{ fontSize: '18px' }} />
                      </Badge>
                    }
                    title={
                      <div>
                        <Tag color={getSeverityColor(issue.severity)}>
                          {issue.severity.toUpperCase()}
                        </Tag>
                        {issue.component} - {issue.message}
                      </div>
                    }
                    description={
                      <div>
                        <Paragraph style={{ margin: '4px 0' }}>
                          <strong>Descripción:</strong> {issue.description}
                        </Paragraph>
                        <Paragraph style={{ margin: '4px 0' }}>
                          <strong>Solución:</strong> {issue.solution}
                        </Paragraph>
                        <Text type="secondary">{issue.timestamp}</Text>
                      </div>
                    }
                  />
                </List.Item>
              )}
            />
          </TabPane>

          <TabPane tab="📊 Stored Procedures" key="procedures">
            <Row gutter={16}>
              <Col span={8}>
                <Card size="small" title="📦 Bronze Layer">
                  <List size="small">
                    <List.Item>
                      <div>
                        <Text code>sp_bronze_simbad</Text>
                        <br />
                        <Text type="secondary">Landing → Bronze conversion</Text>
                      </div>
                      <Tag color="green">ACTIVO</Tag>
                    </List.Item>
                    <List.Item>
                      <div>
                        <Text code>sp_bronze_macro</Text>
                        <br />
                        <Text type="secondary">Macro indicators ETL</Text>
                      </div>
                      <Tag color="green">ACTIVO</Tag>
                    </List.Item>
                  </List>
                </Card>
              </Col>
              <Col span={8}>
                <Card size="small" title="🥈 Silver Layer">
                  <List size="small">
                    <List.Item>
                      <div>
                        <Text code>sp_silver_merge</Text>
                        <br />
                        <Text type="secondary">MERGE incremental + dedupe</Text>
                      </div>
                      <Tag color="blue">EN CURSO</Tag>
                    </List.Item>
                    <List.Item>
                      <div>
                        <Text code>sp_silver_quality</Text>
                        <br />
                        <Text type="secondary">Data quality validation</Text>
                      </div>
                      <Tag color="orange">WARNING</Tag>
                    </List.Item>
                  </List>
                </Card>
              </Col>
              <Col span={8}>
                <Card size="small" title="🥇 Gold Layer">
                  <List size="small">
                    <List.Item>
                      <div>
                        <Text code>sp_gold_indicators</Text>
                        <br />
                        <Text type="secondary">Business metrics calculation</Text>
                      </div>
                      <Tag color="default">PENDIENTE</Tag>
                    </List.Item>
                    <List.Item>
                      <div>
                        <Text code>sp_gold_external</Text>
                        <br />
                        <Text type="secondary">External data integration</Text>
                      </div>
                      <Tag color="default">PENDIENTE</Tag>
                    </List.Item>
                  </List>
                </Card>
              </Col>
            </Row>
          </TabPane>
        </Tabs>
      </Card>

      {/* Modal de Logs Detallados */}
      <Modal
        title={
          <div>
            <DatabaseOutlined /> Logs Detallados - {selectedExecution?.component}
            <Tag color={getStatusColor(selectedExecution?.status || '')} style={{ marginLeft: 8 }}>
              {selectedExecution?.status?.toUpperCase()}
            </Tag>
          </div>
        }
        open={logsModalVisible}
        onCancel={() => setLogsModalVisible(false)}
        width={1000}
        footer={[
          <Button key="download" icon={<DownloadOutlined />}>
            Descargar Logs
          </Button>,
          <Button key="close" onClick={() => setLogsModalVisible(false)}>
            Cerrar
          </Button>
        ]}
      >
        {selectedExecution && (
          <div>
            {/* Información del Job */}
            <Card size="small" style={{ marginBottom: 16 }}>
              <Row gutter={16}>
                <Col span={6}>
                  <Statistic title="Inicio" value={selectedExecution.startTime} />
                </Col>
                <Col span={6}>
                  <Statistic title="Fin" value={selectedExecution.endTime} />
                </Col>
                <Col span={6}>
                  <Statistic title="Duración" value={selectedExecution.duration} />
                </Col>
                <Col span={6}>
                  <Statistic title="Registros" value={selectedExecution.recordsProcessed.toLocaleString()} />
                </Col>
              </Row>
              {selectedExecution.errorDetails && (
                <Alert
                  message="Error Detectado"
                  description={selectedExecution.errorDetails}
                  type="error"
                  showIcon
                  style={{ marginTop: 16 }}
                />
              )}
            </Card>

            {/* Timeline de Logs */}
            <div style={{ backgroundColor: '#001529', color: '#fff', padding: '16px', borderRadius: '4px', fontFamily: 'monospace', maxHeight: '400px', overflow: 'auto' }}>
              {selectedExecution.logs.map((log, index) => (
                <div key={index} style={{ marginBottom: '8px', padding: '4px 0' }}>
                  <span style={{ color: '#666' }}>[{log.timestamp}]</span>{' '}
                  <span style={{ color: getLogLevelColor(log.level), fontWeight: 'bold' }}>
                    [{log.level}]
                  </span>{' '}
                  <span style={{ color: '#faad14' }}>[{log.component}]</span>{' '}
                  <span>{log.message}</span>
                  {log.details && (
                    <div style={{ marginLeft: '20px', color: '#d9d9d9', fontSize: '12px' }}>
                      ↳ {log.details}
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
};

export default Monitoring;