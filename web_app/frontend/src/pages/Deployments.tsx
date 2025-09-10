import React, { useState } from 'react';
import { 
  Card, 
  Row, 
  Col, 
  Button, 
  Progress, 
  Tag, 
  Space, 
  Modal, 
  Table, 
  Timeline,
  Divider,
  Alert,
  Tabs,
  Tooltip,
  Badge,
  List,
  Typography
} from 'antd';
import { 
  PlayCircleOutlined,
  ReloadOutlined,
  EyeOutlined,
  CloudOutlined,
  DatabaseOutlined,
  ApiOutlined,
  SettingOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
  ClockCircleOutlined,
  CodeOutlined,
  FileTextOutlined,
  BugOutlined
} from '@ant-design/icons';
import { usePipelineStatus } from '../hooks/usePipelineData';

const { Title, Text, Paragraph } = Typography;
const { TabPane } = Tabs;

interface DeploymentLayer {
  id: string;
  name: string;
  description: string;
  status: 'running' | 'completed' | 'failed' | 'pending';
  progress: number;
  services: DeploymentService[];
  lastDeployment?: string;
  estimatedTime?: string;
  icon: React.ReactNode;
  color: string;
}

interface DeploymentService {
  name: string;
  type: 'storage' | 'compute' | 'database' | 'scheduler' | 'build' | 'api' | 'script';
  status: 'healthy' | 'warning' | 'error' | 'deploying';
  description: string;
  details?: string;
}

const Deployments: React.FC = () => {
  const { data: pipelineStatus, isLoading } = usePipelineStatus();
  const [selectedDeployment, setSelectedDeployment] = useState<string | null>(null);
  const [logsModalVisible, setLogsModalVisible] = useState(false);
  const [activeLayer, setActiveLayer] = useState<string | null>(null);

  // Configuración de capas basada en la arquitectura real del proyecto
  const deploymentLayers: DeploymentLayer[] = [
    {
      id: 'landing',
      name: 'Capa Landing',
      description: 'Recolección y almacenamiento de datos externos (SIMBAD, Macroeconomía)',
      status: 'completed',
      progress: 100,
      lastDeployment: '2025-01-10 08:30:00',
      estimatedTime: '15-20 min',
      icon: <ApiOutlined />,
      color: '#52c41a',
      services: [
        { 
          name: 'Cloud Build - Landing Scrapers', 
          type: 'build', 
          status: 'healthy',
          description: 'Pipeline CI/CD para scrapers SIMBAD y Macroeconomía',
          details: 'Dockerfile + cloudbuild.yaml configurados'
        },
        { 
          name: 'Cloud Run - SIMBAD Scraper', 
          type: 'api', 
          status: 'healthy',
          description: 'Servicio de scraping de datos bancarios SIMBAD',
          details: 'Python + Selenium + scheduling mensual'
        },
        { 
          name: 'Cloud Run - Macro Scraper', 
          type: 'api', 
          status: 'healthy',
          description: 'Scraper de indicadores macroeconómicos',
          details: 'APIs IMF + BCRD para inflación y tipo de cambio'
        },
        { 
          name: 'Cloud Storage - Landing Bucket', 
          type: 'storage', 
          status: 'healthy',
          description: 'Almacenamiento de archivos CSV/JSON raw',
          details: 'gs://dae-integrador-2025/lakehouse/landing/'
        },
        { 
          name: 'Cloud Scheduler - Monthly Trigger', 
          type: 'scheduler', 
          status: 'healthy',
          description: 'Trigger automático día 20 de cada mes',
          details: 'Cron: 0 0 20 * *'
        },
        { 
          name: 'Trigger Cloud Build', 
          type: 'build', 
          status: 'healthy',
          description: 'Webhook para despliegues automáticos',
          details: 'GitHub integration + branch protection'
        }
      ]
    },
    {
      id: 'bronze',
      name: 'Capa Bronze',
      description: 'Procesamiento inicial y conversión a formato Parquet',
      status: 'running',
      progress: 75,
      lastDeployment: '2025-01-10 08:45:00',
      estimatedTime: '10-15 min',
      icon: <DatabaseOutlined />,
      color: '#cd7f32',
      services: [
        { 
          name: 'Cloud Storage - Bronze Layer', 
          type: 'storage', 
          status: 'healthy',
          description: 'Almacenamiento Parquet particionado por fecha',
          details: 'gs://dae-integrador-2025/lakehouse/bronze/'
        },
        { 
          name: 'DataProc - Bronze Processing', 
          type: 'compute', 
          status: 'deploying',
          description: 'Cluster Spark para procesar datos históricos',
          details: '1 master + 4 workers n2-standard-2'
        },
        { 
          name: 'BigQuery - External Tables', 
          type: 'database', 
          status: 'healthy',
          description: 'Tablas externas apuntando a archivos Parquet',
          details: 'simbad_bronze_parquet_ext + macro indicators'
        },
        { 
          name: 'PySpark Scripts', 
          type: 'script', 
          status: 'healthy',
          description: 'Scripts de transformación Landing -> Bronze',
          details: 'land_simbad_bronze.py + land_macroeconomics_bronze.py'
        },
        { 
          name: 'Cloud Scheduler - Incremental Load', 
          type: 'scheduler', 
          status: 'warning',
          description: 'Carga incremental post-landing',
          details: 'Triggered after landing completion'
        }
      ]
    },
    {
      id: 'silver',
      name: 'Capa Silver',
      description: 'Limpieza, deduplicación y normalización de datos',
      status: 'pending',
      progress: 0,
      lastDeployment: '2025-01-10 09:00:00',
      estimatedTime: '8-12 min',
      icon: <SettingOutlined />,
      color: '#c0c0c0',
      services: [
        { 
          name: 'BigQuery - Silver Dataset', 
          type: 'database', 
          status: 'healthy',
          description: 'Dataset limpio y normalizado',
          details: 'silver_clean.simbad_hipotecarios'
        },
        { 
          name: 'Stored Procedure - sp_silver_merge', 
          type: 'script', 
          status: 'healthy',
          description: 'MERGE incremental Bronze -> Silver',
          details: 'MERGE + DEDUPE + normalization'
        },
        { 
          name: 'BigQuery Scheduled Query', 
          type: 'scheduler', 
          status: 'pending',
          description: 'Ejecución automática del SP cada hora',
          details: 'Triggered by Bronze completion'
        },
        { 
          name: 'Data Quality Checks', 
          type: 'script', 
          status: 'warning',
          description: 'Validaciones de calidad de datos',
          details: 'Completeness, accuracy, schema validation'
        }
      ]
    },
    {
      id: 'gold',
      name: 'Capa Gold',
      description: 'Métricas de negocio e indicadores analíticos finales',
      status: 'pending',
      progress: 0,
      lastDeployment: null,
      estimatedTime: '5-8 min',
      icon: <CloudOutlined />,
      color: '#ffd700',
      services: [
        { 
          name: 'BigQuery - Gold Dataset', 
          type: 'database', 
          status: 'pending',
          description: 'Métricas finales para BI y reportes',
          details: 'gold.simbad_gold con indicadores calculados'
        },
        { 
          name: 'Stored Procedure - sp_gold_indicators', 
          type: 'script', 
          status: 'pending',
          description: 'Cálculo de métricas de riesgo y negocio',
          details: 'Tasa mora, cobertura garantía, PD agregada'
        },
        { 
          name: 'BigQuery Scheduled Query', 
          type: 'scheduler', 
          status: 'pending',
          description: 'Actualización diaria de métricas Gold',
          details: 'Cron diario post Silver completion'
        },
        { 
          name: 'External Data Integration', 
          type: 'api', 
          status: 'pending',
          description: 'Integración con inflación y tipo de cambio',
          details: 'JOIN con bronze_inflacion_12m_ext + bronze_tipo_cambio_ext'
        }
      ]
    }
  ];

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed': return 'success';
      case 'running': return 'processing';
      case 'failed': return 'error';
      default: return 'default';
    }
  };

  const getServiceIcon = (type: string) => {
    switch (type) {
      case 'storage': return <CloudOutlined />;
      case 'compute': return <SettingOutlined />;
      case 'database': return <DatabaseOutlined />;
      case 'scheduler': return <ClockCircleOutlined />;
      case 'build': return <CodeOutlined />;
      case 'api': return <ApiOutlined />;
      case 'script': return <FileTextOutlined />;
      default: return <SettingOutlined />;
    }
  };

  const getServiceStatusColor = (status: string) => {
    switch (status) {
      case 'healthy': return 'green';
      case 'warning': return 'orange';
      case 'error': return 'red';
      case 'deploying': return 'blue';
      default: return 'default';
    }
  };

  const handleDeploy = (layerId: string) => {
    setActiveLayer(layerId);
    // Aquí iría la lógica real de despliegue
    Modal.info({
      title: 'Iniciar Despliegue',
      content: `¿Confirmar despliegue de la capa ${layerId}?`,
      onOk: () => {
        console.log(`Deploying layer: ${layerId}`);
      }
    });
  };

  const showLogs = (layerId: string) => {
    setSelectedDeployment(layerId);
    setLogsModalVisible(true);
  };

  const mockLogs = [
    { time: '10:30:15', level: 'INFO', message: 'Iniciando despliegue de servicios...' },
    { time: '10:30:16', level: 'INFO', message: 'Verificando configuración Cloud Build...' },
    { time: '10:30:18', level: 'INFO', message: 'Construyendo imagen Docker...' },
    { time: '10:30:35', level: 'SUCCESS', message: 'Imagen construida exitosamente' },
    { time: '10:30:36', level: 'INFO', message: 'Desplegando a Cloud Run...' },
    { time: '10:30:45', level: 'WARNING', message: 'Tiempo de respuesta más lento de lo esperado' },
    { time: '10:30:50', level: 'INFO', message: 'Servicio desplegado correctamente' },
    { time: '10:30:51', level: 'INFO', message: 'Ejecutando pruebas de salud...' },
    { time: '10:30:55', level: 'SUCCESS', message: 'Todas las pruebas pasaron ✓' },
  ];

  return (
    <div style={{ padding: '0 24px' }}>
      <Row gutter={[0, 16]}>
        <Col span={24}>
          <Card>
            <Title level={3} style={{ margin: 0 }}>
              🚀 Pipeline de Despliegues por Capas
            </Title>
            <Paragraph type="secondary" style={{ marginTop: 8 }}>
              Gestión y monitoreo de despliegues organizados por capas de la arquitectura de datos
            </Paragraph>
            
            <Alert
              message="Arquitectura Lakehouse - Landing → Bronze → Silver → Gold"
              description="Cada capa incluye múltiples servicios GCP coordinados para el procesamiento de datos SIMBAD y macroeconómicos"
              type="info"
              showIcon
              style={{ marginTop: 16 }}
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
        {deploymentLayers.map((layer) => (
          <Col span={12} key={layer.id}>
            <Card
              title={
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span style={{ color: layer.color, fontSize: '18px' }}>
                    {layer.icon}
                  </span>
                  <span>{layer.name}</span>
                  <Tag color={getStatusColor(layer.status)}>
                    {layer.status.toUpperCase()}
                  </Tag>
                </div>
              }
              extra={
                <Space>
                  <Button 
                    type="primary" 
                    icon={<PlayCircleOutlined />} 
                    size="small"
                    onClick={() => handleDeploy(layer.id)}
                    disabled={layer.status === 'running'}
                  >
                    Desplegar
                  </Button>
                  <Button 
                    icon={<EyeOutlined />} 
                    size="small"
                    onClick={() => showLogs(layer.id)}
                  >
                    Logs
                  </Button>
                </Space>
              }
              style={{ minHeight: '400px' }}
            >
              <Paragraph type="secondary" style={{ marginBottom: 16 }}>
                {layer.description}
              </Paragraph>

              {layer.progress > 0 && (
                <div style={{ marginBottom: 16 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                    <Text strong>Progreso del Despliegue</Text>
                    <Text>{layer.progress}%</Text>
                  </div>
                  <Progress 
                    percent={layer.progress} 
                    status={layer.status === 'running' ? 'active' : layer.status === 'failed' ? 'exception' : 'success'}
                    strokeColor={layer.color}
                  />
                </div>
              )}

              <Divider orientation="left" orientationMargin="0">
                <Text strong>Servicios ({layer.services.length})</Text>
              </Divider>

              <List
                size="small"
                dataSource={layer.services}
                renderItem={(service) => (
                  <List.Item
                    actions={[
                      <Tooltip title={service.details}>
                        <Button type="text" icon={<EyeOutlined />} size="small" />
                      </Tooltip>
                    ]}
                  >
                    <List.Item.Meta
                      avatar={
                        <Badge 
                          status={getServiceStatusColor(service.status) as any}
                          dot
                        >
                          {getServiceIcon(service.type)}
                        </Badge>
                      }
                      title={service.name}
                      description={service.description}
                    />
                  </List.Item>
                )}
              />

              <Divider />
              
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '12px', color: '#666' }}>
                <span>
                  <ClockCircleOutlined /> {layer.estimatedTime}
                </span>
                <span>
                  {layer.lastDeployment ? `Último: ${layer.lastDeployment}` : 'Nunca desplegado'}
                </span>
              </div>
            </Card>
          </Col>
        ))}
      </Row>

      {/* Modal de Logs */}
      <Modal
        title={`Logs de Despliegue - ${selectedDeployment?.toUpperCase()}`}
        open={logsModalVisible}
        onCancel={() => setLogsModalVisible(false)}
        width={800}
        footer={[
          <Button key="close" onClick={() => setLogsModalVisible(false)}>
            Cerrar
          </Button>
        ]}
      >
        <div style={{ backgroundColor: '#001529', color: '#fff', padding: '16px', borderRadius: '4px', fontFamily: 'monospace' }}>
          {mockLogs.map((log, index) => (
            <div key={index} style={{ marginBottom: '4px' }}>
              <span style={{ color: '#666' }}>[{log.time}]</span>{' '}
              <span style={{ 
                color: log.level === 'ERROR' ? '#ff4d4f' : 
                      log.level === 'WARNING' ? '#faad14' : 
                      log.level === 'SUCCESS' ? '#52c41a' : '#1890ff' 
              }}>
                [{log.level}]
              </span>{' '}
              {log.message}
            </div>
          ))}
        </div>
      </Modal>
    </div>
  );
};

export default Deployments;