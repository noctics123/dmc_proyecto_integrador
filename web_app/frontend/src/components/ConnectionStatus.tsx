import React from 'react';
import { Space, Tag } from 'antd';
import { 
  WifiOutlined, 
  DisconnectOutlined, 
  SyncOutlined,
  ClockCircleOutlined 
} from '@ant-design/icons';

interface ConnectionStatusProps {
  isOnline?: boolean;
  isFetching?: boolean;
  lastUpdated?: Date;
  error?: any;
}

const ConnectionStatus: React.FC<ConnectionStatusProps> = ({
  isOnline = true,
  isFetching = false,
  lastUpdated,
  error
}) => {
  const getStatus = () => {
    if (error) {
      return {
        icon: <DisconnectOutlined />,
        color: 'red',
        text: 'Desconectado',
        description: 'Error de conexión con el servidor'
      };
    }
    
    if (isFetching) {
      return {
        icon: <SyncOutlined spin />,
        color: 'blue',
        text: 'Actualizando...',
        description: 'Obteniendo datos del servidor'
      };
    }
    
    if (isOnline) {
      return {
        icon: <WifiOutlined />,
        color: 'green',
        text: 'Conectado',
        description: 'Datos actualizados'
      };
    }
    
    return {
      icon: <DisconnectOutlined />,
      color: 'default',
      text: 'Sin conexión',
      description: 'Verificando conexión...'
    };
  };

  const status = getStatus();
  
  return (
    <Space size="small">
      <Tag 
        icon={status.icon} 
        color={status.color}
        style={{ 
          fontSize: '11px',
          margin: 0,
          padding: '2px 8px',
          borderRadius: '4px'
        }}
      >
        {status.text}
      </Tag>
      {lastUpdated && !error && (
        <span style={{ fontSize: '11px', color: '#666' }}>
          <ClockCircleOutlined style={{ marginRight: '2px' }} />
          {lastUpdated.toLocaleTimeString()}
        </span>
      )}
    </Space>
  );
};

export default ConnectionStatus;