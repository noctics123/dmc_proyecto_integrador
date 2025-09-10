import React from 'react';
import { Layout, Menu } from 'antd';
import { useNavigate, useLocation } from 'react-router-dom';
import {
  DashboardOutlined,
  DatabaseOutlined,
  DeploymentUnitOutlined,
  MonitorOutlined,
  SettingOutlined,
  BarChartOutlined,
  ApiOutlined,
} from '@ant-design/icons';

const { Sider } = Layout;

const AppSidebar: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();

  const menuItems = [
    {
      key: '/dashboard',
      icon: <DashboardOutlined />,
      label: 'Dashboard',
    },
    {
      key: '/pipeline',
      icon: <ApiOutlined />,
      label: 'Pipeline Status',
    },
    {
      key: '/deployments',
      icon: <DeploymentUnitOutlined />,
      label: 'Deployments',
    },
    {
      key: '/data-quality',
      icon: <DatabaseOutlined />,
      label: 'Data Quality',
    },
    {
      key: '/monitoring',
      icon: <MonitorOutlined />,
      label: 'Monitoring',
    },
    {
      key: '/settings',
      icon: <SettingOutlined />,
      label: 'Settings',
    },
  ];

  const handleMenuClick = (e: { key: string }) => {
    navigate(e.key);
  };

  return (
    <Sider
      width={200}
      style={{
        overflow: 'auto',
        height: '100vh',
        position: 'fixed',
        left: 0,
      }}
    >
      <div
        style={{
          height: '32px',
          background: 'rgba(255, 255, 255, 0.3)',
          margin: '16px',
          borderRadius: '4px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          color: 'white',
          fontWeight: 'bold',
        }}
      >
        DMC Pipeline
      </div>
      <Menu
        mode="inline"
        selectedKeys={[location.pathname]}
        style={{ height: '100%', borderRight: 0 }}
        theme="dark"
        items={menuItems}
        onClick={handleMenuClick}
      />
    </Sider>
  );
};

export default AppSidebar;