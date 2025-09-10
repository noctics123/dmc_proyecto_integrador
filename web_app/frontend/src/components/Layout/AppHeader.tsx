import React from 'react';
import { Layout, Typography, Space, Button, Dropdown, Avatar } from 'antd';
import { UserOutlined, LogoutOutlined, BellOutlined } from '@ant-design/icons';
import type { MenuProps } from 'antd';

const { Header } = Layout;
const { Title } = Typography;

const AppHeader: React.FC = () => {
  const userMenuItems: MenuProps['items'] = [
    {
      key: 'profile',
      icon: <UserOutlined />,
      label: 'Profile',
    },
    {
      key: 'logout',
      icon: <LogoutOutlined />,
      label: 'Logout',
    },
  ];

  const handleUserMenuClick = (e: { key: string }) => {
    if (e.key === 'logout') {
      // Handle logout logic here
      console.log('Logout clicked');
    }
  };

  return (
    <Header
      style={{
        position: 'fixed',
        top: 0,
        zIndex: 1000,
        width: '100%',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        background: '#fff',
        borderBottom: '1px solid #f0f0f0',
        paddingLeft: '216px', // Account for sidebar width
        paddingRight: '24px',
      }}
    >
      <Title level={3} style={{ margin: 0, color: '#1890ff' }}>
        Data Pipeline Management
      </Title>
      
      <Space size="middle">
        <Button
          type="text"
          icon={<BellOutlined />}
          size="large"
          style={{ color: '#666' }}
        />
        
        <Dropdown
          menu={{ items: userMenuItems, onClick: handleUserMenuClick }}
          placement="bottomRight"
          trigger={['click']}
        >
          <Avatar
            size="large"
            icon={<UserOutlined />}
            style={{ cursor: 'pointer', backgroundColor: '#1890ff' }}
          />
        </Dropdown>
      </Space>
    </Header>
  );
};

export default AppHeader;