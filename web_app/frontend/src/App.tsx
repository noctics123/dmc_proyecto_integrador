import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { Layout, ConfigProvider } from 'antd';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ReactQueryDevtools } from '@tanstack/react-query-devtools';

import AppHeader from './components/Layout/AppHeader';
import AppSidebar from './components/Layout/AppSidebar';
import Dashboard from './pages/Dashboard';
import PipelineStatus from './pages/PipelineStatus';
import Deployments from './pages/Deployments';
import DataQuality from './pages/DataQuality';
import Monitoring from './pages/Monitoring';
import Settings from './pages/Settings';

import './App.css';

const { Content } = Layout;

// Create a client optimized for real-time updates
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: true, // Refetch when user returns to tab
      retry: 2,
      staleTime: 30 * 1000, // 30 seconds - data is fresh for 30 seconds
      cacheTime: 2 * 60 * 1000, // 2 minutes - keep in cache for 2 minutes
      refetchOnReconnect: true, // Refetch when connection is restored
      notifyOnChangeProps: ['data', 'error', 'isLoading'], // Only re-render when these change
    },
  },
});

function App() {
  return (
    <ConfigProvider
      theme={{
        token: {
          colorPrimary: '#1890ff',
        },
      }}
    >
      <QueryClientProvider client={queryClient}>
        <Router>
          <Layout style={{ minHeight: '100vh' }}>
            <AppSidebar />
            <Layout>
              <AppHeader />
              <Content style={{ margin: '24px 16px 0', overflow: 'initial' }}>
                <div style={{ padding: 24, background: '#fff', minHeight: 360 }}>
                  <Routes>
                    <Route path="/" element={<Navigate to="/dashboard" />} />
                    <Route path="/dashboard" element={<Dashboard />} />
                    <Route path="/pipeline" element={<PipelineStatus />} />
                    <Route path="/deployments" element={<Deployments />} />
                    <Route path="/data-quality" element={<DataQuality />} />
                    <Route path="/monitoring" element={<Monitoring />} />
                    <Route path="/settings" element={<Settings />} />
                  </Routes>
                </div>
              </Content>
            </Layout>
          </Layout>
        </Router>
        <ReactQueryDevtools initialIsOpen={false} />
      </QueryClientProvider>
    </ConfigProvider>
  );
}

export default App;