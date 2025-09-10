// Custom hooks for pipeline data fetching
import { useQuery } from '@tanstack/react-query';
import { pipelineAPI, monitoringAPI } from '../services/api';

export const usePipelineStatus = () => {
  return useQuery({
    queryKey: ['pipelineStatus'],
    queryFn: pipelineAPI.getAllStatus,
    refetchInterval: 15000, // Refetch every 15 seconds for real-time feel
    staleTime: 5000, // Data is fresh for 5 seconds
    refetchIntervalInBackground: true, // Continue polling when tab is not focused
    retry: (failureCount, error) => {
      // Retry up to 3 times, with exponential backoff
      return failureCount < 3;
    },
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 30000), // Exponential backoff
  });
};

export const useComponentStatus = (component: string) => {
  return useQuery({
    queryKey: ['componentStatus', component],
    queryFn: () => pipelineAPI.getComponentStatus(component),
    refetchInterval: 10000, // More frequent updates for individual components
    enabled: !!component,
    refetchIntervalInBackground: true,
    retry: 2,
  });
};

export const useDataQuality = () => {
  return useQuery({
    queryKey: ['dataQuality'],
    queryFn: monitoringAPI.getDataQuality,
    refetchInterval: 30000, // Every 30 seconds instead of 60
    staleTime: 15000, // Fresh for 15 seconds
    refetchIntervalInBackground: true,
    retry: 2,
  });
};

export const useCostMetrics = () => {
  return useQuery({
    queryKey: ['costMetrics'],
    queryFn: monitoringAPI.getCostMetrics,
    refetchInterval: 120000, // Every 2 minutes instead of 5
    staleTime: 60000, // Fresh for 1 minute
    refetchIntervalInBackground: true,
    retry: 2,
  });
};

export const usePerformanceMetrics = () => {
  return useQuery({
    queryKey: ['performanceMetrics'],
    queryFn: monitoringAPI.getPerformanceMetrics,
    refetchInterval: 30000, // Every 30 seconds
    staleTime: 15000, // Fresh for 15 seconds
    refetchIntervalInBackground: true,
    retry: 2,
  });
};

// Hook para seguimiento de progreso de trabajos en tiempo real
export const useJobProgress = (jobId: string, enabled = true) => {
  return useQuery({
    queryKey: ['jobProgress', jobId],
    queryFn: () => monitoringAPI.getJobProgress(jobId),
    refetchInterval: 5000, // Every 5 seconds for real-time feel
    staleTime: 1000, // Data is fresh for only 1 second
    refetchIntervalInBackground: true,
    enabled: enabled && !!jobId,
    retry: 1,
  });
};

// Hook para obtener logs de despliegue en tiempo real
export const useDeploymentLogs = (jobId: string, enabled = true) => {
  return useQuery({
    queryKey: ['deploymentLogs', jobId],
    queryFn: () => monitoringAPI.getDeploymentLogs(jobId, 1000),
    refetchInterval: 3000, // Every 3 seconds for logs
    staleTime: 500, // Very fresh data for logs
    refetchIntervalInBackground: true,
    enabled: enabled && !!jobId,
    retry: 1,
  });
};