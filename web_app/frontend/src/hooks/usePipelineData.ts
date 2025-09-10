// Custom hooks for pipeline data fetching
import { useQuery } from '@tanstack/react-query';
import { pipelineAPI, monitoringAPI } from '../services/api';

export const usePipelineStatus = () => {
  return useQuery({
    queryKey: ['pipelineStatus'],
    queryFn: pipelineAPI.getAllStatus,
    refetchInterval: 30000, // Refetch every 30 seconds
    staleTime: 10000, // Data is fresh for 10 seconds
  });
};

export const useComponentStatus = (component: string) => {
  return useQuery({
    queryKey: ['componentStatus', component],
    queryFn: () => pipelineAPI.getComponentStatus(component),
    refetchInterval: 15000,
    enabled: !!component,
  });
};

export const useDataQuality = () => {
  return useQuery({
    queryKey: ['dataQuality'],
    queryFn: monitoringAPI.getDataQuality,
    refetchInterval: 60000, // Refetch every minute
    staleTime: 30000,
  });
};

export const useCostMetrics = () => {
  return useQuery({
    queryKey: ['costMetrics'],
    queryFn: monitoringAPI.getCostMetrics,
    refetchInterval: 300000, // Refetch every 5 minutes
    staleTime: 120000, // Data is fresh for 2 minutes
  });
};

export const usePerformanceMetrics = () => {
  return useQuery({
    queryKey: ['performanceMetrics'],
    queryFn: monitoringAPI.getPerformanceMetrics,
    refetchInterval: 60000,
    staleTime: 30000,
  });
};