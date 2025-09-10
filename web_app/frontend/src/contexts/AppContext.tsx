import React, { createContext, useContext, useReducer, ReactNode } from 'react';

interface DeploymentState {
  activeLayer: string | null;
  deployments: {
    [key: string]: {
      status: 'running' | 'completed' | 'failed' | 'pending';
      progress: number;
      logs: string[];
      startTime?: string;
      endTime?: string;
    };
  };
}

interface AppState {
  deployments: DeploymentState;
  lastRefresh: number;
  realTimeMode: boolean;
}

type AppAction = 
  | { type: 'START_DEPLOYMENT'; payload: { layerId: string } }
  | { type: 'UPDATE_DEPLOYMENT_PROGRESS'; payload: { layerId: string; progress: number; logs?: string[] } }
  | { type: 'COMPLETE_DEPLOYMENT'; payload: { layerId: string; success: boolean } }
  | { type: 'SET_ACTIVE_LAYER'; payload: string | null }
  | { type: 'FORCE_REFRESH' }
  | { type: 'TOGGLE_REAL_TIME' };

const initialState: AppState = {
  deployments: {
    activeLayer: null,
    deployments: {}
  },
  lastRefresh: Date.now(),
  realTimeMode: true
};

function appReducer(state: AppState, action: AppAction): AppState {
  switch (action.type) {
    case 'START_DEPLOYMENT':
      return {
        ...state,
        deployments: {
          ...state.deployments,
          activeLayer: action.payload.layerId,
          deployments: {
            ...state.deployments.deployments,
            [action.payload.layerId]: {
              status: 'running',
              progress: 0,
              logs: [`[${new Date().toLocaleTimeString()}] Iniciando despliegue de ${action.payload.layerId}...`],
              startTime: new Date().toISOString()
            }
          }
        }
      };
    
    case 'UPDATE_DEPLOYMENT_PROGRESS':
      const currentDeployment = state.deployments.deployments[action.payload.layerId];
      return {
        ...state,
        deployments: {
          ...state.deployments,
          deployments: {
            ...state.deployments.deployments,
            [action.payload.layerId]: {
              ...currentDeployment,
              progress: action.payload.progress,
              logs: action.payload.logs ? [...(currentDeployment?.logs || []), ...action.payload.logs] : currentDeployment?.logs || []
            }
          }
        }
      };
    
    case 'COMPLETE_DEPLOYMENT':
      return {
        ...state,
        deployments: {
          ...state.deployments,
          deployments: {
            ...state.deployments.deployments,
            [action.payload.layerId]: {
              ...state.deployments.deployments[action.payload.layerId],
              status: action.payload.success ? 'completed' : 'failed',
              progress: action.payload.success ? 100 : state.deployments.deployments[action.payload.layerId]?.progress || 0,
              endTime: new Date().toISOString(),
              logs: [
                ...(state.deployments.deployments[action.payload.layerId]?.logs || []),
                `[${new Date().toLocaleTimeString()}] Despliegue ${action.payload.success ? 'completado' : 'fallido'}`
              ]
            }
          }
        }
      };
    
    case 'SET_ACTIVE_LAYER':
      return {
        ...state,
        deployments: {
          ...state.deployments,
          activeLayer: action.payload
        }
      };
    
    case 'FORCE_REFRESH':
      return {
        ...state,
        lastRefresh: Date.now()
      };
    
    case 'TOGGLE_REAL_TIME':
      return {
        ...state,
        realTimeMode: !state.realTimeMode
      };
    
    default:
      return state;
  }
}

const AppContext = createContext<{
  state: AppState;
  dispatch: React.Dispatch<AppAction>;
} | null>(null);

export function AppProvider({ children }: { children: ReactNode }) {
  const [state, dispatch] = useReducer(appReducer, initialState);

  return (
    <AppContext.Provider value={{ state, dispatch }}>
      {children}
    </AppContext.Provider>
  );
}

export function useAppContext() {
  const context = useContext(AppContext);
  if (!context) {
    throw new Error('useAppContext must be used within AppProvider');
  }
  return context;
}

export default AppContext;