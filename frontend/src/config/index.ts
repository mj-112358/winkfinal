import { AppConfig } from '../types';

export const config: AppConfig = {
  apiBaseUrl: import.meta.env.VITE_API_URL || 'http://localhost:8000',
  useMockData: import.meta.env.VITE_USE_MOCK_DATA === 'true',
  refreshInterval: 5000, // 5 seconds
  maxRetries: 3,
};

export const endpoints = {
  cameras: '/api/cameras',
  zones: '/api/zones',
  zoneScreenshot: '/api/zones/screenshot',
  zoneOverlay: '/api/zones/overlay',
  metricsHourly: '/api/metrics/hourly',
  metricsDaily: '/api/metrics/daily',
  metricsDailyByCamera: '/api/metrics/daily_by_camera',
  analyticsComprehensive: '/api/analytics/comprehensive',
  analyticsSpikes: '/api/analytics/spikes',
  analyticsRealtime: '/api/analytics/realtime',
  analyticsAlerts: '/api/analytics/alerts',
  events: '/api/events',
  insights: '/api/insights/combined',
  zoneAnalytics: (cameraId: number) => `/api/zones/${cameraId}/analytics`,
  eventAnalyze: (eventId: number) => `/api/events/${eventId}/analyze`,
  alertResolve: (alertId: number) => `/api/analytics/alerts/${alertId}/resolve`,
} as const;