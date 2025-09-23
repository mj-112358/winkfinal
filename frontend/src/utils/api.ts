import { config, endpoints } from '../config';
import type { 
  Camera, Zone, HourlyMetric, DailyMetric, LiveMetric, 
  Spike, Event, Alert, InsightRequest, ZoneAnalytics 
} from '../types';

class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
    this.name = 'ApiError';
  }
}

async function makeRequest<T>(
  endpoint: string, 
  options: RequestInit = {}
): Promise<T> {
  const url = `${config.apiBaseUrl}${endpoint}`;
  
  try {
    const response = await fetch(url, {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    });

    if (!response.ok) {
      throw new ApiError(response.status, `API Error: ${response.statusText}`);
    }

    return await response.json();
  } catch (error) {
    if (error instanceof ApiError) throw error;
    throw new Error(`Network error: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

export const api = {
  // Cameras
  getCameras: (): Promise<Camera[]> => 
    makeRequest(endpoints.cameras),
  
  createCamera: (camera: Omit<Camera, 'id'>): Promise<{ id: number }> =>
    makeRequest(endpoints.cameras, {
      method: 'POST',
      body: JSON.stringify(camera),
    }),
  
  deleteCamera: (id: number): Promise<{ status: string }> =>
    makeRequest(`${endpoints.cameras}/${id}`, { method: 'DELETE' }),

  // Zones
  getZones: (cameraId?: number): Promise<{ screenshot: any; zones: Zone[] }> =>
    makeRequest(`${endpoints.zones}${cameraId ? `?camera_id=${cameraId}` : ''}`),
  
  uploadZoneScreenshot: (
    cameraId: number, 
    file: File, 
    width: number, 
    height: number
  ): Promise<{ status: string; path: string }> => {
    const formData = new FormData();
    formData.append('camera_id', cameraId.toString());
    formData.append('file', file);
    formData.append('img_width', width.toString());
    formData.append('img_height', height.toString());
    
    return makeRequest(endpoints.zoneScreenshot, {
      method: 'POST',
      body: formData,
      headers: {}, // Let browser set Content-Type for FormData
    });
  },
  
  createZone: (
    cameraId: number,
    name: string,
    ztype: string,
    polygon: Array<[number, number]>
  ): Promise<{ id: number }> => {
    const formData = new FormData();
    formData.append('camera_id', cameraId.toString());
    formData.append('name', name);
    formData.append('ztype', ztype);
    formData.append('polygon_json', JSON.stringify(polygon));
    
    return makeRequest(endpoints.zones, {
      method: 'POST',
      body: formData,
      headers: {},
    });
  },
  
  deleteZone: (id: number): Promise<{ status: string }> =>
    makeRequest(`${endpoints.zones}/${id}`, { method: 'DELETE' }),
  
  getZoneOverlay: (cameraId: number): string =>
    `${config.apiBaseUrl}${endpoints.zoneOverlay}?camera_id=${cameraId}`,

  // Metrics
  getHourlyMetrics: (start: string, end: string): Promise<HourlyMetric[]> =>
    makeRequest(`${endpoints.metricsHourly}?start=${start}&end=${end}`),
  
  getDailyMetrics: (days: number = 7): Promise<DailyMetric[]> =>
    makeRequest(`${endpoints.metricsDaily}?days=${days}`),
  
  getDailyMetricsByCamera: (days: number = 7): Promise<Record<string, any[]>> =>
    makeRequest(`${endpoints.metricsDailyByCamera}?days=${days}`),

  // Analytics
  getComprehensiveAnalytics: (days: number = 30): Promise<any> =>
    makeRequest(endpoints.analyticsComprehensive, {
      method: 'POST',
      body: JSON.stringify({ days, include_zones: true, include_trends: true }),
    }),
  
  getSpikeAnalysis: (date?: string): Promise<{
    date: string;
    spikes_detected: number;
    spikes: Spike[];
    baselines: any;
    analysis_summary: any;
  }> => {
    const params = date ? `?date=${date}` : '';
    return makeRequest(`${endpoints.analyticsSpikes}${params}`);
  },
  
  getRealtimeMetrics: (): Promise<{
    timestamp: string;
    live_metrics: Record<string, LiveMetric>;
    total_live_count: number;
  }> => makeRequest(endpoints.analyticsRealtime),
  
  getZoneAnalytics: (cameraId: number, days: number = 7): Promise<ZoneAnalytics> =>
    makeRequest(endpoints.zoneAnalytics(cameraId) + `?days=${days}`),

  // Events
  getEvents: (): Promise<Event[]> =>
    makeRequest(endpoints.events),
  
  createEvent: (event: Omit<Event, 'id' | 'created_at'>): Promise<{ id: number; status: string }> =>
    makeRequest(endpoints.events, {
      method: 'POST',
      body: JSON.stringify(event),
    }),
  
  analyzeEvent: (eventId: number): Promise<any> =>
    makeRequest(endpoints.eventAnalyze(eventId), { method: 'POST' }),

  // Alerts
  getAlerts: (): Promise<{ alerts: Alert[]; alert_count: number }> =>
    makeRequest(endpoints.analyticsAlerts),
  
  resolveAlert: (alertId: number): Promise<{ status: string }> =>
    makeRequest(endpoints.alertResolve(alertId), { method: 'POST' }),

  // Insights
  getInsights: (request: InsightRequest): Promise<any> =>
    makeRequest(endpoints.insights, {
      method: 'POST',
      body: JSON.stringify(request),
    }),
};

export { ApiError };