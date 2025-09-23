import { 
  Camera, Zone, HourlyMetric, DailyMetric, LiveMetric, 
  Spike, Event, Alert, ZoneAnalytics 
} from '../types';

// Generate realistic mock data by store
export const mockCameras: Camera[] = [
  // Store 1 - Main Store Location
  { id: 1, name: 'Main Entrance Camera', rtsp_url: 'rtsp://192.168.1.100:554/stream1', enabled: true, store_id: 1 },
  { id: 2, name: 'Checkout Area A', rtsp_url: 'rtsp://192.168.1.101:554/stream1', enabled: true, store_id: 1 },
  { id: 3, name: 'Electronics Section', rtsp_url: 'rtsp://192.168.1.102:554/stream1', enabled: true, store_id: 1 },
  { id: 4, name: 'Grocery Aisle 1-3', rtsp_url: 'rtsp://192.168.1.103:554/stream1', enabled: true, store_id: 1 },
  
  // Store 2 - North Mall Branch
  { id: 5, name: 'Mall Entrance', rtsp_url: 'rtsp://192.168.2.100:554/stream1', enabled: true, store_id: 2 },
  { id: 6, name: 'Checkout Area B', rtsp_url: 'rtsp://192.168.2.101:554/stream1', enabled: true, store_id: 2 },
  { id: 7, name: 'Fashion Section', rtsp_url: 'rtsp://192.168.2.102:554/stream1', enabled: false, store_id: 2 },
  
  // Store 3 - Airport Terminal Store
  { id: 8, name: 'Terminal Entrance', rtsp_url: 'rtsp://192.168.3.100:554/stream1', enabled: true, store_id: 3 },
  { id: 9, name: 'Travel Essentials', rtsp_url: 'rtsp://192.168.3.101:554/stream1', enabled: true, store_id: 3 },
  
  // Store 4 - West Side Outlet (inactive)
  { id: 10, name: 'Outlet Entrance', rtsp_url: 'rtsp://192.168.4.100:554/stream1', enabled: false, store_id: 4 },
];

export const mockZones: Zone[] = [
  { id: 1, name: 'Main Entrance', ztype: 'entry', polygon: [[100, 100], [400, 100], [400, 300], [100, 300]] },
  { id: 2, name: 'Checkout Queue', ztype: 'queue', polygon: [[450, 150], [600, 150], [600, 350], [450, 350]] },
  { id: 3, name: 'Electronics Shelf', ztype: 'shelf', polygon: [[200, 400], [500, 400], [500, 600], [200, 600]] },
  { id: 4, name: 'Exit Area', ztype: 'entry', polygon: [[650, 100], [800, 100], [800, 300], [650, 300]] },
];

export const generateMockHourlyMetrics = (hours: number = 24): HourlyMetric[] => {
  const metrics: HourlyMetric[] = [];
  const now = new Date();
  
  for (let i = hours; i >= 0; i--) {
    const hour = new Date(now.getTime() - i * 60 * 60 * 1000);
    const hourStr = hour.toISOString().slice(0, 13) + ':00:00';
    
    // Generate realistic patterns (higher traffic during business hours)
    const isPeakHour = hour.getHours() >= 10 && hour.getHours() <= 20;
    const baseFootfall = isPeakHour ? 15 + Math.random() * 25 : 2 + Math.random() * 8;
    
    mockCameras.forEach(camera => {
      if (camera.enabled) {
        metrics.push({
          camera_id: camera.id,
          hour_start: hourStr,
          footfall: Math.floor(baseFootfall + Math.random() * 10),
          unique_visitors: Math.floor(baseFootfall * 0.8),
          dwell_avg: 45 + Math.random() * 60,
          dwell_p95: 120 + Math.random() * 180,
          queue_wait_avg: 30 + Math.random() * 90,
          interactions: Math.floor(baseFootfall * 0.3),
          zones: {
            'Main Entrance': Math.floor(baseFootfall * 0.9),
            'Checkout Queue': Math.floor(baseFootfall * 0.4),
            'Electronics Shelf': Math.floor(baseFootfall * 0.2),
          },
        });
      }
    });
  }
  
  return metrics;
};

export const generateMockDailyMetrics = (days: number = 7): DailyMetric[] => {
  const metrics: DailyMetric[] = [];
  const now = new Date();
  
  for (let i = days; i >= 0; i--) {
    const date = new Date(now.getTime() - i * 24 * 60 * 60 * 1000);
    const dateStr = date.toISOString().slice(0, 10);
    
    // Weekend vs weekday patterns
    const isWeekend = date.getDay() === 0 || date.getDay() === 6;
    const baseFootfall = isWeekend ? 180 + Math.random() * 120 : 250 + Math.random() * 150;
    
    metrics.push({
      date: dateStr,
      total_footfall: Math.floor(baseFootfall),
      unique_visitors: Math.floor(baseFootfall * 0.85),
      dwell_avg: 52 + Math.random() * 30,
      dwell_p95: 145 + Math.random() * 60,
      queue_wait_avg: 35 + Math.random() * 40,
      interactions: Math.floor(baseFootfall * 0.25),
      peak_hour: `${9 + Math.floor(Math.random() * 8)}:00:00`,
      peak_footfall: Math.floor(baseFootfall * 0.15),
      conversion_rate: 0.15 + Math.random() * 0.2,
      avg_visit_duration: 45 + Math.random() * 30,
    });
  }
  
  return metrics.reverse();
};

export const generateMockLiveMetrics = (): Record<string, LiveMetric> => {
  const liveMetrics: Record<string, LiveMetric> = {};
  
  mockCameras.forEach(camera => {
    if (camera.enabled) {
      liveMetrics[camera.id.toString()] = {
        camera_id: camera.id.toString(),
        camera_name: camera.name,
        live_count: Math.floor(Math.random() * 15),
        last_updated: new Date().toISOString(),
      };
    }
  });
  
  return liveMetrics;
};

export const generateMockSpikes = (): Spike[] => {
  const spikes: Spike[] = [];
  const now = new Date();
  
  // Generate a few random spikes for today
  for (let i = 0; i < 3; i++) {
    const hour = new Date(now.getTime() - Math.random() * 12 * 60 * 60 * 1000);
    spikes.push({
      type: ['footfall_spike', 'interaction_spike', 'dwell_anomaly'][Math.floor(Math.random() * 3)],
      severity: ['medium', 'high', 'critical'][Math.floor(Math.random() * 3)] as 'medium' | 'high' | 'critical',
      hour_start: hour.toISOString().slice(0, 13) + ':00:00',
      camera_id: Math.floor(Math.random() * 4) + 1,
      value: 45 + Math.random() * 55,
      baseline_mean: 25,
      baseline_std: 8,
      spike_magnitude: 1.8 + Math.random() * 1.2,
      description: 'Unusual activity detected in store area',
    });
  }
  
  return spikes;
};

export const generateMockEvents = (): Event[] => [
  {
    id: 1,
    name: 'Black Friday Sale',
    event_type: 'promotion',
    start_date: '2024-11-29',
    end_date: '2024-11-29',
    description: 'Major discount event with up to 70% off',
    created_at: '2024-11-20T10:00:00Z',
  },
  {
    id: 2,
    name: 'Diwali Festival',
    event_type: 'festival',
    start_date: '2024-11-12',
    end_date: '2024-11-15',
    description: 'Festival of lights celebration with special offers',
    created_at: '2024-11-01T10:00:00Z',
  },
];

export const generateMockAlerts = (): Alert[] => [
  {
    id: 1,
    alert_type: 'camera_offline',
    severity: 'warning',
    message: 'Electronics Section camera has been offline for 15 minutes',
    created_at: new Date(Date.now() - 15 * 60 * 1000).toISOString(),
  },
  {
    id: 2,
    alert_type: 'unusual_activity',
    severity: 'info',
    message: 'Footfall spike detected in entrance area - 200% above normal',
    created_at: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(),
  },
];

export const generateMockZoneAnalytics = (cameraId: number): ZoneAnalytics => ({
  camera_id: cameraId,
  analysis_period: {
    start: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10),
    end: new Date().toISOString().slice(0, 10),
  },
  total_hours_analyzed: 168,
  zone_performance: {
    'Main Entrance': {
      total_visits: 1250,
      average_hourly: 7.4,
      peak_hourly: 25,
      utilization_rate: 85.2,
    },
    'Checkout Queue': {
      total_visits: 580,
      average_hourly: 3.5,
      peak_hourly: 12,
      utilization_rate: 62.1,
    },
    'Electronics Shelf': {
      total_visits: 340,
      average_hourly: 2.0,
      peak_hourly: 8,
      utilization_rate: 45.8,
    },
  },
});

// Mock API responses
export const mockApi = {
  getCameras: () => Promise.resolve(mockCameras),
  createCamera: () => Promise.resolve({ id: Date.now() }),
  deleteCamera: () => Promise.resolve({ status: 'ok' }),
  
  getZones: () => Promise.resolve({ 
    screenshot: { path: null, width: null, height: null }, 
    zones: mockZones 
  }),
  uploadZoneScreenshot: () => Promise.resolve({ status: 'ok', path: '/path/to/screenshot.png' }),
  createZone: () => Promise.resolve({ id: Date.now() }),
  deleteZone: () => Promise.resolve({ status: 'ok' }),
  getZoneOverlay: () => '/api/zones/overlay?camera_id=1',
  
  getHourlyMetrics: () => Promise.resolve(generateMockHourlyMetrics()),
  getDailyMetrics: () => Promise.resolve(generateMockDailyMetrics()),
  getDailyMetricsByCamera: () => Promise.resolve({}),
  
  getComprehensiveAnalytics: () => Promise.resolve({
    performance_analysis: {},
    anomalies: generateMockSpikes(),
    zone_analytics: {},
    ai_insights: 'Store performance is above average with peak hours between 2-4 PM.',
  }),
  getSpikeAnalysis: () => Promise.resolve({
    date: new Date().toISOString().slice(0, 10),
    spikes_detected: 3,
    spikes: generateMockSpikes(),
    baselines: {},
    analysis_summary: {},
  }),
  getRealtimeMetrics: () => Promise.resolve({
    timestamp: new Date().toISOString(),
    live_metrics: generateMockLiveMetrics(),
    total_live_count: 25,
  }),
  getZoneAnalytics: (cameraId: number) => Promise.resolve(generateMockZoneAnalytics(cameraId)),
  
  getEvents: () => Promise.resolve(generateMockEvents()),
  createEvent: () => Promise.resolve({ id: Date.now(), status: 'created' }),
  analyzeEvent: () => Promise.resolve({}),
  
  getAlerts: () => Promise.resolve({ alerts: generateMockAlerts(), alert_count: 2 }),
  resolveAlert: () => Promise.resolve({ status: 'resolved' }),
  
  getInsights: () => Promise.resolve({
    weekly: { insights: 'Performance trending upward this week.' },
    extras: {},
  }),
};