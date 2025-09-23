// API Types
export interface Camera {
  id: number;
  name: string;
  rtsp_url: string;
  enabled: boolean;
  created_at?: string;
  location?: string;
  camera_type?: string;
}

export interface Zone {
  id: number;
  name: string;
  ztype: 'entry' | 'queue' | 'shelf' | 'checkout' | 'general';
  polygon: Array<[number, number]>;
  color?: string;
  priority?: number;
  area?: number;
}

export interface ZoneScreenshot {
  path: string | null;
  width: number | null;
  height: number | null;
}

export interface HourlyMetric {
  camera_id: number;
  hour_start: string;
  footfall: number;
  unique_visitors: number;
  dwell_avg: number;
  dwell_p95: number;
  queue_wait_avg: number;
  interactions: number;
  zones: Record<string, number>;
}

export interface DailyMetric {
  date: string;
  total_footfall: number;
  unique_visitors: number;
  dwell_avg: number;
  dwell_p95: number;
  queue_wait_avg: number;
  interactions: number;
  peak_hour: string | null;
  peak_footfall: number;
  conversion_rate: number;
  avg_visit_duration: number;
}

export interface LiveMetric {
  camera_id: string;
  camera_name: string;
  live_count: number;
  last_updated: string;
}

export interface Spike {
  type: string;
  severity: 'low' | 'medium' | 'high' | 'critical';
  hour_start: string;
  camera_id: number;
  value: number;
  baseline_mean: number;
  baseline_std: number;
  spike_magnitude: number;
  description: string;
}

export interface Event {
  id: number;
  name: string;
  event_type: 'promotion' | 'festival' | 'sale';
  start_date: string;
  end_date: string;
  description?: string;
  created_at: string;
}

export interface Alert {
  id: number;
  alert_type: string;
  severity: 'info' | 'warning' | 'error' | 'critical';
  message: string;
  created_at: string;
}

export interface InsightRequest {
  period_weeks: number;
  promo_enabled: boolean;
  promo_start?: string;
  promo_end?: string;
  festival_enabled: boolean;
  festival_start?: string;
  festival_end?: string;
}

export interface ZoneAnalytics {
  camera_id: number;
  analysis_period: {
    start: string;
    end: string;
  };
  total_hours_analyzed: number;
  zone_performance: Record<string, {
    total_visits: number;
    average_hourly: number;
    peak_hourly: number;
    utilization_rate: number;
  }>;
}

// UI Types
export interface KPICardProps {
  title: string;
  value: string | number;
  change?: number;
  trend?: 'up' | 'down' | 'neutral';
  loading?: boolean;
  icon?: React.ReactNode;
  suffix?: string;
  prefix?: string;
}

export interface ChartDataPoint {
  time: string;
  value: number;
  label?: string;
  category?: string;
}

export interface FilterState {
  dateRange: [Date | null, Date | null];
  cameras: number[];
  zones: string[];
}

// Config Types
export interface AppConfig {
  apiBaseUrl: string;
  useMockData: boolean;
  refreshInterval: number;
  maxRetries: number;
}

// Store Types
export interface Store {
  id: string;
  name: string;
  timezone: string;
  location?: string;
}