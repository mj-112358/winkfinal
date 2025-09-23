import React from 'react';
import { motion } from 'framer-motion';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ErrorBar,
} from 'recharts';
import { BarChart3, Clock, TrendingUp } from 'lucide-react';
import Card from '../ui/Card';

interface ZoneDwellData {
  zone: string;
  avgDwell: number;
  p95Dwell: number;
  totalVisits: number;
  utilizationRate: number;
}

interface ZoneDwellChartProps {
  data: ZoneDwellData[];
  loading?: boolean;
  height?: number;
  showMetric?: 'average' | 'p95' | 'utilization';
}

const ZoneDwellChart: React.FC<ZoneDwellChartProps> = ({
  data,
  loading = false,
  height = 300,
  showMetric = 'average',
}) => {
  const [selectedMetric, setSelectedMetric] = React.useState(showMetric);

  const getChartData = () => {
    return data.map(item => ({
      zone: item.zone.length > 12 ? item.zone.substring(0, 12) + '...' : item.zone,
      fullZone: item.zone,
      value: selectedMetric === 'average' 
        ? item.avgDwell 
        : selectedMetric === 'p95' 
        ? item.p95Dwell 
        : item.utilizationRate,
      error: selectedMetric === 'average' ? item.p95Dwell - item.avgDwell : 0,
      visits: item.totalVisits,
      utilization: item.utilizationRate,
    }));
  };

  const getValueFormatter = (value: number) => {
    if (selectedMetric === 'utilization') {
      return `${value.toFixed(1)}%`;
    }
    return `${Math.round(value)}s`;
  };

  const getMetricLabel = () => {
    switch (selectedMetric) {
      case 'average': return 'Avg Dwell Time';
      case 'p95': return '95th Percentile Dwell';
      case 'utilization': return 'Zone Utilization';
      default: return 'Dwell Time';
    }
  };

  const getMetricColor = () => {
    switch (selectedMetric) {
      case 'average': return 'var(--primary)';
      case 'p95': return 'var(--accent)';
      case 'utilization': return 'var(--success)';
      default: return 'var(--primary)';
    }
  };

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <motion.div
          className="bg-white p-4 rounded-lg shadow-lg border border-gray-100 min-w-[200px]"
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.1 }}
        >
          <p className="font-semibold text-text mb-2">{data.fullZone}</p>
          <div className="space-y-1 text-sm">
            <div className="flex justify-between">
              <span className="text-muted">{getMetricLabel()}:</span>
              <span className="font-medium">{getValueFormatter(payload[0].value)}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-muted">Total Visits:</span>
              <span className="font-medium">{data.visits.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-muted">Utilization:</span>
              <span className="font-medium">{data.utilization.toFixed(1)}%</span>
            </div>
          </div>
        </motion.div>
      );
    }
    return null;
  };

  const metricOptions = [
    { value: 'average', label: 'Average', icon: Clock },
    { value: 'p95', label: '95th %ile', icon: TrendingUp },
    { value: 'utilization', label: 'Utilization', icon: BarChart3 },
  ];

  return (
    <Card
      title="Zone Dwell Analysis"
      subtitle="Time spent in different store areas"
      loading={loading}
      actions={
        <div className="flex items-center gap-1 bg-gray-50 rounded-lg p-1">
          {metricOptions.map((option) => {
            const Icon = option.icon;
            return (
              <button
                key={option.value}
                onClick={() => setSelectedMetric(option.value as any)}
                className={`px-3 py-1.5 text-xs font-medium rounded-md transition-all duration-200 flex items-center gap-1 ${
                  selectedMetric === option.value
                    ? 'bg-white text-primary shadow-sm'
                    : 'text-muted hover:text-text'
                }`}
              >
                <Icon className="h-3 w-3" />
                {option.label}
              </button>
            );
          })}
        </div>
      }
    >
      <div style={{ height }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={getChartData()}
            margin={{ top: 10, right: 10, left: 0, bottom: 20 }}
          >
            <CartesianGrid 
              strokeDasharray="3 3" 
              stroke="rgba(0,0,0,0.05)"
              vertical={false}
            />
            
            <XAxis
              dataKey="zone"
              axisLine={false}
              tickLine={false}
              tick={{ fontSize: 11, fill: 'var(--muted)' }}
              angle={-45}
              textAnchor="end"
              height={60}
            />
            
            <YAxis
              axisLine={false}
              tickLine={false}
              tick={{ fontSize: 12, fill: 'var(--muted)' }}
              width={60}
              tickFormatter={getValueFormatter}
            />
            
            <Tooltip content={<CustomTooltip />} />
            
            <Bar
              dataKey="value"
              fill={getMetricColor()}
              radius={[4, 4, 0, 0]}
              strokeWidth={0}
            >
              {selectedMetric === 'average' && (
                <ErrorBar 
                  dataKey="error" 
                  width={4} 
                  stroke="var(--muted)"
                  strokeWidth={1}
                />
              )}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      <motion.div
        className="mt-4 flex items-center justify-between text-xs text-muted"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 0.3 }}
      >
        <span>Zone performance metrics</span>
        <span>
          {selectedMetric === 'average' && 'Error bars show 95th percentile'}
          {selectedMetric === 'p95' && 'Upper range of dwell times'}
          {selectedMetric === 'utilization' && 'Percentage of time zone is occupied'}
        </span>
      </motion.div>
    </Card>
  );
};

export default ZoneDwellChart;