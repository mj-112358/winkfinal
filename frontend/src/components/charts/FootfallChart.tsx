import React from 'react';
import { motion } from 'framer-motion';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts';
import { format, parseISO } from 'date-fns';
import Card from '../ui/Card';
import { ChartDataPoint } from '../../types';

interface FootfallChartProps {
  data: ChartDataPoint[];
  peakHour?: string;
  loading?: boolean;
  height?: number;
}

const FootfallChart: React.FC<FootfallChartProps> = ({
  data,
  peakHour,
  loading = false,
  height = 300,
}) => {
  const formatXAxisLabel = (value: string) => {
    try {
      return format(parseISO(value), 'HH:mm');
    } catch {
      return value;
    }
  };

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <motion.div
          className="bg-white p-3 rounded-lg shadow-lg border border-gray-100"
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.1 }}
        >
          <p className="font-medium text-text mb-1">
            {formatXAxisLabel(label)}
          </p>
          <p className="text-primary">
            Footfall: <span className="font-semibold">{payload[0].value}</span>
          </p>
          {peakHour && label === peakHour && (
            <p className="text-xs text-warning mt-1">📈 Peak Hour</p>
          )}
        </motion.div>
      );
    }
    return null;
  };

  const gradientId = 'footfallGradient';

  return (
    <Card
      title="Footfall by Hour"
      subtitle="Real-time visitor count throughout the day"
      loading={loading}
      actions={
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-1">
            <div className="w-3 h-3 bg-primary rounded-full"></div>
            <span className="text-xs text-muted">Visitors</span>
          </div>
        </div>
      }
    >
      <div style={{ height }}>
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
            <defs>
              <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="var(--primary)" stopOpacity={0.3} />
                <stop offset="95%" stopColor="var(--primary)" stopOpacity={0.05} />
              </linearGradient>
            </defs>
            
            <CartesianGrid 
              strokeDasharray="3 3" 
              stroke="rgba(0,0,0,0.05)"
              vertical={false}
            />
            
            <XAxis
              dataKey="time"
              axisLine={false}
              tickLine={false}
              tick={{ fontSize: 12, fill: 'var(--muted)' }}
              tickFormatter={formatXAxisLabel}
            />
            
            <YAxis
              axisLine={false}
              tickLine={false}
              tick={{ fontSize: 12, fill: 'var(--muted)' }}
              width={40}
            />
            
            <Tooltip content={<CustomTooltip />} />
            
            {/* Peak hour indicator */}
            {peakHour && (
              <ReferenceLine
                x={peakHour}
                stroke="var(--warning)"
                strokeDasharray="5 5"
                strokeWidth={2}
              />
            )}
            
            <Area
              type="monotone"
              dataKey="value"
              stroke="var(--primary)"
              strokeWidth={2}
              fill={`url(#${gradientId})`}
              dot={false}
              activeDot={{
                r: 6,
                fill: 'var(--primary)',
                stroke: 'white',
                strokeWidth: 2,
              }}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
      
      {peakHour && (
        <motion.div
          className="mt-4 p-3 bg-warning/10 rounded-lg border border-warning/20"
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
        >
          <div className="flex items-center gap-2 text-sm">
            <span className="text-warning">📈</span>
            <span className="text-text">
              Peak hour detected at <strong>{formatXAxisLabel(peakHour)}</strong>
            </span>
          </div>
        </motion.div>
      )}
    </Card>
  );
};

export default FootfallChart;