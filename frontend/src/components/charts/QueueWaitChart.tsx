import React from 'react';
import { motion } from 'framer-motion';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
  Area,
  ComposedChart,
} from 'recharts';
import { format, parseISO } from 'date-fns';
import { Clock, AlertTriangle, CheckCircle } from 'lucide-react';
import Card from '../ui/Card';

interface QueueWaitData {
  time: string;
  waitTime: number;
  queueLength?: number;
  threshold?: number;
}

interface QueueWaitChartProps {
  data: QueueWaitData[];
  slaThreshold?: number;
  loading?: boolean;
  height?: number;
}

const QueueWaitChart: React.FC<QueueWaitChartProps> = ({
  data,
  slaThreshold = 180, // 3 minutes default SLA
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

  const formatTime = (seconds: number) => {
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    return `${minutes}m ${remainingSeconds}s`;
  };

  // Calculate SLA compliance
  const slaCompliance = React.useMemo(() => {
    if (data.length === 0) return 100;
    const compliantPeriods = data.filter(d => d.waitTime <= slaThreshold).length;
    return (compliantPeriods / data.length) * 100;
  }, [data, slaThreshold]);

  const averageWaitTime = React.useMemo(() => {
    if (data.length === 0) return 0;
    return data.reduce((sum, d) => sum + d.waitTime, 0) / data.length;
  }, [data]);

  const maxWaitTime = React.useMemo(() => {
    if (data.length === 0) return 0;
    return Math.max(...data.map(d => d.waitTime));
  }, [data]);

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      const isOverThreshold = data.waitTime > slaThreshold;
      
      return (
        <motion.div
          className="bg-white p-3 rounded-lg shadow-lg border border-gray-100"
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.1 }}
        >
          <p className="font-medium text-text mb-2">
            {formatXAxisLabel(label)}
          </p>
          <div className="space-y-1">
            <div className="flex items-center gap-2">
              <div className={`w-2 h-2 rounded-full ${isOverThreshold ? 'bg-danger' : 'bg-success'}`}></div>
              <span className="text-sm">
                Wait Time: <strong>{formatTime(data.waitTime)}</strong>
              </span>
            </div>
            {data.queueLength && (
              <p className="text-xs text-muted">
                Queue Length: {data.queueLength} people
              </p>
            )}
            <p className={`text-xs font-medium ${isOverThreshold ? 'text-danger' : 'text-success'}`}>
              {isOverThreshold ? '⚠️ Above SLA' : '✅ Within SLA'}
            </p>
          </div>
        </motion.div>
      );
    }
    return null;
  };

  const getSLAStatus = () => {
    if (slaCompliance >= 95) return { status: 'excellent', color: 'text-success', icon: CheckCircle };
    if (slaCompliance >= 85) return { status: 'good', color: 'text-primary', icon: Clock };
    return { status: 'needs attention', color: 'text-danger', icon: AlertTriangle };
  };

  const slaStatus = getSLAStatus();
  const StatusIcon = slaStatus.icon;

  return (
    <Card
      title="Queue Wait Time"
      subtitle="Real-time queue performance monitoring"
      loading={loading}
      actions={
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-1 text-xs">
            <div className="w-3 h-3 bg-primary rounded-full"></div>
            <span className="text-muted">Wait Time</span>
          </div>
          <div className="flex items-center gap-1 text-xs">
            <div className="w-3 h-3 border-2 border-danger border-dashed rounded"></div>
            <span className="text-muted">SLA Threshold</span>
          </div>
        </div>
      }
    >
      <div style={{ height }}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
            <defs>
              <linearGradient id="queueGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="var(--primary)" stopOpacity={0.2} />
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
              width={50}
              tickFormatter={(value) => `${Math.round(value / 60)}m`}
            />
            
            <Tooltip content={<CustomTooltip />} />
            
            {/* SLA Threshold Line */}
            <ReferenceLine
              y={slaThreshold}
              stroke="var(--danger)"
              strokeDasharray="8 8"
              strokeWidth={2}
              label={{ value: "SLA Threshold", position: "topRight" }}
            />
            
            {/* Queue wait time area */}
            <Area
              type="monotone"
              dataKey="waitTime"
              stroke="var(--primary)"
              strokeWidth={2}
              fill="url(#queueGradient)"
              dot={false}
            />
            
            {/* Queue wait time line */}
            <Line
              type="monotone"
              dataKey="waitTime"
              stroke="var(--primary)"
              strokeWidth={3}
              dot={false}
              activeDot={{
                r: 6,
                fill: 'var(--primary)',
                stroke: 'white',
                strokeWidth: 2,
              }}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      {/* SLA Summary */}
      <motion.div
        className="mt-4 grid grid-cols-1 md:grid-cols-3 gap-4"
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.3 }}
      >
        <div className="text-center">
          <div className="text-lg font-bold text-text">{slaCompliance.toFixed(1)}%</div>
          <div className="text-xs text-muted">SLA Compliance</div>
        </div>
        <div className="text-center">
          <div className="text-lg font-bold text-text">{formatTime(Math.round(averageWaitTime))}</div>
          <div className="text-xs text-muted">Average Wait</div>
        </div>
        <div className="text-center">
          <div className="text-lg font-bold text-text">{formatTime(Math.round(maxWaitTime))}</div>
          <div className="text-xs text-muted">Max Wait Time</div>
        </div>
      </motion.div>

      <motion.div
        className={`mt-3 p-3 rounded-lg border flex items-center gap-2 ${
          slaStatus.status === 'excellent' 
            ? 'bg-success/10 border-success/20' 
            : slaStatus.status === 'good'
            ? 'bg-primary/10 border-primary/20'
            : 'bg-danger/10 border-danger/20'
        }`}
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.4 }}
      >
        <StatusIcon className={`h-4 w-4 ${slaStatus.color}`} />
        <span className={`text-sm font-medium ${slaStatus.color}`}>
          Queue performance: {slaStatus.status}
        </span>
      </motion.div>
    </Card>
  );
};

export default QueueWaitChart;