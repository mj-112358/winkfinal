import React from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Users, TrendingUp, Clock } from 'lucide-react';
import Card from '../ui/Card';

interface LiveZoneData {
  zoneName: string;
  currentCount: number;
  maxCapacity?: number;
  avgDwell: number;
  hourlyTrend: number; // percentage change
  utilizationRate: number;
}

interface LiveZoneBarProps {
  data: LiveZoneData[];
  loading?: boolean;
  autoRefresh?: boolean;
  refreshInterval?: number;
}

const LiveZoneBar: React.FC<LiveZoneBarProps> = ({
  data,
  loading = false,
  autoRefresh = true,
  refreshInterval = 5000,
}) => {
  const [lastUpdated, setLastUpdated] = React.useState(new Date());

  React.useEffect(() => {
    if (autoRefresh) {
      const interval = setInterval(() => {
        setLastUpdated(new Date());
      }, refreshInterval);
      return () => clearInterval(interval);
    }
  }, [autoRefresh, refreshInterval]);

  const maxCount = Math.max(...data.map(d => d.currentCount), 1);

  const getUtilizationColor = (rate: number) => {
    if (rate >= 80) return 'bg-danger';
    if (rate >= 60) return 'bg-warning';
    if (rate >= 40) return 'bg-success';
    return 'bg-primary';
  };

  const getTrendIcon = (trend: number) => {
    if (trend > 5) return { icon: TrendingUp, color: 'text-success', rotation: 0 };
    if (trend < -5) return { icon: TrendingUp, color: 'text-danger', rotation: 180 };
    return { icon: TrendingUp, color: 'text-muted', rotation: 90 };
  };

  return (
    <Card
      title="Live Zone Monitoring"
      subtitle={`Real-time visitor presence • Updated ${lastUpdated.toLocaleTimeString()}`}
      loading={loading}
      actions={
        <div className="flex items-center gap-2">
          <motion.div
            className="w-2 h-2 bg-success rounded-full"
            animate={{ scale: [1, 1.2, 1] }}
            transition={{ duration: 2, repeat: Infinity }}
          />
          <span className="text-xs text-muted">Live</span>
        </div>
      }
    >
      <div className="space-y-4">
        <AnimatePresence mode="popLayout">
          {data.map((zone, index) => {
            const barWidth = (zone.currentCount / maxCount) * 100;
            const trend = getTrendIcon(zone.hourlyTrend);
            const TrendIcon = trend.icon;

            return (
              <motion.div
                key={zone.zoneName}
                layout
                initial={{ opacity: 0, x: -20 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: 20 }}
                transition={{ duration: 0.3, delay: index * 0.05 }}
                className="relative"
              >
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <h4 className="font-medium text-text text-sm">{zone.zoneName}</h4>
                    <motion.div
                      className={trend.color}
                      style={{ transform: `rotate(${trend.rotation}deg)` }}
                      whileHover={{ scale: 1.1 }}
                    >
                      <TrendIcon className="h-3 w-3" />
                    </motion.div>
                  </div>
                  
                  <div className="flex items-center gap-3 text-xs text-muted">
                    <div className="flex items-center gap-1">
                      <Users className="h-3 w-3" />
                      <span className="font-medium text-text">{zone.currentCount}</span>
                      {zone.maxCapacity && (
                        <span>/ {zone.maxCapacity}</span>
                      )}
                    </div>
                    <div className="flex items-center gap-1">
                      <Clock className="h-3 w-3" />
                      <span>{Math.round(zone.avgDwell)}s</span>
                    </div>
                  </div>
                </div>

                {/* Progress Bar */}
                <div className="relative h-3 bg-gray-100 rounded-full overflow-hidden">
                  <motion.div
                    className={`h-full rounded-full ${getUtilizationColor(zone.utilizationRate)}`}
                    initial={{ width: 0 }}
                    animate={{ width: `${barWidth}%` }}
                    transition={{ duration: 0.6, ease: 'easeOut', delay: index * 0.1 }}
                  />
                  
                  {/* Utilization overlay */}
                  <motion.div
                    className="absolute inset-0 bg-gradient-to-r from-transparent via-white/20 to-transparent"
                    animate={{ x: ['-100%', '100%'] }}
                    transition={{ 
                      duration: 2, 
                      repeat: Infinity, 
                      ease: 'easeInOut',
                      delay: index * 0.2 
                    }}
                  />
                </div>

                {/* Zone Stats */}
                <div className="flex items-center justify-between mt-2 text-xs">
                  <div className="flex items-center gap-3">
                    <span className="text-muted">
                      Utilization: <span className="font-medium">{zone.utilizationRate.toFixed(1)}%</span>
                    </span>
                    <span className="text-muted">
                      Trend: <span className={`font-medium ${
                        zone.hourlyTrend > 0 ? 'text-success' : 
                        zone.hourlyTrend < 0 ? 'text-danger' : 'text-muted'
                      }`}>
                        {zone.hourlyTrend > 0 ? '+' : ''}{zone.hourlyTrend.toFixed(1)}%
                      </span>
                    </span>
                  </div>
                </div>
              </motion.div>
            );
          })}
        </AnimatePresence>

        {data.length === 0 && !loading && (
          <motion.div
            className="text-center py-8 text-muted"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
          >
            <Users className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p>No active zones detected</p>
            <p className="text-xs mt-1">Configure zones to start monitoring</p>
          </motion.div>
        )}
      </div>

      {/* Summary Stats */}
      {data.length > 0 && (
        <motion.div
          className="mt-6 pt-4 border-t border-gray-100 grid grid-cols-3 gap-4"
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.5 }}
        >
          <div className="text-center">
            <div className="text-lg font-bold text-text">
              {data.reduce((sum, zone) => sum + zone.currentCount, 0)}
            </div>
            <div className="text-xs text-muted">Total Visitors</div>
          </div>
          <div className="text-center">
            <div className="text-lg font-bold text-text">
              {Math.round(data.reduce((sum, zone) => sum + zone.avgDwell, 0) / data.length)}s
            </div>
            <div className="text-xs text-muted">Avg Dwell Time</div>
          </div>
          <div className="text-center">
            <div className="text-lg font-bold text-text">
              {Math.round(data.reduce((sum, zone) => sum + zone.utilizationRate, 0) / data.length)}%
            </div>
            <div className="text-xs text-muted">Avg Utilization</div>
          </div>
        </motion.div>
      )}
    </Card>
  );
};

export default LiveZoneBar;