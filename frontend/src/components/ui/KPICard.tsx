import React from 'react';
import { motion } from 'framer-motion';
import { TrendingUp, TrendingDown, Minus } from 'lucide-react';
import clsx from 'clsx';
import { KPICardProps } from '../../types';

const KPICard: React.FC<KPICardProps> = ({
  title,
  value,
  change,
  trend = 'neutral',
  loading = false,
  icon,
  suffix = '',
  prefix = '',
}) => {
  const formatValue = (val: string | number): string => {
    if (typeof val === 'number') {
      if (val >= 1000000) {
        return (val / 1000000).toFixed(1) + 'M';
      } else if (val >= 1000) {
        return (val / 1000).toFixed(1) + 'K';
      }
      return val.toLocaleString();
    }
    return val;
  };

  const trendIcon = {
    up: TrendingUp,
    down: TrendingDown,
    neutral: Minus,
  };

  const trendColor = {
    up: 'text-success',
    down: 'text-danger',
    neutral: 'text-muted',
  };

  const TrendIcon = trendIcon[trend];

  return (
    <motion.div
      className="card card-hover relative overflow-hidden"
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      transition={{ duration: 0.2 }}
    >
      {/* Primary accent border */}
      <div className="absolute left-0 top-0 bottom-0 w-1 bg-primary"></div>
      
      <div className="p-6">
        <div className="flex items-start justify-between">
          <div className="min-w-0 flex-1">
            <p className="text-sm font-medium text-muted mb-2 truncate">
              {title}
            </p>
            
            {loading ? (
              <div className="space-y-2">
                <div className="loading-skeleton h-8 w-24"></div>
                <div className="loading-skeleton h-4 w-16"></div>
              </div>
            ) : (
              <>
                <div className="flex items-baseline gap-1">
                  {prefix && (
                    <span className="text-lg font-semibold text-muted">
                      {prefix}
                    </span>
                  )}
                  <motion.span
                    className="text-3xl font-bold text-text"
                    initial={{ scale: 0.8 }}
                    animate={{ scale: 1 }}
                    key={value} // Re-animate when value changes
                  >
                    {formatValue(value)}
                  </motion.span>
                  {suffix && (
                    <span className="text-lg font-semibold text-muted">
                      {suffix}
                    </span>
                  )}
                </div>
                
                {change !== undefined && (
                  <motion.div
                    className="flex items-center gap-1 mt-2"
                    initial={{ opacity: 0, x: -10 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ delay: 0.1 }}
                  >
                    <TrendIcon className={clsx('h-4 w-4', trendColor[trend])} />
                    <span className={clsx('text-sm font-medium', trendColor[trend])}>
                      {Math.abs(change)}%
                    </span>
                    <span className="text-xs text-muted">vs last period</span>
                  </motion.div>
                )}
              </>
            )}
          </div>
          
          {icon && (
            <motion.div
              className="flex-shrink-0 ml-4"
              initial={{ scale: 0.8, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              transition={{ delay: 0.1 }}
            >
              <div className="h-12 w-12 bg-primary-100 rounded-lg flex items-center justify-center">
                <div className="text-primary">
                  {icon}
                </div>
              </div>
            </motion.div>
          )}
        </div>
      </div>
      
      {/* Hover effect */}
      <motion.div
        className="absolute inset-0 bg-primary opacity-0 pointer-events-none"
        whileHover={{ opacity: 0.02 }}
        transition={{ duration: 0.2 }}
      />
    </motion.div>
  );
};

export default KPICard;