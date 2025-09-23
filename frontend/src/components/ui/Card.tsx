import React from 'react';
import { motion } from 'framer-motion';
import { MoreHorizontal } from 'lucide-react';
import clsx from 'clsx';

interface CardProps {
  children: React.ReactNode;
  className?: string;
  hover?: boolean;
  padding?: 'none' | 'sm' | 'md' | 'lg';
  title?: string;
  subtitle?: string;
  actions?: React.ReactNode;
  loading?: boolean;
}

const Card: React.FC<CardProps> = ({
  children,
  className = '',
  hover = true,
  padding = 'md',
  title,
  subtitle,
  actions,
  loading = false,
}) => {
  const paddingClasses = {
    none: 'p-0',
    sm: 'p-4',
    md: 'p-6',
    lg: 'p-8',
  };

  const headerPaddingClasses = {
    none: 'px-0 py-0',
    sm: 'px-4 pt-4 pb-2',
    md: 'px-6 pt-6 pb-4',
    lg: 'px-8 pt-8 pb-4',
  };

  return (
    <motion.div
      className={clsx(
        'card',
        hover && 'card-hover',
        loading && 'animate-pulse',
        className
      )}
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.2 }}
    >
      {(title || subtitle || actions) && (
        <div className={clsx(
          'border-b border-gray-100 flex items-start justify-between',
          headerPaddingClasses[padding]
        )}>
          <div className="min-w-0 flex-1">
            {title && (
              <h3 className="text-lg font-semibold text-text truncate">
                {title}
              </h3>
            )}
            {subtitle && (
              <p className="text-sm text-muted mt-1">
                {subtitle}
              </p>
            )}
          </div>
          {actions && (
            <div className="ml-4 flex-shrink-0">
              {actions}
            </div>
          )}
        </div>
      )}
      
      <div className={clsx(
        (title || subtitle || actions) ? 'pt-0' : '',
        paddingClasses[padding]
      )}>
        {loading ? (
          <div className="space-y-3">
            <div className="loading-skeleton h-4 w-3/4"></div>
            <div className="loading-skeleton h-4 w-1/2"></div>
            <div className="loading-skeleton h-4 w-5/6"></div>
          </div>
        ) : (
          children
        )}
      </div>
    </motion.div>
  );
};

export default Card;