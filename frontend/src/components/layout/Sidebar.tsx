import React from 'react';
import { motion } from 'framer-motion';
import { NavLink, useLocation } from 'react-router-dom';
import { 
  LayoutDashboard, 
  Activity, 
  Camera, 
  MapPin, 
  Lightbulb, 
  BarChart3, 
  Settings,
  ChevronLeft,
  ChevronRight
} from 'lucide-react';
import clsx from 'clsx';

interface SidebarProps {
  collapsed?: boolean;
  onToggleCollapse?: () => void;
}

const Sidebar: React.FC<SidebarProps> = ({ 
  collapsed = false, 
  onToggleCollapse 
}) => {
  const location = useLocation();

  const navItems = [
    {
      path: '/',
      icon: LayoutDashboard,
      label: 'Dashboard',
      description: 'Store overview & KPIs'
    },
    {
      path: '/live',
      icon: Activity,
      label: 'Live',
      description: 'Real-time monitoring'
    },
    {
      path: '/cameras',
      icon: Camera,
      label: 'Cameras',
      description: 'RTSP camera management'
    },
    {
      path: '/zones',
      icon: MapPin,
      label: 'Zones',
      description: 'Zone configuration'
    },
    {
      path: '/insights',
      icon: Lightbulb,
      label: 'Insights',
      description: 'AI-powered analytics'
    },
    {
      path: '/reports',
      icon: BarChart3,
      label: 'Reports',
      description: 'Detailed analytics'
    },
    {
      path: '/settings',
      icon: Settings,
      label: 'Settings',
      description: 'System configuration'
    },
  ];

  const sidebarVariants = {
    expanded: { width: '16rem' },
    collapsed: { width: '5rem' }
  };

  return (
    <motion.aside
      className="bg-white border-r border-gray-100 flex flex-col relative"
      variants={sidebarVariants}
      animate={collapsed ? 'collapsed' : 'expanded'}
      transition={{ duration: 0.3, ease: 'easeInOut' }}
    >
      {/* Toggle Button */}
      {onToggleCollapse && (
        <motion.button
          className="absolute -right-3 top-6 bg-white border border-gray-200 rounded-full p-1 hover:bg-gray-50 z-10 shadow-sm"
          onClick={onToggleCollapse}
          whileHover={{ scale: 1.1 }}
          whileTap={{ scale: 0.9 }}
        >
          {collapsed ? (
            <ChevronRight className="h-4 w-4 text-muted" />
          ) : (
            <ChevronLeft className="h-4 w-4 text-muted" />
          )}
        </motion.button>
      )}

      {/* Navigation */}
      <nav className="flex-1 px-4 py-6 space-y-2">
        {navItems.map((item) => {
          const Icon = item.icon;
          const isActive = location.pathname === item.path;

          return (
            <motion.div
              key={item.path}
              whileHover={{ x: 2 }}
              transition={{ duration: 0.1 }}
            >
              <NavLink
                to={item.path}
                className={clsx(
                  'flex items-center px-3 py-3 rounded-lg text-sm font-medium transition-all duration-200 group relative overflow-hidden',
                  isActive
                    ? 'bg-primary text-white shadow-lg'
                    : 'text-muted hover:text-text hover:bg-gray-50'
                )}
              >
                {/* Active indicator */}
                {isActive && (
                  <motion.div
                    className="absolute inset-0 bg-primary"
                    layoutId="activeTab"
                    transition={{ type: 'spring', stiffness: 500, damping: 30 }}
                  />
                )}

                <div className="relative z-10 flex items-center">
                  <Icon className="h-5 w-5 flex-shrink-0" />
                  
                  {!collapsed && (
                    <motion.div
                      className="ml-3 min-w-0"
                      initial={false}
                      animate={{ opacity: collapsed ? 0 : 1 }}
                      transition={{ duration: 0.2 }}
                    >
                      <div className="font-medium truncate">{item.label}</div>
                      <div className={clsx(
                        'text-xs mt-0.5 truncate',
                        isActive ? 'text-white/70' : 'text-muted'
                      )}>
                        {item.description}
                      </div>
                    </motion.div>
                  )}
                </div>

                {/* Tooltip for collapsed state */}
                {collapsed && (
                  <div className="absolute left-full ml-2 px-2 py-1 bg-gray-900 text-white text-xs rounded opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none whitespace-nowrap z-50">
                    {item.label}
                  </div>
                )}
              </NavLink>
            </motion.div>
          );
        })}
      </nav>

      {/* Footer */}
      {!collapsed && (
        <motion.div
          className="px-4 py-4 border-t border-gray-100"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.2 }}
        >
          <div className="text-xs text-muted text-center">
            <div className="font-medium">Wink v2.0.0</div>
            <div className="mt-1">Retail Analytics Platform</div>
          </div>
        </motion.div>
      )}
    </motion.aside>
  );
};

export default Sidebar;