import React from 'react';
import { motion } from 'framer-motion';
import { Bell, Settings, User, HelpCircle, LogOut } from 'lucide-react';
import { useAuth } from '../../contexts/AuthContext';
import Logo from '../ui/Logo';
import StoreSelector from '../ui/StoreSelector';

interface TopNavProps {
  onLogoClick?: () => void;
}

const TopNav: React.FC<TopNavProps> = ({ onLogoClick }) => {
  const [notificationCount] = React.useState(3);
  const { user, logout } = useAuth();

  const handleLogout = () => {
    logout();
  };

  return (
    <motion.nav
      className="bg-white border-b border-gray-100 px-6 py-4 sticky top-0 z-50"
      initial={{ opacity: 0, y: -10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3 }}
    >
      <div className="flex items-center justify-between">
        {/* Logo and Brand */}
        <Logo onClick={onLogoClick} />
        
        {/* Store Selector */}
        <div className="hidden md:flex items-center">
          <StoreSelector />
        </div>

        {/* Right Actions */}
        <div className="flex items-center gap-2">
          {/* Help */}
          <motion.button
            className="btn-ghost p-2"
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
          >
            <HelpCircle className="h-5 w-5" />
          </motion.button>

          {/* Notifications */}
          <motion.button
            className="btn-ghost p-2 relative"
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
          >
            <Bell className="h-5 w-5" />
            {notificationCount > 0 && (
              <motion.span
                className="absolute -top-1 -right-1 h-5 w-5 bg-danger text-white text-xs rounded-full flex items-center justify-center"
                initial={{ scale: 0 }}
                animate={{ scale: 1 }}
                transition={{ type: 'spring', stiffness: 500, damping: 30 }}
              >
                {notificationCount}
              </motion.span>
            )}
          </motion.button>

          {/* Settings */}
          <motion.button
            className="btn-ghost p-2"
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
          >
            <Settings className="h-5 w-5" />
          </motion.button>

          {/* Profile */}
          <div className="flex items-center gap-2 ml-2">
            <div className="h-8 w-8 bg-primary-100 rounded-full flex items-center justify-center">
              <User className="h-4 w-4 text-primary-600" />
            </div>
            <span className="text-sm text-gray-600 hidden sm:block">{user?.email}</span>

            {/* Logout */}
            <motion.button
              onClick={handleLogout}
              className="btn-ghost p-2 text-red-600 hover:text-red-700 hover:bg-red-50"
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
              title="Logout"
            >
              <LogOut className="h-4 w-4" />
            </motion.button>
          </div>
        </div>
      </div>
    </motion.nav>
  );
};

export default TopNav;