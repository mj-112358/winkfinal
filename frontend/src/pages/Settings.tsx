import React from 'react';
import { motion } from 'framer-motion';
import { 
  Settings as SettingsIcon, 
  Save, 
  RefreshCw, 
  Bell, 
  Shield, 
  Database,
  Camera,
  Brain,
  Globe,
  Mail,
  Smartphone,
  Key,
  Eye,
  EyeOff,
  AlertTriangle,
  CheckCircle,
  Info
} from 'lucide-react';
import { toast } from 'react-hot-toast';

import Card from '../components/ui/Card';
import { config } from '../config';

interface SettingsData {
  general: {
    store_name: string;
    store_id: string;
    timezone: string;
    refresh_interval: number;
    debug_mode: boolean;
  };
  analytics: {
    ai_insights_enabled: boolean;
    spike_detection_enabled: boolean;
    promotion_detection_sensitivity: number;
    festival_detection_enabled: boolean;
    data_retention_days: number;
  };
  alerts: {
    email_notifications: boolean;
    sms_notifications: boolean;
    webhook_url: string;
    alert_thresholds: {
      camera_offline: number;
      queue_wait_time: number;
      high_occupancy: number;
    };
  };
  cameras: {
    default_rtsp_timeout: number;
    detection_confidence: number;
    tracking_max_disappeared: number;
    zone_entry_threshold: number;
  };
  api: {
    openai_api_key: string;
    redis_url: string;
    database_url: string;
    api_rate_limit: number;
  };
  security: {
    session_timeout: number;
    require_2fa: boolean;
    allowed_ip_ranges: string[];
    api_key_expiry_days: number;
  };
}

const Settings: React.FC = () => {
  const [loading, setLoading] = React.useState(false);
  const [saving, setSaving] = React.useState(false);
  const [activeTab, setActiveTab] = React.useState('general');
  const [showApiKeys, setShowApiKeys] = React.useState(false);
  
  const [settings, setSettings] = React.useState<SettingsData>({
    general: {
      store_name: 'Wink AI Store',
      store_id: 'store_001',
      timezone: 'UTC',
      refresh_interval: 5000,
      debug_mode: false,
    },
    analytics: {
      ai_insights_enabled: true,
      spike_detection_enabled: true,
      promotion_detection_sensitivity: 80,
      festival_detection_enabled: true,
      data_retention_days: 90,
    },
    alerts: {
      email_notifications: true,
      sms_notifications: false,
      webhook_url: '',
      alert_thresholds: {
        camera_offline: 5,
        queue_wait_time: 180,
        high_occupancy: 85,
      },
    },
    cameras: {
      default_rtsp_timeout: 30,
      detection_confidence: 0.7,
      tracking_max_disappeared: 50,
      zone_entry_threshold: 0.5,
    },
    api: {
      openai_api_key: '',
      redis_url: 'redis://localhost:6379',
      database_url: '',
      api_rate_limit: 1000,
    },
    security: {
      session_timeout: 24,
      require_2fa: false,
      allowed_ip_ranges: ['0.0.0.0/0'],
      api_key_expiry_days: 365,
    },
  });

  const tabs = [
    { id: 'general', label: 'General', icon: SettingsIcon },
    { id: 'analytics', label: 'Analytics', icon: Brain },
    { id: 'alerts', label: 'Alerts', icon: Bell },
    { id: 'cameras', label: 'Cameras', icon: Camera },
    { id: 'api', label: 'API & Integrations', icon: Globe },
    { id: 'security', label: 'Security', icon: Shield },
  ];

  const saveSettings = async () => {
    try {
      setSaving(true);
      
      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 1500));
      
      // In real implementation, you would save to backend
      console.log('Saving settings:', settings);
      
      toast.success('Settings saved successfully');
    } catch (error) {
      console.error('Error saving settings:', error);
      toast.error('Failed to save settings');
    } finally {
      setSaving(false);
    }
  };

  const resetSettings = () => {
    if (confirm('Are you sure you want to reset all settings to default values?')) {
      // Reset to default values
      setSettings({
        ...settings,
        [activeTab]: getDefaultSettings(activeTab),
      });
      toast.success(`${tabs.find(t => t.id === activeTab)?.label} settings reset to defaults`);
    }
  };

  const getDefaultSettings = (tab: string) => {
    const defaults: any = {
      general: {
        store_name: 'Wink AI Store',
        store_id: 'store_001',
        timezone: 'UTC',
        refresh_interval: 5000,
        debug_mode: false,
      },
      analytics: {
        ai_insights_enabled: true,
        spike_detection_enabled: true,
        promotion_detection_sensitivity: 80,
        festival_detection_enabled: true,
        data_retention_days: 90,
      },
      // ... other defaults
    };
    return defaults[tab] || {};
  };

  const testConnection = async (type: string) => {
    try {
      toast.loading(`Testing ${type} connection...`);
      await new Promise(resolve => setTimeout(resolve, 2000));
      toast.dismiss();
      toast.success(`${type} connection successful`);
    } catch (error) {
      toast.dismiss();
      toast.error(`${type} connection failed`);
    }
  };

  const updateSettings = (section: keyof SettingsData, key: string, value: any) => {
    setSettings(prev => ({
      ...prev,
      [section]: {
        ...prev[section],
        [key]: value,
      },
    }));
  };

  const updateNestedSettings = (section: keyof SettingsData, parentKey: string, key: string, value: any) => {
    setSettings(prev => ({
      ...prev,
      [section]: {
        ...prev[section],
        [parentKey]: {
          ...(prev[section] as any)[parentKey],
          [key]: value,
        },
      },
    }));
  };

  const containerVariants = {
    hidden: { opacity: 0 },
    visible: {
      opacity: 1,
      transition: {
        staggerChildren: 0.1
      }
    }
  };

  const itemVariants = {
    hidden: { opacity: 0, y: 20 },
    visible: { opacity: 1, y: 0 }
  };

  const renderTabContent = () => {
    switch (activeTab) {
      case 'general':
        return (
          <div className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Store Name
                </label>
                <input
                  type="text"
                  value={settings.general.store_name}
                  onChange={(e) => updateSettings('general', 'store_name', e.target.value)}
                  className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Store ID
                </label>
                <input
                  type="text"
                  value={settings.general.store_id}
                  onChange={(e) => updateSettings('general', 'store_id', e.target.value)}
                  className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Timezone
                </label>
                <select
                  value={settings.general.timezone}
                  onChange={(e) => updateSettings('general', 'timezone', e.target.value)}
                  className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                >
                  <option value="UTC">UTC</option>
                  <option value="America/New_York">Eastern Time</option>
                  <option value="America/Chicago">Central Time</option>
                  <option value="America/Denver">Mountain Time</option>
                  <option value="America/Los_Angeles">Pacific Time</option>
                  <option value="Asia/Kolkata">India Standard Time</option>
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Refresh Interval (ms)
                </label>
                <input
                  type="number"
                  min="1000"
                  max="60000"
                  step="1000"
                  value={settings.general.refresh_interval}
                  onChange={(e) => updateSettings('general', 'refresh_interval', parseInt(e.target.value))}
                  className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                />
              </div>
            </div>

            <div className="flex items-center gap-2">
              <input
                type="checkbox"
                id="debug-mode"
                checked={settings.general.debug_mode}
                onChange={(e) => updateSettings('general', 'debug_mode', e.target.checked)}
                className="w-4 h-4 text-primary bg-gray-100 border-gray-300 rounded focus:ring-primary"
              />
              <label htmlFor="debug-mode" className="text-sm text-gray-700">
                Enable debug mode (shows additional logging and debugging information)
              </label>
            </div>
          </div>
        );

      case 'analytics':
        return (
          <div className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="space-y-4">
                <div className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    id="ai-insights"
                    checked={settings.analytics.ai_insights_enabled}
                    onChange={(e) => updateSettings('analytics', 'ai_insights_enabled', e.target.checked)}
                    className="w-4 h-4 text-primary bg-gray-100 border-gray-300 rounded focus:ring-primary"
                  />
                  <label htmlFor="ai-insights" className="text-sm text-gray-700">
                    Enable AI-powered insights
                  </label>
                </div>

                <div className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    id="spike-detection"
                    checked={settings.analytics.spike_detection_enabled}
                    onChange={(e) => updateSettings('analytics', 'spike_detection_enabled', e.target.checked)}
                    className="w-4 h-4 text-primary bg-gray-100 border-gray-300 rounded focus:ring-primary"
                  />
                  <label htmlFor="spike-detection" className="text-sm text-gray-700">
                    Enable spike detection
                  </label>
                </div>

                <div className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    id="festival-detection"
                    checked={settings.analytics.festival_detection_enabled}
                    onChange={(e) => updateSettings('analytics', 'festival_detection_enabled', e.target.checked)}
                    className="w-4 h-4 text-primary bg-gray-100 border-gray-300 rounded focus:ring-primary"
                  />
                  <label htmlFor="festival-detection" className="text-sm text-gray-700">
                    Enable festival impact detection
                  </label>
                </div>
              </div>

              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Promotion Detection Sensitivity (%)
                  </label>
                  <input
                    type="range"
                    min="50"
                    max="95"
                    step="5"
                    value={settings.analytics.promotion_detection_sensitivity}
                    onChange={(e) => updateSettings('analytics', 'promotion_detection_sensitivity', parseInt(e.target.value))}
                    className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer"
                  />
                  <div className="flex justify-between text-xs text-gray-500 mt-1">
                    <span>Less Sensitive</span>
                    <span>{settings.analytics.promotion_detection_sensitivity}%</span>
                    <span>More Sensitive</span>
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Data Retention (days)
                  </label>
                  <select
                    value={settings.analytics.data_retention_days}
                    onChange={(e) => updateSettings('analytics', 'data_retention_days', parseInt(e.target.value))}
                    className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                  >
                    <option value={30}>30 days</option>
                    <option value={90}>90 days</option>
                    <option value={180}>180 days</option>
                    <option value={365}>1 year</option>
                  </select>
                </div>
              </div>
            </div>
          </div>
        );

      case 'alerts':
        return (
          <div className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="space-y-4">
                <h4 className="font-medium text-text">Notification Channels</h4>
                
                <div className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    id="email-notifications"
                    checked={settings.alerts.email_notifications}
                    onChange={(e) => updateSettings('alerts', 'email_notifications', e.target.checked)}
                    className="w-4 h-4 text-primary bg-gray-100 border-gray-300 rounded focus:ring-primary"
                  />
                  <Mail className="h-4 w-4 text-muted" />
                  <label htmlFor="email-notifications" className="text-sm text-gray-700">
                    Email notifications
                  </label>
                </div>

                <div className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    id="sms-notifications"
                    checked={settings.alerts.sms_notifications}
                    onChange={(e) => updateSettings('alerts', 'sms_notifications', e.target.checked)}
                    className="w-4 h-4 text-primary bg-gray-100 border-gray-300 rounded focus:ring-primary"
                  />
                  <Smartphone className="h-4 w-4 text-muted" />
                  <label htmlFor="sms-notifications" className="text-sm text-gray-700">
                    SMS notifications
                  </label>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Webhook URL
                  </label>
                  <input
                    type="url"
                    value={settings.alerts.webhook_url}
                    onChange={(e) => updateSettings('alerts', 'webhook_url', e.target.value)}
                    className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                    placeholder="https://your-webhook-url.com"
                  />
                </div>
              </div>

              <div className="space-y-4">
                <h4 className="font-medium text-text">Alert Thresholds</h4>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Camera Offline (minutes)
                  </label>
                  <input
                    type="number"
                    min="1"
                    max="60"
                    value={settings.alerts.alert_thresholds.camera_offline}
                    onChange={(e) => updateNestedSettings('alerts', 'alert_thresholds', 'camera_offline', parseInt(e.target.value))}
                    className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Queue Wait Time (seconds)
                  </label>
                  <input
                    type="number"
                    min="30"
                    max="600"
                    value={settings.alerts.alert_thresholds.queue_wait_time}
                    onChange={(e) => updateNestedSettings('alerts', 'alert_thresholds', 'queue_wait_time', parseInt(e.target.value))}
                    className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    High Occupancy (%)
                  </label>
                  <input
                    type="number"
                    min="50"
                    max="100"
                    value={settings.alerts.alert_thresholds.high_occupancy}
                    onChange={(e) => updateNestedSettings('alerts', 'alert_thresholds', 'high_occupancy', parseInt(e.target.value))}
                    className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                  />
                </div>
              </div>
            </div>
          </div>
        );

      case 'cameras':
        return (
          <div className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Default RTSP Timeout (seconds)
                </label>
                <input
                  type="number"
                  min="10"
                  max="120"
                  value={settings.cameras.default_rtsp_timeout}
                  onChange={(e) => updateSettings('cameras', 'default_rtsp_timeout', parseInt(e.target.value))}
                  className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Detection Confidence Threshold
                </label>
                <input
                  type="range"
                  min="0.3"
                  max="0.95"
                  step="0.05"
                  value={settings.cameras.detection_confidence}
                  onChange={(e) => updateSettings('cameras', 'detection_confidence', parseFloat(e.target.value))}
                  className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer"
                />
                <div className="flex justify-between text-xs text-gray-500 mt-1">
                  <span>0.3</span>
                  <span>{settings.cameras.detection_confidence}</span>
                  <span>0.95</span>
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Tracking Max Disappeared Frames
                </label>
                <input
                  type="number"
                  min="10"
                  max="100"
                  value={settings.cameras.tracking_max_disappeared}
                  onChange={(e) => updateSettings('cameras', 'tracking_max_disappeared', parseInt(e.target.value))}
                  className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Zone Entry Threshold
                </label>
                <input
                  type="range"
                  min="0.1"
                  max="1.0"
                  step="0.1"
                  value={settings.cameras.zone_entry_threshold}
                  onChange={(e) => updateSettings('cameras', 'zone_entry_threshold', parseFloat(e.target.value))}
                  className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer"
                />
                <div className="flex justify-between text-xs text-gray-500 mt-1">
                  <span>0.1</span>
                  <span>{settings.cameras.zone_entry_threshold}</span>
                  <span>1.0</span>
                </div>
              </div>
            </div>
          </div>
        );

      case 'api':
        return (
          <div className="space-y-6">
            <div className="flex items-center justify-between mb-4">
              <h4 className="font-medium text-text">API Keys & Configuration</h4>
              <button
                onClick={() => setShowApiKeys(!showApiKeys)}
                className="btn-secondary text-xs flex items-center gap-2"
              >
                {showApiKeys ? <EyeOff className="h-3 w-3" /> : <Eye className="h-3 w-3" />}
                {showApiKeys ? 'Hide' : 'Show'} Keys
              </button>
            </div>

            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  OpenAI API Key
                </label>
                <div className="relative">
                  <input
                    type={showApiKeys ? 'text' : 'password'}
                    value={settings.api.openai_api_key}
                    onChange={(e) => updateSettings('api', 'openai_api_key', e.target.value)}
                    className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent pr-12"
                    placeholder="sk-..."
                  />
                  <button
                    onClick={() => testConnection('OpenAI')}
                    className="absolute right-3 top-1/2 transform -translate-y-1/2 text-xs text-primary hover:text-primary-dark"
                  >
                    Test
                  </button>
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Redis URL
                </label>
                <div className="relative">
                  <input
                    type={showApiKeys ? 'text' : 'password'}
                    value={settings.api.redis_url}
                    onChange={(e) => updateSettings('api', 'redis_url', e.target.value)}
                    className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent pr-12"
                    placeholder="redis://localhost:6379"
                  />
                  <button
                    onClick={() => testConnection('Redis')}
                    className="absolute right-3 top-1/2 transform -translate-y-1/2 text-xs text-primary hover:text-primary-dark"
                  >
                    Test
                  </button>
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Database URL
                </label>
                <div className="relative">
                  <input
                    type={showApiKeys ? 'text' : 'password'}
                    value={settings.api.database_url}
                    onChange={(e) => updateSettings('api', 'database_url', e.target.value)}
                    className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent pr-12"
                    placeholder="postgresql://user:pass@localhost:5432/wink"
                  />
                  <button
                    onClick={() => testConnection('Database')}
                    className="absolute right-3 top-1/2 transform -translate-y-1/2 text-xs text-primary hover:text-primary-dark"
                  >
                    Test
                  </button>
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  API Rate Limit (requests/hour)
                </label>
                <input
                  type="number"
                  min="100"
                  max="10000"
                  step="100"
                  value={settings.api.api_rate_limit}
                  onChange={(e) => updateSettings('api', 'api_rate_limit', parseInt(e.target.value))}
                  className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                />
              </div>
            </div>
          </div>
        );

      case 'security':
        return (
          <div className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Session Timeout (hours)
                </label>
                <input
                  type="number"
                  min="1"
                  max="72"
                  value={settings.security.session_timeout}
                  onChange={(e) => updateSettings('security', 'session_timeout', parseInt(e.target.value))}
                  className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  API Key Expiry (days)
                </label>
                <input
                  type="number"
                  min="30"
                  max="730"
                  value={settings.security.api_key_expiry_days}
                  onChange={(e) => updateSettings('security', 'api_key_expiry_days', parseInt(e.target.value))}
                  className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                />
              </div>
            </div>

            <div className="flex items-center gap-2">
              <input
                type="checkbox"
                id="require-2fa"
                checked={settings.security.require_2fa}
                onChange={(e) => updateSettings('security', 'require_2fa', e.target.checked)}
                className="w-4 h-4 text-primary bg-gray-100 border-gray-300 rounded focus:ring-primary"
              />
              <label htmlFor="require-2fa" className="text-sm text-gray-700">
                Require two-factor authentication for all users
              </label>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Allowed IP Ranges (CIDR notation)
              </label>
              <textarea
                value={settings.security.allowed_ip_ranges.join('\n')}
                onChange={(e) => updateSettings('security', 'allowed_ip_ranges', e.target.value.split('\n').filter(ip => ip.trim()))}
                className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                rows={4}
                placeholder="192.168.1.0/24&#10;10.0.0.0/8&#10;0.0.0.0/0"
              />
              <p className="text-xs text-gray-500 mt-1">
                Enter one IP range per line. Use 0.0.0.0/0 to allow all IPs.
              </p>
            </div>
          </div>
        );

      default:
        return null;
    }
  };

  return (
    <div className="min-h-full bg-bg-subtle">
      {/* Header */}
      <div className="gradient-header px-6 py-8">
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.3 }}
          className="flex items-center justify-between"
        >
          <div>
            <h1 className="text-3xl font-bold text-text mb-2">System Settings</h1>
            <p className="text-muted">Configure platform settings and preferences</p>
          </div>
          
          <div className="flex items-center gap-3">
            <motion.button
              onClick={resetSettings}
              className="btn-secondary flex items-center gap-2"
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
            >
              <RefreshCw className="h-4 w-4" />
              Reset Tab
            </motion.button>
            
            <motion.button
              onClick={saveSettings}
              disabled={saving}
              className="btn-primary flex items-center gap-2"
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
            >
              {saving ? (
                <>
                  <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                  Saving...
                </>
              ) : (
                <>
                  <Save className="h-4 w-4" />
                  Save Settings
                </>
              )}
            </motion.button>
          </div>
        </motion.div>
      </div>

      <div className="px-6 pb-6 -mt-4">
        <motion.div
          variants={containerVariants}
          initial="hidden"
          animate="visible"
          className="space-y-6"
        >
          {/* Settings Navigation */}
          <motion.div variants={itemVariants}>
            <div className="bg-white rounded-lg border border-gray-100 overflow-hidden">
              <div className="flex border-b border-gray-100 overflow-x-auto">
                {tabs.map((tab) => {
                  const IconComponent = tab.icon;
                  return (
                    <button
                      key={tab.id}
                      onClick={() => setActiveTab(tab.id)}
                      className={`flex items-center gap-2 px-6 py-4 whitespace-nowrap font-medium transition-colors ${
                        activeTab === tab.id
                          ? 'text-primary border-b-2 border-primary bg-blue-50'
                          : 'text-muted hover:text-text hover:bg-gray-50'
                      }`}
                    >
                      <IconComponent className="h-4 w-4" />
                      {tab.label}
                    </button>
                  );
                })}
              </div>

              <div className="p-6">
                {renderTabContent()}
              </div>
            </div>
          </motion.div>

          {/* Information Box */}
          <motion.div variants={itemVariants}>
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
              <div className="flex items-start gap-3">
                <Info className="h-5 w-5 text-blue-600 flex-shrink-0 mt-0.5" />
                <div>
                  <h4 className="text-sm font-medium text-blue-900 mb-1">
                    Configuration Notes
                  </h4>
                  <p className="text-sm text-blue-800">
                    Changes to these settings will take effect immediately for new operations. 
                    Some settings may require a system restart to apply to existing processes. 
                    Always test your configuration in a development environment before applying to production.
                  </p>
                </div>
              </div>
            </div>
          </motion.div>
        </motion.div>
      </div>
    </div>
  );
};

export default Settings;