import React from 'react';
import { motion } from 'framer-motion';
import { 
  Activity, 
  Camera, 
  Users, 
  Wifi, 
  WifiOff, 
  RefreshCw,
  Play,
  Pause,
  Settings
} from 'lucide-react';
import { toast } from 'react-hot-toast';

import Card from '../components/ui/Card';
import LiveZoneBar from '../components/charts/LiveZoneBar';
import KPICard from '../components/ui/KPICard';

import { config } from '../config';
import { api } from '../utils/api';
import { mockApi, generateMockLiveMetrics, mockCameras } from '../utils/mockData';
import { Camera as CameraType, LiveMetric } from '../types';

const Live: React.FC = () => {
  const [loading, setLoading] = React.useState(true);
  const [autoRefresh, setAutoRefresh] = React.useState(true);
  const [lastUpdated, setLastUpdated] = React.useState(new Date());
  const [cameras, setCameras] = React.useState<CameraType[]>([]);
  const [liveMetrics, setLiveMetrics] = React.useState<Record<string, LiveMetric>>({});
  const [liveZones, setLiveZones] = React.useState([]);

  const apiClient = config.useMockData ? mockApi : api;

  React.useEffect(() => {
    loadInitialData();
  }, []);

  React.useEffect(() => {
    if (autoRefresh) {
      const interval = setInterval(() => {
        loadLiveData();
      }, config.refreshInterval);
      return () => clearInterval(interval);
    }
  }, [autoRefresh]);

  const loadInitialData = async () => {
    try {
      setLoading(true);
      const [camerasData] = await Promise.all([
        apiClient.getCameras(),
      ]);
      
      setCameras(camerasData);
      await loadLiveData();
    } catch (error) {
      console.error('Error loading initial data:', error);
      toast.error('Failed to load camera data');
    } finally {
      setLoading(false);
    }
  };

  const loadLiveData = async () => {
    try {
      if (config.useMockData) {
        const mockLiveData = generateMockLiveMetrics();
        setLiveMetrics(mockLiveData);
        
        // Generate zone data
        const zones = Object.values(mockLiveData).map(metric => ({
          zoneName: `Zone ${metric.camera_id}`,
          currentCount: metric.live_count,
          maxCapacity: 25,
          avgDwell: 45 + Math.random() * 60,
          hourlyTrend: (Math.random() - 0.5) * 30,
          utilizationRate: (metric.live_count / 25) * 100,
        }));
        setLiveZones(zones);
      } else {
        const realtimeData = await apiClient.getRealtimeMetrics();
        setLiveMetrics(realtimeData.live_metrics);
        
        // Process for zone display
        const zones = Object.values(realtimeData.live_metrics).map(metric => ({
          zoneName: metric.camera_name,
          currentCount: metric.live_count,
          maxCapacity: 25,
          avgDwell: 45,
          hourlyTrend: 0,
          utilizationRate: (metric.live_count / 25) * 100,
        }));
        setLiveZones(zones);
      }
      
      setLastUpdated(new Date());
    } catch (error) {
      console.error('Error loading live data:', error);
      if (!config.useMockData) {
        toast.error('Failed to load live data');
      }
    }
  };

  const handleRefresh = () => {
    loadLiveData();
    toast.success('Data refreshed');
  };

  const getCameraStatus = (camera: CameraType) => {
    const metric = liveMetrics[camera.id.toString()];
    if (!metric) return 'offline';
    
    const lastUpdate = new Date(metric.last_updated);
    const now = new Date();
    const diffMinutes = (now.getTime() - lastUpdate.getTime()) / (1000 * 60);
    
    if (diffMinutes > 5) return 'disconnected';
    if (diffMinutes > 2) return 'reconnecting';
    return 'live';
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'live': return 'text-success';
      case 'reconnecting': return 'text-warning';
      case 'disconnected': return 'text-danger';
      case 'offline': return 'text-muted';
      default: return 'text-muted';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'live': return Wifi;
      case 'reconnecting': return RefreshCw;
      case 'disconnected': 
      case 'offline': return WifiOff;
      default: return WifiOff;
    }
  };

  const totalLiveCount = Object.values(liveMetrics).reduce(
    (sum, metric) => sum + metric.live_count, 0
  );

  const activeCameras = cameras.filter(camera => camera.enabled).length;
  const liveCameras = cameras.filter(camera => 
    getCameraStatus(camera) === 'live'
  ).length;

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
            <h1 className="text-3xl font-bold text-text mb-2">Live Monitoring</h1>
            <p className="text-muted">Real-time visitor tracking and camera status</p>
          </div>
          
          <div className="flex items-center gap-4">
            <motion.button
              onClick={() => setAutoRefresh(!autoRefresh)}
              className={`btn ${autoRefresh ? 'btn-primary' : 'btn-secondary'} flex items-center gap-2`}
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
            >
              {autoRefresh ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4" />}
              {autoRefresh ? 'Pause' : 'Resume'}
            </motion.button>
            
            <motion.button
              onClick={handleRefresh}
              className="btn-secondary flex items-center gap-2"
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
            >
              <RefreshCw className="h-4 w-4" />
              Refresh
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
          {/* Live Stats KPIs */}
          <motion.div variants={itemVariants} className="grid grid-cols-1 md:grid-cols-4 gap-6">
            <KPICard
              title="Total Live Count"
              value={totalLiveCount}
              loading={loading}
              icon={<Users className="h-5 w-5" />}
            />
            <KPICard
              title="Active Cameras"
              value={`${liveCameras}/${activeCameras}`}
              loading={loading}
              icon={<Camera className="h-5 w-5" />}
            />
            <KPICard
              title="System Status"
              value={liveCameras === activeCameras ? 'All Online' : 'Issues Detected'}
              loading={loading}
              icon={<Activity className="h-5 w-5" />}
            />
            <KPICard
              title="Last Updated"
              value={lastUpdated.toLocaleTimeString()}
              loading={loading}
              icon={<RefreshCw className="h-5 w-5" />}
            />
          </motion.div>

          {/* Live Zone Monitoring */}
          <motion.div variants={itemVariants}>
            <LiveZoneBar
              data={liveZones}
              loading={loading}
              autoRefresh={autoRefresh}
              refreshInterval={config.refreshInterval}
            />
          </motion.div>

          {/* Camera Status Grid */}
          <motion.div variants={itemVariants}>
            <Card
              title="Camera Status"
              subtitle={`${cameras.length} cameras configured • ${liveCameras} online`}
              loading={loading}
            >
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {cameras.map((camera, index) => {
                  const status = getCameraStatus(camera);
                  const StatusIcon = getStatusIcon(status);
                  const metric = liveMetrics[camera.id.toString()];
                  
                  return (
                    <motion.div
                      key={camera.id}
                      initial={{ opacity: 0, scale: 0.95 }}
                      animate={{ opacity: 1, scale: 1 }}
                      transition={{ delay: index * 0.05 }}
                      className="bg-white p-4 rounded-lg border border-gray-100 hover:shadow-md transition-all duration-200"
                    >
                      <div className="flex items-start justify-between mb-3">
                        <div className="min-w-0 flex-1">
                          <h3 className="font-medium text-text truncate">{camera.name}</h3>
                          <p className="text-xs text-muted mt-1">{camera.rtsp_url}</p>
                        </div>
                        
                        <motion.div
                          className={`${getStatusColor(status)} flex-shrink-0 ml-2`}
                          animate={status === 'reconnecting' ? { rotate: 360 } : {}}
                          transition={{ duration: 2, repeat: status === 'reconnecting' ? Infinity : 0 }}
                        >
                          <StatusIcon className="h-5 w-5" />
                        </motion.div>
                      </div>

                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-4">
                          <div className="text-center">
                            <div className="text-lg font-bold text-text">
                              {metric?.live_count || 0}
                            </div>
                            <div className="text-xs text-muted">Live Count</div>
                          </div>
                          
                          <div className="text-center">
                            <div className={`text-sm font-medium ${getStatusColor(status)}`}>
                              {status.charAt(0).toUpperCase() + status.slice(1)}
                            </div>
                            <div className="text-xs text-muted">Status</div>
                          </div>
                        </div>

                        <button className="btn-ghost p-1">
                          <Settings className="h-4 w-4" />
                        </button>
                      </div>

                      {/* 10-minute sparkline would go here */}
                      <div className="mt-3 h-8 bg-gray-50 rounded flex items-center justify-center">
                        <span className="text-xs text-muted">10min history</span>
                      </div>
                    </motion.div>
                  );
                })}
              </div>

              {cameras.length === 0 && (
                <motion.div
                  className="text-center py-12 text-muted"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                >
                  <Camera className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <h3 className="text-lg font-medium mb-2">No Cameras Configured</h3>
                  <p className="text-sm mb-4">Add cameras to start live monitoring</p>
                  <button 
                    onClick={() => window.location.href = '/cameras'}
                    className="btn-primary"
                  >
                    Add Camera
                  </button>
                </motion.div>
              )}
            </Card>
          </motion.div>
        </motion.div>
      </div>
    </div>
  );
};

export default Live;