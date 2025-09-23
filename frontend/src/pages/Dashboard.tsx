import React from 'react';
import { motion } from 'framer-motion';
import { 
  Users, 
  Clock, 
  ShoppingCart, 
  TrendingUp, 
  MapPin,
  Lightbulb,
  AlertCircle,
  Activity
} from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { toast } from 'react-hot-toast';

import KPICard from '../components/ui/KPICard';
import Card from '../components/ui/Card';
import FootfallChart from '../components/charts/FootfallChart';
import ZoneDwellChart from '../components/charts/ZoneDwellChart';
import QueueWaitChart from '../components/charts/QueueWaitChart';
import LiveZoneBar from '../components/charts/LiveZoneBar';

import { config } from '../config';
import { api } from '../utils/api';
import { mockApi, generateMockHourlyMetrics, generateMockLiveMetrics } from '../utils/mockData';

const Dashboard: React.FC = () => {
  const navigate = useNavigate();
  const [loading, setLoading] = React.useState(true);
  const [kpiData, setKpiData] = React.useState({
    todayFootfall: 0,
    peakHour: '',
    avgDwell: 0,
    avgQueueWait: 0,
    interactions: 0,
  });

  const [chartData, setChartData] = React.useState({
    footfall: [],
    zoneDwell: [],
    queueWait: [],
    liveZones: [],
  });

  const [insights, setInsights] = React.useState('');

  const apiClient = config.useMockData ? mockApi : api;

  React.useEffect(() => {
    loadDashboardData();
    
    // Set up auto-refresh for live data
    const interval = setInterval(() => {
      loadLiveData();
    }, config.refreshInterval);

    return () => clearInterval(interval);
  }, []);

  const loadDashboardData = async () => {
    try {
      setLoading(true);

      // Load daily metrics for KPIs
      const dailyMetrics = await apiClient.getDailyMetrics(1);
      const todayMetric = dailyMetrics[0];

      if (todayMetric) {
        setKpiData({
          todayFootfall: todayMetric.total_footfall,
          peakHour: todayMetric.peak_hour || '',
          avgDwell: Math.round(todayMetric.dwell_avg),
          avgQueueWait: Math.round(todayMetric.queue_wait_avg),
          interactions: todayMetric.interactions,
        });
      }

      // Load hourly data for charts
      const endTime = new Date().toISOString();
      const startTime = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
      
      if (config.useMockData) {
        const hourlyData = generateMockHourlyMetrics(24);
        const footfallData = hourlyData.map(h => ({
          time: h.hour_start,
          value: h.footfall,
        }));

        const zoneDwellData = [
          { zone: 'Entrance', avgDwell: 45, p95Dwell: 120, totalVisits: 150, utilizationRate: 75 },
          { zone: 'Checkout', avgDwell: 180, p95Dwell: 300, totalVisits: 85, utilizationRate: 65 },
          { zone: 'Electronics', avgDwell: 90, p95Dwell: 200, totalVisits: 45, utilizationRate: 35 },
          { zone: 'Grocery', avgDwell: 60, p95Dwell: 150, totalVisits: 120, utilizationRate: 55 },
        ];

        const queueData = hourlyData.map(h => ({
          time: h.hour_start,
          waitTime: h.queue_wait_avg,
          queueLength: Math.floor(Math.random() * 8) + 1,
        }));

        setChartData({
          footfall: footfallData,
          zoneDwell: zoneDwellData,
          queueWait: queueData,
          liveZones: [],
        });
      } else {
        const hourlyMetrics = await apiClient.getHourlyMetrics(startTime, endTime);
        
        // Process data for charts
        const footfallData = hourlyMetrics.map(h => ({
          time: h.hour_start,
          value: h.footfall,
        }));

        // Zone dwell data would need to be processed from zone analytics
        setChartData(prev => ({
          ...prev,
          footfall: footfallData,
        }));
      }

      await loadLiveData();
      await loadInsights();

    } catch (error) {
      console.error('Error loading dashboard data:', error);
      toast.error('Failed to load dashboard data');
    } finally {
      setLoading(false);
    }
  };

  const loadLiveData = async () => {
    try {
      if (config.useMockData) {
        const liveData = generateMockLiveMetrics();
        const liveZones = Object.values(liveData).map(metric => ({
          zoneName: metric.camera_name,
          currentCount: metric.live_count,
          maxCapacity: 20,
          avgDwell: 45 + Math.random() * 60,
          hourlyTrend: (Math.random() - 0.5) * 20,
          utilizationRate: (metric.live_count / 20) * 100,
        }));

        setChartData(prev => ({
          ...prev,
          liveZones,
        }));
      } else {
        const realtimeData = await apiClient.getRealtimeMetrics();
        // Process realtime data for live zones
        const liveZones = Object.values(realtimeData.live_metrics).map(metric => ({
          zoneName: metric.camera_name,
          currentCount: metric.live_count,
          maxCapacity: 20, // This would come from configuration
          avgDwell: 45, // This would come from recent analytics
          hourlyTrend: 0, // This would come from trend calculation
          utilizationRate: (metric.live_count / 20) * 100,
        }));

        setChartData(prev => ({
          ...prev,
          liveZones,
        }));
      }
    } catch (error) {
      console.error('Error loading live data:', error);
    }
  };

  const loadInsights = async () => {
    try {
      const insightsResponse = await apiClient.getInsights({
        period_weeks: 1,
        promo_enabled: false,
        festival_enabled: false,
      });

      if (insightsResponse.weekly?.insights) {
        setInsights(insightsResponse.weekly.insights);
      }
    } catch (error) {
      console.error('Error loading insights:', error);
    }
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

  return (
    <div className="min-h-full bg-bg-subtle">
      {/* Header */}
      <div className="gradient-header px-6 py-8">
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.3 }}
        >
          <h1 className="text-3xl font-bold text-text mb-2">Store Overview</h1>
          <p className="text-muted">Real-time analytics and performance insights</p>
        </motion.div>
      </div>

      <div className="px-6 pb-6 -mt-4">
        <motion.div
          variants={containerVariants}
          initial="hidden"
          animate="visible"
          className="space-y-6"
        >
          {/* KPI Cards */}
          <motion.div variants={itemVariants} className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-6">
            <KPICard
              title="Today's Footfall"
              value={kpiData.todayFootfall}
              change={12.5}
              trend="up"
              loading={loading}
              icon={<Users className="h-5 w-5" />}
            />
            <KPICard
              title="Peak Hour"
              value={kpiData.peakHour ? kpiData.peakHour.split('T')[1]?.split(':').slice(0, 2).join(':') : '--'}
              loading={loading}
              icon={<TrendingUp className="h-5 w-5" />}
            />
            <KPICard
              title="Avg Dwell Time"
              value={kpiData.avgDwell}
              suffix="s"
              change={-5.2}
              trend="down"
              loading={loading}
              icon={<Clock className="h-5 w-5" />}
            />
            <KPICard
              title="Queue Wait Time"
              value={kpiData.avgQueueWait}
              suffix="s"
              change={8.1}
              trend="up"
              loading={loading}
              icon={<Clock className="h-5 w-5" />}
            />
            <KPICard
              title="Interactions"
              value={kpiData.interactions}
              change={15.3}
              trend="up"
              loading={loading}
              icon={<ShoppingCart className="h-5 w-5" />}
            />
          </motion.div>

          {/* Charts Row 1 */}
          <motion.div variants={itemVariants} className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <FootfallChart
              data={chartData.footfall}
              peakHour={kpiData.peakHour}
              loading={loading}
              height={320}
            />
            <LiveZoneBar
              data={chartData.liveZones}
              loading={loading}
              autoRefresh={true}
            />
          </motion.div>

          {/* Charts Row 2 */}
          <motion.div variants={itemVariants} className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <ZoneDwellChart
              data={chartData.zoneDwell}
              loading={loading}
              height={320}
            />
            <QueueWaitChart
              data={chartData.queueWait}
              slaThreshold={180}
              loading={loading}
              height={320}
            />
          </motion.div>

          {/* Insights and Quick Actions */}
          <motion.div variants={itemVariants} className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            {/* AI Insights Preview */}
            <div className="lg:col-span-2">
              <Card
                title="AI Insights Preview"
                subtitle="Latest performance analysis and recommendations"
                actions={
                  <button
                    onClick={() => navigate('/insights')}
                    className="btn-primary text-xs"
                  >
                    View Full Analysis
                  </button>
                }
                loading={loading}
              >
                {insights ? (
                  <div className="prose prose-sm max-w-none">
                    <p className="text-muted leading-relaxed">{insights}</p>
                  </div>
                ) : (
                  <div className="flex items-center gap-3 text-muted">
                    <Lightbulb className="h-5 w-5" />
                    <span>AI insights will appear here once data is processed</span>
                  </div>
                )}
              </Card>
            </div>

            {/* Quick Actions */}
            <Card title="Quick Actions" subtitle="Common tasks and shortcuts">
              <div className="space-y-3">
                <button
                  onClick={() => navigate('/cameras')}
                  className="w-full btn-secondary justify-start"
                >
                  <MapPin className="h-4 w-4 mr-2" />
                  Manage Cameras
                </button>
                <button
                  onClick={() => navigate('/zones')}
                  className="w-full btn-secondary justify-start"
                >
                  <MapPin className="h-4 w-4 mr-2" />
                  Configure Zones
                </button>
                <button
                  onClick={() => navigate('/live')}
                  className="w-full btn-secondary justify-start"
                >
                  <Activity className="h-4 w-4 mr-2" />
                  Live Monitoring
                </button>
                <button
                  onClick={() => navigate('/insights')}
                  className="w-full btn-secondary justify-start"
                >
                  <Lightbulb className="h-4 w-4 mr-2" />
                  Full Insights
                </button>
              </div>
            </Card>
          </motion.div>
        </motion.div>
      </div>
    </div>
  );
};

export default Dashboard;