import React from 'react';
import { motion } from 'framer-motion';
import { 
  Lightbulb, 
  TrendingUp, 
  TrendingDown, 
  AlertTriangle,
  Calendar,
  Clock,
  Users,
  ShoppingCart,
  BarChart3,
  PieChart,
  Download,
  RefreshCw,
  Sparkles,
  Target,
  Brain
} from 'lucide-react';
import { toast } from 'react-hot-toast';

import Card from '../components/ui/Card';
import KPICard from '../components/ui/KPICard';
import { config } from '../config';
import { api } from '../utils/api';
import { mockApi } from '../utils/mockData';

interface InsightData {
  weekly?: {
    insights: string;
    recommendations: string[];
    performance_score: number;
  };
  promotion_analysis?: {
    detected_spikes: Array<{
      date: string;
      spike_magnitude: number;
      confidence: number;
      type: 'promotion' | 'festival' | 'organic';
    }>;
    recommendations: string[];
  };
  trends?: {
    footfall_trend: number;
    dwell_trend: number;
    conversion_trend: number;
    peak_hours: string[];
  };
}

const Insights: React.FC = () => {
  const [loading, setLoading] = React.useState(true);
  const [insightData, setInsightData] = React.useState<InsightData>({});
  const [selectedPeriod, setSelectedPeriod] = React.useState(7);
  const [promoEnabled, setPromoEnabled] = React.useState(true);
  const [festivalEnabled, setFestivalEnabled] = React.useState(true);
  const [lastUpdated, setLastUpdated] = React.useState(new Date());

  const apiClient = config.useMockData ? mockApi : api;

  React.useEffect(() => {
    loadInsights();
  }, [selectedPeriod, promoEnabled, festivalEnabled]);

  const loadInsights = async () => {
    try {
      setLoading(true);
      
      const insights = await apiClient.getInsights({
        period_weeks: selectedPeriod / 7,
        promo_enabled: promoEnabled,
        festival_enabled: festivalEnabled,
      });

      setInsightData(insights);
      setLastUpdated(new Date());
    } catch (error) {
      console.error('Error loading insights:', error);
      toast.error('Failed to load insights');
    } finally {
      setLoading(false);
    }
  };

  const handleRefresh = () => {
    loadInsights();
    toast.success('Insights refreshed');
  };

  const handleExportReport = () => {
    // Create a comprehensive report
    const report = {
      generated_at: new Date().toISOString(),
      period_days: selectedPeriod,
      ...insightData,
    };

    const blob = new Blob([JSON.stringify(report, null, 2)], { 
      type: 'application/json' 
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `wink-ai-insights-${new Date().toISOString().split('T')[0]}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    toast.success('Report exported successfully');
  };

  const getPerformanceColor = (score: number) => {
    if (score >= 80) return 'text-success';
    if (score >= 60) return 'text-warning';
    return 'text-danger';
  };

  const getTrendIcon = (trend: number) => {
    if (trend > 5) return TrendingUp;
    if (trend < -5) return TrendingDown;
    return BarChart3;
  };

  const getTrendColor = (trend: number) => {
    if (trend > 5) return 'text-success';
    if (trend < -5) return 'text-danger';
    return 'text-muted';
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
          className="flex items-center justify-between"
        >
          <div>
            <h1 className="text-3xl font-bold text-text mb-2">AI Insights</h1>
            <p className="text-muted">Advanced analytics and performance recommendations powered by AI</p>
          </div>
          
          <div className="flex items-center gap-3">
            <motion.button
              onClick={handleRefresh}
              className="btn-secondary flex items-center gap-2"
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
            >
              <RefreshCw className="h-4 w-4" />
              Refresh
            </motion.button>
            
            <motion.button
              onClick={handleExportReport}
              className="btn-primary flex items-center gap-2"
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
            >
              <Download className="h-4 w-4" />
              Export Report
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
          {/* Analysis Controls */}
          <motion.div variants={itemVariants}>
            <Card title="Analysis Configuration" subtitle="Customize your insights analysis">
              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Analysis Period
                  </label>
                  <select
                    value={selectedPeriod}
                    onChange={(e) => setSelectedPeriod(parseInt(e.target.value))}
                    className="w-full p-2 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                  >
                    <option value={7}>Last 7 Days</option>
                    <option value={14}>Last 14 Days</option>
                    <option value={30}>Last 30 Days</option>
                    <option value={90}>Last 90 Days</option>
                  </select>
                </div>

                <div className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    id="promo-analysis"
                    checked={promoEnabled}
                    onChange={(e) => setPromoEnabled(e.target.checked)}
                    className="w-4 h-4 text-primary bg-gray-100 border-gray-300 rounded focus:ring-primary"
                  />
                  <label htmlFor="promo-analysis" className="text-sm text-gray-700">
                    Promotion Spike Detection
                  </label>
                </div>

                <div className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    id="festival-analysis"
                    checked={festivalEnabled}
                    onChange={(e) => setFestivalEnabled(e.target.checked)}
                    className="w-4 h-4 text-primary bg-gray-100 border-gray-300 rounded focus:ring-primary"
                  />
                  <label htmlFor="festival-analysis" className="text-sm text-gray-700">
                    Festival Impact Analysis
                  </label>
                </div>

                <div className="text-right">
                  <p className="text-xs text-muted mb-1">Last Updated</p>
                  <p className="text-sm font-medium">{lastUpdated.toLocaleTimeString()}</p>
                </div>
              </div>
            </Card>
          </motion.div>

          {/* Performance Overview */}
          <motion.div variants={itemVariants} className="grid grid-cols-1 md:grid-cols-4 gap-6">
            <KPICard
              title="Performance Score"
              value={insightData.weekly?.performance_score || 0}
              suffix="/100"
              loading={loading}
              icon={<Target className="h-5 w-5" />}
              valueClassName={getPerformanceColor(insightData.weekly?.performance_score || 0)}
            />
            
            <KPICard
              title="Footfall Trend"
              value={insightData.trends?.footfall_trend || 0}
              suffix="%"
              trend={insightData.trends?.footfall_trend && insightData.trends.footfall_trend > 0 ? 'up' : 'down'}
              loading={loading}
              icon={React.createElement(getTrendIcon(insightData.trends?.footfall_trend || 0), { className: "h-5 w-5" })}
            />
            
            <KPICard
              title="Dwell Time Trend"
              value={insightData.trends?.dwell_trend || 0}
              suffix="%"
              trend={insightData.trends?.dwell_trend && insightData.trends.dwell_trend > 0 ? 'up' : 'down'}
              loading={loading}
              icon={<Clock className="h-5 w-5" />}
            />
            
            <KPICard
              title="Detected Spikes"
              value={insightData.promotion_analysis?.detected_spikes?.length || 0}
              loading={loading}
              icon={<AlertTriangle className="h-5 w-5" />}
            />
          </motion.div>

          {/* Main Insights */}
          <motion.div variants={itemVariants} className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* AI Analysis */}
            <Card
              title="AI Analysis"
              subtitle="Comprehensive performance insights"
              loading={loading}
              icon={<Brain className="h-5 w-5" />}
            >
              {insightData.weekly?.insights ? (
                <div className="space-y-4">
                  <div className="prose prose-sm max-w-none">
                    <div className="text-muted leading-relaxed whitespace-pre-line">
                      {insightData.weekly.insights}
                    </div>
                  </div>
                  
                  {insightData.weekly.recommendations && insightData.weekly.recommendations.length > 0 && (
                    <div>
                      <h4 className="font-medium text-text mb-2 flex items-center gap-2">
                        <Sparkles className="h-4 w-4" />
                        Key Recommendations
                      </h4>
                      <ul className="space-y-2">
                        {insightData.weekly.recommendations.map((rec, index) => (
                          <li key={index} className="flex items-start gap-2 text-sm text-muted">
                            <div className="w-1.5 h-1.5 bg-primary rounded-full mt-2 flex-shrink-0" />
                            {rec}
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              ) : (
                <div className="flex items-center gap-3 text-muted py-8">
                  <Lightbulb className="h-5 w-5" />
                  <span>AI insights will appear here once sufficient data is available</span>
                </div>
              )}
            </Card>

            {/* Spike Detection */}
            <Card
              title="Spike Detection"
              subtitle="Promotion and festival impact analysis"
              loading={loading}
              icon={<AlertTriangle className="h-5 w-5" />}
            >
              {insightData.promotion_analysis?.detected_spikes && insightData.promotion_analysis.detected_spikes.length > 0 ? (
                <div className="space-y-4">
                  <div className="space-y-3">
                    {insightData.promotion_analysis.detected_spikes.map((spike, index) => (
                      <div key={index} className="p-3 bg-gray-50 rounded-lg">
                        <div className="flex items-center justify-between mb-2">
                          <div className="flex items-center gap-2">
                            <div className={`w-2 h-2 rounded-full ${
                              spike.type === 'promotion' ? 'bg-blue-500' :
                              spike.type === 'festival' ? 'bg-purple-500' : 'bg-green-500'
                            }`} />
                            <span className="font-medium text-sm text-text">
                              {spike.type.charAt(0).toUpperCase() + spike.type.slice(1)} Spike
                            </span>
                          </div>
                          <span className="text-xs text-muted">
                            {spike.confidence.toFixed(0)}% confidence
                          </span>
                        </div>
                        <div className="text-sm text-muted">
                          <div>Date: {new Date(spike.date).toLocaleDateString()}</div>
                          <div>Magnitude: +{spike.spike_magnitude.toFixed(1)}%</div>
                        </div>
                      </div>
                    ))}
                  </div>

                  {insightData.promotion_analysis.recommendations && (
                    <div>
                      <h4 className="font-medium text-text mb-2">Spike Analysis Recommendations</h4>
                      <ul className="space-y-1">
                        {insightData.promotion_analysis.recommendations.map((rec, index) => (
                          <li key={index} className="text-sm text-muted flex items-start gap-2">
                            <div className="w-1.5 h-1.5 bg-orange-500 rounded-full mt-2 flex-shrink-0" />
                            {rec}
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              ) : (
                <div className="flex items-center gap-3 text-muted py-8">
                  <BarChart3 className="h-5 w-5" />
                  <span>No significant spikes detected in the selected period</span>
                </div>
              )}
            </Card>
          </motion.div>

          {/* Trends Analysis */}
          {insightData.trends && (
            <motion.div variants={itemVariants}>
              <Card
                title="Trend Analysis"
                subtitle="Performance trends and patterns"
                loading={loading}
                icon={<TrendingUp className="h-5 w-5" />}
              >
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <div>
                    <h4 className="font-medium text-text mb-3">Performance Trends</h4>
                    <div className="space-y-3">
                      <div className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                        <div className="flex items-center gap-2">
                          <Users className="h-4 w-4 text-muted" />
                          <span className="text-sm text-text">Footfall</span>
                        </div>
                        <div className={`flex items-center gap-1 font-medium ${getTrendColor(insightData.trends.footfall_trend)}`}>
                          {React.createElement(getTrendIcon(insightData.trends.footfall_trend), { className: "h-4 w-4" })}
                          <span>{insightData.trends.footfall_trend > 0 ? '+' : ''}{insightData.trends.footfall_trend.toFixed(1)}%</span>
                        </div>
                      </div>

                      <div className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                        <div className="flex items-center gap-2">
                          <Clock className="h-4 w-4 text-muted" />
                          <span className="text-sm text-text">Dwell Time</span>
                        </div>
                        <div className={`flex items-center gap-1 font-medium ${getTrendColor(insightData.trends.dwell_trend)}`}>
                          {React.createElement(getTrendIcon(insightData.trends.dwell_trend), { className: "h-4 w-4" })}
                          <span>{insightData.trends.dwell_trend > 0 ? '+' : ''}{insightData.trends.dwell_trend.toFixed(1)}%</span>
                        </div>
                      </div>

                      <div className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                        <div className="flex items-center gap-2">
                          <ShoppingCart className="h-4 w-4 text-muted" />
                          <span className="text-sm text-text">Conversion</span>
                        </div>
                        <div className={`flex items-center gap-1 font-medium ${getTrendColor(insightData.trends.conversion_trend)}`}>
                          {React.createElement(getTrendIcon(insightData.trends.conversion_trend), { className: "h-4 w-4" })}
                          <span>{insightData.trends.conversion_trend > 0 ? '+' : ''}{insightData.trends.conversion_trend.toFixed(1)}%</span>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div>
                    <h4 className="font-medium text-text mb-3">Peak Hours</h4>
                    <div className="space-y-2">
                      {insightData.trends.peak_hours && insightData.trends.peak_hours.length > 0 ? (
                        insightData.trends.peak_hours.map((hour, index) => (
                          <div key={index} className="flex items-center gap-2 p-2 bg-blue-50 rounded">
                            <Calendar className="h-4 w-4 text-primary" />
                            <span className="text-sm text-text">{hour}</span>
                          </div>
                        ))
                      ) : (
                        <div className="text-sm text-muted">No peak hours identified</div>
                      )}
                    </div>
                  </div>
                </div>
              </Card>
            </motion.div>
          )}
        </motion.div>
      </div>
    </div>
  );
};

export default Insights;