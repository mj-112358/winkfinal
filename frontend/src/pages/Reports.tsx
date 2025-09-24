import React from 'react';
import { motion } from 'framer-motion';
import { 
  Download, 
  Calendar, 
  Filter, 
  FileText, 
  BarChart3,
  PieChart,
  TrendingUp,
  Users,
  Clock,
  MapPin,
  Eye,
  Share2,
  Mail,
  Printer,
  Save
} from 'lucide-react';
import { toast } from 'react-hot-toast';

import Card from '../components/ui/Card';
import FootfallChart from '../components/charts/FootfallChart';
import ZoneDwellChart from '../components/charts/ZoneDwellChart';
import QueueWaitChart from '../components/charts/QueueWaitChart';
import { config } from '../config';
import { api } from '../utils/api';
import { mockApi, generateMockHourlyMetrics } from '../utils/mockData';

interface ReportFilters {
  startDate: string;
  endDate: string;
  cameras: string[];
  zones: string[];
  metrics: string[];
  format: 'pdf' | 'excel' | 'csv' | 'json';
}

const Reports: React.FC = () => {
  const [loading, setLoading] = React.useState(false);
  const [reportData, setReportData] = React.useState<any>({});
  const [cameras, setCameras] = React.useState([]);
  const [zones, setZones] = React.useState([]);
  const [previewMode, setPreviewMode] = React.useState(false);
  
  const [filters, setFilters] = React.useState<ReportFilters>({
    startDate: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
    endDate: new Date().toISOString().split('T')[0],
    cameras: [],
    zones: [],
    metrics: ['footfall', 'dwell_time', 'queue_wait'],
    format: 'pdf'
  });

  const apiClient = config.useMockData ? mockApi : api;

  React.useEffect(() => {
    loadMetadata();
  }, []);

  const loadMetadata = async () => {
    try {
      const [camerasData, zonesData] = await Promise.all([
        apiClient.getCameras(),
        apiClient.getZones?.() || Promise.resolve([])
      ]);
      
      setCameras(camerasData);
      setZones(zonesData);
      
      // Set all cameras and zones as selected by default
      setFilters(prev => ({
        ...prev,
        cameras: camerasData.map((c: any) => c.id.toString()),
        zones: zonesData.map((z: any) => z.id.toString())
      }));
    } catch (error) {
      console.error('Error loading metadata:', error);
      toast.error('Failed to load cameras and zones');
    }
  };

  const generateReport = async () => {
    try {
      setLoading(true);

      // Generate mock data for preview
      const hourlyData = generateMockHourlyMetrics(24 * 7); // 7 days of hourly data
      
      const reportData = {
        metadata: {
          generated_at: new Date().toISOString(),
          period: {
            start: filters.startDate,
            end: filters.endDate
          },
          filters,
          total_cameras: filters.cameras.length,
          total_zones: filters.zones.length
        },
        summary: {
          total_footfall: hourlyData.reduce((sum, h) => sum + h.footfall, 0),
          avg_dwell_time: hourlyData.reduce((sum, h) => sum + h.dwell_avg, 0) / hourlyData.length,
          avg_queue_wait: hourlyData.reduce((sum, h) => sum + h.queue_wait_avg, 0) / hourlyData.length,
          peak_hour: hourlyData.reduce((peak, h) => h.footfall > peak.footfall ? h : peak, hourlyData[0]),
        },
        hourly_data: hourlyData,
        daily_summary: generateDailySummary(hourlyData),
        zone_analytics: generateZoneAnalytics(),
        camera_performance: generateCameraPerformance(),
        insights: [
          "Peak traffic occurs between 2 PM and 4 PM on weekdays",
          "Zone 1 (Entrance) shows highest engagement with 15% above average dwell time",
          "Queue wait times are consistently under SLA threshold of 3 minutes",
          "Weekend traffic is 23% higher than weekday average"
        ]
      };

      setReportData(reportData);
      setPreviewMode(true);
    } catch (error) {
      console.error('Error generating report:', error);
      toast.error('Failed to generate report');
    } finally {
      setLoading(false);
    }
  };

  const generateDailySummary = (hourlyData: any[]) => {
    const dailyGroups = hourlyData.reduce((groups, hour) => {
      const date = hour.hour_start.split('T')[0];
      if (!groups[date]) {
        groups[date] = [];
      }
      groups[date].push(hour);
      return groups;
    }, {} as Record<string, any[]>);

    return Object.entries(dailyGroups).map(([date, hours]) => ({
      date,
      total_footfall: hours.reduce((sum, h) => sum + h.footfall, 0),
      avg_dwell: hours.reduce((sum, h) => sum + h.dwell_avg, 0) / hours.length,
      avg_queue_wait: hours.reduce((sum, h) => sum + h.queue_wait_avg, 0) / hours.length,
      peak_hour: hours.reduce((peak, h) => h.footfall > peak.footfall ? h : peak, hours[0])
    }));
  };

  const generateZoneAnalytics = () => {
    return [
      { zone: 'Entrance', footfall: 1250, avg_dwell: 45, utilization: 78 },
      { zone: 'Checkout', footfall: 980, avg_dwell: 180, utilization: 65 },
      { zone: 'Electronics', footfall: 560, avg_dwell: 120, utilization: 42 },
      { zone: 'Grocery', footfall: 720, avg_dwell: 90, utilization: 55 }
    ];
  };

  const generateCameraPerformance = () => {
    return cameras.map((camera: any, index) => ({
      camera_name: camera.name,
      uptime: 95 + Math.random() * 5,
      total_detections: 1000 + Math.random() * 2000,
      avg_confidence: 85 + Math.random() * 10,
      last_maintenance: new Date(Date.now() - Math.random() * 30 * 24 * 60 * 60 * 1000).toISOString()
    }));
  };

  const exportReport = async () => {
    try {
      let filename = `wink-ai-report-${filters.startDate}-to-${filters.endDate}`;
      let blob: Blob;

      switch (filters.format) {
        case 'json':
          blob = new Blob([JSON.stringify(reportData, null, 2)], { 
            type: 'application/json' 
          });
          filename += '.json';
          break;
        
        case 'csv':
          const csvData = convertToCSV(reportData);
          blob = new Blob([csvData], { type: 'text/csv' });
          filename += '.csv';
          break;
        
        case 'excel':
          // For demo purposes, export as CSV (in real implementation, use xlsx library)
          const excelData = convertToCSV(reportData);
          blob = new Blob([excelData], { type: 'application/vnd.ms-excel' });
          filename += '.xls';
          break;
        
        default: // PDF
          const pdfData = generatePDFContent(reportData);
          blob = new Blob([pdfData], { type: 'application/pdf' });
          filename += '.pdf';
          break;
      }

      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
      
      toast.success(`Report exported as ${filters.format.toUpperCase()}`);
    } catch (error) {
      console.error('Error exporting report:', error);
      toast.error('Failed to export report');
    }
  };

  const convertToCSV = (data: any) => {
    if (!data.hourly_data) return '';
    
    const headers = ['Date', 'Hour', 'Footfall', 'Avg Dwell Time', 'Queue Wait Time'];
    const rows = data.hourly_data.map((row: any) => [
      row.hour_start.split('T')[0],
      new Date(row.hour_start).getHours(),
      row.footfall,
      row.dwell_avg.toFixed(1),
      row.queue_wait_avg.toFixed(1)
    ]);

    return [headers, ...rows].map(row => row.join(',')).join('\n');
  };

  const generatePDFContent = (data: any) => {
    // In a real implementation, you would use a PDF library like jsPDF
    return `Wink Analytics Report
Generated: ${new Date().toLocaleString()}
Period: ${filters.startDate} to ${filters.endDate}

SUMMARY
Total Footfall: ${data.summary?.total_footfall || 0}
Average Dwell Time: ${data.summary?.avg_dwell_time?.toFixed(1) || 0}s
Average Queue Wait: ${data.summary?.avg_queue_wait?.toFixed(1) || 0}s

INSIGHTS
${data.insights?.join('\n') || 'No insights available'}
`;
  };

  const shareReport = () => {
    if (navigator.share) {
      navigator.share({
        title: 'Wink Analytics Report',
        text: 'Check out this retail analytics report',
        url: window.location.href
      });
    } else {
      navigator.clipboard.writeText(window.location.href);
      toast.success('Report link copied to clipboard');
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
          className="flex items-center justify-between"
        >
          <div>
            <h1 className="text-3xl font-bold text-text mb-2">Analytics Reports</h1>
            <p className="text-muted">Generate comprehensive analytics reports with custom filters</p>
          </div>
          
          <div className="flex items-center gap-3">
            {previewMode && (
              <>
                <motion.button
                  onClick={shareReport}
                  className="btn-secondary flex items-center gap-2"
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                >
                  <Share2 className="h-4 w-4" />
                  Share
                </motion.button>
                
                <motion.button
                  onClick={exportReport}
                  className="btn-primary flex items-center gap-2"
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                >
                  <Download className="h-4 w-4" />
                  Export
                </motion.button>
              </>
            )}
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
          {!previewMode ? (
            <>
              {/* Report Configuration */}
              <motion.div variants={itemVariants}>
                <Card
                  title="Report Configuration"
                  subtitle="Customize your analytics report"
                  icon={<Filter className="h-5 w-5" />}
                >
                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {/* Date Range */}
                    <div className="space-y-4">
                      <h4 className="font-medium text-text">Date Range</h4>
                      <div className="grid grid-cols-2 gap-3">
                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-1">
                            Start Date
                          </label>
                          <input
                            type="date"
                            value={filters.startDate}
                            onChange={(e) => setFilters(prev => ({ ...prev, startDate: e.target.value }))}
                            className="w-full p-2 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                          />
                        </div>
                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-1">
                            End Date
                          </label>
                          <input
                            type="date"
                            value={filters.endDate}
                            onChange={(e) => setFilters(prev => ({ ...prev, endDate: e.target.value }))}
                            className="w-full p-2 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                          />
                        </div>
                      </div>
                    </div>

                    {/* Export Format */}
                    <div className="space-y-4">
                      <h4 className="font-medium text-text">Export Format</h4>
                      <select
                        value={filters.format}
                        onChange={(e) => setFilters(prev => ({ ...prev, format: e.target.value as any }))}
                        className="w-full p-2 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                      >
                        <option value="pdf">PDF Report</option>
                        <option value="excel">Excel Spreadsheet</option>
                        <option value="csv">CSV Data</option>
                        <option value="json">JSON Data</option>
                      </select>
                    </div>
                  </div>

                  <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mt-6">
                    {/* Cameras */}
                    <div>
                      <h4 className="font-medium text-text mb-3">Cameras</h4>
                      <div className="space-y-2 max-h-40 overflow-y-auto">
                        <label className="flex items-center gap-2">
                          <input
                            type="checkbox"
                            checked={filters.cameras.length === cameras.length}
                            onChange={(e) => {
                              if (e.target.checked) {
                                setFilters(prev => ({ ...prev, cameras: cameras.map((c: any) => c.id.toString()) }));
                              } else {
                                setFilters(prev => ({ ...prev, cameras: [] }));
                              }
                            }}
                            className="w-4 h-4 text-primary bg-gray-100 border-gray-300 rounded focus:ring-primary"
                          />
                          <span className="text-sm font-medium">All Cameras</span>
                        </label>
                        {cameras.map((camera: any) => (
                          <label key={camera.id} className="flex items-center gap-2">
                            <input
                              type="checkbox"
                              checked={filters.cameras.includes(camera.id.toString())}
                              onChange={(e) => {
                                if (e.target.checked) {
                                  setFilters(prev => ({ ...prev, cameras: [...prev.cameras, camera.id.toString()] }));
                                } else {
                                  setFilters(prev => ({ ...prev, cameras: prev.cameras.filter(id => id !== camera.id.toString()) }));
                                }
                              }}
                              className="w-4 h-4 text-primary bg-gray-100 border-gray-300 rounded focus:ring-primary"
                            />
                            <span className="text-sm text-gray-700">{camera.name}</span>
                          </label>
                        ))}
                      </div>
                    </div>

                    {/* Zones */}
                    <div>
                      <h4 className="font-medium text-text mb-3">Zones</h4>
                      <div className="space-y-2 max-h-40 overflow-y-auto">
                        <label className="flex items-center gap-2">
                          <input
                            type="checkbox"
                            checked={filters.zones.length === zones.length}
                            onChange={(e) => {
                              if (e.target.checked) {
                                setFilters(prev => ({ ...prev, zones: zones.map((z: any) => z.id.toString()) }));
                              } else {
                                setFilters(prev => ({ ...prev, zones: [] }));
                              }
                            }}
                            className="w-4 h-4 text-primary bg-gray-100 border-gray-300 rounded focus:ring-primary"
                          />
                          <span className="text-sm font-medium">All Zones</span>
                        </label>
                        {zones.map((zone: any) => (
                          <label key={zone.id} className="flex items-center gap-2">
                            <input
                              type="checkbox"
                              checked={filters.zones.includes(zone.id.toString())}
                              onChange={(e) => {
                                if (e.target.checked) {
                                  setFilters(prev => ({ ...prev, zones: [...prev.zones, zone.id.toString()] }));
                                } else {
                                  setFilters(prev => ({ ...prev, zones: prev.zones.filter(id => id !== zone.id.toString()) }));
                                }
                              }}
                              className="w-4 h-4 text-primary bg-gray-100 border-gray-300 rounded focus:ring-primary"
                            />
                            <span className="text-sm text-gray-700">{zone.name}</span>
                          </label>
                        ))}
                      </div>
                    </div>

                    {/* Metrics */}
                    <div>
                      <h4 className="font-medium text-text mb-3">Metrics</h4>
                      <div className="space-y-2">
                        {[
                          { id: 'footfall', label: 'Footfall Analysis', icon: Users },
                          { id: 'dwell_time', label: 'Dwell Time', icon: Clock },
                          { id: 'queue_wait', label: 'Queue Wait Time', icon: Clock },
                          { id: 'zone_analytics', label: 'Zone Analytics', icon: MapPin },
                          { id: 'trends', label: 'Trend Analysis', icon: TrendingUp }
                        ].map(metric => (
                          <label key={metric.id} className="flex items-center gap-2">
                            <input
                              type="checkbox"
                              checked={filters.metrics.includes(metric.id)}
                              onChange={(e) => {
                                if (e.target.checked) {
                                  setFilters(prev => ({ ...prev, metrics: [...prev.metrics, metric.id] }));
                                } else {
                                  setFilters(prev => ({ ...prev, metrics: prev.metrics.filter(m => m !== metric.id) }));
                                }
                              }}
                              className="w-4 h-4 text-primary bg-gray-100 border-gray-300 rounded focus:ring-primary"
                            />
                            <metric.icon className="h-4 w-4 text-muted" />
                            <span className="text-sm text-gray-700">{metric.label}</span>
                          </label>
                        ))}
                      </div>
                    </div>
                  </div>

                  <div className="flex gap-3 pt-6 border-t">
                    <button
                      onClick={generateReport}
                      disabled={loading}
                      className="btn-primary flex items-center gap-2"
                    >
                      {loading ? (
                        <>
                          <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                          Generating...
                        </>
                      ) : (
                        <>
                          <Eye className="h-4 w-4" />
                          Preview Report
                        </>
                      )}
                    </button>
                  </div>
                </Card>
              </motion.div>
            </>
          ) : (
            <>
              {/* Report Preview */}
              <motion.div variants={itemVariants}>
                <Card
                  title="Report Preview"
                  subtitle={`Generated for period: ${filters.startDate} to ${filters.endDate}`}
                  actions={
                    <button
                      onClick={() => setPreviewMode(false)}
                      className="btn-secondary text-xs"
                    >
                      Back to Configuration
                    </button>
                  }
                >
                  {/* Report Summary */}
                  <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
                    <div className="bg-blue-50 p-4 rounded-lg">
                      <div className="flex items-center gap-2 mb-2">
                        <Users className="h-4 w-4 text-blue-600" />
                        <span className="text-sm font-medium text-blue-900">Total Footfall</span>
                      </div>
                      <div className="text-2xl font-bold text-blue-900">
                        {reportData.summary?.total_footfall?.toLocaleString() || 0}
                      </div>
                    </div>

                    <div className="bg-green-50 p-4 rounded-lg">
                      <div className="flex items-center gap-2 mb-2">
                        <Clock className="h-4 w-4 text-green-600" />
                        <span className="text-sm font-medium text-green-900">Avg Dwell Time</span>
                      </div>
                      <div className="text-2xl font-bold text-green-900">
                        {reportData.summary?.avg_dwell_time?.toFixed(1) || 0}s
                      </div>
                    </div>

                    <div className="bg-orange-50 p-4 rounded-lg">
                      <div className="flex items-center gap-2 mb-2">
                        <Clock className="h-4 w-4 text-orange-600" />
                        <span className="text-sm font-medium text-orange-900">Avg Queue Wait</span>
                      </div>
                      <div className="text-2xl font-bold text-orange-900">
                        {reportData.summary?.avg_queue_wait?.toFixed(1) || 0}s
                      </div>
                    </div>

                    <div className="bg-purple-50 p-4 rounded-lg">
                      <div className="flex items-center gap-2 mb-2">
                        <TrendingUp className="h-4 w-4 text-purple-600" />
                        <span className="text-sm font-medium text-purple-900">Peak Hour</span>
                      </div>
                      <div className="text-lg font-bold text-purple-900">
                        {reportData.summary?.peak_hour ? 
                          new Date(reportData.summary.peak_hour.hour_start).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'}) : 
                          '--'
                        }
                      </div>
                    </div>
                  </div>

                  {/* Charts */}
                  {filters.metrics.includes('footfall') && (
                    <div className="mb-6">
                      <FootfallChart
                        data={reportData.hourly_data?.map((h: any) => ({
                          time: h.hour_start,
                          value: h.footfall
                        })) || []}
                        loading={false}
                        height={300}
                      />
                    </div>
                  )}

                  {/* Insights */}
                  {reportData.insights && (
                    <div className="mb-6">
                      <h4 className="font-medium text-text mb-3">Key Insights</h4>
                      <ul className="space-y-2">
                        {reportData.insights.map((insight: string, index: number) => (
                          <li key={index} className="flex items-start gap-2 text-sm text-muted">
                            <div className="w-1.5 h-1.5 bg-primary rounded-full mt-2 flex-shrink-0" />
                            {insight}
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                </Card>
              </motion.div>
            </>
          )}
        </motion.div>
      </div>
    </div>
  );
};

export default Reports;