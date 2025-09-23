import React from 'react';
import { motion } from 'framer-motion';
import { 
  Camera, 
  Plus, 
  Trash2, 
  Edit3, 
  Wifi, 
  WifiOff, 
  CheckCircle, 
  AlertCircle,
  RefreshCw,
  Settings,
  Monitor,
  MapPin
} from 'lucide-react';
import { toast } from 'react-hot-toast';

import Card from '../components/ui/Card';
import { config } from '../config';
import { api } from '../utils/api';
import { mockApi, mockCameras } from '../utils/mockData';
import { Camera as CameraType } from '../types';

const Cameras: React.FC = () => {
  const [loading, setLoading] = React.useState(true);
  const [cameras, setCameras] = React.useState<CameraType[]>([]);
  const [showAddForm, setShowAddForm] = React.useState(false);
  const [editingCamera, setEditingCamera] = React.useState<CameraType | null>(null);
  const [testingCamera, setTestingCamera] = React.useState<string | null>(null);

  const apiClient = config.useMockData ? mockApi : api;

  React.useEffect(() => {
    loadCameras();
  }, []);

  const loadCameras = async () => {
    try {
      setLoading(true);
      const camerasData = await apiClient.getCameras();
      setCameras(camerasData);
    } catch (error) {
      console.error('Error loading cameras:', error);
      toast.error('Failed to load cameras');
    } finally {
      setLoading(false);
    }
  };

  const handleAddCamera = async (cameraData: Partial<CameraType>) => {
    try {
      await apiClient.addCamera(cameraData);
      toast.success('Camera added successfully');
      setShowAddForm(false);
      loadCameras();
    } catch (error) {
      console.error('Error adding camera:', error);
      toast.error('Failed to add camera');
    }
  };

  const handleUpdateCamera = async (id: number, cameraData: Partial<CameraType>) => {
    try {
      await apiClient.updateCamera(id, cameraData);
      toast.success('Camera updated successfully');
      setEditingCamera(null);
      loadCameras();
    } catch (error) {
      console.error('Error updating camera:', error);
      toast.error('Failed to update camera');
    }
  };

  const handleDeleteCamera = async (id: number) => {
    if (!confirm('Are you sure you want to delete this camera?')) return;

    try {
      await apiClient.deleteCamera(id);
      toast.success('Camera deleted successfully');
      loadCameras();
    } catch (error) {
      console.error('Error deleting camera:', error);
      toast.error('Failed to delete camera');
    }
  };

  const handleTestConnection = async (camera: CameraType) => {
    setTestingCamera(camera.id.toString());
    try {
      // Simulate connection test
      await new Promise(resolve => setTimeout(resolve, 2000));
      toast.success(`Connection to ${camera.name} successful`);
    } catch (error) {
      toast.error(`Failed to connect to ${camera.name}`);
    } finally {
      setTestingCamera(null);
    }
  };

  const getCameraStatus = (camera: CameraType) => {
    if (!camera.enabled) return 'disabled';
    
    // Simulate status based on mock data
    const random = Math.random();
    if (random > 0.8) return 'offline';
    if (random > 0.9) return 'error';
    return 'online';
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'online': return 'text-success';
      case 'offline': return 'text-muted';
      case 'error': return 'text-danger';
      case 'disabled': return 'text-muted';
      default: return 'text-muted';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'online': return Wifi;
      case 'offline': 
      case 'disabled': return WifiOff;
      case 'error': return AlertCircle;
      default: return WifiOff;
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
            <h1 className="text-3xl font-bold text-text mb-2">Camera Management</h1>
            <p className="text-muted">Configure and monitor RTSP camera feeds</p>
          </div>
          
          <motion.button
            onClick={() => setShowAddForm(true)}
            className="btn-primary flex items-center gap-2"
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
          >
            <Plus className="h-4 w-4" />
            Add Camera
          </motion.button>
        </motion.div>
      </div>

      <div className="px-6 pb-6 -mt-4">
        <motion.div
          variants={containerVariants}
          initial="hidden"
          animate="visible"
          className="space-y-6"
        >
          {/* Camera Grid */}
          <motion.div variants={itemVariants}>
            <Card
              title="Cameras"
              subtitle={`${cameras.length} cameras configured • ${cameras.filter(c => getCameraStatus(c) === 'online').length} online`}
              loading={loading}
            >
              {cameras.length > 0 ? (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                  {cameras.map((camera, index) => {
                    const status = getCameraStatus(camera);
                    const StatusIcon = getStatusIcon(status);
                    
                    return (
                      <motion.div
                        key={camera.id}
                        initial={{ opacity: 0, scale: 0.95 }}
                        animate={{ opacity: 1, scale: 1 }}
                        transition={{ delay: index * 0.05 }}
                        className="bg-white p-6 rounded-lg border border-gray-100 hover:shadow-md transition-all duration-200"
                      >
                        <div className="flex items-start justify-between mb-4">
                          <div className="min-w-0 flex-1">
                            <h3 className="font-semibold text-text truncate">{camera.name}</h3>
                            <p className="text-sm text-muted mt-1">ID: {camera.id}</p>
                          </div>
                          
                          <motion.div
                            className={`${getStatusColor(status)} flex-shrink-0 ml-2`}
                          >
                            <StatusIcon className="h-5 w-5" />
                          </motion.div>
                        </div>

                        <div className="space-y-3 mb-4">
                          <div className="text-sm">
                            <span className="text-muted">RTSP URL:</span>
                            <p className="font-mono text-xs mt-1 p-2 bg-gray-50 rounded break-all">
                              {camera.rtsp_url}
                            </p>
                          </div>
                          
                          <div className="flex items-center justify-between text-sm">
                            <span className="text-muted">Status:</span>
                            <span className={`font-medium ${getStatusColor(status)}`}>
                              {status.charAt(0).toUpperCase() + status.slice(1)}
                            </span>
                          </div>
                          
                          <div className="flex items-center justify-between text-sm">
                            <span className="text-muted">Enabled:</span>
                            <span className={camera.enabled ? 'text-success' : 'text-muted'}>
                              {camera.enabled ? 'Yes' : 'No'}
                            </span>
                          </div>
                        </div>

                        <div className="flex items-center gap-2">
                          <button
                            onClick={() => handleTestConnection(camera)}
                            disabled={testingCamera === camera.id.toString()}
                            className="btn-secondary flex-1 text-xs flex items-center justify-center gap-1"
                          >
                            {testingCamera === camera.id.toString() ? (
                              <RefreshCw className="h-3 w-3 animate-spin" />
                            ) : (
                              <CheckCircle className="h-3 w-3" />
                            )}
                            Test
                          </button>
                          
                          <button
                            onClick={() => setEditingCamera(camera)}
                            className="btn-ghost p-2"
                          >
                            <Edit3 className="h-4 w-4" />
                          </button>
                          
                          <button
                            onClick={() => handleDeleteCamera(camera.id)}
                            className="btn-ghost p-2 text-danger hover:bg-red-50"
                          >
                            <Trash2 className="h-4 w-4" />
                          </button>
                        </div>
                      </motion.div>
                    );
                  })}
                </div>
              ) : (
                <motion.div
                  className="text-center py-12 text-muted"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                >
                  <Camera className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <h3 className="text-lg font-medium mb-2">No Cameras Configured</h3>
                  <p className="text-sm mb-4">Add your first camera to start monitoring</p>
                  <button 
                    onClick={() => setShowAddForm(true)}
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

      {/* Add/Edit Camera Modal */}
      {(showAddForm || editingCamera) && (
        <CameraForm
          camera={editingCamera}
          onSubmit={editingCamera ? 
            (data) => handleUpdateCamera(editingCamera.id, data) : 
            handleAddCamera
          }
          onClose={() => {
            setShowAddForm(false);
            setEditingCamera(null);
          }}
        />
      )}
    </div>
  );
};

interface CameraFormProps {
  camera?: CameraType | null;
  onSubmit: (data: Partial<CameraType>) => void;
  onClose: () => void;
}

const CameraForm: React.FC<CameraFormProps> = ({ camera, onSubmit, onClose }) => {
  const [formData, setFormData] = React.useState({
    name: camera?.name || '',
    rtsp_url: camera?.rtsp_url || '',
    enabled: camera?.enabled ?? true,
    store_id: camera?.store_id || 1,
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.name.trim() || !formData.rtsp_url.trim()) {
      toast.error('Please fill in all required fields');
      return;
    }
    onSubmit(formData);
  };

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50"
      onClick={onClose}
    >
      <motion.div
        initial={{ scale: 0.95, opacity: 0 }}
        animate={{ scale: 1, opacity: 1 }}
        exit={{ scale: 0.95, opacity: 0 }}
        className="bg-white rounded-lg p-6 w-full max-w-md"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-semibold">
            {camera ? 'Edit Camera' : 'Add Camera'}
          </h2>
          <button onClick={onClose} className="btn-ghost p-1">
            <Plus className="h-5 w-5 rotate-45" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Camera Name *
            </label>
            <input
              type="text"
              value={formData.name}
              onChange={(e) => setFormData(prev => ({ ...prev, name: e.target.value }))}
              className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
              placeholder="Enter camera name"
              required
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              RTSP URL *
            </label>
            <input
              type="text"
              value={formData.rtsp_url}
              onChange={(e) => setFormData(prev => ({ ...prev, rtsp_url: e.target.value }))}
              className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent font-mono text-sm"
              placeholder="rtsp://username:password@ip:port/stream"
              required
            />
            <p className="text-xs text-muted mt-1">
              Example: rtsp://admin:password@192.168.1.100:554/stream1
            </p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Store ID
            </label>
            <input
              type="number"
              value={formData.store_id}
              onChange={(e) => setFormData(prev => ({ ...prev, store_id: parseInt(e.target.value) }))}
              className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
              min="1"
            />
          </div>

          <div className="flex items-center gap-2">
            <input
              type="checkbox"
              id="enabled"
              checked={formData.enabled}
              onChange={(e) => setFormData(prev => ({ ...prev, enabled: e.target.checked }))}
              className="w-4 h-4 text-primary bg-gray-100 border-gray-300 rounded focus:ring-primary"
            />
            <label htmlFor="enabled" className="text-sm text-gray-700">
              Enable camera
            </label>
          </div>

          <div className="flex gap-3 pt-4">
            <button
              type="button"
              onClick={onClose}
              className="btn-secondary flex-1"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="btn-primary flex-1"
            >
              {camera ? 'Update' : 'Add'} Camera
            </button>
          </div>
        </form>
      </motion.div>
    </motion.div>
  );
};

export default Cameras;