import React from 'react';
import { motion } from 'framer-motion';
import { 
  MapPin, 
  Plus, 
  Trash2, 
  Edit3, 
  Upload, 
  Save, 
  RotateCcw,
  Eye,
  EyeOff,
  Target,
  Square
} from 'lucide-react';
import { toast } from 'react-hot-toast';

import Card from '../components/ui/Card';
import { config } from '../config';
import { api } from '../utils/api';
import { mockApi } from '../utils/mockData';

interface Zone {
  id: number;
  name: string;
  camera_id: number;
  camera_name: string;
  coordinates: Array<{ x: number; y: number }>;
  zone_type: string;
  enabled: boolean;
  created_at: string;
}

const Zones: React.FC = () => {
  const [loading, setLoading] = React.useState(true);
  const [zones, setZones] = React.useState<Zone[]>([]);
  const [cameras, setCameras] = React.useState([]);
  const [showZoneEditor, setShowZoneEditor] = React.useState(false);
  const [editingZone, setEditingZone] = React.useState<Zone | null>(null);
  const [selectedCamera, setSelectedCamera] = React.useState<number | null>(null);

  const apiClient = config.useMockData ? mockApi : api;

  React.useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    try {
      setLoading(true);
      const [zonesData, camerasData] = await Promise.all([
        apiClient.getZones?.() || Promise.resolve([]),
        apiClient.getCameras()
      ]);
      
      setZones(zonesData);
      setCameras(camerasData);
    } catch (error) {
      console.error('Error loading data:', error);
      toast.error('Failed to load zones and cameras');
    } finally {
      setLoading(false);
    }
  };

  const handleAddZone = () => {
    setEditingZone(null);
    setShowZoneEditor(true);
  };

  const handleEditZone = (zone: Zone) => {
    setEditingZone(zone);
    setSelectedCamera(zone.camera_id);
    setShowZoneEditor(true);
  };

  const handleDeleteZone = async (id: number) => {
    if (!confirm('Are you sure you want to delete this zone?')) return;

    try {
      await apiClient.deleteZone?.(id);
      toast.success('Zone deleted successfully');
      loadData();
    } catch (error) {
      console.error('Error deleting zone:', error);
      toast.error('Failed to delete zone');
    }
  };

  const handleSaveZone = async (zoneData: Partial<Zone>) => {
    try {
      if (editingZone) {
        await apiClient.updateZone?.(editingZone.id, zoneData);
        toast.success('Zone updated successfully');
      } else {
        await apiClient.addZone?.(zoneData);
        toast.success('Zone created successfully');
      }
      setShowZoneEditor(false);
      setEditingZone(null);
      loadData();
    } catch (error) {
      console.error('Error saving zone:', error);
      toast.error('Failed to save zone');
    }
  };

  const getZoneTypeColor = (type: string) => {
    switch (type) {
      case 'entrance': return 'bg-blue-100 text-blue-800';
      case 'checkout': return 'bg-green-100 text-green-800';
      case 'product': return 'bg-purple-100 text-purple-800';
      case 'queue': return 'bg-orange-100 text-orange-800';
      default: return 'bg-gray-100 text-gray-800';
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
            <h1 className="text-3xl font-bold text-text mb-2">Zone Configuration</h1>
            <p className="text-muted">Define analytics zones using polygon coordinates over camera screenshots</p>
          </div>
          
          <motion.button
            onClick={handleAddZone}
            className="btn-primary flex items-center gap-2"
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
          >
            <Plus className="h-4 w-4" />
            Add Zone
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
          {/* Zones List */}
          <motion.div variants={itemVariants}>
            <Card
              title="Configured Zones"
              subtitle={`${zones.length} zones configured across ${cameras.length} cameras`}
              loading={loading}
            >
              {zones.length > 0 ? (
                <div className="space-y-4">
                  {zones.map((zone, index) => (
                    <motion.div
                      key={zone.id}
                      initial={{ opacity: 0, x: -20 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: index * 0.05 }}
                      className="bg-white p-4 rounded-lg border border-gray-100 hover:shadow-md transition-all duration-200"
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex-1">
                          <div className="flex items-center gap-3 mb-2">
                            <h3 className="font-semibold text-text">{zone.name}</h3>
                            <span className={`px-2 py-1 rounded-full text-xs font-medium ${getZoneTypeColor(zone.zone_type)}`}>
                              {zone.zone_type}
                            </span>
                            {!zone.enabled && (
                              <span className="px-2 py-1 rounded-full text-xs font-medium bg-gray-100 text-gray-600">
                                Disabled
                              </span>
                            )}
                          </div>
                          
                          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm text-muted">
                            <div>
                              <span className="font-medium">Camera:</span> {zone.camera_name}
                            </div>
                            <div>
                              <span className="font-medium">Points:</span> {zone.coordinates?.length || 0} coordinates
                            </div>
                            <div>
                              <span className="font-medium">Created:</span> {new Date(zone.created_at).toLocaleDateString()}
                            </div>
                          </div>
                        </div>

                        <div className="flex items-center gap-2 ml-4">
                          <button
                            onClick={() => handleEditZone(zone)}
                            className="btn-ghost p-2"
                            title="Edit zone"
                          >
                            <Edit3 className="h-4 w-4" />
                          </button>
                          
                          <button
                            onClick={() => handleDeleteZone(zone.id)}
                            className="btn-ghost p-2 text-danger hover:bg-red-50"
                            title="Delete zone"
                          >
                            <Trash2 className="h-4 w-4" />
                          </button>
                        </div>
                      </div>
                    </motion.div>
                  ))}
                </div>
              ) : (
                <motion.div
                  className="text-center py-12 text-muted"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                >
                  <MapPin className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <h3 className="text-lg font-medium mb-2">No Zones Configured</h3>
                  <p className="text-sm mb-4">Create your first zone to start tracking analytics</p>
                  <button 
                    onClick={handleAddZone}
                    className="btn-primary"
                  >
                    Add Zone
                  </button>
                </motion.div>
              )}
            </Card>
          </motion.div>
        </motion.div>
      </div>

      {/* Zone Editor Modal */}
      {showZoneEditor && (
        <ZoneEditor
          zone={editingZone}
          cameras={cameras}
          selectedCamera={selectedCamera}
          onCameraSelect={setSelectedCamera}
          onSave={handleSaveZone}
          onClose={() => {
            setShowZoneEditor(false);
            setEditingZone(null);
            setSelectedCamera(null);
          }}
        />
      )}
    </div>
  );
};

interface ZoneEditorProps {
  zone?: Zone | null;
  cameras: any[];
  selectedCamera: number | null;
  onCameraSelect: (cameraId: number | null) => void;
  onSave: (data: Partial<Zone>) => void;
  onClose: () => void;
}

const ZoneEditor: React.FC<ZoneEditorProps> = ({ 
  zone, 
  cameras, 
  selectedCamera, 
  onCameraSelect, 
  onSave, 
  onClose 
}) => {
  const canvasRef = React.useRef<HTMLCanvasElement>(null);
  const [screenshot, setScreenshot] = React.useState<string | null>(null);
  const [coordinates, setCoordinates] = React.useState<Array<{ x: number; y: number }>>(
    zone?.coordinates || []
  );
  const [formData, setFormData] = React.useState({
    name: zone?.name || '',
    zone_type: zone?.zone_type || 'entrance',
    enabled: zone?.enabled ?? true,
  });
  const [isDrawing, setIsDrawing] = React.useState(false);

  const handleScreenshotUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        setScreenshot(e.target?.result as string);
        setCoordinates([]); // Reset coordinates when new screenshot is uploaded
      };
      reader.readAsDataURL(file);
    }
  };

  const handleCanvasClick = (event: React.MouseEvent<HTMLCanvasElement>) => {
    if (!isDrawing) return;

    const canvas = canvasRef.current;
    if (!canvas) return;

    const rect = canvas.getBoundingClientRect();
    const x = ((event.clientX - rect.left) / rect.width) * 100; // Convert to percentage
    const y = ((event.clientY - rect.top) / rect.height) * 100;

    setCoordinates(prev => [...prev, { x, y }]);
  };

  const handleSave = () => {
    if (!formData.name.trim()) {
      toast.error('Zone name is required');
      return;
    }

    if (!selectedCamera) {
      toast.error('Please select a camera');
      return;
    }

    if (coordinates.length < 3) {
      toast.error('Zone must have at least 3 points');
      return;
    }

    onSave({
      ...formData,
      camera_id: selectedCamera,
      coordinates,
    });
  };

  React.useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !screenshot) return;

    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const img = new Image();
    img.onload = () => {
      // Set canvas size to match aspect ratio
      const aspectRatio = img.width / img.height;
      canvas.width = 600;
      canvas.height = 600 / aspectRatio;

      // Draw screenshot
      ctx.drawImage(img, 0, 0, canvas.width, canvas.height);

      // Draw polygon if coordinates exist
      if (coordinates.length > 0) {
        ctx.strokeStyle = '#1463FF';
        ctx.fillStyle = 'rgba(20, 99, 255, 0.2)';
        ctx.lineWidth = 2;

        ctx.beginPath();
        coordinates.forEach((point, index) => {
          const x = (point.x / 100) * canvas.width;
          const y = (point.y / 100) * canvas.height;
          
          if (index === 0) {
            ctx.moveTo(x, y);
          } else {
            ctx.lineTo(x, y);
          }

          // Draw point
          ctx.fillStyle = '#1463FF';
          ctx.beginPath();
          ctx.arc(x, y, 4, 0, 2 * Math.PI);
          ctx.fill();
        });

        if (coordinates.length > 2) {
          ctx.closePath();
          ctx.fillStyle = 'rgba(20, 99, 255, 0.2)';
          ctx.fill();
        }
        ctx.stroke();
      }
    };
    img.src = screenshot;
  }, [screenshot, coordinates]);

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
        className="bg-white rounded-lg p-6 w-full max-w-5xl max-h-[90vh] overflow-y-auto"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-semibold">
            {zone ? 'Edit Zone' : 'Create Zone'}
          </h2>
          <button onClick={onClose} className="btn-ghost p-1">
            <Plus className="h-5 w-5 rotate-45" />
          </button>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Form */}
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Zone Name *
              </label>
              <input
                type="text"
                value={formData.name}
                onChange={(e) => setFormData(prev => ({ ...prev, name: e.target.value }))}
                className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                placeholder="Enter zone name"
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Camera *
              </label>
              <select
                value={selectedCamera || ''}
                onChange={(e) => onCameraSelect(e.target.value ? parseInt(e.target.value) : null)}
                className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
                required
              >
                <option value="">Select camera</option>
                {cameras.map(camera => (
                  <option key={camera.id} value={camera.id}>
                    {camera.name}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Zone Type
              </label>
              <select
                value={formData.zone_type}
                onChange={(e) => setFormData(prev => ({ ...prev, zone_type: e.target.value }))}
                className="w-full p-3 border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent"
              >
                <option value="entrance">Entrance</option>
                <option value="checkout">Checkout</option>
                <option value="product">Product Area</option>
                <option value="queue">Queue Area</option>
                <option value="other">Other</option>
              </select>
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
                Enable zone
              </label>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Upload Screenshot
              </label>
              <div className="relative">
                <input
                  type="file"
                  accept="image/*"
                  onChange={handleScreenshotUpload}
                  className="hidden"
                  id="screenshot-upload"
                />
                <label
                  htmlFor="screenshot-upload"
                  className="flex items-center justify-center gap-2 w-full p-3 border-2 border-dashed border-gray-300 rounded-lg cursor-pointer hover:border-primary transition-colors"
                >
                  <Upload className="h-4 w-4" />
                  Choose Screenshot
                </label>
              </div>
              <p className="text-xs text-muted mt-1">
                Upload a screenshot from the camera view to define zone coordinates
              </p>
            </div>

            {coordinates.length > 0 && (
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Coordinates ({coordinates.length} points)
                </label>
                <div className="max-h-32 overflow-y-auto text-xs font-mono bg-gray-50 p-2 rounded">
                  {coordinates.map((coord, index) => (
                    <div key={index}>
                      Point {index + 1}: ({coord.x.toFixed(1)}%, {coord.y.toFixed(1)}%)
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* Canvas */}
          <div className="lg:col-span-2">
            <div className="mb-4 flex items-center gap-2">
              <button
                onClick={() => setIsDrawing(!isDrawing)}
                className={`btn ${isDrawing ? 'btn-primary' : 'btn-secondary'} flex items-center gap-2`}
              >
                {isDrawing ? <Target className="h-4 w-4" /> : <Square className="h-4 w-4" />}
                {isDrawing ? 'Drawing Mode' : 'View Mode'}
              </button>
              
              <button
                onClick={() => setCoordinates([])}
                className="btn-secondary flex items-center gap-2"
                disabled={coordinates.length === 0}
              >
                <RotateCcw className="h-4 w-4" />
                Reset
              </button>
            </div>

            <div className="border-2 border-gray-200 rounded-lg overflow-hidden bg-gray-100">
              {screenshot ? (
                <canvas
                  ref={canvasRef}
                  onClick={handleCanvasClick}
                  className={`w-full h-auto ${isDrawing ? 'cursor-crosshair' : 'cursor-default'}`}
                  style={{ maxHeight: '400px' }}
                />
              ) : (
                <div className="flex items-center justify-center h-64 text-muted">
                  <div className="text-center">
                    <Upload className="h-12 w-12 mx-auto mb-2 opacity-50" />
                    <p>Upload a screenshot to start defining zones</p>
                  </div>
                </div>
              )}
            </div>

            {screenshot && (
              <p className="text-xs text-muted mt-2">
                {isDrawing 
                  ? 'Click on the image to add points and create a polygon zone' 
                  : 'Enable drawing mode to add or modify zone points'
                }
              </p>
            )}
          </div>
        </div>

        <div className="flex gap-3 pt-6 border-t">
          <button
            type="button"
            onClick={onClose}
            className="btn-secondary flex-1"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            className="btn-primary flex-1 flex items-center justify-center gap-2"
          >
            <Save className="h-4 w-4" />
            {zone ? 'Update' : 'Create'} Zone
          </button>
        </div>
      </motion.div>
    </motion.div>
  );
};

export default Zones;