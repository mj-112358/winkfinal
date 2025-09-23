import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { 
  ChevronDown, 
  Store, 
  Plus, 
  Settings,
  MapPin,
  Clock,
  CheckCircle,
  XCircle
} from 'lucide-react';
import { useStore } from '../../contexts/StoreContext';

const StoreSelector: React.FC = () => {
  const { currentStore, stores, setCurrentStore } = useStore();
  const [isOpen, setIsOpen] = useState(false);

  const handleStoreSelect = (store: any) => {
    setCurrentStore(store);
    setIsOpen(false);
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'active':
        return <CheckCircle className="h-4 w-4 text-green-500" />;
      case 'inactive':
        return <XCircle className="h-4 w-4 text-gray-400" />;
      default:
        return <Store className="h-4 w-4 text-gray-400" />;
    }
  };

  return (
    <div className="relative">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="flex items-center gap-2 px-3 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-200 rounded-lg hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-primary/20 transition-colors"
      >
        <Store className="h-4 w-4 text-primary" />
        <span className="hidden sm:inline">
          {currentStore?.name || 'Select Store'}
        </span>
        <ChevronDown className={`h-4 w-4 transition-transform ${isOpen ? 'rotate-180' : ''}`} />
      </button>

      {isOpen && (
        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -10 }}
          className="absolute top-full left-0 mt-2 w-80 bg-white border border-gray-200 rounded-lg shadow-lg z-50"
        >
          <div className="p-3 border-b border-gray-100">
            <h3 className="font-medium text-gray-900">Select Store Location</h3>
            <p className="text-sm text-gray-500">Choose the store location to manage</p>
          </div>

          <div className="max-h-64 overflow-y-auto">
            {stores.map((store) => (
              <button
                key={store.id}
                onClick={() => handleStoreSelect(store)}
                className={`w-full p-3 text-left hover:bg-gray-50 transition-colors ${
                  currentStore?.id === store.id ? 'bg-blue-50 border-r-2 border-primary' : ''
                }`}
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      {getStatusIcon(store.status)}
                      <h4 className="font-medium text-gray-900 truncate">
                        {store.name}
                      </h4>
                    </div>
                    
                    <div className="space-y-1">
                      <div className="flex items-center gap-1 text-xs text-gray-500">
                        <MapPin className="h-3 w-3" />
                        <span className="truncate">{store.location}</span>
                      </div>
                      
                      <div className="flex items-center gap-1 text-xs text-gray-500">
                        <Clock className="h-3 w-3" />
                        <span>{store.timezone}</span>
                      </div>
                    </div>
                  </div>

                  <div className="ml-2">
                    <span className={`px-2 py-1 text-xs font-medium rounded-full ${
                      store.status === 'active' 
                        ? 'bg-green-100 text-green-800' 
                        : 'bg-gray-100 text-gray-600'
                    }`}>
                      {store.status}
                    </span>
                  </div>
                </div>
              </button>
            ))}
          </div>

          <div className="p-3 border-t border-gray-100">
            <button
              onClick={() => {
                setIsOpen(false);
                // TODO: Open store management modal
                console.log('Open store management');
              }}
              className="w-full flex items-center gap-2 px-3 py-2 text-sm text-gray-600 hover:text-gray-900 hover:bg-gray-50 rounded-lg transition-colors"
            >
              <Settings className="h-4 w-4" />
              Manage Stores
            </button>
            
            <button
              onClick={() => {
                setIsOpen(false);
                // TODO: Open add store modal
                console.log('Open add store modal');
              }}
              className="w-full flex items-center gap-2 px-3 py-2 text-sm text-primary hover:bg-blue-50 rounded-lg transition-colors mt-1"
            >
              <Plus className="h-4 w-4" />
              Add New Store
            </button>
          </div>
        </motion.div>
      )}

      {/* Backdrop */}
      {isOpen && (
        <div
          className="fixed inset-0 z-40"
          onClick={() => setIsOpen(false)}
        />
      )}
    </div>
  );
};

export default StoreSelector;