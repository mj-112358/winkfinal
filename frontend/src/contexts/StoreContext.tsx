import React, { createContext, useContext, useState, useEffect } from 'react';

interface Store {
  id: number;
  name: string;
  location: string;
  timezone: string;
  status: 'active' | 'inactive';
  created_at: string;
}

interface StoreContextType {
  currentStore: Store | null;
  stores: Store[];
  setCurrentStore: (store: Store) => void;
  loadStores: () => Promise<void>;
  createStore: (storeData: Partial<Store>) => Promise<void>;
  updateStore: (id: number, storeData: Partial<Store>) => Promise<void>;
  deleteStore: (id: number) => Promise<void>;
}

const StoreContext = createContext<StoreContextType | undefined>(undefined);

export const useStore = () => {
  const context = useContext(StoreContext);
  if (context === undefined) {
    throw new Error('useStore must be used within a StoreProvider');
  }
  return context;
};

const mockStores: Store[] = [
  {
    id: 1,
    name: 'Main Store Location',
    location: 'Downtown Plaza, Store #101',
    timezone: 'America/New_York',
    status: 'active',
    created_at: '2024-01-15T09:00:00Z'
  },
  {
    id: 2,
    name: 'North Mall Branch',
    location: 'North Shopping Mall, Level 2',
    timezone: 'America/New_York',
    status: 'active',
    created_at: '2024-02-01T10:30:00Z'
  },
  {
    id: 3,
    name: 'Airport Terminal Store',
    location: 'International Airport, Terminal A',
    timezone: 'America/New_York',
    status: 'active',
    created_at: '2024-02-15T14:00:00Z'
  },
  {
    id: 4,
    name: 'West Side Outlet',
    location: 'West Side Shopping Center',
    timezone: 'America/Chicago',
    status: 'inactive',
    created_at: '2024-03-01T11:00:00Z'
  }
];

export const StoreProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [currentStore, setCurrentStore] = useState<Store | null>(null);
  const [stores, setStores] = useState<Store[]>([]);

  useEffect(() => {
    loadStores();
  }, []);

  const loadStores = async () => {
    try {
      // In a real app, this would fetch from the API
      // For now, use mock data
      setStores(mockStores);
      
      // Set the first active store as default if none is selected
      if (!currentStore) {
        const defaultStore = mockStores.find(store => store.status === 'active');
        if (defaultStore) {
          setCurrentStore(defaultStore);
        }
      }
    } catch (error) {
      console.error('Error loading stores:', error);
    }
  };

  const createStore = async (storeData: Partial<Store>) => {
    try {
      // In a real app, this would make an API call
      const newStore: Store = {
        id: Math.max(...stores.map(s => s.id)) + 1,
        name: storeData.name || 'New Store',
        location: storeData.location || '',
        timezone: storeData.timezone || 'UTC',
        status: storeData.status || 'active',
        created_at: new Date().toISOString()
      };
      
      setStores(prev => [...prev, newStore]);
      return Promise.resolve();
    } catch (error) {
      console.error('Error creating store:', error);
      throw error;
    }
  };

  const updateStore = async (id: number, storeData: Partial<Store>) => {
    try {
      // In a real app, this would make an API call
      setStores(prev => prev.map(store => 
        store.id === id ? { ...store, ...storeData } : store
      ));
      
      // Update current store if it's the one being updated
      if (currentStore?.id === id) {
        setCurrentStore(prev => prev ? { ...prev, ...storeData } : null);
      }
      
      return Promise.resolve();
    } catch (error) {
      console.error('Error updating store:', error);
      throw error;
    }
  };

  const deleteStore = async (id: number) => {
    try {
      // In a real app, this would make an API call
      setStores(prev => prev.filter(store => store.id !== id));
      
      // If the deleted store was the current one, switch to another
      if (currentStore?.id === id) {
        const remainingStores = stores.filter(store => store.id !== id);
        const newCurrent = remainingStores.find(store => store.status === 'active');
        setCurrentStore(newCurrent || null);
      }
      
      return Promise.resolve();
    } catch (error) {
      console.error('Error deleting store:', error);
      throw error;
    }
  };

  const value: StoreContextType = {
    currentStore,
    stores,
    setCurrentStore,
    loadStores,
    createStore,
    updateStore,
    deleteStore,
  };

  return (
    <StoreContext.Provider value={value}>
      {children}
    </StoreContext.Provider>
  );
};