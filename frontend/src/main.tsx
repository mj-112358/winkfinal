
import React from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { Toaster } from 'react-hot-toast';

// Import CSS
import './index.css';

// Import contexts
import { StoreProvider } from './contexts/StoreContext';
import { AuthProvider } from './contexts/AuthContext';

// Import layout and auth
import AppLayout from './components/layout/AppLayout';
import ProtectedRoute from './components/auth/ProtectedRoute';

// Import pages
import Dashboard from './pages/Dashboard';
import Live from './pages/Live';
import Cameras from './pages/Cameras';
import Zones from './pages/Zones';
import Insights from './pages/Insights';
import Reports from './pages/Reports';
import Settings from './pages/Settings';

function App() {
  return (
    <AuthProvider>
      <StoreProvider>
        <BrowserRouter>
          <ProtectedRoute>
            <Routes>
              <Route path="/" element={<AppLayout />}>
                <Route index element={<Dashboard />} />
                <Route path="live" element={<Live />} />
                <Route path="cameras" element={<Cameras />} />
                <Route path="zones" element={<Zones />} />
                <Route path="insights" element={<Insights />} />
                <Route path="reports" element={<Reports />} />
                <Route path="settings" element={<Settings />} />
              </Route>
            </Routes>
          </ProtectedRoute>
          <Toaster
            position="top-right"
            toastOptions={{
              duration: 4000,
              style: {
                background: '#fff',
                color: '#0B1220',
                border: '1px solid #E5E7EB',
                borderRadius: '0.75rem',
                boxShadow: '0 10px 15px -3px rgba(0, 0, 0, 0.1)',
              },
            }}
          />
        </BrowserRouter>
      </StoreProvider>
    </AuthProvider>
  );
}

createRoot(document.getElementById('root')!).render(<App />);
