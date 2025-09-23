
import sqlite3, threading, os

DB_PATH = os.getenv("DB_PATH", "wink_store.db")

class DB:
    def __init__(self, path): self.path=path; self._lock=threading.Lock()
    def transaction(self): return _Conn(self.path, self._lock)
class _Conn:
    def __init__(self, path, lock): self.path=path; self.lock=lock
    def __enter__(self):
        self.lock.acquire()
        self.conn = sqlite3.connect(self.path, timeout=60, check_same_thread=False)
        return self.conn
    def __exit__(self, *args):
        try: self.conn.close()
        finally: self.lock.release()

db = DB(DB_PATH)

def migrate_all():
    with db.transaction() as conn:
        c = conn.cursor()
        
        # Core store and camera management
        c.execute("""CREATE TABLE IF NOT EXISTS store_info (
            id TEXT PRIMARY KEY, name TEXT, location TEXT,
            timezone TEXT DEFAULT 'UTC', created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            
        c.execute("""CREATE TABLE IF NOT EXISTS cameras (
            id INTEGER PRIMARY KEY AUTOINCREMENT, store_id TEXT NOT NULL,
            name TEXT NOT NULL, rtsp_url TEXT NOT NULL, enabled INTEGER DEFAULT 1,
            location TEXT, camera_type TEXT DEFAULT 'general',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_cameras_store ON cameras(store_id)")
        
        # Zone management with enhanced features
        c.execute("""CREATE TABLE IF NOT EXISTS zone_screenshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT, store_id TEXT NOT NULL, camera_id INTEGER NOT NULL,
            file_path TEXT NOT NULL, img_width INTEGER NOT NULL, img_height INTEGER NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(store_id, camera_id))""")
            
        c.execute("""CREATE TABLE IF NOT EXISTS zones (
            id INTEGER PRIMARY KEY AUTOINCREMENT, store_id TEXT NOT NULL, camera_id INTEGER NOT NULL,
            name TEXT NOT NULL, ztype TEXT NOT NULL, polygon_json TEXT NOT NULL,
            color TEXT DEFAULT '#00ff00', priority INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_zones_store_cam ON zones(store_id, camera_id)")
        
        # Enhanced tracking tables
        c.execute("""CREATE TABLE IF NOT EXISTS track_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, store_id TEXT NOT NULL, camera_id INTEGER NOT NULL,
            track_id INTEGER NOT NULL, entry_time TEXT NOT NULL, exit_time TEXT,
            total_dwell REAL DEFAULT 0, zones_visited TEXT, queue_events TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_track_sessions_store_time ON track_sessions(store_id, entry_time)")
        
        # Zone events with enhanced metadata
        c.execute("""CREATE TABLE IF NOT EXISTS zone_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, store_id TEXT NOT NULL, camera_id INTEGER NOT NULL,
            zone_id TEXT, event_type TEXT NOT NULL, value REAL, person_id TEXT, 
            ts TEXT NOT NULL, metadata TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_zone_events_store_ts ON zone_events(store_id, ts)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_zone_events_person ON zone_events(person_id)")
        
        # Daily unique visitors tracking
        c.execute("""CREATE TABLE IF NOT EXISTS unique_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT, store_id TEXT NOT NULL, camera_id INTEGER NOT NULL,
            ymd TEXT NOT NULL, person_id TEXT NOT NULL, first_seen TEXT, last_seen TEXT,
            total_visits INTEGER DEFAULT 1, total_dwell REAL DEFAULT 0,
            UNIQUE(store_id,camera_id,ymd,person_id))""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_unique_daily_store_date ON unique_daily(store_id, ymd)")
        
        # Enhanced hourly metrics
        c.execute("""CREATE TABLE IF NOT EXISTS hourly_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT, store_id TEXT NOT NULL, camera_id INTEGER NOT NULL,
            hour_start TEXT NOT NULL, footfall INTEGER DEFAULT 0, unique_visitors INTEGER DEFAULT 0,
            dwell_avg REAL DEFAULT 0, dwell_p95 REAL DEFAULT 0, queue_wait_avg REAL DEFAULT 0,
            interactions INTEGER DEFAULT 0, entrance_count INTEGER DEFAULT 0, exit_count INTEGER DEFAULT 0,
            zones_json TEXT, metadata_json TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(store_id, camera_id, hour_start))""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_hourly_store_hour ON hourly_metrics(store_id, hour_start)")
        
        # Enhanced daily metrics
        c.execute("""CREATE TABLE IF NOT EXISTS daily_store_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT, store_id TEXT NOT NULL, date TEXT NOT NULL,
            total_footfall INTEGER DEFAULT 0, unique_visitors INTEGER DEFAULT 0,
            dwell_avg REAL DEFAULT 0, dwell_p95 REAL DEFAULT 0, queue_wait_avg REAL DEFAULT 0,
            interactions INTEGER DEFAULT 0, peak_hour TEXT, peak_footfall INTEGER DEFAULT 0,
            conversion_rate REAL DEFAULT 0, avg_visit_duration REAL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(store_id,date))""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_daily_store_date ON daily_store_metrics(store_id, date)")
        
        # Promotion and festival tracking
        c.execute("""CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, store_id TEXT NOT NULL,
            name TEXT NOT NULL, event_type TEXT NOT NULL, start_date TEXT NOT NULL,
            end_date TEXT NOT NULL, description TEXT, metadata_json TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_events_store_dates ON events(store_id, start_date, end_date)")
        
        # Anomaly detection for spike analysis
        c.execute("""CREATE TABLE IF NOT EXISTS anomalies (
            id INTEGER PRIMARY KEY AUTOINCREMENT, store_id TEXT NOT NULL, camera_id INTEGER,
            anomaly_type TEXT NOT NULL, detected_at TEXT NOT NULL, severity TEXT DEFAULT 'medium',
            value REAL, baseline_value REAL, threshold REAL, 
            description TEXT, metadata_json TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_anomalies_store_time ON anomalies(store_id, detected_at)")
        
        # Real-time alerts
        c.execute("""CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, store_id TEXT NOT NULL,
            alert_type TEXT NOT NULL, severity TEXT DEFAULT 'info',
            message TEXT NOT NULL, resolved INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP, resolved_at TEXT)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_store_resolved ON alerts(store_id, resolved)")
        
        conn.commit()

def set_local_store(store_id, store_name):
    with db.transaction() as conn:
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO store_info (id, name) VALUES (?,?)", (store_id, store_name))
        c.execute("UPDATE store_info SET name=? WHERE id=?", (store_name, store_id))
        conn.commit()
