"""
Database manager with PostgreSQL support, SQLAlchemy, and Row-Level Security.
Handles multi-tenant data isolation and connection management.
"""

import os
import logging
from contextlib import contextmanager
from typing import Optional, Generator
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from .models import Base

# Load environment variables early
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.database_url = self._get_database_url()
        self.enable_rls = os.getenv("ENABLE_RLS", "false").lower() == "true"
        
        # Create engine with appropriate settings
        if self.database_url.startswith("postgresql"):
            self.engine = create_engine(
                self.database_url,
                pool_pre_ping=True,
                pool_recycle=300,
                echo=os.getenv("SQL_DEBUG", "false").lower() == "true"
            )
        else:
            # SQLite fallback for development
            self.engine = create_engine(
                self.database_url,
                poolclass=StaticPool,
                connect_args={"check_same_thread": False},
                echo=os.getenv("SQL_DEBUG", "false").lower() == "true"
            )
        
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
    def _get_database_url(self) -> str:
        """Get database URL from environment with fallback to SQLite."""
        if os.getenv("DATABASE_URL"):
            return os.getenv("DATABASE_URL")
        
        # PostgreSQL configuration
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        pg_user = os.getenv("POSTGRES_USER", "wink")
        pg_password = os.getenv("POSTGRES_PASSWORD", "")
        pg_db = os.getenv("POSTGRES_DB", "wink_analytics")
        
        if pg_password:
            return f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
        
        # SQLite fallback
        db_path = os.getenv("DB_PATH", "wink_store.db")
        return f"sqlite:///{db_path}"
    
    def create_tables(self):
        """Create all database tables."""
        logger.info("Creating database tables...")
        Base.metadata.create_all(bind=self.engine)
        logger.info("Database tables created successfully")
    
    def setup_rls_policies(self):
        """Set up Row-Level Security policies for multi-tenant isolation."""
        if not self.enable_rls or not self.database_url.startswith("postgresql"):
            logger.info("RLS disabled or not using PostgreSQL")
            return
        
        logger.info("Setting up Row-Level Security policies...")
        
        with self.engine.connect() as conn:
            # Enable RLS on all tenant tables
            tables_with_store_id = [
                "users", "cameras", "zones", "tracks", "events", 
                "metrics_daily", "insights", "promotion_events", "invites"
            ]
            
            for table in tables_with_store_id:
                try:
                    # Enable RLS
                    conn.execute(text(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"))
                    
                    # Drop existing policies if they exist
                    conn.execute(text(f"DROP POLICY IF EXISTS tenant_isolation ON {table}"))
                    
                    # Create RLS policy
                    conn.execute(text(f"""
                        CREATE POLICY tenant_isolation ON {table}
                        FOR ALL
                        TO PUBLIC
                        USING (store_id = current_setting('app.store_id')::uuid)
                    """))
                    
                    logger.info(f"RLS policy created for table: {table}")
                    
                except Exception as e:
                    logger.error(f"Failed to create RLS policy for {table}: {e}")
            
            conn.commit()
        
        logger.info("RLS policies setup completed")
    
    @contextmanager
    def get_session(self, store_id: Optional[str] = None) -> Generator[Session, None, None]:
        """Get a database session with optional store context for RLS."""
        session = self.SessionLocal()
        try:
            # Set store context for RLS if enabled and store_id provided
            if self.enable_rls and store_id and self.database_url.startswith("postgresql"):
                session.execute(text(f"SET app.store_id = '{store_id}'"))
            
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    def get_session_factory(self):
        """Get the session factory for dependency injection."""
        return self.SessionLocal
    
    def migrate_from_sqlite(self, sqlite_path: str):
        """Migrate data from existing SQLite database to new schema."""
        logger.info(f"Migrating data from SQLite: {sqlite_path}")
        
        # This would be implemented based on the specific migration needs
        # For now, we'll create a placeholder that can be expanded
        
        import sqlite3
        from .models import Store, Camera, Zone
        from datetime import datetime
        import uuid
        
        # Connect to old SQLite database
        sqlite_conn = sqlite3.connect(sqlite_path)
        sqlite_cursor = sqlite_conn.cursor()
        
        with self.get_session() as session:
            # Migrate stores first
            sqlite_cursor.execute("SELECT DISTINCT store_id, name FROM store_info")
            for row in sqlite_cursor.fetchall():
                store_id_str, store_name = row
                if not store_name:
                    store_name = f"Store {store_id_str}"
                
                store = Store(
                    id=uuid.uuid4(),
                    name=store_name,
                    created_at=datetime.utcnow()
                )
                session.add(store)
                session.flush()  # Get the ID
                
                # Store mapping for migration
                old_to_new_store_id = {store_id_str: store.id}
                
                # Migrate cameras for this store
                sqlite_cursor.execute("""
                    SELECT id, name, rtsp_url, enabled, created_at 
                    FROM cameras WHERE store_id = ?
                """, (store_id_str,))
                
                for cam_row in sqlite_cursor.fetchall():
                    cam_id, cam_name, rtsp_url, enabled, created_at = cam_row
                    
                    camera = Camera(
                        id=uuid.uuid4(),
                        store_id=store.id,
                        name=cam_name,
                        rtsp_url=rtsp_url,
                        status="offline",
                        created_at=datetime.fromisoformat(created_at) if created_at else datetime.utcnow()
                    )
                    session.add(camera)
                    session.flush()
                    
                    # Migrate zones for this camera
                    sqlite_cursor.execute("""
                        SELECT name, ztype, polygon_json, created_at
                        FROM zones WHERE store_id = ? AND camera_id = ?
                    """, (store_id_str, cam_id))
                    
                    for zone_row in sqlite_cursor.fetchall():
                        zone_name, ztype, polygon_json, zone_created_at = zone_row
                        
                        zone = Zone(
                            id=uuid.uuid4(),
                            store_id=store.id,
                            camera_id=camera.id,
                            name=zone_name,
                            ztype=ztype,
                            polygon_json=polygon_json,
                            created_at=datetime.fromisoformat(zone_created_at) if zone_created_at else datetime.utcnow()
                        )
                        session.add(zone)
        
        sqlite_conn.close()
        logger.info("SQLite migration completed")

# Global database instance
db_manager = DatabaseManager()

def get_database() -> DatabaseManager:
    """Get the global database manager instance."""
    return db_manager

def get_db_session():
    """Dependency for FastAPI to get database session."""
    session = db_manager.SessionLocal()
    try:
        yield session
    finally:
        session.close()