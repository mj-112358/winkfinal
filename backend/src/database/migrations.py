"""
Database migration system for Wink platform.
Handles schema versioning and migration management.
"""

import os
import logging
from typing import List, Dict, Any
from sqlalchemy import text, MetaData, Table, Column, String, Integer, DateTime
from sqlalchemy.orm import Session
from datetime import datetime
from .database import get_database
from .models import Base

logger = logging.getLogger(__name__)

class MigrationManager:
    def __init__(self):
        self.db_manager = get_database()
        self.current_version = "1.0.0"
        
    def create_migration_table(self):
        """Create the migration tracking table."""
        with self.db_manager.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    id SERIAL PRIMARY KEY,
                    version VARCHAR(50) NOT NULL UNIQUE,
                    description TEXT,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.commit()
    
    def get_applied_migrations(self) -> List[str]:
        """Get list of applied migration versions."""
        try:
            with self.db_manager.engine.connect() as conn:
                result = conn.execute(text("SELECT version FROM schema_migrations ORDER BY applied_at"))
                return [row[0] for row in result.fetchall()]
        except Exception as e:
            logger.warning(f"Could not read migrations table: {e}")
            return []
    
    def mark_migration_applied(self, version: str, description: str = ""):
        """Mark a migration as applied."""
        with self.db_manager.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO schema_migrations (version, description, applied_at)
                VALUES (:version, :description, :applied_at)
                ON CONFLICT (version) DO NOTHING
            """), {
                "version": version,
                "description": description,
                "applied_at": datetime.utcnow()
            })
            conn.commit()
    
    def run_initial_migration(self):
        """Run the initial migration to create all tables."""
        self.create_migration_table()
        
        applied_migrations = self.get_applied_migrations()
        
        if "1.0.0" not in applied_migrations:
            logger.info("Running initial migration (1.0.0)...")
            
            # Create all tables
            self.db_manager.create_tables()
            
            # Set up RLS policies
            self.db_manager.setup_rls_policies()
            
            # Mark migration as applied
            self.mark_migration_applied("1.0.0", "Initial schema with multi-tenant support")
            
            logger.info("Initial migration completed successfully")
        else:
            logger.info("Initial migration already applied")
    
    def migrate_from_legacy(self, legacy_db_path: str = None):
        """Migrate from legacy SQLite database."""
        if not legacy_db_path:
            legacy_db_path = os.getenv("LEGACY_DB_PATH", "wink_store.db")
        
        if not os.path.exists(legacy_db_path):
            logger.info("No legacy database found, skipping migration")
            return
        
        applied_migrations = self.get_applied_migrations()
        
        if "1.0.1" not in applied_migrations:
            logger.info("Running legacy migration (1.0.1)...")
            
            try:
                # Run the legacy migration
                self.db_manager.migrate_from_sqlite(legacy_db_path)
                
                # Mark migration as applied
                self.mark_migration_applied("1.0.1", "Migration from legacy SQLite database")
                
                logger.info("Legacy migration completed successfully")
                
            except Exception as e:
                logger.error(f"Legacy migration failed: {e}")
                raise
        else:
            logger.info("Legacy migration already applied")
    
    def run_all_migrations(self):
        """Run all pending migrations."""
        logger.info("Starting database migrations...")
        
        # Run initial migration
        self.run_initial_migration()
        
        # Run legacy migration if needed
        if os.getenv("MIGRATE_FROM_LEGACY", "false").lower() == "true":
            self.migrate_from_legacy()
        
        logger.info("All migrations completed")
    
    def get_schema_version(self) -> str:
        """Get the current schema version."""
        applied_migrations = self.get_applied_migrations()
        if applied_migrations:
            return applied_migrations[-1]
        return "0.0.0"

# Global migration manager
migration_manager = MigrationManager()

def run_migrations():
    """Run all database migrations."""
    migration_manager.run_all_migrations()

def get_schema_version() -> str:
    """Get the current database schema version."""
    return migration_manager.get_schema_version()