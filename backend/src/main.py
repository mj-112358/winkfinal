"""
Main FastAPI application for Wink retail analytics platform.
Production-ready multi-tenant system with authentication and RTSP processing.
"""

import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Import database and auth components
from .database.database import get_database
from .database.migrations import run_migrations
from .services.camera_processor import cleanup_processors

# Import route modules
from .api.auth_routes import router as auth_router
from .api.camera_routes import router as camera_router
from .api.dashboard_routes import router as dashboard_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Starting Wink Analytics Platform...")
    
    # Run database migrations
    try:
        run_migrations()
        logger.info("Database migrations completed")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    
    # Startup complete
    logger.info("Application startup completed")
    
    yield
    
    # Shutdown cleanup
    logger.info("Shutting down application...")
    
    # Stop all camera processors
    try:
        await cleanup_processors()
        logger.info("Camera processors stopped")
    except Exception as e:
        logger.error(f"Error stopping processors: {e}")
    
    logger.info("Application shutdown completed")

# Create FastAPI app
app = FastAPI(
    title="Wink Analytics Platform",
    description="Production-ready retail analytics with person detection and zone analytics",
    version="2.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("ALLOWED_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth_router)
app.include_router(camera_router)
app.include_router(dashboard_router)

# Health check endpoints
@app.get("/")
async def root():
    """Root endpoint with service information."""
    return {
        "service": "Wink Analytics Platform",
        "version": "2.0.0",
        "status": "running",
        "features": [
            "Multi-tenant architecture",
            "JWT authentication",
            "Real-time person detection",
            "Zone analytics",
            "Auto-scaling camera processors"
        ]
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        # Check database connection
        from sqlalchemy import text
        db = get_database()
        with db.get_session() as session:
            # Simple query to test connection
            session.execute(text("SELECT 1"))
        
        return {
            "status": "healthy",
            "timestamp": "2025-09-23T07:00:00Z",
            "database": "connected",
            "version": "2.0.0"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Service unavailable: {str(e)}")

@app.get("/metrics")
async def metrics():
    """Metrics endpoint for monitoring."""
    # This would integrate with Prometheus metrics
    return {
        "active_cameras": 0,  # Would be calculated from database
        "active_processors": 0,  # Would be calculated from processor manager
        "total_events_today": 0,  # Would be calculated from events
        "uptime_seconds": 0  # Would be calculated from startup time
    }

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler for unhandled errors."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": "An unexpected error occurred"
        }
    )

if __name__ == "__main__":
    import uvicorn
    
    # Development server configuration
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
