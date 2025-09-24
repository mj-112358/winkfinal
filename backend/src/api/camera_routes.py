"""
Camera management routes for the Wink platform.
Handles camera CRUD operations, status monitoring, and processor management.
"""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel

from ..auth.middleware import get_current_user, require_manager, get_store_context
from ..database.database import get_db_session
from ..database.models import User, Camera
from ..services.camera_processor import start_camera_processor, stop_camera_processor, get_camera_status

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/cameras", tags=["cameras"])

# Request/Response models
class CameraCreateRequest(BaseModel):
    name: str
    rtsp_url: str
    section: Optional[str] = None

class CameraUpdateRequest(BaseModel):
    name: Optional[str] = None
    rtsp_url: Optional[str] = None
    section: Optional[str] = None

class CameraResponse(BaseModel):
    id: str
    name: str
    rtsp_url: str
    section: Optional[str]
    status: str
    last_heartbeat_at: Optional[str]
    last_error: Optional[str]
    created_at: str

class CameraHealthResponse(BaseModel):
    id: str
    status: str
    last_heartbeat_at: Optional[str]
    last_error: Optional[str]
    processor_running: bool
    stream_info: Optional[Dict[str, Any]]

@router.get("", response_model=List[CameraResponse])
async def list_cameras(
    user: User = Depends(require_manager()),
    db: Session = Depends(get_db_session),
    store_id: str = Depends(get_store_context)
):
    """Get all cameras for the current store."""
    cameras = db.query(Camera).filter(Camera.store_id == store_id).all()
    
    return [
        CameraResponse(
            id=str(camera.id),
            name=camera.name,
            rtsp_url=camera.rtsp_url,
            section=camera.section,
            status=camera.status,
            last_heartbeat_at=camera.last_heartbeat_at.isoformat() if camera.last_heartbeat_at else None,
            last_error=camera.last_error,
            created_at=camera.created_at.isoformat()
        )
        for camera in cameras
    ]

@router.post("", response_model=CameraResponse)
async def create_camera(
    request: CameraCreateRequest,
    background_tasks: BackgroundTasks,
    user: User = Depends(require_manager()),
    db: Session = Depends(get_db_session),
    store_id: str = Depends(get_store_context)
):
    """Create a new camera and start its processor."""
    # Validate RTSP URL format
    if not request.rtsp_url.startswith(('rtsp://', 'rtmp://', 'http://', 'https://')):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid RTSP URL format"
        )
    
    # Create camera
    camera = Camera(
        store_id=store_id,
        name=request.name,
        rtsp_url=request.rtsp_url,
        section=request.section,
        status="connecting"
    )
    
    db.add(camera)
    db.commit()
    db.refresh(camera)
    
    # Start camera processor in background
    background_tasks.add_task(
        start_camera_processor,
        camera_id=str(camera.id),
        rtsp_url=request.rtsp_url,
        store_id=store_id
    )
    
    logger.info(f"Created camera {camera.name} for store {store_id}")
    
    return CameraResponse(
        id=str(camera.id),
        name=camera.name,
        rtsp_url=camera.rtsp_url,
        section=camera.section,
        status=camera.status,
        last_heartbeat_at=None,
        last_error=None,
        created_at=camera.created_at.isoformat()
    )

@router.get("/{camera_id}", response_model=CameraResponse)
async def get_camera(
    camera_id: str,
    user: User = Depends(require_manager()),
    db: Session = Depends(get_db_session),
    store_id: str = Depends(get_store_context)
):
    """Get a specific camera by ID."""
    camera = db.query(Camera).filter(
        Camera.id == camera_id,
        Camera.store_id == store_id
    ).first()
    
    if not camera:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Camera not found"
        )
    
    return CameraResponse(
        id=str(camera.id),
        name=camera.name,
        rtsp_url=camera.rtsp_url,
        section=camera.section,
        status=camera.status,
        last_heartbeat_at=camera.last_heartbeat_at.isoformat() if camera.last_heartbeat_at else None,
        last_error=camera.last_error,
        created_at=camera.created_at.isoformat()
    )

@router.put("/{camera_id}", response_model=CameraResponse)
async def update_camera(
    camera_id: str,
    request: CameraUpdateRequest,
    background_tasks: BackgroundTasks,
    user: User = Depends(require_manager()),
    db: Session = Depends(get_db_session),
    store_id: str = Depends(get_store_context)
):
    """Update a camera's configuration."""
    camera = db.query(Camera).filter(
        Camera.id == camera_id,
        Camera.store_id == store_id
    ).first()
    
    if not camera:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Camera not found"
        )
    
    # Check if RTSP URL is being changed
    rtsp_changed = request.rtsp_url and request.rtsp_url != camera.rtsp_url
    
    # Update camera fields
    if request.name is not None:
        camera.name = request.name
    if request.rtsp_url is not None:
        camera.rtsp_url = request.rtsp_url
    if request.section is not None:
        camera.section = request.section
    
    # If RTSP URL changed, restart processor
    if rtsp_changed:
        camera.status = "connecting"
        camera.last_error = None
        
        # Stop old processor and start new one
        background_tasks.add_task(stop_camera_processor, camera_id)
        background_tasks.add_task(
            start_camera_processor,
            camera_id=camera_id,
            rtsp_url=request.rtsp_url,
            store_id=store_id
        )
    
    db.commit()
    db.refresh(camera)
    
    return CameraResponse(
        id=str(camera.id),
        name=camera.name,
        rtsp_url=camera.rtsp_url,
        section=camera.section,
        status=camera.status,
        last_heartbeat_at=camera.last_heartbeat_at.isoformat() if camera.last_heartbeat_at else None,
        last_error=camera.last_error,
        created_at=camera.created_at.isoformat()
    )

@router.delete("/{camera_id}")
async def delete_camera(
    camera_id: str,
    background_tasks: BackgroundTasks,
    user: User = Depends(require_manager()),
    db: Session = Depends(get_db_session),
    store_id: str = Depends(get_store_context)
):
    """Delete a camera and stop its processor."""
    camera = db.query(Camera).filter(
        Camera.id == camera_id,
        Camera.store_id == store_id
    ).first()
    
    if not camera:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Camera not found"
        )
    
    # Stop camera processor
    background_tasks.add_task(stop_camera_processor, camera_id)
    
    # Delete camera
    db.delete(camera)
    db.commit()
    
    logger.info(f"Deleted camera {camera.name} for store {store_id}")
    
    return {"message": "Camera deleted successfully"}

@router.get("/{camera_id}/health", response_model=CameraHealthResponse)
async def get_camera_health(
    camera_id: str,
    user: User = Depends(require_manager()),
    db: Session = Depends(get_db_session),
    store_id: str = Depends(get_store_context)
):
    """Get detailed health information for a camera."""
    camera = db.query(Camera).filter(
        Camera.id == camera_id,
        Camera.store_id == store_id
    ).first()
    
    if not camera:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Camera not found"
        )
    
    # Get processor status
    processor_status = get_camera_status(camera_id)
    
    return CameraHealthResponse(
        id=str(camera.id),
        status=camera.status,
        last_heartbeat_at=camera.last_heartbeat_at.isoformat() if camera.last_heartbeat_at else None,
        last_error=camera.last_error,
        processor_running=processor_status.get("running", False),
        stream_info=processor_status.get("stream_info")
    )

@router.post("/{camera_id}/restart")
async def restart_camera(
    camera_id: str,
    background_tasks: BackgroundTasks,
    user: User = Depends(require_manager()),
    db: Session = Depends(get_db_session),
    store_id: str = Depends(get_store_context)
):
    """Restart a camera's processor."""
    camera = db.query(Camera).filter(
        Camera.id == camera_id,
        Camera.store_id == store_id
    ).first()
    
    if not camera:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Camera not found"
        )
    
    # Update status
    camera.status = "connecting"
    camera.last_error = None
    db.commit()
    
    # Restart processor
    background_tasks.add_task(stop_camera_processor, camera_id)
    background_tasks.add_task(
        start_camera_processor,
        camera_id=camera_id,
        rtsp_url=camera.rtsp_url,
        store_id=store_id
    )
    
    return {"message": "Camera restart initiated"}

@router.post("/{camera_id}/test")
async def test_camera_connection(
    camera_id: str,
    user: User = Depends(require_manager()),
    db: Session = Depends(get_db_session),
    store_id: str = Depends(get_store_context)
):
    """Test camera RTSP connection without starting full processor."""
    camera = db.query(Camera).filter(
        Camera.id == camera_id,
        Camera.store_id == store_id
    ).first()
    
    if not camera:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Camera not found"
        )
    
    # Test connection (this would be implemented in the camera service)
    try:
        # Placeholder for actual connection test
        connection_test = await test_rtsp_connection(camera.rtsp_url)
        
        return {
            "camera_id": camera_id,
            "rtsp_url": camera.rtsp_url,
            "connection_status": "success" if connection_test else "failed",
            "test_timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Camera connection test failed: {e}")
        return {
            "camera_id": camera_id,
            "rtsp_url": camera.rtsp_url,
            "connection_status": "error",
            "error": str(e),
            "test_timestamp": datetime.utcnow().isoformat()
        }

# Placeholder function for RTSP connection testing
async def test_rtsp_connection(rtsp_url: str) -> bool:
    """Test RTSP connection (placeholder implementation)."""
    # This would use OpenCV or similar to test the connection
    # For now, just return True
    logger.info(f"Testing RTSP connection to: {rtsp_url}")
    return True