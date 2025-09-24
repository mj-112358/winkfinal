"""
Dashboard routes for the Wink platform.
Includes metrics, zones, analytics, and insights endpoints.
"""

import os
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, List
from uuid import UUID
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Query, Depends
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..auth.middleware import get_current_user, get_store_context
from ..database.database import get_db_session
from ..database.models import User, Store

router = APIRouter(prefix="/api", tags=["dashboard"])

# Pydantic models
class CameraIn(BaseModel):
    name: str
    rtsp_url: str
    enabled: bool = True

class InsightsRequest(BaseModel):
    period_weeks: int = 1

class EventRequest(BaseModel):
    name: str
    event_type: str  # promotion, festival, sale
    start_date: str
    end_date: str
    description: str = ""

class AnalyticsRequest(BaseModel):
    days: int = 30
    include_zones: bool = True
    include_trends: bool = True

class PeriodRequest(BaseModel):
    start_date: str
    end_date: str
    type: str  # promo | festival

class CombinedRequest(BaseModel):
    period_weeks: int = 1
    promo_enabled: bool = False
    promo_start: str | None = None
    promo_end: str | None = None
    festival_enabled: bool = False
    festival_start: str | None = None
    festival_end: str | None = None

# Helper function to get database connection (simplified)
def get_db_connection():
    """Get database connection - this should be replaced with proper database session"""
    # For now, return a mock database connection
    # In production, this would use the proper database connection
    from ..database.db_manager import db
    return db

def current_store_id():
    """Get current store ID - this should be replaced with proper store context"""
    return os.getenv("STORE_ID", "default_store")

# Camera management endpoints
@router.get("/cameras")
async def list_cameras(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Get all cameras for the current store"""
    try:
        # Use the database session to get cameras
        from ..database.models import Camera
        cameras = db.query(Camera).filter(Camera.store_id == str(user.store_id)).all()

        return [
            {
                "id": str(camera.id),
                "name": camera.name,
                "rtsp_url": camera.rtsp_url,
                "enabled": camera.status in ["live", "connecting"]  # Convert status to enabled
            }
            for camera in cameras
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.post("/cameras")
async def add_camera(
    cam: CameraIn,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Add a new camera"""
    try:
        from ..database.models import Camera
        new_camera = Camera(
            store_id=user.store_id,
            name=cam.name,
            rtsp_url=cam.rtsp_url,
            status="connecting" if cam.enabled else "offline"  # Convert enabled to status
        )
        db.add(new_camera)
        db.commit()
        db.refresh(new_camera)

        return {"id": str(new_camera.id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to add camera: {str(e)}")

@router.delete("/cameras/{camera_id}")
async def delete_camera(
    camera_id: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Delete a camera"""
    try:
        from ..database.models import Camera
        camera = db.query(Camera).filter(
            Camera.id == UUID(camera_id),
            Camera.store_id == user.store_id
        ).first()

        if not camera:
            raise HTTPException(status_code=404, detail="Camera not found")

        db.delete(camera)
        db.commit()

        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete camera: {str(e)}")

# Zone management endpoints
@router.get("/zones")
async def list_zones(
    camera_id: Optional[int] = Query(None),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Get zones for a camera or all zones for the store"""
    try:
        # This would need to be implemented with proper zone models
        # For now, return empty zones
        return {
            "screenshot": None,
            "zones": []
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get zones: {str(e)}")

@router.post("/zones")
async def add_zone(
    camera_id: int = Form(...),
    name: str = Form(...),
    ztype: str = Form(...),
    polygon_json: str = Form(...),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Add a new zone"""
    try:
        # This would need proper zone model implementation
        coords = json.loads(polygon_json)
        # For now, just return a mock ID
        return {"id": 1}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to add zone: {str(e)}")

@router.delete("/zones/{zone_id}")
async def delete_zone(
    zone_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Delete a zone"""
    try:
        # This would need proper zone model implementation
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete zone: {str(e)}")

# Metrics endpoints
@router.get("/metrics/hourly")
async def metrics_hourly(
    start: str,
    end: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Get hourly metrics"""
    try:
        # This would query the proper metrics tables
        return []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get hourly metrics: {str(e)}")

@router.get("/metrics/daily")
async def metrics_daily(
    days: int = 7,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Get daily metrics"""
    try:
        # For now, return empty metrics
        return []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get daily metrics: {str(e)}")

@router.get("/metrics/daily_by_camera")
async def metrics_daily_by_camera(
    days: int = 7,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Get daily metrics by camera"""
    try:
        return {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get camera metrics: {str(e)}")

# Analytics endpoints
@router.get("/analytics/realtime")
async def get_realtime_metrics(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Get real-time store metrics"""
    try:
        # Try to get real-time metrics from Redis if available
        try:
            import redis
            redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))

            # Get live camera counts
            live_metrics = {}
            from ..database.models import Camera
            cameras = db.query(Camera).filter(
                Camera.store_id == str(user.store_id),
                Camera.enabled == True
            ).all()

            for camera in cameras:
                try:
                    live_count = redis_client.get(f"live_count:{camera.id}")
                    count_value = int(live_count.decode() if live_count else 0)
                    live_metrics[str(camera.id)] = {
                        "camera_name": camera.name,
                        "live_count": count_value,
                        "last_updated": datetime.now(timezone.utc).isoformat()
                    }
                except:
                    live_metrics[str(camera.id)] = {
                        "camera_name": camera.name,
                        "live_count": 0,
                        "last_updated": datetime.now(timezone.utc).isoformat()
                    }

            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "live_metrics": live_metrics,
                "total_live_count": sum(m["live_count"] for m in live_metrics.values())
            }
        except:
            # Fall back to zero metrics if Redis is unavailable
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "live_metrics": {},
                "total_live_count": 0
            }
    except Exception as e:
        return {
            "error": "Real-time metrics unavailable",
            "reason": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@router.post("/analytics/comprehensive")
async def comprehensive_analytics(
    req: AnalyticsRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Get comprehensive store analytics"""
    try:
        # This would use the enhanced analytics engine
        return {
            "performance_analysis": {},
            "anomalies": [],
            "zone_analytics": {},
            "ai_insights": "Analytics engine not fully implemented yet",
            "analysis_metadata": {
                "generated_at": datetime.now().isoformat(),
                "days_analyzed": req.days,
                "cameras_analyzed": 0
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analytics error: {str(e)}")

@router.get("/analytics/spikes")
async def get_spike_analysis(
    date: Optional[str] = None,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Get spike detection analysis"""
    if date is None:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    return {
        "date": date,
        "spikes_detected": 0,
        "spikes": [],
        "baselines": {},
        "analysis_summary": {
            "high_severity": 0,
            "medium_severity": 0,
            "critical_severity": 0
        }
    }

@router.get("/analytics/alerts")
async def get_active_alerts(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Get active alerts"""
    return {"alerts": [], "alert_count": 0}

@router.post("/analytics/alerts/{alert_id}/resolve")
async def resolve_alert(
    alert_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Mark an alert as resolved"""
    return {"status": "resolved"}

# Events endpoints
@router.post("/events")
async def create_event(
    event: EventRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Create a promotion/festival event"""
    return {"id": 1, "status": "created"}

@router.get("/events")
async def list_events(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """List all events for the store"""
    return []

@router.post("/events/{event_id}/analyze")
async def analyze_event_impact(
    event_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Analyze event impact"""
    return {
        "event": {
            "id": event_id,
            "name": "Sample Event",
            "type": "promotion"
        },
        "impact_analysis": {}
    }

# Insights endpoints
@router.post("/insights/weekly")
async def insights_weekly(
    req: InsightsRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Get weekly insights"""
    return {
        "insights": "Weekly insights analysis would be generated here",
        "payload": {}
    }

@router.post("/insights/period")
async def insights_period(
    req: PeriodRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Get period-based insights"""
    return {
        "insights": "Period insights analysis would be generated here",
        "period": [],
        "baseline": []
    }

@router.post("/insights/combined")
async def insights_combined(
    req: CombinedRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Get combined insights"""
    base = await insights_weekly(InsightsRequest(period_weeks=req.period_weeks), user, db)
    extras = {}

    if req.promo_enabled and req.promo_start and req.promo_end:
        extras["promo"] = await insights_period(
            PeriodRequest(type="promo", start_date=req.promo_start, end_date=req.promo_end),
            user, db
        )

    if req.festival_enabled and req.festival_start and req.festival_end:
        extras["festival"] = await insights_period(
            PeriodRequest(type="festival", start_date=req.festival_start, end_date=req.festival_end),
            user, db
        )

    return {"weekly": base, "extras": extras}

# Zone analytics endpoints
@router.get("/zones/{camera_id}/analytics")
async def get_zone_analytics(
    camera_id: int,
    days: int = 7,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Get detailed zone analytics for a camera"""
    return {
        "camera_id": camera_id,
        "zone_configuration": {
            "total_zones": 0,
            "zones_by_type": {},
            "zone_details": []
        },
        "performance_analysis": {}
    }