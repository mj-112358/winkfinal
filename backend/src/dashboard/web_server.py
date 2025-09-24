
import os, json
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from dotenv import load_dotenv

from ..database.db_manager import db, migrate_all, set_local_store
from ..core.store_scope import current_store_id
from ..analytics.analytics_engine import recompute_daily_store_metrics

load_dotenv()

app=FastAPI(title="WINK Store Backend")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def boot():
    migrate_all()
    set_local_store(os.getenv("STORE_ID","unknown_store"), os.getenv("STORE_NAME","Unknown Store"))
    Path(os.getenv("ASSETS_DIR","assets")).mkdir(parents=True, exist_ok=True)

# ---- Camera management ----
class CameraIn(BaseModel):
    name:str; rtsp_url:str; enabled:bool=True

@app.get("/api/cameras")
async def list_cameras():
    sid=current_store_id()
    with db.transaction() as conn:
        c=conn.cursor(); c.execute("SELECT id,name,rtsp_url,enabled FROM cameras WHERE store_id=?", (sid,))
        return [{"id":r[0],"name":r[1],"rtsp_url":r[2],"enabled":bool(r[3])} for r in c.fetchall()]

@app.post("/api/cameras")
async def add_camera(cam:CameraIn):
    sid=current_store_id()
    with db.transaction() as conn:
        c=conn.cursor(); c.execute("INSERT INTO cameras (store_id,name,rtsp_url,enabled) VALUES (?,?,?,?)",(sid,cam.name,cam.rtsp_url,1 if cam.enabled else 0))
        conn.commit(); return {"id": c.lastrowid}

@app.delete("/api/cameras/{camera_id}")
async def del_camera(camera_id:int):
    sid=current_store_id()
    with db.transaction() as conn:
        c=conn.cursor(); c.execute("DELETE FROM cameras WHERE id=? AND store_id=?", (camera_id,sid))
        conn.commit(); return {"status":"ok"}

# ---- Zones & screenshots ----
ASSETS_DIR=Path(os.getenv("ASSETS_DIR","assets")).resolve()

@app.post("/api/zones/screenshot")
async def upload_screenshot(camera_id:int=Form(...), file:UploadFile=File(...), img_width:int=Form(...), img_height:int=Form(...)):
    sid=current_store_id()
    out=ASSETS_DIR/"zones"/sid/str(camera_id); out.mkdir(parents=True, exist_ok=True)
    fp=out/"screenshot.png"; fp.write_bytes(await file.read())
    with db.transaction() as conn:
        c=conn.cursor()
        c.execute("""INSERT INTO zone_screenshots (store_id,camera_id,file_path,img_width,img_height)
                     VALUES (?,?,?,?,?)
                     ON CONFLICT(store_id,camera_id) DO UPDATE SET file_path=excluded.file_path,img_width=excluded.img_width,img_height=excluded.img_height""",
                  (sid,camera_id,str(fp),img_width,img_height))
        conn.commit()
    return {"status":"ok","path":str(fp)}

@app.get("/api/zones")
async def list_zones(camera_id: Optional[int] = Query(None)):
    sid = current_store_id()
    with db.transaction() as conn:
        c = conn.cursor()
        
        if camera_id is not None:
            # Get zones for specific camera
            c.execute("SELECT file_path,img_width,img_height FROM zone_screenshots WHERE store_id=? AND camera_id=?", (sid, camera_id))
            shot = c.fetchone()
            c.execute("SELECT id,name,ztype,polygon_json FROM zones WHERE store_id=? AND camera_id=?", (sid, camera_id))
            zones = [{"id": r[0], "name": r[1], "ztype": r[2], "polygon": json.loads(r[3])} for r in c.fetchall()]
            return {"screenshot": {"path": shot[0] if shot else None, "width": shot[1] if shot else None, "height": shot[2] if shot else None}, "zones": zones}
        else:
            # Get all zones for this store
            c.execute("SELECT id,name,ztype,polygon_json FROM zones WHERE store_id=?", (sid,))
            zones = [{"id": r[0], "name": r[1], "ztype": r[2], "polygon": json.loads(r[3])} for r in c.fetchall()]
            return {"screenshot": None, "zones": zones}

@app.post("/api/zones")
async def add_zone(camera_id:int=Form(...), name:str=Form(...), ztype:str=Form(...), polygon_json:str=Form(...)):
    sid=current_store_id()
    coords=json.loads(polygon_json)
    with db.transaction() as conn:
        c=conn.cursor()
        c.execute("INSERT INTO zones (store_id,camera_id,name,ztype,polygon_json) VALUES (?,?,?,?,?)",(sid,camera_id,name,ztype,json.dumps(coords)))
        conn.commit(); return {"id": c.lastrowid}

@app.delete("/api/zones/{zone_id}")
async def delete_zone(zone_id:int):
    sid=current_store_id()
    with db.transaction() as conn:
        c=conn.cursor(); c.execute("DELETE FROM zones WHERE id=? AND store_id=?", (zone_id,sid))
        conn.commit(); return {"status":"ok"}

@app.get("/api/zones/overlay")
async def overlay(camera_id:int):
    from PIL import Image, ImageDraw
    sid=current_store_id()
    with db.transaction() as conn:
        c=conn.cursor()
        c.execute("SELECT file_path FROM zone_screenshots WHERE store_id=? AND camera_id=?", (sid,camera_id))
        row=c.fetchone()
        if not row: raise HTTPException(status_code=404, detail="Upload screenshot first")
        c.execute("SELECT name,ztype,polygon_json FROM zones WHERE store_id=? AND camera_id=?", (sid,camera_id))
        zones=[(r[0],r[1],json.loads(r[2])) for r in c.fetchall()]
    img=Image.open(row[0]).convert("RGB")
    draw=ImageDraw.Draw(img,"RGBA")
    for name,zt,poly in zones:
        draw.polygon([(p[0],p[1]) for p in poly], outline=(0,255,0,255), fill=(0,255,0,60))
        draw.text((poly[0][0]+3, poly[0][1]+3), f"{name} ({zt})", fill=(255,255,255,255))
    out=ASSETS_DIR/"zones"/sid/str(camera_id)/"overlay.png"; out.parent.mkdir(parents=True,exist_ok=True); img.save(out,"PNG")
    return FileResponse(str(out))

# ---- Metrics read ----
@app.get("/api/metrics/hourly")
async def metrics_hourly(start:str, end:str):
    sid=current_store_id()
    with db.transaction() as conn:
        c=conn.cursor()
        c.execute("""SELECT camera_id,hour_start,footfall,unique_visitors,dwell_avg,dwell_p95,queue_wait_avg,interactions,zones_json
                     FROM hourly_metrics WHERE store_id=? AND hour_start BETWEEN ? AND ? ORDER BY hour_start""",(sid,start,end))
        rows=c.fetchall()
    out=[]; import json as _j
    for r in rows:
        out.append({"camera_id":r[0],"hour_start":r[1],"footfall":r[2],"unique_visitors":r[3],
                    "dwell_avg":r[4],"dwell_p95":r[5],"queue_wait_avg":r[6],"interactions":r[7],
                    "zones": _j.loads(r[8]) if r[8] else {}})
    return out

@app.get("/api/metrics/daily")
async def metrics_daily(days:int=7):
    sid=current_store_id()
    with db.transaction() as conn:
        c=conn.cursor()
        c.execute("""SELECT date,dwell_avg,queue_wait_avg,interactions,peak_hour
                     FROM daily_store_metrics WHERE store_id=? ORDER BY date DESC LIMIT ?""",(sid,days))
        rows=c.fetchall()
    return [{"date":r[0],"dwell_avg":r[1],"queue_wait_avg":r[2],"interactions":r[3],"peak_hour":r[4]} for r in rows]

@app.get("/api/metrics/daily_by_camera")
async def metrics_daily_by_camera(days:int=7):
    sid=current_store_id()
    with db.transaction() as conn:
        c=conn.cursor()
        c.execute("""SELECT DISTINCT substr(hour_start,1,10) d FROM hourly_metrics WHERE store_id=? ORDER BY d DESC LIMIT ?""",(sid,days))
        days_list=[r[0] for r in c.fetchall()][::-1]
        out={}
        for d in days_list:
            s=f"{d}T00:00:00"; e=f"{d}T23:59:59"
            c.execute("""SELECT camera_id, SUM(footfall), json_group_array(zones_json)
                         FROM hourly_metrics WHERE store_id=? AND hour_start BETWEEN ? AND ? GROUP BY camera_id""",(sid,s,e))
            rows=c.fetchall(); import json as _j
            for cam,foot,zagg in rows:
                zones={}
                if zagg:
                    try: arr=_j.loads(zagg) if isinstance(zagg,str) else zagg
                    except: arr=[]
                    if isinstance(arr,list):
                        for z in arr:
                            if not z: continue
                            if isinstance(z,str):
                                try: z=_j.loads(z)
                                except: z={}
                            for k,v in (z or {}).items(): zones[k]=zones.get(k,0)+(v or 0)
                out.setdefault(str(cam),[]).append({"date":d,"footfall":int(foot or 0),"zones":zones})
    return out

# ---- Enhanced Analytics and AI Insights ----
from ..analytics.analytics_engine import EnhancedAnalyticsEngine
from ..analytics.spike_detector import SpikeDetector

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

async def _openai_chat(payload: dict) -> str:
    from openai import OpenAI
    key = os.getenv("OPENAI_API_KEY")
    if not key: 
        return "OpenAI disabled (set OPENAI_API_KEY)."
    
    client = OpenAI(api_key=key)
    system = (
        "You are an expert retail analytics consultant. Analyze the provided data to give "
        "actionable insights about store performance, customer behavior, and operational optimization. "
        "Focus on: 1) Key performance trends, 2) Anomalies and their implications, "
        "3) Specific recommendations for improvement, 4) Zone-specific insights. "
        "Be concise but comprehensive. Use bullet points for clarity."
    )
    
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini", 
            temperature=0.3, 
            max_tokens=1200,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(payload, indent=2)}
            ]
        )
        return resp.choices[0].message.content or ""
    except Exception as e:
        return f"AI analysis failed: {str(e)}"

@app.post("/api/insights/weekly")
async def insights_weekly(req:InsightsRequest):
    sid=current_store_id()
    with db.transaction() as conn:
        c=conn.cursor()
        c.execute("""SELECT date,dwell_avg,queue_wait_avg,interactions,peak_hour FROM daily_store_metrics
                     WHERE store_id=? ORDER BY date DESC LIMIT ?""",(sid,req.period_weeks*7))
        recent=c.fetchall()
        c.execute("""SELECT date,dwell_avg,queue_wait_avg,interactions FROM daily_store_metrics
                     WHERE store_id=? ORDER BY date DESC LIMIT ?""",(sid,26*7))
        hist=c.fetchall()
        dates=[r[0] for r in recent]; per_camera={}
        for d in dates:
            s=f"{d}T00:00:00"; e=f"{d}T23:59:59"
            c.execute("""SELECT camera_id,SUM(footfall),json_group_array(zones_json) FROM hourly_metrics
                         WHERE store_id=? AND hour_start BETWEEN ? AND ? GROUP BY camera_id""",(sid,s,e))
            rows=c.fetchall(); import json as _j
            for cam,foot,zagg in rows:
                zmap={}
                if zagg:
                    try: arr=_j.loads(zagg) if isinstance(zagg, str) else zagg
                    except: arr=[]
                    if isinstance(arr,list):
                        for z in arr:
                            if not z: continue
                            if isinstance(z,str):
                                try: z=_j.loads(z)
                                except: z={}
                            for k,v in (z or {}).items(): zmap[k]=zmap.get(k,0)+(v or 0)
                per_camera.setdefault(str(cam),[]).append({"date":d,"footfall":int(foot or 0),"zones":zmap})
    payload={"store_id":sid,
             "recent":[{"date":a,"dwell_avg":b,"queue_wait_avg":c,"interactions":d,"peak_hour":e} for (a,b,c,d,e) in recent],
             "history":[{"date":a,"dwell_avg":b,"queue_wait_avg":c,"interactions":d} for (a,b,c,d) in hist],
             "per_camera":per_camera,
             "note":"Do NOT sum footfall/unique across cameras."}
    txt=await _openai_chat(payload)
    return {"insights":txt, "payload":payload}

class PeriodRequest(BaseModel):
    start_date:str; end_date:str; type:str  # promo | festival

@app.post("/api/insights/period")
async def insights_period(req:PeriodRequest):
    sid=current_store_id()
    with db.transaction() as conn:
        c=conn.cursor()
        c.execute("""SELECT date,dwell_avg,queue_wait_avg,interactions FROM daily_store_metrics
                     WHERE store_id=? AND date BETWEEN ? AND ? ORDER BY date""",(sid,req.start_date,req.end_date))
        period=[{"date":a,"dwell_avg":b,"queue_wait_avg":c,"interactions":d} for (a,b,c,d) in c.fetchall()]
        c.execute("""SELECT date,dwell_avg,queue_wait_avg,interactions FROM daily_store_metrics
                     WHERE store_id=? AND date < ? ORDER BY date DESC LIMIT ?""",(sid,req.start_date,len(period)))
        baseline=[{"date":a,"dwell_avg":b,"queue_wait_avg":c,"interactions":d} for (a,b,c,d) in c.fetchall()][::-1]
    payload={"store_id":sid,"type":req.type,"period":period,"baseline":baseline}
    txt=await _openai_chat(payload)
    return {"insights":txt,"period":period,"baseline":baseline}

class CombinedRequest(BaseModel):
    period_weeks:int=1
    promo_enabled:bool=False; promo_start:str|None=None; promo_end:str|None=None
    festival_enabled:bool=False; festival_start:str|None=None; festival_end:str|None=None

# ---- Enhanced Analytics Endpoints ----

@app.post("/api/analytics/comprehensive")
async def comprehensive_analytics(req: AnalyticsRequest):
    """Get comprehensive store analytics with trends and insights"""
    engine = EnhancedAnalyticsEngine()
    spike_detector = SpikeDetector()
    
    # Get store performance analysis
    performance = engine.analyze_store_performance(req.days)
    
    # Get recent anomalies
    anomalies = spike_detector.get_recent_anomalies(req.days)
    
    # Get zone analytics if requested
    zone_analytics = {}
    if req.include_zones:
        sid = current_store_id()
        with db.transaction() as conn:
            c = conn.cursor()
            c.execute("SELECT id FROM cameras WHERE store_id=? AND enabled=1", (sid,))
            cameras = [row[0] for row in c.fetchall()]
            
            for camera_id in cameras:
                zone_analytics[str(camera_id)] = engine.get_zone_performance_analysis(camera_id, req.days)
    
    # Prepare AI analysis payload
    ai_payload = {
        "store_performance": performance,
        "recent_anomalies": anomalies[:10],  # Latest 10 anomalies
        "zone_analytics": zone_analytics,
        "analysis_request": {
            "days_analyzed": req.days,
            "analysis_date": datetime.now().strftime("%Y-%m-%d")
        }
    }
    
    # Get AI insights
    ai_insights = await _openai_chat(ai_payload)
    
    return {
        "performance_analysis": performance,
        "anomalies": anomalies,
        "zone_analytics": zone_analytics,
        "ai_insights": ai_insights,
        "analysis_metadata": {
            "generated_at": datetime.now().isoformat(),
            "days_analyzed": req.days,
            "cameras_analyzed": len(zone_analytics)
        }
    }

from typing import Optional

@app.get("/api/analytics/spikes")
async def get_spike_analysis(date: Optional[str] = None):
    """Get spike detection analysis for a specific date"""
    spike_detector = SpikeDetector()
    
    if date is None:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    spikes = spike_detector.detect_hourly_spikes(date)
    
    # Get baseline metrics for context
    baselines = {
        "footfall": spike_detector.calculate_baseline_metrics("footfall"),
        "interactions": spike_detector.calculate_baseline_metrics("interactions"),
        "dwell_time": spike_detector.calculate_baseline_metrics("dwell_time")
    }
    
    return {
        "date": date,
        "spikes_detected": len(spikes),
        "spikes": spikes,
        "baselines": baselines,
        "analysis_summary": {
            "high_severity": len([s for s in spikes if s["severity"] == "high"]),
            "medium_severity": len([s for s in spikes if s["severity"] == "medium"]),
            "critical_severity": len([s for s in spikes if s["severity"] == "critical"])
        }
    }

@app.post("/api/events")
async def create_event(event: EventRequest):
    """Create a promotion/festival event for tracking"""
    sid = current_store_id()
    
    with db.transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO events (store_id, name, event_type, start_date, end_date, description)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (sid, event.name, event.event_type, event.start_date, event.end_date, event.description))
        
        event_id = c.lastrowid
        conn.commit()
    
    return {"id": event_id, "status": "created"}

@app.get("/api/events")
async def list_events():
    """List all events for the store"""
    sid = current_store_id()
    
    with db.transaction() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, name, event_type, start_date, end_date, description, created_at
            FROM events WHERE store_id=? ORDER BY start_date DESC
        """, (sid,))
        
        events = []
        for row in c.fetchall():
            events.append({
                "id": row[0],
                "name": row[1],
                "event_type": row[2],
                "start_date": row[3],
                "end_date": row[4],
                "description": row[5],
                "created_at": row[6]
            })
    
    return events

@app.post("/api/events/{event_id}/analyze")
async def analyze_event_impact(event_id: int):
    """Analyze the impact of a specific event"""
    sid = current_store_id()
    spike_detector = SpikeDetector()
    
    # Get event details
    with db.transaction() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT name, event_type, start_date, end_date 
            FROM events WHERE id=? AND store_id=?
        """, (event_id, sid))
        
        event_data = c.fetchone()
        if not event_data:
            raise HTTPException(status_code=404, detail="Event not found")
    
    name, event_type, start_date, end_date = event_data
    
    if event_type in ["promotion", "sale"]:
        impact = spike_detector.detect_promotion_impact(start_date, end_date)
    else:
        # For festivals, analyze as a single-day event
        festival_dates = [start_date]
        if start_date != end_date:
            # If multi-day festival, add end date
            festival_dates.append(end_date)
        impact = spike_detector.detect_festival_patterns(festival_dates)
    
    return {
        "event": {
            "id": event_id,
            "name": name,
            "type": event_type,
            "start_date": start_date,
            "end_date": end_date
        },
        "impact_analysis": impact
    }

@app.get("/api/analytics/realtime")
async def get_realtime_metrics():
    """Get real-time store metrics"""
    try:
        import redis
        redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))
        
        sid = current_store_id()
        
        # Get live camera counts
        live_metrics = {}
        with db.transaction() as conn:
            c = conn.cursor()
            c.execute("SELECT id, name FROM cameras WHERE store_id=? AND enabled=1", (sid,))
            cameras = c.fetchall()
            
            for camera_id, camera_name in cameras:
                try:
                    live_count = redis_client.get(f"live_count:{camera_id}")
                    if callable(getattr(live_count, "__await__", None)):
                        live_count = await live_count
                    count_value = int(live_count.decode() if live_count else 0) # type: ignore
                    live_metrics[str(camera_id)] = {
                        "camera_name": camera_name,
                        "live_count": count_value,
                        "last_updated": datetime.now(timezone.utc).isoformat()
                    }
                except:
                    live_metrics[str(camera_id)] = {
                        "camera_name": camera_name,
                        "live_count": 0,
                        "last_updated": datetime.now(timezone.utc).isoformat()
                    }
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "live_metrics": live_metrics,
            "total_live_count": sum(m["live_count"] for m in live_metrics.values())
        }
        
    except Exception as e:
        return {
            "error": "Real-time metrics unavailable",
            "reason": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@app.get("/api/analytics/alerts")
async def get_active_alerts():
    """Get active alerts for the store"""
    sid = current_store_id()
    
    with db.transaction() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, alert_type, severity, message, created_at
            FROM alerts WHERE store_id=? AND resolved=0 
            ORDER BY created_at DESC LIMIT 50
        """, (sid,))
        
        alerts = []
        for row in c.fetchall():
            alerts.append({
                "id": row[0],
                "alert_type": row[1],
                "severity": row[2],
                "message": row[3],
                "created_at": row[4]
            })
    
    return {"alerts": alerts, "alert_count": len(alerts)}

@app.post("/api/analytics/alerts/{alert_id}/resolve")
async def resolve_alert(alert_id: int):
    """Mark an alert as resolved"""
    sid = current_store_id()
    
    with db.transaction() as conn:
        c = conn.cursor()
        c.execute("""
            UPDATE alerts SET resolved=1, resolved_at=? 
            WHERE id=? AND store_id=?
        """, (datetime.now(timezone.utc).isoformat(), alert_id, sid))
        conn.commit()
    
    return {"status": "resolved"}

@app.get("/api/zones/{camera_id}/analytics")
async def get_zone_analytics(camera_id: int, days: int = 7):
    """Get detailed zone analytics for a camera"""
    engine = EnhancedAnalyticsEngine()
    zone_analytics = engine.get_zone_performance_analysis(camera_id, days)
    
    # Get zone configuration for additional context
    from ..core.zone_manager import EnhancedZoneManager
    zm = EnhancedZoneManager(camera_id)
    
    # Add zone configuration to analytics
    zone_config = {
        "total_zones": len(zm.zones),
        "zones_by_type": {},
        "zone_details": []
    }
    
    for zone in zm.zones:
        zone_type = zone["ztype"]
        zone_config["zones_by_type"][zone_type] = zone_config["zones_by_type"].get(zone_type, 0) + 1
        zone_config["zone_details"].append({
            "name": zone["name"],
            "type": zone["ztype"],
            "area": zone["area"],
            "priority": zone["priority"]
        })
    
    return {
        "camera_id": camera_id,
        "zone_configuration": zone_config,
        "performance_analysis": zone_analytics
    }

@app.post("/api/insights/combined")
async def insights_combined(req: CombinedRequest):
    base = await insights_weekly(InsightsRequest(period_weeks=req.period_weeks))
    extras = {}
    if req.promo_enabled and req.promo_start and req.promo_end:
        extras["promo"] = await insights_period(PeriodRequest(type="promo", start_date=req.promo_start, end_date=req.promo_end))
    if req.festival_enabled and req.festival_start and req.festival_end:
        extras["festival"] = await insights_period(PeriodRequest(type="festival", start_date=req.festival_start, end_date=req.festival_end))
    return {"weekly": base, "extras": extras}

@app.get("/")
def root():
    return {"ok": True, "service": "WINK Analytics Platform", "version": "2.0.0"}

@app.get("/healthz")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}