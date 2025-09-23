
import os, time, cv2, numpy as np, json
from datetime import datetime, timezone
from ultralytics import YOLO
import redis
from ..database.db_manager import db
from ..core.store_scope import current_store_id
from ..core.zone_manager import ZoneManager

MODEL_DEVICE=os.getenv("MODEL_DEVICE","cpu")
REDIS_URL=os.getenv("REDIS_URL","redis://localhost:6379")

class EnhancedCentroidTracker:
    def __init__(self, camera_id):
        self.camera_id = camera_id
        self.next_id = 1
        self.tracks = {}  # id -> {cx, cy, last_ts, entry_time, zones_history, dwell_start}
        self.redis_client = redis.from_url(REDIS_URL) if REDIS_URL else None
        self.track_timeout = 10  # seconds
        self.max_distance = 75  # pixels
        
    def update(self, dets):
        assigned = set()
        out = []
        now = time.time()
        
        # Match detections to existing tracks
        for cx, cy, w, h in dets:
            best_track = None
            best_distance = float('inf')
            
            for tid, track_data in self.tracks.items():
                if tid in assigned:
                    continue
                    
                # Calculate distance with velocity prediction
                dt = now - track_data['last_ts']
                predicted_cx = track_data['cx'] + track_data.get('vx', 0) * dt
                predicted_cy = track_data['cy'] + track_data.get('vy', 0) * dt
                
                distance = np.sqrt((predicted_cx - cx)**2 + (predicted_cy - cy)**2)
                
                if distance < best_distance and distance < self.max_distance:
                    best_distance = distance
                    best_track = tid
            
            if best_track is not None:
                # Update existing track
                assigned.add(best_track)
                old_track = self.tracks[best_track]
                
                # Calculate velocity
                dt = now - old_track['last_ts']
                vx = (cx - old_track['cx']) / max(dt, 0.1)
                vy = (cy - old_track['cy']) / max(dt, 0.1)
                
                self.tracks[best_track].update({
                    'cx': cx, 'cy': cy, 'last_ts': now,
                    'vx': vx, 'vy': vy
                })
                
                out.append((best_track, cx, cy, w, h))
                
                # Update Redis with live tracking data
                if self.redis_client:
                    self._update_redis_track(best_track, cx, cy, w, h)
            else:
                # Create new track
                tid = self.next_id
                self.next_id += 1
                
                self.tracks[tid] = {
                    'cx': cx, 'cy': cy, 'last_ts': now,
                    'entry_time': now, 'zones_history': [],
                    'dwell_start': {}, 'vx': 0, 'vy': 0,
                    'total_dwell': 0, 'queue_entries': []
                }
                
                out.append((tid, cx, cy, w, h))
                
                # Update Redis
                if self.redis_client:
                    self._update_redis_track(tid, cx, cy, w, h)
        
        # Remove expired tracks
        expired_tracks = []
        for tid, track_data in self.tracks.items():
            if now - track_data['last_ts'] > self.track_timeout:
                expired_tracks.append(tid)
                
                # Log track completion to database
                self._finalize_track(tid, track_data)
                
                # Remove from Redis
                if self.redis_client:
                    self.redis_client.delete(f"track:{self.camera_id}:{tid}")
        
        for tid in expired_tracks:
            del self.tracks[tid]
        
        return out
    
    def _update_redis_track(self, track_id, cx, cy, w, h):
        """Update real-time tracking data in Redis"""
        try:
            track_data = {
                'camera_id': self.camera_id,
                'track_id': track_id,
                'cx': cx, 'cy': cy, 'w': w, 'h': h,
                'timestamp': time.time(),
                'zones': list(self.tracks[track_id].get('current_zones', []))
            }
            
            self.redis_client.setex(
                f"track:{self.camera_id}:{track_id}",
                30,  # 30 second expiry
                json.dumps(track_data)
            )
            
            # Update live metrics
            self.redis_client.incr(f"live_count:{self.camera_id}")
            self.redis_client.expire(f"live_count:{self.camera_id}", 60)
            
        except Exception as e:
            print(f"Redis update error: {e}")
    
    def _finalize_track(self, track_id, track_data):
        """Log completed track metrics to database"""
        try:
            sid = current_store_id()
            total_time = time.time() - track_data['entry_time']
            
            with db.transaction() as conn:
                c = conn.cursor()
                c.execute("""
                    INSERT INTO track_sessions 
                    (store_id, camera_id, track_id, entry_time, exit_time, total_dwell, 
                     zones_visited, queue_events, created_at)
                    VALUES (?,?,?,?,?,?,?,?,?)
                """, (
                    sid, self.camera_id, track_id,
                    datetime.fromtimestamp(track_data['entry_time']).isoformat(),
                    datetime.now().isoformat(),
                    track_data.get('total_dwell', 0),
                    json.dumps(track_data.get('zones_history', [])),
                    json.dumps(track_data.get('queue_entries', [])),
                    datetime.now().isoformat()
                ))
                conn.commit()
        except Exception as e:
            print(f"Track finalization error: {e}")
    
    def get_track_dwell_time(self, track_id, zone_name):
        """Get current dwell time for a track in a specific zone"""
        if track_id not in self.tracks:
            return 0
            
        dwell_start = self.tracks[track_id]['dwell_start'].get(zone_name)
        if dwell_start:
            return time.time() - dwell_start
        return 0
    
    def update_zone_presence(self, track_id, current_zones, previous_zones):
        """Update zone presence and calculate dwell times"""
        if track_id not in self.tracks:
            return
            
        track = self.tracks[track_id]
        now = time.time()
        
        # Handle zone exits (calculate dwell time)
        for zone in previous_zones:
            if zone not in current_zones and zone in track['dwell_start']:
                dwell_duration = now - track['dwell_start'][zone]
                track['total_dwell'] += dwell_duration
                del track['dwell_start'][zone]
                
                # Log zone session
                track['zones_history'].append({
                    'zone': zone,
                    'enter_time': track['dwell_start'].get(zone, now),
                    'exit_time': now,
                    'duration': dwell_duration
                })
        
        # Handle zone entries
        for zone in current_zones:
            if zone not in previous_zones:
                track['dwell_start'][zone] = now
        
        track['current_zones'] = current_zones

def _publish_event(camera_id, zone_id, event_type, value, person_id, ts):
    sid=current_store_id()
    with db.transaction() as conn:
        c=conn.cursor()
        c.execute("""INSERT INTO zone_events (store_id,camera_id,zone_id,event_type,value,person_id,ts)
                     VALUES (?,?,?,?,?,?,?)""",(sid,camera_id,zone_id,event_type,value,person_id,ts))
        conn.commit()

def _mark_unique(camera_id, person_id, ymd):
    sid=current_store_id()
    with db.transaction() as conn:
        c=conn.cursor()
        c.execute("""INSERT OR IGNORE INTO unique_daily (store_id,camera_id,ymd,person_id) VALUES (?,?,?,?)""",(sid,camera_id,ymd,person_id))
        conn.commit()

def _flush_hour(camera_id, hour_key, metrics):
    sid=current_store_id()
    import json
    with db.transaction() as conn:
        c=conn.cursor()
        c.execute("""INSERT INTO hourly_metrics (store_id,camera_id,hour_start,footfall,unique_visitors,dwell_avg,dwell_p95,queue_wait_avg,interactions,zones_json)
                     VALUES (?,?,?,?,?,?,?,?,?,?)""",
                  (sid,camera_id,hour_key,metrics.get("footfall",0),metrics.get("unique_visitors",0),
                   metrics.get("dwell_avg",0.0),metrics.get("dwell_p95",0.0),
                   metrics.get("queue_wait_avg",0.0),metrics.get("interactions",0),json.dumps(metrics.get("zones",{}))))
        conn.commit()

class QueueManager:
    def __init__(self, camera_id):
        self.camera_id = camera_id
        self.queue_entries = {}  # track_id -> entry_time
        self.queue_wait_times = []
        
    def track_queue_entry(self, track_id, zone_type):
        """Track when someone enters a queue zone"""
        if zone_type == "queue" and track_id not in self.queue_entries:
            self.queue_entries[track_id] = time.time()
    
    def track_queue_exit(self, track_id, zone_type):
        """Track when someone exits a queue zone"""
        if zone_type == "queue" and track_id in self.queue_entries:
            wait_time = time.time() - self.queue_entries[track_id]
            self.queue_wait_times.append(wait_time)
            del self.queue_entries[track_id]
            return wait_time
        return 0
    
    def get_average_wait_time(self):
        """Get average queue wait time for current period"""
        if not self.queue_wait_times:
            return 0.0
        return float(np.mean(self.queue_wait_times))
    
    def reset_period(self):
        """Reset queue metrics for new time period"""
        self.queue_wait_times = []

def run_camera(camera_id: int, rtsp_url: str):
    cap = cv2.VideoCapture(rtsp_url)
    
    # Initialize camera with auto-reconnect
    retry_count = 0
    max_retries = 5
    
    while not cap.isOpened() and retry_count < max_retries:
        print(f"Attempting to connect to camera {camera_id} (attempt {retry_count + 1})")
        cap = cv2.VideoCapture(rtsp_url)
        retry_count += 1
        time.sleep(5)
    
    if not cap.isOpened():
        print(f"Failed to connect to camera {camera_id} after {max_retries} attempts")
        return
    
    # Enhanced initialization
    model = YOLO("yolov8n.pt")
    tracker = EnhancedCentroidTracker(camera_id)
    zm = ZoneManager(camera_id)
    queue_manager = QueueManager(camera_id)
    
    per_track_zones = {}
    hour_key = None
    metrics = {
        "footfall": 0, "unique_visitors": 0, "dwell_avg": 0.0, 
        "dwell_p95": 0.0, "queue_wait_avg": 0.0, "interactions": 0, 
        "zones": {}, "entrance_count": 0, "exit_count": 0
    }
    
    frame_count = 0
    detection_interval = max(1, int(os.getenv("DETECTION_INTERVAL", "3")))  # Process every Nth frame
    
    print(f"Started processing camera {camera_id}")
    
    while True:
        try:
            ok, frame = cap.read()
            if not ok:
                print(f"Failed to read frame from camera {camera_id}, reconnecting...")
                cap.release()
                time.sleep(2)
                cap = cv2.VideoCapture(rtsp_url)
                continue
                
            H, W = frame.shape[:2]
            now = datetime.now(timezone.utc)
            ymd = now.strftime("%Y-%m-%d")
            hk = now.strftime("%Y-%m-%dT%H:00:00")
            
            # Handle hour transition
            if hour_key is None:
                hour_key = hk
            elif hk != hour_key:
                # Calculate final metrics for the hour
                metrics["queue_wait_avg"] = queue_manager.get_average_wait_time()
                
                # Calculate unique visitors from tracker data
                unique_count = len([t for t in tracker.tracks.values() 
                                 if (now.timestamp() - t['entry_time']) < 3600])
                metrics["unique_visitors"] = unique_count
                
                # Calculate dwell statistics
                all_dwell_times = []
                for track in tracker.tracks.values():
                    if track.get('total_dwell', 0) > 0:
                        all_dwell_times.append(track['total_dwell'])
                
                if all_dwell_times:
                    metrics["dwell_avg"] = float(np.mean(all_dwell_times))
                    metrics["dwell_p95"] = float(np.percentile(all_dwell_times, 95))
                
                _flush_hour(camera_id, hour_key, metrics)
                
                # Reset for new hour
                hour_key = hk
                metrics = {
                    "footfall": 0, "unique_visitors": 0, "dwell_avg": 0.0,
                    "dwell_p95": 0.0, "queue_wait_avg": 0.0, "interactions": 0,
                    "zones": {}, "entrance_count": 0, "exit_count": 0
                }
                queue_manager.reset_period()
            
            # Skip detection for performance optimization
            frame_count += 1
            if frame_count % detection_interval != 0:
                time.sleep(1/max(1, int(os.getenv("FRAME_RATE", "12"))))
                continue
            
            # Person detection
            res = model(frame, imgsz=640, device=MODEL_DEVICE, verbose=False)
            dets = []
            
            for r in res:
                for b in r.boxes:
                    if int(b.cls[0].item()) != 0:  # Only persons
                        continue
                    
                    conf = float(b.conf[0].item())
                    if conf < 0.5:  # Confidence threshold
                        continue
                        
                    x1, y1, x2, y2 = b.xyxy[0].tolist()
                    w, h = x2 - x1, y2 - y1
                    cx, cy = x1 + w/2, y1 + h/2
                    
                    # Filter out very small detections (likely false positives)
                    if w * h < 1000:  # Minimum area threshold
                        continue
                        
                    dets.append((cx, cy, w, h))
            
            # Update tracking
            tracks = tracker.update(dets)
            
            # Process each tracked person
            for tid, cx, cy, w, h in tracks:
                _mark_unique(camera_id, str(tid), ymd)
                
                # Zone classification
                hits = zm.classify(W, H, cx, cy)
                current_zones = set([h["name"] for h in hits])
                previous_zones = set(per_track_zones.get(tid, []))
                
                # Update tracker with zone information
                tracker.update_zone_presence(tid, current_zones, previous_zones)
                
                # Zone transition events
                for zone_name in (current_zones - previous_zones):
                    _publish_event(camera_id, zone_name, "enter", 1, str(tid), now.isoformat())
                    
                    # Handle specific zone types
                    zone_info = next((z for z in hits if z["name"] == zone_name), None)
                    if zone_info:
                        zone_type = zone_info["ztype"]
                        
                        # Queue management
                        if zone_type == "queue":
                            queue_manager.track_queue_entry(tid, zone_type)
                        
                        # Entrance tracking
                        elif zone_type == "entry":
                            if tid not in [t for t in per_track_zones.keys()]:
                                metrics["footfall"] += 1
                                metrics["entrance_count"] += 1
                
                for zone_name in (previous_zones - current_zones):
                    _publish_event(camera_id, zone_name, "exit", 1, str(tid), now.isoformat())
                    
                    # Handle queue exits
                    zone_exits = zm.classify(W, H, cx, cy)  # Get zone info for exits
                    for zone_info in zone_exits:
                        if zone_info["name"] == zone_name and zone_info["ztype"] == "queue":
                            wait_time = queue_manager.track_queue_exit(tid, "queue")
                
                # Continuous presence events
                for zone_name in current_zones:
                    _publish_event(camera_id, zone_name, "presence", 1, str(tid), now.isoformat())
                
                # Interaction tracking
                shelf_interactions = any(z["ztype"] == "shelf" for z in hits)
                if shelf_interactions:
                    metrics["interactions"] += 1
                
                # Zone-specific metrics
                for zone_hit in hits:
                    zone_name = zone_hit["name"]
                    metrics["zones"][zone_name] = metrics["zones"].get(zone_name, 0) + 1
                
                # Update zone tracking
                per_track_zones[tid] = current_zones
            
            # Performance monitoring
            if frame_count % 100 == 0:
                active_tracks = len(tracker.tracks)
                print(f"Camera {camera_id}: {active_tracks} active tracks, Frame {frame_count}")
            
        except Exception as e:
            print(f"Error processing camera {camera_id}: {e}")
            time.sleep(1)
            continue
        
        # Frame rate control
        time.sleep(1/max(1, int(os.getenv("FRAME_RATE", "12"))))
