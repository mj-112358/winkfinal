"""
Camera processor worker that handles person detection, tracking, and event generation.
This is the main worker process that runs for each camera.
"""

import os
import sys
import json
import time
import logging
import argparse
import signal
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

import cv2
import numpy as np
import redis
from ultralytics import YOLO
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PersonTracker:
    """Simple person tracking using centroid tracking."""
    
    def __init__(self, max_disappeared: int = 30):
        self.next_id = 0
        self.objects = {}
        self.disappeared = {}
        self.max_disappeared = max_disappeared
    
    def register(self, centroid: Tuple[int, int]) -> int:
        """Register a new object with the next available ID."""
        self.objects[self.next_id] = centroid
        self.disappeared[self.next_id] = 0
        self.next_id += 1
        return self.next_id - 1
    
    def deregister(self, object_id: int):
        """Deregister an object."""
        del self.objects[object_id]
        del self.disappeared[object_id]
    
    def update(self, detections: List[Tuple[int, int]]) -> Dict[int, Tuple[int, int]]:
        """Update tracker with new detections."""
        if len(detections) == 0:
            # Mark all existing objects as disappeared
            for object_id in list(self.disappeared.keys()):
                self.disappeared[object_id] += 1
                if self.disappeared[object_id] > self.max_disappeared:
                    self.deregister(object_id)
            return self.objects.copy()
        
        if len(self.objects) == 0:
            # Register all detections as new objects
            for i, detection in enumerate(detections):
                self.register(detection)
        else:
            # Compute distance between existing objects and new detections
            object_centroids = list(self.objects.values())
            object_ids = list(self.objects.keys())
            
            # Compute distance matrix
            distances = np.linalg.norm(
                np.array(object_centroids)[:, np.newaxis] - np.array(detections),
                axis=2
            )
            
            # Find minimum distance assignments
            rows = distances.min(axis=1).argsort()
            cols = distances.argmin(axis=1)[rows]
            
            used_row_indices = set()
            used_col_indices = set()
            
            # Update existing objects
            for (row, col) in zip(rows, cols):
                if row in used_row_indices or col in used_col_indices:
                    continue
                
                if distances[row, col] <= 50:  # Distance threshold
                    object_id = object_ids[row]
                    self.objects[object_id] = detections[col]
                    self.disappeared[object_id] = 0
                    
                    used_row_indices.add(row)
                    used_col_indices.add(col)
            
            # Handle unmatched detections and objects
            unused_row_indices = set(range(0, distances.shape[0])) - used_row_indices
            unused_col_indices = set(range(0, distances.shape[1])) - used_col_indices
            
            # If more objects than detections, mark objects as disappeared
            if distances.shape[0] >= distances.shape[1]:
                for row in unused_row_indices:
                    object_id = object_ids[row]
                    self.disappeared[object_id] += 1
                    
                    if self.disappeared[object_id] > self.max_disappeared:
                        self.deregister(object_id)
            
            # Register new detections
            else:
                for col in unused_col_indices:
                    self.register(detections[col])
        
        return self.objects.copy()

class ZoneManager:
    """Manages zone definitions and point-in-polygon detection."""
    
    def __init__(self, zones_config: List[Dict[str, Any]]):
        self.zones = {}
        for zone in zones_config:
            self.zones[zone["id"]] = {
                "name": zone["name"],
                "type": zone["ztype"],
                "polygon": np.array(zone["polygon"], dtype=np.int32)
            }
    
    def point_in_zone(self, point: Tuple[int, int], zone_id: str) -> bool:
        """Check if a point is inside a zone polygon."""
        if zone_id not in self.zones:
            return False
        
        polygon = self.zones[zone_id]["polygon"]
        return cv2.pointPolygonTest(polygon, point, False) >= 0
    
    def get_zones_for_point(self, point: Tuple[int, int]) -> List[str]:
        """Get all zones that contain the given point."""
        zones = []
        for zone_id in self.zones:
            if self.point_in_zone(point, zone_id):
                zones.append(zone_id)
        return zones

class CameraProcessor:
    """Main camera processor for person detection and tracking."""
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.camera_id = self.config["camera_id"]
        self.store_id = self.config["store_id"]
        self.rtsp_url = self.config["rtsp_url"]
        
        # Initialize components
        self.model = YOLO(self.config["model_path"])
        self.tracker = PersonTracker()
        self.redis_client = redis.from_url(self.config["redis_url"])
        
        # Database connection
        self.engine = create_engine(self.config["database_url"])
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        # Zone management (will be loaded from database)
        self.zone_manager = None
        self._load_zones()
        
        # State tracking
        self.person_states = {}  # Track person states for dwell time
        self.running = True
        
        # Performance metrics
        self.frame_count = 0
        self.detection_count = 0
        self.last_heartbeat = time.time()
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def _load_zones(self):
        """Load zone definitions from database."""
        try:
            with self.SessionLocal() as db:
                # This would query zones from the database
                # For now, use empty zones
                self.zone_manager = ZoneManager([])
                logger.info(f"Loaded {len(self.zone_manager.zones)} zones for camera {self.camera_id}")
        except Exception as e:
            logger.error(f"Failed to load zones: {e}")
            self.zone_manager = ZoneManager([])
    
    def _update_heartbeat(self):
        """Update camera heartbeat in database."""
        try:
            with self.SessionLocal() as db:
                # Update camera last_heartbeat_at
                # This would be the actual database update
                self.redis_client.set(f"heartbeat:{self.camera_id}", int(time.time()))
                logger.debug(f"Updated heartbeat for camera {self.camera_id}")
        except Exception as e:
            logger.error(f"Failed to update heartbeat: {e}")
    
    def _detect_persons(self, frame: np.ndarray) -> List[Tuple[int, int]]:
        """Detect persons in frame and return centroids."""
        try:
            results = self.model(frame, classes=[0], verbose=False)  # Class 0 is person
            centroids = []
            
            for result in results:
                boxes = result.boxes
                if boxes is not None:
                    for box in boxes:
                        x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                        confidence = box.conf[0].cpu().numpy()
                        
                        if confidence > 0.5:  # Confidence threshold
                            centroid_x = int((x1 + x2) / 2)
                            centroid_y = int((y1 + y2) / 2)
                            centroids.append((centroid_x, centroid_y))
            
            return centroids
            
        except Exception as e:
            logger.error(f"Detection error: {e}")
            return []
    
    def _process_tracking_results(self, tracked_objects: Dict[int, Tuple[int, int]]):
        """Process tracking results and generate events."""
        current_time = datetime.utcnow()
        
        for person_id, centroid in tracked_objects.items():
            person_uuid = f"{self.camera_id}_{person_id}"
            
            # Get zones for this person
            zones = self.zone_manager.get_zones_for_point(centroid)
            
            # Update person state
            if person_uuid not in self.person_states:
                self.person_states[person_uuid] = {
                    "first_seen": current_time,
                    "last_seen": current_time,
                    "zones": set(),
                    "dwell_start": {}
                }
                
                # Generate entry event
                self._generate_event(person_uuid, "entry", {"centroid": centroid})
            
            person_state = self.person_states[person_uuid]
            person_state["last_seen"] = current_time
            
            # Check for zone transitions
            previous_zones = person_state["zones"]
            current_zones = set(zones)
            
            # Zone entry events
            for zone_id in current_zones - previous_zones:
                person_state["dwell_start"][zone_id] = current_time
                self._generate_event(person_uuid, "zone_entry", {
                    "zone_id": zone_id,
                    "centroid": centroid
                })
            
            # Zone exit events
            for zone_id in previous_zones - current_zones:
                if zone_id in person_state["dwell_start"]:
                    dwell_time = (current_time - person_state["dwell_start"][zone_id]).total_seconds()
                    self._generate_event(person_uuid, "zone_exit", {
                        "zone_id": zone_id,
                        "dwell_time": dwell_time,
                        "centroid": centroid
                    })
                    del person_state["dwell_start"][zone_id]
            
            person_state["zones"] = current_zones
        
        # Clean up disappeared persons
        self._cleanup_disappeared_persons()
    
    def _generate_event(self, person_uuid: str, event_type: str, payload: Dict[str, Any]):
        """Generate and store an event."""
        try:
            event_data = {
                "store_id": self.store_id,
                "camera_id": self.camera_id,
                "person_uuid": person_uuid,
                "event_type": event_type,
                "timestamp": datetime.utcnow().isoformat(),
                "payload": payload
            }
            
            # Store in Redis for real-time processing
            self.redis_client.lpush(f"events:{self.store_id}", json.dumps(event_data))
            
            # Also store directly in database for persistence
            # This would be the actual database insert
            
            self.detection_count += 1
            logger.debug(f"Generated {event_type} event for person {person_uuid}")
            
        except Exception as e:
            logger.error(f"Failed to generate event: {e}")
    
    def _cleanup_disappeared_persons(self):
        """Clean up persons that have disappeared."""
        current_time = datetime.utcnow()
        timeout = timedelta(seconds=30)  # 30 second timeout
        
        disappeared_persons = []
        for person_uuid, state in self.person_states.items():
            if current_time - state["last_seen"] > timeout:
                disappeared_persons.append(person_uuid)
        
        for person_uuid in disappeared_persons:
            # Generate exit event
            self._generate_event(person_uuid, "exit", {
                "total_time": (current_time - self.person_states[person_uuid]["first_seen"]).total_seconds()
            })
            
            del self.person_states[person_uuid]
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    def run(self):
        """Main processing loop."""
        # Set up signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        logger.info(f"Starting camera processor for {self.camera_id}")
        
        # Open video stream
        cap = cv2.VideoCapture(self.rtsp_url)
        if not cap.isOpened():
            logger.error(f"Failed to open RTSP stream: {self.rtsp_url}")
            return
        
        # Update camera status to live
        try:
            self.redis_client.set(f"camera_status:{self.camera_id}", "live")
        except Exception as e:
            logger.error(f"Failed to update camera status: {e}")
        
        last_detection_time = time.time()
        heartbeat_interval = self.config.get("heartbeat_interval", 30)
        detection_interval = self.config.get("detection_interval", 0.1)
        
        try:
            while self.running:
                ret, frame = cap.read()
                if not ret:
                    logger.warning("Failed to read frame, attempting to reconnect...")
                    cap.release()
                    time.sleep(5)
                    cap = cv2.VideoCapture(self.rtsp_url)
                    continue
                
                self.frame_count += 1
                current_time = time.time()
                
                # Run detection at specified interval
                if current_time - last_detection_time >= detection_interval:
                    centroids = self._detect_persons(frame)
                    tracked_objects = self.tracker.update(centroids)
                    self._process_tracking_results(tracked_objects)
                    last_detection_time = current_time
                
                # Update heartbeat
                if current_time - self.last_heartbeat >= heartbeat_interval:
                    self._update_heartbeat()
                    self.last_heartbeat = current_time
                
                # Small delay to prevent excessive CPU usage
                time.sleep(0.01)
                
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
        finally:
            cap.release()
            # Update camera status to offline
            try:
                self.redis_client.set(f"camera_status:{self.camera_id}", "offline")
            except:
                pass
            logger.info(f"Camera processor stopped for {self.camera_id}")

def main():
    """Main entry point for the processor worker."""
    parser = argparse.ArgumentParser(description="Camera processor worker")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    parser.add_argument("--camera-id", required=True, help="Camera ID")
    
    args = parser.parse_args()
    
    try:
        processor = CameraProcessor(args.config)
        processor.run()
    except KeyboardInterrupt:
        logger.info("Processor interrupted by user")
    except Exception as e:
        logger.error(f"Processor failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()