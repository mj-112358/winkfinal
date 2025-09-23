
from typing import List, Tuple, Dict, Any, Optional
import json
import numpy as np
from ..database.db_manager import db
from .store_scope import current_store_id

def point_in_poly(x: float, y: float, poly: List[Tuple[float, float]]) -> bool:
    """Improved point-in-polygon using ray casting algorithm"""
    if len(poly) < 3:
        return False
    
    inside = False
    n = len(poly)
    
    for i in range(n):
        x1, y1 = poly[i]
        x2, y2 = poly[(i + 1) % n]
        
        if ((y1 > y) != (y2 > y)) and (x < (x2 - x1) * (y - y1) / (y2 - y1 + 1e-12) + x1):
            inside = not inside
    
    return inside

def polygon_area(poly: List[Tuple[float, float]]) -> float:
    """Calculate polygon area using shoelace formula"""
    if len(poly) < 3:
        return 0
    
    area = 0
    for i in range(len(poly)):
        j = (i + 1) % len(poly)
        area += poly[i][0] * poly[j][1]
        area -= poly[j][0] * poly[i][1]
    
    return abs(area) / 2

def polygon_centroid(poly: List[Tuple[float, float]]) -> Tuple[float, float]:
    """Calculate polygon centroid"""
    if len(poly) < 3:
        return (0, 0)
    
    area = polygon_area(poly)
    if area == 0:
        return (0, 0)
    
    cx = cy = 0
    for i in range(len(poly)):
        j = (i + 1) % len(poly)
        factor = (poly[i][0] * poly[j][1] - poly[j][0] * poly[i][1])
        cx += (poly[i][0] + poly[j][0]) * factor
        cy += (poly[i][1] + poly[j][1]) * factor
    
    return (cx / (6 * area), cy / (6 * area))

class EnhancedZoneManager:
    def __init__(self, camera_id: int):
        self.camera_id = camera_id
        self.store_id = current_store_id()
        self.img_w = None
        self.img_h = None
        self.zones = []
        self.zone_hierarchy = {}  # For nested zones
        self.zone_priorities = {}  # Zone priority mapping
        self._load()
    
    def _load(self):
        """Load zones and screenshot information from database"""
        with db.transaction() as conn:
            c = conn.cursor()
            
            # Load screenshot dimensions
            c.execute("""
                SELECT img_width, img_height FROM zone_screenshots 
                WHERE store_id=? AND camera_id=?
            """, (self.store_id, self.camera_id))
            row = c.fetchone()
            if row:
                self.img_w, self.img_h = row
            
            # Load zones with enhanced properties
            c.execute("""
                SELECT id, name, ztype, polygon_json, color, priority 
                FROM zones WHERE store_id=? AND camera_id=? 
                ORDER BY priority DESC
            """, (self.store_id, self.camera_id))
            
            for zone_id, name, ztype, polygon_json, color, priority in c.fetchall():
                try:
                    poly = json.loads(polygon_json)
                except:
                    poly = []
                
                zone_data = {
                    "id": zone_id,
                    "name": name,
                    "ztype": ztype,
                    "poly_scr": poly,
                    "color": color or "#00ff00",
                    "priority": priority or 1,
                    "area": polygon_area(poly) if poly else 0,
                    "centroid": polygon_centroid(poly) if poly else (0, 0)
                }
                
                self.zones.append(zone_data)
                self.zone_priorities[name] = priority or 1
    
    def reload(self):
        """Reload zones from database"""
        self.zones = []
        self.zone_hierarchy = {}
        self.zone_priorities = {}
        self._load()
    
    def _scale_polygon(self, fw: int, fh: int, poly: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        """Scale polygon coordinates from screenshot to frame dimensions"""
        if not self.img_w or not self.img_h or not poly:
            return []
        
        sx = fw / self.img_w
        sy = fh / self.img_h
        
        return [(p[0] * sx, p[1] * sy) for p in poly]
    
    def get_scaled_zones(self, fw: int, fh: int) -> List[Dict[str, Any]]:
        """Get all zones scaled to frame dimensions"""
        scaled_zones = []
        
        for zone in self.zones:
            scaled_poly = self._scale_polygon(fw, fh, zone["poly_scr"])
            if scaled_poly:
                scaled_zone = zone.copy()
                scaled_zone["poly"] = scaled_poly
                scaled_zone["scaled_area"] = polygon_area(scaled_poly)
                scaled_zone["scaled_centroid"] = polygon_centroid(scaled_poly)
                scaled_zones.append(scaled_zone)
        
        return scaled_zones
    
    def classify(self, fw: int, fh: int, cx: float, cy: float) -> List[Dict[str, Any]]:
        """Classify point into zones with priority handling"""
        hits = []
        scaled_zones = self.get_scaled_zones(fw, fh)
        
        # Sort by priority (higher priority first)
        scaled_zones.sort(key=lambda z: z["priority"], reverse=True)
        
        for zone in scaled_zones:
            if point_in_poly(cx, cy, zone["poly"]):
                hits.append({
                    "id": zone["id"],
                    "name": zone["name"],
                    "ztype": zone["ztype"],
                    "priority": zone["priority"],
                    "area": zone["scaled_area"],
                    "color": zone["color"]
                })
        
        return hits
    
    def get_zone_by_name(self, zone_name: str) -> Optional[Dict[str, Any]]:
        """Get zone information by name"""
        for zone in self.zones:
            if zone["name"] == zone_name:
                return zone
        return None
    
    def get_zones_by_type(self, zone_type: str) -> List[Dict[str, Any]]:
        """Get all zones of a specific type"""
        return [zone for zone in self.zones if zone["ztype"] == zone_type]
    
    def validate_polygon(self, polygon: List[Tuple[float, float]]) -> Dict[str, Any]:
        """Validate polygon and return metrics"""
        if len(polygon) < 3:
            return {"valid": False, "error": "Polygon must have at least 3 points"}
        
        area = polygon_area(polygon)
        if area < 100:  # Minimum area threshold
            return {"valid": False, "error": "Polygon area too small"}
        
        return {
            "valid": True,
            "area": area,
            "centroid": polygon_centroid(polygon),
            "perimeter": self._calculate_perimeter(polygon)
        }
    
    def _calculate_perimeter(self, polygon: List[Tuple[float, float]]) -> float:
        """Calculate polygon perimeter"""
        if len(polygon) < 2:
            return 0
        
        perimeter = 0
        for i in range(len(polygon)):
            j = (i + 1) % len(polygon)
            dx = polygon[j][0] - polygon[i][0]
            dy = polygon[j][1] - polygon[i][1]
            perimeter += np.sqrt(dx * dx + dy * dy)
        
        return perimeter
    
    def detect_zone_conflicts(self, new_polygon: List[Tuple[float, float]], exclude_zone_id: int = None) -> List[Dict[str, Any]]:
        """Detect overlapping zones that might conflict"""
        conflicts = []
        new_area = polygon_area(new_polygon)
        
        for zone in self.zones:
            if exclude_zone_id and zone["id"] == exclude_zone_id:
                continue
                
            # Check for significant overlap
            overlap_area = self._calculate_polygon_overlap(new_polygon, zone["poly_scr"])
            if overlap_area > min(new_area, zone["area"]) * 0.3:  # 30% overlap threshold
                conflicts.append({
                    "zone_id": zone["id"],
                    "zone_name": zone["name"],
                    "overlap_area": overlap_area,
                    "overlap_percentage": overlap_area / min(new_area, zone["area"]) * 100
                })
        
        return conflicts
    
    def _calculate_polygon_overlap(self, poly1: List[Tuple[float, float]], poly2: List[Tuple[float, float]]) -> float:
        """Calculate approximate overlap area between two polygons"""
        # Simplified overlap calculation using bounding box intersection
        if not poly1 or not poly2:
            return 0
        
        # Get bounding boxes
        min_x1, max_x1 = min(p[0] for p in poly1), max(p[0] for p in poly1)
        min_y1, max_y1 = min(p[1] for p in poly1), max(p[1] for p in poly1)
        
        min_x2, max_x2 = min(p[0] for p in poly2), max(p[0] for p in poly2)
        min_y2, max_y2 = min(p[1] for p in poly2), max(p[1] for p in poly2)
        
        # Calculate intersection of bounding boxes
        left = max(min_x1, min_x2)
        right = min(max_x1, max_x2)
        top = max(min_y1, min_y2)
        bottom = min(max_y1, max_y2)
        
        if left < right and top < bottom:
            return (right - left) * (bottom - top)
        
        return 0
    
    def get_zone_statistics(self, fw: int, fh: int) -> Dict[str, Any]:
        """Get comprehensive zone statistics"""
        scaled_zones = self.get_scaled_zones(fw, fh)
        
        stats = {
            "total_zones": len(scaled_zones),
            "zones_by_type": {},
            "total_coverage_area": 0,
            "average_zone_size": 0,
            "zone_density": 0
        }
        
        total_area = 0
        for zone in scaled_zones:
            zone_type = zone["ztype"]
            stats["zones_by_type"][zone_type] = stats["zones_by_type"].get(zone_type, 0) + 1
            total_area += zone["scaled_area"]
        
        frame_area = fw * fh
        stats["total_coverage_area"] = total_area
        stats["coverage_percentage"] = (total_area / frame_area) * 100 if frame_area > 0 else 0
        stats["average_zone_size"] = total_area / len(scaled_zones) if scaled_zones else 0
        stats["zone_density"] = len(scaled_zones) / frame_area * 10000 if frame_area > 0 else 0  # zones per 10k pixels
        
        return stats

# Maintain backward compatibility
ZoneManager = EnhancedZoneManager
