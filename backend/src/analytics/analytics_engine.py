
import numpy as np
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from ..database.db_manager import db
from ..core.store_scope import current_store_id
from .spike_detector import SpikeDetector

class EnhancedAnalyticsEngine:
    def __init__(self):
        self.store_id = current_store_id()
        self.spike_detector = SpikeDetector()
    
    def recompute_daily_store_metrics(self, target_date: str = None) -> Dict[str, Any]:
        """Enhanced daily metrics computation with comprehensive analytics"""
        if not target_date:
            target_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        start = f"{target_date}T00:00:00"
        end = f"{target_date}T23:59:59"
        
        with db.transaction() as conn:
            c = conn.cursor()
            
            # Get hourly data for the day
            c.execute("""
                SELECT hour_start, SUM(footfall) as total_footfall, COUNT(DISTINCT camera_id) as cameras,
                       AVG(dwell_avg) as avg_dwell, AVG(queue_wait_avg) as avg_queue,
                       SUM(interactions) as total_interactions, SUM(unique_visitors) as total_unique,
                       SUM(entrance_count) as total_entrance, SUM(exit_count) as total_exit
                FROM hourly_metrics
                WHERE store_id=? AND hour_start BETWEEN ? AND ?
                GROUP BY hour_start
                ORDER BY hour_start
            """, (self.store_id, start, end))
            
            hourly_data = c.fetchall()
            
            if not hourly_data:
                return self._create_empty_metrics(target_date)
            
            # Calculate aggregated metrics
            total_footfall = sum(row[1] for row in hourly_data if row[1])
            total_unique_visitors = sum(row[6] for row in hourly_data if row[6])
            total_interactions = sum(row[5] for row in hourly_data if row[5])
            total_entrance = sum(row[7] for row in hourly_data if row[7])
            total_exit = sum(row[8] for row in hourly_data if row[8])
            
            # Calculate averages (excluding zeros)
            dwell_values = [row[3] for row in hourly_data if row[3] and row[3] > 0]
            queue_values = [row[4] for row in hourly_data if row[4] and row[4] > 0]
            
            dwell_avg = float(np.mean(dwell_values)) if dwell_values else 0.0
            dwell_p95 = float(np.percentile(dwell_values, 95)) if dwell_values else 0.0
            queue_avg = float(np.mean(queue_values)) if queue_values else 0.0
            
            # Find peak hour
            peak_hour = None
            peak_footfall = 0
            for row in hourly_data:
                if row[1] and row[1] > peak_footfall:
                    peak_footfall = row[1]
                    peak_hour = row[0]
            
            # Calculate conversion rate (interactions per visitor)
            conversion_rate = (total_interactions / max(total_unique_visitors, 1)) * 100
            
            # Calculate average visit duration
            avg_visit_duration = dwell_avg
            
            # Insert/update daily metrics
            c.execute("""
                INSERT INTO daily_store_metrics 
                (store_id, date, total_footfall, unique_visitors, dwell_avg, dwell_p95,
                 queue_wait_avg, interactions, peak_hour, peak_footfall, conversion_rate, avg_visit_duration)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(store_id,date) DO UPDATE SET 
                    total_footfall=excluded.total_footfall,
                    unique_visitors=excluded.unique_visitors,
                    dwell_avg=excluded.dwell_avg,
                    dwell_p95=excluded.dwell_p95,
                    queue_wait_avg=excluded.queue_wait_avg,
                    interactions=excluded.interactions,
                    peak_hour=excluded.peak_hour,
                    peak_footfall=excluded.peak_footfall,
                    conversion_rate=excluded.conversion_rate,
                    avg_visit_duration=excluded.avg_visit_duration
            """, (self.store_id, target_date, total_footfall, total_unique_visitors,
                  dwell_avg, dwell_p95, queue_avg, total_interactions,
                  peak_hour, peak_footfall, conversion_rate, avg_visit_duration))
            
            conn.commit()
            
            # Detect and log anomalies
            self._detect_daily_anomalies(target_date, {
                'footfall': total_footfall,
                'interactions': total_interactions,
                'dwell_avg': dwell_avg,
                'conversion_rate': conversion_rate
            })
        
        return {
            "store_id": self.store_id,
            "date": target_date,
            "total_footfall": total_footfall,
            "unique_visitors": total_unique_visitors,
            "dwell_avg": dwell_avg,
            "dwell_p95": dwell_p95,
            "queue_wait_avg": queue_avg,
            "interactions": total_interactions,
            "peak_hour": peak_hour,
            "peak_footfall": peak_footfall,
            "conversion_rate": conversion_rate,
            "avg_visit_duration": avg_visit_duration
        }
    
    def _create_empty_metrics(self, target_date: str) -> Dict[str, Any]:
        """Create empty metrics for days with no data"""
        return {
            "store_id": self.store_id,
            "date": target_date,
            "total_footfall": 0,
            "unique_visitors": 0,
            "dwell_avg": 0.0,
            "dwell_p95": 0.0,
            "queue_wait_avg": 0.0,
            "interactions": 0,
            "peak_hour": None,
            "peak_footfall": 0,
            "conversion_rate": 0.0,
            "avg_visit_duration": 0.0
        }
    
    def _detect_daily_anomalies(self, date: str, metrics: Dict[str, float]):
        """Detect and log daily anomalies"""
        # Get baseline metrics
        baseline_footfall = self.spike_detector.calculate_baseline_metrics("footfall")
        baseline_interactions = self.spike_detector.calculate_baseline_metrics("interactions")
        
        # Check for anomalies
        if metrics['footfall'] > baseline_footfall['mean'] + (2 * baseline_footfall['std']):
            self.spike_detector.log_anomaly(
                "daily_footfall_spike", metrics['footfall'], baseline_footfall['mean'],
                f"Daily footfall spike: {metrics['footfall']} vs baseline {baseline_footfall['mean']:.1f}",
                severity="high"
            )
        
        if metrics['interactions'] > baseline_interactions['mean'] + (2 * baseline_interactions['std']):
            self.spike_detector.log_anomaly(
                "daily_interaction_spike", metrics['interactions'], baseline_interactions['mean'],
                f"Daily interaction spike: {metrics['interactions']} vs baseline {baseline_interactions['mean']:.1f}",
                severity="high"
            )
    
    def analyze_store_performance(self, days: int = 30) -> Dict[str, Any]:
        """Comprehensive store performance analysis"""
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)
        
        with db.transaction() as conn:
            c = conn.cursor()
            
            # Get daily metrics for the period
            c.execute("""
                SELECT date, total_footfall, unique_visitors, dwell_avg, 
                       interactions, conversion_rate, peak_hour
                FROM daily_store_metrics
                WHERE store_id = ? AND date BETWEEN ? AND ?
                ORDER BY date
            """, (self.store_id, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
            
            daily_data = c.fetchall()
            
            if not daily_data:
                return {"error": "No data available for analysis"}
            
            # Calculate performance metrics
            footfall_values = [row[1] for row in daily_data if row[1]]
            visitor_values = [row[2] for row in daily_data if row[2]]
            dwell_values = [row[3] for row in daily_data if row[3] and row[3] > 0]
            interaction_values = [row[4] for row in daily_data if row[4]]
            conversion_values = [row[5] for row in daily_data if row[5] and row[5] > 0]
            
            # Trend analysis
            footfall_trend = self._calculate_trend(footfall_values)
            visitor_trend = self._calculate_trend(visitor_values)
            interaction_trend = self._calculate_trend(interaction_values)
            
            # Peak hours analysis
            peak_hours = [row[6] for row in daily_data if row[6]]
            peak_hour_distribution = self._analyze_peak_hours(peak_hours)
            
            return {
                "analysis_period": {"start": start_date.strftime("%Y-%m-%d"), "end": end_date.strftime("%Y-%m-%d")},
                "summary": {
                    "avg_daily_footfall": float(np.mean(footfall_values)) if footfall_values else 0,
                    "avg_daily_visitors": float(np.mean(visitor_values)) if visitor_values else 0,
                    "avg_dwell_time": float(np.mean(dwell_values)) if dwell_values else 0,
                    "avg_daily_interactions": float(np.mean(interaction_values)) if interaction_values else 0,
                    "avg_conversion_rate": float(np.mean(conversion_values)) if conversion_values else 0,
                    "total_footfall": sum(footfall_values),
                    "total_interactions": sum(interaction_values)
                },
                "trends": {
                    "footfall_trend": footfall_trend,
                    "visitor_trend": visitor_trend,
                    "interaction_trend": interaction_trend
                },
                "peak_hours": peak_hour_distribution,
                "variability": {
                    "footfall_cv": (np.std(footfall_values) / np.mean(footfall_values)) * 100 if footfall_values and np.mean(footfall_values) > 0 else 0,
                    "visitor_cv": (np.std(visitor_values) / np.mean(visitor_values)) * 100 if visitor_values and np.mean(visitor_values) > 0 else 0
                }
            }
    
    def _calculate_trend(self, values: List[float]) -> Dict[str, Any]:
        """Calculate trend direction and strength"""
        if len(values) < 2:
            return {"direction": "insufficient_data", "strength": 0, "slope": 0}
        
        x = np.arange(len(values))
        slope = np.polyfit(x, values, 1)[0]
        
        # Calculate trend strength (R-squared)
        correlation = np.corrcoef(x, values)[0, 1]
        r_squared = correlation ** 2 if not np.isnan(correlation) else 0
        
        if slope > 0.1:
            direction = "increasing"
        elif slope < -0.1:
            direction = "decreasing"
        else:
            direction = "stable"
        
        return {
            "direction": direction,
            "strength": float(r_squared),
            "slope": float(slope)
        }
    
    def _analyze_peak_hours(self, peak_hours: List[str]) -> Dict[str, Any]:
        """Analyze peak hour patterns"""
        if not peak_hours:
            return {"most_common": None, "distribution": {}}
        
        # Extract hour from timestamp
        hours = []
        for peak_hour in peak_hours:
            try:
                dt = datetime.fromisoformat(peak_hour.replace('Z', '+00:00'))
                hours.append(dt.hour)
            except:
                continue
        
        if not hours:
            return {"most_common": None, "distribution": {}}
        
        # Count frequency
        hour_counts = {}
        for hour in hours:
            hour_counts[hour] = hour_counts.get(hour, 0) + 1
        
        most_common_hour = max(hour_counts, key=hour_counts.get)
        
        return {
            "most_common": most_common_hour,
            "most_common_count": hour_counts[most_common_hour],
            "distribution": hour_counts
        }
    
    def get_zone_performance_analysis(self, camera_id: int, days: int = 7) -> Dict[str, Any]:
        """Analyze zone-specific performance"""
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)
        
        with db.transaction() as conn:
            c = conn.cursor()
            
            # Get hourly zone data
            c.execute("""
                SELECT hour_start, zones_json
                FROM hourly_metrics
                WHERE store_id = ? AND camera_id = ? AND hour_start BETWEEN ? AND ?
                ORDER BY hour_start
            """, (self.store_id, camera_id, start_date.isoformat(), end_date.isoformat()))
            
            zone_aggregates = {}
            total_hours = 0
            
            for row in c.fetchall():
                total_hours += 1
                zones_data = json.loads(row[1]) if row[1] else {}
                
                for zone_name, count in zones_data.items():
                    if zone_name not in zone_aggregates:
                        zone_aggregates[zone_name] = []
                    zone_aggregates[zone_name].append(count)
            
            # Calculate zone statistics
            zone_stats = {}
            for zone_name, counts in zone_aggregates.items():
                zone_stats[zone_name] = {
                    "total_visits": sum(counts),
                    "average_hourly": float(np.mean(counts)),
                    "peak_hourly": max(counts),
                    "utilization_rate": (len([c for c in counts if c > 0]) / max(total_hours, 1)) * 100
                }
            
            return {
                "camera_id": camera_id,
                "analysis_period": {"start": start_date.strftime("%Y-%m-%d"), "end": end_date.strftime("%Y-%m-%d")},
                "total_hours_analyzed": total_hours,
                "zone_performance": zone_stats
            }

# Maintain backward compatibility
def recompute_daily_store_metrics(target_date: str = None):
    """Backward compatible function"""
    engine = EnhancedAnalyticsEngine()
    return engine.recompute_daily_store_metrics(target_date)
