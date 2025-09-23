import numpy as np
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from ..database.db_manager import db
from ..core.store_scope import current_store_id

class SpikeDetector:
    def __init__(self):
        self.store_id = current_store_id()
        self.baseline_days = 14  # Days to calculate baseline
        self.spike_threshold_multiplier = 2.0  # Spike if 2x baseline
        self.anomaly_sensitivity = 1.5  # Lower = more sensitive
        
    def calculate_baseline_metrics(self, metric_type: str = "footfall", days_back: int = None) -> Dict[str, float]:
        """Calculate baseline metrics for comparison"""
        if days_back is None:
            days_back = self.baseline_days
            
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days_back)
        
        with db.transaction() as conn:
            c = conn.cursor()
            
            if metric_type == "footfall":
                c.execute("""
                    SELECT hour_start, SUM(footfall) as total_footfall
                    FROM hourly_metrics 
                    WHERE store_id = ? AND hour_start BETWEEN ? AND ?
                    GROUP BY hour_start
                    ORDER BY hour_start
                """, (self.store_id, start_date.isoformat(), end_date.isoformat()))
                
                hourly_data = [(row[0], row[1]) for row in c.fetchall()]
                
            elif metric_type == "interactions":
                c.execute("""
                    SELECT hour_start, SUM(interactions) as total_interactions
                    FROM hourly_metrics 
                    WHERE store_id = ? AND hour_start BETWEEN ? AND ?
                    GROUP BY hour_start
                    ORDER BY hour_start
                """, (self.store_id, start_date.isoformat(), end_date.isoformat()))
                
                hourly_data = [(row[0], row[1]) for row in c.fetchall()]
                
            elif metric_type == "dwell_time":
                c.execute("""
                    SELECT hour_start, AVG(dwell_avg) as avg_dwell
                    FROM hourly_metrics 
                    WHERE store_id = ? AND hour_start BETWEEN ? AND ? AND dwell_avg > 0
                    GROUP BY hour_start
                    ORDER BY hour_start
                """, (self.store_id, start_date.isoformat(), end_date.isoformat()))
                
                hourly_data = [(row[0], row[1] or 0) for row in c.fetchall()]
        
        if not hourly_data:
            return {"mean": 0, "std": 0, "median": 0, "p95": 0, "p99": 0}
        
        values = [value for _, value in hourly_data]
        
        return {
            "mean": float(np.mean(values)),
            "std": float(np.std(values)),
            "median": float(np.median(values)),
            "p95": float(np.percentile(values, 95)),
            "p99": float(np.percentile(values, 99)),
            "min": float(np.min(values)),
            "max": float(np.max(values))
        }
    
    def detect_hourly_spikes(self, date: str = None) -> List[Dict[str, Any]]:
        """Detect spikes in hourly data for a specific date"""
        if date is None:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        spikes = []
        
        # Get baseline metrics
        footfall_baseline = self.calculate_baseline_metrics("footfall")
        interaction_baseline = self.calculate_baseline_metrics("interactions")
        dwell_baseline = self.calculate_baseline_metrics("dwell_time")
        
        # Get current day data
        start_time = f"{date}T00:00:00"
        end_time = f"{date}T23:59:59"
        
        with db.transaction() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT hour_start, SUM(footfall) as footfall, SUM(interactions) as interactions, 
                       AVG(dwell_avg) as dwell_avg, camera_id
                FROM hourly_metrics 
                WHERE store_id = ? AND hour_start BETWEEN ? AND ?
                GROUP BY hour_start, camera_id
                ORDER BY hour_start
            """, (self.store_id, start_time, end_time))
            
            for row in c.fetchall():
                hour_start, footfall, interactions, dwell_avg, camera_id = row
                hour = datetime.fromisoformat(hour_start.replace('Z', '+00:00')).hour
                
                # Check for footfall spikes
                if footfall > footfall_baseline["mean"] + (footfall_baseline["std"] * self.spike_threshold_multiplier):
                    spikes.append({
                        "type": "footfall_spike",
                        "severity": self._calculate_severity(footfall, footfall_baseline),
                        "hour_start": hour_start,
                        "camera_id": camera_id,
                        "value": footfall,
                        "baseline_mean": footfall_baseline["mean"],
                        "baseline_std": footfall_baseline["std"],
                        "spike_magnitude": footfall / max(footfall_baseline["mean"], 1),
                        "description": f"Footfall spike detected: {footfall} vs baseline {footfall_baseline['mean']:.1f}"
                    })
                
                # Check for interaction spikes
                if interactions and interactions > interaction_baseline["mean"] + (interaction_baseline["std"] * self.spike_threshold_multiplier):
                    spikes.append({
                        "type": "interaction_spike",
                        "severity": self._calculate_severity(interactions, interaction_baseline),
                        "hour_start": hour_start,
                        "camera_id": camera_id,
                        "value": interactions,
                        "baseline_mean": interaction_baseline["mean"],
                        "baseline_std": interaction_baseline["std"],
                        "spike_magnitude": interactions / max(interaction_baseline["mean"], 1),
                        "description": f"Interaction spike detected: {interactions} vs baseline {interaction_baseline['mean']:.1f}"
                    })
                
                # Check for dwell time anomalies
                if dwell_avg and abs(dwell_avg - dwell_baseline["mean"]) > (dwell_baseline["std"] * self.anomaly_sensitivity):
                    anomaly_type = "high_dwell" if dwell_avg > dwell_baseline["mean"] else "low_dwell"
                    spikes.append({
                        "type": f"dwell_{anomaly_type}",
                        "severity": self._calculate_severity(abs(dwell_avg - dwell_baseline["mean"]), {"mean": 0, "std": dwell_baseline["std"]}),
                        "hour_start": hour_start,
                        "camera_id": camera_id,
                        "value": dwell_avg,
                        "baseline_mean": dwell_baseline["mean"],
                        "baseline_std": dwell_baseline["std"],
                        "anomaly_magnitude": abs(dwell_avg - dwell_baseline["mean"]) / max(dwell_baseline["std"], 1),
                        "description": f"Dwell time anomaly: {dwell_avg:.1f}s vs baseline {dwell_baseline['mean']:.1f}s"
                    })
        
        return spikes
    
    def _calculate_severity(self, value: float, baseline: Dict[str, float]) -> str:
        """Calculate severity based on deviation from baseline"""
        if baseline["std"] == 0:
            return "medium"
            
        z_score = abs(value - baseline["mean"]) / baseline["std"]
        
        if z_score > 3:
            return "critical"
        elif z_score > 2:
            return "high"
        elif z_score > 1:
            return "medium"
        else:
            return "low"
    
    def detect_promotion_impact(self, promotion_start: str, promotion_end: str) -> Dict[str, Any]:
        """Analyze the impact of a promotion period"""
        
        # Calculate baseline before promotion
        promo_start_dt = datetime.fromisoformat(promotion_start)
        baseline_start = promo_start_dt - timedelta(days=self.baseline_days)
        baseline_end = promo_start_dt - timedelta(days=1)
        
        # Get baseline metrics
        baseline_footfall = self._get_period_metrics("footfall", baseline_start.isoformat(), baseline_end.isoformat())
        baseline_interactions = self._get_period_metrics("interactions", baseline_start.isoformat(), baseline_end.isoformat())
        baseline_dwell = self._get_period_metrics("dwell_time", baseline_start.isoformat(), baseline_end.isoformat())
        
        # Get promotion period metrics
        promo_footfall = self._get_period_metrics("footfall", promotion_start, promotion_end)
        promo_interactions = self._get_period_metrics("interactions", promotion_start, promotion_end)
        promo_dwell = self._get_period_metrics("dwell_time", promotion_start, promotion_end)
        
        # Calculate impact
        impact_analysis = {
            "promotion_period": {"start": promotion_start, "end": promotion_end},
            "baseline_period": {"start": baseline_start.isoformat(), "end": baseline_end.isoformat()},
            "footfall_impact": {
                "baseline_avg": baseline_footfall["daily_avg"],
                "promotion_avg": promo_footfall["daily_avg"],
                "percentage_change": ((promo_footfall["daily_avg"] - baseline_footfall["daily_avg"]) / max(baseline_footfall["daily_avg"], 1)) * 100,
                "absolute_change": promo_footfall["daily_avg"] - baseline_footfall["daily_avg"]
            },
            "interaction_impact": {
                "baseline_avg": baseline_interactions["daily_avg"],
                "promotion_avg": promo_interactions["daily_avg"],
                "percentage_change": ((promo_interactions["daily_avg"] - baseline_interactions["daily_avg"]) / max(baseline_interactions["daily_avg"], 1)) * 100,
                "absolute_change": promo_interactions["daily_avg"] - baseline_interactions["daily_avg"]
            },
            "dwell_impact": {
                "baseline_avg": baseline_dwell["daily_avg"],
                "promotion_avg": promo_dwell["daily_avg"],
                "percentage_change": ((promo_dwell["daily_avg"] - baseline_dwell["daily_avg"]) / max(baseline_dwell["daily_avg"], 1)) * 100,
                "absolute_change": promo_dwell["daily_avg"] - baseline_dwell["daily_avg"]
            }
        }
        
        # Determine overall impact
        footfall_change = impact_analysis["footfall_impact"]["percentage_change"]
        interaction_change = impact_analysis["interaction_impact"]["percentage_change"]
        
        if footfall_change > 20 and interaction_change > 15:
            impact_analysis["overall_impact"] = "high_positive"
        elif footfall_change > 10 and interaction_change > 10:
            impact_analysis["overall_impact"] = "moderate_positive"
        elif footfall_change > 5 or interaction_change > 5:
            impact_analysis["overall_impact"] = "low_positive"
        elif footfall_change < -10 or interaction_change < -10:
            impact_analysis["overall_impact"] = "negative"
        else:
            impact_analysis["overall_impact"] = "minimal"
        
        return impact_analysis
    
    def _get_period_metrics(self, metric_type: str, start_date: str, end_date: str) -> Dict[str, float]:
        """Get aggregated metrics for a period"""
        with db.transaction() as conn:
            c = conn.cursor()
            
            if metric_type == "footfall":
                c.execute("""
                    SELECT DATE(hour_start) as date, SUM(footfall) as daily_total
                    FROM hourly_metrics 
                    WHERE store_id = ? AND hour_start BETWEEN ? AND ?
                    GROUP BY DATE(hour_start)
                """, (self.store_id, start_date, end_date))
                
            elif metric_type == "interactions":
                c.execute("""
                    SELECT DATE(hour_start) as date, SUM(interactions) as daily_total
                    FROM hourly_metrics 
                    WHERE store_id = ? AND hour_start BETWEEN ? AND ?
                    GROUP BY DATE(hour_start)
                """, (self.store_id, start_date, end_date))
                
            elif metric_type == "dwell_time":
                c.execute("""
                    SELECT DATE(hour_start) as date, AVG(dwell_avg) as daily_avg
                    FROM hourly_metrics 
                    WHERE store_id = ? AND hour_start BETWEEN ? AND ? AND dwell_avg > 0
                    GROUP BY DATE(hour_start)
                """, (self.store_id, start_date, end_date))
            
            daily_values = [row[1] for row in c.fetchall() if row[1] is not None]
        
        if not daily_values:
            return {"daily_avg": 0, "total": 0, "days": 0}
        
        return {
            "daily_avg": float(np.mean(daily_values)),
            "total": float(np.sum(daily_values)),
            "days": len(daily_values),
            "std": float(np.std(daily_values)) if len(daily_values) > 1 else 0
        }
    
    def detect_festival_patterns(self, festival_dates: List[str]) -> Dict[str, Any]:
        """Detect patterns during festival periods"""
        festival_analysis = []
        
        for festival_date in festival_dates:
            # Analyze 3 days around the festival
            festival_dt = datetime.fromisoformat(festival_date)
            start_date = (festival_dt - timedelta(days=1)).strftime("%Y-%m-%d")
            end_date = (festival_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            
            # Get baseline (2 weeks before)
            baseline_start = (festival_dt - timedelta(days=21)).strftime("%Y-%m-%d")
            baseline_end = (festival_dt - timedelta(days=8)).strftime("%Y-%m-%d")
            
            festival_metrics = self._get_period_metrics("footfall", start_date, end_date)
            baseline_metrics = self._get_period_metrics("footfall", baseline_start, baseline_end)
            
            festival_analysis.append({
                "festival_date": festival_date,
                "footfall_increase": ((festival_metrics["daily_avg"] - baseline_metrics["daily_avg"]) / max(baseline_metrics["daily_avg"], 1)) * 100,
                "peak_day_footfall": festival_metrics["total"] / max(festival_metrics["days"], 1),
                "baseline_avg": baseline_metrics["daily_avg"],
                "festival_avg": festival_metrics["daily_avg"]
            })
        
        return {
            "festivals_analyzed": len(festival_dates),
            "average_festival_boost": np.mean([f["footfall_increase"] for f in festival_analysis]),
            "festival_details": festival_analysis
        }
    
    def log_anomaly(self, anomaly_type: str, value: float, baseline_value: float, 
                   description: str, severity: str = "medium", camera_id: int = None) -> int:
        """Log detected anomaly to database"""
        with db.transaction() as conn:
            c = conn.cursor()
            
            threshold = abs(value - baseline_value)
            metadata = {
                "detection_method": "statistical_analysis",
                "baseline_days": self.baseline_days,
                "spike_threshold": self.spike_threshold_multiplier
            }
            
            c.execute("""
                INSERT INTO anomalies 
                (store_id, camera_id, anomaly_type, detected_at, severity, value, 
                 baseline_value, threshold, description, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.store_id, camera_id, anomaly_type, datetime.now(timezone.utc).isoformat(),
                severity, value, baseline_value, threshold, description, json.dumps(metadata)
            ))
            
            conn.commit()
            return c.lastrowid
    
    def get_recent_anomalies(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get recent anomalies for analysis"""
        start_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        
        with db.transaction() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT anomaly_type, detected_at, severity, value, baseline_value, 
                       description, camera_id, metadata_json
                FROM anomalies 
                WHERE store_id = ? AND detected_at >= ?
                ORDER BY detected_at DESC
            """, (self.store_id, start_date))
            
            anomalies = []
            for row in c.fetchall():
                metadata = json.loads(row[7]) if row[7] else {}
                anomalies.append({
                    "type": row[0],
                    "detected_at": row[1],
                    "severity": row[2],
                    "value": row[3],
                    "baseline_value": row[4],
                    "description": row[5],
                    "camera_id": row[6],
                    "metadata": metadata
                })
        
        return anomalies