#!/usr/bin/env python3

import sqlite3
import json
from datetime import datetime, timedelta
import random

# Connect to the database
conn = sqlite3.connect('wink_store.db')
cursor = conn.cursor()

# Seed some hourly metrics for the last 24 hours
def seed_hourly_metrics():
    print("Seeding hourly metrics...")
    
    # Get camera IDs
    cursor.execute("SELECT id FROM cameras WHERE enabled = 1")
    camera_ids = [row[0] for row in cursor.fetchall()]
    
    # Generate data for last 24 hours
    now = datetime.now()
    for hours_ago in range(24, 0, -1):
        hour_start = now - timedelta(hours=hours_ago)
        hour_str = hour_start.strftime('%Y-%m-%d %H:00:00')
        
        for camera_id in camera_ids:
            # Generate realistic patterns (higher during business hours)
            is_business_hour = 9 <= hour_start.hour <= 21
            base_footfall = random.randint(15, 45) if is_business_hour else random.randint(2, 12)
            
            footfall = base_footfall + random.randint(-5, 15)
            unique_visitors = int(footfall * 0.8)
            dwell_avg = 45 + random.randint(-20, 40)
            dwell_p95 = dwell_avg + random.randint(30, 120)
            queue_wait_avg = random.randint(15, 180)
            interactions = int(footfall * random.uniform(0.2, 0.5))
            
            zones_data = {
                'entrance': int(footfall * 0.9),
                'checkout': int(footfall * 0.4),
                'product_area': int(footfall * 0.3)
            }
            
            cursor.execute("""
                INSERT OR REPLACE INTO hourly_metrics 
                (store_id, camera_id, hour_start, footfall, unique_visitors, dwell_avg, dwell_p95, 
                 queue_wait_avg, interactions, zones_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, ("store_example_001", camera_id, hour_str, footfall, unique_visitors, dwell_avg, dwell_p95,
                  queue_wait_avg, interactions, json.dumps(zones_data)))

def seed_daily_metrics():
    print("Seeding daily metrics...")
    
    # Generate data for last 7 days
    now = datetime.now().date()
    for days_ago in range(7, 0, -1):
        date = now - timedelta(days=days_ago)
        date_str = date.strftime('%Y-%m-%d')
        
        dwell_avg = 60 + random.randint(-15, 30)
        queue_wait_avg = random.randint(45, 120)
        interactions = random.randint(50, 200)
        
        # Peak hour (random hour between 12-18)
        peak_hour = f"{random.randint(12, 18):02d}:00:00"
        
        cursor.execute("""
            INSERT OR REPLACE INTO daily_store_metrics 
            (store_id, date, dwell_avg, queue_wait_avg, interactions, peak_hour)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ("store_example_001", date_str, dwell_avg, queue_wait_avg, interactions, peak_hour))

def seed_live_metrics():
    print("Seeding live metrics...")
    # Skip live metrics for now as table might not exist
    pass

def create_zones():
    print("Creating zones...")
    
    # Get camera IDs
    cursor.execute("SELECT id FROM cameras WHERE enabled = 1")
    camera_ids = [row[0] for row in cursor.fetchall()]
    
    zone_types = ['entrance', 'checkout', 'product_area', 'queue']
    zone_names = [
        'Main Entrance', 'Checkout Area', 'Electronics Section', 'Grocery Aisle',
        'Customer Service', 'Exit Zone', 'Product Display', 'Queue Area'
    ]
    
    for i, camera_id in enumerate(camera_ids):
        zone_name = zone_names[i % len(zone_names)]
        zone_type = zone_types[i % len(zone_types)]
        
        # Simple rectangular polygon coordinates
        x1, y1 = random.randint(50, 200), random.randint(50, 150)
        x2, y2 = x1 + random.randint(100, 300), y1 + random.randint(100, 200)
        
        polygon = [[x1, y1], [x2, y1], [x2, y2], [x1, y2]]
        
        cursor.execute("""
            INSERT OR IGNORE INTO zones 
            (store_id, camera_id, name, ztype, polygon_json)
            VALUES (?, ?, ?, ?, ?)
        """, ("store_example_001", camera_id, zone_name, zone_type, json.dumps(polygon)))

if __name__ == "__main__":
    print("Starting data seeding...")
    
    try:
        seed_hourly_metrics()
        seed_daily_metrics()
        seed_live_metrics()
        create_zones()
        
        conn.commit()
        print("✅ Data seeding completed successfully!")
        
        # Print summary
        cursor.execute("SELECT COUNT(*) FROM hourly_metrics")
        hourly_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM daily_store_metrics")
        daily_count = cursor.fetchone()[0]
        
        live_count = 0  # Skip live metrics count
        
        cursor.execute("SELECT COUNT(*) FROM zones")
        zones_count = cursor.fetchone()[0]
        
        print(f"📊 Created {hourly_count} hourly metrics")
        print(f"📊 Created {daily_count} daily metrics")  
        print(f"📊 Created {live_count} live metrics")
        print(f"📊 Created {zones_count} zones")
        
    except Exception as e:
        print(f"❌ Error seeding data: {e}")
        conn.rollback()
    finally:
        conn.close()