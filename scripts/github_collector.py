import os
import requests
import psycopg2
from datetime import datetime
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

STATIONS = [
    "Brussels-Central",
    "Antwerp-Central", 
    "Ghent-Sint-Pieters",
    "Liège-Guillemins"
]

def get_db_connection():
    """Connect to Railway PostgreSQL"""
    return psycopg2.connect(
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT'],
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        sslmode='require' 
    )

def ensure_schema():
    """Create tables if they don't exist"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS departures_raw (
            id SERIAL PRIMARY KEY,
            station VARCHAR(100) NOT NULL,
            train_id VARCHAR(50) NOT NULL,
            vehicle VARCHAR(50),
            platform VARCHAR(10),
            scheduled_time TIMESTAMP NOT NULL,
            delay_seconds INT DEFAULT 0,
            cancelled BOOLEAN DEFAULT FALSE,
            destination VARCHAR(100),
            collected_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_station 
        ON departures_raw(station, scheduled_time DESC);
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_collected 
        ON departures_raw(collected_at DESC);
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("✅ Schema ready")

def collect_station(station):
    """Collect data for one station"""
    url = f"https://api.irail.be/liveboard/?station={station}&format=json&fast=true"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        departures = data.get('departures', {}).get('departure', [])
        collected_at = datetime.fromtimestamp(int(data.get('timestamp', 0)))
        station_name = data.get('stationinfo', {}).get('standardname', station)
        
        if not departures:
            logging.warning(f"⚠️  No departures for {station}")
            return 0
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        inserted = 0
        for dep in departures:
            try:
                cursor.execute("""
                    INSERT INTO departures_raw 
                    (station, train_id, vehicle, platform, scheduled_time, 
                     delay_seconds, cancelled, destination, collected_at)
                    VALUES (%s, %s, %s, %s, to_timestamp(%s), %s, %s, %s, %s)
                """, (
                    station_name,
                    dep.get('vehicle', '').replace('BE.NMBS.', ''),
                    dep.get('vehicle', ''),
                    dep.get('platform', ''),
                    int(dep.get('time', 0)),
                    int(dep.get('delay', 0)),
                    int(dep.get('canceled', 0)) == 1,
                    dep.get('station', ''),
                    collected_at
                ))
                inserted += 1
            except psycopg2.IntegrityError:
                conn.rollback()
                continue
            except Exception as e:
                logging.debug(f"Skipping record: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"✅ {station_name}: {inserted}/{len(departures)} departures")
        return inserted
        
    except Exception as e:
        logging.error(f"❌ {station}: {e}")
        return 0

def main():
    """Main collection function"""
    logging.info("🚂 GitHub Actions Data Collection Started")
    logging.info("=" * 60)
    
    try:
        # Ensure schema exists
        ensure_schema()
        
        # Collect from all stations
        total = 0
        for station in STATIONS:
            count = collect_station(station)
            total += count
        
        logging.info("=" * 60)
        logging.info(f"✅ TOTAL: {total} departures collected")
        
        # Get overall stats
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT station) as stations,
                MAX(collected_at) as last_collection
            FROM departures_raw
        """)
        stats = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if stats and stats[0]:
            logging.info(f"📊 Database: {stats[0]} total records from {stats[1]} stations")
            logging.info(f"🕐 Last collection: {stats[2]}")
        
        return 0
        
    except Exception as e:
        logging.error(f"❌ Collection failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())