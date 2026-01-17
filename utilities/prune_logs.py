# ---------------------------------------------------------------------------------------
# ZIGGY DATABASE PRUNER (STANDALONE)
# ---------------------------------------------------------------------------------------
# PURPOSE:  Keep database size under control by deleting old roadside data.
# TARGET:   Delete records > 60 days old ONLY if scanner is NOT a 'GAT' (Truck Tracker).
# FREQ:     Runs every 24 hours.
# DRIVER:   mysql.connector (Native to your Docker setup)
# ---------------------------------------------------------------------------------------
import time
import mysql.connector
import sys
import os
from datetime import datetime, timedelta

# --- CONFIGURATION ---
# Using 127.0.0.1 because your docker-compose uses "network_mode: host"
DB_CONFIG = {
    'user': 'technoshed_user',
    'password': 'FatSausageBun',
    'host': '10.0.1.2',
    'database': 'ziggy_main',
    'autocommit': True
}

SETTINGS = {
    'RETENTION_DAYS': 45,      # Delete data older than this
    'CHUNK_SIZE': 5000,        # Delete in batches to prevent locking
    'PROTECTED_PREFIX': 'GAT', # NEVER delete devices starting with this
    'SLEEP_INTERVAL': 86400    # 24 Hours in seconds
}

def get_db_connection():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except mysql.connector.Error as err:
        print(f"[ERROR] DB Connection Failed: {err}")
        return None

def run_prune_job():
    conn = get_db_connection()
    if not conn: return

    try:
        cursor = conn.cursor()
        
        # Calculate 60 days ago
        cutoff_date = datetime.now() - timedelta(days=SETTINGS['RETENTION_DAYS'])
        date_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

        print(f"\n[JOB START] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f" -> CUTOFF:  {date_str}")
        print(f" -> TARGET:  Non-GAT devices (Roadside/Tactical)")

        total_deleted = 0
        while True:
            # DELETE OLD records where device does NOT start with GAT
            # We use %s parameterization which works for values, but prefixes 
            # like 'GAT%' need to be handled carefully in the string or passed safely.
            # Here we embed the prefix logic safely since it's hardcoded in settings.
            sql = f"""
                DELETE FROM ble_logs 
                WHERE timestamp_utc < %s 
                AND scanner_device NOT LIKE '{SETTINGS['PROTECTED_PREFIX']}%%'
                LIMIT {SETTINGS['CHUNK_SIZE']}
            """
            
            cursor.execute(sql, (date_str,))
            deleted_count = cursor.rowcount
            total_deleted += deleted_count
            
            # Progress bar
            sys.stdout.write(f"\r -> Pruning... {total_deleted} rows removed.")
            sys.stdout.flush()
            
            # If we deleted less than the chunk size, we are done
            if deleted_count < SETTINGS['CHUNK_SIZE']:
                break
            
            # Small sleep to avoid hogging IO
            time.sleep(0.1)

        print(f"\n[JOB DONE] Total rows removed: {total_deleted}")

    except mysql.connector.Error as e:
        print(f"\n[ERROR] MySQL Error: {e}")
    except Exception as e:
        print(f"\n[ERROR] General Error: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    print("--- Ziggy Standalone Pruner Started ---")
    print(f"Mode: Loop (Every {SETTINGS['SLEEP_INTERVAL']} seconds)")

    # 1. Run immediately on container start
    run_prune_job()

    # 2. Enter the forever loop
    while True:
        time.sleep(SETTINGS['SLEEP_INTERVAL'])
        run_prune_job()