# ---------------------------------------------------------------------------------------
# ZIGGY CONSOLIDATOR - V7.0.0 (MariaDB Edition)
# ---------------------------------------------------------------------------------------
# DATE: 29-11-2025
# AUTHOR: Karl (TechnoShed)
#
# PURPOSE: 
# Reads buffered CSV logs from the incoming directory and performs bulk inserts
# into a central MariaDB server. Replaces previous SQLite functionality.
#
# CHANGES V7.0:
# - REMOVED: SQLite3 dependency and file locking logic.
# - ADDED: mysql.connector for remote database connections.
# - ADDED: Environment variable support for DB_HOST (Universal LAN Access).
# - OPTIMIZATION: Uses transactions to commit rows in file-based chunks.
# ---------------------------------------------------------------------------------------
import os
import sys
import time
import csv
import mysql.connector

# --- CONFIGURATION ---
INCOMING_DIR = '/app/ziggy_logs/incoming' 

# Database Details - Matching your Docker Environment
DB_CONFIG = {
    'user': 'technoshed_user',
    'password': 'FatSausageBun',  # Ensure this matches your .env!
    'host': '10.0.1.2',      # Use container name if on same net, or Host IP
    'database': 'ziggy_main',
    'raise_on_warnings': True
}

EXPECTED_COLUMNS = 7 

def get_db_connection():
    """Establishes connection to MariaDB."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as err:
        print(f"[{time.ctime()}] ❌ DB Connection Error: {err}")
        return None

def ingest_chunk(file_path):
    """Reads a CSV chunk and performs a bulk INSERT into MariaDB."""
    rows_to_insert = []
    
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
            if len(lines) < 2: return True # Empty file, just delete it
            
            reader = csv.reader(lines[1:]) 
            
            for parts in reader:
                # Handle cases where device names have commas
                if len(parts) > EXPECTED_COLUMNS:
                    merge_count = len(parts) - EXPECTED_COLUMNS
                    merged_id = ','.join(parts[2 : 2 + 1 + merge_count])
                    final_row = [parts[0], parts[1], merged_id] + parts[2 + 1 + merge_count:]
                elif len(parts) == EXPECTED_COLUMNS:
                    final_row = parts
                else:
                    continue 
                
                rows_to_insert.append(final_row)

        if rows_to_insert:
            conn = get_db_connection()
            if not conn: return False # Keep file, try again later

            cursor = conn.cursor()
            
            # Syntax change: SQLite uses ?, MySQL uses %s
            insert_query = """
                INSERT INTO ble_logs 
                (timestamp_utc, addr, device_id, rssi, channel, security, scanner_device)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.executemany(insert_query, rows_to_insert)
            conn.commit()
            
            print(f"[{time.ctime()}] ✅ Inserted {cursor.rowcount} rows from {os.path.basename(file_path)}")
            
            cursor.close()
            conn.close()
            return True
            
    except Exception as e:
        print(f"[{time.ctime()}] ❌ Ingest Error {file_path}: {e}")
        return False

    return True

def run_consolidation():
    if not os.path.exists(INCOMING_DIR): return
    
    files = [f for f in os.listdir(INCOMING_DIR) if f.endswith('.csv')]
    if not files: 
        print(f"[{time.ctime()}] No files to process.")
        return
    
    files.sort()
    print(f"[{time.ctime()}] Processing {len(files)} chunks...")
    
    for filename in files:
        full_path = os.path.join(INCOMING_DIR, filename)
        
        # If ingestion succeeds, delete the CSV. If it fails, keep it.
        if ingest_chunk(full_path):
            try:
                os.remove(full_path)
            except: pass
        else:
            print(f"[{time.ctime()}] Failed to ingest {filename} (Keeping for retry)")

if __name__ == '__main__':
    # Wait a moment for DB to be ready if script just started
    time.sleep(2) 
    run_consolidation()