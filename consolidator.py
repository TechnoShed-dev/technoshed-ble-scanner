# ---------------------------------------------------------------------------------------
# ZIGGY CONSOLIDATOR V6.1 (SQLite + Permissions Fix)
# Purpose: Ingests CSV logs into SQLite and fixes permissions so Grafana can read it.
# ---------------------------------------------------------------------------------------
import os
import sys
import time
import sqlite3
import csv

# --- CONFIGURATION ---
LOGS_DIR = '/app/ziggy_logs' 
INCOMING_DIR = os.path.join(LOGS_DIR, 'incoming') 
DB_FILENAME = "ziggy_data.db"
DB_PATH = os.path.join(LOGS_DIR, DB_FILENAME)

EXPECTED_COLUMNS = 7 

def init_db():
    """Creates the SQLite DB and Table if they don't exist, and FIXES PERMISSIONS."""
    try:
        # 1. Connect (Creates file if missing)
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # 2. Enable WAL Mode
        c.execute('PRAGMA journal_mode=WAL;')
        
        # 3. Create Table
        c.execute('''
            CREATE TABLE IF NOT EXISTS ble_logs (
                timestamp_utc TEXT,
                addr TEXT,
                device_id TEXT,
                rssi INTEGER,
                channel TEXT,
                security TEXT,
                scanner_device TEXT
            )
        ''')
        
        # 4. Create Indexes
        c.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON ble_logs (timestamp_utc);')
        c.execute('CREATE INDEX IF NOT EXISTS idx_dev_id ON ble_logs (device_id);')
        
        conn.commit()
        conn.close()

        # 5. CRITICAL PERMISSION FIX
        # Force the file to be Read/Write for Everyone (666)
        # This allows Grafana (User 472) to read a file created by Docker (Root)
        os.chmod(DB_PATH, 0o666) 
        print(f"[{time.ctime()}] DB initialized and permissions set to 666.")

    except Exception as e:
        print(f"[{time.ctime()}] DB Init Error: {e}")
        # Don't exit, try to continue
        pass

def ingest_chunk(file_path):
    """Reads a CSV chunk and performs a bulk INSERT into SQLite."""
    rows_to_insert = []
    
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
            if len(lines) < 2: return True 
            
            reader = csv.reader(lines[1:]) 
            
            for parts in reader:
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
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.executemany('''
                INSERT INTO ble_logs (timestamp_utc, addr, device_id, rssi, channel, security, scanner_device)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', rows_to_insert)
            conn.commit()
            conn.close()
            return True
            
    except Exception as e:
        print(f"[{time.ctime()}] Ingest Error {file_path}: {e}")
        return False

    return True

def run_consolidation():
    # Ensure DB exists and has correct permissions BEFORE checking for files
    init_db() 
    
    if not os.path.exists(INCOMING_DIR): return
    files = [f for f in os.listdir(INCOMING_DIR) if f.endswith('.csv')]
    if not files: return
    
    files.sort()
    print(f"[{time.ctime()}] Processing {len(files)} chunks...")
    
    for filename in files:
        full_path = os.path.join(INCOMING_DIR, filename)
        if ingest_chunk(full_path):
            try:
                os.remove(full_path)
            except: pass
        else:
            print(f"[{time.ctime()}] Failed to ingest {filename}")

if __name__ == '__main__':
    run_consolidation()