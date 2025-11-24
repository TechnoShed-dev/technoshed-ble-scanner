# ---------------------------------------------------------------------------------------
# ZIGGY FINAL IMPORTER
# Purpose: Takes the "master_ziggy_log_PREVIEW.csv" and inserts it into SQLite.
#          Does NOT modify data. Preserves every single row/timestamp.
# ---------------------------------------------------------------------------------------
import os
import sqlite3
import csv
import sys

# --- CONFIGURATION ---
LOGS_DIR = '/app/ziggy_logs' 
DB_PATH = os.path.join(LOGS_DIR, 'ziggy_data.db')
INPUT_FILE = os.path.join(LOGS_DIR, 'master_ziggy_log_PREVIEW.csv')

BATCH_SIZE = 5000 

def connect_db():
    if not os.path.exists(DB_PATH):
        # If DB doesn't exist, we must create the schema first
        print(f"Database not found. Initializing new DB at {DB_PATH}...")
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('PRAGMA journal_mode=WAL;')
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
        c.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON ble_logs (timestamp_utc);')
        c.execute('CREATE INDEX IF NOT EXISTS idx_dev_id ON ble_logs (device_id);')
        conn.commit()
        return conn
    return sqlite3.connect(DB_PATH)

def main():
    print("--- ZIGGY FINAL IMPORT ---")
    print(f"Reading: {INPUT_FILE}")
    print(f"Target:  {DB_PATH}")
    
    if not os.path.exists(INPUT_FILE):
        print("CRITICAL: Preview file not found. Run 'clean_master_csv_v2.py' first!")
        sys.exit(1)

    conn = connect_db()
    c = conn.cursor()

    rows_buffer = []
    total_inserted = 0
    
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            # Skip the header row
            header = next(reader, None)
            
            for row in reader:
                # Basic safety check: Ensure we have 7 columns
                if len(row) != 7:
                    continue # Skip malformed rows (shouldn't happen if Cleaner ran)
                
                rows_buffer.append(row)
                
                if len(rows_buffer) >= BATCH_SIZE:
                    c.executemany('''
                        INSERT INTO ble_logs (timestamp_utc, addr, device_id, rssi, channel, security, scanner_device)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', rows_buffer)
                    conn.commit()
                    total_inserted += len(rows_buffer)
                    rows_buffer = []
                    print(f"Imported {total_inserted} rows...", end='\r')
            
            # Insert remaining
            if rows_buffer:
                c.executemany('''
                    INSERT INTO ble_logs (timestamp_utc, addr, device_id, rssi, channel, security, scanner_device)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', rows_buffer)
                conn.commit()
                total_inserted += len(rows_buffer)

        print(f"\nSUCCESS. Imported {total_inserted} rows into the database.")
        
    except Exception as e:
        print(f"\nError: {e}")
    finally:
        conn.close()
        # Fix permissions one last time
        try:
            os.chmod(DB_PATH, 0o666)
        except: pass

if __name__ == "__main__":
    main()