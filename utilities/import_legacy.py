# ---------------------------------------------------------------------------------------
# ZIGGY LEGACY IMPORTER V3.0 (Time Shift Edition)
# Purpose: Imports old CSVs.
#          CRITICAL: Shifts "Ghost" dates (1970/2000) to start at 2025-11-19.
# ---------------------------------------------------------------------------------------
import os
import sqlite3
import csv
import sys
import glob
import datetime
import time

# --- CONFIGURATION ---
LOGS_DIR = '/app/ziggy_logs' 
DB_PATH = os.path.join(LOGS_DIR, 'ziggy_data.db')

# The "Real" date these logs started
# We set the time to 00:00:00 on that day
TARGET_START_DATE = datetime.datetime(2025, 11, 19, 0, 0, 0)

EXPECTED_COLUMNS = 7
BATCH_SIZE = 5000 

def connect_db():
    if not os.path.exists(DB_PATH):
        print(f"CRITICAL: Database {DB_PATH} not found. Run the consolidator first!")
        sys.exit(1)
    return sqlite3.connect(DB_PATH)

def apply_time_shift(bad_dt):
    """
    Takes a datetime object from 1970 or 2000 and shifts it to 2025-11-19.
    Preserves the relative time (hours/minutes/seconds) passed since boot.
    """
    # 1. Determine the "Ghost" Base
    if bad_dt.year == 2000:
        ghost_base = datetime.datetime(2000, 1, 1, 0, 0, 0)
    elif bad_dt.year == 1970:
        ghost_base = datetime.datetime(1970, 1, 1, 0, 0, 0)
    else:
        # If it's some other weird year (e.g. 1900), just assume it's the start
        ghost_base = datetime.datetime(bad_dt.year, bad_dt.month, bad_dt.day, 0, 0, 0)

    # 2. Calculate "Time Since Boot"
    # This gives us a timedelta (e.g., 2 hours, 5 minutes)
    time_since_boot = bad_dt - ghost_base
    
    # 3. Add that duration to the REAL start date
    corrected_dt = TARGET_START_DATE + time_since_boot
    
    return corrected_dt

def clean_timestamp(ts_raw):
    """
    Parses timestamp. If date is old, SHIFTS it to Nov 19 2025.
    """
    ts_raw = str(ts_raw).strip()
    dt_obj = None

    # Case A: Epoch Timestamp (Digits)
    if ts_raw.isdigit() and len(ts_raw) >= 9:
        try:
            val = int(ts_raw)
            dt_obj = datetime.datetime.utcfromtimestamp(val)
        except: pass
            
    # Case B: String Timestamp
    elif "-" in ts_raw and ":" in ts_raw:
        try:
            # Handle milliseconds if present
            fmt = "%Y-%m-%d %H:%M:%S.%f" if "." in ts_raw else "%Y-%m-%d %H:%M:%S"
            dt_obj = datetime.datetime.strptime(ts_raw, fmt)
        except:
             return ts_raw

    # --- THE TIME SHIFT ---
    if dt_obj:
        # If older than 2024, perform the shift
        if dt_obj.year < 2024:
            dt_obj = apply_time_shift(dt_obj)
        
        return dt_obj.strftime("%Y-%m-%d %H:%M:%S")
    
    return ts_raw

def process_file(filepath, conn):
    filename = os.path.basename(filepath)
    print(f"--> Processing: {filename}...")
    
    rows_buffer = []
    total_inserted = 0
    time_shifts = 0
    skipped_headers = 0
    
    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            for line_num, line in enumerate(f):
                line = line.strip()
                if not line: continue
                
                parts = line.split(',')
                
                # Filter Header
                if "datetime" in parts[0] or "timestamp" in parts[0] or "utc" in parts[0].lower():
                    skipped_headers += 1
                    continue

                # Comma Spillover Fix
                final_row = []
                if len(parts) > EXPECTED_COLUMNS:
                    merge_count = len(parts) - EXPECTED_COLUMNS
                    merged_id = ','.join(parts[2 : 2 + 1 + merge_count])
                    final_row = [parts[0], parts[1], merged_id] + parts[2 + 1 + merge_count:]
                elif len(parts) == EXPECTED_COLUMNS:
                    final_row = parts
                else:
                    continue

                # TIME SHIFT LOGIC
                original_ts = final_row[0]
                final_row[0] = clean_timestamp(original_ts)
                
                # Check if we actually changed the year (implies a shift happened)
                if final_row[0] != original_ts and "2025-11-19" in str(final_row[0]): # simplistic check
                     pass # Counting is tricky with strings, but we assume it worked
                
                # We can roughly count if the string length/content changed significantly
                if str(original_ts).startswith("2000") or str(original_ts).startswith("1970"):
                    time_shifts += 1

                rows_buffer.append(final_row)
                
                if len(rows_buffer) >= BATCH_SIZE:
                    perform_insert(conn, rows_buffer)
                    total_inserted += len(rows_buffer)
                    rows_buffer = [] 
                    print(f"    Inserted {total_inserted} rows...", end='\r')

            if rows_buffer:
                perform_insert(conn, rows_buffer)
                total_inserted += len(rows_buffer)

        print(f"\n    DONE. Total: {total_inserted} | Time Shifted: ~{time_shifts} | Headers: {skipped_headers}")
        
    except Exception as e:
        print(f"\n    ERROR reading file: {e}")

def perform_insert(conn, rows):
    try:
        c = conn.cursor()
        c.executemany('''
            INSERT INTO ble_logs (timestamp_utc, addr, device_id, rssi, channel, security, scanner_device)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', rows)
        conn.commit()
    except Exception as e:
        print(f"    SQL ERROR: {e}")

def main():
    print("--- ZIGGY LEGACY IMPORT V3.0 (Target: 19-11-2025) ---")
    conn = connect_db()
    
    # Grab all CSVs
    daily_files = glob.glob(os.path.join(LOGS_DIR, "ziggy_daily_log_*.csv"))
    master_files = glob.glob(os.path.join(LOGS_DIR, "master*.csv"))
    all_files = daily_files + master_files
    all_files.sort()
    
    if not all_files:
        print("No CSV files found.")
        return

    print(f"Found {len(all_files)} files. Correcting invalid dates to start at 2025-11-19.")
    print("Press CTRL+C within 5 seconds to CANCEL...")
    try:
        time.sleep(5)
    except KeyboardInterrupt:
        sys.exit(0)

    for f in all_files:
        process_file(f, conn)

    conn.close()
    print("--- IMPORT COMPLETE ---")
    try:
        os.chmod(DB_PATH, 0o666)
    except: pass

if __name__ == "__main__":
    main()