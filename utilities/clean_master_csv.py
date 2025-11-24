# ---------------------------------------------------------------------------------------
# ZIGGY MASTERFILE CLEANER V2.1 (Target: Nov 15, 2025)
# Purpose: Cleans master CSV, shifts timestamps to start at 15/11/2025, 
#          and TRIMS trailing garbage commas.
# ---------------------------------------------------------------------------------------
import os
import sys
import datetime
import csv

# --- CONFIGURATION ---
LOGS_DIR = '/app/ziggy_logs'
INPUT_FILE = os.path.join(LOGS_DIR, 'master_ziggy_log.csv')
OUTPUT_FILE = os.path.join(LOGS_DIR, 'master_ziggy_log_PREVIEW.csv')

# --- THE TIME SHIFT TARGET ---
# Ghost dates (1970/2000) will be shifted to start here:
TARGET_START_DATE = datetime.datetime(2025, 11, 15, 0, 0, 0)
# -----------------------------

EXPECTED_COLUMNS = 7
MASTER_HEADER = ["datetime_utc", "addr", "id", "rssi", "chan", "sec", "dev"]

def apply_time_shift(bad_dt):
    # Determine the "Ghost" Base (When the Pico thought it was)
    if bad_dt.year == 2000:
        ghost_base = datetime.datetime(2000, 1, 1, 0, 0, 0)
    elif bad_dt.year == 1970:
        ghost_base = datetime.datetime(1970, 1, 1, 0, 0, 0)
    else:
        ghost_base = datetime.datetime(bad_dt.year, bad_dt.month, bad_dt.day, 0, 0, 0)

    # Calculate how long the device was running since that ghost start
    time_since_boot = bad_dt - ghost_base
    
    # Add that duration to our REAL start date (15/11/2025)
    return TARGET_START_DATE + time_since_boot

def fix_timestamp(ts_raw):
    ts_raw = str(ts_raw).strip()
    dt_obj = None
    was_shifted = False

    # A: Epoch (Digits)
    if ts_raw.replace('.','',1).isdigit() and len(ts_raw) >= 9:
        try:
            val = float(ts_raw)
            dt_obj = datetime.datetime.utcfromtimestamp(val)
        except: pass
            
    # B: String (ISO format)
    elif "-" in ts_raw and ":" in ts_raw:
        try:
            fmt = "%Y-%m-%d %H:%M:%S.%f" if "." in ts_raw else "%Y-%m-%d %H:%M:%S"
            dt_obj = datetime.datetime.strptime(ts_raw, fmt)
        except: return ts_raw, False

    if dt_obj:
        # If the year is older than 2024, apply the shift
        if dt_obj.year < 2024:
            dt_obj = apply_time_shift(dt_obj)
            was_shifted = True
        return dt_obj.strftime("%Y-%m-%d %H:%M:%S"), was_shifted
    
    return ts_raw, False

def run_cleaner():
    print(f"--- ZIGGY CSV CLEANER (Target: 15/11/2025) ---")
    print(f"Input:  {INPUT_FILE}")
    print(f"Output: {OUTPUT_FILE}")
    print("--------------------------------")

    if not os.path.exists(INPUT_FILE):
        print("Error: Input file not found.")
        return

    stats = {
        'total_rows': 0,
        'headers_removed': 0,
        'epoch_fixes': 0,
        'tail_trims': 0,
        'id_merges': 0,
        'bad_rows': 0
    }

    try:
        with open(INPUT_FILE, 'r', encoding='utf-8', errors='replace') as fin, \
             open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as fout:
            
            writer = csv.writer(fout)
            writer.writerow(MASTER_HEADER)
            
            for line in fin:
                line = line.strip()
                if not line: continue
                
                parts = line.split(',')
                
                # --- FILTER 0: Remove Header Lines ---
                if "datetime" in parts[0] or "timestamp" in parts[0] or "utc" in parts[0].lower():
                    stats['headers_removed'] += 1
                    continue

                # --- FILTER 1: TAIL TRIMMER ---
                # Remove empty trailing columns
                original_len = len(parts)
                while len(parts) > EXPECTED_COLUMNS and parts[-1].strip() == '':
                    parts.pop()
                if len(parts) < original_len:
                    stats['tail_trims'] += 1

                # --- FILTER 2: Column Logic ---
                if len(parts) > EXPECTED_COLUMNS:
                    # Merge broken ID columns
                    merge_count = len(parts) - EXPECTED_COLUMNS
                    merged_id = ','.join(parts[2 : 2 + 1 + merge_count])
                    final_row = [parts[0], parts[1], merged_id] + parts[2 + 1 + merge_count:]
                    stats['id_merges'] += 1
                elif len(parts) == EXPECTED_COLUMNS:
                    final_row = parts
                else:
                    stats['bad_rows'] += 1
                    continue

                # --- FILTER 3: Hard Stop Limit ---
                final_row = final_row[:EXPECTED_COLUMNS]

                # --- FILTER 4: Time Shift ---
                original_ts = final_row[0]
                new_ts, changed = fix_timestamp(original_ts)
                final_row[0] = new_ts
                
                if changed:
                    stats['epoch_fixes'] += 1

                writer.writerow(final_row)
                stats['total_rows'] += 1
                
                if stats['total_rows'] % 5000 == 0:
                    print(f"Processed {stats['total_rows']} rows...", end='\r')

    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")
        return

    print(f"\n\n--- COMPLETE ---")
    print(f"Saved to: {OUTPUT_FILE}")
    print(f"Total Rows:     {stats['total_rows']}")
    print(f"Tail Trims:     {stats['tail_trims']}")
    print(f"Time Shifts:    {stats['epoch_fixes']}")
    print(f"ID Merges:      {stats['id_merges']}")
    
    try:
        os.chmod(OUTPUT_FILE, 0o666)
    except: pass

if __name__ == "__main__":
    run_cleaner()