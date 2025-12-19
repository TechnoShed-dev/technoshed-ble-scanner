# ---------------------------------------------------------------------------------------
# ZIGGY CONSOLIDATOR - V7.2.0 (Sanitization Fix)
# ---------------------------------------------------------------------------------------
# DATE: 19-12-2025
# AUTHOR: Karl (TechnoShed)
#
# CHANGES V7.2:
# - FIXED: "Incorrect integer value" error.
# - LOGIC: Converts empty strings "" in CSV to Python None (SQL NULL) for ID columns.
# ---------------------------------------------------------------------------------------
import os
import sys
import time
import csv
import mysql.connector

# --- CONFIGURATION ---
INCOMING_DIR = '/app/ziggy_logs/incoming' 

# Database Details
DB_CONFIG = {
    'user': 'technoshed_user',
    'password': 'FatSausageBun',  
    'host': '10.0.1.2',      
    'database': 'ziggy_main',
    'raise_on_warnings': True
}

# Old Format: rssi, channel, security, scanner_device (4 cols)
# New Format: rssi, channel, security, scanner_device, company_id, appearance_id (6 cols)
OLD_TAIL_LEN = 4
NEW_TAIL_LEN = 6

def get_db_connection():
    """Establishes connection to MariaDB."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as err:
        print(f"[{time.ctime()}] ❌ DB Connection Error: {err}")
        return None

def clean_int(value):
    """Sanitizes CSV strings for SQL Integer columns. '' becomes None."""
    if value is None: return None
    if isinstance(value, str):
        value = value.strip()
        if value == '': return None
    return value

def ingest_chunk(file_path):
    """Reads a CSV chunk and performs a bulk INSERT into MariaDB."""
    rows_to_insert = []
    
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
            if len(lines) < 2: return True 
            
            # --- SMART DETECTION ---
            header = lines[0].strip()
            if "company_id" in header or "appearance_id" in header:
                is_new_format = True
                expected_cols = 3 + NEW_TAIL_LEN 
            else:
                is_new_format = False
                expected_cols = 3 + OLD_TAIL_LEN

            reader = csv.reader(lines[1:]) 
            
            for parts in reader:
                current_len = len(parts)
                
                # 1. Handle Commas in Names (Dynamic Slice)
                if current_len >= expected_cols:
                    merge_count = current_len - expected_cols
                    merged_id = ','.join(parts[2 : 2 + 1 + merge_count])
                    data_slice_start = 2 + 1 + merge_count
                    
                    # Construct Base Row
                    final_row = [parts[0], parts[1], merged_id] + parts[data_slice_start:]
                    
                    # 2. Normalization & Sanitization
                    if not is_new_format:
                        # Old format: Add placeholders
                        final_row.extend([None, None]) 
                    else:
                        # New format: SANITIZE the last two columns (Indices 7 and 8)
                        # We must convert '' to None to avoid SQL errors
                        final_row[7] = clean_int(final_row[7]) # company_id
                        final_row[8] = clean_int(final_row[8]) # appearance_id
                        
                    rows_to_insert.append(final_row)

        if rows_to_insert:
            conn = get_db_connection()
            if not conn: return False 

            cursor = conn.cursor()
            
            insert_query = """
                INSERT INTO ble_logs 
                (timestamp_utc, addr, device_id, rssi, channel, security, scanner_device, company_id, appearance_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.executemany(insert_query, rows_to_insert)
            conn.commit()
            
            print(f"[{time.ctime()}] ✅ Inserted {cursor.rowcount} rows from {os.path.basename(file_path)} (Mode: {'New' if is_new_format else 'Old'})")
            
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
            print(f"[{time.ctime()}] Failed to ingest {filename} (Keeping for retry)")

if __name__ == '__main__':
    time.sleep(2) 
    run_consolidation()