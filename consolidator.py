# ---------------------------------------------------------------------------------------
# ZIGGY CONSOLIDATOR - V7.3.0 (Inference Engine)
# ---------------------------------------------------------------------------------------
# DATE: 19-12-2025
# AUTHOR: Karl (TechnoShed)
#
# CHANGES V7.3:
# - ADDED: Inference Logic to guess Manufacturer from Name (JBL, Fitbit, BYD, etc.)
# - ADDED: Support for 'man_text' and 'type_text' columns in DB.
# ---------------------------------------------------------------------------------------
import os
import sys
import time
import csv
import mysql.connector
import re

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

# --- INFERENCE ENGINE ---
# --- INFERENCE ENGINE ---
# --- INFERENCE ENGINE ---
def infer_device_details(name):
    if not name: return (None, None)
    n = name.lower()

    # --- 1. WORK / LOGISTICS / AUTOMOTIVE ---
    # Trucks & Tachos
    if n.startswith("dtco"): return ("Continental", "Tachograph")
    if "se5000" in n: return ("Stoneridge", "Tachograph")
    if n.startswith("volvo"): return ("Volvo Group", "Truck/System")
    
    # Telematics & OBD
    if n.startswith("fmc") or n.startswith("fmb"): return ("Teltonika", "Fleet Tracker")
    if n.startswith("ldl"): return ("Lantronix", "Gateway")
    if n.startswith("lmu_"): return ("CalAmp", "Telematics Unit")
    if "vlinker" in n: return ("Vgate", "OBDII Adapter")

    # Vehicles & Accessories
    if "byd" in n: return ("BYD Auto", "Vehicle System")
    if n == "ccc3": return ("Vehicle System", "Digital Key (CCC)")
    if "carabc" in n: return ("CarABC", "CarPlay Adapter")
    if "car-bt" in n: return ("Generic", "Car Audio Adapter")       # <--- ADDED
    if "car music" in n: return ("Generic", "Car Audio Adapter")
    if "exhaust" in n: return ("Maxhaust/Thor", "Active Sound")
    if "highway controller" in n: return ("Ninebot / Xiaomi", "Electric Scooter")

    # High-End Vehicle Systems
    if "audi_mmi" in n: return ("Audi", "Vehicle System")
    if "mb hotspot" in n: return ("Mercedes-Benz", "Vehicle System")

    # --- 2. CAMERAS & DASHCAMS ---
    if "blackvue" in n: return ("BlackVue", "Dashcam")
    if "nextbase" in n: return ("Nextbase", "Dashcam")
    if "drv-a310w" in n: return ("Kenwood", "Dashcam")
    if "dashcam" in n or "garmin" in n: return ("Garmin", "Dashcam / GPS")
    if "ble_dēzl" in n: return ("Garmin", "Truck SatNav")
    if "f70pro" in n: return ("Thinkware", "Dashcam")
    if "osmo" in n: return ("DJI", "Gimbal / Camera")

    # --- 3. AUDIO (Sennheiser, Sony, Bose, JBL) ---
    if "momentum" in n: return ("Sennheiser", "Audio / Headset")
    if "jabra" in n: return ("Jabra", "Headset")
    if "heavys" in n: return ("Heavys", "Headphones")
    if "bose" in n: return ("Bose", "Audio")
    if "jlab" in n: return ("JLab", "Audio / Headset")
    
    # Sony Catch-all
    if any(x in n for x in ["wh-", "wf-", "wi-", "srs-", "ult wear", "sony"]):
        return ("Sony", "Audio / Headset")

    # JBL / Harman
    if any(x in n for x in ["flip", "clip", "boombox", "pulse", "jbl", "tune"]):
        return ("JBL (Harman)", "Audio / Speaker")

    # Generic Audio Catch-all (for "Buds", "Pods", "TWS")
    if any(x in n for x in ["buds", "pods", "tws", "true wireless"]): # <--- ADDED
        return ("Generic Audio", "Earbuds")

    # --- 4. SMART HOME / IOT ---
    if n == "ty": return ("Tuya (Smart Life)", "IoT / Smart Plug")
    if "technoshed" in n or "techno toaster" in n: return ("TechnoShed", "Custom Device")
    if "suta" in n: return ("Suta", "Smart Bed")
    if "bui330" in n: return ("Bosch", "eBike Display")
    if "govee" in n: return ("Govee", "Smart Light")
    if "ledble" in n: return ("Generic", "LED Controller")
    if "ion 200" in n: return ("Bontrager", "Bike Light")
    
    # Payment Terminals
    if "sumup" in n: return ("SumUp", "Payment Terminal")
    if "square" in n and "reader" in n: return ("Square", "Payment Terminal") # <--- ADDED
    
    # Solar
    if any(x in n for x in ["smartsolar", "bluesolar", "ve.direct"]):
        return ("Victron Energy", "Solar/Battery Controller")
    
    # Routers / TV
    if n.startswith("sky"): return ("Sky", "Set-top Box / Router")
    if n.startswith("vm") and any(c.isdigit() for c in n): return ("Virgin Media", "Router")
    if n.startswith("ee ") or n.startswith("ee-"): return ("EE", "Router")
    if "[tv]" in n or "samsung" in n: return ("Samsung", "Smart TV")

    # --- 5. WEARABLES & COMPUTING ---
    if "apple" in n or "ibeacon" in n: return ("Apple", "Device / Beacon")
    if "windows" in n: return ("Microsoft", "Windows Device")
    if "moto g" in n: return ("Motorola", "Mobile Phone")
    if "huawei" in n or n.startswith("gt2"): return ("Huawei", "Wearable")
    if "mi" == n or "mi band" in n: return ("Xiaomi", "Wearable")
    if "u9" == n: return ("Huami (Amazfit)", "Wearable")
    if "p66" in n: return ("Popglory", "Smartwatch")
    
    # Fitbit Collision Logic
    if "charge" in n:
        if "jbl" in n: return ("JBL (Harman)", "Audio / Speaker")
        if re.search(r'charge\s*\d+', n): return ("Fitbit", "Wearable")
    if "versa" in n or "inspire" in n or "fitbit" in n: return ("Fitbit", "Wearable")
    if "polar" in n: return ("Polar", "Heart Rate Monitor")
    if "whoop" in n: return ("Whoop", "Fitness Tracker")
    
    return (None, None)

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
                
                if current_len >= expected_cols:
                    # 1. Handle Commas in Names (Dynamic Slice)
                    merge_count = current_len - expected_cols
                    device_name = ','.join(parts[2 : 2 + 1 + merge_count])
                    data_slice_start = 2 + 1 + merge_count
                    
                    # 2. RUN INFERENCE ON NAME
                    inf_man, inf_type = infer_device_details(device_name)

                    # 3. Construct Base Row
                    # Schema: [time, addr, name, rssi, chan, sec, scanner, comp_id, app_id, MAN_TEXT, TYPE_TEXT]
                    final_row = [parts[0], parts[1], device_name] + parts[data_slice_start:]
                    
                    # 4. Normalization & Sanitization
                    if not is_new_format:
                        # Old format: Add None for IDs, plus inferred text
                        final_row.extend([None, None, inf_man, inf_type]) 
                    else:
                        # New format: Sanitize IDs
                        final_row[7] = clean_int(final_row[7]) # company_id
                        final_row[8] = clean_int(final_row[8]) # appearance_id
                        # Append inferred text to the end
                        final_row.append(inf_man)
                        final_row.append(inf_type)
                        
                    rows_to_insert.append(final_row)

        if rows_to_insert:
            conn = get_db_connection()
            if not conn: return False 

            cursor = conn.cursor()
            
            # UPDATED QUERY with 2 new columns
            insert_query = """
                INSERT INTO ble_logs 
                (timestamp_utc, addr, device_id, rssi, channel, security, scanner_device, company_id, appearance_id, man_text, type_text)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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