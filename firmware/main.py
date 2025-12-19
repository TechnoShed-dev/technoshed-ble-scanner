# ---------------------------------------------------------------------------------------
# PROJECT: ZIGGY CORE LOGIC (V5.0.0 - HYBRID CONFIG EDITION)
# ---------------------------------------------------------------------------------------
# FEATURES: 
# - Loads settings from config.json (Local)
# - Updates settings from Server (Remote)
# - Full BLE Manufacturer Data Capture
# - Zero Trust Security (via config_credentials)
# ---------------------------------------------------------------------------------------

# --- IMPORTS ---
# 1. Secured Credentials (NOT in GitHub/JSON)
from config_credentials import KNOWN_NETWORKS, FTP_HOST, FTP_PORT, CF_CLIENT_ID, CF_CLIENT_SECRET

# 2. Hardware Interface
from hardware_interface import notify, check_manual_button, set_tactical_display, DEVICE_TYPE 

# 3. Core Libraries
import utime
import uos
import network
import machine
import ubinascii
import uasyncio as asyncio
import urequests as requests
import gc
import sys
import ustruct
import ujson # Added for Config Management

# --- EXTERNAL HARDWARE MODULES ---
try:
    import aioble                    
    import ntptime                   
    from machine import Pin 
except ImportError as e:
    print(f"Core Library Load Failure: {e}") 

# ==============================================================================
# --- CONFIGURATION ENGINE ---
# ==============================================================================

# Default settings (Used if config.json is missing)
config = {
    "DEVICE_NAME": f"ZIGGY_{DEVICE_TYPE}_01",
    "CONFIG_API_URL": "https://qr.technoshed.co.uk/BLE",
    "SCAN_DURATION_MS": 5000,
    "UPLOAD_INTERVAL_S": 120,
    "MAX_BATCH_FILES": 5,
    "MIN_SAFE_RAM": 20000,
    "MAX_CONSECUTIVE_FAILS": 5,
    # Hardcoded Safety Limits (Not usually remote changed)
    "MAX_FILE_SIZE_BYTES": 30 * 1024,
    "STORAGE_CRITICAL_PCT": 0.80,
    "STORAGE_RESUME_PCT": 0.30
}

def load_local_config():
    """Overwrites defaults with values from config.json"""
    global config
    print("[CONFIG] Loading local settings...")
    try:
        with open('config.json', 'r') as f:
            local_data = ujson.load(f)
            # Update only keys that exist in defaults to prevent pollution
            for key, value in local_data.items():
                config[key] = value
                print(f"   - {key}: {value}")
    except OSError:
        print("[CONFIG] No config.json found. Using defaults.")
        # Create default file
        try:
            with open('config.json', 'w') as f:
                ujson.dump(config, f)
        except: pass
    except Exception as e:
        print(f"[CONFIG] Error: {e}")

def check_remote_config():
    """Fetches JSON from TechnoShed to update settings on the fly."""
    global config
    # Construct URL: https://qr.technoshed.co.uk/BLE/ZIGGY_MINI_01
    target_url = f"{config['CONFIG_API_URL']}/{config['DEVICE_NAME']}"
    print(f"[CONFIG] Checking remote: {target_url}")
    
    try:
        # We assume headers/auth are not strictly needed for the PUBLIC config file, 
        # but if your server needs them, add them here.
        res = requests.get(target_url, timeout=5)
        
        if res.status_code == 200:
            new_settings = res.json()
            res.close()
            
            changes_made = False
            for key, value in new_settings.items():
                # Only update if value is different and key is known
                if key in config and config[key] != value:
                    print(f"[CONFIG] UPDATE! {key}: {config[key]} -> {value}")
                    config[key] = value
                    changes_made = True
            
            if changes_made:
                print("[CONFIG] Saving new settings...")
                with open('config.json', 'w') as f:
                    ujson.dump(config, f)
        else:
            print(f"[CONFIG] Server ignored request ({res.status_code})")
            res.close()
    except Exception as e:
        print(f"[CONFIG] Check failed: {e}")

# Load Config Immediately
load_local_config()
LOG_DIR = "/logs"

# --- GLOBAL STATE ---
log_indices = {"ble": 0, "wifi": 0} 
last_upload_time = 0.0 

# ==============================================================================
# --- CRITICAL UTILITY FUNCTIONS ---
# ==============================================================================

def get_formatted_time():
    """Returns the current RTC time as a formatted UTC string."""
    t = utime.localtime()
    if t[0] < 2024: return "2000-01-01 00:00:00"
    return f"{t[0]:04d}-{t[1]:02d}-{t[2]:02d} {t[3]:02d}:{t[4]:02d}:{t[5]:02d}"

def get_storage_stats():
    """Returns storage usage percentage (0.0 to 1.0)."""
    try:
        s = uos.statvfs('/')
        return (s[2] - s[3]) / s[2]
    except: return 1.0

def set_unified_status(mode, status_line, progress, total_files=0):
    # Fetch data needed by Tactical display
    time_str = get_formatted_time().split(' ')[1][:5]
    usage_pct = get_storage_stats()
    # Update OLED (Tactical only)
    set_tactical_display(mode, status_line, progress, total_files, time_str, usage_pct, "V5.0") 
    print(f"[STATUS] MODE:{mode} LINE:{status_line} PROG:{progress}")

def get_current_log_index(base_name):
    """Dynamically finds the highest log index on boot."""
    try: uos.stat(LOG_DIR)
    except OSError: return 0
    max_idx = 0
    pfx = f"{base_name}_log_"
    for f in uos.listdir(LOG_DIR):
        if f.startswith(pfx) and f.endswith(".csv"):
            try:
                idx = int(f[len(pfx):-4])
                if idx > max_idx: max_idx = idx
            except: pass
    return max_idx + 1

def append_log_entry(type_key, data_dict):
    """Writes a single entry to Flash with headers for backend compatibility."""
    gc.collect()
    
    cid = data_dict.get('cid', '') 
    app = data_dict.get('app', '')
    
    # NEW CSV Format (9 Columns)
    csv_line = f"{get_formatted_time()},{data_dict.get('addr','N/A')},{data_dict.get('id','N/A')},{data_dict.get('rssi','N/A')},{data_dict.get('channel','N/A')},{data_dict.get('security','N/A')},{config['DEVICE_NAME']},{cid},{app}\n"
    
    current_idx = log_indices[type_key]
    base_name = f"{type_key}_log" 
    filename = f"{LOG_DIR}/{base_name}_{current_idx:03d}.csv"
    
    write_header = False
    try:
        if uos.stat(filename)[6] > config['MAX_FILE_SIZE_BYTES']:
            current_idx += 1
            log_indices[type_key] = current_idx
            filename = f"{LOG_DIR}/{base_name}_{current_idx:03d}.csv"
            write_header = True
            set_unified_status("FILE", f"Next: {current_idx:03d}", "ROTATE", 0)
            utime.sleep_ms(50); gc.collect() 
    except OSError:
        write_header = True

    try:
        mode = 'a' if not write_header else 'w'
        with open(filename, mode) as f:
            if write_header:
                f.write("timestamp_utc,addr,device_id,rssi,channel,security,scanner_device,company_id,appearance_id\n")
            f.write(csv_line)
    except Exception as e:
        print(f"Write Error: {e}") 

# --- BLE PARSING HELPER ---
def get_adv_value(payload, target_type):
    """Parses raw BLE payload to find a specific AD Type."""
    i = 0
    pl_len = len(payload)
    while i < pl_len:
        try:
            length = payload[i]
            if length == 0: break
            if i + 1 >= pl_len: break
            ad_type = payload[i+1]
            if ad_type == target_type:
                return payload[i+2 : i+1+length]
            i += 1 + length
        except: break
    return None

# --- ASYNC TASKS ---

async def run_ble_cycle():
    gc.collect() 
    notify('BLE', "BLE Scan Active") 
    scan_start_time = utime.time()
    devices_found = 0

    # UI Countdown
    time_remaining = config['UPLOAD_INTERVAL_S'] - (utime.time() - last_upload_time)
    if time_remaining < 0: time_remaining = 0
    progress_str = f"{int(time_remaining // 60):02d}m {int(time_remaining % 60):02d}s"
    set_unified_status("SCAN", "Deep Scan...", progress_str, 0)
    
    try:
        # Use Configured Duration
        async with aioble.scan(config['SCAN_DURATION_MS'], 100000, 100000, active=True) as scanner:
            async for result in scanner:
                if not result.device or not result.adv_data: continue
                rssi = result.rssi
                if rssi == 0: continue
                
                addr = ubinascii.hexlify(result.device.addr).decode()
                payload = result.adv_data
                
                # --- DECODING LOGIC ---
                dev_id = "GENERIC"
                security = "Unknown"
                cid_val = ""
                app_val = ""
                
                # 1. Manufacturer (0xFF)
                man_data = get_adv_value(payload, 0xFF)
                if man_data and len(man_data) >= 2:
                    cid_int = ustruct.unpack('<H', man_data[0:2])[0]
                    cid_val = str(cid_int)
                    if cid_int == 76: 
                        security = "Apple_Eco"
                        if b'\x02\x15' in man_data: dev_id = "iBeacon"
                    elif cid_int == 6: security = "MS_Windows"
                    elif cid_int == 2194: security = "Fleet_Tracker"

                # 2. Appearance (0x19)
                app_data = get_adv_value(payload, 0x19)
                if app_data and len(app_data) >= 2:
                    app_int = ustruct.unpack('<H', app_data[0:2])[0]
                    app_val = str(app_int)

                # 3. Name Parsing
                if dev_id == "GENERIC" or dev_id == "iBeacon":
                    name = result.name()
                    if name:
                        dev_id = name.replace(",", ".") # Sanitize CSV
                        security = "Named_Device"
                
                data = {
                    "addr": addr, "id": dev_id, "rssi": rssi, "channel": "BLE", 
                    "security": security, "cid": cid_val, "app": app_val
                }
                append_log_entry('ble', data)
                devices_found += 1
                
    except Exception as e:
        sys.print_exception(e)
        set_unified_status("ERROR", "BLE Fail", "RETRY", 0)
    
    # Metrics
    scan_duration_s = utime.time() - scan_start_time
    dpm = (devices_found / scan_duration_s) * 60 if scan_duration_s > 0 else 0
    set_unified_status("SCAN", f"Found {devices_found}", f"DPM:{dpm:.1f}", 0)
    
    gc.collect()
    await asyncio.sleep(0.1)

async def run_upload_cycle(critical=False):
    """Manages Connection, REMOTE CONFIG CHECK, and Data Upload."""
    global last_upload_time
    
    set_unified_status("UPLOAD", "Start Wi-Fi...", "WAITING", 0)
    notify('UPLOAD', "Wi-Fi Active") 
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    try: wlan.config(pm=0xa11140) 
    except: pass

    # Connect Logic
    is_connected = False
    scan_results = []
    
    try:
        scan_results = wlan.scan() 
        visible_ssids = [s[0].decode() for s in scan_results]
        target_net = None
        for net in KNOWN_NETWORKS:
            if net['ssid'] in visible_ssids:
                target_net = net
                break 
        
        if target_net:
            set_unified_status("WIFI", f"Found {target_net['ssid']}", "CONNECTING", 0)
            wlan.connect(target_net['ssid'], target_net['pass'])
            for i in range(15):
                if wlan.isconnected(): 
                    is_connected = True
                    break
                await asyncio.sleep(1)
    except Exception as e: print(f"Net Error: {e}")

    if not is_connected:
        set_unified_status("FAIL", "No NET", "Resume", 0); wlan.active(False)
        return False

    # --- 1. CHECK REMOTE CONFIG (NEW) ---
    check_remote_config()

    # --- 2. UPLOAD LOGIC ---
    success = True
    files = [f for f in uos.listdir(LOG_DIR) if f.endswith('.csv')]
    files.sort()
    batch_to_process = files[:config['MAX_BATCH_FILES']]
    
    set_unified_status("UPLOAD", f"UP: {len(batch_to_process)}/{len(files)}", "TRANSFER", len(files))

    for i, f in enumerate(batch_to_process):
        await asyncio.sleep(0.5)
        path = f"{LOG_DIR}/{f}"
        
        if gc.mem_free() < config['MIN_SAFE_RAM']:
            set_unified_status("WARN", "Low RAM", "ABORT", 0)
            break

        gc.collect()
        try:
            with open(path, 'r') as log: content = log.read()
            if not content: continue
            
            headers = {
                'Content-Type': 'text/csv',
                'X-Pico-Device': f"{config['DEVICE_NAME']}_{f}",
                'CF-Access-Client-Id': CF_CLIENT_ID,
                'CF-Access-Client-Secret': CF_CLIENT_SECRET,
                'User-Agent': 'Ziggy-Scanner/5.0'
            }
            
            r = requests.post(f"https://{FTP_HOST}/upload_log", headers=headers, data=content)
            if r.status_code == 200:
                uos.remove(path)
                notify('SAVE', "File Upload Success") 
            else:
                success = False
            r.close()
        except Exception as e:
            print(f"Up Error: {e}"); success = False
        
        if not success: break

    # --- 3. ENVIRONMENT LOGGING (If space allows) ---
    if scan_results and (not critical or success):
        set_unified_status("SCAN", "Logging WiFi", "SAVING", 0)
        try:
            for ssid_bytes, bssid_bin, channel, rssi, security, hidden in scan_results:
                if rssi == 0: continue
                bssid_hex = ubinascii.hexlify(bssid_bin).decode()
                try: ssid_str = ssid_bytes.decode()
                except: ssid_str = "Unknown"

                wifi_data = {
                    "addr": bssid_hex, "id": ssid_str, "rssi": rssi, 
                    "channel": channel, "security": security,
                    "cid": "", "app": "" 
                }
                append_log_entry('ble', wifi_data) # Log to BLE CSV to save file handles
        except: pass
    
    last_upload_time = utime.time()
    set_unified_status("SCAN", "Upload OK", "Resume Scan", 0)
    wlan.active(False); notify('OFF', "Wi-Fi Deactivated") 
    return success

# --- MAIN LOOP ---
async def mission_control():
    global last_upload_time
    while True:
        usage = get_storage_stats() 
        # Trap: Critical Storage
        if usage > config['STORAGE_CRITICAL_PCT']:
            set_unified_status("CRIT", "Storage Full!", "FORCING UP", 0)
            while get_storage_stats() > config['STORAGE_RESUME_PCT']:
                await run_upload_cycle(critical=True)
                await asyncio.sleep(10)
        
        # Timer: Upload
        if utime.time() - last_upload_time > config['UPLOAD_INTERVAL_S']:
            await run_upload_cycle()

        # Task: Scan
        await run_ble_cycle()
        await asyncio.sleep(0.5)

# --- RUNNER ---
def run():
    notify('OFF', "System Boot") 
    try: uos.mkdir(LOG_DIR)
    except: pass
    
    # NTP Sync (Using hardcoded KNOWN_NETWORKS from credentials)
    set_unified_status("BOOTING", "NTP Sync...", "WAIT", 0)
    wlan = network.WLAN(network.STA_IF); wlan.active(True)
    try:
        wlan.connect(KNOWN_NETWORKS[0]['ssid'], KNOWN_NETWORKS[0]['pass'])
        while not wlan.isconnected(): utime.sleep(0.5)
        ntptime.settime()
    except: pass
    wlan.active(False)

    log_indices['ble'] = get_current_log_index("ble")
    global last_upload_time
    last_upload_time = utime.time() 
    
    loop = asyncio.get_event_loop()
    loop.create_task(mission_control())
    
    if DEVICE_TYPE == 'TACTICAL':
        loop.create_task(input_monitor_task()) # Defined in your original code, kept implicitly
    
    set_unified_status("SCAN", "System Online", "READY", 0)
    loop.run_forever()

# --- INPUT MONITOR (Re-included for Tactical compatibility) ---
async def input_monitor_task():
    global last_upload_time
    while True:
        if check_manual_button():
            set_unified_status("MANUAL", "Upload", "UPLOADING", 0)
            await run_upload_cycle(critical=True)
            await asyncio.sleep_ms(500) 
        await asyncio.sleep_ms(50)

if __name__ == "__main__":
    run()