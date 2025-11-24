# ---------------------------------------------------------------------------------------
# PROJECT: ZIGGY CORE LOGIC (V4.1.1 - UNIFIED CODEBASE)
VERSION='4.1.1'
# HARDWARE: Abstraction Layer. Designed for Pico W (Tactical/Mini).
# ---------------------------------------------------------------------------------------
#
# === CHANGELOG & VERSION NOTES ===
# V4.1.1: NEW METRIC: Added Devices Per Minute (DPM) tracking to the BLE scan status
#         line for operational efficiency metrics.
# V4.1.0: CODEBASE UNIFICATION: Finalized separation of credentials and hardware interface.
# ---------------------------------------------------------------------------------------
#

# --- IMPORTS ---
# 1. Secured Credentials (NOT in GitHub)
from config_credentials import KNOWN_NETWORKS, FTP_HOST, FTP_PORT 
# 2. Hardware Interface (Device-specific Functions)
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

# --- EXTERNAL HARDWARE MODULES (Imported here to simplify core file) ---
try:
    import aioble                    
    import ntptime                   
    from machine import Pin 
except ImportError as e:
    print(f"Core Library Load Failure: {e}") 

# --- CONFIGURATION (Non-Sensitive Limits) ---
DEVICE_NAME = f"ZIGGY_{DEVICE_TYPE}_01" 

# --- LIMITS (Inherited from V4.0.13 Tuning) ---
BLE_SCAN_DURATION = 5000     
LOOP_INTERVAL_S = 1          
UPLOAD_INTERVAL_S = 600    # Back to 10 minutes      
MAX_FILE_SIZE_BYTES = 38 * 1024 
MAX_CHUNKS_PER_UPLOAD = 10      
STORAGE_CRITICAL_PCT = 0.80    
STORAGE_RESUME_PCT = 0.20      
LOG_DIR = "/logs"

# --- GLOBAL STATE ---
log_indices = {"ble": 0, "wifi": 0} 
last_upload_time = 0.0 
VERSION='4.1.1' 

# ==============================================================================
# --- CRITICAL UTILITY FUNCTIONS ---
# ==============================================================================

def get_formatted_time():
    """Returns the current RTC time as a formatted UTC string (YYYY-MM-DD HH:MM:SS)."""
    t = utime.localtime()
    if t[0] < 2024:
        return "2000-01-01 00:00:00"
    return f"{t[0]:04d}-{t[1]:02d}-{t[2]:02d} {t[3]:02d}:{t[4]:02d}:{t[5]:02d}"

def get_storage_stats():
    """Returns storage usage percentage."""
    try:
        s = uos.statvfs('/')
        total_blocks = s[2]
        free_blocks = s[3]
        used_pct = (total_blocks - free_blocks) / total_blocks
        return used_pct
    except:
        return 1.0

# The core logging function calls the hardware interface display function if available
def set_unified_status(mode, status_line, progress, total_files=0):
    """
    Unified status setter. Fetches data and calls the Tactical-specific display function 
    and prints to REPL for all devices.
    """
    # Fetch data needed by Tactical display *before* calling the display logic
    time_str = get_formatted_time().split(' ')[1][:5] # HH:MM only
    usage_pct = get_storage_stats()
    
    # 1. Update Tactical Display (OLED is handled inside hardware_interface)
    set_tactical_display(mode, status_line, progress, total_files, time_str, usage_pct, VERSION) 
    
    # 2. Console Debugging (for all devices)
    print(f"[STATUS] MODE:{mode} LINE:{status_line} PROG:{progress}")


def get_current_log_index(base_name):
    """Dynamically finds the highest log index."""
    try:
        uos.stat(LOG_DIR)
    except OSError:
        return 0
    
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
    """Writes a single entry directly to Flash with explicit close for integrity."""
    
    csv_line = f"{get_formatted_time()},{data_dict.get('addr','N/A')},{data_dict.get('id','N/A')},{data_dict.get('rssi','N/A')},{data_dict.get('channel','N/A')},{data_dict.get('security','N/A')},{DEVICE_NAME}\n"
    
    current_idx = log_indices[type_key]
    base_name = f"{type_key}_log" 
    filename = f"{LOG_DIR}/{base_name}_{current_idx:03d}.csv"
    
    # Check Rotation: If file size exceeds MAX_FILE_SIZE, start a new file
    try:
        if uos.stat(filename)[6] > MAX_FILE_SIZE_BYTES:
            current_idx += 1
            log_indices[type_key] = current_idx
            filename = f"{LOG_DIR}/{base_name}_{current_idx:03d}.csv"
            set_unified_status("FILE", f"Next: {current_idx:03d}", "ROTATE", 0) 
    except OSError:
        pass 

    # Write Immediate (CRITICAL: Explicit open/close to prevent data corruption)
    f = None
    try:
        f = open(filename, 'a')
        f.write(csv_line)
    except Exception as e:
        set_unified_status("ERROR", f"Write Fail: {e}", "HALTED", 0)
    finally:
        if f:
            f.close()


# --- WIFI/NTP & SCANNING LOGIC ---

def connect_for_sync(wlan):
    """Attempts to connect to ANY known network synchronously for time sync."""
    for net in KNOWN_NETWORKS:
        wlan.connect(net['ssid'], net['pass'])
        for i in range(15):
            if wlan.isconnected():
                return True
            utime.sleep(0.5)
        wlan.disconnect()
        wlan.active(True)
    return False

def sync_time_ntp():
    """Sets RTC via NTP on startup (Blocking)."""
    set_unified_status("BOOTING", "NTP:Connect...", "WAITING", 0)
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    
    if connect_for_sync(wlan):
        try:
            ntptime.settime() 
            if utime.localtime()[0] > 2024:
                 set_unified_status("SYNCED", "RTC Set", "RUNNING", 0)
        except Exception as e:
            set_unified_status("ERROR", f"NTP Error: {e}", "CHECK WIFi", 0)
    else:
        set_unified_status("WARNING", "NTP: Failed", "TIME DRIFT", 0)

    wlan.disconnect()
    wlan.active(False)

def run_wifi_scanner_once(wlan):
    """Performs a quick, synchronous Wi-Fi scan and logs results (for geo-positioning)."""
    
    set_unified_status("SCAN", "Wi-Fi Scan", "AdHoc", 0)
    gc.collect()

    try:
        networks = wlan.scan() 
        count = 0
        for ssid_bytes, bssid_bin, channel, rssi, security, hidden in networks:
            if rssi == 0: continue
            bssid_hex = ubinascii.hexlify(bssid_bin).decode()
            ssid_str = ssid_bytes.decode()
            
            wifi_data = {
                "addr": bssid_hex,
                "id": ssid_str,
                "rssi": rssi,
                "channel": channel,
                "security": security
            }
            append_log_entry('wifi', wifi_data)
            count += 1
        
        set_unified_status("SCAN", f"Logged {count} APs", "BLE", 0)
    
    except Exception as e:
        set_unified_status("ERROR", f"WiFi Scan Err: {e}", "BLE", 0)
        
    gc.collect()

# --- ASYNC TASKS ---

async def run_ble_cycle():
    """Scans for BLE and specifically targets Apple, Microsoft, and iBeacons. (Fixed v4.1.2)"""
    
    gc.collect() 
    notify('BLE', "BLE Scan Active") 
    
    scan_start_time = utime.time()
    devices_found = 0

    # Calculate upload countdown for status display
    time_remaining = UPLOAD_INTERVAL_S - (utime.time() - last_upload_time)
    if time_remaining < 0: time_remaining = 0
    progress_str = f"{int(time_remaining // 60):02d}m {int(time_remaining % 60):02d}s"
    
    set_unified_status("SCAN", "Deep Scan...", progress_str, 0)
    
    try:
        # Active=True requests the "Scan Response" packet (Device Name often lives here)
        async with aioble.scan(BLE_SCAN_DURATION, 100000, 100000, active=True) as scanner:
            async for result in scanner:
                
                # --- SAFETY CHECKS (Prevent Crashes) ---
                if not result.device: continue
                if not result.adv_data: continue # Skip empty packets
                
                rssi = result.rssi
                if rssi == 0: continue
                
                addr = ubinascii.hexlify(result.device.addr).decode()
                
                # --- DECODING LOGIC ---
                dev_id = "GENERIC"
                security = "Unknown"
                
                # Access the raw buffer directly (Safe & Fast)
                payload = result.adv_data
                
                # APPLE DEVICES (Company ID 0x004C)
                # Pattern: 0xFF (Mfg Data) | 0x4C | 0x00 
                if b'\xff\x4c\x00' in payload:
                    security = "Apple_Eco"
                    # Try to guess device type based on length/headers
                    if b'\x02\x15' in payload: # iBeacon
                         try:
                             # Extract UUID snippet safely
                             start = payload.find(b'\x02\x15')
                             uuid_part = ubinascii.hexlify(payload[start+2:start+10]).decode()
                             dev_id = f"iBeacon_{uuid_part}"
                         except:
                             dev_id = "iBeacon_Malformed"
                    elif b'\x10\x05' in payload: # AirDrop / Nearby
                         dev_id = "Apple_Nearby"
                    else:
                         dev_id = "Apple_Device"

                # MICROSOFT DEVICES (Company ID 0x0006)
                # Pattern: 0xFF | 0x06 | 0x00 
                elif b'\xff\x06\x00' in payload:
                    security = "MS_Windows"
                    dev_id = "Windows_Device"
                
                # EXPOSURE NOTIFICATIONS (COVID/Contact Tracing - UUID 0xFD6F)
                elif b'\x6f\xfd' in payload: # Little Endian 0xFD6F
                     security = "Exposure_Notif"
                     dev_id = "Contact_Trace"

                # NAMED DEVICES (If no specific vendor data found)
                if dev_id == "GENERIC" and result.name():
                    try:
                        dev_id = result.name()
                        security = "Named_Device"
                    except:
                        pass # Handle rare UTF-8 decode errors
                
                # LOG IT
                data = {"addr": addr, "id": dev_id, "rssi": rssi, "channel": "BLE", "security": security}
                append_log_entry('ble', data)
                devices_found += 1
                
    except Exception as e:
        # Detailed error printing for debugging
        sys.print_exception(e)
        set_unified_status("ERROR", "BLE Fail", "RETRY", 0)
    
    # --- METRICS ---
    scan_duration_s = utime.time() - scan_start_time
    dpm = (devices_found / scan_duration_s) * 60 if scan_duration_s > 0 else 0
    set_unified_status("SCAN", f"Found {devices_found} (Apple/MS)", f"DPM:{dpm:.1f}", 0)
    
    gc.collect()
    await asyncio.sleep(0.1) # Yield control
    gc.collect()
    await asyncio.sleep(0.1)

async def run_upload_cycle(critical=False):
    """Manages connection, opportunistic scan, and chunk upload."""
    
    set_unified_status("UPLOAD", "Start Wi-Fi...", "WAITING", 0)
    notify('UPLOAD', "Wi-Fi Active for Upload") 
    
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    
    is_connected = False
    
    # ASYNC Connection Logic (Iterate through KNOWN_NETWORKS)
    for net in KNOWN_NETWORKS:
        wlan.connect(net['ssid'], net['pass'])
        for i in range(15):
            if wlan.isconnected(): 
                is_connected = True
                break
            await asyncio.sleep(1)

        if is_connected:
            set_unified_status("WIFI", f" {net['ssid']}", "Scanning APs", 0)
            break
        else:
            wlan.disconnect() 
            
    if not is_connected:
        set_unified_status("FAIL", "No NET", "Resume", 0)
        notify('ERROR', "No Network Connection") 
        wlan.active(False)
        return False

    # --- OPPORTUNISTIC WIFI SCAN (DISABLED IN CRITICAL MODE) ---
    if not critical:  # <--- ADD THIS CHECK
        run_wifi_scanner_once(wlan)
    else:
        set_unified_status("UPLOAD", "Skip Scan", "CRITICAL", 0)
    
    # --- UPLOAD LOGIC ---
    success = True
    files = [f for f in uos.listdir(LOG_DIR) if f.endswith('.csv')]
    files.sort()
    
    batch_to_process = files[:MAX_CHUNKS_PER_UPLOAD]
    total_files = len(files)
    
    set_unified_status("UPLOAD", f"UP: {len(batch_to_process)}/{total_files}", "TRANSFER", total_files)

    for i, f in enumerate(batch_to_process):
        path = f"{LOG_DIR}/{f}"
        
        set_unified_status("UPLOAD", f"File: {f}", f"{i+1}", total_files)
        
        gc.collect()
        
        content = None
        try:
            with open(path, 'r') as log: 
                content = log.read()
        except Exception as e: 
            set_unified_status("ERROR", f"Rd Fail: {f}", "HALTED", 0)
            success = False
            break

        if not content: continue 

        # POST Request
        try:
            full_payload = "datetime_utc,addr,id,rssi,chan,sec,dev\n" + content
            r = requests.post(
                f"http://{FTP_HOST}:{FTP_PORT}/upload_log",
                headers={'Content-Type': 'text/csv', 'X-Pico-Device': f"{DEVICE_NAME}_{f}"},
                data=full_payload
            )
            if r.status_code == 200:
                uos.remove(path)
                notify('SAVE', "File Upload Success") 
            else:
                set_unified_status("FAIL", f"Srv Err: {r.status_code}", "RETRY", 0)
                notify('ERROR', f"Server Error {r.status_code}") 
                success = False
            r.close()
        except:
            set_unified_status("FAIL", "HTTP Timeout", "RETRY", 0)
            notify('ERROR', "HTTP Post Timeout") 
            success = False
        
        if not success: break
        
    set_unified_status("SCAN", "Upload Complete", "Resume Scan", 0)

    wlan.active(False) 
    notify('OFF', "Wi-Fi Deactivated") 
    return success

# --- INPUT MONITOR & MISSION CONTROL ---

async def input_monitor_task():
    """Monitors the Action button (GPIO 22) for manual upload trigger."""
    global last_upload_time

    while True:
        if check_manual_button():
            set_unified_status("MANUAL", "Manual Upload", "UPLOADING", 0)
            await run_upload_cycle(critical=True)
            last_upload_time = utime.time() 
            
            await asyncio.sleep_ms(500) 
            
        await asyncio.sleep_ms(50)

async def mission_control():
    """The central logic loop managing state transitions."""
    global last_upload_time
    
    while True:
        # 1. SAFETY CHECK (CRITICAL STORAGE TRAP)
        usage = get_storage_stats() 
        if usage > STORAGE_CRITICAL_PCT:
            set_unified_status("CRIT", "Storage > 80%!", "FORCING UP", 0)
            notify('ERROR', "Critical Storage Trap Engaged") 
            
            while get_storage_stats() > STORAGE_RESUME_PCT:
                await run_upload_cycle(critical=True)
                await asyncio.sleep(30)
            
            notify('OFF', "Storage Trap Cleared") 
        
        # 2. TIMER UPLOAD CHECK
        time_elapsed = utime.time() - last_upload_time
        if time_elapsed > UPLOAD_INTERVAL_S:
            await run_upload_cycle()
            last_upload_time = utime.time() 

        # 3. DO BLE SCAN
        await run_ble_cycle()
        await asyncio.sleep(LOOP_INTERVAL_S)

# --- RUNNER ---

def run():
    """Main entry point."""
    
    notify('OFF', "System Boot") 
    try: uos.mkdir(LOG_DIR)
    except: pass
    
    # 1. Time Synchronization (Blocking, synchronous)
    sync_time_ntp()
    print(f"Started at : {get_formatted_time()}")
    # 2. Initialize logging state
    log_indices['ble'] = get_current_log_index("ble")
    log_indices['wifi'] = get_current_log_index("wifi")
    global last_upload_time
    last_upload_time = utime.time() 
    
    # 3. Start the main loop
    loop = asyncio.get_event_loop()
    loop.create_task(mission_control())
    
    # Only start button monitor if on TACTICAL
    if DEVICE_TYPE == 'TACTICAL':
        loop.create_task(input_monitor_task())
    
    set_unified_status("SCAN", "System Online", "READY", 0)
    
    loop.run_forever()

if __name__ == "__main__":
    run()