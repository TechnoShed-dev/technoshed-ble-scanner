# ---------------------------------------------------------------------------------------
# PROJECT: ZIGGY CORE LOGIC (V4.1.3 - STABILITY GOLD MASTER)
VERSION='4.1.3'
# HARDWARE: Abstraction Layer. Designed for Pico W (Tactical/Mini).
# ---------------------------------------------------------------------------------------
#
# === CHANGELOG & VERSION NOTES ===
# V4.1.3: STABILITY & EFFICIENCY:
#         - CRITICAL: Added wlan.config(pm=0xa11140) to disable power saving (Fixes SSL Drops).
#         - LOGIC: Implemented "Smart Scan" connection (Scans first, connects to best match).
#         - OPTIMIZATION: Merged WiFi environment logging into the connection phase (Removed separate scan).
#         - NETWORKING: Added User-Agent header to bypass Cloudflare Bot Fight Mode.
# V4.1.2: ADDED: Client id and secret for Zero Trust Security. Changed to HTTPS.
# V4.1.1: METRICS: Added DPM (Devices Per Minute) tracking.
# ---------------------------------------------------------------------------------------

# --- IMPORTS ---
# 1. Secured Credentials (NOT in GitHub)
from config_credentials import KNOWN_NETWORKS, FTP_HOST, FTP_PORT, CF_CLIENT_ID, CF_CLIENT_SECRET
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

# --- EXTERNAL HARDWARE MODULES ---
try:
    import aioble                    
    import ntptime                   
    from machine import Pin 
except ImportError as e:
    print(f"Core Library Load Failure: {e}") 

# --- CONFIGURATION ---
DEVICE_NAME = f"ZIGGY_{DEVICE_TYPE}_01" 

# --- LIMITS (Technoshed "Gold Master" Tuning) ---
BLE_SCAN_DURATION = 5000        # 5 Seconds per scan tick
LOOP_INTERVAL_S = 1             # Yield time between ticks
UPLOAD_INTERVAL_S = 300         # 5 Minutes (Reduced gap for better resolution)
MAX_FILE_SIZE_BYTES = 32 * 1024 # 32KB (Critical: Larger files cause ENOMEM during SSL)
MAX_CHUNKS_PER_UPLOAD = 5       # 5 Files (Reduced from 10 to prevent Heap Fragmentation)
STORAGE_CRITICAL_PCT = 0.80     # Stop scanning if > 80% full
STORAGE_RESUME_PCT = 0.30       # Resume scanning only when < 30%
LOG_DIR = "/logs"

# --- GLOBAL STATE ---
log_indices = {"ble": 0, "wifi": 0} 
last_upload_time = 0.0 
 

# ==============================================================================
# --- CRITICAL UTILITY FUNCTIONS ---
# ==============================================================================

def get_formatted_time():
    """Returns the current RTC time as a formatted UTC string (YYYY-MM-DD HH:MM:SS)."""
    t = utime.localtime()
    # Check for 'Ghost Dates' (Pre-sync)
    if t[0] < 2024:
        return "2000-01-01 00:00:00"
    return f"{t[0]:04d}-{t[1]:02d}-{t[2]:02d} {t[3]:02d}:{t[4]:02d}:{t[5]:02d}"

def get_storage_stats():
    """Returns storage usage percentage (0.0 to 1.0)."""
    try:
        s = uos.statvfs('/')
        total_blocks = s[2]
        free_blocks = s[3]
        used_pct = (total_blocks - free_blocks) / total_blocks
        return used_pct
    except:
        return 1.0 # Assume full on error to trigger safety mechanisms

def set_unified_status(mode, status_line, progress, total_files=0):
    """
    Unified status setter. Fetches data and calls the Tactical-specific display function 
    and prints to REPL for debugging.
    """
    # Fetch data needed by Tactical display
    time_str = get_formatted_time().split(' ')[1][:5] # HH:MM only
    usage_pct = get_storage_stats()
    
    # 1. Update OLED (Handled inside hardware_interface)
    set_tactical_display(mode, status_line, progress, total_files, time_str, usage_pct, VERSION) 
    
    # 2. Console Debugging
    print(f"[STATUS] MODE:{mode} LINE:{status_line} PROG:{progress}")


def get_current_log_index(base_name):
    """Dynamically finds the highest log index on boot."""
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
    """Writes a single entry directly to Flash with memory and power protection."""
    
    # 1. CRITICAL: Clear RAM before creating strings/files
    gc.collect()
    
    # Format: Time, Addr, ID, RSSI, Chan, Sec, Device
    csv_line = f"{get_formatted_time()},{data_dict.get('addr','N/A')},{data_dict.get('id','N/A')},{data_dict.get('rssi','N/A')},{data_dict.get('channel','N/A')},{data_dict.get('security','N/A')},{DEVICE_NAME}\n"
    
    current_idx = log_indices[type_key]
    base_name = f"{type_key}_log" 
    filename = f"{LOG_DIR}/{base_name}_{current_idx:03d}.csv"
    
    # Check Rotation Logic
    try:
        if uos.stat(filename)[6] > MAX_FILE_SIZE_BYTES:
            current_idx += 1
            log_indices[type_key] = current_idx
            filename = f"{LOG_DIR}/{base_name}_{current_idx:03d}.csv"
            
            # Update Display
            set_unified_status("FILE", f"Next: {current_idx:03d}", "ROTATE", 0)
            
            # 2. SAFETY PAUSE: Prevent Brownouts
            # Updating OLED + Opening New File + BLE radio can cause voltage dip.
            utime.sleep_ms(50) 
            gc.collect() 
            
    except OSError:
        pass # File doesn't exist yet, created on write.

    # Write Immediate
    f = None
    try:
        f = open(filename, 'a')
        f.write(csv_line)
    except Exception as e:
        print(f"Write Error: {e}") 
    finally:
        if f:
            f.close()

# --- WIFI/NTP & SCANNING LOGIC ---

def connect_for_sync(wlan):
    """Smart Connect (Blocking): Scans first, connects to highest priority visible network."""
    
    # 1. SCAN PHASE
    set_unified_status("BOOTING", "Scanning...", "SEEKING", 0)
    target_net = None
    
    try:
        scan_results = wlan.scan()
        visible_ssids = [s[0].decode() for s in scan_results]
        
        # Priority Match: Iterate known list to find best match
        for net in KNOWN_NETWORKS:
            if net['ssid'] in visible_ssids:
                target_net = net
                break 
    except Exception as e:
        print(f"Boot Scan Error: {e}")

    # 2. CONNECT PHASE
    if target_net:
        set_unified_status("BOOTING", f"Found {target_net['ssid']}", "JOINING", 0)
        
        # Stability: Disable Power Saving for robust handshake
        wlan.config(pm=0xa11140) 
        
        wlan.connect(target_net['ssid'], target_net['pass'])
        
        # Wait for connection
        for i in range(15):
            if wlan.isconnected():
                return True
            utime.sleep(0.5)
            
    return False

def sync_time_ntp():
    """Sets RTC via NTP on startup."""
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


# --- ASYNC TASKS ---

async def run_ble_cycle():
    """Scans for BLE and targets Apple, Microsoft, and iBeacons."""
    
    gc.collect() 
    notify('BLE', "BLE Scan Active") 
    
    scan_start_time = utime.time()
    devices_found = 0

    # UI Countdown
    time_remaining = UPLOAD_INTERVAL_S - (utime.time() - last_upload_time)
    if time_remaining < 0: time_remaining = 0
    progress_str = f"{int(time_remaining // 60):02d}m {int(time_remaining % 60):02d}s"
    
    set_unified_status("SCAN", "Deep Scan...", progress_str, 0)
    
    try:
        # Active Scan to get names
        async with aioble.scan(BLE_SCAN_DURATION, 100000, 100000, active=True) as scanner:
            async for result in scanner:
                
                # --- SAFETY CHECKS ---
                if not result.device: continue
                if not result.adv_data: continue 
                
                rssi = result.rssi
                if rssi == 0: continue
                
                addr = ubinascii.hexlify(result.device.addr).decode()
                
                # --- DECODING LOGIC ---
                dev_id = "GENERIC"
                security = "Unknown"
                payload = result.adv_data
                
                # VENDOR ID MATCHING
                if b'\xff\x4c\x00' in payload: # Apple (0x004C)
                    security = "Apple_Eco"
                    if b'\x02\x15' in payload: 
                         try:
                             start = payload.find(b'\x02\x15')
                             uuid_part = ubinascii.hexlify(payload[start+2:start+10]).decode()
                             dev_id = f"iBeacon_{uuid_part}"
                         except:
                             dev_id = "iBeacon_Malformed"
                    elif b'\x10\x05' in payload: dev_id = "Apple_Nearby"
                    else: dev_id = "Apple_Device"

                elif b'\xff\x06\x00' in payload: # Microsoft (0x0006)
                    security = "MS_Windows"
                    dev_id = "Windows_Device"
                
                elif b'\x6f\xfd' in payload: # Exposure Notification
                     security = "Exposure_Notif"
                     dev_id = "Contact_Trace"

                # NAME PARSING
                if dev_id == "GENERIC" and result.name():
                    try:
                        dev_id = result.name()
                        security = "Named_Device"
                    except: pass 
                
                # LOG IT
                data = {"addr": addr, "id": dev_id, "rssi": rssi, "channel": "BLE", "security": security}
                append_log_entry('ble', data)
                devices_found += 1
                
    except Exception as e:
        sys.print_exception(e)
        set_unified_status("ERROR", "BLE Fail", "RETRY", 0)
    
    # METRICS
    scan_duration_s = utime.time() - scan_start_time
    dpm = (devices_found / scan_duration_s) * 60 if scan_duration_s > 0 else 0
    set_unified_status("SCAN", f"Found {devices_found} (Apple/MS)", f"DPM:{dpm:.1f}", 0)
    
    gc.collect()
    await asyncio.sleep(0.1) 

async def run_upload_cycle(critical=False):
    """Manages Smart Connection, Data Upload, and Environment Logging."""
    
    set_unified_status("UPLOAD", "Start Wi-Fi...", "WAITING", 0)
    notify('UPLOAD', "Wi-Fi Active for Upload") 
    
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    
    # 1. CRITICAL: DISABLE POWER SAVING
    # This prevents 'ECONNABORTED' during heavy SSL handshakes
    try: wlan.config(pm=0xa11140) 
    except: pass

    is_connected = False
    target_net = None
    scan_results = [] 

    set_unified_status("WIFI", "Scanning...", "SEEKING", 0)
    
    # 2. SMART SCAN (Capture environment once)
    try:
        scan_results = wlan.scan() 
        visible_ssids = [s[0].decode() for s in scan_results]
        for net in KNOWN_NETWORKS:
            if net['ssid'] in visible_ssids:
                target_net = net
                break 
    except Exception as e:
        print(f"Scan Error: {e}")

    # 3. SURGICAL CONNECT
    if target_net:
        set_unified_status("WIFI", f"Found {target_net['ssid']}", "CONNECTING", 0)
        wlan.connect(target_net['ssid'], target_net['pass'])
        for i in range(15):
            if wlan.isconnected(): 
                is_connected = True
                break
            set_unified_status("WIFI", f" {target_net['ssid']}", f"Auth {i+1}/15", 0)
            await asyncio.sleep(1)
    else:
        set_unified_status("WIFI", "No Known Net", "SKIPPING", 0)
            
    if not is_connected:
        set_unified_status("FAIL", "No NET", "Resume", 0)
        notify('ERROR', "No Network Connection") 
        wlan.active(False)
        return False

    # --- UPLOAD LOGIC ---
    success = True
    files = [f for f in uos.listdir(LOG_DIR) if f.endswith('.csv')]
    files.sort()
    
    # Process batch (Limited to prevent heap fragmentation)
    batch_to_process = files[:MAX_CHUNKS_PER_UPLOAD]
    total_files = len(files)
    
    set_unified_status("UPLOAD", f"UP: {len(batch_to_process)}/{total_files}", "TRANSFER", total_files)

    for i, f in enumerate(batch_to_process):
        # Stability Pause: Allow network stack to drain
        await asyncio.sleep(1) 
        
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
        
        # Headers (Including User-Agent for Cloudflare)
        headers = {
            'Content-Type': 'text/csv',
            'X-Pico-Device': f"{DEVICE_NAME}_{f}",
            'CF-Access-Client-Id': CF_CLIENT_ID,
            'CF-Access-Client-Secret': CF_CLIENT_SECRET,
            'User-Agent': 'Ziggy-Scanner/4.1'
        }
        
        try:
            # HTTPS POST (Port 443 implicit)
            r = requests.post(f"https://{FTP_HOST}/upload_log", headers=headers, data=content)
            
            if r.status_code == 200:
                uos.remove(path)
                notify('SAVE', "File Upload Success") 
            else:
                set_unified_status("FAIL", f"Srv Err: {r.status_code}", "RETRY", 0)
                notify('ERROR', f"Server Error {r.status_code}") 
                success = False
            r.close()
        except Exception as e:
            sys.print_exception(e) # Print real error (Memory vs Network)
            set_unified_status("FAIL", "Net/SSL Fail", "RETRY", 0)
            notify('ERROR', "Network Failure") 
            success = False
        
        if not success: break

    # --- ENVIRONMENT LOGGING ---
    # "Earn Your Keep": Only log the WiFi environment if we successfully uploaded data
    # (freeing space) OR if we are not in critical storage mode.
    if scan_results and (not critical or success):
        set_unified_status("SCAN", "Logging WiFi", "SAVING", 0)
        try:
            count = 0
            for ssid_bytes, bssid_bin, channel, rssi, security, hidden in scan_results:
                if rssi == 0: continue
                bssid_hex = ubinascii.hexlify(bssid_bin).decode()
                try: ssid_str = ssid_bytes.decode()
                except: ssid_str = "Unknown"

                wifi_data = {"addr": bssid_hex, "id": ssid_str, "rssi": rssi, "channel": channel, "security": security}
                append_log_entry('wifi', wifi_data)
                count += 1
            print(f"[WIFI] Logged {count} networks.")
        except Exception as e:
            print(f"WiFi Log Error: {e}")
    
    set_unified_status("SCAN", "Upload OK", "Resume Scan", 0)
    wlan.active(False) 
    notify('OFF', "Wi-Fi Deactivated") 
    return success

# --- INPUT MONITOR & MISSION CONTROL ---

async def input_monitor_task():
    """Monitors the Action button (GPIO 22) for manual upload trigger."""
    global last_upload_time

    while True:
        if check_manual_button():
            set_unified_status("MANUAL", "Upload", "UPLOADING", 0)
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
            
            # Trap loop: Stop scanning, just try to upload
            while get_storage_stats() > STORAGE_RESUME_PCT:
                await run_upload_cycle(critical=True)
                await asyncio.sleep(10)
                gc.collect()
            
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
    
    # 1. Time Synchronization
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
    
    if DEVICE_TYPE == 'TACTICAL':
        loop.create_task(input_monitor_task())
    
    set_unified_status("SCAN", "System Online", "READY", 0)
    loop.run_forever()

if __name__ == "__main__":
    run()