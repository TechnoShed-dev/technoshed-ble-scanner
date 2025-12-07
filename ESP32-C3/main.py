# ---------------------------------------------------------------------------------------
# ZIGGY MICRO APPLICATION - V2.2.1 (Boot & Blast)
# ---------------------------------------------------------------------------------------
# DEVICE:  ESP32-C3 (Ziggy Micro)
# CHANGE:  V2.2.1 - Added tweaks for funcatioanlity.
#          Checks for backlog immediately on startup while RAM is fresh and
#          WiFi is likely still active from boot.py.
# ---------------------------------------------------------------------------------------

import uasyncio as asyncio
import aioble
import bluetooth
import urequests as requests
import secrets
import time
import machine
import gc
import binascii
import network
import os
import errno

# --- CONFIGURATION ---
DEVICE_NAME = "DEVICE_NAME" # <<<----- SET DEVICE NAME HERE

SCAN_DURATION_MS = 30000 # 30 Second scan to capture distant pings
UPLOAD_INTERVAL_S = 40   # 40 Second upload cycle 
  
# CATCH-UP SETTINGS
MAX_BATCH_FILES = 5      
MIN_SAFE_RAM = 30000     # Increased safety buffer (was 25000)
MAX_STORED_FILES = 50    
MAX_CONSECUTIVE_FAILS = 5 

# --- LED SETUP ---
try:
    led = machine.Pin("LED", machine.Pin.OUT)
except:
    led = machine.Pin(8, machine.Pin.OUT) 

# --- PERSISTENT COUNTER ---
def get_next_counter():
    count = 3000
    try:
        if "counter.txt" in os.listdir():
            with open("counter.txt", "r") as f:
                count = int(f.read())
    except: pass
    new_count = count + 1
    try:
        with open("counter.txt", "w") as f:
            f.write(str(new_count))
    except: pass
    return new_count

# --- HELPERS ---
def get_formatted_time():
    t = time.localtime()
    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}".format(t[0], t[1], t[2], t[3], t[4], t[5])

def format_mac_address(addr_hex):
    try:
        s = addr_hex.upper()
        return ':'.join(s[i:i+2] for i in range(0, 12, 2))
    except:
        return "00:00:00:00:00:00"

# --- SMART NETWORK MANAGER ---
async def connect_smart_wifi():
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    
    # POWER MANAGEMENT FIX
    try: wlan.config(pm=0xa11140)
    except: pass 

    # SCAN
    print("[WiFi] Scanning...")
    target_net = None
    try:
        scan_results = wlan.scan()
        visible_ssids = [s[0].decode() for s in scan_results]
        for net in secrets.KNOWN_NETWORKS:
            if net['ssid'] in visible_ssids:
                target_net = net
                break
    except Exception as e:
        print(f"[WiFi] Scan Error: {e}")
        return False

    # CONNECT
    if target_net:
        print(f"[WiFi] Connecting to {target_net['ssid']}...")
        wlan.connect(target_net['ssid'], target_net['pass'])
        for i in range(15):
            if wlan.isconnected():
                print(f"[WiFi] Online.")
                return True
            await asyncio.sleep(1)
    
    return False

def disconnect_wifi():
    wlan = network.WLAN(network.STA_IF)
    if wlan.active():
        wlan.disconnect()
        wlan.active(False)
        print("[WiFi] Radio OFF")
    
# --- FILE SYSTEM MANAGERS ---
def save_scan_to_flash(scan_results, counter):
    gc.collect()
    timestamp = get_formatted_time()
    filename = f"{DEVICE_NAME}_ble_log_{counter}.csv"
    
    try:
        with open(filename, 'w') as f:
            f.write("timestamp,addr,id,rssi,channel,security,device\n")
            for result in scan_results:
                formatted_addr = format_mac_address(result['addr'])
                dev_id = result['id'].replace(",", " ") 
                rssi = result['rssi']
                security = result['security']
                
                line = "{},{},{},{},{},{},{}\n".format(
                    timestamp, formatted_addr, dev_id, rssi, "BLE", security, DEVICE_NAME
                )
                f.write(line)
        print(f"[Storage] Saved {filename}")
        return True
    except Exception as e:
        print(f"[Storage] Write Error: {e}")
        return False

def manage_storage():
    try:
        files = [f for f in os.listdir() if f.endswith('.csv') and '_ble_log_' in f]
        if len(files) > MAX_STORED_FILES:
            files.sort()
            excess = len(files) - MAX_STORED_FILES
            for i in range(excess):
                os.remove(files[i])
                print(f"[Storage] Pruned: {files[i]}")
    except Exception as e:
        print(f"[Storage] Cleanup Error: {e}")

def get_oldest_files(limit):
    try:
        files = [f for f in os.listdir() if f.endswith('.csv') and '_ble_log_' in f]
        files.sort()
        return files[:limit]
    except:
        return []

def upload_single_file(filename):
    """Reads a file and uploads it. Returns True on success."""
    # Aggressive GC before allocation
    gc.collect()
    
    csv_payload = None
    try:
        with open(filename, 'r') as f:
            csv_payload = f.read()
    except Exception as e:
        print(f"[Upload] Read Error: {e}")
        try: os.remove(filename) 
        except: pass
        return False

    free_ram = gc.mem_free()
    
    # STRICT SAFETY CHECK
    if free_ram < MIN_SAFE_RAM:
        print(f"[Upload] Low RAM ({free_ram}). Aborting upload.")
        return False

    headers = {
        'Content-Type': 'text/csv',
        'X-Pico-Device': filename,
        'CF-Access-Client-Id': secrets.CF_CLIENT_ID,
        'CF-Access-Client-Secret': secrets.CF_CLIENT_SECRET,
        'User-Agent': 'Ziggy-Micro/2.2'
    }

    try:
        led.on()
        print(f"[Upload] Sending {filename} ({len(csv_payload)}b)...")
        
        # GC again right before the heavy SSL handshake
        gc.collect() 
        
        response = requests.post(secrets.SERVER_URL, headers=headers, data=csv_payload, timeout=20)
        led.off()
        
        status = response.status_code
        response.close() 
        
        # Clear the payload from RAM immediately
        csv_payload = None
        gc.collect()
        
        if status == 200:
            print(f"[Upload] Success")
            return True
        else:
            print(f"[Upload] Reject: {status}")
            return False 
            
    except OSError as e:
        led.off()
        if e.errno == errno.ENOMEM:
            print("[Upload] Error: ENOMEM (RAM Exhausted)")
        else:
            print(f"[Upload] Network Error: {e}")
        return False
    except Exception as e:
        led.off()
        print(f"[Upload] Error: {e}")
        return False

async def scan_and_upload_loop():
    print("[ZiggyMicro] Starting V2.2 (Boot & Blast)...")
    
    # --- PHASE 0: BOOT BACKLOG CLEAR ---
    # Try to upload immediately using the fresh RAM and connection from boot.py
    # We do NOT run the disconnect_wifi() here initially.
    print("[System] Checking backlog on boot...")
    if get_oldest_files(MAX_BATCH_FILES):
        print("[System] Backlog found. Using boot connection to upload...")
        
        # We reuse the connection if possible. 
        # If boot.py failed but this runs, connect_smart_wifi will try again.
        if await connect_smart_wifi():
            pending_files = get_oldest_files(MAX_BATCH_FILES)
            for filename in pending_files:
                if gc.mem_free() < MIN_SAFE_RAM:
                    print(f"[System] Low RAM ({gc.mem_free()}). Stopping boot batch.")
                    break
                
                if upload_single_file(filename):
                     print(f"[Storage] Deleting {filename}")
                     try: os.remove(filename)
                     except: pass
                else:
                    print("[System] Boot Upload Error. Stopping.")
                    break
                gc.collect()
                time.sleep(1)
        else:
            print("[System] Boot Connect Failed.")

    # NOW we disconnect to ensure radio silence for the first scan
    disconnect_wifi()
    
    fail_count = 0
    
    while True:
        gc.collect()
        
        # --- PHASE 1: SCAN ---
        found_devices = []
        print(f"\n[Scanner] BLE Scanning for {int(SCAN_DURATION_MS/1000)} s...")
        try:
            async with aioble.scan(duration_ms=SCAN_DURATION_MS, interval_us=30000, window_us=30000, active=True) as scanner:
                async for result in scanner:
                    if not result.device: continue
                    
                    dev_id = "GENERIC"
                    security = "Unknown"
                    payload = bytes(result.adv_data) if result.adv_data else b''
                    
                    if b'\xff\x4c\x00' in payload:
                        security = "Apple_Eco"
                        if b'\x02\x15' in payload: 
                             try:
                                 start = payload.find(b'\x02\x15')
                                 uuid_part = binascii.hexlify(payload[start+2:start+10]).decode()
                                 dev_id = f"iBeacon_{uuid_part}"
                             except: dev_id = "iBeacon_Malformed"
                        elif b'\x10\x05' in payload: dev_id = "GENERIC"
                        else: dev_id = "GENERIC"
                    elif b'\x6f\xfd' in payload:
                          security = "Exposure_Notif"
                          dev_id = "Contact_Trace"
                    
                    if result.name():
                        if dev_id == "GENERIC": security = "Named_Device"
                        dev_id = result.name()

                    # NAMED ONLY FILTER
                    if dev_id == "GENERIC" and security == "Unknown":
                        continue

                    raw_addr = binascii.hexlify(result.device.addr).decode()
                    if any(d['addr'] == raw_addr for d in found_devices): continue
                    
                    found_devices.append({
                        'addr': raw_addr, 'id': dev_id, 'rssi': result.rssi, 'security': security
                    })
                    print(f"   -> Found: {dev_id}")
        except Exception as e:
            print(f"[Scanner] Error: {e}")

        # --- PHASE 2: SAVE ---
        if found_devices:
            counter = get_next_counter()
            save_scan_to_flash(found_devices, counter)
            manage_storage()
        else:
            print("[Scanner] No Named Devices.")

        # --- MEMORY CLEANUP (CRITICAL) ---
        # We must clear the scan results from RAM before we try to start WiFi SSL
        del found_devices 
        found_devices = None
        gc.collect()
        
        # --- RADIO SWAP (CRITICAL) ---
        # Kill Bluetooth to free up the shared radio RAM for WiFi/SSL
        if bluetooth.BLE().active():
            print("[System] Killing BLE to free RAM...")
            bluetooth.BLE().active(False)
        
        # --- PHASE 3: CATCH-UP UPLOAD ---
        # Get up to 5 files to clear backlog
        pending_files = get_oldest_files(MAX_BATCH_FILES)
        
        if pending_files:
            print(f"[System] Backlog: {len(pending_files)} files. Starting batch...")
            
            if await connect_smart_wifi():
                for filename in pending_files:
                    
                    # 1. Check RAM before EVERY upload
                    if gc.mem_free() < MIN_SAFE_RAM:
                        print(f"[System] Low RAM ({gc.mem_free()}). Stopping batch.")
                        break 

                    # 2. Upload
                    if upload_single_file(filename):
                        print(f"[Storage] Deleting {filename}")
                        try: os.remove(filename)
                        except: pass
                        fail_count = 0 
                    else:
                        fail_count += 1
                        print(f"[System] Fail Count: {fail_count}/{MAX_CONSECUTIVE_FAILS}")
                        # If a network error occurs, stop the batch
                        break 
                    
                    # 3. Clean RAM between files
                    gc.collect()
                    time.sleep(1) # Breath
                
                disconnect_wifi()
            else:
                fail_count += 1
                print(f"[WiFi] Connect Fail. Count: {fail_count}")
                disconnect_wifi()
                
        # --- PHASE 4: REBOOT CHECK ---
        if fail_count >= MAX_CONSECUTIVE_FAILS:
            print("[System] CRITICAL: Too many failures. Rebooting...")
            time.sleep(2)
            machine.reset()

        # --- SLEEP ---
        remaining_time = UPLOAD_INTERVAL_S - (SCAN_DURATION_MS / 1000) - 5
        if remaining_time > 0:
            print(f"[System] Sleeping {remaining_time}s...")
            await asyncio.sleep(remaining_time)

try:
    asyncio.run(scan_and_upload_loop())
except KeyboardInterrupt:
    print("Stopped by User")
except Exception as e:
    print(f"CRITICAL CRASH: {e}")
    machine.reset()