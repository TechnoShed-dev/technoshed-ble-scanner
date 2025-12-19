# ---------------------------------------------------------------------------------------
# ZIGGY MICRO APPLICATION - V3.0.0 (Fleet Edition)
# ---------------------------------------------------------------------------------------
# DEVICE:  ESP32-C3 (Ziggy Micro)
# CHANGE:  V3.0.0 - Full Config Migration & Remote Management
#          - All variables moved to config dictionary
#          - Buffer & Burst logic fixed (checks threshold before connect)
#          - Remote JSON fetch added for Over-The-Air setting updates
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
import ujson

# --- DEFAULT CONFIGURATION (Failsafe) ---
# These are used if config.json is missing or unreadable
config = {
    "DEVICE_NAME": "UNNAMED_DEVICE",
    "SCAN_DURATION_MS": 30000,
    "UPLOAD_INTERVAL_S": 150,
    "MAX_BATCH_FILES": 5,
    "MIN_SAFE_RAM": 30000,
    "MAX_STORED_FILES": 50,
    "MAX_CONSECUTIVE_FAILS": 5
}

# --- LED SETUP ---
try:
    led = machine.Pin("LED", machine.Pin.OUT)
except:
    led = machine.Pin(8, machine.Pin.OUT) 

# --- CONFIG MANAGER ---
def load_local_config():
    global config
    print("[Config] Loading local settings...")
    try:
        with open('config.json', 'r') as f:
            local_data = ujson.load(f)
            # Update our config dictionary with whatever was in the file
            for key, value in local_data.items():
                if key in config:
                    config[key] = value
                    print(f"   - {key}: {value}")
    except OSError:
        print("[Config] No config.json found. Using defaults.")
    except Exception as e:
        print(f"[Config] Error reading file: {e}. Using defaults.")

def check_remote_config():
    global config
    
    # NEW URL STRUCTURE
    # e.g. https://qr.technoshed.co.uk/BLE/GAT-YARD-02
    target_url = f"https://qr.technoshed.co.uk/BLE/{config['DEVICE_NAME']}"
    print(f"[Config] Checking remote: {target_url}")
    
    try:
        # 5-second timeout to prevent hanging
        res = requests.get(target_url, timeout=5)
        
        if res.status_code == 200:
            try:
                new_settings = res.json()
            except:
                print("[Config] Error: Response is not valid JSON")
                res.close()
                return False

            res.close()
            
            changes_made = False
            
            # Compare and update only if different
            for key, value in new_settings.items():
                if key in config and config[key] != value:
                    print(f"[Config] Change detected! {key}: {config[key]} -> {value}")
                    config[key] = value
                    changes_made = True
            
            if changes_made:
                print("[Config] Saving new settings to flash...")
                with open('config.json', 'w') as f:
                    ujson.dump(config, f)
                return True 
            else:
                print("[Config] Remote matches local. No changes.")
        else:
            print(f"[Config] Server returned {res.status_code}")
            res.close()
            
    except Exception as e:
        print(f"[Config] Update failed: {e}")
    
    return False

# Load config immediately on import
load_local_config()

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
    filename = f"{config['DEVICE_NAME']}_ble_log_{counter}.csv"
    
    try:
        with open(filename, 'w') as f:
            f.write("timestamp,addr,id,rssi,channel,security,device\n")
            for result in scan_results:
                formatted_addr = format_mac_address(result['addr'])
                dev_id = result['id'].replace(",", " ") 
                rssi = result['rssi']
                security = result['security']
                
                line = "{},{},{},{},{},{},{}\n".format(
                    timestamp, formatted_addr, dev_id, rssi, "BLE", security, config['DEVICE_NAME']
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
        if len(files) > config['MAX_STORED_FILES']:
            files.sort()
            excess = len(files) - config['MAX_STORED_FILES']
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
    
    # STRICT SAFETY CHECK using Config
    if free_ram < config['MIN_SAFE_RAM']:
        print(f"[Upload] Low RAM ({free_ram}). Aborting upload.")
        return False

    headers = {
        'Content-Type': 'text/csv',
        'X-Pico-Device': filename,
        'CF-Access-Client-Id': secrets.CF_CLIENT_ID,
        'CF-Access-Client-Secret': secrets.CF_CLIENT_SECRET,
        'User-Agent': 'Ziggy-Micro/3.0'
    }

    try:
        led.on()
        print(f"[Upload] Sending {filename} ({len(csv_payload)}b)...")
        gc.collect() 
        
        response = requests.post(secrets.SERVER_URL, headers=headers, data=csv_payload, timeout=20)
        led.off()
        
        status = response.status_code
        response.close() 
        
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
    print(f"[ZiggyMicro] Starting V3.0 ({config['DEVICE_NAME']})...")
    
    # --- PHASE 0: BOOT BACKLOG CLEAR ---
    print("[System] Checking backlog on boot...")
    # Using config value for batch size
    if get_oldest_files(config['MAX_BATCH_FILES']):
        print("[System] Backlog found. Using boot connection to upload...")
        
        if await connect_smart_wifi():
            # Check for remote config update on boot while we have connection!
            check_remote_config()
            
            pending_files = get_oldest_files(config['MAX_BATCH_FILES'])
            for filename in pending_files:
                if gc.mem_free() < config['MIN_SAFE_RAM']:
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

    disconnect_wifi()
    
    fail_count = 0
    
    while True:
        gc.collect()
        
        # --- PHASE 1: SCAN ---
        found_devices = []
        scan_dur = config['SCAN_DURATION_MS']
        print(f"\n[Scanner] BLE Scanning for {int(scan_dur/1000)} s...")
        
        try:
            async with aioble.scan(duration_ms=scan_dur, interval_us=30000, window_us=30000, active=True) as scanner:
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
                        else: dev_id = "GENERIC"
                    
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

        # --- MEMORY CLEANUP ---
        del found_devices 
        found_devices = None
        gc.collect()
        
        # --- RADIO SWAP ---
        if bluetooth.BLE().active():
            print("[System] Killing BLE to free RAM...")
            bluetooth.BLE().active(False)
        
        # --- PHASE 3: CATCH-UP UPLOAD (BUFFER & BURST) ---
        # 1. Count Total Files
        try:
            all_logs = [f for f in os.listdir() if f.endswith('.csv') and '_ble_log_' in f]
            total_count = len(all_logs)
        except:
            total_count = 0
            
        print(f"[System] Buffer Status: {total_count}/{config['MAX_BATCH_FILES']} files.")

        # 2. DECISION: Only upload if buffer is FULL or RAM is DANGEROUS
        should_upload = (total_count >= config['MAX_BATCH_FILES']) or (gc.mem_free() < config['MIN_SAFE_RAM'])
        
        if should_upload:
            print(f"[System] Threshold met. Starting Batch Upload...")
            
            pending_files = get_oldest_files(config['MAX_BATCH_FILES'])
            
            if await connect_smart_wifi():
                
                # --- NEW: CHECK REMOTE CONFIG ---
                # Check for updates while we are online!
                check_remote_config()
                
                for filename in pending_files:
                    
                    # Check RAM before EVERY upload
                    if gc.mem_free() < config['MIN_SAFE_RAM']:
                        print(f"[System] Low RAM ({gc.mem_free()}). Stopping batch.")
                        break 

                    # Upload
                    if upload_single_file(filename):
                        print(f"[Storage] Deleting {filename}")
                        try: os.remove(filename)
                        except: pass
                        fail_count = 0 
                    else:
                        fail_count += 1
                        print(f"[System] Fail Count: {fail_count}/{config['MAX_CONSECUTIVE_FAILS']}")
                        break 
                    
                    gc.collect()
                    time.sleep(1) 
                
                disconnect_wifi()
            else:
                fail_count += 1
                print(f"[WiFi] Connect Fail. Count: {fail_count}")
                disconnect_wifi()
        else:
             print("[System] Buffer not full. Skipping WiFi to save power.")
                
        # --- PHASE 4: REBOOT CHECK ---
        if fail_count >= config['MAX_CONSECUTIVE_FAILS']:
            print("[System] CRITICAL: Too many failures. Rebooting...")
            time.sleep(2)
            machine.reset()

        # --- SLEEP ---
        # Calculate sleep based on config values
        remaining_time = config['UPLOAD_INTERVAL_S'] - (config['SCAN_DURATION_MS'] / 1000) - 5
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