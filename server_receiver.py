# ---------------------------------------------------------------------------------------
# ZIGGY SERVER RECEIVER - V4.4 (Nuclear Logging & Auto-Flush)
# ---------------------------------------------------------------------------------------
import os
import logging
import sys
from datetime import datetime
from flask import Flask, request

# --- 1. NUCLEAR LOGGING DISABLE ---
# We disable the logger explicitly to stop the spam.
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
log.disabled = True

INCOMING_DIR = '/app/ziggy_logs/incoming' 

app = Flask(__name__)

if not os.path.exists(INCOMING_DIR):
    os.makedirs(INCOMING_DIR)

@app.route('/upload_log', methods=['POST'])
def upload_log():
    if request.method != 'POST':
        return 'Method Not Allowed', 405

    device_file_name = request.headers.get('X-Pico-Device')
    if not device_file_name:
        # flush=True forces the print to appear immediately
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️  Rejected upload: Missing X-Pico-Device Header", flush=True)
        return 'Missing X-Pico-Device Header', 400
    
    base_name, _ = os.path.splitext(device_file_name)
    final_file_name = f"{base_name}.csv"
    
    if "_ble_log" in final_file_name:
        device_id = final_file_name.split('_ble_log')[0]
    else:
        device_id = "Unknown_Device"

    temp_file_path = os.path.join(INCOMING_DIR, f"{final_file_name}.part")
    final_file_path = os.path.join(INCOMING_DIR, final_file_name)

    try:
        data = request.get_data()
        file_size_kb = len(data) / 1024
        
        with open(temp_file_path, 'wb') as f:
            f.write(data)

        os.rename(temp_file_path, final_file_path)

        # --- CUSTOM SUCCESS LOG (With Flush) ---
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 📥 Received {file_size_kb:.1f}KB from {device_id} ({final_file_name})", flush=True)
        return 'Log received for background processing.', 200

    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Server Error: {e}", flush=True)
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        return f'Server error: {e}', 500

if __name__ == '__main__':
    # Force stdout to flush line by line (redundant safety)
    sys.stdout.reconfigure(line_buffering=True)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🚀 Flask Receiver Started on Port 5001 (Quiet Mode v4.4)", flush=True)
    app.run(host='0.0.0.0', port=5001, debug=False)