# ---------------------------------------------------------------------------------------
# ZIGGY SERVER RECEIVER (V4.0 - Universal Client Support)
# Purpose: Receives logs from ANY Ziggy device (Tactical or Mini) and saves them 
#          to the incoming folder, ensuring the filename always has a .csv extension.
# Fixes: Handles missing .csv extension from legacy clients (Tactical).
# ---------------------------------------------------------------------------------------
import os
from flask import Flask, request

# --- CONFIGURATION ---
# The consolidator script looks in this directory.
INCOMING_DIR = '/app/ziggy_logs/incoming' 
# ---------------------

app = Flask(__name__)

# Ensure the INCOMING directory exists inside the mounted volume
if not os.path.exists(INCOMING_DIR):
    os.makedirs(INCOMING_DIR)
    print(f"Created incoming log directory: {INCOMING_DIR}")


@app.route('/upload_log', methods=['POST'])
def upload_log():
    # 1. IMMEDIATE VALIDATION
    if request.method != 'POST':
        return 'Method Not Allowed', 405

    # The Mini/Tactical sends the target filename via this header
    device_file_name = request.headers.get('X-Pico-Device')
    
    if not device_file_name:
        return 'Missing X-Pico-Device Header', 400
    
    # --- CRITICAL FIX: ENSURE .csv EXTENSION ---
    # The header is typically "DEVICE_NAME_log_001.csv"
    # We strip any existing extension and re-add .csv to ensure uniformity.
    base_name, _ = os.path.splitext(device_file_name)
    final_file_name = f"{base_name}.csv"
    
    # 2. QUICK RECEIVE & DUMP
    temp_file_path = os.path.join(INCOMING_DIR, final_file_name)

    try:
        data = request.get_data()
        
        # This synchronous file write is fast enough (40KB max)
        with open(temp_file_path, 'wb') as f:
            f.write(data)

        # 3. RETURN 200 OK IMMEDIATELY
        print(f"[{os.path.basename(temp_file_path)}] received successfully. Responding 200 OK.")
        return 'Log received for background processing.', 200

    except Exception as e:
        print(f"Server Error during file save: {e}")
        return f'Server error: {e}', 500

if __name__ == '__main__':
    # Listen on 0.0.0.0 (for Docker host mode) and Port 5001 (Ziggy's target port)
    print("Starting Flask server on 0.0.0.0:5001...")
    app.run(host='0.0.0.0', port=5001, debug=False)