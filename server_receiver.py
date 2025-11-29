# ---------------------------------------------------------------------------------------
# ZIGGY SERVER RECEIVER - V4.2 (Stable)
# ---------------------------------------------------------------------------------------
# PURPOSE: 
# Lightweight Flask app to receive raw log files from sensor nodes.
# Uses atomic write strategy (.part -> .csv) to ensure the Consolidator 
# never reads a half-written file.
# ---------------------------------------------------------------------------------------
import os
from flask import Flask, request

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
        return 'Missing X-Pico-Device Header', 400
    
    # Ensure .csv extension
    base_name, _ = os.path.splitext(device_file_name)
    final_file_name = f"{base_name}.csv"
    
    # --- ATOMIC WRITE STRATEGY ---
    # 1. Write to a temporary ".part" file (Consolidator ignores these)
    temp_file_path = os.path.join(INCOMING_DIR, f"{final_file_name}.part")
    final_file_path = os.path.join(INCOMING_DIR, final_file_name)

    try:
        data = request.get_data()
        
        with open(temp_file_path, 'wb') as f:
            f.write(data)

        # 2. Rename to .csv only when fully written (Atomic Operation)
        os.rename(temp_file_path, final_file_path)

        print(f"[{final_file_name}] received and finalized.")
        return 'Log received for background processing.', 200

    except Exception as e:
        print(f"Server Error: {e}")
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path) # Cleanup fragments
        return f'Server error: {e}', 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False)