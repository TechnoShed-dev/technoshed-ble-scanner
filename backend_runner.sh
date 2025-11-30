#!/bin/bash
# ZIGGY UNIFIED BACKEND RUNNER

# --- 1. Installation ---
echo "--- Installing Python requirements once ---"
pip install -r requirements.txt

# --- 2. Start Flask Receiver (Background) ---
echo "--- Starting Flask Receiver on port 5001 ---"
# Note: The 'receiver.py' script must be modified to run the Flask app 
# using Gunicorn or Waitress for production stability, but we'll use Flask's
# built-in server for simplicity and run it in the background (&).
python /app/server_receiver.py & 

# --- 3. Start Consolidation Loop (Foreground) ---
echo "--- Starting Consolidation Loop (every 60 seconds) ---"
while true; do 
  echo "--- Running Consolidation Script ---"
  python /app/consolidator.py
  sleep 60 # sleep for 60 seconds (1 minute)
done

# Wait command is not strictly needed here since the while loop is foreground, 
# but it's good practice for background processes.
# wait
