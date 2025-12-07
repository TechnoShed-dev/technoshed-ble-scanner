# ---------------------------------------------------------------------------------------
# SECURED FILE: config_credentials.py
# CRITICAL: This file MUST be added to .gitignore. Do not upload to GitHub.
# ---------------------------------------------------------------------------------------

# --- NETWORK CONFIGURATION (SSID/PASS) ---
# List of known Wi-Fi networks and their credentials.
KNOWN_NETWORKS = [
    {"ssid": "Home SSID", "pass": "Your Password 1"},		# Home Network
    {"ssid": "Work SSID", "pass": "Work Password"}			# Work Networkt
]

# --- FTP SERVER CONFIGURATION ---
SERVER_URL = "<<< Link to your backend >>> /upload_log" 
FTP_PORT = 443

CF_CLIENT_ID = "<<<YOUR SECRET ID>>>>"
CF_CLIENT_SECRET = "<<<<YOUR SECRET>>>>"
