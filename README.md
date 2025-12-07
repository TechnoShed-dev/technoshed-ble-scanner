Ziggy Transporter Tracker (BLE Fleet Monitor)

Version: 1.0.0

Status: Production Active

Overview

The Ziggy Transporter Tracker is an IoT solution designed to monitor the movement of fleet vehicles (specifically car transporters) in real-time.

It uses low-cost ESP32-C3 microcontrollers as "Micro Scanners" to listen for Bluetooth Low Energy (BLE) beacons emitted by the trucks. When a specific fleet vehicle (identified by the GAT prefix) is detected, the scanner uploads the sighting directly to a central backend for visualization in Grafana.

Key Features

Targeted Scanning: Filters specifically for devices named GAT*, ignoring random Bluetooth noise (phones, headphones, etc.).

Direct Upload: Bypasses complex local hubs; scanners upload JSON data directly to the cloud/backend via WiFi.

Resilient Connectivity: Features a "Boot & Blast" logic that buffers data if WiFi is down and bursts it when connected.

Zero-Maintenance: Designed to be headless. Just power it on.

Hardware

Device: ESP32-C3 SuperMini (or standard ESP32)

Power: 5V USB (Power bank or Wall adapter)

Software Setup

Prerequisites

MicroPython firmware flashed to the ESP32.

aioble library installed on the device (via mip or manual copy).

Installation

Clone the Repo:

git clone [https://github.com/TechnoShed-dev/ble-scanner.git](https://github.com/TechnoShed-dev/ble-scanner.git)


Configure Credentials:
Rename secrets_template.py to secrets.py and add your WiFi and Backend details:

SERVER_URL = "[https://your-backend-api.com/submit](https://your-backend-api.com/submit)"
KNOWN_NETWORKS = [
    {'ssid': 'Your_WiFi', 'pass': 'Your_Password'}
]


Flash the Code:
Upload main.py, boot.py, and secrets.py to the ESP32.

Deploy:
Plug it in near the gate or parking area.

Data Flow

Scan: Device listens for 10 seconds.

Filter: Checks if device name contains "GAT".

Buffer: Stores valid hits in RAM.

Upload: Connects to WiFi and POSTs JSON to SERVER_URL.

Sleep: Disconnects WiFi to save power and clear radio interference.

License

Internal Tool / MIT License (Edit as needed)