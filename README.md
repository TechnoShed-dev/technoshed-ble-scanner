# Technoshed BLE Scanner (SIGINT Node)

**A Passive Pattern-of-Life Tracker for High Street Traffic.**

This project uses "Ziggy" nodes (Raspberry Pi Pico W) to capture Bluetooth Low Energy (BLE) digital exhaust, and a Raspberry Pi 5 backend to analyze traffic density, dwell time, and device composition.

## 📂 Project Structure

```text
├── backend_runner.sh         # Entry point for the Docker container
├── consolidator.py           # ETL Script: CSV -> SQLite
├── custom.ini                # Grafana custom settings
├── docker-compose.yml        # Orchestrates Backend + Grafana
├── firmware/                 # MicroPython code for Pico W
│   ├── config_credentials.py # WiFi Secrets (Not in Repo)
│   ├── hardware_interface.py # HAL for Mini/Tactical
│   ├── lib/                  # Hardware drivers
│   │   ├── neopixel.py
│   │   └── ssd1306.py
│   └── main.py               # Main Logic Loop
├── grafana-config.yaml       # Datasource provisioning
├── requirements.txt          # Python dependencies
├── server_receiver.py        # Flask App for receiving uploads
└── utilities/                # Maintenance & Data Repair tools
    ├── clean_master_csv.py
    ├── import_clean_master.py
    └── import_legacy.py

🚀 Quick Start

1. The Hardware (Pico W)

    Copy the contents of the firmware/ folder to your Pico W.

    Important: You must manually create firmware/config_credentials.py with your WiFi SSID/Password (see main.py for variable names).

2. The Backend (Pi 5)

Run the stack directly from this root directory:
Bash

docker-compose up -d --build

    Dashboard: Access Grafana at http://localhost:3000 (Default User: admin / Pass: admin).

    Data Storage: Logs are saved to the ziggy_logs/ directory (auto-created on first run).

🛠 Utilities & Maintenance

This project includes several helper scripts located in the utilities/ folder to help manage data integrity.

To run these inside the Docker container:

    Import Legacy Data (Time-Shift Fix):

        Fixes "Ghost Dates" (1970/2000) by shifting them to the correct 2025 start date.
    Bash

docker exec -it ziggy_unified_backend python /app/utilities/import_legacy.py

Clean & Inspect Master CSV:

    Trims trailing commas and generates a preview file for manual inspection.

Bash

    docker exec -it ziggy_unified_backend python /app/utilities/clean_master_csv.py
