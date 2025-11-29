# Technoshed BLE Scanner (SIGINT Node)

**A Passive Pattern-of-Life Tracker for High Street Traffic.**

This project uses "Ziggy" nodes (Raspberry Pi Pico W) to capture Bluetooth Low Energy (BLE) digital exhaust, and a Raspberry Pi 5 backend to analyze traffic density, dwell time, and device composition.

# TechnoShed BLE Scanner - v7.0.0 (MariaDB Edition)

## ⚠️ Major Update: Database Migration
As of version 7.0.0, this project no longer supports SQLite (`.db` files). 
It now requires a connection to a MySQL/MariaDB server.

### Why the change?
With log data exceeding 7 million rows, SQLite became a performance bottleneck for Grafana visualizations. The switch to MariaDB allows for:
- Concurrent writes from multiple sensors.
- Significant performance boost in queries (Indexed DATETIME).
- Centralized storage for multiple scanner nodes.

### New Prerequisites
- **MariaDB/MySQL Server** (Docker or Bare Metal).
- Python Library: `mysql-connector-python` (added to `requirements.txt`).

### Configuration
The `consolidator.py` script now looks for the database host. 
You can set this via Environment Variable in your Docker Compose:

```yaml
environment:
  - DB_HOST=10.0.1.2  # IP of your MariaDB Server
  - DB_USER=technoshed_user
  - DB_PASS=your_password
  
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

### 1. The Hardware (Pico W)
1. Copy the contents of the `firmware/` folder to your Pico W.
2. **Configuration:**
   * Locate `firmware/config_credentials.example.py`.
   * Rename it to `config_credentials.py`.
   * Open it and enter your Wi-Fi credentials and Cloudflare Service Tokens.
   * *Note: `config_credentials.py` is ignored by Git to protect your secrets.*

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
