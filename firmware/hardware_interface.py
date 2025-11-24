# ---------------------------------------------------------------------------------------
# HARDWARE ABSTRACTION LAYER: hardware_interface.py
# This file defines device-specific constants and functions for the CORE logic (main.py).
# ---------------------------------------------------------------------------------------
import utime
from machine import Pin, I2C # Keep imports local to this file

# --- DEVICE TYPE CONSTANTS ---
# !!! CHANGE THIS CONSTANT TO SWITCH THE DEVICE'S BEHAVIOR !!!
DEVICE_TYPE = 'TACTICAL' # Options: 'TACTICAL', 'MINI' 
# !!!-------------------------------------------------!!!

# --- PIN DEFINITIONS ---
# TACTICAL:
I2C_SDA_PIN = 20 
I2C_SCL_PIN = 21
NEOPIXEL_PIN = 28 
ACTION_BUTTON_PIN = 22 # Manual Upload Trigger

# MINI:
ONBOARD_LED_PIN = 'LED' # Standard Pico W Onboard LED

# --- NEOPIXEL CONFIG (Colors for Notifier) ---
COLOR_OFF = (0, 0, 0)
COLOR_ERROR = (255, 0, 0)      # RED
COLOR_SAVE = (0, 255, 0)       # GREEN
COLOR_UPLOAD = (255, 165, 0)   # YELLOW/ORANGE
COLOR_BLE = (0, 0, 10)         # DIM BLUE
COLOR_MAP = {
    'ERROR': COLOR_ERROR, 'SAVE': COLOR_SAVE, 'UPLOAD': COLOR_UPLOAD, 
    'BLE': COLOR_BLE, 'OFF': COLOR_OFF
}
# Global to manage OLED state
OLED_STATE = {"status_line": "SYSTEM STARTUP", "progress": "INIT", "mode": "BOOTING"}


# --- HARDWARE INITIALIZATION & PLACEHOLDERS ---
try:
    import neopixel
    # Only import SSD1306 if we are on Tactical
    if DEVICE_TYPE == 'TACTICAL':
        from ssd1306 import SSD1306_I2C 
        
        # TACTICAL: Initialize NeoPixel, OLED, and Button
        np = neopixel.NeoPixel(Pin(NEOPIXEL_PIN), 8)
        i2c = I2C(0, sda=Pin(I2C_SDA_PIN), scl=Pin(I2C_SCL_PIN), freq=400000)
        oled = SSD1306_I2C(128, 64, i2c)
        action_button = Pin(ACTION_BUTTON_PIN, Pin.IN, Pin.PULL_UP)
        led_onboard = None
    
    elif DEVICE_TYPE == 'MINI':
        # MINI: Initialize Onboard LED and set up placeholder/dummy objects
        led_onboard = Pin(ONBOARD_LED_PIN, Pin.OUT)
        np = None # No NeoPixel
        action_button = None # No button

        class DummyOLED:
            def fill(self, color): pass
            def text(self, t, x, y, c=1): pass
            def show(self): pass
        oled = DummyOLED()

except Exception as e:
    # Fallback/Dummy Initialization
    print(f"Hardware Init Failure: {e}")
    np = None
    oled = None
    action_button = None
    led_onboard = None
    

# ==============================================================================
# --- UNIFIED NOTIFIER FUNCTION (Called by main.py) ---
# ==============================================================================

def notify(code: str, message: str = ""):
    """Handles both console logging and physical device notification."""
    color = COLOR_MAP.get(code, COLOR_OFF)
    
    # 1. CONSOLE / REPL OUTPUT (Debugging Aid)
    print(f"[NOTIFIER] DEVICE:{DEVICE_TYPE} STATE:{code} MSG: {message}")
    
    # 2. PHYSICAL DEVICE OUTPUT
    if DEVICE_TYPE == 'TACTICAL':
        # TACTICAL: Use NeoPixel
        if np:
            np.fill(color)
            np.write()
    
    elif DEVICE_TYPE == 'MINI':
        # MINI: Use Onboard LED (Simple ON/OFF for critical states)
        if led_onboard:
            # For BLE, SAVE, UPLOAD (Active states), turn LED ON
            if color != COLOR_OFF:
                led_onboard.value(1) 
            else:
                led_onboard.value(0) 

# ==============================================================================
# --- TACTICAL-SPECIFIC DISPLAY FUNCTION (Moved from main.py V4.0.13) ---
# ==============================================================================

def set_tactical_display(mode, status_line, progress, total_files, time_str, usage_pct, version):
    """Updates the OLED display (Tactical only) with a Storage Bar Graph."""
    global OLED_STATE

    if DEVICE_TYPE != 'TACTICAL' or not oled:
        return

    # Map verbose modes to short display codes
    mode_map = {
        "BOOTING": f"{version}", "SCAN": "LOG", "UPLOAD": "UPLOAD", 
        "CRITICAL": "CRIT", "ERROR": "FAIL", "MANUAL": "MAN",
        "FILE": "FILE", "WIFI": "WIFI", "SYNCED": "SYNC", "WARNING": "WARN"
    }
    short_mode = mode_map.get(mode, mode[:4]).upper() 
    
    OLED_STATE['mode'] = mode
    OLED_STATE['status_line'] = status_line
    OLED_STATE['progress'] = progress

    oled.fill(0)
    
    # Line 1: Mode (Left) | Time (Right)
    oled.text(short_mode, 0, 0, 1)
    oled.text(time_str, 90, 0, 1) 

    # Line 2: Dashed Separator
    oled.text("-" * 16, 0, 12, 1) 

    # Line 3: Action Context
    oled.text(f"{status_line[:20]}", 0, 24, 1)

    # Line 4: Progress / Info
    if mode == "UPLOAD":
        oled.text(f"CHUNK {progress}/{total_files}", 0, 40, 1)
    elif mode == "CRITICAL":
        oled.text("STORAGE TRAP", 0, 40, 1)
    else: 
        oled.text(f"{progress}", 0, 40, 1)

    # --- LINE 5: STORAGE BAR GRAPH (The Upgrade) ---
    # Draw the container box (Outline)
    # x=0, y=54, width=128, height=10
    oled.rect(0, 54, 128, 10, 1)
    
    # Calculate fill width based on usage (0.0 to 1.0)
    # We cap it at 126 pixels (inside the 1px border)
    if usage_pct > 1.0: usage_pct = 1.0
    if usage_pct < 0.0: usage_pct = 0.0
    
    bar_width = int(usage_pct * 126)
    
    # Draw the filled portion (Inside the box)
    if bar_width > 0:
        oled.fill_rect(1, 55, bar_width, 8, 1)

    # Optional: If Critical (>80%), you might want to invert the colors or flash
    # But for now, a full bar is a clear enough warning!
        
    oled.show()

def check_manual_button():
    """Checks the physical button state."""
    if DEVICE_TYPE == 'TACTICAL' and action_button:
        return action_button.value() == 0
    return False