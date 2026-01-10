# ==========================================
#           CONFIGURATION
# ==========================================

# --- GLOBAL SAFETY SETTINGS ---
GLOBAL_SAFE_Z = 195.0  # Height safe to travel between modules
SAFE_CENTER_X = 110.0  # Center of the workspace X
SAFE_CENTER_Y = 110.0  # Center of the workspace Y

# Pipette Constants
STEPS_PER_UL = 0.3019
DEFAULT_TARGET_UL = 100.0
MOVEMENT_SPEED = 1000

# Limits
MIN_PIPETTE_VOL = 100.0
MAX_PIPETTE_VOL = 1000.0

# Manual Control Constants
JOG_SPEED_XY = 1500  # mm/min for X and Y
JOG_SPEED_Z = 1000  # mm/min for Z
PIP_SPEED = 1000  # mm/min for Pipette actuation

# Polling Settings
POLL_INTERVAL_MS = 1000
IDLE_TIMEOUT_BEFORE_POLL = 2.0

# --- CALIBRATION PIN CONFIGURATION ---
# --- DO NOT CHANGE (Hardcoded Defaults) ---
CALIBRATION_PIN_CONFIG_DEFAULT = {
    "PIN_X": 109.2,
    "PIN_Y": 114.3,
    "PIN_Z": 132.8
}

# Active Configuration (Will be overwritten by JSON if exists)
CALIBRATION_PIN_CONFIG = CALIBRATION_PIN_CONFIG_DEFAULT.copy()

# --- EJECT STATION CONFIGURATION ---
EJECT_STATION_CONFIG = {
    "APPROACH_X": 188.7,  # X position to enter the eject area
    "APPROACH_Y": 185.4,  # Y position to enter the eject area
    "Z_SAFE": 190.0,  # Safe Z height for travel to the station
    "Z_EJECT_START": 160.2,  # Z height to lower to before stripping tip
    "EJECT_TARGET_Y": 220.0,  # Y position to move to strip the tip
    "Z_RETRACT": 190.0  # Z height to move up to after stripping
}

# --- TIP RACK CONFIGURATION ---
TIP_RACK_CONFIG = {
    "A1_X": 177.9, "A1_Y": 144.2,
    "F4_X": 208.2, "F4_Y": 94.4,
    "Z_TRAVEL": 185.0,
    "Z_PICK": 95.0,
}

# --- 96 WELL PLATE CONFIGURATION ---
PLATE_CONFIG = {
    "A1_X": 4.4, "A1_Y": 227.0,
    "H12_X": 103.5, "H12_Y": 164.1,
    "Z_SAFE": 110.0,
    "Z_ASPIRATE": 77.30, "Z_DISPENSE": 97.30
}

# --- FALCON RACK CONFIGURATION ---
FALCON_RACK_CONFIG = {
    "15ML_A1_X": 15.6, "15ML_A1_Y": 130.1,
    "15ML_B3_X": 53.8, "15ML_B3_Y": 110.0,
    "50ML_X": 78.3, "50ML_Y": 120.5,
    "Z_SAFE": 195.0,
    "Z_ASPIRATE": 80.0, "Z_DISPENSE": 180.0
}

# --- WASH STATION CONFIGURATION ---
WASH_RACK_CONFIG = {
    "A1_X": 160.7, "A1_Y": 62.8,  # Wash A
    "B2_X": 196.1, "B2_Y": 28.4,  # Waste B
    "Z_SAFE": 195.0,
    "Z_ASPIRATE": 105, "Z_DISPENSE": 170.0
}

# --- 4 ML RACK CONFIGURATION ---
_4ML_RACK_CONFIG = {
    "A1_X": -4.0, "A1_Y": 79.7,
    "A8_X": 121.6, "A8_Y": 79.7,
    "Z_SAFE": 121.6,
    "Z_ASPIRATE": 80.0, "Z_DISPENSE": 100.0
}

# --- FILTER EPPI RACK CONFIGURATION (Row B) ---
FILTER_EPPI_RACK_CONFIG = {
    "B1_X": -4.0, "B1_Y": 63.1,
    "B8_X": 121.6, "B8_Y": 63.1,
    "Z_SAFE": 121.6,
    "Z_ASPIRATE": 93.6, "Z_DISPENSE": 110.0
}

# --- EPPI RACK CONFIGURATION (Row C) ---
EPPI_RACK_CONFIG = {
    "C1_X": -4.0, "C1_Y": 48.6,
    "C8_X": 121.6, "C8_Y": 48.6,
    "Z_SAFE": 121.6,
    "Z_ASPIRATE": 72.0, "Z_DISPENSE": 105.0
}

# --- HPLC VIAL RACK CONFIGURATION (Row D) ---
HPLC_VIAL_RACK_CONFIG = {
    "D1_X": -4.0, "D1_Y": 32.6,
    "D8_X": 121.6, "D8_Y": 32.6,
    "Z_SAFE": 121.6,
    "Z_ASPIRATE": 88.6, "Z_DISPENSE": 110.0
}

# --- HPLC VIAL INSERT RACK CONFIGURATION (Row E) ---
HPLC_VIAL_INSERT_RACK_CONFIG = {
    "E1_X": -3.9, "E1_Y": 17.0,
    "E8_X": 121.6, "E8_Y": 17.0,
    "Z_SAFE": 121.6,
    "Z_ASPIRATE": 105.5, "Z_DISPENSE": 110.0
}

# --- SCREWCAP VIAL RACK CONFIGURATION (Row F) ---
SCREWCAP_VIAL_RACK_CONFIG = {
    "F1_X": -4.0, "F1_Y": 1.5,
    "F8_X": 121.6, "F8_Y": 1.5,
    "Z_SAFE": 121.6,
    "Z_ASPIRATE": 73.9, "Z_DISPENSE": 105.0
}

# Global Setup Commands
CALIBRATION_SETUP_GCODE = [
    "M107",  # Fan off
    "M104 S0",  # Hotend off
    "M140 S0",  # Bed off
    "G90",  # Absolute positioning
    "G21",  # Millimeter units
    "M302 S0",  # Allow cold extrusion
    "M82",  # Absolute extrusion mode
    "M906 E150",  # Pipette motor heat
    "M84 E S3"  # Disable E stepper after 3s idle
]