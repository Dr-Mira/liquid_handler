import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import time
import queue
import serial
import serial.tools.list_ports
import re
import random
import math
import json
import os
import glob
from datetime import datetime

# ==========================================
#           CONFIGURATION
# ==========================================

# --- DEFAULT CONFIGURATIONS ---
# These are fallback values in case the config.json file is missing or corrupted

# --- CALIBRATION PIN ANCHOR (The Reference Point) ---
CALIBRATION_PIN_CONFIG_DEFAULT = {
    "PIN_X": 109.7,
    "PIN_Y": 112.0,
    "PIN_Z": 133.7
}

# Center Default Configuration
CENTER_CONFIG_DEFAULT = {
    "GLOBAL_SAFE_Z_OFFSET": 37.2,
    "SAFE_CENTER_X_OFFSET": 0.8,
    "SAFE_CENTER_Y_OFFSET": -4.3
}

# Parking Default Configuration (Relative Coordinates)
PARKING_CONFIG_DEFAULT = {
    "PARK_HEAD_X": 79.5,
    "PARK_HEAD_Y": 71.1,
    "PARK_HEAD_Z": 2.4
}

# Pipette Constants Default Configuration
PIPETTE_CONFIG_DEFAULT = {
    "STEPS_PER_UL": 0.3019,
    "DEFAULT_TARGET_UL": 200.0,
    "MOVEMENT_SPEED": 1000,
    "AIR_GAP_UL": 200,
    "MIN_PIPETTE_VOL": 100.0,
    "MAX_PIPETTE_VOL": 1000.0
}

# Volatile Logic Default Configuration
VOLATILE_CONFIG_DEFAULT = {
    "VOLATILE_DRIFT_RATE": 2000,
    "VOLATILE_MOVE_SPEED": 1500
}

# Manual Control Constants Default Configuration
MANUAL_CONTROL_CONFIG_DEFAULT = {
    "JOG_SPEED_XY": 8000,
    "JOG_SPEED_Z": 8000,
    "PIP_SPEED": 2000
}

# Communication Polling Settings Default Configuration
COMMUNICATION_CONFIG_DEFAULT = {
    "POLL_INTERVAL_MS": 1000,
    "IDLE_TIMEOUT_BEFORE_POLL": 2.0
}

# --- ACTIVE CONFIGURATIONS (Will be overwritten by JSON if exists) ---
CALIBRATION_PIN_CONFIG = CALIBRATION_PIN_CONFIG_DEFAULT.copy()
CENTER_CONFIG = CENTER_CONFIG_DEFAULT.copy()
PARKING_CONFIG = PARKING_CONFIG_DEFAULT.copy()
PIPETTE_CONFIG = PIPETTE_CONFIG_DEFAULT.copy()
VOLATILE_CONFIG = VOLATILE_CONFIG_DEFAULT.copy()
MANUAL_CONTROL_CONFIG = MANUAL_CONTROL_CONFIG_DEFAULT.copy()
COMMUNICATION_CONFIG = COMMUNICATION_CONFIG_DEFAULT.copy()

# --- CONVENIENCE VARIABLES (For backward compatibility) ---
# Center
GLOBAL_SAFE_Z_OFFSET = CENTER_CONFIG["GLOBAL_SAFE_Z_OFFSET"]
SAFE_CENTER_X_OFFSET = CENTER_CONFIG["SAFE_CENTER_X_OFFSET"]
SAFE_CENTER_Y_OFFSET = CENTER_CONFIG["SAFE_CENTER_Y_OFFSET"]

# Parking
PARK_HEAD_X = PARKING_CONFIG["PARK_HEAD_X"]
PARK_HEAD_Y = PARKING_CONFIG["PARK_HEAD_Y"]
PARK_HEAD_Z = PARKING_CONFIG["PARK_HEAD_Z"]

# Pipette Constants
STEPS_PER_UL = PIPETTE_CONFIG["STEPS_PER_UL"]
DEFAULT_TARGET_UL = PIPETTE_CONFIG["DEFAULT_TARGET_UL"]
MOVEMENT_SPEED = PIPETTE_CONFIG["MOVEMENT_SPEED"]
AIR_GAP_UL = PIPETTE_CONFIG["AIR_GAP_UL"]
MIN_PIPETTE_VOL = PIPETTE_CONFIG["MIN_PIPETTE_VOL"]
MAX_PIPETTE_VOL = PIPETTE_CONFIG["MAX_PIPETTE_VOL"]

# Volatile Logic
VOLATILE_DRIFT_RATE = VOLATILE_CONFIG["VOLATILE_DRIFT_RATE"]
VOLATILE_MOVE_SPEED = VOLATILE_CONFIG["VOLATILE_MOVE_SPEED"]

# Manual Control Constants
JOG_SPEED_XY = MANUAL_CONTROL_CONFIG["JOG_SPEED_XY"]
JOG_SPEED_Z = MANUAL_CONTROL_CONFIG["JOG_SPEED_Z"]
PIP_SPEED = MANUAL_CONTROL_CONFIG["PIP_SPEED"]

# Communication Polling Settings
POLL_INTERVAL_MS = COMMUNICATION_CONFIG["POLL_INTERVAL_MS"]
IDLE_TIMEOUT_BEFORE_POLL = COMMUNICATION_CONFIG["IDLE_TIMEOUT_BEFORE_POLL"]

# --- EJECT STATION CONFIGURATION DEFAULT (Relative Offsets) ---
EJECT_STATION_CONFIG_DEFAULT = {
    "APPROACH_X": 78.5,
    "APPROACH_Y": 71.1,
    "Z_SAFE": 32.2,
    "Z_EJECT_START": 2.0,
    "EJECT_TARGET_Y": 106.7,
    "Z_RETRACT": 32.2
}

# --- TIP RACK CONFIGURATION DEFAULT (Relative Offsets) ---
TIP_RACK_CONFIG_DEFAULT = {
    "A1_X": 69.4, "A1_Y": 31.0,
    "F4_X": 99.8, "F4_Y": -18.9,
    "Z_TRAVEL": 27.2,
    "Z_PICK": -62.8
}

# --- 96 WELL PLATE CONFIGURATION DEFAULT (Relative Offsets) ---
PLATE_CONFIG_DEFAULT = {
    "A1_X": -104.6, "A1_Y": 112.1,
    "H12_X": -5.4, "H12_Y": 49.3,
    "Z_SAFE": 2.2,
    "Z_ASPIRATE": -30.5,
    "Z_DISPENSE": -10.5
}

# --- FALCON RACK CONFIGURATION DEFAULT (Relative Offsets) ---
FALCON_RACK_CONFIG_DEFAULT = {
    "15ML_A1_X": -94.9, "15ML_A1_Y": 15.8,
    "15ML_B3_X": -57.0, "15ML_B3_Y": -4.4,
    "50ML_X": -31.3, "50ML_Y": 5.5,
    "Z_SAFE": 37.2,
    "Z_ASPIRATE": -85.0,
    "Z_DISPENSE": 22.2
}

# --- WASH STATION CONFIGURATION DEFAULT (Relative Offsets) ---
WASH_RACK_CONFIG_DEFAULT = {
    "A1_X": 51.5, "A1_Y": -51.5,
    "B2_X": 88.3, "B2_Y": -88.0,
    "Z_SAFE": 37.2,
    "Z_ASPIRATE": -52.8,
    "Z_DISPENSE": 12.2
}

# --- 4 ML RACK CONFIGURATION DEFAULT (Relative Offsets) ---
_4ML_RACK_CONFIG_DEFAULT = {
    "A1_X": -114.5, "A1_Y": -34.6,
    "A8_X": 12.4, "A8_Y": -34.6,
    "Z_SAFE": 13.8,
    "Z_ASPIRATE": -31.5,
    "Z_DISPENSE": -1.0
}

# --- FILTER EPPI RACK CONFIGURATION DEFAULT (Row B) (Relative Offsets) ---
FILTER_EPPI_RACK_CONFIG_DEFAULT = {
    "B1_X": -114.1, "B1_Y": -51.2,
    "B8_X": 12.4, "B8_Y": -51.2,
    "Z_SAFE": 13.8,
    "Z_ASPIRATE": -14.2,
    "Z_DISPENSE": 2.2
}

# --- ACTIVE CONFIGURATIONS (Will be overwritten by JSON if exists) ---
EJECT_STATION_CONFIG = EJECT_STATION_CONFIG_DEFAULT.copy()
TIP_RACK_CONFIG = TIP_RACK_CONFIG_DEFAULT.copy()
PLATE_CONFIG = PLATE_CONFIG_DEFAULT.copy()
FALCON_RACK_CONFIG = FALCON_RACK_CONFIG_DEFAULT.copy()
WASH_RACK_CONFIG = WASH_RACK_CONFIG_DEFAULT.copy()
_4ML_RACK_CONFIG = _4ML_RACK_CONFIG_DEFAULT.copy()
FILTER_EPPI_RACK_CONFIG = FILTER_EPPI_RACK_CONFIG_DEFAULT.copy()

# --- EPPI RACK CONFIGURATION DEFAULT (Row C) (Relative Offsets) ---
EPPI_RACK_CONFIG_DEFAULT = {
    "C1_X": -113.6, "C1_Y": -66.2,
    "C8_X": 12.2, "C8_Y": -67.2,
    "Z_SAFE": 13.8,
    "Z_ASPIRATE": -35.2,
    "Z_DISPENSE": -2.8
}

# --- HPLC VIAL RACK CONFIGURATION DEFAULT (Row D) (Relative Offsets) ---
HPLC_VIAL_RACK_CONFIG_DEFAULT = {
    "D1_X": -113.2, "D1_Y": -81.7,
    "D8_X": 12.4, "D8_Y": -81.7,
    "Z_SAFE": 13.8,
    "Z_ASPIRATE": -19.2,
    "Z_DISPENSE": 2.2
}

# --- HPLC VIAL INSERT RACK CONFIGURATION DEFAULT (Row E) (Relative Offsets) ---
HPLC_VIAL_INSERT_RACK_CONFIG_DEFAULT = {
    "E1_X": -113.6, "E1_Y": -97.9,
    "E8_X": 12.9, "E8_Y": -98.7,
    "Z_SAFE": 13.8,
    "Z_ASPIRATE": -2.3,
    "Z_DISPENSE": 2.2
}

# --- SCREWCAP VIAL RACK CONFIGURATION DEFAULT (Row F) (Relative Offsets) ---
SCREWCAP_VIAL_RACK_CONFIG_DEFAULT = {
    "F1_X": -113.6, "F1_Y": -113.7,
    "F8_X": 12.9, "F8_Y": -114.2,
    "Z_SAFE": 13.8,
    "Z_ASPIRATE": -31.9,
    "Z_DISPENSE": -2.8
}

# --- ACTIVE CONFIGURATIONS (Will be overwritten by JSON if exists) ---
EPPI_RACK_CONFIG = EPPI_RACK_CONFIG_DEFAULT.copy()
HPLC_VIAL_RACK_CONFIG = HPLC_VIAL_RACK_CONFIG_DEFAULT.copy()
HPLC_VIAL_INSERT_RACK_CONFIG = HPLC_VIAL_INSERT_RACK_CONFIG_DEFAULT.copy()
SCREWCAP_VIAL_RACK_CONFIG = SCREWCAP_VIAL_RACK_CONFIG_DEFAULT.copy()

# --- MODULE GROUPS FOR OPTIMIZATION ---
SMALL_VIAL_MODULES = ["4ML", "FILTER_EPPI", "EPPI", "HPLC", "HPLC_INSERT", "SCREWCAP"]

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


class SequenceAbortedError(Exception):
    """Custom exception to break out of sequence threads immediately."""
    pass


# ==========================================
#           MAIN APPLICATION
# ==========================================

class LiquidHandlerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Mira Liquid Handler")
        self.root.geometry("1024x600")
        self.root.resizable(False, False)

        # --- LOGGING SETUP ---
        self.log_dir = os.path.join(os.path.dirname(__file__), ".log")
        if not os.path.exists(self.log_dir):
            try:
                os.makedirs(self.log_dir)
            except Exception as e:
                print(f"Error creating log directory: {e}")

        # Serial Objects
        self.ser = None
        self.reader_thread = None
        self.stop_event = threading.Event()
        self.rx_queue = queue.Queue()
        self.ok_event = threading.Event()

        # Serial Lock & Idle Timer
        self.serial_lock = threading.Lock()
        self.is_sequence_running = False
        self.last_action_time = time.time()

        # --- PAUSE / ABORT CONTROL ---
        self.is_paused = False
        self.is_aborted = False

        # --- STATE TRACKING ---
        self.last_known_module = "Unknown"
        self.current_x = 0.0
        self.current_y = 0.0
        self.current_z = 0.0

        # UI Variables
        self.port_var = tk.StringVar(value="/dev/ttyUSB0")
        self.baud_var = tk.StringVar(value="115200")
        self.status_var = tk.StringVar(value="Disconnected")

        # Bottom Bar Variables
        self.coord_x_var = tk.StringVar(value="0.00")
        self.coord_y_var = tk.StringVar(value="0.00")
        self.coord_z_var = tk.StringVar(value="0.00")
        self.live_vol_var = tk.StringVar(value=f"{DEFAULT_TARGET_UL:.1f}")
        self.module_hover_var = tk.StringVar(value="None")

        # STATUS VARIABLE
        self.last_cmd_var = tk.StringVar(value="Idle")

        # Calibration Variables
        self.current_vol_var = tk.StringVar()
        self.target_vol_var = tk.StringVar(value=str(int(DEFAULT_TARGET_UL)))

        # Manual Control Variables
        self.step_size_var = tk.DoubleVar(value=10.0)
        self.pipette_move_var = tk.StringVar(value="100")
        self.raw_gcode_var = tk.StringVar()

        self.current_pipette_volume = DEFAULT_TARGET_UL
        self.vol_display_var = tk.StringVar(value=f"{self.current_pipette_volume:.1f} uL")

        # --- LOAD CONFIGURATION ---
        self.config_file = os.path.join(os.path.dirname(__file__), "config.json")
        self.load_calibration_config()

        # --- MODULE INVENTORY INITIALIZATION ---
        self.tip_rows = ["A", "B", "C", "D", "E", "F"]
        self.tip_cols = ["1", "2", "3", "4"]
        self.tip_inventory = {f"{r}{c}": True for r in self.tip_rows for c in self.tip_cols}
        self.tip_buttons = {}

        self.plate_rows = ["A", "B", "C", "D", "E", "F", "G", "H"]
        self.plate_cols = [str(i) for i in range(1, 13)]
        self.plate_wells = [f"{r}{c}" for r in self.plate_rows for c in self.plate_cols]

        self.falcon_positions = ["A1", "A2", "A3", "B1", "B2", "B3", "50mL"]
        self.wash_positions = ["Wash A", "Wash B", "Wash C", "Trash"]
        self._4ml_positions = [f"A{i}" for i in range(1, 9)]
        self.filter_eppi_positions = [f"B{i}" for i in range(1, 9)]
        self.eppi_positions = [f"C{i}" for i in range(1, 9)]
        self.hplc_positions = [f"D{i}" for i in range(1, 9)]
        self.hplc_insert_positions = [f"E{i}" for i in range(1, 9)]
        self.screwcap_positions = [f"F{i}" for i in range(1, 9)]

        # --- MODULE MAPPING FOR DYNAMIC DROPDOWNS ---
        self.module_options_map = {
            "96 Well Plate": self.plate_wells,
            "Falcon Rack": self.falcon_positions,
            "4mL Rack": self._4ml_positions,
            "Filter Eppi": self.filter_eppi_positions,
            "Eppi Rack": self.eppi_positions,
            "HPLC Vial": self.hplc_positions,
            "HPLC Insert": self.hplc_insert_positions,
            "Screwcap Vial": self.screwcap_positions,
            "Wash Station": self.wash_positions
        }

        # --- MODULE DEFINITION DICTIONARY ---
        self.modules = {
            "TIPS": {
                "label": "Tips", "var": tk.StringVar(), "values": [],
                "btn_text": "PICK", "cmd": self.pick_tip_sequence
            },
            "PLATE": {
                "label": "96 Well Plate", "var": tk.StringVar(), "values": self.plate_wells,
                "btn_text": "GO", "cmd": lambda: self.generic_move_sequence("PLATE", self.modules["PLATE"]["var"].get())
            },
            "FALCON": {
                "label": "Falcon Rack", "var": tk.StringVar(), "values": self.falcon_positions,
                "btn_text": "GO",
                "cmd": lambda: self.generic_move_sequence("FALCON", self.modules["FALCON"]["var"].get())
            },
            "WASH": {
                "label": "Wash Station", "var": tk.StringVar(), "values": self.wash_positions,
                "btn_text": "GO", "cmd": lambda: self.generic_move_sequence("WASH", self.modules["WASH"]["var"].get())
            },
            "4ML": {
                "label": "4mL Rack", "var": tk.StringVar(), "values": self._4ml_positions,
                "btn_text": "GO", "cmd": lambda: self.generic_move_sequence("4ML", self.modules["4ML"]["var"].get())
            },
            "FILTER_EPPI": {
                "label": "Filter Eppi", "var": tk.StringVar(), "values": self.filter_eppi_positions,
                "btn_text": "GO",
                "cmd": lambda: self.generic_move_sequence("FILTER_EPPI", self.modules["FILTER_EPPI"]["var"].get())
            },
            "EPPI": {
                "label": "Eppi Rack", "var": tk.StringVar(), "values": self.eppi_positions,
                "btn_text": "GO", "cmd": lambda: self.generic_move_sequence("EPPI", self.modules["EPPI"]["var"].get())
            },
            "HPLC": {
                "label": "HPLC Vial", "var": tk.StringVar(), "values": self.hplc_positions,
                "btn_text": "GO", "cmd": lambda: self.generic_move_sequence("HPLC", self.modules["HPLC"]["var"].get())
            },
            "HPLC_INSERT": {
                "label": "HPLC Insert", "var": tk.StringVar(), "values": self.hplc_insert_positions,
                "btn_text": "GO",
                "cmd": lambda: self.generic_move_sequence("HPLC_INSERT", self.modules["HPLC_INSERT"]["var"].get())
            },
            "SCREWCAP": {
                "label": "Screwcap Vial", "var": tk.StringVar(), "values": self.screwcap_positions,
                "btn_text": "GO",
                "cmd": lambda: self.generic_move_sequence("SCREWCAP", self.modules["SCREWCAP"]["var"].get())
            },
        }

        # Set defaults for dropdowns
        for key in self.modules:
            if self.modules[key]["values"]:
                self.modules[key]["var"].set(self.modules[key]["values"][0])

        # --- TEST 96 MIXING VARIABLES ---
        self.vial_a_var = tk.StringVar(value="A1")
        self.vial_b_var = tk.StringVar(value="A2")
        self.diluent_var = tk.StringVar(value="50mL")

        # --- CALIBRATION VARIABLES ---
        self.calibration_module_var = tk.StringVar(value="96 well plate")

        self._build_ui()
        self._poll_rx_queue()
        self.refresh_ports()
        self._poll_position_loop()
        self.root.after(500, self.attempt_auto_connect)

        # --- START POSITION LOGGING THREAD ---
        self.pos_log_thread = threading.Thread(target=self._position_logger_loop, daemon=True)
        self.pos_log_thread.start()

    def load_calibration_config(self):
        global CALIBRATION_PIN_CONFIG, CENTER_CONFIG, PARKING_CONFIG, PIPETTE_CONFIG, VOLATILE_CONFIG
        global MANUAL_CONTROL_CONFIG, COMMUNICATION_CONFIG, EJECT_STATION_CONFIG, TIP_RACK_CONFIG
        global PLATE_CONFIG, FALCON_RACK_CONFIG, WASH_RACK_CONFIG, _4ML_RACK_CONFIG, FILTER_EPPI_RACK_CONFIG
        global EPPI_RACK_CONFIG, HPLC_VIAL_RACK_CONFIG, HPLC_VIAL_INSERT_RACK_CONFIG, SCREWCAP_VIAL_RACK_CONFIG
        global GLOBAL_SAFE_Z_OFFSET, SAFE_CENTER_X_OFFSET, SAFE_CENTER_Y_OFFSET
        global PARK_HEAD_X, PARK_HEAD_Y, PARK_HEAD_Z
        global STEPS_PER_UL, DEFAULT_TARGET_UL, MOVEMENT_SPEED, AIR_GAP_UL, MIN_PIPETTE_VOL, MAX_PIPETTE_VOL
        global VOLATILE_DRIFT_RATE, VOLATILE_MOVE_SPEED
        global JOG_SPEED_XY, JOG_SPEED_Z, PIP_SPEED
        global POLL_INTERVAL_MS, IDLE_TIMEOUT_BEFORE_POLL

        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, "r") as f:
                    config = json.load(f)

                    # Load calibration pin config
                    if all(k in config for k in ["PIN_X", "PIN_Y", "PIN_Z"]):
                        CALIBRATION_PIN_CONFIG.update({
                            "PIN_X": config["PIN_X"],
                            "PIN_Y": config["PIN_Y"],
                            "PIN_Z": config["PIN_Z"]
                        })

                    # Load center config
                    if "CENTER" in config:
                        CENTER_CONFIG.update(config["CENTER"])
                        # Update convenience variables
                        GLOBAL_SAFE_Z_OFFSET = CENTER_CONFIG["GLOBAL_SAFE_Z_OFFSET"]
                        SAFE_CENTER_X_OFFSET = CENTER_CONFIG["SAFE_CENTER_X_OFFSET"]
                        SAFE_CENTER_Y_OFFSET = CENTER_CONFIG["SAFE_CENTER_Y_OFFSET"]

                    # Load parking config
                    if "PARKING" in config:
                        PARKING_CONFIG.update(config["PARKING"])
                        # Update convenience variables
                        PARK_HEAD_X = PARKING_CONFIG["PARK_HEAD_X"]
                        PARK_HEAD_Y = PARKING_CONFIG["PARK_HEAD_Y"]
                        PARK_HEAD_Z = PARKING_CONFIG["PARK_HEAD_Z"]

                    # Load pipette config
                    if "PIPETTE" in config:
                        PIPETTE_CONFIG.update(config["PIPETTE"])
                        # Update convenience variables
                        STEPS_PER_UL = PIPETTE_CONFIG["STEPS_PER_UL"]
                        DEFAULT_TARGET_UL = PIPETTE_CONFIG["DEFAULT_TARGET_UL"]
                        MOVEMENT_SPEED = PIPETTE_CONFIG["MOVEMENT_SPEED"]
                        AIR_GAP_UL = PIPETTE_CONFIG["AIR_GAP_UL"]
                        MIN_PIPETTE_VOL = PIPETTE_CONFIG["MIN_PIPETTE_VOL"]
                        MAX_PIPETTE_VOL = PIPETTE_CONFIG["MAX_PIPETTE_VOL"]

                    # Load volatile config
                    if "VOLATILE" in config:
                        VOLATILE_CONFIG.update(config["VOLATILE"])
                        # Update convenience variables
                        VOLATILE_DRIFT_RATE = VOLATILE_CONFIG["VOLATILE_DRIFT_RATE"]
                        VOLATILE_MOVE_SPEED = VOLATILE_CONFIG["VOLATILE_MOVE_SPEED"]

                    # Load manual control config
                    if "MANUAL_CONTROL" in config:
                        MANUAL_CONTROL_CONFIG.update(config["MANUAL_CONTROL"])
                        # Update convenience variables
                        JOG_SPEED_XY = MANUAL_CONTROL_CONFIG["JOG_SPEED_XY"]
                        JOG_SPEED_Z = MANUAL_CONTROL_CONFIG["JOG_SPEED_Z"]
                        PIP_SPEED = MANUAL_CONTROL_CONFIG["PIP_SPEED"]

                    # Load communication config
                    if "COMMUNICATION" in config:
                        COMMUNICATION_CONFIG.update(config["COMMUNICATION"])
                        # Update convenience variables
                        POLL_INTERVAL_MS = COMMUNICATION_CONFIG["POLL_INTERVAL_MS"]
                        IDLE_TIMEOUT_BEFORE_POLL = COMMUNICATION_CONFIG["IDLE_TIMEOUT_BEFORE_POLL"]

                    # Load rack configurations
                    if "EJECT_STATION_CONFIG" in config:
                        EJECT_STATION_CONFIG.update(config["EJECT_STATION_CONFIG"])

                    if "TIP_RACK_CONFIG" in config:
                        TIP_RACK_CONFIG.update(config["TIP_RACK_CONFIG"])

                    if "PLATE_CONFIG" in config:
                        PLATE_CONFIG.update(config["PLATE_CONFIG"])

                    if "FALCON_RACK_CONFIG" in config:
                        FALCON_RACK_CONFIG.update(config["FALCON_RACK_CONFIG"])

                    if "WASH_RACK_CONFIG" in config:
                        WASH_RACK_CONFIG.update(config["WASH_RACK_CONFIG"])

                    if "4ML_RACK_CONFIG" in config:
                        _4ML_RACK_CONFIG.update(config["4ML_RACK_CONFIG"])

                    if "FILTER_EPPI_RACK_CONFIG" in config:
                        FILTER_EPPI_RACK_CONFIG.update(config["FILTER_EPPI_RACK_CONFIG"])

                    if "EPPI_RACK_CONFIG" in config:
                        EPPI_RACK_CONFIG.update(config["EPPI_RACK_CONFIG"])

                    if "HPLC_VIAL_RACK_CONFIG" in config:
                        HPLC_VIAL_RACK_CONFIG.update(config["HPLC_VIAL_RACK_CONFIG"])

                    if "HPLC_VIAL_INSERT_RACK_CONFIG" in config:
                        HPLC_VIAL_INSERT_RACK_CONFIG.update(config["HPLC_VIAL_INSERT_RACK_CONFIG"])

                    if "SCREWCAP_VIAL_RACK_CONFIG" in config:
                        SCREWCAP_VIAL_RACK_CONFIG.update(config["SCREWCAP_VIAL_RACK_CONFIG"])

                    print(f"[CONFIG] Loaded from {self.config_file}")
            except Exception as e:
                print(f"[CONFIG] Error loading JSON: {e}. Using defaults.")

    def save_calibration_config(self, new_config):
        try:
            with open(self.config_file, "w") as f:
                json.dump(new_config, f, indent=4)
        except Exception as e:
            messagebox.showerror("Save Error", f"Could not save config: {e}")

    def attempt_auto_connect(self):
        target_port = "/dev/ttyUSB0"
        available_ports = [p.device for p in serial.tools.list_ports.comports()]
        if target_port in available_ports:
            self.port_var.set(target_port)
            self.log_line(f"[SYSTEM] Auto-connecting to {target_port}...")
            self.connect()

    def _build_ui(self):
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(side="top", fill="both", expand=True, padx=2, pady=2)

        self.tab_init = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_init, text=" Initialization ")
        self._build_initialization_tab(self.tab_init)

        self.tab_transfer = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_transfer, text=" Transfer Liquid ")
        self._build_transfer_liquid_tab(self.tab_transfer)

        self.tab_combine = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_combine, text=" Combine Fractions ")
        self._build_combine_fractions_tab(self.tab_combine)

        self.tab_dilution = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_dilution, text=" Dilution ")
        self._build_dilution_tab(self.tab_dilution)

        self.tab_aliquots = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_aliquots, text=" Aliquots ")
        self._build_aliquots_tab(self.tab_aliquots)

        self.tab_movement = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_movement, text=" Movement / XYZ ")
        self._build_movement_tab(self.tab_movement)

        self.tab_pipette = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_pipette, text=" Pipette Control ")
        self._build_pipette_tab(self.tab_pipette)

        self.tab_tips = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_tips, text=" Log ")
        self._build_maintenance_tab(self.tab_tips)

        self.tab_testing = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_testing, text=" Testing ")
        self._build_testing_tab(self.tab_testing)

        self.tab_calibration = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_calibration, text=" Calibration ")
        self._build_calibration_tab(self.tab_calibration)

        # Bottom Bar
        bottom_frame = ttk.Frame(self.root, relief="sunken", borderwidth=1)
        bottom_frame.pack(side="bottom", fill="x", padx=0, pady=0)
        lbl_font = ("Consolas", 10, "bold")
        val_font = ("Consolas", 10)

        left_container = ttk.Frame(bottom_frame)
        left_container.pack(side="left", fill="x", padx=5, pady=2)
        ttk.Label(left_container, text="X:", font=lbl_font).pack(side="left")
        ttk.Label(left_container, textvariable=self.coord_x_var, font=val_font, width=6).pack(side="left")
        ttk.Label(left_container, text=" Y:", font=lbl_font).pack(side="left")
        ttk.Label(left_container, textvariable=self.coord_y_var, font=val_font, width=6).pack(side="left")
        ttk.Label(left_container, text=" Z:", font=lbl_font).pack(side="left")
        ttk.Label(left_container, textvariable=self.coord_z_var, font=val_font, width=6).pack(side="left")
        ttk.Label(left_container, text=" V:", font=lbl_font, foreground="blue").pack(side="left", padx=(10, 0))
        ttk.Label(left_container, textvariable=self.live_vol_var, font=val_font, foreground="blue", width=6).pack(
            side="left")
        ttk.Label(left_container, text=" Mod:", font=lbl_font).pack(side="left", padx=(10, 0))
        ttk.Label(left_container, textvariable=self.module_hover_var, font=val_font, width=15).pack(side="left")

        right_container = ttk.Frame(bottom_frame)
        right_container.pack(side="right", padx=5, pady=2)

        # ABORT BUTTON (Modified: Initially Disabled)
        self.abort_btn = tk.Button(right_container, text="Soft Stop", font=("Arial", 10, "bold"), fg="black",
                                   command=self.abort_sequence, state="disabled")
        self.abort_btn.pack(side="left", padx=10)

        self.status_icon_lbl = tk.Label(right_container, text="✘", font=("Arial", 12, "bold"), fg="red")
        self.status_icon_lbl.pack(side="right", padx=2)
        ttk.Label(right_container, textvariable=self.port_var, font=("Arial", 9)).pack(side="right", padx=2)

        mid_container = ttk.Frame(bottom_frame)
        mid_container.pack(side="left", fill="x", expand=True, padx=10)
        ttk.Label(mid_container, text="STATUS:", font=lbl_font, foreground="#555").pack(side="left")
        ttk.Label(mid_container, textvariable=self.last_cmd_var, font=("Arial", 9, "italic"), foreground="#333").pack(
            side="left", padx=5)

    def _build_initialization_tab(self, parent):
        conn_frame = ttk.LabelFrame(parent, text="Connection", padding=2)
        conn_frame.pack(side="top", fill="x", padx=5, pady=2)
        ttk.Label(conn_frame, text="Port:").pack(side="left", padx=5)
        self.port_combo = ttk.Combobox(conn_frame, textvariable=self.port_var, width=15, state="readonly")
        self.port_combo.pack(side="left", padx=5)
        ttk.Label(conn_frame, text="Baud:").pack(side="left", padx=5)
        self.baud_combo = ttk.Combobox(conn_frame, textvariable=self.baud_var, width=10, state="readonly",
                                       values=["115200", "250000"])
        self.baud_combo.pack(side="left", padx=5)
        ttk.Button(conn_frame, text="Refresh", command=self.refresh_ports).pack(side="left", padx=5)
        self.connect_btn = ttk.Button(conn_frame, text="Connect", command=self.toggle_connection)
        self.connect_btn.pack(side="left", padx=5)

        bottom_container = ttk.Frame(parent)
        bottom_container.pack(side="top", fill="both", expand=True, padx=5, pady=2)

        left_col = ttk.LabelFrame(bottom_container, text="Pipette Volume Init", padding=5)
        left_col.pack(side="left", fill="both", expand=True, padx=(0, 2))
        ttk.Label(left_col, text="Enter Current Vol:", font=("Arial", 9)).pack(pady=2)
        vol_entry = ttk.Entry(left_col, textvariable=self.current_vol_var, font=("Arial", 12), width=8,
                              justify="center")
        vol_entry.pack(pady=2)
        ttk.Label(left_col, text="⬇ Sync To ⬇", font=("Arial", 10)).pack(pady=2)
        ttk.Label(left_col, text="Target Volume:", font=("Arial", 9)).pack(pady=2)
        target_entry = ttk.Entry(left_col, textvariable=self.target_vol_var, font=("Arial", 12), width=8,
                                 justify="center")
        target_entry.pack(pady=2)
        ttk.Button(left_col, text="SYNC & MOVE", command=self.run_calibration_sequence).pack(fill="x", pady=5, ipady=3)
        ttk.Button(left_col, text="HOME ALL", command=lambda: self.send_home("All")).pack(fill="x", pady=5, ipady=3)

        right_col = ttk.LabelFrame(bottom_container, text="Tip Inventory", padding=5)
        right_col.pack(side="right", fill="both", expand=True, padx=(2, 0))
        grid_frame = ttk.Frame(right_col)
        grid_frame.pack(expand=True)
        for r_idx, r in enumerate(self.tip_rows):
            for c_idx, c in enumerate(self.tip_cols):
                key = f"{r}{c}"
                btn = tk.Button(grid_frame, text=key, width=3, height=1, font=("Arial", 8, "bold"),
                                command=lambda k=key: self.toggle_tip_state(k))
                btn.grid(row=r_idx, column=c_idx, padx=1, pady=1)
                self.tip_buttons[key] = btn
        self.update_tip_grid_colors()
        btn_frame = ttk.Frame(right_col)
        btn_frame.pack(fill="x", pady=5)
        ttk.Button(btn_frame, text="Reset All Fresh", command=self.reset_all_tips_fresh).pack(fill="x", pady=1)
        ttk.Button(btn_frame, text="Reset All Empty", command=self.reset_all_tips_empty).pack(fill="x", pady=1)

    def _build_transfer_liquid_tab(self, parent):
        frame = ttk.Frame(parent, padding=10)
        frame.pack(fill="both", expand=True)

        table = ttk.Frame(frame)
        table.pack(fill="x", pady=(0, 5))
        table.grid_anchor("w")

        cols = [
            ("Execute", 8), ("Line", 4), ("Source Mod", 12), ("Source Pos", 10),
            ("Dest Vial", 15), ("Vol (uL)", 8), ("Volatile", 8),
            ("Wash Vol", 8), ("Wash Times", 8), ("Wash Source", 12)
        ]

        for c, (text, w) in enumerate(cols):
            ttk.Label(
                table, text=text, width=w,
                font=("Arial", 9, "bold"),
                anchor="center"
            ).grid(row=0, column=c, padx=2, pady=(0, 4), sticky="ew")

        dest_options = []
        dest_options.extend([f"4mL {p}" for p in self._4ml_positions])
        dest_options.extend([f"Falcon {p}" for p in self.falcon_positions])
        dest_options.extend([f"Filter Eppi {p}" for p in self.filter_eppi_positions])
        dest_options.extend([f"Eppi {p}" for p in self.eppi_positions])
        dest_options.extend([f"HPLC {p}" for p in self.hplc_positions])
        dest_options.extend([f"HPLC Insert {p}" for p in self.hplc_insert_positions])
        dest_options.extend([f"Screwcap {p}" for p in self.screwcap_positions])
        dest_options.extend(self.wash_positions)

        vol_options = ["10", "50"] + [str(x) for x in range(100, 1700, 100)]
        wash_vol_options_std = ["0"] + [str(x) for x in range(100, 900, 100)]
        wash_vol_options_volatile = ["0", "100", "200", "300", "400"]
        wash_times_options = [str(x) for x in range(1, 6)]

        def wash_enabled(val: str) -> bool:
            try:
                return float(val) != 0.0
            except (TypeError, ValueError):
                return False

        self.transfer_rows = []
        module_names = list(self.module_options_map.keys())

        for i in range(8):
            row_vars = {
                "execute": tk.BooleanVar(value=False),
                "src_mod": tk.StringVar(value=""),
                "src_pos": tk.StringVar(value=""),
                "dest": tk.StringVar(value=""),
                "vol": tk.StringVar(value="800"),
                "volatile": tk.BooleanVar(value=False),
                "wash_vol": tk.StringVar(value="0"),
                "wash_times": tk.StringVar(value="2"),
                "wash_src": tk.StringVar(value="Wash A"),
            }

            r = i + 1

            ttk.Checkbutton(table, variable=row_vars["execute"]).grid(row=r, column=0, padx=2, pady=2)
            ttk.Label(table, text=f"{i + 1}", width=4, anchor="center").grid(row=r, column=1, padx=2, pady=2)

            cb_mod = ttk.Combobox(
                table, textvariable=row_vars["src_mod"],
                values=module_names, width=12, state="readonly"
            )
            cb_mod.grid(row=r, column=2, padx=2, pady=2)

            cb_pos = ttk.Combobox(table, textvariable=row_vars["src_pos"], width=10, state="readonly")
            cb_pos.grid(row=r, column=3, padx=2, pady=2)
            row_vars["_src_pos_combo"] = cb_pos

            cb_mod.bind(
                "<<ComboboxSelected>>",
                lambda e, m=row_vars["src_mod"], p=cb_pos, v=row_vars["src_pos"]:
                self._update_source_pos_options(m, p, v)
            )
            self._update_source_pos_options(row_vars["src_mod"], cb_pos, row_vars["src_pos"])

            ttk.Combobox(
                table, textvariable=row_vars["dest"],
                values=dest_options, width=15, state="readonly"
            ).grid(row=r, column=4, padx=2, pady=2)

            ttk.Combobox(
                table, textvariable=row_vars["vol"],
                values=vol_options, width=8, state="readonly"
            ).grid(row=r, column=5, padx=2, pady=2)

            ttk.Checkbutton(table, variable=row_vars["volatile"]).grid(row=r, column=6, padx=2, pady=2)

            cb_wash_vol = ttk.Combobox(
                table, textvariable=row_vars["wash_vol"],
                values=wash_vol_options_std, width=8, state="readonly"
            )
            cb_wash_vol.grid(row=r, column=7, padx=2, pady=2)

            cb_wash_times = ttk.Combobox(
                table, textvariable=row_vars["wash_times"],
                values=wash_times_options, width=8, state="readonly"
            )
            cb_wash_times.grid(row=r, column=8, padx=2, pady=2)

            cb_wash_src = ttk.Combobox(
                table, textvariable=row_vars["wash_src"],
                values=dest_options, width=12, state="readonly"
            )
            cb_wash_src.grid(row=r, column=9, padx=2, pady=2)

            def update_wash_vol_choices(*_, cb=cb_wash_vol, rv=row_vars):
                if rv["volatile"].get():
                    cb["values"] = wash_vol_options_volatile
                    if rv["wash_vol"].get() not in wash_vol_options_volatile:
                        rv["wash_vol"].set("0")
                else:
                    cb["values"] = wash_vol_options_std
                    if rv["wash_vol"].get() not in wash_vol_options_std:
                        rv["wash_vol"].set("0")

            row_vars["volatile"].trace_add("write", update_wash_vol_choices)

            def update_wash_visibility(*_, rv=row_vars, t_cb=cb_wash_times, s_cb=cb_wash_src):
                enabled = wash_enabled(rv["wash_vol"].get())
                if enabled:
                    t_cb.grid()
                    s_cb.grid()
                    if rv["wash_times"].get() not in wash_times_options:
                        rv["wash_times"].set(wash_times_options[0])
                    if not rv["wash_src"].get():
                        rv["wash_src"].set("Wash A")
                else:
                    t_cb.grid_remove()
                    s_cb.grid_remove()

            row_vars["wash_vol"].trace_add("write", update_wash_visibility)
            update_wash_visibility()

            self.transfer_rows.append(row_vars)

        btn_frame = ttk.Frame(frame, padding=10)
        btn_frame.pack(fill="x", pady=10)

        # EXECUTE AND PAUSE BUTTONS ROW
        exec_row = ttk.Frame(btn_frame)
        exec_row.pack(fill="x", pady=5)

        self.transfer_exec_btn = ttk.Button(
            exec_row, text="EXECUTE TRANSFER SEQUENCE",
            command=lambda: threading.Thread(target=self.transfer_liquid_sequence, daemon=True).start()
        )
        self.transfer_exec_btn.pack(side="left", fill="x", expand=True, padx=(0, 5), ipady=5)

        self.transfer_pause_btn = ttk.Button(
            exec_row, text="PAUSE",
            command=self.toggle_pause
        )
        self.transfer_pause_btn.pack(side="left", fill="x", padx=(5, 0), ipady=5)

        ttk.Label(btn_frame, text="Presets:", font=("Arial", 8, "italic")).pack(anchor="w", pady=(8, 2))

        presets_row = ttk.Frame(btn_frame)
        presets_row.pack(fill="x")

        ttk.Button(presets_row, text="P1", command=self.load_transfer_preset_1).pack(
            side="left", expand=True, fill="x", padx=2, ipady=2
        )
        ttk.Button(presets_row, text="P2", command=self.load_transfer_preset_2).pack(
            side="left", expand=True, fill="x", padx=2, ipady=2
        )
        ttk.Button(presets_row, text="P3", command=self.load_transfer_preset_3).pack(
            side="left", expand=True, fill="x", padx=2, ipady=2
        )
        ttk.Button(presets_row, text="P4", command=self.load_transfer_preset_4).pack(
            side="left", expand=True, fill="x", padx=2, ipady=2
        )
        ttk.Button(presets_row, text="P5", command=self.load_transfer_preset_5).pack(
            side="left", expand=True, fill="x", padx=2, ipady=2
        )

        ttk.Label(
            btn_frame,
            text="Check 'Execute' box for rows you want to run.",
            font=("Arial", 8, "italic")
        ).pack(pady=5)

    def _update_source_pos_options(self, mod_var, pos_combo, pos_var):
        mod_name = mod_var.get()
        if mod_name in self.module_options_map:
            values = self.module_options_map[mod_name]
            pos_combo['values'] = values
            if values:
                pos_var.set(values[0])
            else:
                pos_var.set("")

    def _preset_val_to_str(self, v):
        if v is None:
            return ""
        if isinstance(v, bool):
            return "1" if v else "0"
        if isinstance(v, int):
            return str(v)
        if isinstance(v, float):
            return str(int(v)) if v.is_integer() else str(v)
        return str(v)

    def _set_transfer_row_source(self, row_vars, src_mod_name, src_pos_name):
        row_vars["src_mod"].set(src_mod_name)
        pos_combo = row_vars.get("_src_pos_combo")
        values = self.module_options_map.get(src_mod_name, [])
        if pos_combo is not None:
            pos_combo["values"] = values
        if values:
            if src_pos_name in values:
                row_vars["src_pos"].set(src_pos_name)
            else:
                row_vars["src_pos"].set(values[0])
        else:
            row_vars["src_pos"].set("")

    def _apply_transfer_table_preset(self, preset_rows, preset_name=""):
        if not hasattr(self, "transfer_rows") or not self.transfer_rows:
            return
        defaults = {
            "execute": False,
            "src_mod": "4mL Rack",
            "src_pos": "A1",
            "dest": "Filter Eppi B1",
            "vol": "500",
            "volatile": False,
            "wash_vol": "0",
            "wash_times": "2",
            "wash_src": "Wash A",
        }
        for i, row_vars in enumerate(self.transfer_rows):
            spec = preset_rows[i] if i < len(preset_rows) else {}
            if spec is None:
                spec = {}
            execute = bool(spec.get("execute", defaults["execute"]))
            src_mod = spec.get("src_mod", defaults["src_mod"])
            src_pos = spec.get("src_pos", defaults["src_pos"])
            dest = spec.get("dest", defaults["dest"])
            vol = self._preset_val_to_str(spec.get("vol", defaults["vol"]))
            volatile = bool(spec.get("volatile", defaults["volatile"]))
            wash_vol = self._preset_val_to_str(spec.get("wash_vol", defaults["wash_vol"]))
            wash_times = self._preset_val_to_str(spec.get("wash_times", defaults["wash_times"]))
            wash_src = spec.get("wash_src", defaults["wash_src"])
            row_vars["execute"].set(execute)
            self._set_transfer_row_source(row_vars, src_mod, src_pos)
            row_vars["dest"].set(dest)
            row_vars["vol"].set(vol)
            row_vars["volatile"].set(volatile)
            row_vars["wash_vol"].set(wash_vol)
            row_vars["wash_times"].set(wash_times)
            row_vars["wash_src"].set(wash_src)
        try:
            if preset_name:
                self.log_line(f"[UI] Transfer preset loaded: {preset_name}")
        except Exception:
            pass

    def load_transfer_preset_1(self):
        preset = [
            {"execute": False, "src_mod": "4mL Rack", "src_pos": "A1", "dest": "Filter Eppi B1", "vol": 900,
             "volatile": True, "wash_vol": 200, "wash_times": 2, "wash_src": "Wash B"},
            {"execute": False, "src_mod": "4mL Rack", "src_pos": "A2", "dest": "Filter Eppi B2", "vol": 900,
             "volatile": True, "wash_vol": 200, "wash_times": 2, "wash_src": "Wash B"},
            {"execute": False, "src_mod": "4mL Rack", "src_pos": "A3", "dest": "Filter Eppi B3", "vol": 900,
             "volatile": True, "wash_vol": 200, "wash_times": 2, "wash_src": "Wash B"},
            {"execute": False, "src_mod": "4mL Rack", "src_pos": "A4", "dest": "Filter Eppi B4", "vol": 900,
             "volatile": True, "wash_vol": 200, "wash_times": 2, "wash_src": "Wash B"},
            {"execute": False, "src_mod": "4mL Rack", "src_pos": "A5", "dest": "Filter Eppi B5", "vol": 900,
             "volatile": True, "wash_vol": 200, "wash_times": 2, "wash_src": "Wash B"},
            {"execute": False, "src_mod": "4mL Rack", "src_pos": "A6", "dest": "Filter Eppi B6", "vol": 900,
             "volatile": True, "wash_vol": 200, "wash_times": 2, "wash_src": "Wash B"},
            {"execute": False, "src_mod": "4mL Rack", "src_pos": "A7", "dest": "Filter Eppi B7", "vol": 900,
             "volatile": True, "wash_vol": 200, "wash_times": 2, "wash_src": "Wash B"},
            {"execute": False, "src_mod": "4mL Rack", "src_pos": "A8", "dest": "Filter Eppi B8", "vol": 900,
             "volatile": True, "wash_vol": 200, "wash_times": 2, "wash_src": "Wash B"},
        ]
        self._apply_transfer_table_preset(preset, preset_name="Preset 1")

    def load_transfer_preset_2(self):
        preset = [
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C1", "dest": "Filter Eppi B1", "vol": 800,
             "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C2", "dest": "Filter Eppi B2", "vol": 800,
             "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C3", "dest": "Filter Eppi B3", "vol": 800,
             "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C4", "dest": "Filter Eppi B4", "vol": 800,
             "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C5", "dest": "Filter Eppi B5", "vol": 800,
             "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C6", "dest": "Filter Eppi B6", "vol": 800,
             "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C7", "dest": "Filter Eppi B7", "vol": 800,
             "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C8", "dest": "Filter Eppi B8", "vol": 800,
             "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
        ]
        self._apply_transfer_table_preset(preset, preset_name="Preset 2")

    def load_transfer_preset_3(self):
        preset = [
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C1", "dest": "HPLC D1", "vol": 800,
             "volatile": False, "wash_vol": 100, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C2", "dest": "HPLC D2", "vol": 800,
             "volatile": False, "wash_vol": 100, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C3", "dest": "HPLC D3", "vol": 800,
             "volatile": False, "wash_vol": 100, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C4", "dest": "HPLC D4", "vol": 800,
             "volatile": False, "wash_vol": 100, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C5", "dest": "HPLC D5", "vol": 800,
             "volatile": False, "wash_vol": 100, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C6", "dest": "HPLC D6", "vol": 800,
             "volatile": False, "wash_vol": 100, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C7", "dest": "HPLC D7", "vol": 800,
             "volatile": False, "wash_vol": 100, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C8", "dest": "HPLC D8", "vol": 800,
             "volatile": False, "wash_vol": 100, "wash_times": 1, "wash_src": "Wash A"},
        ]
        self._apply_transfer_table_preset(preset, preset_name="Preset 3")

    def load_transfer_preset_4(self):
        preset = [
            {"execute": False, "src_mod": "Falcon Rack", "src_pos": "A1", "dest": "Filter Eppi B1", "vol": 800,
             "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Falcon Rack", "src_pos": "A2", "dest": "Filter Eppi B2", "vol": 800,
             "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Falcon Rack", "src_pos": "A3", "dest": "Filter Eppi B3", "vol": 800,
             "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Falcon Rack", "src_pos": "B1", "dest": "Filter Eppi B4", "vol": 800,
             "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Falcon Rack", "src_pos": "B2", "dest": "Filter Eppi B5", "vol": 800,
             "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Falcon Rack", "src_pos": "B3", "dest": "Filter Eppi B6", "vol": 800,
             "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "", "src_pos": "", "dest": "", "vol": 0, "volatile": False, "wash_vol": 0,
             "wash_times": 2, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "", "src_pos": "", "dest": "", "vol": 0, "volatile": False, "wash_vol": 0,
             "wash_times": 2, "wash_src": "Wash A"},
        ]
        self._apply_transfer_table_preset(preset, preset_name="Preset 4")

    def load_transfer_preset_5(self):
        preset = [
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F1", "dest": "HPLC Insert E1", "vol": 35,
             "volatile": False, "wash_vol": 0, "wash_times": 0, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F2", "dest": "HPLC Insert E2", "vol": 35,
             "volatile": False, "wash_vol": 0, "wash_times": 0, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F3", "dest": "HPLC Insert E3", "vol": 35,
             "volatile": False, "wash_vol": 0, "wash_times": 0, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F4", "dest": "HPLC Insert E4", "vol": 35,
             "volatile": False, "wash_vol": 0, "wash_times": 0, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F5", "dest": "HPLC Insert E5", "vol": 35,
             "volatile": False, "wash_vol": 0, "wash_times": 0, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F6", "dest": "HPLC Insert E6", "vol": 35,
             "volatile": False, "wash_vol": 0, "wash_times": 0, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F7", "dest": "HPLC Insert E7", "vol": 35,
             "volatile": False, "wash_vol": 0, "wash_times": 0, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F8", "dest": "HPLC Insert E8", "vol": 35,
             "volatile": False, "wash_vol": 0, "wash_times": 0, "wash_src": "Wash A"},
        ]
        self._apply_transfer_table_preset(preset, preset_name="Preset 5")

        # ==========================================
    #           ALIQUOT PRESETS
    # ==========================================

    def _apply_aliquot_preset(self, preset_rows, preset_name=""):
        if not hasattr(self, "aliquot_rows") or not self.aliquot_rows:
            return
        defaults = {
            "execute": False,
            "source": "",
            "volume": "800.0",
            "dest_start": "",
            "dest_end": "",
        }
        for i, row_vars in enumerate(self.aliquot_rows):
            spec = preset_rows[i] if i < len(preset_rows) else {}
            if spec is None:
                spec = {}
            execute = bool(spec.get("execute", defaults["execute"]))
            source = spec.get("source", defaults["source"])
            volume = self._preset_val_to_str(spec.get("volume", defaults["volume"]))
            dest_start = spec.get("dest_start", defaults["dest_start"])
            dest_end = spec.get("dest_end", defaults["dest_end"])

            row_vars["execute"].set(execute)
            row_vars["source"].set(source)
            row_vars["volume"].set(volume)
            row_vars["dest_start"].set(dest_start)
            row_vars["dest_end"].set(dest_end)
        try:
            if preset_name:
                self.log_line(f"[UI] Aliquot preset loaded: {preset_name}")
        except Exception:
            pass

    def load_aliquot_preset_1(self):
        preset = [
            {"execute": True, "source": "96Well A1", "volume": 640, "dest_start": "Eppi C1", "dest_end": "Eppi C4"},
            {"execute": True, "source": "96Well B1", "volume": 640, "dest_start": "Eppi C5", "dest_end": "Eppi C8"},
            {"execute": True, "source": "96Well C1", "volume": 640, "dest_start": "Screwcap F1",
             "dest_end": "Screwcap F4"},
            {"execute": True, "source": "96Well D1", "volume": 640, "dest_start": "Screwcap F5",
             "dest_end": "Screwcap F8"},
            {"execute": False, "source": "96Well E1", "volume": 640, "dest_start": "Eppi C1",
             "dest_end": "Eppi C4"},
            {"execute": False, "source": "96Well F1", "volume": 640, "dest_start": "Eppi C5",
             "dest_end": "Eppi C8"},
            {"execute": False, "source": "96Well G1", "volume": 640, "dest_start": "Screwcap F1",
             "dest_end": "Screwcap F4"},
            {"execute": False, "source": "96Well H1", "volume": 640, "dest_start": "Screwcap F5",
             "dest_end": "Screwcap F8"},
        ]
        self._apply_aliquot_preset(preset, preset_name="P1")

    def load_aliquot_preset_2(self):
        preset = [
            {"execute": True, "source": "96Well A5", "volume": 640, "dest_start": "Eppi C1", "dest_end": "Eppi C4"},
            {"execute": True, "source": "96Well B5", "volume": 640, "dest_start": "Eppi C5", "dest_end": "Eppi C8"},
            {"execute": True, "source": "96Well C5", "volume": 640, "dest_start": "Screwcap F1",
             "dest_end": "Screwcap F4"},
            {"execute": True, "source": "96Well D5", "volume": 640, "dest_start": "Screwcap F5",
             "dest_end": "Screwcap F8"},
            {"execute": False, "source": "96Well E5", "volume": 640, "dest_start": "Eppi C1",
             "dest_end": "Eppi C4"},
            {"execute": False, "source": "96Well F5", "volume": 640, "dest_start": "Eppi C5",
             "dest_end": "Eppi C8"},
            {"execute": False, "source": "96Well G5", "volume": 640, "dest_start": "Screwcap F1",
             "dest_end": "Screwcap F4"},
            {"execute": False, "source": "96Well H5", "volume": 640, "dest_start": "Screwcap F5",
             "dest_end": "Screwcap F8"},
        ]
        self._apply_aliquot_preset(preset, preset_name="P2")

    def load_aliquot_preset_3(self):
        preset = [
            {"execute": True, "source": "96Well A9", "volume": 640, "dest_start": "Eppi C1", "dest_end": "Eppi C4"},
            {"execute": True, "source": "96Well B9", "volume": 640, "dest_start": "Eppi C5", "dest_end": "Eppi C8"},
            {"execute": True, "source": "96Well C9", "volume": 640, "dest_start": "Screwcap F1",
             "dest_end": "Screwcap F4"},
            {"execute": True, "source": "96Well D9", "volume": 640, "dest_start": "Screwcap F5",
             "dest_end": "Screwcap F8"},
            {"execute": False, "source": "96Well E9", "volume": 640, "dest_start": "Eppi C1",
             "dest_end": "Eppi C4"},
            {"execute": False, "source": "96Well F9", "volume": 640, "dest_start": "Eppi C5",
             "dest_end": "Eppi C8"},
            {"execute": False, "source": "96Well G9", "volume": 640, "dest_start": "Screwcap F1",
             "dest_end": "Screwcap F4"},
            {"execute": False, "source": "96Well H9", "volume": 640, "dest_start": "Screwcap F5",
             "dest_end": "Screwcap F8"},
        ]
        self._apply_aliquot_preset(preset, preset_name="P3")

    # ==========================================
    #           DILUTION PRESETS
    # ==========================================

    def _apply_dilution_preset(self, preset_rows, preset_name=""):
        if not hasattr(self, "dilution_rows") or not self.dilution_rows:
            return
        defaults = {
            "execute": False,
            "src_mod": "",
            "src_pos": "",
            "src_conc": "",
            "diluent": "Wash A",
            "plate_col": "1",
            "final_conc": "",
        }
        for i, row_vars in enumerate(self.dilution_rows):
            spec = preset_rows[i] if i < len(preset_rows) else {}
            if spec is None:
                spec = {}
            execute = bool(spec.get("execute", defaults["execute"]))
            src_mod = spec.get("src_mod", defaults["src_mod"])
            src_pos = spec.get("src_pos", defaults["src_pos"])
            src_conc = self._preset_val_to_str(spec.get("src_conc", defaults["src_conc"]))
            diluent = spec.get("diluent", defaults["diluent"])
            plate_col = self._preset_val_to_str(spec.get("plate_col", defaults["plate_col"]))
            final_conc = self._preset_val_to_str(spec.get("final_conc", defaults["final_conc"]))

            row_vars["execute"].set(execute)
            self._set_transfer_row_source(row_vars, src_mod, src_pos)
            row_vars["src_conc"].set(src_conc)
            row_vars["diluent"].set(diluent)
            row_vars["plate_col"].set(plate_col)
            row_vars["final_conc"].set(final_conc)
        try:
            if preset_name:
                self.log_line(f"[UI] Dilution preset loaded: {preset_name}")
        except Exception:
            pass

    def load_dilution_preset_1(self):
        preset = [
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F1", "src_conc": "", "diluent": "Wash A",
             "plate_col": 1, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F2", "src_conc": "", "diluent": "Wash A",
             "plate_col": 1, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F3", "src_conc": "", "diluent": "Wash A",
             "plate_col": 1, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F4", "src_conc": "", "diluent": "Wash A",
             "plate_col": 1, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F5", "src_conc": "", "diluent": "Wash A",
             "plate_col": 1, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F6", "src_conc": "", "diluent": "Wash A",
             "plate_col": 1, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F7", "src_conc": "", "diluent": "Wash A",
             "plate_col": 1, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F8", "src_conc": "", "diluent": "Wash A",
             "plate_col": 1, "final_conc": 1.25},
        ]
        self._apply_dilution_preset(preset, preset_name="P1")

    def load_dilution_preset_2(self):
        preset = [
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F1", "src_conc": "", "diluent": "Wash A",
             "plate_col": 5, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F2", "src_conc": "", "diluent": "Wash A",
             "plate_col": 5, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F3", "src_conc": "", "diluent": "Wash A",
             "plate_col": 5, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F4", "src_conc": "", "diluent": "Wash A",
             "plate_col": 5, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F5", "src_conc": "", "diluent": "Wash A",
             "plate_col": 5, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F6", "src_conc": "", "diluent": "Wash A",
             "plate_col": 5, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F7", "src_conc": "", "diluent": "Wash A",
             "plate_col": 5, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F8", "src_conc": "", "diluent": "Wash A",
             "plate_col": 5, "final_conc": 1.25},
        ]
        self._apply_dilution_preset(preset, preset_name="P2")

    def load_dilution_preset_3(self):
        preset = [
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F1", "src_conc": "", "diluent": "Wash A",
             "plate_col": 9, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F2", "src_conc": "", "diluent": "Wash A",
             "plate_col": 9, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F3", "src_conc": "", "diluent": "Wash A",
             "plate_col": 9, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F4", "src_conc": "", "diluent": "Wash A",
             "plate_col": 9, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F5", "src_conc": "", "diluent": "Wash A",
             "plate_col": 9, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F6", "src_conc": "", "diluent": "Wash A",
             "plate_col": 9, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F7", "src_conc": "", "diluent": "Wash A",
             "plate_col": 9, "final_conc": 1.25},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F8", "src_conc": "", "diluent": "Wash A",
             "plate_col": 9, "final_conc": 1.25},
        ]
        self._apply_dilution_preset(preset, preset_name="P3")


    def _build_combine_fractions_tab(self, parent):
        frame = ttk.Frame(parent, padding=10)
        frame.pack(fill="both", expand=True)

        table = ttk.Frame(frame)
        table.pack(fill="x", pady=(0, 5))
        table.grid_anchor("w")

        cols = [
            ("Execute", 8), ("Line", 6), ("Presat Tip", 10), ("Presat Src", 12),
            ("Source Start", 12), ("Source End", 12),
            ("Dest Falcon", 12), ("Vol (uL)", 10),
            ("Wash Vol", 8), ("Wash Times", 8), ("Wash Source", 12)
        ]

        for c, (text, w) in enumerate(cols):
            ttk.Label(
                table, text=text, width=w,
                font=("Arial", 9, "bold"),
                anchor="center"
            ).grid(row=0, column=c, padx=2, pady=(0, 4), sticky="ew")

        def wash_enabled(val: str) -> bool:
            try:
                return float(val) != 0.0
            except (TypeError, ValueError):
                return False

        self.combine_rows = []
        vol_options = [str(x) for x in range(100, 1700, 100)]
        default_falcons = ["A1", "A2", "A3", "B1", "B2", "B3"]
        wash_vol_options = ["0"] + [str(x) for x in range(100, 900, 100)]
        wash_times_options = [str(x) for x in range(1, 6)]
        source_options = self.wash_positions + [f"Falcon {p}" for p in self.falcon_positions]
        presat_options = ["Wash A", "Wash B", "Wash C"]

        for i in range(6):
            row_vars = {
                "execute": tk.BooleanVar(value=False),
                "presat": tk.BooleanVar(value=True),
                "presat_src": tk.StringVar(value="Wash B"),
                "start": tk.StringVar(),
                "end": tk.StringVar(),
                "dest": tk.StringVar(),
                "vol": tk.StringVar(value="600"),
                "wash_vol": tk.StringVar(value="0"),
                "wash_times": tk.StringVar(value="1"),
                "wash_src": tk.StringVar(value="Wash A"),
            }

            r = i + 1

            ttk.Checkbutton(table, variable=row_vars["execute"]).grid(row=r, column=0, padx=2, pady=2)
            ttk.Label(table, text=f"Line {i + 1}", width=6).grid(row=r, column=1, padx=2, pady=2, sticky="w")

            # Presaturation Checkbox
            ttk.Checkbutton(table, variable=row_vars["presat"]).grid(row=r, column=2, padx=2, pady=2)

            # Presaturation Source Combobox
            cb_presat = ttk.Combobox(
                table, textvariable=row_vars["presat_src"],
                values=presat_options, width=10, state="readonly"
            )
            cb_presat.grid(row=r, column=3, padx=2, pady=2)

            def toggle_presat_combo(*_, rv=row_vars, cb=cb_presat):
                if rv["presat"].get():
                    cb.grid()
                else:
                    cb.grid_remove()

            row_vars["presat"].trace_add("write", toggle_presat_combo)
            toggle_presat_combo()

            # Source Start - Hybrid Combobox (writable with auto-capitalization)
            cb_start = ttk.Combobox(
                table, textvariable=row_vars["start"],
                values=self.plate_wells, width=10, state="normal"
            )
            cb_start.grid(row=r, column=4, padx=2, pady=2)

            # Auto-capitalize function for start
            def auto_capitalize_start(*_, var=row_vars["start"]):
                value = var.get()
                if value and len(value) > 0:
                    # Capitalize the first character if it's a letter
                    if value[0].isalpha() and value[0].islower():
                        var.set(value[0].upper() + value[1:])

            row_vars["start"].trace_add("write", auto_capitalize_start)

            # Source End - Hybrid Combobox (writable with auto-capitalization)
            cb_end = ttk.Combobox(
                table, textvariable=row_vars["end"],
                values=self.plate_wells, width=10, state="normal"
            )
            cb_end.grid(row=r, column=5, padx=2, pady=2)

            # Auto-capitalize function for end
            def auto_capitalize_end(*_, var=row_vars["end"]):
                value = var.get()
                if value and len(value) > 0:
                    # Capitalize the first character if it's a letter
                    if value[0].isalpha() and value[0].islower():
                        var.set(value[0].upper() + value[1:])

            row_vars["end"].trace_add("write", auto_capitalize_end)

            cb_dest = ttk.Combobox(
                table, textvariable=row_vars["dest"],
                values=self.falcon_positions, width=10, state="readonly"
            )
            cb_dest.grid(row=r, column=6, padx=2, pady=2)

            if i < len(default_falcons):
                row_vars["dest"].set(default_falcons[i])

            cb_dest.bind("<<ComboboxSelected>>", lambda e: self._update_falcon_exclusivity())

            ttk.Combobox(
                table, textvariable=row_vars["vol"],
                values=vol_options, width=8, state="readonly"
            ).grid(row=r, column=7, padx=2, pady=2)

            cb_wash_vol = ttk.Combobox(
                table, textvariable=row_vars["wash_vol"],
                values=wash_vol_options, width=8, state="readonly"
            )
            cb_wash_vol.grid(row=r, column=8, padx=2, pady=2)

            cb_wash_times = ttk.Combobox(
                table, textvariable=row_vars["wash_times"],
                values=wash_times_options, width=8, state="readonly"
            )
            cb_wash_times.grid(row=r, column=9, padx=2, pady=2)

            cb_wash_src = ttk.Combobox(
                table, textvariable=row_vars["wash_src"],
                values=source_options, width=12, state="readonly"
            )
            cb_wash_src.grid(row=r, column=10, padx=2, pady=2)

            def update_wash_visibility(*_, rv=row_vars, t_cb=cb_wash_times, s_cb=cb_wash_src):
                enabled = wash_enabled(rv["wash_vol"].get())
                if enabled:
                    t_cb.grid()
                    s_cb.grid()
                    if rv["wash_times"].get() not in wash_times_options:
                        rv["wash_times"].set(wash_times_options[0])
                    if not rv["wash_src"].get():
                        rv["wash_src"].set("Wash A")
                else:
                    t_cb.grid_remove()
                    s_cb.grid_remove()

            row_vars["wash_vol"].trace_add("write", update_wash_visibility)
            update_wash_visibility()

            self.combine_rows.append({"vars": row_vars, "widgets": {"dest": cb_dest}})

        self._update_falcon_exclusivity()

        btn_frame = ttk.Frame(frame, padding=10)
        btn_frame.pack(fill="x", pady=10)

        # EXECUTE AND PAUSE BUTTONS ROW
        exec_row = ttk.Frame(btn_frame)
        exec_row.pack(fill="x", pady=5)

        self.combine_exec_btn = ttk.Button(
            exec_row, text="RUN COMBINE SEQUENCE",
            command=lambda: threading.Thread(target=self.combine_fractions_sequence, daemon=True).start()
        )
        self.combine_exec_btn.pack(side="left", fill="x", expand=True, padx=(0, 5), ipady=5)

        self.combine_pause_btn = ttk.Button(
            exec_row, text="PAUSE",
            command=self.toggle_pause
        )
        self.combine_pause_btn.pack(side="left", fill="x", padx=(5, 0), ipady=5)

        ttk.Label(
            btn_frame,
            text="Check 'Execute' box for lines you want to run.",
            font=("Arial", 8, "italic")
        ).pack(pady=5)

    def _build_aliquots_tab(self, parent):
        frame = ttk.Frame(parent, padding=10)
        frame.pack(fill="both", expand=True)

        table = ttk.Frame(frame)
        table.pack(fill="x", pady=(0, 5))
        table.grid_anchor("w")

        cols = [
            ("Execute", 8), ("Line", 4), ("Source Vial", 14), ("Volume (uL)", 10),
            ("Dest Start", 15), ("Dest End", 15)
        ]

        for c, (text, w) in enumerate(cols):
            ttk.Label(
                table, text=text, width=w,
                font=("Arial", 9, "bold"),
                anchor="center"
            ).grid(row=0, column=c, padx=2, pady=(0, 4), sticky="ew")

        # SOURCE OPTIONS: All small vial modules + Falcon + 96 Well Plate
        source_vial_positions = []
        source_vial_positions.extend([f"Falcon {p}" for p in self.falcon_positions])
        source_vial_positions.extend([f"4mL {p}" for p in self._4ml_positions])
        source_vial_positions.extend([f"Filter Eppi {p}" for p in self.filter_eppi_positions])
        source_vial_positions.extend([f"Eppi {p}" for p in self.eppi_positions])
        source_vial_positions.extend([f"HPLC {p}" for p in self.hplc_positions])
        source_vial_positions.extend([f"HPLC Insert {p}" for p in self.hplc_insert_positions])
        source_vial_positions.extend([f"Screwcap {p}" for p in self.screwcap_positions])
        source_vial_positions.extend([f"96Well {p}" for p in self.plate_wells])

        # Destination options: only small vial modules (A1 to F8)
        small_vial_positions = []
        small_vial_positions.extend([f"4mL {p}" for p in self._4ml_positions])
        small_vial_positions.extend([f"Filter Eppi {p}" for p in self.filter_eppi_positions])
        small_vial_positions.extend([f"Eppi {p}" for p in self.eppi_positions])
        small_vial_positions.extend([f"HPLC {p}" for p in self.hplc_positions])
        small_vial_positions.extend([f"HPLC Insert {p}" for p in self.hplc_insert_positions])
        small_vial_positions.extend([f"Screwcap {p}" for p in self.screwcap_positions])

        # ---- float validation for volume entry (allows "585.4") ----
        float_re = re.compile(r"^\d*([.]\d*)?$")

        def validate_float(P: str) -> bool:
            return bool(float_re.match(P))

        vcmd = (self.root.register(validate_float), "%P")

        self.aliquot_rows = []

        for i in range(8):
            row_vars = {
                "execute": tk.BooleanVar(value=False),
                "source": tk.StringVar(value=""),
                "volume": tk.StringVar(value="800.0"),
                "dest_start": tk.StringVar(value=""),
                "dest_end": tk.StringVar(value=""),
            }

            r = i + 1

            ttk.Checkbutton(table, variable=row_vars["execute"]).grid(row=r, column=0, padx=2, pady=2)
            ttk.Label(table, text=f"{i + 1}", width=4, anchor="center").grid(row=r, column=1, padx=2, pady=2)

            ttk.Combobox(
                table, textvariable=row_vars["source"],
                values=source_vial_positions, width=14, state="readonly"
            ).grid(row=r, column=2, padx=2, pady=2)

            ttk.Entry(
                table,
                textvariable=row_vars["volume"],
                width=10,
                justify="center",
                validate="key",
                validatecommand=vcmd
            ).grid(row=r, column=3, padx=2, pady=2)

            ttk.Combobox(
                table, textvariable=row_vars["dest_start"],
                values=small_vial_positions, width=15, state="readonly"
            ).grid(row=r, column=4, padx=2, pady=2)

            ttk.Combobox(
                table, textvariable=row_vars["dest_end"],
                values=small_vial_positions, width=15, state="readonly"
            ).grid(row=r, column=5, padx=2, pady=2)

            self.aliquot_rows.append(row_vars)

        btn_frame = ttk.Frame(frame, padding=10)
        btn_frame.pack(fill="x", pady=10)

        # EXECUTE AND PAUSE BUTTONS ROW
        exec_row = ttk.Frame(btn_frame)
        exec_row.pack(fill="x", pady=5)

        self.aliquot_exec_btn = ttk.Button(
            exec_row, text="EXECUTE ALIQUOT SEQUENCE",
            command=lambda: threading.Thread(target=self.aliquots_sequence, daemon=True).start()
        )
        self.aliquot_exec_btn.pack(side="left", fill="x", expand=True, padx=(0, 5), ipady=5)

        self.aliquot_pause_btn = ttk.Button(
            exec_row, text="PAUSE",
            command=self.toggle_pause
        )
        self.aliquot_pause_btn.pack(side="left", fill="x", padx=(5, 0), ipady=5)

        # PRESET BUTTONS
        ttk.Label(btn_frame, text="Presets:", font=("Arial", 8, "italic")).pack(anchor="w", pady=(8, 2))

        presets_row = ttk.Frame(btn_frame)
        presets_row.pack(fill="x")

        ttk.Button(presets_row, text="P1", command=self.load_aliquot_preset_1).pack(
            side="left", expand=True, fill="x", padx=2, ipady=2
        )
        ttk.Button(presets_row, text="P2", command=self.load_aliquot_preset_2).pack(
            side="left", expand=True, fill="x", padx=2, ipady=2
        )
        ttk.Button(presets_row, text="P3", command=self.load_aliquot_preset_3).pack(
            side="left", expand=True, fill="x", padx=2, ipady=2
        )

        ttk.Label(
            btn_frame,
            text="Check 'Execute' box for rows you want to run. Robot will pick fresh tip for each row.",
            font=("Arial", 8, "italic")
        ).pack(pady=5)

    def _build_dilution_tab(self, parent):
        frame = ttk.Frame(parent, padding=5)
        frame.pack(fill="both", expand=True)

        table = ttk.Frame(frame)
        table.pack(fill="x", pady=(0, 5))
        table.grid_anchor("w")

        cols = [
            ("Exec", 4), ("Line", 3), ("Source Mod", 12), ("Source Pos", 10),
            ("Conc (ug/mL)", 10), ("Diluent", 14),
            ("Plate Col", 7), ("Final Conc", 10)
        ]

        for c, (text, w) in enumerate(cols):
            ttk.Label(
                table, text=text, width=w,
                font=("Arial", 9, "bold"),
                anchor="center"
            ).grid(row=0, column=c, padx=2, pady=(0, 4), sticky="ew")

        diluent_options = []
        diluent_options.extend(["Wash A", "Wash B", "Wash C"])
        diluent_options.extend([f"Falcon {p}" for p in self.falcon_positions])

        plate_col_options = [str(i) for i in range(1, 13)]

        module_names = list(self.module_options_map.keys())

        self.dilution_rows = []

        for i in range(8):
            row_vars = {
                "execute": tk.BooleanVar(value=False),
                "src_mod": tk.StringVar(value=""),
                "src_pos": tk.StringVar(value=""),
                "src_conc": tk.StringVar(value=""),
                "diluent": tk.StringVar(value="Wash A"),
                "plate_col": tk.StringVar(value="1"),
                "final_conc": tk.StringVar(value=""),
            }

            r = i + 1

            ttk.Checkbutton(table, variable=row_vars["execute"]).grid(row=r, column=0, padx=2, pady=2)
            ttk.Label(table, text=f"{i + 1}", width=3, anchor="center").grid(row=r, column=1, padx=2, pady=2)

            cb_mod = ttk.Combobox(
                table, textvariable=row_vars["src_mod"],
                values=module_names, width=12, state="readonly"
            )
            cb_mod.grid(row=r, column=2, padx=2, pady=2)

            cb_pos = ttk.Combobox(table, textvariable=row_vars["src_pos"], width=10, state="readonly")
            cb_pos.grid(row=r, column=3, padx=2, pady=2)
            row_vars["_src_pos_combo"] = cb_pos

            cb_mod.bind(
                "<<ComboboxSelected>>",
                lambda e, m=row_vars["src_mod"], p=cb_pos, v=row_vars["src_pos"]:
                self._update_source_pos_options(m, p, v)
            )
            self._update_source_pos_options(row_vars["src_mod"], cb_pos, row_vars["src_pos"])

            ttk.Entry(
                table, textvariable=row_vars["src_conc"],
                width=10, justify="center"
            ).grid(row=r, column=4, padx=2, pady=2)

            ttk.Combobox(
                table, textvariable=row_vars["diluent"],
                values=diluent_options, width=14, state="readonly"
            ).grid(row=r, column=5, padx=2, pady=2)

            ttk.Combobox(
                table, textvariable=row_vars["plate_col"],
                values=plate_col_options, width=7, state="readonly"
            ).grid(row=r, column=6, padx=2, pady=2)

            ttk.Entry(
                table, textvariable=row_vars["final_conc"],
                width=10, justify="center"
            ).grid(row=r, column=7, padx=2, pady=2)

            self.dilution_rows.append(row_vars)

        btn_frame = ttk.Frame(frame, padding=5)
        btn_frame.pack(fill="x", pady=5)

        exec_row = ttk.Frame(btn_frame)
        exec_row.pack(fill="x", pady=5)

        self.dilution_exec_btn = ttk.Button(
            exec_row, text="EXECUTE DILUTION SEQUENCE",
            command=lambda: threading.Thread(target=self.dilution_sequence, daemon=True).start()
        )
        self.dilution_exec_btn.pack(side="left", fill="x", expand=True, padx=(0, 5), ipady=5)

        self.dilution_pause_btn = ttk.Button(
            exec_row, text="PAUSE",
            command=self.toggle_pause
        )
        self.dilution_pause_btn.pack(side="left", fill="x", padx=(5, 0), ipady=5)

        # PRESET BUTTONS
        ttk.Label(btn_frame, text="Presets:", font=("Arial", 8, "italic")).pack(anchor="w", pady=(8, 2))

        presets_row = ttk.Frame(btn_frame)
        presets_row.pack(fill="x")

        ttk.Button(presets_row, text="P1", command=self.load_dilution_preset_1).pack(
            side="left", expand=True, fill="x", padx=2, ipady=2
        )
        ttk.Button(presets_row, text="P2", command=self.load_dilution_preset_2).pack(
            side="left", expand=True, fill="x", padx=2, ipady=2
        )
        ttk.Button(presets_row, text="P3", command=self.load_dilution_preset_3).pack(
            side="left", expand=True, fill="x", padx=2, ipady=2
        )

        ttk.Label(
            btn_frame,
            text="Serial dilution via 96-well plate. Each row uses one plate row (A-H). "
                 "Plate column defines the starting column for dilution steps.",
            font=("Arial", 8, "italic"), wraplength=900, justify="left"
        ).pack(pady=5)

    def _compute_dilution_steps(self, src_conc, final_conc, max_vol=800.0, min_transfer=80.0, max_transfer=800.0):
        """Compute serial dilution steps needed to go from src_conc to final_conc.
        Each step produces a well with total volume = max_vol (800 uL).
        Returns list of dicts: [{"transfer_vol": X, "diluent_vol": Y, "result_conc": Z}, ...]
        The last step's result_conc should equal final_conc.
        """
        steps = []
        current_conc = src_conc

        while current_conc > final_conc * 1.0001:
            ratio = final_conc / current_conc
            if ratio >= min_transfer / max_vol:
                transfer_vol = ratio * max_vol
                transfer_vol = max(min_transfer, min(max_transfer, round(transfer_vol, 1)))
                diluent_vol = round(max_vol - transfer_vol, 1)
                new_conc = current_conc * (transfer_vol / max_vol)
                steps.append({
                    "transfer_vol": transfer_vol,
                    "diluent_vol": diluent_vol,
                    "result_conc": round(new_conc, 6)
                })
                current_conc = new_conc
                break
            else:
                transfer_vol = min_transfer
                diluent_vol = round(max_vol - transfer_vol, 1)
                new_conc = current_conc * (transfer_vol / max_vol)
                steps.append({
                    "transfer_vol": transfer_vol,
                    "diluent_vol": diluent_vol,
                    "result_conc": round(new_conc, 6)
                })
                current_conc = new_conc

        return steps

    def dilution_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        plate_rows = ["A", "B", "C", "D", "E", "F", "G", "H"]
        air_gap_ul = float(AIR_GAP_UL)
        e_gap_pos = -1 * air_gap_ul * STEPS_PER_UL
        e_blowout_pos = -1 * 100.0 * STEPS_PER_UL
        global_safe_z = self.resolve_coords(0, 0, GLOBAL_SAFE_Z_OFFSET)[2]

        tasks = []
        for idx, row in enumerate(self.dilution_rows):
            if not row["execute"].get():
                continue
            try:
                src_conc = float(row["src_conc"].get())
                final_conc = float(row["final_conc"].get())
                plate_col = int(row["plate_col"].get())
            except (TypeError, ValueError):
                self.log_line(f"[DILUTION] Skipping line {idx + 1}: invalid numeric input.")
                continue

            src_mod_name = row["src_mod"].get()
            src_pos_name = row["src_pos"].get()
            full_source_str = self._construct_combo_string(src_mod_name, src_pos_name)
            diluent_str = row["diluent"].get()

            if not full_source_str or not diluent_str:
                self.log_line(f"[DILUTION] Skipping line {idx + 1}: missing source/diluent.")
                continue
            if src_conc <= 0 or final_conc <= 0 or final_conc >= src_conc:
                self.log_line(f"[DILUTION] Skipping line {idx + 1}: conc must be positive and final < source.")
                continue

            steps = self._compute_dilution_steps(src_conc, final_conc)
            if not steps:
                self.log_line(f"[DILUTION] Skipping line {idx + 1}: could not compute dilution steps.")
                continue

            cols_needed = len(steps)
            if plate_col + cols_needed - 1 > 12:
                self.log_line(
                    f"[DILUTION] Skipping line {idx + 1}: needs {cols_needed} columns starting at {plate_col}, exceeds plate.")
                continue

            plate_row_char = plate_rows[idx]
            well_names = [f"{plate_row_char}{plate_col + s}" for s in range(cols_needed)]

            tasks.append({
                "line": idx + 1,
                "source": full_source_str,
                "diluent": diluent_str,
                "src_conc": src_conc,
                "final_conc": final_conc,
                "steps": steps,
                "wells": well_names,
                "plate_row": plate_row_char,
            })

        if not tasks:
            messagebox.showinfo("No Tasks", "No valid dilution lines selected for execution.")
            return

        total_steps = sum(len(t["steps"]) for t in tasks)
        self.log_command(f"[DILUTION] Starting sequence: {len(tasks)} rows, {total_steps} total dilution steps.")

        def run_seq():
            current_simulated_module = self.last_known_module
            self.log_line("[DILUTION] Ensuring no tip is loaded at start...")
            self._send_lines_with_ok(self._get_eject_tip_commands())
            self.update_last_module("EJECT")
            current_simulated_module = "EJECT"

            for task in tasks:
                line_num = task["line"]
                steps = task["steps"]
                wells = task["wells"]
                source_str = task["source"]
                diluent_str = task["diluent"]

                self.log_line(
                    f"=== DILUTION Line {line_num}: {task['src_conc']} -> {task['final_conc']} ug/mL, {len(steps)} steps ===")

                for step_idx, step in enumerate(steps):
                    transfer_vol = step["transfer_vol"]
                    diluent_vol = step["diluent_vol"]
                    result_conc = step["result_conc"]
                    dest_well = wells[step_idx]
                    dest_str = f"PLATE {dest_well}"

                    if step_idx == 0:
                        asp_source = source_str
                    else:
                        asp_source = f"PLATE {wells[step_idx - 1]}"

                    self.log_line(
                        f"--- Step {step_idx + 1}/{len(steps)}: {transfer_vol}uL from {asp_source} + {diluent_vol}uL diluent -> {dest_well} ({result_conc} ug/mL) ---")
                    self.last_cmd_var.set(f"L{line_num} Step {step_idx + 1}/{len(steps)}: {dest_well}")

                    # === PHASE A: Transfer sample to well ===
                    if step_idx == 0:
                        # First step: pick a fresh tip for sample transfer
                        tip_key = self._find_next_available_tip()
                        if not tip_key:
                            messagebox.showerror("No Tips", f"Ran out of tips at Line {line_num} step {step_idx + 1}.")
                            self.last_cmd_var.set("Idle")
                            return

                        self.log_line(f"[L{line_num}] Picking tip {tip_key} for sample transfer...")
                        self._send_lines_with_ok(
                            self._get_pick_tip_commands(tip_key, start_module=current_simulated_module))
                        self.tip_inventory[tip_key] = False
                        self.root.after(0, self.update_tip_grid_colors)
                        self.update_last_module("TIPS")
                        current_simulated_module = "TIPS"
                    else:
                        # Reuse mixing tip from previous step (already loaded)
                        self.log_line(f"[L{line_num}] Reusing mixing tip for transfer from {asp_source}...")

                    # Aspirate from source
                    src_mod, src_x, src_y, src_safe_z, src_asp_z, _ = self.get_coords_from_combo(asp_source)

                    use_opt_z_src = (current_simulated_module in SMALL_VIAL_MODULES and src_mod in SMALL_VIAL_MODULES)
                    travel_z_src = self.resolve_coords(0, 0, _4ML_RACK_CONFIG["Z_SAFE"])[
                        2] if use_opt_z_src else global_safe_z

                    cmds = []
                    cmds.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")
                    if current_simulated_module == src_mod:
                        cmds.append(f"G0 Z{src_safe_z:.2f} F{JOG_SPEED_Z}")
                        cmds.append(f"G0 X{src_x:.2f} Y{src_y:.2f} F{JOG_SPEED_XY}")
                    else:
                        cmds.append(f"G0 Z{travel_z_src:.2f} F{JOG_SPEED_Z}")
                        cmds.append(f"G0 X{src_x:.2f} Y{src_y:.2f} F{JOG_SPEED_XY}")
                        cmds.append(f"G0 Z{src_safe_z:.2f} F{JOG_SPEED_Z}")

                    # Overdraw 10% for small transfers (<100 uL) to compensate
                    # for pipette under-delivery at low volumes (e.g. 80 uL -> 88 uL)
                    asp_vol = transfer_vol * 1.10 if transfer_vol < 100 else transfer_vol
                    e_loaded_pos = -1 * (air_gap_ul + asp_vol) * STEPS_PER_UL
                    cmds.append(f"G0 Z{src_asp_z:.2f} F{JOG_SPEED_Z}")
                    cmds.append(f"G1 E{e_loaded_pos:.3f} F{PIP_SPEED}")
                    self._send_lines_with_ok(cmds)
                    self.update_last_module(src_mod)
                    current_simulated_module = src_mod

                    # Dispense into dest well
                    dest_mod, dest_x, dest_y, dest_safe_z, _, dest_disp_z = self.get_coords_from_combo(dest_str)

                    use_opt_z_dest = (current_simulated_module in SMALL_VIAL_MODULES and dest_mod in SMALL_VIAL_MODULES)
                    travel_z_dest = self.resolve_coords(0, 0, _4ML_RACK_CONFIG["Z_SAFE"])[
                        2] if use_opt_z_dest else global_safe_z

                    cmds_disp = []
                    cmds_disp.append(f"G0 Z{src_safe_z:.2f} F{JOG_SPEED_Z}")
                    cmds_disp.append(f"G0 Z{travel_z_dest:.2f} F{JOG_SPEED_Z}")
                    cmds_disp.append(f"G0 X{dest_x:.2f} Y{dest_y:.2f} F{JOG_SPEED_XY}")
                    cmds_disp.append(f"G0 Z{dest_safe_z:.2f} F{JOG_SPEED_Z}")
                    cmds_disp.append(f"G0 Z{dest_disp_z:.2f} F{JOG_SPEED_Z}")
                    cmds_disp.append(f"G1 E{e_blowout_pos:.3f} F{PIP_SPEED}")
                    cmds_disp.append(f"G0 Z{dest_safe_z:.2f} F{JOG_SPEED_Z}")
                    self._send_lines_with_ok(cmds_disp)
                    self.update_last_module(dest_mod)
                    current_simulated_module = dest_mod

                    # Eject sample tip
                    self.log_line(f"[L{line_num}] Ejecting sample tip...")
                    self._send_lines_with_ok(self._get_eject_tip_commands())
                    self.update_last_module("EJECT")
                    current_simulated_module = "EJECT"

                    # === PHASE B: Add diluent to well ===
                    tip_key = self._find_next_available_tip()
                    if not tip_key:
                        messagebox.showerror("No Tips",
                                             f"Ran out of tips at Line {line_num} step {step_idx + 1} (diluent).")
                        self.last_cmd_var.set("Idle")
                        return

                    self.log_line(f"[L{line_num}] Picking tip {tip_key} for diluent...")
                    self._send_lines_with_ok(
                        self._get_pick_tip_commands(tip_key, start_module=current_simulated_module))
                    self.tip_inventory[tip_key] = False
                    self.root.after(0, self.update_tip_grid_colors)
                    self.update_last_module("TIPS")
                    current_simulated_module = "TIPS"

                    # Aspirate diluent
                    dil_mod, dil_x, dil_y, dil_safe_z, dil_asp_z, _ = self.get_coords_from_combo(diluent_str)

                    use_opt_z_dil = (current_simulated_module in SMALL_VIAL_MODULES and dil_mod in SMALL_VIAL_MODULES)
                    travel_z_dil = self.resolve_coords(0, 0, _4ML_RACK_CONFIG["Z_SAFE"])[
                        2] if use_opt_z_dil else global_safe_z

                    cmds_dil = []
                    cmds_dil.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")
                    if current_simulated_module == dil_mod:
                        cmds_dil.append(f"G0 Z{dil_safe_z:.2f} F{JOG_SPEED_Z}")
                        cmds_dil.append(f"G0 X{dil_x:.2f} Y{dil_y:.2f} F{JOG_SPEED_XY}")
                    else:
                        cmds_dil.append(f"G0 Z{travel_z_dil:.2f} F{JOG_SPEED_Z}")
                        cmds_dil.append(f"G0 X{dil_x:.2f} Y{dil_y:.2f} F{JOG_SPEED_XY}")
                        cmds_dil.append(f"G0 Z{dil_safe_z:.2f} F{JOG_SPEED_Z}")

                    e_dil_loaded = -1 * (air_gap_ul + diluent_vol) * STEPS_PER_UL
                    cmds_dil.append(f"G0 Z{dil_asp_z:.2f} F{JOG_SPEED_Z}")
                    cmds_dil.append(f"G1 E{e_dil_loaded:.3f} F{PIP_SPEED}")
                    self._send_lines_with_ok(cmds_dil)
                    self.update_last_module(dil_mod)
                    current_simulated_module = dil_mod

                    # Dispense diluent into dest well
                    cmds_dil_disp = []
                    cmds_dil_disp.append(f"G0 Z{dil_safe_z:.2f} F{JOG_SPEED_Z}")

                    use_opt_z_dil2 = (current_simulated_module in SMALL_VIAL_MODULES and dest_mod in SMALL_VIAL_MODULES)
                    travel_z_dil2 = self.resolve_coords(0, 0, _4ML_RACK_CONFIG["Z_SAFE"])[
                        2] if use_opt_z_dil2 else global_safe_z

                    cmds_dil_disp.append(f"G0 Z{travel_z_dil2:.2f} F{JOG_SPEED_Z}")
                    cmds_dil_disp.append(f"G0 X{dest_x:.2f} Y{dest_y:.2f} F{JOG_SPEED_XY}")
                    cmds_dil_disp.append(f"G0 Z{dest_safe_z:.2f} F{JOG_SPEED_Z}")
                    cmds_dil_disp.append(f"G0 Z{dest_disp_z:.2f} F{JOG_SPEED_Z}")
                    cmds_dil_disp.append(f"G1 E{e_blowout_pos:.3f} F{PIP_SPEED}")
                    cmds_dil_disp.append(f"G0 Z{dest_safe_z:.2f} F{JOG_SPEED_Z}")
                    self._send_lines_with_ok(cmds_dil_disp)
                    self.update_last_module(dest_mod)
                    current_simulated_module = dest_mod

                    # === PHASE C: Mix with same diluent tip ===
                    mix_times = 1 if step_idx == 0 and (transfer_vol / 800.0) >= 0.5 else 2
                    self.log_line(f"[L{line_num}] Mixing {dest_well} {mix_times} time(s) with diluent tip...")

                    abs_plate_asp_z = self.resolve_coords(0, 0, PLATE_CONFIG["Z_ASPIRATE"])[2]
                    abs_plate_disp_z = self.resolve_coords(0, 0, PLATE_CONFIG["Z_DISPENSE"])[2]
                    abs_plate_safe_z = self.resolve_coords(0, 0, PLATE_CONFIG["Z_SAFE"])[2]

                    e_mix_start = -1 * 200.0 * STEPS_PER_UL
                    e_mix_asp = -1 * 1000.0 * STEPS_PER_UL
                    e_mix_disp = -1 * 100.0 * STEPS_PER_UL

                    cmds_mix = []
                    cmds_mix.append(f"G1 E{e_mix_start:.3f} F{PIP_SPEED}")
                    cmds_mix.append(f"G0 Z{abs_plate_asp_z:.2f} F{JOG_SPEED_Z}")
                    for _ in range(mix_times):
                        cmds_mix.append(f"G1 E{e_mix_asp:.3f} F{PIP_SPEED}")
                        cmds_mix.append(f"G0 Z{abs_plate_disp_z:.2f} F{JOG_SPEED_Z}")
                        cmds_mix.append(f"G1 E{e_mix_disp:.3f} F{PIP_SPEED}")
                        cmds_mix.append(f"G0 Z{abs_plate_asp_z:.2f} F{JOG_SPEED_Z}")
                    cmds_mix.append(f"G0 Z{abs_plate_safe_z:.2f} F{JOG_SPEED_Z}")
                    cmds_mix.append("M18 E")
                    self._send_lines_with_ok(cmds_mix)

                    self.current_pipette_volume = 100.0
                    self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")
                    self.live_vol_var.set(f"{self.current_pipette_volume:.1f}")

                    # Keep mixing tip for next step's sample transfer, only eject on last step
                    if step_idx < len(steps) - 1:
                        self.log_line(f"[L{line_num}] Keeping mixing tip for next transfer step...")
                        # Tip stays loaded; current_simulated_module remains dest_mod (PLATE)
                    else:
                        # Last step - eject tip
                        self.log_line(f"[L{line_num}] Ejecting final tip...")
                        self._send_lines_with_ok(self._get_eject_tip_commands())
                        self.update_last_module("EJECT")
                        current_simulated_module = "EJECT"

                self.log_line(f"=== DILUTION Line {line_num} COMPLETE ===")

            self.log_command("[DILUTION] All lines complete. Parking.")
            self.last_cmd_var.set("Parking...")
            self._send_lines_with_ok(self._get_park_head_commands())
            self.update_last_module("PARK")
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def _update_falcon_exclusivity(self):
        all_falcons = set(self.falcon_positions)
        selected_falcons = set()
        for row in self.combine_rows:
            val = row["vars"]["dest"].get()
            if val: selected_falcons.add(val)
        for row in self.combine_rows:
            current_val = row["vars"]["dest"].get()
            available = sorted(list(
                (all_falcons - selected_falcons) | {current_val} if current_val else (all_falcons - selected_falcons)))
            available = [x for x in available if x]
            row["widgets"]["dest"]["values"] = available

    def _build_movement_tab(self, parent):
        main_layout = ttk.Frame(parent, padding=10)
        main_layout.pack(fill="both", expand=True)
        step_frame = ttk.LabelFrame(main_layout, text="Step Size (mm)", padding=5)
        step_frame.pack(fill="x", pady=(0, 10))
        for val in [0.1, 0.5, 1.0, 5.0, 10.0, 50.0]:
            ttk.Radiobutton(step_frame, text=f"{float(val)}", variable=self.step_size_var, value=val).pack(side="left",
                                                                                                           expand=True)
        controls_frame = ttk.Frame(main_layout)
        controls_frame.pack(expand=True, fill="both")
        xy_frame = ttk.LabelFrame(controls_frame, text="XY Axis", padding=10)
        xy_frame.pack(side="left", fill="both", expand=True, padx=(0, 5))
        xy_grid = ttk.Frame(xy_frame)
        xy_grid.pack(expand=True)
        ttk.Button(xy_grid, text="Y+", width=8, command=lambda: self.send_jog("Y", 1)).grid(row=0, column=1, pady=5)
        ttk.Button(xy_grid, text="X-", width=8, command=lambda: self.send_jog("X", -1)).grid(row=1, column=0, padx=5)
        ttk.Button(xy_grid, text="Home XY", width=8, command=lambda: self.send_home("XY")).grid(row=1, column=1, padx=5)
        ttk.Button(xy_grid, text="X+", width=8, command=lambda: self.send_jog("X", 1)).grid(row=1, column=2, padx=5)
        ttk.Button(xy_grid, text="Y-", width=8, command=lambda: self.send_jog("Y", -1)).grid(row=2, column=1, pady=5)
        z_frame = ttk.LabelFrame(controls_frame, text="Z Axis", padding=10)
        z_frame.pack(side="right", fill="both", expand=True, padx=(5, 0))
        ttk.Button(z_frame, text="Z+ (Up)", command=lambda: self.send_jog("Z", 1)).pack(pady=5, fill='x')
        ttk.Button(z_frame, text="Home Z", command=lambda: self.send_home("Z")).pack(pady=5, fill='x')
        ttk.Button(z_frame, text="Z- (Down)", command=lambda: self.send_jog("Z", -1)).pack(pady=5, fill='x')
        ttk.Button(main_layout, text="HOME ALL (G28)", command=lambda: self.send_home("All")).pack(fill="x", pady=10)
        gcode_frame = ttk.LabelFrame(main_layout, text="Raw G-Code", padding=5)
        gcode_frame.pack(side="bottom", fill="x", pady=(10, 0))
        self.gcode_entry = ttk.Entry(gcode_frame, textvariable=self.raw_gcode_var, font=("Consolas", 10))
        self.gcode_entry.pack(side="left", fill="x", expand=True, padx=(0, 5))
        self.gcode_entry.bind('<Return>', lambda event: self.send_raw_gcode_command())
        ttk.Button(gcode_frame, text="SEND", width=10, command=self.send_raw_gcode_command).pack(side="right")

    def _build_pipette_tab(self, parent):
        scroll_frame = ttk.Frame(parent)
        scroll_frame.pack(fill="both", expand=True)
        pip_frame = ttk.LabelFrame(scroll_frame, text="Pipette Actuation", padding=2)
        pip_frame.pack(fill="x", padx=5, pady=2)
        top_pip_frame = ttk.Frame(pip_frame)
        top_pip_frame.pack(fill="x", pady=(0, 2))
        ttk.Label(top_pip_frame, text="Current Volume:", font=("Arial", 10)).pack(side="left")
        self.vol_display_lbl = ttk.Label(top_pip_frame, textvariable=self.vol_display_var, font=("Arial", 12, "bold"),
                                         foreground="blue")
        self.vol_display_lbl.pack(side="right")
        pip_entry_frame = ttk.Frame(pip_frame)
        pip_entry_frame.pack(fill="x", pady=(0, 2))
        ttk.Label(pip_entry_frame, text="Step Volume (uL):", font=("Arial", 10)).pack(side="left")
        pip_entry = ttk.Entry(pip_entry_frame, textvariable=self.pipette_move_var, font=("Arial", 10), width=8,
                              justify="center")
        pip_entry.pack(side="right")
        act_btn_frame = ttk.Frame(pip_frame)
        act_btn_frame.pack(fill="x", pady=1)
        ttk.Button(act_btn_frame, text="ASPIRATE", command=lambda: self.manual_pipette_move("aspirate")).pack(
            side="left", fill="x", expand=True, padx=(0, 2))
        ttk.Button(act_btn_frame, text="DISPENSE", command=lambda: self.manual_pipette_move("dispense")).pack(
            side="right", fill="x", expand=True, padx=(2, 0))
        smart_btn_frame = ttk.Frame(pip_frame)
        smart_btn_frame.pack(fill="x", pady=1)
        ttk.Button(smart_btn_frame, text="ASP @ POS (Smart)",
                   command=lambda: self.smart_pipette_sequence("aspirate")).pack(side="left", fill="x", expand=True,
                                                                                 padx=(0, 2))
        ttk.Button(smart_btn_frame, text="DISP @ POS (Smart)",
                   command=lambda: self.smart_pipette_sequence("dispense")).pack(side="right", fill="x", expand=True,
                                                                                 padx=(2, 0))
        mix_eject_frame = ttk.Frame(pip_frame)
        mix_eject_frame.pack(fill="x", pady=2)
        ttk.Button(mix_eject_frame, text="MIX WELL", command=self.mix_well_sequence).pack(side="left", fill="x",
                                                                                          expand=True, padx=(0, 2))
        ttk.Button(mix_eject_frame, text="EJECT TIP", command=self.eject_tip_sequence).pack(side="right", fill="x",
                                                                                            expand=True, padx=(2, 0))
        nav_frame = ttk.LabelFrame(scroll_frame, text="Navigation & Workflows", padding=2)
        nav_frame.pack(fill="both", expand=True, padx=5, pady=2)
        module_order = ["TIPS", "PLATE", "FALCON", "WASH", "4ML", "FILTER_EPPI", "EPPI", "HPLC", "HPLC_INSERT",
                        "SCREWCAP"]
        for i, mod_key in enumerate(module_order):
            mod_data = self.modules[mod_key]
            row = i // 2
            col = i % 2
            cell_frame = ttk.Frame(nav_frame, borderwidth=1, relief="solid")
            cell_frame.grid(row=row, column=col, padx=3, pady=2, sticky="nsew")
            inner = ttk.Frame(cell_frame, padding=2)
            inner.pack(fill="x", expand=True)
            ttk.Label(inner, text=mod_data["label"], width=13, font=("Arial", 9, "bold")).pack(side="left", padx=(2, 5))
            ttk.Button(inner, text=mod_data["btn_text"], width=5, command=mod_data["cmd"]).pack(side="right", padx=2)
            cb = ttk.Combobox(inner, textvariable=mod_data["var"], state="readonly", width=8, values=mod_data["values"])
            cb.pack(side="left", fill="x", expand=True, padx=2)
        nav_frame.columnconfigure(0, weight=1)
        nav_frame.columnconfigure(1, weight=1)
        self.update_available_tips_combo()

    def _build_maintenance_tab(self, parent):
        split_frame = ttk.Frame(parent, padding=10)
        split_frame.pack(fill="both", expand=True)
        left_frame = ttk.LabelFrame(split_frame, text="Controls", padding=10)
        left_frame.pack(side="left", fill="y", padx=(0, 5))
        ttk.Button(left_frame, text="PARK HEAD", command=self.park_head_sequence).pack(fill="x", pady=10)
        ttk.Button(left_frame, text="UNPAUSE / RESUME", command=self.send_resume).pack(fill="x", pady=10)
        right_frame = ttk.LabelFrame(split_frame, text="System Log", padding=5)
        right_frame.pack(side="right", fill="both", expand=True, padx=5, pady=5)
        self.log = scrolledtext.ScrolledText(right_frame, state="disabled", wrap="word", font=("Consolas", 9))
        self.log.pack(fill="both", expand=True)

    def _build_testing_tab(self, parent):
        frame = ttk.Frame(parent, padding=20)
        frame.pack(fill="both", expand=True)
        rack_frame = ttk.LabelFrame(frame, text="Tip Rack Module Test", padding=5)
        rack_frame.pack(fill="x", pady=5)
        r1_row = ttk.Frame(rack_frame)
        r1_row.pack(fill="x", expand=True)
        ttk.Button(r1_row, text="TEST: Tip Rack Module (Random Pick/Eject)",
                   command=lambda: threading.Thread(target=self.test_rack_module_sequence, daemon=True).start()
                   ).pack(side="left", fill="x", expand=True, padx=(0, 5))
        plate_frame = ttk.LabelFrame(frame, text="96 Plate Robustness Test", padding=5)
        plate_frame.pack(fill="x", pady=5)
        r2_row = ttk.Frame(plate_frame)
        r2_row.pack(fill="x", expand=True)
        ttk.Button(r2_row, text="TEST: 96 Full Plate Seq (Robustness)",
                   command=lambda: threading.Thread(target=self.test_96_plate_robustness_sequence, daemon=True).start()
                   ).pack(side="left", fill="x", expand=True, padx=(0, 5))
        mixing_frame = ttk.LabelFrame(frame, text="96 Well Plate Mixing Test", padding=5)
        mixing_frame.pack(fill="x", pady=5)
        config_row = ttk.Frame(mixing_frame)
        config_row.pack(fill="x", pady=(0, 5))
        source_options = self.falcon_positions + [f"4mL_{pos}" for pos in self._4ml_positions]
        ttk.Label(config_row, text="Vial A (Row A):").pack(side="left", padx=(0, 2))
        ttk.Combobox(config_row, textvariable=self.vial_a_var, values=source_options, width=10, state="readonly").pack(
            side="left", padx=(0, 10))
        ttk.Label(config_row, text="Vial B (Col 1):").pack(side="left", padx=(0, 2))
        ttk.Combobox(config_row, textvariable=self.vial_b_var, values=source_options, width=10, state="readonly").pack(
            side="left", padx=(0, 10))
        ttk.Label(config_row, text="Diluent:").pack(side="left", padx=(0, 2))
        ttk.Combobox(config_row, textvariable=self.diluent_var, values=self.falcon_positions, width=6,
                     state="readonly").pack(side="left", padx=(0, 10))
        action_row = ttk.Frame(mixing_frame)
        action_row.pack(fill="x", expand=True)
        ttk.Button(action_row, text="TEST: 96 Well Plate Mixing",
                   command=lambda: threading.Thread(target=self.test_96_mixing_sequence, daemon=True).start()
                   ).pack(side="left", fill="x", expand=True, padx=(0, 5))

    def _build_calibration_tab(self, parent):
        frame = ttk.Frame(parent, padding=20)
        frame.pack(fill="both", expand=True)

        # Absolute Motion Calibration (moved from initialization tab)
        abs_calib_frame = ttk.LabelFrame(frame, text="Absolute Motion Calibration", padding=10)
        abs_calib_frame.pack(fill="x", pady=(0, 10))
        ttk.Label(abs_calib_frame, text="Calibrate the machine's absolute position using the calibration pin.",
                  font=("Arial", 9)).pack(pady=(0, 5))
        ttk.Button(abs_calib_frame, text="Calibrate", command=self.start_pin_calibration_sequence).pack(fill="x",
                                                                                                        pady=5)

        # Module Calibration
        module_calib_frame = ttk.LabelFrame(frame, text="Module Calibration", padding=10)
        module_calib_frame.pack(fill="x", pady=5)
        ttk.Label(module_calib_frame, text="Calibrate the first and last positions of a selected module.",
                  font=("Arial", 9)).pack(pady=(0, 5))

        # Module selection row
        module_row = ttk.Frame(module_calib_frame)
        module_row.pack(fill="x", pady=(0, 10))
        ttk.Label(module_row, text="Module:").pack(side="left", padx=(0, 5))

        module_options = [
            "tip rack", "96 well plate", "15 mL falcon rack", "50 mL falcon rack",
            "wash rack", "4mL rack", "filter eppi rack", "eppi rack",
            "hplc vial insert rack", "screwcap vial rack"
        ]

        ttk.Combobox(module_row, textvariable=self.calibration_module_var, values=module_options,
                     width=20, state="readonly").pack(side="left", padx=(0, 10))

        ttk.Button(module_row, text="Calibrate Module",
                   command=self.start_module_calibration_sequence).pack(side="left")

    # ==========================================
    #           LOGIC & COMMS
    # ==========================================

    def log_line(self, text):
        # 1. Update GUI
        if hasattr(self, 'log') and self.log:
            self.log.configure(state="normal")
            self.log.insert("end", text + "\n")
            self.log.see("end")
            self.log.configure(state="disabled")
        else:
            print(f"[PRE-INIT LOG]: {text}")

        # 2. Append to Daily Log File
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            fname = os.path.join(self.log_dir, f"gcode-{today}.txt")
            now = datetime.now()
            time_str = now.strftime("%H:%M:%S")
            with open(fname, "a", encoding="utf-8") as f:
                f.write(f"{time_str}, {text}\n")
        except Exception as e:
            print(f"File Log Error: {e}")

    def log_command(self, text):
        self.log_line(f"[CMD] {text}")

    def _position_logger_loop(self):
        """
        Background thread that logs current memory position every 10 seconds.
        Does NOT queue G-code to the machine.
        """
        while not self.stop_event.is_set():
            try:
                now = datetime.now()
                today = now.strftime("%Y-%m-%d")
                time_str = now.strftime("%H:%M:%S")
                fname = os.path.join(self.log_dir, f"positions-{today}.txt")

                # Read from internal memory variables
                x = self.current_x
                y = self.current_y
                z = self.current_z
                vol = self.current_pipette_volume

                # Format: HH:MM:SS -> Data
                entry = f"{time_str} -> X:{x:.2f} Y:{y:.2f} Z:{z:.2f} Vol:{vol:.1f}\n"

                with open(fname, "a", encoding="utf-8") as f:
                    f.write(entry)

            except Exception as e:
                print(f"Pos Log Error: {e}")

            # Wait 10 seconds
            time.sleep(10)

    def refresh_ports(self):
        ports = [p.device for p in serial.tools.list_ports.comports()]
        self.port_combo["values"] = ports
        if ports and not self.port_var.get():
            self.port_var.set(ports[0])

    def toggle_connection(self):
        if self.ser and self.ser.is_open:
            self.disconnect()
        else:
            self.connect()

    def update_connection_status_icon(self, is_connected):
        if is_connected:
            self.status_icon_lbl.config(text="✔", fg="green")
            self.status_var.set(f"Connected: {self.port_var.get()}")
            self.connect_btn.configure(text="Disconnect")
        else:
            self.status_icon_lbl.config(text="✘", fg="red")
            self.status_var.set("Disconnected")
            self.connect_btn.configure(text="Connect")
            self.coord_x_var.set("0.00")
            self.coord_y_var.set("0.00")
            self.coord_z_var.set("0.00")

    def connect(self):
        port = self.port_var.get().strip()
        if not port:
            messagebox.showerror("No port", "Select a serial port first.")
            return
        baud = int(self.baud_var.get())
        try:
            self.ser = serial.Serial(port=port, baudrate=baud, timeout=0.1)
        except Exception as e:
            self.log_line(f"[ERROR] Connection failed: {e}")
            messagebox.showerror("Connection failed", str(e))
            return
        time.sleep(0.5)
        self.stop_event.clear()
        self.ok_event.clear()
        self.reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self.reader_thread.start()
        self.log_line(f"[HOST] Connected to {port} @ {baud}")
        self.update_connection_status_icon(True)

        # Check logs after hardware has time to initialize (3 seconds)
        # This gives the startup sequence time to complete before checking positions
        self.root.after(3000, self.check_last_position_log)

        threading.Thread(target=self._run_startup_sequence, daemon=True).start()

    def _get_live_coordinates(self, timeout=3.0):
        """
        Send M114 command and get live coordinates from the machine.
        Returns (x, y, z) as floats or (None, None, None) if failed/timeout.
        """
        if not self.ser or not self.ser.is_open:
            return None, None, None
        
        # Use a local event to wait for response
        response_event = threading.Event()
        result = {'x': None, 'y': None, 'z': None}
        
        def parse_response(line):
            match = re.search(r"X:([0-9.-]+)\s*Y:([0-9.-]+)\s*Z:([0-9.-]+)", line)
            if match:
                result['x'] = float(match.group(1))
                result['y'] = float(match.group(2))
                result['z'] = float(match.group(3))
                response_event.set()
        
        # Temporarily add a custom parser for this request
        original_parse = self._parse_coordinates
        self._parse_coordinates = parse_response
        
        try:
            self._send_raw("M114\n")
            # Wait for response with timeout
            if response_event.wait(timeout=timeout):
                return result['x'], result['y'], result['z']
            else:
                return None, None, None
        except Exception as e:
            print(f"Error getting live coordinates: {e}")
            return None, None, None
        finally:
            # Restore original parser
            self._parse_coordinates = original_parse

    def _is_valid_coordinates(self, x, y, z):
        """
        Check if coordinates are valid (not magic numbers).
        Magic numbers indicate machine is off/frozen: X=-13, Y=-15, Z=0
        Returns True if coordinates are valid, False if they are magic/unreasonable.
        """
        if x is None or y is None or z is None:
            return False
        # Check for magic numbers (machine off/frozen)
        if int(x) == -13 and int(y) == -15 and int(z) == 0:
            return False
        # Check for other unreasonable values
        # Valid range: X and Y typically -10 to 200, Z typically 0 to 210
        if x < -50 or x > 500 or y < -50 or y > 500 or z < -10 or z > 250:
            return False
        return True

    def check_last_position_log(self):
        """
        Reads the last line of the most recent log file to determine safety.
        Implements a 5-second delay before showing popup to allow hardware sync.
        Only shows popup if machine is not homed (magic number coordinates detected).
        """
        try:
            # Find most recent file
            list_of_files = glob.glob(os.path.join(self.log_dir, "positions-*.txt"))
            if not list_of_files:
                return  # No logs, assume first run

            latest_file = max(list_of_files, key=os.path.getctime)

            last_line = ""
            with open(latest_file, "r") as f:
                lines = f.readlines()
                if lines:
                    last_line = lines[-1]

            if not last_line:
                return

            # Parse X, Y, Z from log
            # Format: HH:MM:SS -> X:0.00 Y:0.00 Z:0.00 Vol:200.0
            match = re.search(r"X:([0-9.-]+)\s*Y:([0-9.-]+)\s*Z:([0-9.-]+)", last_line)
            if not match:
                return

            log_x = float(match.group(1))
            log_y = float(match.group(2))
            log_z = float(match.group(3))

            # CHECK 1: Magic Numbers (Machine Off/Freeze)
            # X -13 Y -15 Z 0
            if int(log_x) == -13 and int(log_y) == -15 and int(log_z) == 0:
                # Machine was off/frozen - need to show popup after delay
                self._show_delayed_home_popup(log_x, log_y, log_z, is_magic_numbers=True)
                return

            # CHECK 2: High Z Danger
            if log_z > 190:
                messagebox.showwarning(
                    "High Z Warning",
                    f"Last known Z position was {log_z:.1f} (High).\n\nPlease move the head down manually before homing to avoid crashing into the top frame."
                )
                return

            # CHECK 3: Position Mismatch (Generic)
            # On startup, machine usually reports 0,0,0 or unknown until homed.
            # Check live coordinates to see if machine is properly initialized
            live_x, live_y, live_z = self._get_live_coordinates(timeout=3.0)
            
            # If live coordinates are valid (not magic numbers), skip popup
            if self._is_valid_coordinates(live_x, live_y, live_z):
                self.log_line("[STARTUP] Live coordinates valid, skipping home popup.")
                return
            
            # Machine appears not homed - show popup after delay
            self._show_delayed_home_popup(log_x, log_y, log_z, is_magic_numbers=False)

        except Exception as e:
            print(f"Error checking log file: {e}")

    def _show_delayed_home_popup(self, log_x, log_y, log_z, is_magic_numbers=False):
        """
        Shows a delayed popup asking user to home the machine.
        Implements 5-second delay to allow hardware to synchronize.
        """
        def show_popup():
            """This runs after the 5-second delay"""
            # Get fresh live coordinates before showing popup
            live_x, live_y, live_z = self._get_live_coordinates(timeout=2.0)
            
            # Check if coordinates are now valid - if so, don't show popup
            if self._is_valid_coordinates(live_x, live_y, live_z):
                self.log_line("[STARTUP] Coordinates now valid, skipping home popup.")
                return
            
            # Show the popup with option to home
            if is_magic_numbers:
                response = messagebox.askyesno(
                    "Startup Check",
                    f"Machine may not be properly homed.\n\n"
                    f"Last log position: X{log_x:.1f} Y{log_y:.1f} Z{log_z:.1f}\n"
                    f"Current position: X{live_x if live_x is not None else '?'} "
                    f"Y{live_y if live_y is not None else '?'} "
                    f"Z{live_z if live_z is not None else '?'}\n\n"
                    f"Do you want to HOME ALL now?"
                )
            else:
                response = messagebox.askyesno(
                    "Position Check",
                    f"Last known position: X{log_x:.1f} Y{log_y:.1f} Z{log_z:.1f}.\n"
                    f"Machine may not be homed.\n\n"
                    f"Do you want to HOME ALL now?"
                )
            
            if response:
                self.send_home("All")
        
        # Schedule popup after 5 seconds (5000ms)
        self.root.after(5000, show_popup)
        self.log_line("[STARTUP] Waiting 5 seconds for hardware sync before showing home prompt...")

    def _run_startup_sequence(self):
        self.last_cmd_var.set("Initializing...")
        self.log_line("[INIT] Waiting for printer boot...")
        time.sleep(2.0)
        self.log_line("[INIT] Sending Setup G-Code...")
        self._send_lines_with_ok(CALIBRATION_SETUP_GCODE)
        self._send_raw("M115\n")
        self.log_line("[INIT] Setup Complete.")
        self.update_last_module("None")
        self.last_cmd_var.set("Idle")

    def disconnect(self):
        self.stop_event.set()
        if self.reader_thread and self.reader_thread.is_alive():
            self.reader_thread.join(timeout=1.0)
        if self.pos_log_thread and self.pos_log_thread.is_alive():
            pass
        if self.ser:
            try:
                self.ser.close()
            except Exception:
                pass
            self.ser = None
        self.update_connection_status_icon(False)
        self.log_line("[HOST] Disconnected")
        self.last_cmd_var.set("Disconnected")

    def _poll_position_loop(self):
        time_since_last_cmd = time.time() - self.last_action_time
        should_poll = (
                not self.is_sequence_running and
                time_since_last_cmd > IDLE_TIMEOUT_BEFORE_POLL
        )
        if self.ser and self.ser.is_open and should_poll:
            if self.ok_event.is_set() or self.rx_queue.empty():
                try:
                    self._send_raw("M114\n")
                except:
                    pass
        self.root.after(POLL_INTERVAL_MS, self._poll_position_loop)

    def _parse_coordinates(self, line):
        match = re.search(r"X:([0-9.-]+)\s*Y:([0-9.-]+)\s*Z:([0-9.-]+)", line)
        if match:
            x, y, z = match.groups()
            self.coord_x_var.set(x)
            self.coord_y_var.set(y)
            self.coord_z_var.set(z)
            try:
                self.current_x = float(x)
                self.current_y = float(y)
                self.current_z = float(z)
            except ValueError:
                pass

    def _reader_loop(self):
        buffer = b""
        while not self.stop_event.is_set():
            if not self.ser or not self.ser.is_open:
                break
            try:
                chunk = self.ser.read(256)
                if chunk:
                    buffer += chunk
                    while b"\n" in buffer:
                        line, buffer = buffer.split(b"\n", 1)
                        text = line.decode("utf-8", errors="replace").strip()
                        if not text: continue
                        if "echo:busy" in text: continue
                        is_ok = text.lower().startswith("ok")
                        is_position = ("X:" in text and "Y:" in text and "Z:" in text)
                        if is_position:
                            self._parse_coordinates(text)
                        elif is_ok:
                            pass
                        else:
                            self.rx_queue.put(f"[PRINTER] {text}")
                        if is_ok:
                            self.ok_event.set()
                else:
                    time.sleep(0.01)
            except Exception as e:
                self.rx_queue.put(f"[HOST] Serial read error: {e}")
                break

    def _poll_rx_queue(self):
        try:
            while True:
                msg = self.rx_queue.get_nowait()
                self.log_line(msg)
        except queue.Empty:
            pass
        self.root.after(50, self._poll_rx_queue)

    def _send_raw(self, data: str):
        with self.serial_lock:
            if self.ser and self.ser.is_open:
                self.ser.write(data.encode("utf-8", errors="replace"))
                self.ser.flush()

    # ==========================================
    #           PAUSE / ABORT LOGIC
    # ==========================================

    def toggle_pause(self):
        self.is_paused = not self.is_paused
        status = "RESUME" if self.is_paused else "PAUSE"
        self.transfer_pause_btn.config(text=status)
        self.combine_pause_btn.config(text=status)
        self.aliquot_pause_btn.config(text=status)
        self.dilution_pause_btn.config(text=status)  # <-- ADD THIS LINE

        if self.is_paused:
            self.log_line("[USER] Paused sequence.")
            self.last_cmd_var.set("PAUSED")
            self.abort_btn.config(state="normal")
        else:
            self.log_line("[USER] Resumed sequence.")
            self.last_cmd_var.set("Resuming...")
            self.abort_btn.config(state="disabled")

    def send_resume(self):
        # This is for the manual resume button in maintenance tab,
        # but also serves to unpause the script logic.
        if self.is_paused:
            self.toggle_pause()
        # Also send M108 in case the firmware is blocking
        if self.ser and self.ser.is_open:
            self._send_raw("M108\n")

    def abort_sequence(self):
        if not self.is_sequence_running:
            self.log_line("[USER] Abort clicked, but no sequence running.")
            # Still run eject/park just in case
            threading.Thread(target=self._emergency_park, daemon=True).start()
            return

        self.log_line("[USER] ABORTING SEQUENCE!")
        self.is_aborted = True
        self.is_paused = False  # Unpause so loop breaks and exception raises

        # MODIFIED: Disable button immediately and reset UI text
        self.abort_btn.config(state="disabled")
        self.transfer_pause_btn.config(text="PAUSE")
        self.combine_pause_btn.config(text="PAUSE")

        # The running thread will catch SequenceAbortedError and exit.
        # We start a separate thread to handle the cleanup (Eject + Park)
        threading.Thread(target=self._emergency_park, daemon=True).start()

    def _emergency_park(self):
        # Wait a moment for the main sequence thread to die/release lock
        time.sleep(5.0)
        self.log_line("[ABORT] Running Emergency Eject & Park...")

        # Reset flags for the new cleanup sequence
        self.is_aborted = False
        self.is_paused = False

        # Run Eject (which usually parks)
        try:
            self.eject_tip_sequence()
        except:
            pass

        # MODIFIED: Explicitly ensure head is parked at the defined parking space
        # in case eject failed or didn't complete the park move.
        try:
            self.park_head_sequence()
        except:
            pass

    def _send_lines_with_ok(self, lines):
        self.is_sequence_running = True
        self.last_action_time = time.time()
        try:
            for line in lines:
                # --- ABORT CHECK ---
                if self.is_aborted:
                    raise SequenceAbortedError("User Aborted")

                # --- PAUSE CHECK ---
                while self.is_paused:
                    time.sleep(0.1)
                    if self.is_aborted:
                        raise SequenceAbortedError("User Aborted")

                self.ok_event.clear()
                try:
                    self.rx_queue.put(f"[HOST] >> {line}")
                    self._send_raw(line + "\n")
                    self.last_action_time = time.time()
                except Exception as e:
                    self.rx_queue.put(f"[HOST] Send error: {e}")
                    return

                current_timeout = 60.0
                cmd_upper = line.upper()
                if "G28" in cmd_upper or "G29" in cmd_upper:
                    current_timeout = 200.0

                ok = self.ok_event.wait(timeout=current_timeout)
                if not ok:
                    self.rx_queue.put(f"[HOST] Error: Timeout waiting for 'ok' on: {line}")
                    self.rx_queue.put("[HOST] Stopping sequence to prevent crash.")
                    return
        except SequenceAbortedError:
            self.rx_queue.put("[HOST] Sequence Aborted by User.")
            return  # Exit immediately
        finally:
            self.is_sequence_running = False
            self.last_action_time = time.time()
            # Don't log "Complete" if aborted
            if not self.is_aborted:
                self.rx_queue.put("[HOST] Sequence Complete")

    def _wait_for_finish(self):
        if not self.ser or not self.ser.is_open: return
        self._send_lines_with_ok(["M400"])

    def update_last_module(self, name):
        self.last_known_module = name
        self.module_hover_var.set(name)

    # ==========================================
    #           COORDINATE MATH
    # ==========================================

    def resolve_coords(self, rel_x, rel_y, rel_z=None):
        abs_x = CALIBRATION_PIN_CONFIG["PIN_X"] + rel_x
        abs_y = CALIBRATION_PIN_CONFIG["PIN_Y"] + rel_y
        abs_z = None
        if rel_z is not None:
            abs_z = CALIBRATION_PIN_CONFIG["PIN_Z"] + rel_z
            return abs_x, abs_y, abs_z
        return abs_x, abs_y

    def _get_interpolated_coords(self, col_idx, row_idx, num_cols, num_rows, start_x, start_y, end_x, end_y):
        if num_cols > 1:
            x_step = (end_x - start_x) / (num_cols - 1)
            x_pos = start_x + (col_idx * x_step)
        else:
            x_pos = start_x
        if num_rows > 1:
            y_step = (end_y - start_y) / (num_rows - 1)
            y_pos = start_y + (row_idx * y_step)
        else:
            y_pos = start_y
        return x_pos, y_pos

    def _parse_combo_string(self, combo_str):
        parts = combo_str.split(" ", 1)
        if len(parts) == 1:
            return "Unknown", parts[0]
        prefix = parts[0]
        suffix = parts[1]
        if combo_str.startswith("96Well"): return "PLATE", combo_str.replace("96Well ", "")
        if combo_str.startswith("Filter Eppi"): return "FILTER_EPPI", combo_str.replace("Filter Eppi ", "")
        if combo_str.startswith("Eppi"): return "EPPI", combo_str.replace("Eppi ", "")
        if combo_str.startswith("HPLC Insert"): return "HPLC_INSERT", combo_str.replace("HPLC Insert ", "")
        if combo_str.startswith("HPLC"): return "HPLC", combo_str.replace("HPLC ", "")
        if combo_str.startswith("Screwcap"): return "SCREWCAP", combo_str.replace("Screwcap ", "")
        if combo_str.startswith("4mL"): return "4ML", combo_str.replace("4mL ", "")
        if combo_str.startswith("Falcon"): return "FALCON", combo_str.replace("Falcon ", "")
        if combo_str.startswith("Wash") or combo_str.startswith("Waste"): return "WASH", combo_str
        if combo_str.startswith("PLATE"): return "PLATE", combo_str.replace("PLATE ", "")
        return "Unknown", combo_str

    def _construct_combo_string(self, mod_name, pos_name):
        if mod_name == "96 Well Plate": return f"PLATE {pos_name}"
        if mod_name == "Falcon Rack": return f"Falcon {pos_name}"
        if mod_name == "4mL Rack": return f"4mL {pos_name}"
        if mod_name == "Filter Eppi": return f"Filter Eppi {pos_name}"
        if mod_name == "Eppi Rack": return f"Eppi {pos_name}"
        if mod_name == "HPLC Vial": return f"HPLC {pos_name}"
        if mod_name == "HPLC Insert": return f"HPLC Insert {pos_name}"
        if mod_name == "Screwcap Vial": return f"Screwcap {pos_name}"
        if mod_name == "Wash Station": return pos_name
        return f"{mod_name} {pos_name}"

    def get_coords_from_combo(self, combo_str):
        mod_name, pos_key = self._parse_combo_string(combo_str)
        x, y = 0.0, 0.0
        rel_safe_z = 0.0
        rel_asp_z = 0.0
        rel_disp_z = 0.0

        if mod_name == "FALCON":
            x, y = self.get_falcon_coordinates(pos_key)
            rel_safe_z = FALCON_RACK_CONFIG["Z_SAFE"]
            rel_asp_z = FALCON_RACK_CONFIG["Z_ASPIRATE"]
            rel_disp_z = FALCON_RACK_CONFIG["Z_DISPENSE"]
        elif mod_name == "WASH":
            x, y = self.get_wash_coordinates(pos_key)
            rel_safe_z = WASH_RACK_CONFIG["Z_SAFE"]
            rel_asp_z = WASH_RACK_CONFIG["Z_ASPIRATE"]
            rel_disp_z = WASH_RACK_CONFIG["Z_DISPENSE"]
        elif mod_name == "4ML":
            x, y = self.get_4ml_coordinates(pos_key)
            rel_safe_z = _4ML_RACK_CONFIG["Z_SAFE"]
            rel_asp_z = _4ML_RACK_CONFIG["Z_ASPIRATE"]
            rel_disp_z = _4ML_RACK_CONFIG["Z_DISPENSE"]
        elif mod_name == "FILTER_EPPI":
            x, y = self.get_1x8_rack_coordinates(pos_key, FILTER_EPPI_RACK_CONFIG, "B")
            rel_safe_z = FILTER_EPPI_RACK_CONFIG["Z_SAFE"]
            rel_asp_z = FILTER_EPPI_RACK_CONFIG["Z_ASPIRATE"]
            rel_disp_z = FILTER_EPPI_RACK_CONFIG["Z_DISPENSE"]
        elif mod_name == "EPPI":
            x, y = self.get_1x8_rack_coordinates(pos_key, EPPI_RACK_CONFIG, "C")
            rel_safe_z = EPPI_RACK_CONFIG["Z_SAFE"]
            rel_asp_z = EPPI_RACK_CONFIG["Z_ASPIRATE"]
            rel_disp_z = EPPI_RACK_CONFIG["Z_DISPENSE"]
        elif mod_name == "HPLC":
            x, y = self.get_1x8_rack_coordinates(pos_key, HPLC_VIAL_RACK_CONFIG, "D")
            rel_safe_z = HPLC_VIAL_RACK_CONFIG["Z_SAFE"]
            rel_asp_z = HPLC_VIAL_RACK_CONFIG["Z_ASPIRATE"]
            rel_disp_z = HPLC_VIAL_RACK_CONFIG["Z_DISPENSE"]
        elif mod_name == "HPLC_INSERT":
            x, y = self.get_1x8_rack_coordinates(pos_key, HPLC_VIAL_INSERT_RACK_CONFIG, "E")
            rel_safe_z = HPLC_VIAL_INSERT_RACK_CONFIG["Z_SAFE"]
            rel_asp_z = HPLC_VIAL_INSERT_RACK_CONFIG["Z_ASPIRATE"]
            rel_disp_z = HPLC_VIAL_INSERT_RACK_CONFIG["Z_DISPENSE"]
        elif mod_name == "SCREWCAP":
            x, y = self.get_1x8_rack_coordinates(pos_key, SCREWCAP_VIAL_RACK_CONFIG, "F")
            rel_safe_z = SCREWCAP_VIAL_RACK_CONFIG["Z_SAFE"]
            rel_asp_z = SCREWCAP_VIAL_RACK_CONFIG["Z_ASPIRATE"]
            rel_disp_z = SCREWCAP_VIAL_RACK_CONFIG["Z_DISPENSE"]
        elif mod_name == "PLATE":
            x, y = self.get_well_coordinates(pos_key)
            rel_safe_z = PLATE_CONFIG["Z_SAFE"]
            rel_asp_z = PLATE_CONFIG["Z_ASPIRATE"]
            rel_disp_z = PLATE_CONFIG["Z_DISPENSE"]

        abs_safe_z = self.resolve_coords(0, 0, rel_safe_z)[2]
        abs_asp_z = self.resolve_coords(0, 0, rel_asp_z)[2]
        abs_disp_z = self.resolve_coords(0, 0, rel_disp_z)[2]
        return mod_name, x, y, abs_safe_z, abs_asp_z, abs_disp_z

    def get_tip_coordinates(self, tip_key):
        row_char = tip_key[0]
        col_num = int(tip_key[1])
        row_idx = self.tip_rows.index(row_char)
        col_idx = col_num - 1
        rx, ry = self._get_interpolated_coords(col_idx, row_idx, 4, 6, TIP_RACK_CONFIG["A1_X"], TIP_RACK_CONFIG["A1_Y"],
                                               TIP_RACK_CONFIG["F4_X"], TIP_RACK_CONFIG["F4_Y"])
        return self.resolve_coords(rx, ry)

    def get_well_coordinates(self, well_key):
        row_char = well_key[0]
        col_num = int(well_key[1:])
        row_idx = self.plate_rows.index(row_char)
        col_idx = col_num - 1
        rx, ry = self._get_interpolated_coords(col_idx, row_idx, 12, 8, PLATE_CONFIG["A1_X"], PLATE_CONFIG["A1_Y"],
                                               PLATE_CONFIG["H12_X"], PLATE_CONFIG["H12_Y"])
        return self.resolve_coords(rx, ry)

    def get_falcon_coordinates(self, falcon_key):
        if falcon_key == "50mL":
            return self.resolve_coords(FALCON_RACK_CONFIG["50ML_X"], FALCON_RACK_CONFIG["50ML_Y"])
        row_char = falcon_key[0]
        col_num = int(falcon_key[1:])
        falcon_rows = ["A", "B"]
        if row_char not in falcon_rows: return 0.0, 0.0
        row_idx = falcon_rows.index(row_char)
        col_idx = col_num - 1
        rx, ry = self._get_interpolated_coords(col_idx, row_idx, 3, 2, FALCON_RACK_CONFIG["15ML_A1_X"],
                                               FALCON_RACK_CONFIG["15ML_A1_Y"], FALCON_RACK_CONFIG["15ML_B3_X"],
                                               FALCON_RACK_CONFIG["15ML_B3_Y"])
        return self.resolve_coords(rx, ry)

    def get_wash_coordinates(self, wash_name):
        mapping = {"Wash A": (0, 0), "Wash B": (1, 0), "Wash C": (0, 1), "Trash": (1, 1)}
        col_idx, row_idx = mapping.get(wash_name, (0, 0))
        rx, ry = self._get_interpolated_coords(col_idx, row_idx, 2, 2, WASH_RACK_CONFIG["A1_X"],
                                               WASH_RACK_CONFIG["A1_Y"],
                                               WASH_RACK_CONFIG["B2_X"], WASH_RACK_CONFIG["B2_Y"])
        return self.resolve_coords(rx, ry)

    def get_4ml_coordinates(self, key):
        if not key.startswith("A"): return 0.0, 0.0
        col_num = int(key[1:])
        col_idx = col_num - 1
        rx, ry = self._get_interpolated_coords(col_idx, 0, 8, 1, _4ML_RACK_CONFIG["A1_X"], _4ML_RACK_CONFIG["A1_Y"],
                                               _4ML_RACK_CONFIG["A8_X"], _4ML_RACK_CONFIG["A8_Y"])
        return self.resolve_coords(rx, ry)

    def get_1x8_rack_coordinates(self, key, config, row_char):
        if not key.startswith(row_char): return 0.0, 0.0
        try:
            col_num = int(key[1:])
        except ValueError:
            return 0.0, 0.0
        col_idx = col_num - 1
        start_x = config[f"{row_char}1_X"]
        start_y = config[f"{row_char}1_Y"]
        end_x = config[f"{row_char}8_X"]
        end_y = config[f"{row_char}8_Y"]
        rx, ry = self._get_interpolated_coords(col_idx, 0, 8, 1, start_x, start_y, end_x, end_y)
        return self.resolve_coords(rx, ry)

    # ==========================================
    #           TIP INVENTORY LOGIC
    # ==========================================

    def toggle_tip_state(self, key):
        self.tip_inventory[key] = not self.tip_inventory[key]
        self.update_tip_grid_colors()
        self.update_available_tips_combo()

    def reset_all_tips_fresh(self):
        for k in self.tip_inventory: self.tip_inventory[k] = True
        self.update_tip_grid_colors()
        self.update_available_tips_combo()

    def reset_all_tips_empty(self):
        for k in self.tip_inventory: self.tip_inventory[k] = False
        self.update_tip_grid_colors()
        self.update_available_tips_combo()

    def update_tip_grid_colors(self):
        for key, btn in self.tip_buttons.items():
            is_fresh = self.tip_inventory[key]
            bg_color = "#90ee90" if is_fresh else "#ffcccb"
            btn.configure(bg=bg_color)

    def update_available_tips_combo(self):
        available = [k for k in self.tip_inventory if self.tip_inventory[k]]
        available.sort()
        self.modules["TIPS"]["values"] = available
        self.modules["TIPS"]["var"].set(available[0] if available else "EMPTY")

    def _find_next_available_tip(self):
        for r in self.tip_rows:
            for c in self.tip_cols:
                key = f"{r}{c}"
                if self.tip_inventory[key]:
                    return key
        return None

    # ==========================================
    #           MOVEMENT COMMANDS
    # ==========================================

    def send_jog(self, axis, direction_sign):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        self.update_last_module("JOG")
        distance = self.step_size_var.get()
        move_val = distance * direction_sign
        feed_rate = JOG_SPEED_Z if axis == 'Z' else JOG_SPEED_XY
        commands = ["G91", f"G0 {axis}{move_val} F{feed_rate}", "G90"]
        self.log_line(f"[MANUAL] Jog {axis} {move_val}mm")

        def run_seq():
            self.last_cmd_var.set(f"Jogging {axis}...")
            self._send_lines_with_ok(commands)
            self._wait_for_finish()
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def _get_park_head_commands(self):
        abs_park_x, abs_park_y, abs_park_z = self.resolve_coords(PARK_HEAD_X, PARK_HEAD_Y, PARK_HEAD_Z)
        _, _, abs_global_safe_z = self.resolve_coords(0, 0, GLOBAL_SAFE_Z_OFFSET)

        return [
            "G90",
            f"G0 Z{abs_global_safe_z:.2f} F{JOG_SPEED_Z}",
            f"G0 X{abs_park_x:.2f} Y{abs_park_y:.2f} F{JOG_SPEED_XY}",
            f"G0 Z{abs_park_z:.2f} F{JOG_SPEED_Z}"
        ]

    def send_home(self, axes):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        self.update_last_module("HOME")
        axes = axes.upper()
        if axes == "ALL":
            home_cmd = "G28"
        else:
            home_cmd = f"G28 {' '.join(list(axes))}"

        commands = [home_cmd, "M18 E"]
        commands.extend(self._get_park_head_commands())

        self.log_line(f"[MANUAL] Homing {axes} and Parking...")

        def run_seq():
            self.last_cmd_var.set(f"Homing {axes}...")
            self._send_lines_with_ok(commands)
            self._wait_for_finish()
            self.update_last_module("PARK")
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def park_head_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        self.log_line("[SYSTEM] Parking Head...")
        self.log_command("Park Head (Defined Coordinates)")
        commands = self._get_park_head_commands()

        def run_seq():
            self.last_cmd_var.set("Parking Head...")
            self._send_lines_with_ok(commands)
            self._wait_for_finish()
            self.update_last_module("PARK")
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def send_raw_gcode_command(self):
        cmd = self.raw_gcode_var.get().strip()
        if not cmd: return
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        self.update_last_module("RAW_GCODE")

        def run_seq():
            self.last_cmd_var.set(f"Running G-Code: {cmd}")
            self._send_lines_with_ok([cmd])
            self._wait_for_finish()
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def manual_pipette_move(self, mode):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        try:
            delta_ul = float(self.pipette_move_var.get())
            if delta_ul <= 0: raise ValueError
        except ValueError:
            messagebox.showerror("Input Error", "Please enter a positive number for volume.")
            return
        if mode == "aspirate":
            new_vol = self.current_pipette_volume + delta_ul
        else:
            new_vol = self.current_pipette_volume - delta_ul
        if new_vol > MAX_PIPETTE_VOL or new_vol < MIN_PIPETTE_VOL:
            messagebox.showwarning("Range Error", f"Limits: {MIN_PIPETTE_VOL}-{MAX_PIPETTE_VOL} uL.")
            return
        target_e_pos = -1 * new_vol * STEPS_PER_UL
        self.log_line(f"[PIP] {mode.upper()}: {self.current_pipette_volume} -> {new_vol} uL")
        self.log_command(f"Pipette {mode}: {delta_ul}uL")
        commands = ["G90", f"G1 E{target_e_pos:.3f} F{PIP_SPEED}", "M18 E"]

        def run_seq():
            self.last_cmd_var.set(f"Pipette: {mode.title()}...")
            self._send_lines_with_ok(commands)
            self._wait_for_finish()
            self.current_pipette_volume = new_vol
            self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")
            self.live_vol_var.set(f"{self.current_pipette_volume:.1f}")
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def smart_pipette_sequence(self, mode):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        if not self.last_known_module:
            messagebox.showerror("Unknown Position", "Move to a module first.")
            return
        if self.last_known_module in ["TIPS", "EJECT", "JOG", "HOME", "RAW_GCODE", "Unknown"]:
            messagebox.showerror("Unsafe Action", "Cannot pipette here.")
            return
        config_map = {
            "PLATE": PLATE_CONFIG, "FALCON": FALCON_RACK_CONFIG, "WASH": WASH_RACK_CONFIG,
            "4ML": _4ML_RACK_CONFIG, "FILTER_EPPI": FILTER_EPPI_RACK_CONFIG,
            "EPPI": EPPI_RACK_CONFIG, "HPLC": HPLC_VIAL_RACK_CONFIG,
            "HPLC_INSERT": HPLC_VIAL_INSERT_RACK_CONFIG, "SCREWCAP": SCREWCAP_VIAL_RACK_CONFIG
        }
        cfg = config_map.get(self.last_known_module)
        if not cfg:
            messagebox.showerror("Error", f"No config for {self.last_known_module}")
            return
        rel_z_action = cfg["Z_ASPIRATE"] if mode == "aspirate" else cfg["Z_DISPENSE"]
        abs_z_action = self.resolve_coords(0, 0, rel_z_action)[2]
        rel_z_safe = cfg["Z_SAFE"]
        abs_z_safe = self.resolve_coords(0, 0, rel_z_safe)[2]
        try:
            delta_ul = float(self.pipette_move_var.get())
            if delta_ul <= 0: raise ValueError
        except ValueError:
            messagebox.showerror("Input Error", "Positive volume required.")
            return
        if mode == "aspirate":
            new_vol = self.current_pipette_volume + delta_ul
        else:
            new_vol = self.current_pipette_volume - delta_ul
        if new_vol > MAX_PIPETTE_VOL or new_vol < MIN_PIPETTE_VOL:
            messagebox.showwarning("Range Error", f"Limits: {MIN_PIPETTE_VOL}-{MAX_PIPETTE_VOL} uL.")
            return
        target_e_pos = -1 * new_vol * STEPS_PER_UL
        self.log_line(f"[SMART] {mode.upper()} at {self.last_known_module} (Z={abs_z_action:.2f})")
        self.log_command(f"Smart {mode}: {delta_ul}uL @ {self.last_known_module}")
        commands = [
            "G90",
            f"G0 Z{abs_z_action:.2f} F{JOG_SPEED_Z}",
            f"G1 E{target_e_pos:.3f} F{PIP_SPEED}",
            f"G0 Z{abs_z_safe:.2f} F{JOG_SPEED_Z}",
            "M18 E"
        ]

        def run_seq():
            self.last_cmd_var.set(f"Smart {mode.title()}...")
            self._send_lines_with_ok(commands)
            self._wait_for_finish()
            self.current_pipette_volume = new_vol
            self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")
            self.live_vol_var.set(f"{self.current_pipette_volume:.1f}")
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def _get_mix_commands(self, cfg):
        abs_z_aspirate = self.resolve_coords(0, 0, cfg["Z_ASPIRATE"])[2]
        abs_z_dispense = self.resolve_coords(0, 0, cfg["Z_DISPENSE"])[2]
        abs_z_safe = self.resolve_coords(0, 0, cfg["Z_SAFE"])[2]
        vol_start = 200.0
        e_pos_start = -1 * vol_start * STEPS_PER_UL
        vol_after_asp = vol_start + 800.0
        e_pos_asp = -1 * vol_after_asp * STEPS_PER_UL
        vol_after_disp = vol_after_asp - 900.0
        e_pos_disp = -1 * vol_after_disp * STEPS_PER_UL
        commands = [
            "G90",
            f"G1 E{e_pos_start:.3f} F{PIP_SPEED}",
            f"G0 Z{abs_z_aspirate:.2f} F{JOG_SPEED_Z}",
            f"G1 E{e_pos_asp:.3f} F{PIP_SPEED}",
            f"G0 Z{abs_z_dispense:.2f} F{JOG_SPEED_Z}",
            f"G1 E{e_pos_disp:.3f} F{PIP_SPEED}",
            f"G0 Z{abs_z_safe:.2f} F{JOG_SPEED_Z}",
            "M18 E"
        ]
        return commands, vol_after_disp

    def mix_well_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        if not self.last_known_module:
            messagebox.showerror("Unknown Position", "Move to a module first.")
            return
        if self.last_known_module in ["TIPS", "EJECT", "JOG", "HOME", "RAW_GCODE", "Unknown"]:
            messagebox.showerror("Unsafe Action", "Cannot mix here.")
            return
        config_map = {
            "PLATE": PLATE_CONFIG, "FALCON": FALCON_RACK_CONFIG, "WASH": WASH_RACK_CONFIG,
            "4ML": _4ML_RACK_CONFIG, "FILTER_EPPI": FILTER_EPPI_RACK_CONFIG,
            "EPPI": EPPI_RACK_CONFIG, "HPLC": HPLC_VIAL_RACK_CONFIG,
            "HPLC_INSERT": HPLC_VIAL_INSERT_RACK_CONFIG, "SCREWCAP": SCREWCAP_VIAL_RACK_CONFIG
        }
        cfg = config_map.get(self.last_known_module)
        if not cfg:
            messagebox.showerror("Error", f"No config for {self.last_known_module}")
            return
        self.log_line(f"[MIX] Mixing at {self.last_known_module}...")
        self.log_command(f"Mix Well (200->1000->100) @ {self.last_known_module}")
        commands, final_vol = self._get_mix_commands(cfg)

        def run_seq():
            self.last_cmd_var.set("Mixing Well...")
            self._send_lines_with_ok(commands)
            self._wait_for_finish()
            self.current_pipette_volume = final_vol
            self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")
            self.live_vol_var.set(f"{self.current_pipette_volume:.1f}")
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def _get_smart_travel_gcode(self, target_module, target_x, target_y, module_abs_safe_z, start_module=None):
        global_safe_z = self.resolve_coords(0, 0, GLOBAL_SAFE_Z_OFFSET)[2]
        current_mod = start_module if start_module is not None else self.last_known_module
        use_optimized_z = (
                current_mod in SMALL_VIAL_MODULES and
                target_module in SMALL_VIAL_MODULES
        )
        if use_optimized_z:
            travel_z = self.resolve_coords(0, 0, _4ML_RACK_CONFIG["Z_SAFE"])[2]
        else:
            travel_z = global_safe_z

        cmds = ["G90"]
        if current_mod == target_module and current_mod is not None:
            cmds.append(f"G0 Z{module_abs_safe_z:.2f} F{JOG_SPEED_Z}")
            cmds.append(f"G0 X{target_x:.2f} Y{target_y:.2f} F{JOG_SPEED_XY}")
        else:
            cmds.append(f"G0 Z{travel_z:.2f} F{JOG_SPEED_Z}")
            cmds.append(f"G0 X{target_x:.2f} Y{target_y:.2f} F{JOG_SPEED_XY}")
            cmds.append(f"G0 Z{module_abs_safe_z:.2f} F{JOG_SPEED_Z}")
        return cmds

    def _get_pick_tip_commands(self, tip_key, start_module=None):
        tx, ty = self.get_tip_coordinates(tip_key)
        abs_rack_safe_z = self.resolve_coords(0, 0, TIP_RACK_CONFIG["Z_TRAVEL"])[2]
        abs_pick_z = self.resolve_coords(0, 0, TIP_RACK_CONFIG["Z_PICK"])[2]
        commands = self._get_smart_travel_gcode("TIPS", tx, ty, abs_rack_safe_z, start_module=start_module)
        commands.extend([f"G0 Z{abs_pick_z:.2f} F500", f"G0 Z{abs_rack_safe_z:.2f} F{JOG_SPEED_Z}"])
        return commands

    def _get_eject_tip_commands(self):
        cfg = EJECT_STATION_CONFIG
        abs_app_x, abs_app_y, abs_safe_z = self.resolve_coords(cfg["APPROACH_X"], cfg["APPROACH_Y"], cfg["Z_SAFE"])
        abs_eject_start_z = self.resolve_coords(0, 0, cfg["Z_EJECT_START"])[2]
        abs_target_y = self.resolve_coords(0, cfg["EJECT_TARGET_Y"])[1]
        abs_retract_z = self.resolve_coords(0, 0, cfg["Z_RETRACT"])[2]
        abs_center_x, abs_center_y, abs_center_z = self.resolve_coords(SAFE_CENTER_X_OFFSET, SAFE_CENTER_Y_OFFSET,
                                                                       GLOBAL_SAFE_Z_OFFSET)
        commands = ["G90"]
        commands.append(f"G0 Z{abs_center_z:.2f} F{JOG_SPEED_Z}")
        commands.append(f"G0 X{abs_center_x:.2f} Y{abs_center_y:.2f} F{JOG_SPEED_XY}")
        commands.append(f"G0 X{abs_app_x:.2f} Y{abs_app_y:.2f} F{JOG_SPEED_XY}")
        commands.append(f"G0 Z{abs_safe_z:.2f} F{JOG_SPEED_Z}")
        commands.append(f"G0 Z{abs_eject_start_z:.2f} F{JOG_SPEED_Z}")
        commands.append(f"G0 Y{abs_target_y:.2f} F800")
        commands.append(f"G0 Z{abs_retract_z:.2f} F250")
        commands.append(f"G0 Z{abs_center_z:.2f} F{JOG_SPEED_Z}")
        return commands

    def eject_tip_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        self.log_line("[SYSTEM] Ejecting Tip...")
        self.log_command("Ejecting Tip")
        commands = self._get_eject_tip_commands()
        commands.extend(self._get_park_head_commands())

        def run_seq():
            self.last_cmd_var.set("Ejecting Tip...")
            self._send_lines_with_ok(commands)
            self._wait_for_finish()
            self.update_last_module("PARK")
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def pick_tip_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        target_tip = self.modules["TIPS"]["var"].get()
        if not target_tip or target_tip == "EMPTY":
            messagebox.showwarning("No Tip", "No fresh tips available or selected.")
            return
        self.log_line(f"[SYSTEM] Picking Tip {target_tip}...")
        self.log_command(f"Pick Tip: {target_tip}")
        commands = self._get_pick_tip_commands(target_tip)

        def run_seq():
            self.last_cmd_var.set(f"Picking Tip: {target_tip}...")
            self._send_lines_with_ok(commands)
            self._wait_for_finish()
            self.update_last_module("TIPS")
            self.tip_inventory[target_tip] = False
            self.root.after(0, self.update_tip_grid_colors)
            self.root.after(0, self.update_available_tips_combo)
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    # ==========================================
    #           TRANSFER LIQUID LOGIC
    # ==========================================

    def _ordered_group_by(self, items, key_fn):
        groups = {}
        order = []
        for it in items:
            k = key_fn(it)
            if k not in groups:
                groups[k] = []
                order.append(k)
            groups[k].append(it)
        return [(k, groups[k]) for k in order]

    def _perform_batch_wash_distribution(
            self,
            wash_src_str: str,
            tasks_for_this_wash: list,
            e_gap_pos: float,
            air_gap_ul: float,
            max_liquid_ul: float = 800.0,
            start_module: str | None = None
    ):
        if not wash_src_str:
            self.log_line("[WASH-BATCH] ERROR: wash_src_str is empty.")
            return start_module if start_module is not None else self.last_known_module

        if not tasks_for_this_wash:
            return start_module if start_module is not None else self.last_known_module

        w_mod, w_x, w_y, w_safe_z, w_asp_z, _ = self.get_coords_from_combo(wash_src_str)
        plan = []
        for t in tasks_for_this_wash:
            try:
                need = float(t.get("wash_vol", 0.0))
            except (TypeError, ValueError):
                need = 0.0
            if need <= 0:
                continue
            s_mod, s_x, s_y, s_safe_z, _, s_disp_z = self.get_coords_from_combo(t["source"])
            plan.append({
                "line": t["line"],
                "need_ul": need,
                "source": t["source"],
                "s_mod": s_mod, "s_x": s_x, "s_y": s_y,
                "s_safe_z": s_safe_z, "s_disp_z": s_disp_z,
            })

        if not plan:
            return start_module if start_module is not None else self.last_known_module

        remaining = [p["need_ul"] for p in plan]
        current_mod = start_module if start_module is not None else self.last_known_module

        while True:
            total_remaining = sum(remaining)
            if total_remaining <= 0.0001:
                break

            load_ul = min(max_liquid_ul, total_remaining)
            in_tip = load_ul

            self.log_line(
                f"[WASH-BATCH] Loading {load_ul:.1f}uL from '{wash_src_str}' (remaining total {total_remaining:.1f}uL)")

            cmds = []
            cmds.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")
            cmds.extend(self._get_smart_travel_gcode(w_mod, w_x, w_y, w_safe_z, start_module=current_mod))
            cmds.append(f"G0 Z{w_asp_z:.2f} F{JOG_SPEED_Z}")

            e_loaded = -1 * (air_gap_ul + load_ul) * STEPS_PER_UL
            cmds.append(f"G1 E{e_loaded:.3f} F{PIP_SPEED}")
            cmds.append(f"G0 Z{w_safe_z:.2f} F{JOG_SPEED_Z}")

            current_mod = w_mod

            for i, p in enumerate(plan):
                if remaining[i] <= 0.0001:
                    continue
                if in_tip <= 0.0001:
                    break

                disp_ul = min(remaining[i], in_tip)

                self.log_line(f"[WASH-BATCH]  -> L{p['line']} dispense {disp_ul:.1f}uL into {p['source']}")

                cmds.extend(self._get_smart_travel_gcode(p["s_mod"], p["s_x"], p["s_y"], p["s_safe_z"],
                                                         start_module=current_mod))
                cmds.append(f"G0 Z{p['s_disp_z']:.2f} F{JOG_SPEED_Z}")

                in_tip -= disp_ul
                remaining[i] = max(0.0, remaining[i] - disp_ul)

                e_after = -1 * (air_gap_ul + in_tip) * STEPS_PER_UL
                cmds.append(f"G1 E{e_after:.3f} F{PIP_SPEED}")
                cmds.append(f"G0 Z{p['s_safe_z']:.2f} F{JOG_SPEED_Z}")

                current_mod = p["s_mod"]

            self._send_lines_with_ok(cmds)

        self._send_lines_with_ok([f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}"])
        self.current_pipette_volume = air_gap_ul
        self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")
        self.live_vol_var.set(f"{self.current_pipette_volume:.1f}")

        return current_mod

    def _perform_wash_mix_and_transfer(
            self,
            source_str: str,
            dest_str: str,
            wash_vol_ul: float,
            e_gap_pos: float,
            air_gap_ul: float,
            start_module: str | None = None
    ):
        s_mod, s_x, s_y, s_safe_z, s_asp_z, _ = self.get_coords_from_combo(source_str)
        d_mod, d_x, d_y, d_safe_z, _, d_disp_z = self.get_coords_from_combo(dest_str)

        current_mod = start_module if start_module is not None else self.last_known_module
        max_collect_ul = MAX_PIPETTE_VOL - air_gap_ul
        collect_ul = min(float(wash_vol_ul) + 50.0, max_collect_ul)

        mix_vol = 500.0
        mix_vol = min(mix_vol, max_collect_ul)

        e_mix_up = -1 * (air_gap_ul) * STEPS_PER_UL
        e_mix_down = -1 * (air_gap_ul + mix_vol) * STEPS_PER_UL
        e_collect = -1 * (air_gap_ul + collect_ul) * STEPS_PER_UL
        e_blowout = -1 * MIN_PIPETTE_VOL * STEPS_PER_UL

        cmds = []
        cmds.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")

        cmds.extend(self._get_smart_travel_gcode(s_mod, s_x, s_y, s_safe_z, start_module=current_mod))
        cmds.append(f"G0 Z{s_asp_z:.2f} F{JOG_SPEED_Z}")

        for _ in range(2):
            cmds.append(f"G1 E{e_mix_down:.3f} F{PIP_SPEED}")
            cmds.append(f"G1 E{e_mix_up:.3f} F{PIP_SPEED}")

        cmds.append(f"G1 E{e_collect:.3f} F{PIP_SPEED}")
        cmds.append(f"G0 Z{s_safe_z:.2f} F{JOG_SPEED_Z}")

        cmds.extend(self._get_smart_travel_gcode(d_mod, d_x, d_y, d_safe_z, start_module=s_mod))
        cmds.append(f"G0 Z{d_disp_z:.2f} F{JOG_SPEED_Z}")
        cmds.append(f"G1 E{e_blowout:.3f} F{PIP_SPEED}")
        cmds.append(f"G0 Z{d_safe_z:.2f} F{JOG_SPEED_Z}")
        cmds.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")

        self._send_lines_with_ok(cmds)

        self.update_last_module(d_mod)
        self.current_pipette_volume = air_gap_ul
        self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")
        self.live_vol_var.set(f"{self.current_pipette_volume:.1f}")

        return d_mod

    def transfer_liquid_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        tasks = []
        for idx, row in enumerate(self.transfer_rows):
            if not row["execute"].get():
                continue

            try:
                vol = float(row["vol"].get())
                wash_vol = float(row["wash_vol"].get())
                wash_times = int(row["wash_times"].get())
            except (TypeError, ValueError):
                continue

            src_mod_name = row["src_mod"].get()
            src_pos_name = row["src_pos"].get()
            full_source_str = self._construct_combo_string(src_mod_name, src_pos_name)

            dest = row["dest"].get()
            wash_src = row["wash_src"].get()

            if not full_source_str or not dest:
                self.log_line(f"[TRANSFER] Skipping line {idx + 1}: missing source/dest.")
                continue
            if wash_vol > 0 and not wash_src:
                self.log_line(f"[TRANSFER] Skipping line {idx + 1}: wash enabled but wash_src empty.")
                continue

            tasks.append({
                "line": idx + 1,
                "source": full_source_str,
                "dest": dest,
                "vol": vol,
                "volatile": row["volatile"].get(),
                "wash_vol": wash_vol,
                "wash_times": wash_times,
                "wash_src": wash_src,
            })

        if not tasks:
            messagebox.showinfo("No Tasks", "No lines selected for execution.")
            return

        self.log_command(f"[TRANSFER] Starting sequence with {len(tasks)} lines (batched wash enabled).")

        air_gap_ul = float(AIR_GAP_UL)
        e_gap_pos = -1 * air_gap_ul * STEPS_PER_UL
        MAX_STD_BATCH = 800.0
        MAX_VOLATILE_BATCH = 500.0
        MAX_WASH_LOAD = MAX_PIPETTE_VOL - air_gap_ul
        REUSE_DISTRIBUTION_TIP_FOR_ONE_RECOVERY = True

        def run_seq():
            current_simulated_module = self.last_known_module
            self.log_line("[TRANSFER] Ensuring no tip is loaded at start...")
            self._send_lines_with_ok(self._get_eject_tip_commands())
            self.update_last_module("EJECT")
            current_simulated_module = "EJECT"

            for task in tasks:
                line_num = task["line"]
                self.log_line(f"--- PHASE 1: Transfer Line {line_num} ---")
                self.last_cmd_var.set(f"L{line_num}: Transfer...")

                tip_key = self._find_next_available_tip()
                if not tip_key:
                    messagebox.showerror("No Tips", f"Ran out of tips at Line {line_num}.")
                    return

                self.log_line(f"[L{line_num}] Picking Tip {tip_key}...")
                self._send_lines_with_ok(self._get_pick_tip_commands(tip_key, start_module=current_simulated_module))
                self.tip_inventory[tip_key] = False
                self.root.after(0, self.update_tip_grid_colors)
                self.update_last_module("TIPS")
                current_simulated_module = "TIPS"

                total_vol = float(task["vol"])
                is_volatile = bool(task["volatile"])
                max_batch = MAX_VOLATILE_BATCH if is_volatile else MAX_STD_BATCH

                remaining_vol = total_vol
                while remaining_vol > 0.0001:
                    batch_vol = min(remaining_vol, max_batch)
                    self.log_line(
                        f"[L{line_num}] Transfer {batch_vol:.1f}uL ({'Volatile' if is_volatile else 'Standard'})")

                    current_simulated_module = self._perform_single_transfer(
                        task["source"], task["dest"], batch_vol, is_volatile,
                        e_gap_pos, air_gap_ul,
                        start_module=current_simulated_module
                    )
                    remaining_vol -= batch_vol

                self.log_line(f"[L{line_num}] Ejecting transfer tip...")
                self._send_lines_with_ok(self._get_eject_tip_commands())
                self.update_last_module("EJECT")
                current_simulated_module = "EJECT"

            wash_tasks = [t for t in tasks if t["wash_vol"] > 0 and t["wash_times"] > 0]
            if wash_tasks:
                max_cycles = max(int(t["wash_times"]) for t in wash_tasks)

                for cycle_idx in range(1, max_cycles + 1):
                    cycle_tasks = [t for t in wash_tasks if int(t["wash_times"]) >= cycle_idx]
                    if not cycle_tasks:
                        continue

                    self.log_line(f"=== PHASE 2: WASH CYCLE {cycle_idx}/{max_cycles} ===")
                    self.last_cmd_var.set(f"Wash cycle {cycle_idx}/{max_cycles}")

                    for wash_src, group in self._ordered_group_by(cycle_tasks, lambda x: x["wash_src"]):
                        if not wash_src:
                            self.log_line("[WASH] Skipping group: wash_src empty.")
                            continue

                        if len(group) == 1:
                            t = group[0]
                            self.log_line(f"[WASH] L{t['line']}: single-task wash (legacy wash cycle).")
                            current_simulated_module = self._perform_wash_cycle(
                                t["wash_src"], t["source"], t["dest"], float(t["wash_vol"]),
                                bool(t["volatile"]), e_gap_pos, air_gap_ul,
                                start_module=current_simulated_module
                            )
                            current_simulated_module = "EJECT"
                            continue

                        self.log_line(f"[WASH-BATCH] Dispensing wash from '{wash_src}' into {len(group)} sources...")
                        dist_tip = self._find_next_available_tip()
                        if not dist_tip:
                            messagebox.showerror("No Tips",
                                                 f"Ran out of tips during wash distribution (cycle {cycle_idx}).")
                            return

                        self._send_lines_with_ok(
                            self._get_pick_tip_commands(dist_tip, start_module=current_simulated_module))
                        self.tip_inventory[dist_tip] = False
                        self.root.after(0, self.update_tip_grid_colors)
                        self.update_last_module("TIPS")
                        current_simulated_module = "TIPS"

                        current_simulated_module = self._perform_batch_wash_distribution(
                            wash_src_str=wash_src,
                            tasks_for_this_wash=group,
                            e_gap_pos=e_gap_pos,
                            air_gap_ul=air_gap_ul,
                            max_liquid_ul=MAX_WASH_LOAD,
                            start_module=current_simulated_module
                        )

                        if REUSE_DISTRIBUTION_TIP_FOR_ONE_RECOVERY:
                            reuse_task = group[-1]
                            self.log_line(
                                f"[WASH-BATCH] Reusing distribution tip for recovery of L{reuse_task['line']} (tip-neutral mode).")

                            current_simulated_module = self._perform_wash_mix_and_transfer(
                                reuse_task["source"], reuse_task["dest"], float(reuse_task["wash_vol"]),
                                e_gap_pos, air_gap_ul,
                                start_module=current_simulated_module
                            )

                            self.log_line("[WASH-BATCH] Ejecting distribution/recovery tip...")
                            self._send_lines_with_ok(self._get_eject_tip_commands())
                            self.update_last_module("EJECT")
                            current_simulated_module = "EJECT"

                            remaining_recovery = group[:-1]
                        else:
                            self.log_line("[WASH-BATCH] Ejecting distribution tip...")
                            self._send_lines_with_ok(self._get_eject_tip_commands())
                            self.update_last_module("EJECT")
                            current_simulated_module = "EJECT"
                            remaining_recovery = group

                        for t in remaining_recovery:
                            line_num = t["line"]
                            self.log_line(f"[WASH] L{line_num}: mix+transfer wash to dest...")

                            tip_key = self._find_next_available_tip()
                            if not tip_key:
                                messagebox.showerror("No Tips",
                                                     f"Ran out of tips during wash recovery at Line {line_num}.")
                                return

                            self._send_lines_with_ok(
                                self._get_pick_tip_commands(tip_key, start_module=current_simulated_module))
                            self.tip_inventory[tip_key] = False
                            self.root.after(0, self.update_tip_grid_colors)
                            self.update_last_module("TIPS")
                            current_simulated_module = "TIPS"

                            current_simulated_module = self._perform_wash_mix_and_transfer(
                                t["source"], t["dest"], float(t["wash_vol"]),
                                e_gap_pos, air_gap_ul,
                                start_module=current_simulated_module
                            )

                            self.log_line(f"[WASH] L{line_num}: ejecting recovery tip...")
                            self._send_lines_with_ok(self._get_eject_tip_commands())
                            self.update_last_module("EJECT")
                            current_simulated_module = "EJECT"

            self.log_command("[TRANSFER] All lines complete. Parking.")
            self.last_cmd_var.set("Parking...")
            self._send_lines_with_ok(self._get_park_head_commands())
            self.update_last_module("PARK")
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def _perform_single_transfer(self, source_str, dest_str, vol, is_volatile, e_gap_pos, air_gap_ul,
                                 start_module=None):
        src_mod, src_x, src_y, src_safe_z, src_asp_z, _ = self.get_coords_from_combo(source_str)
        dest_mod, dest_x, dest_y, dest_safe_z, _, dest_disp_z = self.get_coords_from_combo(dest_str)
        global_safe_z = self.resolve_coords(0, 0, GLOBAL_SAFE_Z_OFFSET)[2]

        current_mod_tracker = start_module if start_module is not None else self.last_known_module
        use_optimized_z_src = (current_mod_tracker in SMALL_VIAL_MODULES and src_mod in SMALL_VIAL_MODULES)
        travel_z_src = self.resolve_coords(0, 0, _4ML_RACK_CONFIG["Z_SAFE"])[
            2] if use_optimized_z_src else global_safe_z

        cmds = []
        cmds.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")

        if current_mod_tracker == src_mod:
            cmds.append(f"G0 Z{src_safe_z:.2f} F{JOG_SPEED_Z}")
            cmds.append(f"G0 X{src_x:.2f} Y{src_y:.2f} F{JOG_SPEED_XY}")
        else:
            cmds.append(f"G0 Z{travel_z_src:.2f} F{JOG_SPEED_Z}")
            cmds.append(f"G0 X{src_x:.2f} Y{src_y:.2f} F{JOG_SPEED_XY}")
            cmds.append(f"G0 Z{src_safe_z:.2f} F{JOG_SPEED_Z}")

        e_loaded_pos = -1 * (air_gap_ul + vol) * STEPS_PER_UL
        cmds.append(f"G0 Z{src_asp_z:.2f} F{JOG_SPEED_Z}")

        if is_volatile:
            mix_vol = 500.0
            e_mix_down = -1 * (air_gap_ul + mix_vol) * STEPS_PER_UL
            e_mix_up = -1 * (air_gap_ul) * STEPS_PER_UL
            self.log_line(f"[VOLATILE] Pre-wetting/Mixing source 3 times...")
            for _ in range(2):
                cmds.append(f"G1 E{e_mix_down:.3f} F{PIP_SPEED}")
                cmds.append(f"G1 E{e_mix_up:.3f} F{PIP_SPEED}")

        cmds.append(f"G1 E{e_loaded_pos:.3f} F{PIP_SPEED}")
        self._send_lines_with_ok(cmds)

        use_optimized_z_dest = (src_mod in SMALL_VIAL_MODULES and dest_mod in SMALL_VIAL_MODULES)
        travel_z_dest = self.resolve_coords(0, 0, _4ML_RACK_CONFIG["Z_SAFE"])[
            2] if use_optimized_z_dest else global_safe_z

        if is_volatile:
            self.log_line("[VOLATILE] Performing synchronized relative travel...")
            dz_lift = travel_z_dest - src_asp_z
            dx = dest_x - src_x
            dy = dest_y - src_y
            dist_xy = math.sqrt(dx ** 2 + dy ** 2)
            dz_drop = dest_safe_z - travel_z_dest
            drift_per_min = VOLATILE_DRIFT_RATE * STEPS_PER_UL
            t_lift = abs(dz_lift) / VOLATILE_MOVE_SPEED
            e_drift_lift = drift_per_min * t_lift
            t_xy = dist_xy / VOLATILE_MOVE_SPEED
            e_drift_xy = drift_per_min * t_xy
            t_drop = abs(dz_drop) / VOLATILE_MOVE_SPEED
            e_drift_drop = drift_per_min * t_drop
            total_drift_steps = e_drift_lift + e_drift_xy + e_drift_drop
            volatile_cmds = [
                "G91",
                f"G1 Z{dz_lift:.2f} E-{e_drift_lift:.3f} F{VOLATILE_MOVE_SPEED}",
                f"G1 X{dx:.2f} Y{dy:.2f} E-{e_drift_xy:.3f} F{VOLATILE_MOVE_SPEED}",
                f"G1 Z{dz_drop:.2f} E-{e_drift_drop:.3f} F{VOLATILE_MOVE_SPEED}",
                "G90"
            ]
            self._send_lines_with_ok(volatile_cmds)
            e_loaded_pos -= total_drift_steps
        else:
            cmds_std = []
            cmds_std.append(f"G0 Z{src_safe_z:.2f} F{JOG_SPEED_Z}")
            cmds_std.append(f"G0 Z{travel_z_dest:.2f} F{JOG_SPEED_Z}")
            cmds_std.append(f"G0 X{dest_x:.2f} Y{dest_y:.2f} F{JOG_SPEED_XY}")
            cmds_std.append(f"G0 Z{dest_safe_z:.2f} F{JOG_SPEED_Z}")
            self._send_lines_with_ok(cmds_std)

        self.update_last_module(dest_mod)
        current_mod_tracker = dest_mod

        e_blowout_pos = -1 * 100.0 * STEPS_PER_UL
        cmds_disp = []

        if is_volatile:
            dz_final = dest_disp_z - dest_safe_z
            t_final = abs(dz_final) / VOLATILE_MOVE_SPEED
            e_drift_final = drift_per_min * t_final
            cmds_disp.append("G91")
            cmds_disp.append(f"G1 Z{dz_final:.2f} E-{e_drift_final:.3f} F{VOLATILE_MOVE_SPEED}")
            cmds_disp.append("G90")
            e_loaded_pos -= e_drift_final
        else:
            cmds_disp.append(f"G0 Z{dest_disp_z:.2f} F{JOG_SPEED_Z}")

        cmds_disp.append(f"G1 E{e_blowout_pos:.3f} F{PIP_SPEED}")
        cmds_disp.append(f"G0 Z{dest_safe_z:.2f} F{JOG_SPEED_Z}")
        cmds_disp.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")

        self._send_lines_with_ok(cmds_disp)
        self.current_pipette_volume = AIR_GAP_UL
        self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")

        return current_mod_tracker

    def _perform_wash_cycle(self, wash_src_str, original_src_str, dest_str, vol, is_volatile, e_gap_pos, air_gap_ul,
                            start_module=None):
        if start_module != "EJECT":
            self.log_line("[WASH] Ejecting dirty tip...")
            self._send_lines_with_ok(self._get_eject_tip_commands())
            self.update_last_module("EJECT")
        else:
            self.log_line("[WASH] Tip already ejected, skipping redundant eject.")

        tip_key = self._find_next_available_tip()
        if not tip_key:
            messagebox.showerror("No Tips", "Ran out of tips during wash.")
            return "EJECT"

        self.log_line(f"[WASH] Picking Tip {tip_key}...")
        self._send_lines_with_ok(self._get_pick_tip_commands(tip_key, start_module="EJECT"))
        self.tip_inventory[tip_key] = False
        self.update_last_module("TIPS")
        self.root.after(0, self.update_tip_grid_colors)
        current_mod_tracker = "TIPS"

        w_mod, w_x, w_y, w_safe_z, w_asp_z, _ = self.get_coords_from_combo(wash_src_str)
        cmds = []
        cmds.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")
        cmds.extend(self._get_smart_travel_gcode(w_mod, w_x, w_y, w_safe_z, start_module=current_mod_tracker))

        e_loaded = -1 * (air_gap_ul + vol) * STEPS_PER_UL
        cmds.append(f"G0 Z{w_asp_z:.2f} F{JOG_SPEED_Z}")
        cmds.append(f"G1 E{e_loaded:.3f} F{PIP_SPEED}")
        cmds.append(f"G0 Z{w_safe_z:.2f} F{JOG_SPEED_Z}")
        self._send_lines_with_ok(cmds)
        self.update_last_module(w_mod)
        current_mod_tracker = w_mod

        s_mod, s_x, s_y, s_safe_z, s_asp_z, s_disp_z = self.get_coords_from_combo(original_src_str)
        cmds_src = []
        cmds_src.extend(self._get_smart_travel_gcode(s_mod, s_x, s_y, s_safe_z, start_module=current_mod_tracker))

        cmds_src.append(f"G0 Z{s_disp_z:.2f} F{JOG_SPEED_Z}")
        cmds_src.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")

        self.log_line("[WASH] Performing robust mixing in source...")
        mix_vol = 200.0
        e_mix_up = -1 * (air_gap_ul) * STEPS_PER_UL
        e_mix_down = -1 * (air_gap_ul + mix_vol) * STEPS_PER_UL

        cmds_src.append(f"G0 Z{s_asp_z:.2f} F{JOG_SPEED_Z}")
        for _ in range(2):
            cmds_src.append(f"G1 E{e_mix_down:.3f} F{PIP_SPEED}")
            cmds_src.append(f"G1 E{e_mix_up:.3f} F{PIP_SPEED}")

        max_collect = MAX_PIPETTE_VOL - air_gap_ul
        collect_vol = min(vol + 50.0, max_collect)
        e_collected = -1 * (air_gap_ul + collect_vol) * STEPS_PER_UL
        cmds_src.append(f"G1 E{e_collected:.3f} F{PIP_SPEED}")
        cmds_src.append(f"G0 Z{s_safe_z:.2f} F{JOG_SPEED_Z}")

        self._send_lines_with_ok(cmds_src)
        self.update_last_module(s_mod)
        current_mod_tracker = s_mod

        self.log_line("[WASH] Transferring mixed wash liquid to destination...")

        d_mod, d_x, d_y, d_safe_z, _, d_disp_z = self.get_coords_from_combo(dest_str)
        cmds_dest = []
        cmds_dest.extend(self._get_smart_travel_gcode(d_mod, d_x, d_y, d_safe_z, start_module=current_mod_tracker))

        e_blowout = -1 * 100.0 * STEPS_PER_UL
        cmds_dest.append(f"G0 Z{d_disp_z:.2f} F{JOG_SPEED_Z}")
        cmds_dest.append(f"G1 E{e_blowout:.3f} F{PIP_SPEED}")
        cmds_dest.append(f"G0 Z{d_safe_z:.2f} F{JOG_SPEED_Z}")
        cmds_dest.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")

        self._send_lines_with_ok(cmds_dest)
        self.update_last_module(d_mod)

        self.log_line("[WASH] Cycle complete. Ejecting wash tip...")
        self._send_lines_with_ok(self._get_eject_tip_commands())
        self.update_last_module("EJECT")
        return "EJECT"

    def combine_fractions_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        tasks = []
        for idx, row in enumerate(self.combine_rows):
            if not row["vars"]["execute"].get():
                continue

            start_well = row["vars"]["start"].get()
            end_well = row["vars"]["end"].get()
            dest_falcon = row["vars"]["dest"].get()
            vol_str = row["vars"]["vol"].get()

            wash_vol_str = row["vars"]["wash_vol"].get()
            wash_times_str = row["vars"]["wash_times"].get()
            wash_src = row["vars"]["wash_src"].get()

            presat = row["vars"]["presat"].get()
            presat_src = row["vars"]["presat_src"].get()

            if not start_well or not end_well or not dest_falcon or not vol_str:
                continue
            try:
                start_idx = self.plate_wells.index(start_well)
                end_idx = self.plate_wells.index(end_well)
            except ValueError:
                self.log_line(f"[ERROR] Line {idx + 1}: Invalid well range.")
                continue
            if start_idx > end_idx:
                self.log_line(f"[ERROR] Line {idx + 1}: Start well must be before End well.")
                continue
            target_wells = self.plate_wells[start_idx: end_idx + 1]
            try:
                vol_per_well = float(vol_str)
                wash_vol = float(wash_vol_str)
                wash_times = int(wash_times_str)
            except ValueError:
                continue
            tasks.append({
                "line": idx + 1,
                "wells": target_wells,
                "dest": dest_falcon,
                "vol": vol_per_well,
                "wash_vol": wash_vol,
                "wash_times": wash_times,
                "wash_src": wash_src,
                "presat": presat,
                "presat_src": presat_src
            })
        if not tasks:
            messagebox.showinfo("No Tasks", "No valid lines configured or selected.")
            return
        self.log_command(f"[COMBINE] Starting sequence with {len(tasks)} lines.")
        plate_safe_z = self.resolve_coords(0, 0, PLATE_CONFIG["Z_SAFE"])[2]
        plate_asp_z = self.resolve_coords(0, 0, PLATE_CONFIG["Z_ASPIRATE"])[2]
        plate_disp_z = self.resolve_coords(0, 0, PLATE_CONFIG["Z_DISPENSE"])[2]
        falcon_safe_z = self.resolve_coords(0, 0, FALCON_RACK_CONFIG["Z_SAFE"])[2]
        falcon_disp_z = self.resolve_coords(0, 0, FALCON_RACK_CONFIG["Z_DISPENSE"])[2]
        air_gap_vol = 200.0
        e_pos_air_gap = -1 * air_gap_vol * STEPS_PER_UL
        e_pos_blowout = -1 * 100.0 * STEPS_PER_UL

        def run_seq():
            for task in tasks:
                line_num = task["line"]
                dest_falcon = task["dest"]
                vol_total = task["vol"]
                wells = task["wells"]
                self.log_line(f"[COMBINE] Processing Line {line_num}: {len(wells)} wells -> {dest_falcon}")
                self.last_cmd_var.set(f"Line {line_num}: Processing...")
                self.log_line(f"[COMBINE] Line {line_num}: Ejecting old tip...")
                self._send_lines_with_ok(self._get_eject_tip_commands())
                self.update_last_module("EJECT")
                current_sim_module = "EJECT"

                tip_key = self._find_next_available_tip()
                if not tip_key:
                    messagebox.showerror("No Tips", f"Ran out of tips at Line {line_num}.")
                    return
                self.log_line(f"[COMBINE] Line {line_num}: Picking Tip {tip_key}...")
                self._send_lines_with_ok(self._get_pick_tip_commands(tip_key, start_module=current_sim_module))
                self.tip_inventory[tip_key] = False
                self.update_last_module("TIPS")
                current_sim_module = "TIPS"

                # --- PRESATURATION LOGIC ---
                if task["presat"]:
                    p_src = task["presat_src"]
                    self.log_line(f"[COMBINE] Presaturating tip at {p_src}...")
                    w_mod, w_x, w_y, w_safe_z, w_asp_z, _ = self.get_coords_from_combo(p_src)

                    cmds_presat = []
                    # Move to Wash Source
                    cmds_presat.extend(
                        self._get_smart_travel_gcode(w_mod, w_x, w_y, w_safe_z, start_module=current_sim_module))

                    # Mix 3x 800uL
                    mix_vol = 800.0
                    e_mix_down = -1 * (air_gap_vol + mix_vol) * STEPS_PER_UL
                    e_mix_up = -1 * (air_gap_vol) * STEPS_PER_UL  # Back to air gap

                    cmds_presat.append(f"G0 Z{w_asp_z:.2f} F{JOG_SPEED_Z}")
                    for _ in range(3):
                        cmds_presat.append(f"G1 E{e_mix_down:.3f} F{PIP_SPEED}")
                        cmds_presat.append(f"G1 E{e_mix_up:.3f} F{PIP_SPEED}")

                    cmds_presat.append(f"G0 Z{w_safe_z:.2f} F{JOG_SPEED_Z}")

                    self._send_lines_with_ok(cmds_presat)
                    self.update_last_module(w_mod)
                    current_sim_module = w_mod
                # ---------------------------

                self.root.after(0, self.update_tip_grid_colors)
                for well in wells:
                    remaining_vol = vol_total
                    if remaining_vol > 800:
                        num_batches = math.ceil(remaining_vol / 800.0)
                        batch_vol = remaining_vol / num_batches
                    else:
                        num_batches = 1
                        batch_vol = remaining_vol
                    for b in range(num_batches):
                        self.last_cmd_var.set(f"L{line_num}: {well}->{dest_falcon} ({b + 1}/{num_batches})")
                        vol_aspirated = batch_vol
                        e_pos_full = -1 * (air_gap_vol + vol_aspirated) * STEPS_PER_UL
                        cmds = []
                        cmds.append(f"G1 E{e_pos_air_gap:.3f} F{PIP_SPEED}")
                        sx, sy = self.get_well_coordinates(well)

                        cmds.extend(
                            self._get_smart_travel_gcode("PLATE", sx, sy, plate_safe_z,
                                                         start_module=current_sim_module))

                        cmds.append(f"G0 Z{plate_asp_z:.2f} F{JOG_SPEED_Z}")
                        cmds.append(f"G1 E{e_pos_full:.3f} F{PIP_SPEED}")
                        cmds.append(f"G0 Z{plate_safe_z:.2f} F{JOG_SPEED_Z}")
                        self.update_last_module("PLATE")
                        current_sim_module = "PLATE"

                        dx, dy = self.get_falcon_coordinates(dest_falcon)

                        cmds.extend(
                            self._get_smart_travel_gcode("FALCON", dx, dy, falcon_safe_z,
                                                         start_module=current_sim_module))

                        cmds.append(f"G0 Z{falcon_disp_z:.2f} F{JOG_SPEED_Z}")
                        cmds.append(f"G1 E{e_pos_blowout:.3f} F{PIP_SPEED}")
                        cmds.append(f"G0 Z{falcon_safe_z:.2f} F{JOG_SPEED_Z}")
                        self._send_lines_with_ok(cmds)
                        self.update_last_module("FALCON")
                        current_sim_module = "FALCON"

                        self.current_pipette_volume = 100.0
                        self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")
                        self.live_vol_var.set(f"{self.current_pipette_volume:.1f}")

                wash_vol = task["wash_vol"]
                wash_times = task["wash_times"]

                if wash_vol > 0:
                    if current_sim_module != "EJECT":
                        self.log_line(f"[COMBINE] Line {line_num}: Ejecting main transfer tip...")
                        self._send_lines_with_ok(self._get_eject_tip_commands())
                        self.update_last_module("EJECT")
                        current_sim_module = "EJECT"

                    for cycle in range(wash_times):
                        tip_key = self._find_next_available_tip()
                        if not tip_key:
                            messagebox.showerror("No Tips", f"Ran out of tips for wash at Line {line_num}.")
                            return

                        self.log_line(f"[COMBINE] Line {line_num}: Picking Wash Tip {tip_key} (Cycle {cycle + 1})...")
                        self._send_lines_with_ok(self._get_pick_tip_commands(tip_key, start_module=current_sim_module))
                        self.tip_inventory[tip_key] = False
                        self.update_last_module("TIPS")
                        current_sim_module = "TIPS"
                        self.root.after(0, self.update_tip_grid_colors)

                        wash_src_str = task["wash_src"]
                        w_mod, w_x, w_y, w_safe_z, w_asp_z, _ = self.get_coords_from_combo(wash_src_str)

                        self.log_line(f"[COMBINE] Line {line_num}: Wash Cycle {cycle + 1}/{wash_times} (Batch Mode)...")

                        for well in wells:
                            self.log_line(f"  -> Distributing {wash_vol}uL Wash to {well}")
                            cmds_dist = []
                            cmds_dist.append(f"G1 E{e_pos_air_gap:.3f} F{PIP_SPEED}")
                            cmds_dist.extend(
                                self._get_smart_travel_gcode(w_mod, w_x, w_y, w_safe_z,
                                                             start_module=current_sim_module))

                            e_loaded = -1 * (air_gap_vol + wash_vol) * STEPS_PER_UL
                            cmds_dist.append(f"G0 Z{w_asp_z:.2f} F{JOG_SPEED_Z}")
                            cmds_dist.append(f"G1 E{e_loaded:.3f} F{PIP_SPEED}")
                            cmds_dist.append(f"G0 Z{w_safe_z:.2f} F{JOG_SPEED_Z}")

                            self._send_lines_with_ok(cmds_dist)
                            self.update_last_module(w_mod)
                            current_sim_module = w_mod

                            wx, wy = self.get_well_coordinates(well)
                            cmds_well = []
                            cmds_well.extend(self._get_smart_travel_gcode("PLATE", wx, wy, plate_safe_z,
                                                                          start_module=current_sim_module))

                            cmds_well.append(f"G0 Z{plate_disp_z:.2f} F{JOG_SPEED_Z}")
                            cmds_well.append(f"G1 E{e_pos_air_gap:.3f} F{PIP_SPEED}")
                            cmds_well.append(f"G0 Z{plate_safe_z:.2f} F{JOG_SPEED_Z}")

                            self._send_lines_with_ok(cmds_well)
                            self.update_last_module("PLATE")
                            current_sim_module = "PLATE"

                        for well in wells:
                            self.log_line(f"  -> Collecting Wash from {well}")
                            wx, wy = self.get_well_coordinates(well)
                            cmds_col = []
                            cmds_col.extend(self._get_smart_travel_gcode("PLATE", wx, wy, plate_safe_z,
                                                                         start_module=current_sim_module))

                            mix_vol = 200.0
                            e_mix_up = -1 * (air_gap_vol) * STEPS_PER_UL
                            e_mix_down = -1 * (air_gap_vol + mix_vol) * STEPS_PER_UL

                            cmds_col.append(f"G0 Z{plate_asp_z:.2f} F{JOG_SPEED_Z}")
                            for _ in range(3):
                                cmds_col.append(f"G1 E{e_mix_down:.3f} F{PIP_SPEED}")
                                cmds_col.append(f"G1 E{e_mix_up:.3f} F{PIP_SPEED}")

                            collect_vol = min(wash_vol + 50.0, 900.0)
                            e_collected = -1 * (air_gap_vol + collect_vol) * STEPS_PER_UL
                            cmds_col.append(f"G1 E{e_collected:.3f} F{PIP_SPEED}")
                            cmds_col.append(f"G0 Z{plate_safe_z:.2f} F{JOG_SPEED_Z}")

                            self._send_lines_with_ok(cmds_col)
                            self.update_last_module("PLATE")
                            current_sim_module = "PLATE"

                            dx, dy = self.get_falcon_coordinates(dest_falcon)
                            cmds_dest = []
                            cmds_dest.extend(self._get_smart_travel_gcode("FALCON", dx, dy, falcon_safe_z,
                                                                          start_module=current_sim_module))

                            cmds_dest.append(f"G0 Z{falcon_disp_z:.2f} F{JOG_SPEED_Z}")
                            cmds_dest.append(f"G1 E{e_pos_blowout:.3f} F{PIP_SPEED}")
                            cmds_dest.append(f"G0 Z{falcon_safe_z:.2f} F{JOG_SPEED_Z}")
                            cmds_dest.append(f"G1 E{e_pos_air_gap:.3f} F{PIP_SPEED}")

                            self._send_lines_with_ok(cmds_dest)
                            self.update_last_module("FALCON")
                            current_sim_module = "FALCON"

                        self.log_line(f"[COMBINE] Line {line_num}: Ejecting wash tip (End of Cycle {cycle + 1})...")
                        self._send_lines_with_ok(self._get_eject_tip_commands())
                        self.update_last_module("EJECT")
                        current_sim_module = "EJECT"

                self.log_line(f"[COMBINE] Line {line_num} Complete.")

            self.log_command("[COMBINE] Sequence Finished. Parking.")
            self.last_cmd_var.set("Parking...")
            self._send_lines_with_ok(self._get_park_head_commands())
            self.update_last_module("PARK")
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def aliquots_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        tasks = []
        for idx, row in enumerate(self.aliquot_rows):
            if not row["execute"].get():
                continue

            source = row["source"].get()
            volume_str = row["volume"].get()
            dest_start_str = row["dest_start"].get()
            dest_end_str = row["dest_end"].get()

            if not source or not volume_str or not dest_start_str or not dest_end_str:
                self.log_line(f"[ALIQUOT] Skipping line {idx + 1}: missing configuration.")
                continue

            try:
                volume = float(volume_str)
            except ValueError:
                self.log_line(f"[ALIQUOT] Skipping line {idx + 1}: invalid volume.")
                continue

            # Parse destination range
            dest_list = self._parse_dest_range(dest_start_str, dest_end_str)
            if not dest_list:
                self.log_line(f"[ALIQUOT] Skipping line {idx + 1}: invalid destination range.")
                continue

            tasks.append({
                "line": idx + 1,
                "source": source,
                "volume": volume,
                "destinations": dest_list,
            })

        if not tasks:
            messagebox.showinfo("No Tasks", "No lines selected for execution.")
            return

        self.log_command(f"[ALIQUOT] Starting sequence with {len(tasks)} lines.")

        air_gap_ul = float(AIR_GAP_UL)
        e_gap_pos = -1 * air_gap_ul * STEPS_PER_UL

        def run_seq():
            current_simulated_module = self.last_known_module

            # Ensure no tip is loaded at start
            self.log_line("[ALIQUOT] Ensuring no tip is loaded at start...")
            self._send_lines_with_ok(self._get_eject_tip_commands())
            self.update_last_module("EJECT")
            current_simulated_module = "EJECT"

            for task in tasks:
                line_num = task["line"]
                source_str = task["source"]
                total_volume = task["volume"]
                destinations = task["destinations"]
                num_destinations = len(destinations)

                if num_destinations == 0:
                    continue

                # Calculate volume per destination
                vol_per_dest = total_volume / num_destinations

                self.log_line(
                    f"[ALIQUOT] Line {line_num}: Distributing {total_volume}uL from {source_str} to {num_destinations} vials ({vol_per_dest:.2f}uL each)")
                self.last_cmd_var.set(f"L{line_num}: Aliquot...")

                # Pick fresh tip
                tip_key = self._find_next_available_tip()
                if not tip_key:
                    messagebox.showerror("No Tips", f"Ran out of tips at Line {line_num}.")
                    return

                self.log_line(f"[ALIQUOT L{line_num}] Picking Tip {tip_key}...")
                self._send_lines_with_ok(self._get_pick_tip_commands(tip_key, start_module=current_simulated_module))
                self.tip_inventory[tip_key] = False
                self.root.after(0, self.update_tip_grid_colors)
                self.update_last_module("TIPS")
                current_simulated_module = "TIPS"

                # --- MODIFIED: Aspirate user volume + 100uL trash ---
                TRASH_VOL_UL = 100.0
                vol_to_aspirate = total_volume + TRASH_VOL_UL

                self.log_line(
                    f"[ALIQUOT L{line_num}] Aspirating {vol_to_aspirate}uL ({total_volume} + 100 trash) from {source_str}...")
                src_mod, src_x, src_y, src_safe_z, src_asp_z, _ = self.get_coords_from_combo(source_str)

                cmds = []
                cmds.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")
                cmds.extend(self._get_smart_travel_gcode(src_mod, src_x, src_y, src_safe_z,
                                                         start_module=current_simulated_module))

                e_loaded = -1 * (air_gap_ul + vol_to_aspirate) * STEPS_PER_UL
                cmds.append(f"G0 Z{src_asp_z:.2f} F{JOG_SPEED_Z}")
                cmds.append(f"G1 E{e_loaded:.3f} F{PIP_SPEED}")
                cmds.append(f"G0 Z{src_safe_z:.2f} F{JOG_SPEED_Z}")

                self._send_lines_with_ok(cmds)
                self.update_last_module(src_mod)
                current_simulated_module = src_mod

                remaining_volume = total_volume
                for dest_str in destinations:
                    self.log_line(f"[ALIQUOT L{line_num}] Dispensing {vol_per_dest:.2f}uL into {dest_str}...")
                    self.last_cmd_var.set(f"L{line_num}: {vol_per_dest:.2f}uL -> {dest_str}")

                    dest_mod, dest_x, dest_y, dest_safe_z, dest_asp_z, dest_disp_z = self.get_coords_from_combo(
                        dest_str)

                    cmds_disp = []
                    cmds_disp.extend(self._get_smart_travel_gcode(
                        dest_mod, dest_x, dest_y, dest_safe_z,
                        start_module=current_simulated_module
                    ))

                    remaining_volume -= vol_per_dest
                    # --- MODIFIED: Ensure we keep TRASH_VOL_UL in the tip ---
                    e_after_disp = -1 * (air_gap_ul + remaining_volume + TRASH_VOL_UL) * STEPS_PER_UL

                    dispense_z = dest_asp_z
                    cmds_disp.append(f"G0 Z{dispense_z:.2f} F{JOG_SPEED_Z}")
                    cmds_disp.append(f"G1 E{e_after_disp:.3f} F{PIP_SPEED}")
                    cmds_disp.append(f"G0 Z{dest_safe_z:.2f} F{JOG_SPEED_Z}")

                    self._send_lines_with_ok(cmds_disp)
                    self.update_last_module(dest_mod)
                    current_simulated_module = dest_mod

                # Update volume display
                self.current_pipette_volume = air_gap_ul
                self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")
                self.live_vol_var.set(f"{self.current_pipette_volume:.1f}")

                # Eject tip
                self.log_line(f"[ALIQUOT L{line_num}] Ejecting tip...")
                self._send_lines_with_ok(self._get_eject_tip_commands())
                self.update_last_module("EJECT")
                current_simulated_module = "EJECT"

            # Park at end
            self.log_command("[ALIQUOT] All lines complete. Parking.")
            self.last_cmd_var.set("Parking...")
            self._send_lines_with_ok(self._get_park_head_commands())
            self.update_last_module("PARK")
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def _parse_dest_range(self, start_str, end_str):
        """
        Parse destination range from combo strings like "4mL A1" to "4mL A3"
        Returns list of full combo strings in order
        """
        # Parse module and position from start
        start_mod, start_pos = self._parse_combo_string(start_str)
        end_mod, end_pos = self._parse_combo_string(end_str)

        # Must be same module
        if start_mod != end_mod:
            return []

        # Get the position list for this module
        if start_mod == "FALCON":
            positions = self.falcon_positions
            prefix = "Falcon "
        elif start_mod == "4ML":
            positions = self._4ml_positions
            prefix = "4mL "
        elif start_mod == "FILTER_EPPI":
            positions = self.filter_eppi_positions
            prefix = "Filter Eppi "
        elif start_mod == "EPPI":
            positions = self.eppi_positions
            prefix = "Eppi "
        elif start_mod == "HPLC":
            positions = self.hplc_positions
            prefix = "HPLC "
        elif start_mod == "HPLC_INSERT":
            positions = self.hplc_insert_positions
            prefix = "HPLC Insert "
        elif start_mod == "SCREWCAP":
            positions = self.screwcap_positions
            prefix = "Screwcap "
        else:
            return []

        # Find indices
        try:
            start_idx = positions.index(start_pos)
            end_idx = positions.index(end_pos)
        except ValueError:
            return []

        if start_idx > end_idx:
            return []

        # Build list of combo strings
        result = []
        for i in range(start_idx, end_idx + 1):
            result.append(f"{prefix}{positions[i]}")

        return result

    def start_pin_calibration_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        tip_key = self._find_next_available_tip()
        if not tip_key:
            messagebox.showerror("No Tips", "No fresh tips available for calibration.")
            return
        self.log_line("[CALIB] Starting Pin Calibration Sequence...")
        cmds = []
        global_safe_z = self.resolve_coords(0, 0, GLOBAL_SAFE_Z_OFFSET)[2]
        cmds.append("G28")
        cmds.append(f"G0 Z{global_safe_z:.2f} F{JOG_SPEED_Z}")
        self.update_last_module("Unknown")
        pick_cmds = self._get_pick_tip_commands(tip_key)
        cmds.extend(pick_cmds)
        pin_x = CALIBRATION_PIN_CONFIG["PIN_X"]
        pin_y = CALIBRATION_PIN_CONFIG["PIN_Y"]
        pin_z = CALIBRATION_PIN_CONFIG["PIN_Z"]
        cmds.append(f"G0 Z{global_safe_z:.2f} F{JOG_SPEED_Z}")
        cmds.append(f"G0 X{pin_x:.2f} Y{pin_y:.2f} F{JOG_SPEED_XY}")
        cmds.append(f"G0 Z{pin_z:.2f} F{JOG_SPEED_Z}")

        def run_seq():
            self.last_cmd_var.set("Calibrating: Moving to pin...")
            self._send_lines_with_ok(cmds)
            self._wait_for_finish()
            self.tip_inventory[tip_key] = False
            self.update_last_module("CALIBRATION_PIN")
            self.root.after(0, self.update_tip_grid_colors)
            self.root.after(0, self.update_available_tips_combo)
            self.last_cmd_var.set("Waiting for User...")
            self.root.after(0, self._show_calibration_decision_popup)
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def _show_calibration_decision_popup(self):
        popup = tk.Toplevel(self.root)
        popup.title("Calibration Check")
        popup.geometry("300x150")
        popup.grab_set()
        ttk.Label(popup, text="Head is at calibration position.", font=("Arial", 10)).pack(pady=10)
        ttk.Label(popup, text="Does the tip touch the pin?", font=("Arial", 10)).pack(pady=5)
        btn_frame = ttk.Frame(popup)
        btn_frame.pack(pady=15)
        ttk.Button(btn_frame, text="ACCEPT (Done)", command=popup.destroy).pack(side="left", padx=10)
        ttk.Button(btn_frame, text="CALIBRATE",
                   command=lambda: [popup.destroy(), self._open_calibration_jog_window()]).pack(side="left", padx=10)

    def _open_calibration_jog_window(self):
        jog_win = tk.Toplevel(self.root)
        jog_win.title("Fine Tune Position")
        jog_win.geometry("450x420")
        jog_win.grab_set()
        ttk.Label(jog_win, text="Jog head until tip touches pin.", font=("Arial", 10, "bold")).pack(pady=10)
        
        # Precision selection frame
        precision_frame = ttk.LabelFrame(jog_win, text="Step Precision", padding=10)
        precision_frame.pack(pady=5)
        self.calib_step_var = tk.DoubleVar(value=0.1)  # Default to 0.1mm
        ttk.Radiobutton(precision_frame, text="0.1 mm", variable=self.calib_step_var, value=0.1).pack(side="left", padx=10)
        ttk.Radiobutton(precision_frame, text="1.0 mm", variable=self.calib_step_var, value=1.0).pack(side="left", padx=10)
        
        # Current precision display
        self.calib_precision_label = ttk.Label(jog_win, text="Current Step: 0.1 mm", font=("Arial", 9))
        self.calib_precision_label.pack(pady=5)
        
        def update_precision_label():
            self.calib_precision_label.config(text=f"Current Step: {self.calib_step_var.get()} mm")
        
        ctrl_frame = ttk.Frame(jog_win)
        ctrl_frame.pack(pady=10)
        original_step = self.step_size_var.get()
        self.step_size_var.set(0.1)

        def close_and_restore():
            self.step_size_var.set(original_step)
            jog_win.destroy()

        def jog_with_precision(axis, direction):
            # Update step_size_var to use selected precision
            self.step_size_var.set(self.calib_step_var.get())
            update_precision_label()
            self.send_jog(axis, direction)

        jog_win.protocol("WM_DELETE_WINDOW", close_and_restore)
        btn_w = 6
        ttk.Button(ctrl_frame, text="Y+", width=btn_w, command=lambda: jog_with_precision("Y", 1)).grid(row=0, column=1,
                                                                                                   pady=5)
        ttk.Button(ctrl_frame, text="Y-", width=btn_w, command=lambda: jog_with_precision("Y", -1)).grid(row=2, column=1,
                                                                                                    pady=5)
        ttk.Button(ctrl_frame, text="X-", width=btn_w, command=lambda: jog_with_precision("X", -1)).grid(row=1, column=0,
                                                                                                    padx=5)
        ttk.Button(ctrl_frame, text="X+", width=btn_w, command=lambda: jog_with_precision("X", 1)).grid(row=1, column=2,
                                                                                                   padx=5)
        ttk.Button(ctrl_frame, text="Z+ (Up)", width=btn_w, command=lambda: jog_with_precision("Z", 1)).grid(row=0, column=4,
                                                                                                            padx=20)
        ttk.Button(ctrl_frame, text="Z- (Dn)", width=btn_w, command=lambda: jog_with_precision("Z", -1)).grid(row=2,
                                                                                                             column=4,
                                                                                                             padx=20)
        bot_frame = ttk.Frame(jog_win)
        bot_frame.pack(side="bottom", fill="x", pady=10, padx=10)
        ttk.Button(bot_frame, text="Revert to Default",
                   command=lambda: [self.revert_calibration_default(), close_and_restore()]).pack(side="left")
        ttk.Button(bot_frame, text="ACCEPT & SAVE",
                   command=lambda: [self.save_calibration_position(), close_and_restore()]).pack(side="right")

    def save_calibration_position(self):
        """Save the calibrated pin position to config.json"""
        # Round values to 0.1 mm precision
        rounded_x = round(self.current_x, 1)
        rounded_y = round(self.current_y, 1)
        rounded_z = round(self.current_z, 1)
        
        # Load existing full config from file
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, "r") as f:
                    full_config = json.load(f)
            else:
                full_config = {}
        except Exception as e:
            messagebox.showerror("Error", f"Could not load config file: {e}")
            return

        # Update only the PIN coordinates (rounded to 0.1mm)
        full_config["PIN_X"] = rounded_x
        full_config["PIN_Y"] = rounded_y
        full_config["PIN_Z"] = rounded_z

        # Save the complete config back to file
        try:
            with open(self.config_file, "w") as f:
                json.dump(full_config, f, indent=4)

            # Update the global config in memory (rounded values)
            global CALIBRATION_PIN_CONFIG
            CALIBRATION_PIN_CONFIG["PIN_X"] = rounded_x
            CALIBRATION_PIN_CONFIG["PIN_Y"] = rounded_y
            CALIBRATION_PIN_CONFIG["PIN_Z"] = rounded_z

            self.log_line(
                f"[CALIB] New Pin Config Saved: X={rounded_x}, Y={rounded_y}, Z={rounded_z}")
            messagebox.showinfo("Saved",
                                f"New calibration coordinates saved to config.json\nX={rounded_x}, Y={rounded_y}, Z={rounded_z}")

        except Exception as e:
            messagebox.showerror("Error", f"Could not save config: {e}")
            self.log_line(f"[CALIB] Error saving pin config: {e}")

    def revert_calibration_default(self):
        """Revert pin calibration to default values"""
        # Load existing full config from file
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, "r") as f:
                    full_config = json.load(f)
            else:
                full_config = {}
        except Exception as e:
            messagebox.showerror("Error", f"Could not load config file: {e}")
            return

        # Update only the PIN coordinates to defaults
        full_config["PIN_X"] = CALIBRATION_PIN_CONFIG_DEFAULT["PIN_X"]
        full_config["PIN_Y"] = CALIBRATION_PIN_CONFIG_DEFAULT["PIN_Y"]
        full_config["PIN_Z"] = CALIBRATION_PIN_CONFIG_DEFAULT["PIN_Z"]

        # Save the complete config back to file
        try:
            with open(self.config_file, "w") as f:
                json.dump(full_config, f, indent=4)

            # Update the global config in memory
            global CALIBRATION_PIN_CONFIG
            CALIBRATION_PIN_CONFIG = CALIBRATION_PIN_CONFIG_DEFAULT.copy()

            self.log_line("[CALIB] Reverted to Default Pin Config.")
            messagebox.showinfo("Reverted", "Calibration reverted to default values.")

        except Exception as e:
            messagebox.showerror("Error", f"Could not save config: {e}")
            self.log_line(f"[CALIB] Error reverting pin config: {e}")

    def get_module_first_last_positions(self, module_name):
        """Get the first and last positions for a given module"""
        positions = {
            "tip rack": ("A1", "F4"),
            "96 well plate": ("A1", "H12"),
            "15 mL falcon rack": ("A1", "B3"),
            "50 mL falcon rack": ("50mL", "50mL"),  # Single position
            "wash rack": ("Wash A", "Trash"),
            "4mL rack": ("A1", "A8"),
            "filter eppi rack": ("B1", "B8"),
            "eppi rack": ("C1", "C8"),
            "hplc vial insert rack": ("E1", "E8"),
            "screwcap vial rack": ("F1", "F8")
        }
        return positions.get(module_name, ("A1", "A1"))

    def start_module_calibration_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        module_name = self.calibration_module_var.get()
        first_pos, last_pos = self.get_module_first_last_positions(module_name)

        # Store calibration state
        self.current_calibration_module = module_name
        self.current_calibration_positions = [first_pos, last_pos]
        self.current_calibration_step = 0  # 0 = first position, 1 = last position

        self.log_line(f"[MODULE_CALIB] Starting {module_name} calibration sequence...")
        self.log_line(f"[MODULE_CALIB] Will calibrate positions: {first_pos} and {last_pos}")

        # Start with first position
        self._calibrate_module_position(first_pos)

    def _calibrate_module_position(self, position):
        """Move to a specific position in the current module and show calibration dialog"""
        module_name = self.current_calibration_module

        # Get coordinates based on module type
        try:
            if module_name == "tip rack":
                x, y = self.get_tip_coordinates(position)
                safe_z = self.resolve_coords(0, 0, TIP_RACK_CONFIG["Z_TRAVEL"])[2]
                calib_z = self.resolve_coords(0, 0, TIP_RACK_CONFIG.get("Z_CALIBRATE", TIP_RACK_CONFIG["Z_PICK"]))[2]
            elif module_name == "96 well plate":
                x, y = self.get_well_coordinates(position)
                safe_z = self.resolve_coords(0, 0, PLATE_CONFIG["Z_SAFE"])[2]
                calib_z = self.resolve_coords(0, 0, PLATE_CONFIG.get("Z_CALIBRATE", PLATE_CONFIG["Z_DISPENSE"]))[2]
            elif module_name == "15 mL falcon rack":
                x, y = self.get_falcon_coordinates(position)
                safe_z = self.resolve_coords(0, 0, FALCON_RACK_CONFIG["Z_SAFE"])[2]
                calib_z = \
                    self.resolve_coords(0, 0, FALCON_RACK_CONFIG.get("Z_CALIBRATE", FALCON_RACK_CONFIG["Z_DISPENSE"]))[
                        2]
            elif module_name == "50 mL falcon rack":
                x, y = self.get_falcon_coordinates(position)
                safe_z = self.resolve_coords(0, 0, FALCON_RACK_CONFIG["Z_SAFE"])[2]
                calib_z = \
                    self.resolve_coords(0, 0, FALCON_RACK_CONFIG.get("Z_CALIBRATE", FALCON_RACK_CONFIG["Z_DISPENSE"]))[
                        2]
            elif module_name == "wash rack":
                x, y = self.get_wash_coordinates(position)
                safe_z = self.resolve_coords(0, 0, WASH_RACK_CONFIG["Z_SAFE"])[2]
                calib_z = \
                    self.resolve_coords(0, 0, WASH_RACK_CONFIG.get("Z_CALIBRATE", WASH_RACK_CONFIG["Z_DISPENSE"]))[2]
            elif module_name == "4mL rack":
                x, y = self.get_4ml_coordinates(position)
                safe_z = self.resolve_coords(0, 0, _4ML_RACK_CONFIG["Z_SAFE"])[2]
                calib_z = \
                    self.resolve_coords(0, 0, _4ML_RACK_CONFIG.get("Z_CALIBRATE", _4ML_RACK_CONFIG["Z_DISPENSE"]))[2]
            elif module_name == "filter eppi rack":
                x, y = self.get_1x8_rack_coordinates(position, FILTER_EPPI_RACK_CONFIG, "B")
                safe_z = self.resolve_coords(0, 0, FILTER_EPPI_RACK_CONFIG["Z_SAFE"])[2]
                calib_z = self.resolve_coords(0, 0, FILTER_EPPI_RACK_CONFIG.get("Z_CALIBRATE",
                                                                                FILTER_EPPI_RACK_CONFIG["Z_DISPENSE"]))[
                    2]
            elif module_name == "eppi rack":
                x, y = self.get_1x8_rack_coordinates(position, EPPI_RACK_CONFIG, "C")
                safe_z = self.resolve_coords(0, 0, EPPI_RACK_CONFIG["Z_SAFE"])[2]
                calib_z = \
                    self.resolve_coords(0, 0, EPPI_RACK_CONFIG.get("Z_CALIBRATE", EPPI_RACK_CONFIG["Z_DISPENSE"]))[2]
            elif module_name == "hplc vial insert rack":
                x, y = self.get_1x8_rack_coordinates(position, HPLC_VIAL_INSERT_RACK_CONFIG, "E")
                safe_z = self.resolve_coords(0, 0, HPLC_VIAL_INSERT_RACK_CONFIG["Z_SAFE"])[2]
                calib_z = self.resolve_coords(0, 0, HPLC_VIAL_INSERT_RACK_CONFIG.get("Z_CALIBRATE",
                                                                                     HPLC_VIAL_INSERT_RACK_CONFIG[
                                                                                         "Z_DISPENSE"]))[2]
            elif module_name == "screwcap vial rack":
                x, y = self.get_1x8_rack_coordinates(position, SCREWCAP_VIAL_RACK_CONFIG, "F")
                safe_z = self.resolve_coords(0, 0, SCREWCAP_VIAL_RACK_CONFIG["Z_SAFE"])[2]
                calib_z = self.resolve_coords(0, 0, SCREWCAP_VIAL_RACK_CONFIG.get("Z_CALIBRATE",
                                                                                  SCREWCAP_VIAL_RACK_CONFIG[
                                                                                      "Z_DISPENSE"]))[2]
            else:
                messagebox.showerror("Error", f"Unknown module: {module_name}")
                return
        except Exception as e:
            messagebox.showerror("Error", f"Failed to get coordinates for {position}: {e}")
            return

        # Store current position being calibrated
        self.current_calibration_position = position
        self.current_calibration_coords = (x, y, calib_z)

        # Move to position using Z_CALIBRATE height
        cmds = []
        global_safe_z = self.resolve_coords(0, 0, GLOBAL_SAFE_Z_OFFSET)[2]
        cmds.append(f"G0 Z{global_safe_z:.2f} F{JOG_SPEED_Z}")
        cmds.append(f"G0 X{x:.2f} Y{y:.2f} F{JOG_SPEED_XY}")
        cmds.append(f"G0 Z{calib_z:.2f} F{JOG_SPEED_Z}")

        def run_seq():
            self.last_cmd_var.set(f"Calibrating: Moving to {module_name} {position}...")
            self._send_lines_with_ok(cmds)
            self._wait_for_finish()
            self.last_cmd_var.set("Waiting for User...")
            self.root.after(0, self._show_module_calibration_decision_popup)
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def _show_module_calibration_decision_popup(self):
        """Show popup asking user to accept or calibrate the current module position"""
        popup = tk.Toplevel(self.root)
        popup.title("Module Calibration Check")
        popup.geometry("350x180")
        popup.grab_set()

        module_name = self.current_calibration_module
        position = self.current_calibration_position
        step = self.current_calibration_step
        total_positions = len(self.current_calibration_positions)

        ttk.Label(popup, text=f"Head is at {module_name} position {position}.",
                  font=("Arial", 10)).pack(pady=10)
        ttk.Label(popup, text=f"Step {step + 1} of {total_positions}",
                  font=("Arial", 9, "italic")).pack(pady=5)
        ttk.Label(popup, text="Is the position correct?", font=("Arial", 10)).pack(pady=5)

        btn_frame = ttk.Frame(popup)
        btn_frame.pack(pady=15)

        def accept_position():
            popup.destroy()
            self._proceed_to_next_calibration_step()

        def calibrate_position():
            popup.destroy()
            self._open_module_calibration_jog_window()

        ttk.Button(btn_frame, text="ACCEPT (Done)", command=accept_position).pack(side="left", padx=10)
        ttk.Button(btn_frame, text="CALIBRATE", command=calibrate_position).pack(side="left", padx=10)

    def _open_module_calibration_jog_window(self):
        """Open jog window for fine-tuning module position"""
        jog_win = tk.Toplevel(self.root)
        jog_win.title("Fine Tune Module Position")
        jog_win.geometry("450x430")
        jog_win.grab_set()

        module_name = self.current_calibration_module
        position = self.current_calibration_position

        ttk.Label(jog_win, text=f"Jog head to correct {module_name} {position} position.",
                  font=("Arial", 10, "bold")).pack(pady=10)
        
        # Precision selection frame
        precision_frame = ttk.LabelFrame(jog_win, text="Step Precision", padding=10)
        precision_frame.pack(pady=5)
        self.calib_step_var = tk.DoubleVar(value=0.1)  # Default to 0.1mm
        ttk.Radiobutton(precision_frame, text="0.1 mm", variable=self.calib_step_var, value=0.1).pack(side="left", padx=10)
        ttk.Radiobutton(precision_frame, text="1.0 mm", variable=self.calib_step_var, value=1.0).pack(side="left", padx=10)
        
        # Current precision display
        self.calib_precision_label = ttk.Label(jog_win, text="Current Step: 0.1 mm", font=("Arial", 9))
        self.calib_precision_label.pack(pady=5)
        
        def update_precision_label():
            self.calib_precision_label.config(text=f"Current Step: {self.calib_step_var.get()} mm")

        ctrl_frame = ttk.Frame(jog_win)
        ctrl_frame.pack(pady=10)

        # Store original step size and set to 0.1mm
        original_step = self.step_size_var.get()
        self.step_size_var.set(0.1)

        def close_and_restore():
            self.step_size_var.set(original_step)
            jog_win.destroy()

        def jog_with_precision(axis, direction):
            # Update step_size_var to use selected precision
            self.step_size_var.set(self.calib_step_var.get())
            update_precision_label()
            self.send_jog(axis, direction)

        jog_win.protocol("WM_DELETE_WINDOW", close_and_restore)

        # Jog buttons
        btn_w = 6
        ttk.Button(ctrl_frame, text="Y+", width=btn_w, command=lambda: jog_with_precision("Y", 1)).grid(row=0, column=1,
                                                                                                   pady=5)
        ttk.Button(ctrl_frame, text="Y-", width=btn_w, command=lambda: jog_with_precision("Y", -1)).grid(row=2, column=1,
                                                                                                    pady=5)
        ttk.Button(ctrl_frame, text="X-", width=btn_w, command=lambda: jog_with_precision("X", -1)).grid(row=1, column=0,
                                                                                                    padx=5)
        ttk.Button(ctrl_frame, text="X+", width=btn_w, command=lambda: jog_with_precision("X", 1)).grid(row=1, column=2,
                                                                                                   padx=5)
        ttk.Button(ctrl_frame, text="Z+ (Up)", width=btn_w, command=lambda: jog_with_precision("Z", 1)).grid(row=0, column=4,
                                                                                                            padx=20)
        ttk.Button(ctrl_frame, text="Z- (Dn)", width=btn_w, command=lambda: jog_with_precision("Z", -1)).grid(row=2,
                                                                                                             column=4,
                                                                                                             padx=20)

        # Bottom buttons
        bot_frame = ttk.Frame(jog_win)
        bot_frame.pack(side="bottom", fill="x", pady=10, padx=10)

        def save_and_continue():
            self.save_module_calibration_position()
            close_and_restore()
            self._proceed_to_next_calibration_step()

        ttk.Button(bot_frame, text="ACCEPT & SAVE", command=save_and_continue).pack(side="right")

    def save_module_calibration_position(self):
        """Save the current calibrated position to config.json"""
        module_name = self.current_calibration_module
        position = self.current_calibration_position

        # Get current absolute coordinates
        new_x = self.current_x
        new_y = self.current_y
        new_z = self.current_z

        # Calculate relative coordinates from calibration pin
        rel_x = new_x - CALIBRATION_PIN_CONFIG["PIN_X"]
        rel_y = new_y - CALIBRATION_PIN_CONFIG["PIN_Y"]
        rel_z = new_z - CALIBRATION_PIN_CONFIG["PIN_Z"]
        
        # Round values to 0.1 mm precision
        rel_x = round(rel_x, 1)
        rel_y = round(rel_y, 1)
        rel_z = round(rel_z, 1)

        # Load existing full config from file
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, "r") as f:
                    full_config = json.load(f)
            else:
                full_config = {}
        except Exception as e:
            messagebox.showerror("Error", f"Could not load config file: {e}")
            return

        # Update the specific fields based on module and position
        try:
            if module_name == "tip rack":
                if "TIP_RACK_CONFIG" not in full_config:
                    full_config["TIP_RACK_CONFIG"] = {}
                if position == "A1":
                    full_config["TIP_RACK_CONFIG"]["A1_X"] = rel_x
                    full_config["TIP_RACK_CONFIG"]["A1_Y"] = rel_y
                elif position == "F4":
                    full_config["TIP_RACK_CONFIG"]["F4_X"] = rel_x
                    full_config["TIP_RACK_CONFIG"]["F4_Y"] = rel_y
                full_config["TIP_RACK_CONFIG"]["Z_CALIBRATE"] = rel_z

            elif module_name == "96 well plate":
                if "PLATE_CONFIG" not in full_config:
                    full_config["PLATE_CONFIG"] = {}
                if position == "A1":
                    full_config["PLATE_CONFIG"]["A1_X"] = rel_x
                    full_config["PLATE_CONFIG"]["A1_Y"] = rel_y
                elif position == "H12":
                    full_config["PLATE_CONFIG"]["H12_X"] = rel_x
                    full_config["PLATE_CONFIG"]["H12_Y"] = rel_y
                full_config["PLATE_CONFIG"]["Z_CALIBRATE"] = rel_z

            elif module_name == "15 mL falcon rack":
                if "FALCON_RACK_CONFIG" not in full_config:
                    full_config["FALCON_RACK_CONFIG"] = {}
                if position == "A1":
                    full_config["FALCON_RACK_CONFIG"]["15ML_A1_X"] = rel_x
                    full_config["FALCON_RACK_CONFIG"]["15ML_A1_Y"] = rel_y
                elif position == "B3":
                    full_config["FALCON_RACK_CONFIG"]["15ML_B3_X"] = rel_x
                    full_config["FALCON_RACK_CONFIG"]["15ML_B3_Y"] = rel_y
                full_config["FALCON_RACK_CONFIG"]["Z_CALIBRATE"] = rel_z

            elif module_name == "50 mL falcon rack":
                if "FALCON_RACK_CONFIG" not in full_config:
                    full_config["FALCON_RACK_CONFIG"] = {}
                full_config["FALCON_RACK_CONFIG"]["50ML_X"] = rel_x
                full_config["FALCON_RACK_CONFIG"]["50ML_Y"] = rel_y
                full_config["FALCON_RACK_CONFIG"]["Z_CALIBRATE"] = rel_z

            elif module_name == "wash rack":
                if "WASH_RACK_CONFIG" not in full_config:
                    full_config["WASH_RACK_CONFIG"] = {}
                if position == "Wash A":
                    full_config["WASH_RACK_CONFIG"]["A1_X"] = rel_x
                    full_config["WASH_RACK_CONFIG"]["A1_Y"] = rel_y
                elif position == "Trash":
                    full_config["WASH_RACK_CONFIG"]["B2_X"] = rel_x
                    full_config["WASH_RACK_CONFIG"]["B2_Y"] = rel_y
                full_config["WASH_RACK_CONFIG"]["Z_CALIBRATE"] = rel_z

            elif module_name == "4mL rack":
                if "4ML_RACK_CONFIG" not in full_config:
                    full_config["4ML_RACK_CONFIG"] = {}
                if position == "A1":
                    full_config["4ML_RACK_CONFIG"]["A1_X"] = rel_x
                    full_config["4ML_RACK_CONFIG"]["A1_Y"] = rel_y
                elif position == "A8":
                    full_config["4ML_RACK_CONFIG"]["A8_X"] = rel_x
                    full_config["4ML_RACK_CONFIG"]["A8_Y"] = rel_y
                full_config["4ML_RACK_CONFIG"]["Z_CALIBRATE"] = rel_z

            elif module_name == "filter eppi rack":
                if "FILTER_EPPI_RACK_CONFIG" not in full_config:
                    full_config["FILTER_EPPI_RACK_CONFIG"] = {}
                if position == "B1":
                    full_config["FILTER_EPPI_RACK_CONFIG"]["B1_X"] = rel_x
                    full_config["FILTER_EPPI_RACK_CONFIG"]["B1_Y"] = rel_y
                elif position == "B8":
                    full_config["FILTER_EPPI_RACK_CONFIG"]["B8_X"] = rel_x
                    full_config["FILTER_EPPI_RACK_CONFIG"]["B8_Y"] = rel_y
                full_config["FILTER_EPPI_RACK_CONFIG"]["Z_CALIBRATE"] = rel_z

            elif module_name == "eppi rack":
                if "EPPI_RACK_CONFIG" not in full_config:
                    full_config["EPPI_RACK_CONFIG"] = {}
                if position == "C1":
                    full_config["EPPI_RACK_CONFIG"]["C1_X"] = rel_x
                    full_config["EPPI_RACK_CONFIG"]["C1_Y"] = rel_y
                elif position == "C8":
                    full_config["EPPI_RACK_CONFIG"]["C8_X"] = rel_x
                    full_config["EPPI_RACK_CONFIG"]["C8_Y"] = rel_y
                full_config["EPPI_RACK_CONFIG"]["Z_CALIBRATE"] = rel_z

            elif module_name == "hplc vial insert rack":
                if "HPLC_VIAL_INSERT_RACK_CONFIG" not in full_config:
                    full_config["HPLC_VIAL_INSERT_RACK_CONFIG"] = {}
                if position == "E1":
                    full_config["HPLC_VIAL_INSERT_RACK_CONFIG"]["E1_X"] = rel_x
                    full_config["HPLC_VIAL_INSERT_RACK_CONFIG"]["E1_Y"] = rel_y
                elif position == "E8":
                    full_config["HPLC_VIAL_INSERT_RACK_CONFIG"]["E8_X"] = rel_x
                    full_config["HPLC_VIAL_INSERT_RACK_CONFIG"]["E8_Y"] = rel_y
                full_config["HPLC_VIAL_INSERT_RACK_CONFIG"]["Z_CALIBRATE"] = rel_z

            elif module_name == "screwcap vial rack":
                if "SCREWCAP_VIAL_RACK_CONFIG" not in full_config:
                    full_config["SCREWCAP_VIAL_RACK_CONFIG"] = {}
                if position == "F1":
                    full_config["SCREWCAP_VIAL_RACK_CONFIG"]["F1_X"] = rel_x
                    full_config["SCREWCAP_VIAL_RACK_CONFIG"]["F1_Y"] = rel_y
                elif position == "F8":
                    full_config["SCREWCAP_VIAL_RACK_CONFIG"]["F8_X"] = rel_x
                    full_config["SCREWCAP_VIAL_RACK_CONFIG"]["F8_Y"] = rel_y
                full_config["SCREWCAP_VIAL_RACK_CONFIG"]["Z_CALIBRATE"] = rel_z

            # Save the complete config back to file
            with open(self.config_file, "w") as f:
                json.dump(full_config, f, indent=4)

            self.log_line(
                f"[MODULE_CALIB] Saved {module_name} {position}: X={rel_x}, Y={rel_y}, Z_CALIBRATE={rel_z}")
            messagebox.showinfo("Saved",
                                f"{module_name} {position} calibration saved!\nX={rel_x}, Y={rel_y}, Z_CALIBRATE={rel_z}")

        except Exception as e:
            messagebox.showerror("Error", f"Failed to save calibration: {e}")
            self.log_line(f"[MODULE_CALIB] Error saving {module_name} {position}: {e}")

    def _proceed_to_next_calibration_step(self):
        """Move to the next position in the calibration sequence or finish"""
        self.current_calibration_step += 1

        if self.current_calibration_step < len(self.current_calibration_positions):
            # Move to next position
            next_position = self.current_calibration_positions[self.current_calibration_step]
            self.log_line(f"[MODULE_CALIB] Moving to next position: {next_position}")
            self._calibrate_module_position(next_position)
        else:
            # Calibration complete
            module_name = self.current_calibration_module
            self.log_line(f"[MODULE_CALIB] {module_name} calibration sequence complete!")
            messagebox.showinfo("Complete", f"{module_name} calibration sequence completed successfully!")

    def test_rack_module_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        all_tips = [f"{r}{c}" for r in self.tip_rows for c in self.tip_cols]
        random.shuffle(all_tips)
        self.log_command(f"[TEST] Starting Rack Test on {len(all_tips)} tips (Randomized)...")
        full_sequence = ["G90"]
        simulated_last_module = self.last_known_module
        for tip_key in all_tips:
            self.last_known_module = simulated_last_module
            pick_cmds = self._get_pick_tip_commands(tip_key, start_module=simulated_last_module)
            full_sequence.extend(pick_cmds)
            simulated_last_module = "TIPS"
            self.last_known_module = "TIPS"
            eject_cmds = self._get_eject_tip_commands()
            full_sequence.extend(eject_cmds)
            simulated_last_module = "EJECT"
            self.last_known_module = "EJECT"
        abs_park_x, abs_park_y, abs_park_z = self.resolve_coords(SAFE_CENTER_X_OFFSET, SAFE_CENTER_Y_OFFSET,
                                                                 GLOBAL_SAFE_Z_OFFSET)
        full_sequence.append(f"G0 X{abs_park_x:.2f} Y{abs_park_y:.2f} Z{abs_park_z:.2f} F{JOG_SPEED_XY}")

        def run_seq():
            self.last_cmd_var.set("Running Rack Test Sequence...")
            self._send_lines_with_ok(full_sequence)
            self._wait_for_finish()
            self.reset_all_tips_empty()
            self.update_last_module("PARK")
            self.log_command("[SYSTEM] Rack Test Complete. Parked.")
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def test_96_plate_robustness_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        self.log_command("[SYSTEM] Starting 96 Plate Robustness Sequence...")
        wash_safe_z = self.resolve_coords(0, 0, WASH_RACK_CONFIG["Z_SAFE"])[2]
        wash_asp_z = self.resolve_coords(0, 0, WASH_RACK_CONFIG["Z_ASPIRATE"])[2]
        plate_safe_z = self.resolve_coords(0, 0, PLATE_CONFIG["Z_SAFE"])[2]
        plate_asp_z = self.resolve_coords(0, 0, PLATE_CONFIG["Z_ASPIRATE"])[2]
        plate_disp_z = self.resolve_coords(0, 0, PLATE_CONFIG["Z_DISPENSE"])[2]
        vol_gap = 200.0
        vol_asp = 800.0
        vol_disp = 900.0
        e_gap_pos = -1 * vol_gap * STEPS_PER_UL
        e_full_pos = -1 * (vol_gap + vol_asp) * STEPS_PER_UL
        e_blowout_pos = -1 * (vol_gap + vol_asp - vol_disp) * STEPS_PER_UL

        def run_seq():
            self.last_cmd_var.set("Starting Plate Robustness Test...")
            current_sim_module = self.last_known_module

            for row_idx, row_char in enumerate(self.plate_rows):
                self.log_line(f"[SYSTEM] Starting Row {row_char}...")
                self.last_cmd_var.set(f"Test: Row {row_char}...")
                self.log_line(f"[SYSTEM] Row {row_char}: Ejecting old tip...")
                self._send_lines_with_ok(self._get_eject_tip_commands())
                self.update_last_module("EJECT")
                current_sim_module = "EJECT"

                tip_key = self._find_next_available_tip()
                if not tip_key:
                    messagebox.showerror("No Tips", f"Ran out of tips at Row {row_char}.")
                    return
                self.log_line(f"[SYSTEM] Row {row_char}: Picking Tip {tip_key}...")
                self._send_lines_with_ok(self._get_pick_tip_commands(tip_key, start_module=current_sim_module))
                self.tip_inventory[tip_key] = False
                self.update_last_module("TIPS")
                current_sim_module = "TIPS"

                self.root.after(0, self.update_tip_grid_colors)
                self.log_line(f"[TEST] Row {row_char}: Initial Charge from Wash A -> {row_char}1")
                cmds_init = []
                cmds_init.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")
                wx, wy = self.get_wash_coordinates("Wash A")
                cmds_init.extend(
                    self._get_smart_travel_gcode("WASH", wx, wy, wash_safe_z, start_module=current_sim_module))
                cmds_init.append(f"G0 Z{wash_asp_z:.2f} F{JOG_SPEED_Z}")
                cmds_init.append(f"G1 E{e_full_pos:.3f} F{PIP_SPEED}")
                cmds_init.append(f"G0 Z{wash_safe_z:.2f} F{JOG_SPEED_Z}")
                current_sim_module = "WASH"

                p1_x, p1_y = self.get_well_coordinates(f"{row_char}1")
                cmds_init.extend(
                    self._get_smart_travel_gcode("PLATE", p1_x, p1_y, plate_safe_z, start_module=current_sim_module))
                cmds_init.append(f"G0 Z{plate_disp_z:.2f} F{JOG_SPEED_Z}")
                cmds_init.append(f"G1 E{e_blowout_pos:.3f} F{PIP_SPEED}")
                cmds_init.append(f"G0 Z{plate_safe_z:.2f} F{JOG_SPEED_Z}")
                self._send_lines_with_ok(cmds_init)
                self.update_last_module("PLATE")
                current_sim_module = "PLATE"

                self.current_pipette_volume = 100.0
                self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")
                self.live_vol_var.set(f"{self.current_pipette_volume:.1f}")
                for col in range(1, 12):
                    self.last_cmd_var.set(f"Test: Row {row_char} Col {col}->{col + 1}")
                    src_well = f"{row_char}{col}"
                    dst_well = f"{row_char}{col + 1}"
                    cmds_xfer = []
                    cmds_xfer.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")
                    sx, sy = self.get_well_coordinates(src_well)
                    cmds_xfer.extend(
                        self._get_smart_travel_gcode("PLATE", sx, sy, plate_safe_z, start_module=current_sim_module))
                    cmds_xfer.append(f"G0 Z{plate_asp_z:.2f} F{JOG_SPEED_Z}")
                    cmds_xfer.append(f"G1 E{e_full_pos:.3f} F{PIP_SPEED}")
                    cmds_xfer.append(f"G0 Z{plate_safe_z:.2f} F{JOG_SPEED_Z}")

                    dx, dy = self.get_well_coordinates(dst_well)
                    cmds_xfer.extend(self._get_smart_travel_gcode("PLATE", dx, dy, plate_safe_z, start_module="PLATE"))
                    cmds_xfer.append(f"G0 Z{plate_disp_z:.2f} F{JOG_SPEED_Z}")
                    cmds_xfer.append(f"G1 E{e_blowout_pos:.3f} F{PIP_SPEED}")
                    cmds_xfer.append(f"G0 Z{plate_safe_z:.2f} F{JOG_SPEED_Z}")
                    self._send_lines_with_ok(cmds_xfer)
                    self.current_pipette_volume = 100.0
                    self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")
                    self.live_vol_var.set(f"{self.current_pipette_volume:.1f}")
            self.log_line("[SYSTEM] All Rows Complete. Ejecting final tip...")
            self.last_cmd_var.set("Test: Final Eject...")
            self._send_lines_with_ok(self._get_eject_tip_commands())
            self.update_last_module("EJECT")
            abs_park_x, abs_park_y, abs_park_z = self.resolve_coords(SAFE_CENTER_X_OFFSET, SAFE_CENTER_Y_OFFSET,
                                                                     GLOBAL_SAFE_Z_OFFSET)
            self._send_lines_with_ok([f"G0 X{abs_park_x:.2f} Y{abs_park_y:.2f} Z{abs_park_z:.2f} F{JOG_SPEED_XY}"])
            self.update_last_module("PARK")
            self.log_command("[SYSTEM] Robustness Sequence Finished.")
            self._wait_for_finish()
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def test_96_mixing_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        vial_a = self.vial_a_var.get()
        vial_b = self.vial_b_var.get()
        diluent = self.diluent_var.get()
        if not vial_a or not vial_b or not diluent:
            messagebox.showerror("Config Error", "Please select Vial A, Vial B, and Diluent.")
            return
        plate_safe_z = self.resolve_coords(0, 0, PLATE_CONFIG["Z_SAFE"])[2]
        plate_asp_z = self.resolve_coords(0, 0, PLATE_CONFIG["Z_ASPIRATE"])[2]
        plate_disp_z = self.resolve_coords(0, 0, PLATE_CONFIG["Z_DISPENSE"])[2]
        falcon_safe_z = self.resolve_coords(0, 0, FALCON_RACK_CONFIG["Z_SAFE"])[2]
        falcon_asp_z = self.resolve_coords(0, 0, FALCON_RACK_CONFIG["Z_ASPIRATE"])[2]
        falcon_disp_z = self.resolve_coords(0, 0, FALCON_RACK_CONFIG["Z_DISPENSE"])[2]
        _4ml_safe_z = self.resolve_coords(0, 0, _4ML_RACK_CONFIG["Z_SAFE"])[2]
        _4ml_asp_z = self.resolve_coords(0, 0, _4ML_RACK_CONFIG["Z_ASPIRATE"])[2]
        _4ml_disp_z = self.resolve_coords(0, 0, _4ML_RACK_CONFIG["Z_DISPENSE"])[2]
        global_safe_z = self.resolve_coords(0, 0, GLOBAL_SAFE_Z_OFFSET)[2]
        AIR_GAP_UL = 200.0
        MAX_ASP_UL = 800.0
        e_gap_pos = -1 * AIR_GAP_UL * STEPS_PER_UL

        def get_source_coords_and_z(source_val):
            if source_val.startswith("4mL_"):
                pos_key = source_val.replace("4mL_", "")
                sx, sy = self.get_4ml_coordinates(pos_key)
                return sx, sy, _4ml_safe_z, _4ml_asp_z, _4ml_disp_z, "4ML"
            else:
                sx, sy = self.get_falcon_coordinates(source_val)
                return sx, sy, falcon_safe_z, falcon_asp_z, falcon_disp_z, "FALCON"

        def distribute_batch(source_vial, target_list, phase_name, submerged=False, start_mod="TIPS"):
            src_x, src_y, src_safe_z, src_asp_z, _, src_mod = get_source_coords_and_z(source_vial)
            target_z = plate_asp_z if submerged else plate_disp_z
            current_tip_vol = 0.0

            current_sim_mod = start_mod

            for task in target_list:
                well = task['well']
                vol_needed = task['vol']
                if vol_needed <= 0: continue
                if current_tip_vol < vol_needed:
                    self.last_cmd_var.set(f"{phase_name}: Refilling from {source_vial}...")
                    cmds = []
                    cmds.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")

                    cmds.extend(
                        self._get_smart_travel_gcode(src_mod, src_x, src_y, src_safe_z, start_module=current_sim_mod))

                    e_full = -1 * (AIR_GAP_UL + MAX_ASP_UL) * STEPS_PER_UL
                    cmds.append(f"G0 Z{src_asp_z:.2f} F{JOG_SPEED_Z}")
                    cmds.append(f"G1 E{e_full:.3f} F{PIP_SPEED}")
                    cmds.append(f"G0 Z{src_safe_z:.2f} F{JOG_SPEED_Z}")
                    self._send_lines_with_ok(cmds)
                    self.update_last_module(src_mod)
                    current_sim_mod = src_mod
                    current_tip_vol = MAX_ASP_UL

                self.last_cmd_var.set(f"{phase_name}: {vol_needed}uL -> {well}")
                dest_x, dest_y = self.get_well_coordinates(well)
                cmds_disp = []

                cmds_disp.extend(
                    self._get_smart_travel_gcode("PLATE", dest_x, dest_y, plate_safe_z, start_module=current_sim_mod))

                new_logical_vol = AIR_GAP_UL + current_tip_vol - vol_needed
                new_e_pos = -1 * new_logical_vol * STEPS_PER_UL
                cmds_disp.append(f"G0 Z{target_z:.2f} F{JOG_SPEED_Z}")
                cmds_disp.append(f"G1 E{new_e_pos:.3f} F{PIP_SPEED}")
                cmds_disp.append(f"G0 Z{plate_safe_z:.2f} F{JOG_SPEED_Z}")
                self._send_lines_with_ok(cmds_disp)
                self.update_last_module("PLATE")
                current_sim_mod = "PLATE"
                current_tip_vol -= vol_needed
            return current_sim_mod

        def run_seq():
            self.log_command("[MIXING] Starting 96 Well Mixing Sequence...")
            matrix_a = []
            for r_char in self.plate_rows:
                for c_idx in range(12):
                    vol = (c_idx + 1) * 5.0
                    matrix_a.append({'well': f"{r_char}{c_idx + 1}", 'vol': vol})
            matrix_b = []
            for r_idx, r_char in enumerate(self.plate_rows):
                vol = (r_idx + 1) * 5.0
                for c_idx in range(12):
                    matrix_b.append({'well': f"{r_char}{c_idx + 1}", 'vol': vol})
            matrix_dil = []
            for r_idx, r_char in enumerate(self.plate_rows):
                for c_idx in range(12):
                    vol_a = (c_idx + 1) * 5.0
                    vol_b = (r_idx + 1) * 5.0
                    needed = 500.0 - (vol_a + vol_b)
                    if needed > 0:
                        matrix_dil.append({'well': f"{r_char}{c_idx + 1}", 'vol': needed})

            current_sim_mod = self.last_known_module

            self.log_line("[MIXING] Phase 1: Distributing Diluent (Single Shot)")
            self._send_lines_with_ok(self._get_eject_tip_commands())
            self.update_last_module("EJECT")
            current_sim_mod = "EJECT"

            tip_key = self._find_next_available_tip()
            if not tip_key: return
            self._send_lines_with_ok(self._get_pick_tip_commands(tip_key, start_module=current_sim_mod))
            self.tip_inventory[tip_key] = False
            self.update_last_module("TIPS")
            current_sim_mod = "TIPS"

            self.root.after(0, self.update_tip_grid_colors)
            src_x, src_y, src_safe_z, src_asp_z, _, src_mod = get_source_coords_and_z(diluent)
            for task in matrix_dil:
                well = task['well']
                vol_needed = task['vol']
                self.last_cmd_var.set(f"Diluent: {vol_needed}uL -> {well}")
                cmds = []
                cmds.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")

                cmds.extend(
                    self._get_smart_travel_gcode(src_mod, src_x, src_y, src_safe_z, start_module=current_sim_mod))

                e_loaded = -1 * (AIR_GAP_UL + vol_needed) * STEPS_PER_UL
                cmds.append(f"G0 Z{src_asp_z:.2f} F{JOG_SPEED_Z}")
                cmds.append(f"G1 E{e_loaded:.3f} F{PIP_SPEED}")
                cmds.append(f"G0 Z{src_safe_z:.2f} F{JOG_SPEED_Z}")
                self.update_last_module(src_mod)
                current_sim_mod = src_mod

                dest_x, dest_y = self.get_well_coordinates(well)

                cmds.extend(
                    self._get_smart_travel_gcode("PLATE", dest_x, dest_y, plate_safe_z, start_module=current_sim_mod))

                e_blowout_target = -1 * 100.0 * STEPS_PER_UL
                cmds.append(f"G0 Z{plate_disp_z:.2f} F{JOG_SPEED_Z}")
                cmds.append(f"G1 E{e_blowout_target:.3f} F{PIP_SPEED}")
                cmds.append(f"G0 Z{plate_safe_z:.2f} F{JOG_SPEED_Z}")
                self._send_lines_with_ok(cmds)
                self.update_last_module("PLATE")
                current_sim_mod = "PLATE"

            self.log_line("[MIXING] Phase 2: Distributing Vial A (Batch)")
            self._send_lines_with_ok(self._get_eject_tip_commands())
            self.update_last_module("EJECT")
            current_sim_mod = "EJECT"

            tip_key = self._find_next_available_tip()
            if not tip_key: return
            self._send_lines_with_ok(self._get_pick_tip_commands(tip_key, start_module=current_sim_mod))
            self.tip_inventory[tip_key] = False
            self.update_last_module("TIPS")
            current_sim_mod = "TIPS"

            self.root.after(0, self.update_tip_grid_colors)
            current_sim_mod = distribute_batch(vial_a, matrix_a, "Vial A", submerged=True, start_mod=current_sim_mod)

            self.log_line("[MIXING] Phase 3: Distributing Vial B (Batch)")
            self._send_lines_with_ok(self._get_eject_tip_commands())
            self.update_last_module("EJECT")
            current_sim_mod = "EJECT"

            tip_key = self._find_next_available_tip()
            if not tip_key: return
            self._send_lines_with_ok(self._get_pick_tip_commands(tip_key, start_module=current_sim_mod))
            self.tip_inventory[tip_key] = False
            self.update_last_module("TIPS")
            current_sim_mod = "TIPS"

            self.root.after(0, self.update_tip_grid_colors)
            current_sim_mod = distribute_batch(vial_b, matrix_b, "Vial B", submerged=True, start_mod=current_sim_mod)

            self.log_line("[MIXING] Sequence Complete. Ejecting...")
            self._send_lines_with_ok(self._get_eject_tip_commands())
            self.update_last_module("EJECT")
            self.park_head_sequence()
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def generic_move_sequence(self, module_name, target_pos):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        if not target_pos: return
        x, y = 0.0, 0.0
        rel_safe_z = 0.0
        if module_name == "PLATE":
            x, y = self.get_well_coordinates(target_pos)
            rel_safe_z = PLATE_CONFIG["Z_SAFE"]
        elif module_name == "FALCON":
            x, y = self.get_falcon_coordinates(target_pos)
            rel_safe_z = FALCON_RACK_CONFIG["Z_SAFE"]
        elif module_name == "WASH":
            x, y = self.get_wash_coordinates(target_pos)
            rel_safe_z = WASH_RACK_CONFIG["Z_SAFE"]
        elif module_name == "4ML":
            x, y = self.get_4ml_coordinates(target_pos)
            rel_safe_z = _4ML_RACK_CONFIG["Z_SAFE"]
        elif module_name == "FILTER_EPPI":
            x, y = self.get_1x8_rack_coordinates(target_pos, FILTER_EPPI_RACK_CONFIG, "B")
            rel_safe_z = FILTER_EPPI_RACK_CONFIG["Z_SAFE"]
        elif module_name == "EPPI":
            x, y = self.get_1x8_rack_coordinates(target_pos, EPPI_RACK_CONFIG, "C")
            rel_safe_z = EPPI_RACK_CONFIG["Z_SAFE"]
        elif module_name == "HPLC":
            x, y = self.get_1x8_rack_coordinates(target_pos, HPLC_VIAL_RACK_CONFIG, "D")
            rel_safe_z = HPLC_VIAL_RACK_CONFIG["Z_SAFE"]
        elif module_name == "HPLC_INSERT":
            x, y = self.get_1x8_rack_coordinates(target_pos, HPLC_VIAL_INSERT_RACK_CONFIG, "E")
            rel_safe_z = HPLC_VIAL_INSERT_RACK_CONFIG["Z_SAFE"]
        elif module_name == "SCREWCAP":
            x, y = self.get_1x8_rack_coordinates(target_pos, SCREWCAP_VIAL_RACK_CONFIG, "F")
            rel_safe_z = SCREWCAP_VIAL_RACK_CONFIG["Z_SAFE"]
        abs_safe_z = self.resolve_coords(0, 0, rel_safe_z)[2]
        self.log_line(f"[SYSTEM] Moving to {module_name} : {target_pos}...")
        self.log_command(f"Move: {module_name} {target_pos}")

        commands = self._get_smart_travel_gcode(module_name, x, y, abs_safe_z)

        def run_seq():
            self.last_cmd_var.set(f"Moving to {module_name} {target_pos}...")
            self._send_lines_with_ok(commands)
            self._wait_for_finish()
            self.update_last_module(module_name)
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def run_calibration_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        try:
            current_ul = float(self.current_vol_var.get().strip())
            target_ul = float(self.target_vol_var.get().strip())
        except ValueError:
            messagebox.showerror("Input Error", "Please enter valid numbers.")
            return
        if current_ul < 0 or target_ul < 0:
            messagebox.showerror("Input Error", "Volume cannot be negative.")
            return
        current_e_pos = -1 * current_ul * STEPS_PER_UL
        target_e_pos = -1 * target_ul * STEPS_PER_UL
        self.log_line(f"[SYSTEM] Calibration: {current_ul}uL -> {target_ul}uL")
        self.log_command(f"Calibrate Pipette: {current_ul} -> {target_ul}uL")
        commands = [f"G92 E{current_e_pos:.3f}", f"G1 E{target_e_pos:.3f} F{MOVEMENT_SPEED}"]
        commands.extend(CALIBRATION_SETUP_GCODE)

        def run_seq():
            self.last_cmd_var.set("Calibrating Pipette Motor...")
            self._send_lines_with_ok(commands)
            self._wait_for_finish()
            self.current_pipette_volume = target_ul
            self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")
            self.live_vol_var.set(f"{self.current_pipette_volume:.1f}")
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()


def main():
    root = tk.Tk()
    try:
        style = ttk.Style()
        style.theme_use('clam')
    except:
        pass
    app = LiquidHandlerApp(root)
    root.protocol("WM_DELETE_WINDOW", lambda: (app.disconnect(), root.destroy()))
    root.mainloop()


if __name__ == "__main__":
    main()