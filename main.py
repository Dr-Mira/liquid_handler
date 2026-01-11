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
from datetime import datetime

# ==========================================
#           CONFIGURATION
# ==========================================

# --- CALIBRATION PIN ANCHOR (The Reference Point) ---
CALIBRATION_PIN_CONFIG_DEFAULT = {
    "PIN_X": 109.2,
    "PIN_Y": 114.3,
    "PIN_Z": 132.8
}

# Active Configuration (Will be overwritten by JSON if exists)
CALIBRATION_PIN_CONFIG = CALIBRATION_PIN_CONFIG_DEFAULT.copy()

# Center
GLOBAL_SAFE_Z_OFFSET = 62.2
SAFE_CENTER_X_OFFSET = 0.8
SAFE_CENTER_Y_OFFSET = -4.3

# Parking (Relative Coordinates)
PARK_HEAD_X = 79.5
PARK_HEAD_Y = 71.1
PARK_HEAD_Z = 27.4

# Pipette Constants
STEPS_PER_UL = 0.3019
DEFAULT_TARGET_UL = 200.0
MOVEMENT_SPEED = 1000
AIR_GAP_UL = 200

# Volatile Logic
# Rate of aspiration (uL/min) to counteract dripping
VOLATILE_DRIFT_RATE = 400
VOLATILE_MOVE_SPEED = 1500

# Limits
MIN_PIPETTE_VOL = 100.0
MAX_PIPETTE_VOL = 1000.0

# Manual Control Constants
JOG_SPEED_XY = 4000
JOG_SPEED_Z = 4000
PIP_SPEED = 2000

# Polling Settings
POLL_INTERVAL_MS = 1000
IDLE_TIMEOUT_BEFORE_POLL = 2.0

# --- EJECT STATION CONFIGURATION (Relative Offsets) ---
EJECT_STATION_CONFIG = {
    "APPROACH_X": 79.5,
    "APPROACH_Y": 71.1,
    "Z_SAFE": 57.2,
    "Z_EJECT_START": 27.4,
    "EJECT_TARGET_Y": 106.7,
    "Z_RETRACT": 57.2
}

# --- TIP RACK CONFIGURATION (Relative Offsets) ---
TIP_RACK_CONFIG = {
    "A1_X": 68.7, "A1_Y": 29.9,
    "F4_X": 99.0, "F4_Y": -19.9,
    "Z_TRAVEL": 52.2,
    "Z_PICK": -37.8,
}

# --- 96 WELL PLATE CONFIGURATION (Relative Offsets) ---
PLATE_CONFIG = {
    "A1_X": -104.8, "A1_Y": 112.7,
    "H12_X": -5.7, "H12_Y": 49.8,
    "Z_SAFE": -22.8,
    "Z_ASPIRATE": -55.5,
    "Z_DISPENSE": -35.5
}

# --- FALCON RACK CONFIGURATION (Relative Offsets) ---
FALCON_RACK_CONFIG = {
    "15ML_A1_X": -93.6, "15ML_A1_Y": 15.8,
    "15ML_B3_X": -55.4, "15ML_B3_Y": -4.3,
    "50ML_X": -30.9, "50ML_Y": 6.2,
    "Z_SAFE": 62.2,
    "Z_ASPIRATE": -58.5,
    "Z_DISPENSE": 47.2
}

# --- WASH STATION CONFIGURATION (Relative Offsets) ---
WASH_RACK_CONFIG = {
    "A1_X": 51.5, "A1_Y": -51.5,
    "B2_X": 86.9, "B2_Y": -85.9,
    "Z_SAFE": 62.2,
    "Z_ASPIRATE": -27.8,
    "Z_DISPENSE": 37.2
}

# --- 4 ML RACK CONFIGURATION (Relative Offsets) ---
_4ML_RACK_CONFIG = {
    "A1_X": -113.2, "A1_Y": -34.6,
    "A8_X": 12.4, "A8_Y": -34.6,
    "Z_SAFE": -11.2,
    "Z_ASPIRATE": -56.5,
    "Z_DISPENSE": -32.8
}

# --- FILTER EPPI RACK CONFIGURATION (Row B) (Relative Offsets) ---
FILTER_EPPI_RACK_CONFIG = {
    "B1_X": -113.2, "B1_Y": -51.2,
    "B8_X": 12.4, "B8_Y": -51.2,
    "Z_SAFE": -11.2,
    "Z_ASPIRATE": -39.2,
    "Z_DISPENSE": -22.8
}

# --- EPPI RACK CONFIGURATION (Row C) (Relative Offsets) ---
EPPI_RACK_CONFIG = {
    "C1_X": -113.2, "C1_Y": -65.7,
    "C8_X": 12.4, "C8_Y": -65.7,
    "Z_SAFE": -11.2,
    "Z_ASPIRATE": -60.8,
    "Z_DISPENSE": -27.8
}

# --- HPLC VIAL RACK CONFIGURATION (Row D) (Relative Offsets) ---
HPLC_VIAL_RACK_CONFIG = {
    "D1_X": -113.2, "D1_Y": -81.7,
    "D8_X": 12.4, "D8_Y": -81.7,
    "Z_SAFE": -11.2,
    "Z_ASPIRATE": -44.2,
    "Z_DISPENSE": -22.8
}

# --- HPLC VIAL INSERT RACK CONFIGURATION (Row E) (Relative Offsets) ---
HPLC_VIAL_INSERT_RACK_CONFIG = {
    "E1_X": -113.1, "E1_Y": -97.3,
    "E8_X": 12.4, "E8_Y": -97.3,
    "Z_SAFE": -11.2,
    "Z_ASPIRATE": -27.3,
    "Z_DISPENSE": -22.8
}

# --- SCREWCAP VIAL RACK CONFIGURATION (Row F) (Relative Offsets) ---
SCREWCAP_VIAL_RACK_CONFIG = {
    "F1_X": -113.2, "F1_Y": -112.8,
    "F8_X": 12.4, "F8_Y": -112.8,
    "Z_SAFE": -11.2,
    "Z_ASPIRATE": -58.9,
    "Z_DISPENSE": -27.8
}

# --- MODULE GROUPS FOR OPTIMIZATION ---
# Modules that share the same low Z clearance (-11.2)
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


# ==========================================
#           MAIN APPLICATION
# ==========================================

class LiquidHandlerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Mira Liquid Handler")
        self.root.geometry("1024x600")
        self.root.resizable(False, False)  # Mandatory size

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

        # --- STATE TRACKING ---
        self.last_known_module = "Unknown"
        # Raw coordinates for saving calibration
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
        self.wash_positions = ["Wash A", "Wash B", "Waste A", "Waste B"]
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

        self._build_ui()
        self._poll_rx_queue()
        self.refresh_ports()
        self._poll_position_loop()
        self.root.after(500, self.attempt_auto_connect)

        # --- START POSITION LOGGING THREAD ---
        self.pos_log_thread = threading.Thread(target=self._position_logger_loop, daemon=True)
        self.pos_log_thread.start()

    def load_calibration_config(self):
        global CALIBRATION_PIN_CONFIG
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, "r") as f:
                    config = json.load(f)
                    if all(k in config for k in ["PIN_X", "PIN_Y", "PIN_Z"]):
                        CALIBRATION_PIN_CONFIG.update(config)
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
        ttk.Separator(left_col, orient="horizontal").pack(fill="x", pady=10)
        ttk.Label(left_col, text="Absolute Motion Calibration", font=("Arial", 10, "bold")).pack(pady=2)
        ttk.Button(left_col, text="Calibrate", command=self.start_pin_calibration_sequence).pack(fill="x", pady=5)

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

        header_frame = ttk.Frame(frame)
        header_frame.pack(fill="x", pady=(0, 5))

        # UPDATED: Split Source into Module and Position
        cols = [
            ("Execute", 8), ("Line", 4), ("Source Mod", 12), ("Source Pos", 10),
            ("Dest Vial", 15), ("Vol (uL)", 8), ("Volatile", 8),
            ("Wash Vol", 8), ("Wash Times", 8), ("Wash Source", 12)
        ]

        for idx, (text, w) in enumerate(cols):
            ttk.Label(header_frame, text=text, width=w, font=("Arial", 9, "bold")).grid(row=0, column=idx, padx=2)

        # Options for Destination (still uses the flat list for now to save space, or could be split too, but prompt focused on Source)
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
                "wash_src": tk.StringVar(value="Wash A")
            }

            row_frame = ttk.Frame(frame)
            row_frame.pack(fill="x", pady=2)

            ttk.Checkbutton(row_frame, variable=row_vars["execute"], width=8).pack(side="left", padx=2)
            ttk.Label(row_frame, text=f"{i + 1}", width=4).pack(side="left", padx=2)

            # Source Module Combobox
            cb_mod = ttk.Combobox(
                row_frame, textvariable=row_vars["src_mod"],
                values=module_names, width=12, state="readonly"
            )
            cb_mod.pack(side="left", padx=2)

            # Source Position Combobox
            cb_pos = ttk.Combobox(row_frame, textvariable=row_vars["src_pos"], width=10, state="readonly")
            cb_pos.pack(side="left", padx=2)

            # Store widget ref so presets can set module+pos cleanly
            row_vars["_src_pos_combo"] = cb_pos

            # Bind event to update positions when module changes
            cb_mod.bind(
                "<<ComboboxSelected>>",
                lambda e, m=row_vars["src_mod"], p=cb_pos, v=row_vars["src_pos"]: self._update_source_pos_options(m, p,
                                                                                                                  v)
            )

            # Initialize positions for the default value
            self._update_source_pos_options(row_vars["src_mod"], cb_pos, row_vars["src_pos"])

            # Dest Vial
            ttk.Combobox(
                row_frame, textvariable=row_vars["dest"],
                values=dest_options, width=15, state="readonly"
            ).pack(side="left", padx=2)

            # Volume
            ttk.Combobox(
                row_frame, textvariable=row_vars["vol"],
                values=vol_options, width=8, state="readonly"
            ).pack(side="left", padx=2)

            # Volatile Checkbox
            chk_volatile = ttk.Checkbutton(row_frame, variable=row_vars["volatile"], text="Volatile", width=10)
            chk_volatile.pack(side="left", padx=2)

            # Wash Volume
            cb_wash_vol = ttk.Combobox(
                row_frame, textvariable=row_vars["wash_vol"],
                values=wash_vol_options_std, width=8, state="readonly"
            )
            cb_wash_vol.pack(side="left", padx=2)

            def update_wash_options(var_name, index, mode, cb=cb_wash_vol, rv=row_vars):
                if rv["volatile"].get():
                    cb['values'] = wash_vol_options_volatile
                    if rv["wash_vol"].get() not in wash_vol_options_volatile:
                        rv["wash_vol"].set("0")
                else:
                    cb['values'] = wash_vol_options_std

            row_vars["volatile"].trace_add("write", update_wash_options)

            # Wash Times
            ttk.Combobox(
                row_frame, textvariable=row_vars["wash_times"],
                values=wash_times_options, width=8, state="readonly"
            ).pack(side="left", padx=2)

            # Wash Source
            ttk.Combobox(
                row_frame, textvariable=row_vars["wash_src"],
                values=dest_options, width=12, state="readonly"
            ).pack(side="left", padx=2)

            self.transfer_rows.append(row_vars)

        btn_frame = ttk.Frame(frame, padding=10)
        btn_frame.pack(fill="x", pady=10)

        ttk.Button(
            btn_frame, text="EXECUTE TRANSFER SEQUENCE",
            command=lambda: threading.Thread(target=self.transfer_liquid_sequence, daemon=True).start()
        ).pack(fill="x", ipady=5)

        # ---- PRESETS: single horizontal line ----
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
        """Helper so you can type numbers as 500 or '500' inside presets."""
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
        """
        Sets Source Module + updates Source Pos combobox values + selects desired position if valid.
        """
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
        """
        Applies a full-table preset (up to 8 rows).

        Each preset row dict supports keys:
          execute (bool)
          src_mod (str)    -> must match Source Mod dropdown (e.g. "4mL Rack", "Falcon Rack", "Wash Station", ...)
          src_pos (str)    -> position for that module (e.g. "A1", "B3", "Wash A", "H12", ...)
          dest (str)       -> must match Dest Vial dropdown (e.g. "Falcon A1", "Filter Eppi B1", "Wash A", ...)
          vol (str/int)    -> e.g. "500" or 500
          volatile (bool)
          wash_vol (str/int)
          wash_times (str/int)
          wash_src (str)   -> must match Wash Source dropdown (same options as Dest Vial)
        """
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

            # Apply in safe order (volatile first so wash options update correctly)
            row_vars["execute"].set(execute)
            self._set_transfer_row_source(row_vars, src_mod, src_pos)
            row_vars["dest"].set(dest)
            row_vars["vol"].set(vol)

            row_vars["volatile"].set(volatile)
            row_vars["wash_vol"].set(wash_vol)
            row_vars["wash_times"].set(wash_times)
            row_vars["wash_src"].set(wash_src)

        # Optional: log to your UI log
        try:
            if preset_name:
                self.log_line(f"[UI] Transfer preset loaded: {preset_name}")
        except Exception:
            pass

    def load_transfer_preset_1(self):
        preset = [
            {"execute": False, "src_mod": "4mL Rack", "src_pos": "A1", "dest": "Filter Eppi B1", "vol": 600, "volatile": True, "wash_vol": 150, "wash_times": 2, "wash_src": "Wash B"},
            {"execute": False, "src_mod": "4mL Rack", "src_pos": "A2", "dest": "Filter Eppi B2", "vol": 600, "volatile": True, "wash_vol": 150, "wash_times": 2, "wash_src": "Wash B"},
            {"execute": False, "src_mod": "4mL Rack", "src_pos": "A3", "dest": "Filter Eppi B3", "vol": 600, "volatile": True, "wash_vol": 150, "wash_times": 2, "wash_src": "Wash B"},
            {"execute": False, "src_mod": "4mL Rack", "src_pos": "A4", "dest": "Filter Eppi B4", "vol": 600, "volatile": True, "wash_vol": 150, "wash_times": 2, "wash_src": "Wash B"},
            {"execute": False, "src_mod": "4mL Rack", "src_pos": "A5", "dest": "Filter Eppi B5", "vol": 600, "volatile": True, "wash_vol": 150, "wash_times": 2, "wash_src": "Wash B"},
            {"execute": False, "src_mod": "4mL Rack", "src_pos": "A6", "dest": "Filter Eppi B6", "vol": 600, "volatile": True, "wash_vol": 150, "wash_times": 2, "wash_src": "Wash B"},
            {"execute": False, "src_mod": "4mL Rack", "src_pos": "A7", "dest": "Filter Eppi B7", "vol": 600, "volatile": True, "wash_vol": 150, "wash_times": 2, "wash_src": "Wash B"},
            {"execute": False, "src_mod": "4mL Rack", "src_pos": "A8", "dest": "Filter Eppi B8", "vol": 600, "volatile": True, "wash_vol": 150, "wash_times": 2, "wash_src": "Wash B"},
        ]
        self._apply_transfer_table_preset(preset, preset_name="Preset 1")

    def load_transfer_preset_2(self):
        preset = [
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C1", "dest": "Filter Eppi B1", "vol": 800, "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C2", "dest": "Filter Eppi B2", "vol": 800, "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C3", "dest": "Filter Eppi B3", "vol": 800, "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C4", "dest": "Filter Eppi B4", "vol": 800, "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C5", "dest": "Filter Eppi B5", "vol": 800, "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C6", "dest": "Filter Eppi B6", "vol": 800, "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C7", "dest": "Filter Eppi B7", "vol": 800, "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C8", "dest": "Filter Eppi B8", "vol": 800, "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
        ]
        self._apply_transfer_table_preset(preset, preset_name="Preset 2")

    def load_transfer_preset_3(self):
        preset = [
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C1", "dest": "HPLC D1", "vol": 800, "volatile": False, "wash_vol": 100, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C2", "dest": "HPLC D2", "vol": 800, "volatile": False, "wash_vol": 100, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C3", "dest": "HPLC D3", "vol": 800, "volatile": False, "wash_vol": 100, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C4", "dest": "HPLC D4", "vol": 800, "volatile": False, "wash_vol": 100, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C5", "dest": "HPLC D5", "vol": 800, "volatile": False, "wash_vol": 100, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C6", "dest": "HPLC D6", "vol": 800, "volatile": False, "wash_vol": 100, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C7", "dest": "HPLC D7", "vol": 800, "volatile": False, "wash_vol": 100, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Eppi Rack", "src_pos": "C8", "dest": "HPLC D8", "vol": 800, "volatile": False, "wash_vol": 100, "wash_times": 1, "wash_src": "Wash A"},
        ]
        self._apply_transfer_table_preset(preset, preset_name="Preset 3")

    def load_transfer_preset_4(self):
        preset = [
            {"execute": False, "src_mod": "Falcon Rack", "src_pos": "A1", "dest": "Filter Eppi A1", "vol": 800, "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Falcon Rack", "src_pos": "A2", "dest": "Filter Eppi A2", "vol": 800, "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Falcon Rack", "src_pos": "A3", "dest": "Filter Eppi A3", "vol": 800, "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Falcon Rack", "src_pos": "A4", "dest": "Filter Eppi A4", "vol": 800, "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Falcon Rack", "src_pos": "A5", "dest": "Filter Eppi A5", "vol": 800, "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Falcon Rack", "src_pos": "A6", "dest": "Filter Eppi A6", "vol": 800, "volatile": False, "wash_vol": 200, "wash_times": 1, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "", "src_pos": "", "dest": "", "vol": 0, "volatile": False, "wash_vol": 0, "wash_times": 2, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "", "src_pos": "", "dest": "", "vol": 0, "volatile": False, "wash_vol": 0, "wash_times": 2, "wash_src": "Wash A"},
        ]
        self._apply_transfer_table_preset(preset, preset_name="Preset 4")

    def load_transfer_preset_5(self):
        preset = [
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F1", "dest": "HPLC Insert E1", "vol": 20, "volatile": False, "wash_vol": 0, "wash_times": 0, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F2", "dest": "HPLC Insert E2", "vol": 20, "volatile": False, "wash_vol": 0, "wash_times": 0, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F3", "dest": "HPLC Insert E3", "vol": 20, "volatile": False, "wash_vol": 0, "wash_times": 0, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F4", "dest": "HPLC Insert E4", "vol": 20, "volatile": False, "wash_vol": 0, "wash_times": 0, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F5", "dest": "HPLC Insert E5", "vol": 20, "volatile": False, "wash_vol": 0, "wash_times": 0, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F6", "dest": "HPLC Insert E6", "vol": 20, "volatile": False, "wash_vol": 0, "wash_times": 0, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F7", "dest": "HPLC Insert E7", "vol": 20, "volatile": False, "wash_vol": 0, "wash_times": 0, "wash_src": "Wash A"},
            {"execute": False, "src_mod": "Screwcap Vial", "src_pos": "F8", "dest": "HPLC Insert E8", "vol": 20, "volatile": False, "wash_vol": 0, "wash_times": 0, "wash_src": "Wash A"},
        ]
        self._apply_transfer_table_preset(preset, preset_name="Preset 5")

    def _build_combine_fractions_tab(self, parent):
        frame = ttk.Frame(parent, padding=10)
        frame.pack(fill="both", expand=True)
        header_frame = ttk.Frame(frame)
        header_frame.pack(fill="x", pady=(0, 5))

        # Updated Columns: Added Execute, Wash Vol, Wash Times, Wash Source
        cols = [
            ("Execute", 8), ("Line", 6), ("Source Start", 12), ("Source End", 12),
            ("Dest Falcon", 12), ("Vol (uL)", 10),
            ("Wash Vol", 8), ("Wash Times", 8), ("Wash Source", 12)
        ]

        for idx, (text, w) in enumerate(cols):
            ttk.Label(header_frame, text=text, width=w, font=("Arial", 9, "bold")).grid(row=0, column=idx, padx=2)

        self.combine_rows = []
        vol_options = [str(x) for x in range(100, 1700, 100)]
        default_falcons = ["A1", "A2", "A3", "B1", "B2", "B3"]
        wash_vol_options = ["0"] + [str(x) for x in range(100, 900, 100)]
        wash_times_options = [str(x) for x in range(1, 6)]
        source_options = self.wash_positions + [f"Falcon {p}" for p in self.falcon_positions]

        for i in range(6):
            row_vars = {
                "execute": tk.BooleanVar(value=False),
                "start": tk.StringVar(),
                "end": tk.StringVar(),
                "dest": tk.StringVar(),
                "vol": tk.StringVar(value="600"),
                "wash_vol": tk.StringVar(value="0"),
                "wash_times": tk.StringVar(value="1"),
                "wash_src": tk.StringVar(value="Wash A")
            }
            row_frame = ttk.Frame(frame)
            row_frame.pack(fill="x", pady=2)

            ttk.Checkbutton(row_frame, variable=row_vars["execute"], width=8).pack(side="left", padx=2)
            ttk.Label(row_frame, text=f"Line {i + 1}", width=6).pack(side="left")
            ttk.Combobox(row_frame, textvariable=row_vars["start"], values=self.plate_wells, width=10,
                         state="readonly").pack(side="left", padx=2)
            ttk.Combobox(row_frame, textvariable=row_vars["end"], values=self.plate_wells, width=10,
                         state="readonly").pack(side="left", padx=2)
            cb_dest = ttk.Combobox(row_frame, textvariable=row_vars["dest"], values=self.falcon_positions, width=10,
                                   state="readonly")
            cb_dest.pack(side="left", padx=2)
            if i < len(default_falcons): row_vars["dest"].set(default_falcons[i])
            cb_dest.bind("<<ComboboxSelected>>", lambda e: self._update_falcon_exclusivity())
            ttk.Combobox(row_frame, textvariable=row_vars["vol"], values=vol_options, width=8, state="readonly").pack(
                side="left", padx=2)

            # Wash Widgets
            ttk.Combobox(row_frame, textvariable=row_vars["wash_vol"], values=wash_vol_options, width=8,
                         state="readonly").pack(side="left", padx=2)
            ttk.Combobox(row_frame, textvariable=row_vars["wash_times"], values=wash_times_options, width=8,
                         state="readonly").pack(side="left", padx=2)
            ttk.Combobox(row_frame, textvariable=row_vars["wash_src"], values=source_options, width=12,
                         state="readonly").pack(side="left", padx=2)

            self.combine_rows.append({"vars": row_vars, "widgets": {"dest": cb_dest}})

        self._update_falcon_exclusivity()
        btn_frame = ttk.Frame(frame, padding=10)
        btn_frame.pack(fill="x", pady=10)
        ttk.Button(btn_frame, text="RUN COMBINE SEQUENCE",
                   command=lambda: threading.Thread(target=self.combine_fractions_sequence, daemon=True).start()).pack(
            fill="x", ipady=5)
        ttk.Label(btn_frame,
                  text="Check 'Execute' box for lines you want to run.",
                  font=("Arial", 8, "italic")).pack(pady=5)

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
                # These are updated by _parse_coordinates when available,
                # or maintained by the script logic.
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
        threading.Thread(target=self._run_startup_sequence, daemon=True).start()

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
            # Join isn't strictly necessary for daemon threads on exit, but good practice
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

    def _send_lines_with_ok(self, lines):
        self.is_sequence_running = True
        self.last_action_time = time.time()
        try:
            for line in lines:
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
        finally:
            self.is_sequence_running = False
            self.last_action_time = time.time()
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
        """Helper to reconstruct the string expected by _parse_combo_string from UI selections"""
        if mod_name == "96 Well Plate": return f"PLATE {pos_name}"
        if mod_name == "Falcon Rack": return f"Falcon {pos_name}"
        if mod_name == "4mL Rack": return f"4mL {pos_name}"
        if mod_name == "Filter Eppi": return f"Filter Eppi {pos_name}"
        if mod_name == "Eppi Rack": return f"Eppi {pos_name}"
        if mod_name == "HPLC Vial": return f"HPLC {pos_name}"
        if mod_name == "HPLC Insert": return f"HPLC Insert {pos_name}"
        if mod_name == "Screwcap Vial": return f"Screwcap {pos_name}"
        if mod_name == "Wash Station": return pos_name  # Wash positions are "Wash A", etc.
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
        mapping = {"Wash A": (0, 0), "Wash B": (1, 0), "Waste A": (0, 1), "Waste B": (1, 1)}
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

    def send_resume(self):
        if not self.ser or not self.ser.is_open: return
        self.log_line("[MANUAL] Sending Resume (M108)...")
        self._send_raw("M108\n")

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
        """
        Generates G-Code to park the head.
        Logic:
        1. Calculate Absolute Park Coordinates (Relative + Pin).
        2. Calculate Global Safe Z (Relative + Pin).
        3. Move Z to Global Safe Z.
        4. Move XY to Park XY.
        5. Move Z to Park Z.
        """
        abs_park_x, abs_park_y, abs_park_z = self.resolve_coords(PARK_HEAD_X, PARK_HEAD_Y, PARK_HEAD_Z)
        _, _, abs_global_safe_z = self.resolve_coords(0, 0, GLOBAL_SAFE_Z_OFFSET)

        return [
            "G90",  # Absolute positioning
            f"G0 Z{abs_global_safe_z:.2f} F{JOG_SPEED_Z}",  # Move to Safe Z first
            f"G0 X{abs_park_x:.2f} Y{abs_park_y:.2f} F{JOG_SPEED_XY}",  # Move to Park XY
            f"G0 Z{abs_park_z:.2f} F{JOG_SPEED_Z}"  # Move down to Park Z
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

        # Homing command followed by disabling extruder
        commands = [home_cmd, "M18 E"]

        # Append Parking Sequence (Safe Z -> Park XY -> Park Z)
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

        # Use the helper method to get the correct sequence
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

        # Use the provided start_module (from pre-calculated sequence) if available,
        # otherwise fall back to the last known state (for manual moves).
        current_mod = start_module if start_module is not None else self.last_known_module

        # --- OPTIMIZATION LOGIC FOR Z HEIGHT ---
        # If moving between two small vial modules, use the lower safe Z
        use_optimized_z = (
                current_mod in SMALL_VIAL_MODULES and
                target_module in SMALL_VIAL_MODULES
        )

        # Calculate the travel Z height
        if use_optimized_z:
            # All small modules share -11.2 relative safe Z
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
        commands.append(f"G0 Y{abs_target_y:.2f} F250")
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

        # Append Parking Sequence for Manual Eject
        commands.extend(self._get_park_head_commands())

        def run_seq():
            self.last_cmd_var.set("Ejecting Tip...")
            self._send_lines_with_ok(commands)
            self._wait_for_finish()
            self.update_last_module("EJECT")
            # Since we parked, update to PARK
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
        # When picking manually, we use self.last_known_module (default)
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

    def transfer_liquid_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        tasks = []
        for idx, row in enumerate(self.transfer_rows):
            if not row["execute"].get():
                continue

            vol_str = row["vol"].get()
            wash_vol_str = row["wash_vol"].get()
            wash_times_str = row["wash_times"].get()
            try:
                vol = float(vol_str)
                wash_vol = float(wash_vol_str)
                wash_times = int(wash_times_str)
            except ValueError:
                continue

            # Construct the source string from the two dropdowns
            src_mod_name = row["src_mod"].get()
            src_pos_name = row["src_pos"].get()
            full_source_str = self._construct_combo_string(src_mod_name, src_pos_name)

            tasks.append({
                "line": idx + 1,
                "source": full_source_str,
                "dest": row["dest"].get(),
                "vol": vol,
                "volatile": row["volatile"].get(),
                "wash_vol": wash_vol,
                "wash_times": wash_times,
                "wash_src": row["wash_src"].get()
            })

        if not tasks:
            messagebox.showinfo("No Tasks", "No lines selected for execution.")
            return

        self.log_command(f"[TRANSFER] Starting sequence with {len(tasks)} lines.")
        e_gap_pos = -1 * AIR_GAP_UL * STEPS_PER_UL
        MAX_STD_BATCH = 800.0
        MAX_VOLATILE_BATCH = 600.0

        def run_seq():
            # Initialize simulated state tracker
            current_simulated_module = self.last_known_module

            for task in tasks:
                line_num = task["line"]
                self.log_line(f"--- Processing Line {line_num} ---")
                self.last_cmd_var.set(f"Line {line_num}: Processing...")

                self.log_line(f"[L{line_num}] Ejecting existing tip...")
                self._send_lines_with_ok(self._get_eject_tip_commands())
                self.update_last_module("EJECT")
                current_simulated_module = "EJECT"

                tip_key = self._find_next_available_tip()
                if not tip_key:
                    messagebox.showerror("No Tips", f"Ran out of tips at Line {line_num}.")
                    return

                self.log_line(f"[L{line_num}] Picking Tip {tip_key}...")
                # Pass current_simulated_module to ensure correct Z travel logic
                self._send_lines_with_ok(self._get_pick_tip_commands(tip_key, start_module=current_simulated_module))
                self.tip_inventory[tip_key] = False
                self.update_last_module("TIPS")
                current_simulated_module = "TIPS"

                self.root.after(0, self.update_tip_grid_colors)
                total_vol = task["vol"]
                is_volatile = task["volatile"]
                max_batch = MAX_VOLATILE_BATCH if is_volatile else MAX_STD_BATCH
                remaining_vol = total_vol

                while remaining_vol > 0:
                    batch_vol = min(remaining_vol, max_batch)
                    self.log_line(
                        f"[L{line_num}] Transferring {batch_vol}uL ({'Volatile' if is_volatile else 'Standard'})")

                    # Pass state to transfer function and receive new state
                    current_simulated_module = self._perform_single_transfer(
                        task["source"], task["dest"], batch_vol, is_volatile, e_gap_pos,
                        AIR_GAP_UL, start_module=current_simulated_module
                    )
                    remaining_vol -= batch_vol

                wash_vol = task["wash_vol"]
                wash_times = task["wash_times"]

                if wash_vol > 0:
                    for i in range(wash_times):
                        self.log_line(f"[L{line_num}] Wash Cycle {i + 1}/{wash_times} ({wash_vol}uL)...")

                        # Perform wash cycle. This function handles Eject -> Pick -> Transfer -> Eject
                        # It returns "EJECT" as the final state.
                        current_simulated_module = self._perform_wash_cycle(
                            task["wash_src"], task["source"], task["dest"], wash_vol, is_volatile,
                            e_gap_pos, AIR_GAP_UL, start_module=current_simulated_module
                        )

            self._send_lines_with_ok(self._get_eject_tip_commands())
            self.log_command("[TRANSFER] All lines complete. Parking.")
            self.park_head_sequence()
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def _perform_single_transfer(self, source_str, dest_str, vol, is_volatile, e_gap_pos, air_gap_ul,
                                 start_module=None):
        src_mod, src_x, src_y, src_safe_z, src_asp_z, _ = self.get_coords_from_combo(source_str)
        dest_mod, dest_x, dest_y, dest_safe_z, _, dest_disp_z = self.get_coords_from_combo(dest_str)
        global_safe_z = self.resolve_coords(0, 0, GLOBAL_SAFE_Z_OFFSET)[2]

        # Use the passed start_module to determine optimization path
        current_mod_tracker = start_module if start_module is not None else self.last_known_module

        # --- Z OPTIMIZATION LOGIC FOR SOURCE APPROACH ---
        use_optimized_z_src = (current_mod_tracker in SMALL_VIAL_MODULES and src_mod in SMALL_VIAL_MODULES)
        # Use the constant defined for 4ML racks (which is -11.2) as the shared low Z height
        travel_z_src = self.resolve_coords(0, 0, _4ML_RACK_CONFIG["Z_SAFE"])[
            2] if use_optimized_z_src else global_safe_z

        cmds = []

        # 1. Move to Source (Standard Absolute Moves)
        cmds.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")

        # Logic to decide if we need to go to safe Z first or direct
        if current_mod_tracker == src_mod:
            cmds.append(f"G0 Z{src_safe_z:.2f} F{JOG_SPEED_Z}")
            cmds.append(f"G0 X{src_x:.2f} Y{src_y:.2f} F{JOG_SPEED_XY}")
        else:
            cmds.append(f"G0 Z{travel_z_src:.2f} F{JOG_SPEED_Z}")
            cmds.append(f"G0 X{src_x:.2f} Y{src_y:.2f} F{JOG_SPEED_XY}")
            cmds.append(f"G0 Z{src_safe_z:.2f} F{JOG_SPEED_Z}")

        # 2. Aspirate
        e_loaded_pos = -1 * (air_gap_ul + vol) * STEPS_PER_UL
        cmds.append(f"G0 Z{src_asp_z:.2f} F{JOG_SPEED_Z}")

        # --- PRE-WETTING / MIXING (Only if Volatile) ---
        if is_volatile:
            mix_vol = 500.0
            e_mix_down = -1 * (air_gap_ul + mix_vol) * STEPS_PER_UL
            e_mix_up = -1 * (air_gap_ul) * STEPS_PER_UL

            self.log_line(f"[VOLATILE] Pre-wetting/Mixing source 3 times...")
            for _ in range(2):
                cmds.append(f"G1 E{e_mix_down:.3f} F{PIP_SPEED}")
                cmds.append(f"G1 E{e_mix_up:.3f} F{PIP_SPEED}")

        # Perform final aspiration
        cmds.append(f"G1 E{e_loaded_pos:.3f} F{PIP_SPEED}")

        # Send the preparation commands before the complex travel
        self._send_lines_with_ok(cmds)

        # --- Z OPTIMIZATION LOGIC FOR DESTINATION ---
        # CRITICAL FIX: We are now physically at 'src_mod'.
        # We must compare 'src_mod' vs 'dest_mod' to decide if we can stay low.
        use_optimized_z_dest = (src_mod in SMALL_VIAL_MODULES and dest_mod in SMALL_VIAL_MODULES)

        # Use the constant defined for 4ML racks (which is -11.2) as the shared low Z height
        travel_z_dest = self.resolve_coords(0, 0, _4ML_RACK_CONFIG["Z_SAFE"])[
            2] if use_optimized_z_dest else global_safe_z

        # 3. TRAVEL TO DESTINATION (The Complex Part)
        if is_volatile:
            self.log_line("[VOLATILE] Performing synchronized relative travel...")

            # A. Calculate Distances (Deltas)
            # 1. Lift out of vial (Target Height - Current Height)
            dz_lift = travel_z_dest - src_asp_z

            # 2. XY Travel
            dx = dest_x - src_x
            dy = dest_y - src_y
            dist_xy = math.sqrt(dx ** 2 + dy ** 2)

            # 3. Drop into dest (Target Height - Current Height)
            dz_drop = dest_safe_z - travel_z_dest

            # B. Calculate E-Drift based on F200 speed
            drift_per_min = VOLATILE_DRIFT_RATE * STEPS_PER_UL

            t_lift = abs(dz_lift) / VOLATILE_MOVE_SPEED
            e_drift_lift = drift_per_min * t_lift

            t_xy = dist_xy / VOLATILE_MOVE_SPEED
            e_drift_xy = drift_per_min * t_xy

            t_drop = abs(dz_drop) / VOLATILE_MOVE_SPEED
            e_drift_drop = drift_per_min * t_drop

            # Total drift to update our internal absolute tracker later
            total_drift_steps = e_drift_lift + e_drift_xy + e_drift_drop

            # C. Construct Relative Commands
            volatile_cmds = [
                "G91",  # Switch to Relative Positioning

                # Move 1: Lift Z
                f"G1 Z{dz_lift:.2f} E-{e_drift_lift:.3f} F{VOLATILE_MOVE_SPEED}",

                # Move 2: Travel XY
                f"G1 X{dx:.2f} Y{dy:.2f} E-{e_drift_xy:.3f} F{VOLATILE_MOVE_SPEED}",

                # Move 3: Drop Z
                f"G1 Z{dz_drop:.2f} E-{e_drift_drop:.3f} F{VOLATILE_MOVE_SPEED}",

                "G90"  # Switch back to Absolute Positioning
            ]

            self._send_lines_with_ok(volatile_cmds)

            # IMPORTANT: Update the python tracker for E!
            e_loaded_pos -= total_drift_steps

        else:
            # Standard Non-Volatile Move
            cmds_std = []
            cmds_std.append(f"G0 Z{src_safe_z:.2f} F{JOG_SPEED_Z}")  # Lift out
            cmds_std.append(f"G0 Z{travel_z_dest:.2f} F{JOG_SPEED_Z}")  # Go to travel height
            cmds_std.append(f"G0 X{dest_x:.2f} Y{dest_y:.2f} F{JOG_SPEED_XY}")  # XY
            cmds_std.append(f"G0 Z{dest_safe_z:.2f} F{JOG_SPEED_Z}")  # Drop
            self._send_lines_with_ok(cmds_std)

        self.update_last_module(dest_mod)
        current_mod_tracker = dest_mod

        # 4. Dispense
        e_blowout_pos = -1 * 100.0 * STEPS_PER_UL
        cmds_disp = []

        # If volatile, we might want to drift during the final plunge to dispense height too
        if is_volatile:
            # Calculate plunge to dispense height
            dz_final = dest_disp_z - dest_safe_z  # Negative
            t_final = abs(dz_final) / VOLATILE_MOVE_SPEED
            e_drift_final = drift_per_min * t_final

            # Execute relative plunge
            cmds_disp.append("G91")
            cmds_disp.append(f"G1 Z{dz_final:.2f} E-{e_drift_final:.3f} F{VOLATILE_MOVE_SPEED}")
            cmds_disp.append("G90")

            # Update tracker again
            e_loaded_pos -= e_drift_final
        else:
            cmds_disp.append(f"G0 Z{dest_disp_z:.2f} F{JOG_SPEED_Z}")

        # Final Blowout (Absolute move)
        cmds_disp.append(f"G1 E{e_blowout_pos:.3f} F{PIP_SPEED}")
        cmds_disp.append(f"G0 Z{dest_safe_z:.2f} F{JOG_SPEED_Z}")
        cmds_disp.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")

        self._send_lines_with_ok(cmds_disp)
        self.current_pipette_volume = AIR_GAP_UL
        self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")

        return current_mod_tracker

    def _perform_wash_cycle(self, wash_src_str, original_src_str, dest_str, vol, is_volatile, e_gap_pos, air_gap_ul,
                            start_module=None):
        """
        Robust Wash Cycle:
        1. Eject dirty tip (if not already ejected).
        2. Pick new tip.
        3. Aspirate Wash Liquid.
        4. Move to Source Vial.
        5. Dispense Wash into Source.
        6. Mix (Up/Down loop).
        7. Aspirate everything.
        8. Move to Dest.
        9. Dispense.
        10. Eject.
        """

        # --- 1. Eject Old Tip ---
        if start_module != "EJECT":
            self.log_line("[WASH] Ejecting dirty tip...")
            self._send_lines_with_ok(self._get_eject_tip_commands())
            self.update_last_module("EJECT")
        else:
            self.log_line("[WASH] Tip already ejected, skipping redundant eject.")

        # --- 2. Pick New Tip ---
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

        # --- 3. Aspirate Wash Liquid ---
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

        # --- 4. Move to Source Vial ---
        s_mod, s_x, s_y, s_safe_z, s_asp_z, s_disp_z = self.get_coords_from_combo(original_src_str)
        cmds_src = []
        cmds_src.extend(self._get_smart_travel_gcode(s_mod, s_x, s_y, s_safe_z, start_module=current_mod_tracker))

        # --- 5. Dispense Wash into Source ---
        # We dispense at Dispense Height (usually higher) to avoid cross-contamination initially,
        # or aspirate height if we want to be submerged immediately.
        # For robust mixing, we usually dispense, then mix.
        cmds_src.append(f"G0 Z{s_disp_z:.2f} F{JOG_SPEED_Z}")
        cmds_src.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")  # Push liquid out (back to air gap)

        # --- 6. Mix Loop (Robust) ---
        self.log_line("[WASH] Performing robust mixing in source...")
        mix_vol = 200.0
        e_mix_up = -1 * (air_gap_ul) * STEPS_PER_UL
        e_mix_down = -1 * (air_gap_ul + mix_vol) * STEPS_PER_UL

        cmds_src.append(f"G0 Z{s_asp_z:.2f} F{JOG_SPEED_Z}")  # Go down to liquid level
        for _ in range(2):
            cmds_src.append(f"G1 E{e_mix_down:.3f} F{PIP_SPEED}")  # Asp
            cmds_src.append(f"G1 E{e_mix_up:.3f} F{PIP_SPEED}")  # Disp

        # --- 7. Aspirate Everything ---
        # Aspirate wash volume + extra to ensure vial is empty
        collect_vol = min(vol + 50.0, 900.0)
        e_collected = -1 * (air_gap_ul + collect_vol) * STEPS_PER_UL
        cmds_src.append(f"G1 E{e_collected:.3f} F{PIP_SPEED}")
        cmds_src.append(f"G0 Z{s_safe_z:.2f} F{JOG_SPEED_Z}")

        self._send_lines_with_ok(cmds_src)
        self.update_last_module(s_mod)
        current_mod_tracker = s_mod

        # --- 8. Move to Dest & 9. Dispense ---
        self.log_line("[WASH] Transferring mixed wash liquid to destination...")

        # We manually handle the move/dispense here instead of calling _perform_single_transfer
        # because we already have liquid in the tip.
        d_mod, d_x, d_y, d_safe_z, _, d_disp_z = self.get_coords_from_combo(dest_str)
        cmds_dest = []
        cmds_dest.extend(self._get_smart_travel_gcode(d_mod, d_x, d_y, d_safe_z, start_module=current_mod_tracker))

        e_blowout = -1 * 100.0 * STEPS_PER_UL
        cmds_dest.append(f"G0 Z{d_disp_z:.2f} F{JOG_SPEED_Z}")
        cmds_dest.append(f"G1 E{e_blowout:.3f} F{PIP_SPEED}")
        cmds_dest.append(f"G0 Z{d_safe_z:.2f} F{JOG_SPEED_Z}")
        cmds_dest.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")  # Reset plunger

        self._send_lines_with_ok(cmds_dest)
        self.update_last_module(d_mod)

        # --- 10. Eject ---
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
            # Check Execute Box
            if not row["vars"]["execute"].get():
                continue

            start_well = row["vars"]["start"].get()
            end_well = row["vars"]["end"].get()
            dest_falcon = row["vars"]["dest"].get()
            vol_str = row["vars"]["vol"].get()

            # Wash vars
            wash_vol_str = row["vars"]["wash_vol"].get()
            wash_times_str = row["vars"]["wash_times"].get()
            wash_src = row["vars"]["wash_src"].get()

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
                "wash_src": wash_src
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
        global_safe_z = self.resolve_coords(0, 0, GLOBAL_SAFE_Z_OFFSET)[2]
        air_gap_vol = 200.0
        e_pos_air_gap = -1 * air_gap_vol * STEPS_PER_UL
        e_pos_blowout = -1 * 100.0 * STEPS_PER_UL

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

                    # Move to Plate Well
                    cmds.extend(
                        self._get_smart_travel_gcode("PLATE", sx, sy, plate_safe_z, start_module=current_sim_module))

                    cmds.append(f"G0 Z{plate_asp_z:.2f} F{JOG_SPEED_Z}")
                    cmds.append(f"G1 E{e_pos_full:.3f} F{PIP_SPEED}")
                    cmds.append(f"G0 Z{plate_safe_z:.2f} F{JOG_SPEED_Z}")
                    self.update_last_module("PLATE")
                    current_sim_module = "PLATE"

                    dx, dy = self.get_falcon_coordinates(dest_falcon)

                    # Move to Falcon
                    cmds.extend(
                        self._get_smart_travel_gcode("FALCON", dx, dy, falcon_safe_z, start_module=current_sim_module))

                    cmds.append(f"G0 Z{falcon_disp_z:.2f} F{JOG_SPEED_Z}")
                    cmds.append(f"G1 E{e_pos_blowout:.3f} F{PIP_SPEED}")
                    cmds.append(f"G0 Z{falcon_safe_z:.2f} F{JOG_SPEED_Z}")
                    self._send_lines_with_ok(cmds)
                    self.update_last_module("FALCON")
                    current_sim_module = "FALCON"

                    self.current_pipette_volume = 100.0
                    self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")
                    self.live_vol_var.set(f"{self.current_pipette_volume:.1f}")

            # --- WASH LOGIC FOR COMBINE FRACTIONS (BATCH MODE) ---
            wash_vol = task["wash_vol"]
            wash_times = task["wash_times"]

            if wash_vol > 0:
                # 1. Eject the tip from the main transfer (if not already done)
                if current_sim_module != "EJECT":
                    self.log_line(f"[COMBINE] Line {line_num}: Ejecting main transfer tip...")
                    self._send_lines_with_ok(self._get_eject_tip_commands())
                    self.update_last_module("EJECT")
                    current_sim_module = "EJECT"

                for cycle in range(wash_times):
                    # --- TIP EXCHANGE LOGIC ---
                    # Always pick a fresh tip for the start of a wash cycle.
                    # If this is not the first cycle, the previous cycle ejected the dirty tip.
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

                    # 3. Handle Wash Cycles
                    wash_src_str = task["wash_src"]
                    falcon_dest_str = f"Falcon {dest_falcon}"

                    # Get Wash Source Coordinates
                    w_mod, w_x, w_y, w_safe_z, w_asp_z, _ = self.get_coords_from_combo(wash_src_str)

                    self.log_line(f"[COMBINE] Line {line_num}: Wash Cycle {cycle + 1}/{wash_times} (Batch Mode)...")

                    # --- A. Distribute Wash Liquid to ALL wells ---
                    for well in wells:
                        self.log_line(f"  -> Distributing {wash_vol}uL Wash to {well}")

                        # Move to Wash Source
                        cmds_dist = []
                        cmds_dist.append(f"G1 E{e_pos_air_gap:.3f} F{PIP_SPEED}")
                        cmds_dist.extend(
                            self._get_smart_travel_gcode(w_mod, w_x, w_y, w_safe_z, start_module=current_sim_module))

                        # Aspirate Wash
                        e_loaded = -1 * (air_gap_vol + wash_vol) * STEPS_PER_UL
                        cmds_dist.append(f"G0 Z{w_asp_z:.2f} F{JOG_SPEED_Z}")
                        cmds_dist.append(f"G1 E{e_loaded:.3f} F{PIP_SPEED}")
                        cmds_dist.append(f"G0 Z{w_safe_z:.2f} F{JOG_SPEED_Z}")

                        self._send_lines_with_ok(cmds_dist)
                        self.update_last_module(w_mod)
                        current_sim_module = w_mod

                        # Move to Well
                        wx, wy = self.get_well_coordinates(well)
                        cmds_well = []
                        cmds_well.extend(self._get_smart_travel_gcode("PLATE", wx, wy, plate_safe_z,
                                                                      start_module=current_sim_module))

                        # Dispense Wash into Well (HIGH Z to avoid contamination)
                        cmds_well.append(f"G0 Z{plate_disp_z:.2f} F{JOG_SPEED_Z}")  # Dispense HIGH
                        cmds_well.append(f"G1 E{e_pos_air_gap:.3f} F{PIP_SPEED}")  # Back to air gap (dispense liquid)
                        cmds_well.append(f"G0 Z{plate_safe_z:.2f} F{JOG_SPEED_Z}")

                        self._send_lines_with_ok(cmds_well)
                        self.update_last_module("PLATE")
                        current_sim_module = "PLATE"

                    # --- B. Collect Wash Liquid from ALL wells ---
                    for well in wells:
                        self.log_line(f"  -> Collecting Wash from {well}")

                        # Move to Well
                        wx, wy = self.get_well_coordinates(well)
                        cmds_col = []
                        cmds_col.extend(self._get_smart_travel_gcode("PLATE", wx, wy, plate_safe_z,
                                                                     start_module=current_sim_module))

                        # Mix and Aspirate
                        # Mix 3 times
                        mix_vol = 200.0
                        e_mix_up = -1 * (air_gap_vol) * STEPS_PER_UL
                        e_mix_down = -1 * (air_gap_vol + mix_vol) * STEPS_PER_UL

                        cmds_col.append(f"G0 Z{plate_asp_z:.2f} F{JOG_SPEED_Z}")
                        for _ in range(3):
                            cmds_col.append(f"G1 E{e_mix_down:.3f} F{PIP_SPEED}")
                            cmds_col.append(f"G1 E{e_mix_up:.3f} F{PIP_SPEED}")

                        # Aspirate (wash_vol + 20uL overage to ensure empty)
                        collect_vol = min(wash_vol + 50.0, 900.0)
                        e_collected = -1 * (air_gap_vol + collect_vol) * STEPS_PER_UL
                        cmds_col.append(f"G1 E{e_collected:.3f} F{PIP_SPEED}")
                        cmds_col.append(f"G0 Z{plate_safe_z:.2f} F{JOG_SPEED_Z}")

                        self._send_lines_with_ok(cmds_col)
                        self.update_last_module("PLATE")
                        current_sim_module = "PLATE"

                        # Move to Dest Falcon
                        dx, dy = self.get_falcon_coordinates(dest_falcon)
                        cmds_dest = []
                        cmds_dest.extend(self._get_smart_travel_gcode("FALCON", dx, dy, falcon_safe_z,
                                                                      start_module=current_sim_module))

                        # Dispense
                        cmds_dest.append(f"G0 Z{falcon_disp_z:.2f} F{JOG_SPEED_Z}")
                        cmds_dest.append(f"G1 E{e_pos_blowout:.3f} F{PIP_SPEED}")
                        cmds_dest.append(f"G0 Z{falcon_safe_z:.2f} F{JOG_SPEED_Z}")
                        cmds_dest.append(f"G1 E{e_pos_air_gap:.3f} F{PIP_SPEED}")  # Reset plunger

                        self._send_lines_with_ok(cmds_dest)
                        self.update_last_module("FALCON")
                        current_sim_module = "FALCON"

                    # 4. Eject Wash Tip (End of Cycle)
                    self.log_line(f"[COMBINE] Line {line_num}: Ejecting wash tip (End of Cycle {cycle + 1})...")
                    self._send_lines_with_ok(self._get_eject_tip_commands())
                    self.update_last_module("EJECT")
                    current_sim_module = "EJECT"

            self.log_line(f"[COMBINE] Line {line_num} Complete.")

        self.log_command("[COMBINE] Sequence Finished. Parking.")
        self.park_head_sequence()

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
        jog_win.geometry("400x350")
        jog_win.grab_set()
        ttk.Label(jog_win, text="Jog head until tip touches pin.", font=("Arial", 10, "bold")).pack(pady=10)
        ttk.Label(jog_win, text="Precision: 0.1 mm", font=("Arial", 9)).pack(pady=5)
        ctrl_frame = ttk.Frame(jog_win)
        ctrl_frame.pack(pady=10)
        original_step = self.step_size_var.get()
        self.step_size_var.set(0.1)

        def close_and_restore():
            self.step_size_var.set(original_step)
            jog_win.destroy()

        jog_win.protocol("WM_DELETE_WINDOW", close_and_restore)
        btn_w = 6
        ttk.Button(ctrl_frame, text="Y+", width=btn_w, command=lambda: self.send_jog("Y", 1)).grid(row=0, column=1,
                                                                                                   pady=5)
        ttk.Button(ctrl_frame, text="Y-", width=btn_w, command=lambda: self.send_jog("Y", -1)).grid(row=2, column=1,
                                                                                                    pady=5)
        ttk.Button(ctrl_frame, text="X-", width=btn_w, command=lambda: self.send_jog("X", -1)).grid(row=1, column=0,
                                                                                                    padx=5)
        ttk.Button(ctrl_frame, text="X+", width=btn_w, command=lambda: self.send_jog("X", 1)).grid(row=1, column=2,
                                                                                                   padx=5)
        ttk.Button(ctrl_frame, text="Z+ (Up)", width=btn_w, command=lambda: self.send_jog("Z", 1)).grid(row=0, column=4,
                                                                                                        padx=20)
        ttk.Button(ctrl_frame, text="Z- (Dn)", width=btn_w, command=lambda: self.send_jog("Z", -1)).grid(row=2,
                                                                                                         column=4,
                                                                                                         padx=20)
        bot_frame = ttk.Frame(jog_win)
        bot_frame.pack(side="bottom", fill="x", pady=10, padx=10)
        ttk.Button(bot_frame, text="Revert to Default",
                   command=lambda: [self.revert_calibration_default(), close_and_restore()]).pack(side="left")
        ttk.Button(bot_frame, text="ACCEPT & SAVE",
                   command=lambda: [self.save_calibration_position(), close_and_restore()]).pack(side="right")

    def save_calibration_position(self):
        new_config = {
            "PIN_X": self.current_x,
            "PIN_Y": self.current_y,
            "PIN_Z": self.current_z
        }
        global CALIBRATION_PIN_CONFIG
        CALIBRATION_PIN_CONFIG.update(new_config)
        self.save_calibration_config(new_config)
        self.log_line(f"[CALIB] New Pin Config Saved: {new_config}")
        messagebox.showinfo("Saved", "New calibration coordinates saved to config.json")

    def revert_calibration_default(self):
        global CALIBRATION_PIN_CONFIG
        CALIBRATION_PIN_CONFIG = CALIBRATION_PIN_CONFIG_DEFAULT.copy()
        self.save_calibration_config(CALIBRATION_PIN_CONFIG)
        self.log_line("[CALIB] Reverted to Default Pin Config.")
        messagebox.showinfo("Reverted", "Calibration reverted to default values.")

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
                    # Already at PLATE, so start_module is PLATE
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

                    # Move to Source
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

                # Move to Plate
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

                # Move to Source
                cmds.extend(
                    self._get_smart_travel_gcode(src_mod, src_x, src_y, src_safe_z, start_module=current_sim_mod))

                e_loaded = -1 * (AIR_GAP_UL + vol_needed) * STEPS_PER_UL
                cmds.append(f"G0 Z{src_asp_z:.2f} F{JOG_SPEED_Z}")
                cmds.append(f"G1 E{e_loaded:.3f} F{PIP_SPEED}")
                cmds.append(f"G0 Z{src_safe_z:.2f} F{JOG_SPEED_Z}")
                self.update_last_module(src_mod)
                current_sim_mod = src_mod

                dest_x, dest_y = self.get_well_coordinates(well)

                # Move to Plate
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

        # For manual moves, we rely on current state (start_module=None)
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