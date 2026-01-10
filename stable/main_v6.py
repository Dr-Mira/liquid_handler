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

# --- RELATIVE OFFSETS ---
GLOBAL_SAFE_Z_OFFSET = 62.2
SAFE_CENTER_X_OFFSET = 0.8
SAFE_CENTER_Y_OFFSET = -4.3

# Pipette Constants
STEPS_PER_UL = 0.3019
DEFAULT_TARGET_UL = 100.0
MOVEMENT_SPEED = 1000

# Limits
MIN_PIPETTE_VOL = 100.0
MAX_PIPETTE_VOL = 1000.0

# Manual Control Constants
JOG_SPEED_XY = 1500
JOG_SPEED_Z = 1500
PIP_SPEED = 1000

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
    "Z_ASPIRATE": -52.8,
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
    "Z_ASPIRATE": -52.8,
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
        self.root.title("Liquid Handler Control (Relative Calibration)")
        self.root.geometry("1024x600")
        self.root.resizable(False, False)  # Mandatory size

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
        self.port_var = tk.StringVar(value="COM5")
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

        # 1. Tips
        self.tip_rows = ["A", "B", "C", "D", "E", "F"]
        self.tip_cols = ["1", "2", "3", "4"]
        self.tip_inventory = {f"{r}{c}": True for r in self.tip_rows for c in self.tip_cols}
        self.tip_buttons = {}

        # 2. Plate
        self.plate_rows = ["A", "B", "C", "D", "E", "F", "G", "H"]
        self.plate_cols = [str(i) for i in range(1, 13)]
        self.plate_wells = [f"{r}{c}" for r in self.plate_rows for c in self.plate_cols]

        # 3. Falcon
        self.falcon_positions = ["A1", "A2", "A3", "B1", "B2", "B3", "50mL"]

        # 4. Wash
        self.wash_positions = ["Wash A", "Wash B", "Waste A", "Waste B"]

        # 5. 4mL Rack (Row A)
        self._4ml_positions = [f"A{i}" for i in range(1, 9)]

        # 6. Filter Eppi (Row B)
        self.filter_eppi_positions = [f"B{i}" for i in range(1, 9)]

        # 7. Eppi (Row C)
        self.eppi_positions = [f"C{i}" for i in range(1, 9)]

        # 8. HPLC Vial (Row D)
        self.hplc_positions = [f"D{i}" for i in range(1, 9)]

        # 9. HPLC Insert (Row E)
        self.hplc_insert_positions = [f"E{i}" for i in range(1, 9)]

        # 10. Screwcap (Row F)
        self.screwcap_positions = [f"F{i}" for i in range(1, 9)]

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

        self._build_ui()
        self._poll_rx_queue()
        self.refresh_ports()
        self._poll_position_loop()
        self.root.after(500, self.attempt_auto_connect)

    def load_calibration_config(self):
        """Loads calibration from JSON, or falls back to default."""
        global CALIBRATION_PIN_CONFIG
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, "r") as f:
                    config = json.load(f)
                    # Validate keys
                    if all(k in config for k in ["PIN_X", "PIN_Y", "PIN_Z"]):
                        CALIBRATION_PIN_CONFIG.update(config)
                        print(f"[CONFIG] Loaded from {self.config_file}")
                    else:
                        print("[CONFIG] Invalid JSON keys. Using defaults.")
            except Exception as e:
                print(f"[CONFIG] Error loading JSON: {e}. Using defaults.")
        else:
            print("[CONFIG] No config file found. Using defaults.")

    def save_calibration_config(self, new_config):
        """Saves the current calibration to JSON."""
        try:
            with open(self.config_file, "w") as f:
                json.dump(new_config, f, indent=4)
            print(f"[CONFIG] Saved to {self.config_file}")
        except Exception as e:
            messagebox.showerror("Save Error", f"Could not save config: {e}")

    def attempt_auto_connect(self):
        target_port = "COM5"
        available_ports = [p.device for p in serial.tools.list_ports.comports()]
        if target_port in available_ports:
            self.port_var.set(target_port)
            self.log_line(f"[SYSTEM] Auto-connecting to {target_port}...")
            self.connect()

    def _build_ui(self):
        # --- Main Tabs (Packed FIRST to take top space) ---
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(side="top", fill="both", expand=True, padx=2, pady=2)

        # 1. Initialization Tab
        self.tab_init = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_init, text=" Initialization ")
        self._build_initialization_tab(self.tab_init)

        # 2. Movement / XYZ Tab
        self.tab_movement = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_movement, text=" Movement / XYZ ")
        self._build_movement_tab(self.tab_movement)

        # 3. Pipette Control Tab
        self.tab_pipette = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_pipette, text=" Pipette Control ")
        self._build_pipette_tab(self.tab_pipette)

        # 4. Maintenance Tab (Modified)
        self.tab_tips = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_tips, text=" Maintenance ")
        self._build_maintenance_tab(self.tab_tips)

        # 5. Testing Tab
        self.tab_testing = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_testing, text=" Testing ")
        self._build_testing_tab(self.tab_testing)

        # --- Bottom Bar (Packed BOTTOM, Fixed Height) ---
        bottom_frame = ttk.Frame(self.root, relief="sunken", borderwidth=1)
        bottom_frame.pack(side="bottom", fill="x", padx=0, pady=0)

        # Font styles
        lbl_font = ("Consolas", 10, "bold")
        val_font = ("Consolas", 10)

        # Left Side: XYZ + V + Module
        left_container = ttk.Frame(bottom_frame)
        left_container.pack(side="left", fill="x", padx=5, pady=2)

        # X
        ttk.Label(left_container, text="X:", font=lbl_font).pack(side="left")
        ttk.Label(left_container, textvariable=self.coord_x_var, font=val_font, width=6).pack(side="left")

        # Y
        ttk.Label(left_container, text=" Y:", font=lbl_font).pack(side="left")
        ttk.Label(left_container, textvariable=self.coord_y_var, font=val_font, width=6).pack(side="left")

        # Z
        ttk.Label(left_container, text=" Z:", font=lbl_font).pack(side="left")
        ttk.Label(left_container, textvariable=self.coord_z_var, font=val_font, width=6).pack(side="left")

        # V (Volume)
        ttk.Label(left_container, text=" V:", font=lbl_font, foreground="blue").pack(side="left", padx=(10, 0))
        ttk.Label(left_container, textvariable=self.live_vol_var, font=val_font, foreground="blue", width=6).pack(
            side="left")

        # Module
        ttk.Label(left_container, text=" Mod:", font=lbl_font).pack(side="left", padx=(10, 0))
        ttk.Label(left_container, textvariable=self.module_hover_var, font=val_font, width=15).pack(side="left")

        # Right Side: Status
        right_container = ttk.Frame(bottom_frame)
        right_container.pack(side="right", padx=5, pady=2)

        self.status_icon_lbl = tk.Label(right_container, text="✘", font=("Arial", 12, "bold"), fg="red")
        self.status_icon_lbl.pack(side="right", padx=2)

        ttk.Label(right_container, textvariable=self.port_var, font=("Arial", 9)).pack(side="right", padx=2)

        # Middle: Last Command (Fills remaining space)
        mid_container = ttk.Frame(bottom_frame)
        mid_container.pack(side="left", fill="x", expand=True, padx=10)

        # UPDATED LABEL TO "STATUS:"
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

        ttk.Button(left_col, text="SYNC & MOVE", command=self.run_calibration_sequence).pack(fill="x", pady=10, ipady=3)

        # --- NEW CALIBRATION BUTTON ---
        ttk.Separator(left_col, orient="horizontal").pack(fill="x", pady=10)
        ttk.Label(left_col, text="Absolute Motion Calibration", font=("Arial", 10, "bold")).pack(pady=2)
        ttk.Button(left_col, text="Calibrate", command=self.start_pin_calibration_sequence).pack(fill="x",
                                                                                                 pady=5)

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

        # Pipette Status & Manual Move
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

        # --- NEW: Split Button Row for Mix and Eject ---
        mix_eject_frame = ttk.Frame(pip_frame)
        mix_eject_frame.pack(fill="x", pady=2)

        ttk.Button(mix_eject_frame, text="MIX WELL", command=self.mix_well_sequence).pack(
            side="left", fill="x", expand=True, padx=(0, 2))

        ttk.Button(mix_eject_frame, text="EJECT TIP", command=self.eject_tip_sequence).pack(
            side="right", fill="x", expand=True, padx=(2, 0))

        # --- NAVIGATION GRID (5 Rows x 2 Columns) ---
        nav_frame = ttk.LabelFrame(scroll_frame, text="Navigation & Workflows", padding=2)
        nav_frame.pack(fill="both", expand=True, padx=5, pady=2)

        # Define the order of modules for the grid
        module_order = [
            "TIPS", "PLATE",
            "FALCON", "WASH",
            "4ML", "FILTER_EPPI",
            "EPPI", "HPLC",
            "HPLC_INSERT", "SCREWCAP"
        ]

        # Create the 5x2 Grid with Compact Layout
        # Layout: [ Label ] [ Combobox ] [ Button ] in one line
        for i, mod_key in enumerate(module_order):
            mod_data = self.modules[mod_key]

            row = i // 2
            col = i % 2

            # Frame for this module cell
            cell_frame = ttk.Frame(nav_frame, borderwidth=1, relief="solid")
            cell_frame.grid(row=row, column=col, padx=3, pady=2, sticky="nsew")

            # Inner container
            inner = ttk.Frame(cell_frame, padding=2)
            inner.pack(fill="x", expand=True)

            # Label (Left)
            ttk.Label(inner, text=mod_data["label"], width=13, font=("Arial", 9, "bold")).pack(side="left", padx=(2, 5))

            # Button (Right - pack first to stick right)
            ttk.Button(inner, text=mod_data["btn_text"], width=5, command=mod_data["cmd"]).pack(side="right", padx=2)

            # Combobox (Middle - fills remaining space)
            cb = ttk.Combobox(inner, textvariable=mod_data["var"], state="readonly", width=8, values=mod_data["values"])
            cb.pack(side="left", fill="x", expand=True, padx=2)

        # Configure grid weights so columns expand equally
        nav_frame.columnconfigure(0, weight=1)
        nav_frame.columnconfigure(1, weight=1)

        # Update tips combo specifically (it's dynamic)
        self.update_available_tips_combo()

    def _build_maintenance_tab(self, parent):
        """
        New Layout:
        Left Side: Park Head, Unpause/Resume buttons.
        Right Side: System Log with scrollbar (Top to Bottom).
        """
        # Split container
        split_frame = ttk.Frame(parent, padding=10)
        split_frame.pack(fill="both", expand=True)

        # --- Left Side (Controls) ---
        left_frame = ttk.LabelFrame(split_frame, text="Controls", padding=10)
        left_frame.pack(side="left", fill="y", padx=(0, 5))

        # UPDATED: Calls self.park_head_sequence instead of send_home("XY")
        ttk.Button(left_frame, text="PARK HEAD", command=self.park_head_sequence).pack(fill="x", pady=10)
        ttk.Button(left_frame, text="UNPAUSE / RESUME", command=self.send_resume).pack(fill="x", pady=10)

        # --- Right Side (System Log) ---
        right_frame = ttk.LabelFrame(split_frame, text="System Log", padding=5)
        right_frame.pack(side="right", fill="both", expand=True, padx=(5, 0))

        self.log = scrolledtext.ScrolledText(right_frame, state="disabled", wrap="word", font=("Consolas", 9))
        self.log.pack(fill="both", expand=True)

    def _build_testing_tab(self, parent):
        """Builds the new Testing tab with compact layout and popup help."""
        frame = ttk.Frame(parent, padding=20)
        frame.pack(fill="both", expand=True)

        # --- Test 1: Tip Rack ---
        rack_frame = ttk.LabelFrame(frame, text="Tip Rack Module Test", padding=5)
        rack_frame.pack(fill="x", pady=5)

        # Container row
        r1_row = ttk.Frame(rack_frame)
        r1_row.pack(fill="x", expand=True)

        desc_rack = "Description: Picks all tips in random order and ejects them immediately."

        # Main Button
        ttk.Button(r1_row, text="TEST: Tip Rack Module (Random Pick/Eject)",
                   command=lambda: threading.Thread(target=self.test_rack_module_sequence, daemon=True).start()
                   ).pack(side="left", fill="x", expand=True, padx=(0, 5))

        # Help Button
        ttk.Button(r1_row, text="?", width=3,
                   command=lambda: messagebox.showinfo("Sequence Info", desc_rack)
                   ).pack(side="right")

        # --- Test 2: 96 Plate Robustness ---
        plate_frame = ttk.LabelFrame(frame, text="96 Plate Robustness Test", padding=5)
        plate_frame.pack(fill="x", pady=5)

        # Container row
        r2_row = ttk.Frame(plate_frame)
        r2_row.pack(fill="x", expand=True)

        desc_plate = ("Description: For each row (A-H):\n"
                      "1. Eject old tip, pick new tip.\n"
                      "2. Set pipette to 200uL (Air Gap).\n"
                      "3. Aspirate 800uL from Wash A (Total 1000uL).\n"
                      "4. Dispense 900uL into Col 1 (Blowout to 100uL).\n"
                      "5. Transfer 800uL from Col 1 -> 2 -> ... -> 12.\n"
                      "   (Logic: Retract to 200uL Air -> Asp 800uL -> Disp 900uL).")

        # Main Button
        ttk.Button(r2_row, text="TEST: 96 Full Plate Seq (Robustness)",
                   command=lambda: threading.Thread(target=self.test_96_plate_robustness_sequence, daemon=True).start()
                   ).pack(side="left", fill="x", expand=True, padx=(0, 5))

        # Help Button
        ttk.Button(r2_row, text="?", width=3,
                   command=lambda: messagebox.showinfo("Sequence Info", desc_plate)
                   ).pack(side="right")

    # ==========================================
    #           LOGIC & COMMS
    # ==========================================

    def log_line(self, text):
        # Ensure log widget exists before writing (might be called during init)
        if hasattr(self, 'log') and self.log:
            self.log.configure(state="normal")
            self.log.insert("end", text + "\n")
            self.log.see("end")
            self.log.configure(state="disabled")
        else:
            print(f"[PRE-INIT LOG]: {text}")

    def log_command(self, text):
        # This just logs to the scrolled text, does not update status bar anymore
        # to avoid overwriting the "Busy" status
        self.log_line(f"[CMD] {text}")

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
            # Update individual coordinate vars for bottom bar
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

                # --- FIX: DYNAMIC TIMEOUT ---
                # Homing (G28) and Leveling (G29) can take much longer than 60s
                # especially if the head is far from home.
                current_timeout = 60.0
                cmd_upper = line.upper()
                if "G28" in cmd_upper or "G29" in cmd_upper:
                    current_timeout = 200.0  # Give it 3+ minutes to be safe

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
        """Sends M400 and waits for OK to ensure physical movement is done."""
        if not self.ser or not self.ser.is_open: return
        self._send_lines_with_ok(["M400"])

    def update_last_module(self, name):
        self.last_known_module = name
        self.module_hover_var.set(name)

    # ==========================================
    #           COORDINATE MATH
    # ==========================================

    def resolve_coords(self, rel_x, rel_y, rel_z=None):
        """
        Ad-hoc calculation of absolute machine coordinates.
        Takes relative offsets and adds them to the CURRENT calibration pin position.
        """
        abs_x = CALIBRATION_PIN_CONFIG["PIN_X"] + rel_x
        abs_y = CALIBRATION_PIN_CONFIG["PIN_Y"] + rel_y
        abs_z = None
        if rel_z is not None:
            abs_z = CALIBRATION_PIN_CONFIG["PIN_Z"] + rel_z
            return abs_x, abs_y, abs_z
        return abs_x, abs_y

    def _get_interpolated_coords(self, col_idx, row_idx, num_cols, num_rows, start_x, start_y, end_x, end_y):
        """Returns Interpolated RELATIVE coordinates."""
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
        # 1 Row (A), 8 Columns
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
        # Also update the combobox widget if it exists
        if hasattr(self, 'tip_combo'):
            self.tip_combo['values'] = available
            self.tip_combo.set(available[0] if available else "EMPTY")

    def _find_next_available_tip(self):
        """Helper to find the first available tip key in order."""
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

        # Calculate absolute Safe Z
        abs_safe_z = self.resolve_coords(0, 0, GLOBAL_SAFE_Z_OFFSET)[2]

        commands = [home_cmd, "M18 E", f"G0 Z{abs_safe_z - 10:.2f}"]
        self.log_line(f"[MANUAL] Homing {axes}...")

        def run_seq():
            self.last_cmd_var.set(f"Homing {axes}...")
            self._send_lines_with_ok(commands)
            self._wait_for_finish()
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def park_head_sequence(self):
        """
        Moves the head to the defined SAFE_CENTER coordinates (XY),
        moves to GLOBAL_SAFE_Z, and then lowers Z by 20mm.
        """
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        self.log_line("[SYSTEM] Parking Head...")
        self.log_command("Park Head (Center + Lower)")

        # Calculate Absolute Coordinates
        # 1. Get Safe Z absolute
        _, _, abs_safe_z = self.resolve_coords(0, 0, GLOBAL_SAFE_Z_OFFSET)

        # 2. Get Center XY absolute
        abs_center_x, abs_center_y = self.resolve_coords(SAFE_CENTER_X_OFFSET, SAFE_CENTER_Y_OFFSET)

        # 3. Calculate Park Z (Safe Z - 20mm)
        # Note: Z grows upwards usually. If Safe Z is high, -20 moves down.
        abs_park_z = abs_safe_z - 20.0

        commands = [
            "G90",
            f"G0 Z{abs_safe_z:.2f} F{JOG_SPEED_Z}",  # Move up to safe height
            f"G0 X{abs_center_x:.2f} Y{abs_center_y:.2f} F{JOG_SPEED_XY}",  # Move to center
            f"G0 Z{abs_park_z:.2f} F{JOG_SPEED_Z}"  # Move down 20mm
        ]

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
        self.raw_gcode_var.set("")

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

    # --- SMART PIPETTE LOGIC ---
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

        # Determine config based on module name
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

        # Resolve Absolute Z Heights
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

    # --- MIX WELL LOGIC ---
    def _get_mix_commands(self, cfg):
        """
        Returns a list of G-Code commands to mix at the current location.
        """
        # Resolve Absolute Z
        abs_z_aspirate = self.resolve_coords(0, 0, cfg["Z_ASPIRATE"])[2]
        abs_z_dispense = self.resolve_coords(0, 0, cfg["Z_DISPENSE"])[2]
        abs_z_safe = self.resolve_coords(0, 0, cfg["Z_SAFE"])[2]

        # 1. Start at 200uL
        vol_start = 200.0
        e_pos_start = -1 * vol_start * STEPS_PER_UL

        # 2. Aspirate 800uL -> Total 1000uL
        vol_after_asp = vol_start + 800.0
        e_pos_asp = -1 * vol_after_asp * STEPS_PER_UL

        # 3. Dispense 900uL -> Total 100uL
        vol_after_disp = vol_after_asp - 900.0
        e_pos_disp = -1 * vol_after_disp * STEPS_PER_UL

        commands = [
            "G90",
            # 1. Ensure pipette is at 200uL range (while hovering)
            f"G1 E{e_pos_start:.3f} F{PIP_SPEED}",
            # 2. Move down to Aspirate Height
            f"G0 Z{abs_z_aspirate:.2f} F{JOG_SPEED_Z}",
            # 3. Aspirate 800uL
            f"G1 E{e_pos_asp:.3f} F{PIP_SPEED}",
            # 4. Move to Dispense Height
            f"G0 Z{abs_z_dispense:.2f} F{JOG_SPEED_Z}",
            # 5. Dispense 900uL
            f"G1 E{e_pos_disp:.3f} F{PIP_SPEED}",
            # 6. Retract to Safe Z
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

    # --- SMART Z HELPER ---
    def _get_smart_travel_gcode(self, target_module, target_x, target_y, module_abs_safe_z):
        """
        Generates travel moves.
        Note: target_x/y and module_abs_safe_z MUST be absolute coordinates.
        """
        # Calculate Global Safe Z absolute
        global_safe_z = self.resolve_coords(0, 0, GLOBAL_SAFE_Z_OFFSET)[2]

        cmds = ["G90"]
        if self.last_known_module == target_module and self.last_known_module is not None:
            cmds.append(f"G0 Z{module_abs_safe_z:.2f} F{JOG_SPEED_Z}")
            cmds.append(f"G0 X{target_x:.2f} Y{target_y:.2f} F{JOG_SPEED_XY}")
        else:
            cmds.append(f"G0 Z{global_safe_z:.2f} F{JOG_SPEED_Z}")
            cmds.append(f"G0 X{target_x:.2f} Y{target_y:.2f} F{JOG_SPEED_XY}")
            cmds.append(f"G0 Z{module_abs_safe_z:.2f} F{JOG_SPEED_Z}")
        return cmds

    # --- HELPER: GET PICK TIP COMMANDS ---
    def _get_pick_tip_commands(self, tip_key):
        """Returns the list of G-code commands to pick a specific tip."""
        tx, ty = self.get_tip_coordinates(tip_key)

        # Resolve Z heights
        abs_rack_safe_z = self.resolve_coords(0, 0, TIP_RACK_CONFIG["Z_TRAVEL"])[2]
        abs_pick_z = self.resolve_coords(0, 0, TIP_RACK_CONFIG["Z_PICK"])[2]

        commands = self._get_smart_travel_gcode("TIPS", tx, ty, abs_rack_safe_z)
        commands.extend([f"G0 Z{abs_pick_z:.2f} F500", f"G0 Z{abs_rack_safe_z:.2f} F{JOG_SPEED_Z}"])
        return commands

    # --- HELPER: GET EJECT TIP COMMANDS ---
    def _get_eject_tip_commands(self):
        """Returns the list of G-code commands to eject a tip."""
        # Use the config dictionary (Relative)
        cfg = EJECT_STATION_CONFIG

        # Resolve all relative coords to absolute
        abs_app_x, abs_app_y, abs_safe_z = self.resolve_coords(cfg["APPROACH_X"], cfg["APPROACH_Y"], cfg["Z_SAFE"])
        abs_eject_start_z = self.resolve_coords(0, 0, cfg["Z_EJECT_START"])[2]
        abs_target_y = self.resolve_coords(0, cfg["EJECT_TARGET_Y"])[1]
        abs_retract_z = self.resolve_coords(0, 0, cfg["Z_RETRACT"])[2]

        # Resolve Global Safe Spot
        abs_center_x, abs_center_y, abs_center_z = self.resolve_coords(SAFE_CENTER_X_OFFSET, SAFE_CENTER_Y_OFFSET,
                                                                       GLOBAL_SAFE_Z_OFFSET)

        commands = ["G90"]

        # 1. Travel to Global Safe Spot first (Requested modification)
        # Move Z up to safe height first
        commands.append(f"G0 Z{abs_center_z:.2f} F{JOG_SPEED_Z}")
        # Move XY to safe center
        commands.append(f"G0 X{abs_center_x:.2f} Y{abs_center_y:.2f} F{JOG_SPEED_XY}")

        # 2. Approach Eject Station
        # Move XY to Eject Approach
        commands.append(f"G0 X{abs_app_x:.2f} Y{abs_app_y:.2f} F{JOG_SPEED_XY}")
        # Move Z down to Eject Safe Z
        commands.append(f"G0 Z{abs_safe_z:.2f} F{JOG_SPEED_Z}")

        # 3. Ejection Sequence
        # Move down to engage
        commands.append(f"G0 Z{abs_eject_start_z:.2f} F{JOG_SPEED_Z}")
        # Move Y to strip tip
        commands.append(f"G0 Y{abs_target_y:.2f} F250")
        # Move Z up
        commands.append(f"G0 Z{abs_retract_z:.2f} F250")
        # Return to Global Safe Z
        commands.append(f"G0 Z{abs_center_z:.2f} F{JOG_SPEED_Z}")

        return commands

    def eject_tip_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        self.log_line("[SYSTEM] Ejecting Tip...")
        self.log_command("Ejecting Tip")

        # Use helper
        commands = self._get_eject_tip_commands()

        def run_seq():
            self.last_cmd_var.set("Ejecting Tip...")
            self._send_lines_with_ok(commands)
            self._wait_for_finish()
            self.update_last_module("EJECT")
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

        # Use helper
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
    #           PIN CALIBRATION LOGIC
    # ==========================================

    def start_pin_calibration_sequence(self):
        """
        1. G28 Home
        2. Find & Pick next tip
        3. Move to current Calibration Pin Config
        4. Popup Window
        """
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        # Find a tip first
        tip_key = self._find_next_available_tip()
        if not tip_key:
            messagebox.showerror("No Tips", "No fresh tips available for calibration.")
            return

        self.log_line("[CALIB] Starting Pin Calibration Sequence...")

        # Build Command Sequence
        cmds = []

        # Calculate Global Safe Z
        global_safe_z = self.resolve_coords(0, 0, GLOBAL_SAFE_Z_OFFSET)[2]

        # 1. Home
        cmds.append("G28")
        cmds.append(f"G0 Z{global_safe_z:.2f} F{JOG_SPEED_Z}")

        # 2. Pick Tip (Manual construction to ensure correct flow after homing)
        # We can use the helper, but we need to know where we are. After G28 we are at home.
        # Helper assumes we might be somewhere else.
        # Let's force last_known_module to None so helper does safe travel
        self.update_last_module("Unknown")

        pick_cmds = self._get_pick_tip_commands(tip_key)
        cmds.extend(pick_cmds)

        # 3. Move to Calibration Pin
        # Use current config
        pin_x = CALIBRATION_PIN_CONFIG["PIN_X"]
        pin_y = CALIBRATION_PIN_CONFIG["PIN_Y"]
        pin_z = CALIBRATION_PIN_CONFIG["PIN_Z"]

        # Move logic: Safe Z -> XY -> Pin Z
        cmds.append(f"G0 Z{global_safe_z:.2f} F{JOG_SPEED_Z}")
        cmds.append(f"G0 X{pin_x:.2f} Y{pin_y:.2f} F{JOG_SPEED_XY}")
        cmds.append(f"G0 Z{pin_z:.2f} F{JOG_SPEED_Z}")

        def run_seq():
            self.last_cmd_var.set("Calibrating: Moving to pin...")
            self._send_lines_with_ok(cmds)
            self._wait_for_finish()
            # Update Tip State
            self.tip_inventory[tip_key] = False
            self.update_last_module("CALIBRATION_PIN")

            # UI Updates on main thread
            self.root.after(0, self.update_tip_grid_colors)
            self.root.after(0, self.update_available_tips_combo)

            # Show Popup
            self.last_cmd_var.set("Waiting for User...")
            self.root.after(0, self._show_calibration_decision_popup)
            # We set Idle after user interaction in the popup, but since this thread ends here:
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def _show_calibration_decision_popup(self):
        popup = tk.Toplevel(self.root)
        popup.title("Calibration Check")
        popup.geometry("300x150")
        popup.grab_set()  # Modal

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

        # Jog Controls
        ctrl_frame = ttk.Frame(jog_win)
        ctrl_frame.pack(pady=10)

        # Override step size for this window temporarily
        original_step = self.step_size_var.get()
        self.step_size_var.set(0.1)

        def close_and_restore():
            self.step_size_var.set(original_step)
            jog_win.destroy()

        jog_win.protocol("WM_DELETE_WINDOW", close_and_restore)

        # Grid Layout for XYZ
        #      Y+   Z+
        # X-  XY   X+
        #      Y-   Z-

        btn_w = 6

        # Y Controls
        ttk.Button(ctrl_frame, text="Y+", width=btn_w, command=lambda: self.send_jog("Y", 1)).grid(row=0, column=1,
                                                                                                   pady=5)
        ttk.Button(ctrl_frame, text="Y-", width=btn_w, command=lambda: self.send_jog("Y", -1)).grid(row=2, column=1,
                                                                                                    pady=5)

        # X Controls
        ttk.Button(ctrl_frame, text="X-", width=btn_w, command=lambda: self.send_jog("X", -1)).grid(row=1, column=0,
                                                                                                    padx=5)
        ttk.Button(ctrl_frame, text="X+", width=btn_w, command=lambda: self.send_jog("X", 1)).grid(row=1, column=2,
                                                                                                   padx=5)

        # Z Controls
        ttk.Button(ctrl_frame, text="Z+ (Up)", width=btn_w, command=lambda: self.send_jog("Z", 1)).grid(row=0, column=4,
                                                                                                        padx=20)
        ttk.Button(ctrl_frame, text="Z- (Dn)", width=btn_w, command=lambda: self.send_jog("Z", -1)).grid(row=2,
                                                                                                         column=4,
                                                                                                         padx=20)

        # Bottom Buttons
        bot_frame = ttk.Frame(jog_win)
        bot_frame.pack(side="bottom", fill="x", pady=10, padx=10)

        ttk.Button(bot_frame, text="Revert to Default",
                   command=lambda: [self.revert_calibration_default(), close_and_restore()]).pack(side="left")
        ttk.Button(bot_frame, text="ACCEPT & SAVE",
                   command=lambda: [self.save_calibration_position(), close_and_restore()]).pack(side="right")

    def save_calibration_position(self):
        # We need the current coordinates.
        # The background poller updates self.current_x/y/z

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
        """
        Picks all 24 tips in random order, ejects them, then parks the head.
        Uses the existing pick and eject logic helpers.
        """
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        # 1. Generate and Shuffle Tips
        all_tips = [f"{r}{c}" for r in self.tip_rows for c in self.tip_cols]
        random.shuffle(all_tips)

        self.log_command(f"[TEST] Starting Rack Test on {len(all_tips)} tips (Randomized)...")

        full_sequence = ["G90"]  # Absolute positioning

        # We need to simulate the module state so _get_smart_travel_gcode works correctly inside the loop
        simulated_last_module = self.last_known_module

        for tip_key in all_tips:
            # --- PICK LOGIC ---
            # Temporarily set state so the helper generates the correct approach path
            self.last_known_module = simulated_last_module
            pick_cmds = self._get_pick_tip_commands(tip_key)
            full_sequence.extend(pick_cmds)

            # Update simulated state to where we are after picking
            simulated_last_module = "TIPS"
            self.last_known_module = "TIPS"

            # --- EJECT LOGIC ---
            eject_cmds = self._get_eject_tip_commands()
            full_sequence.extend(eject_cmds)

            # Update simulated state to where we are after ejecting
            simulated_last_module = "EJECT"
            self.last_known_module = "EJECT"

        # --- PARK LOGIC ---
        abs_park_x, abs_park_y, abs_park_z = self.resolve_coords(SAFE_CENTER_X_OFFSET, SAFE_CENTER_Y_OFFSET,
                                                                 GLOBAL_SAFE_Z_OFFSET)
        full_sequence.append(f"G0 X{abs_park_x:.2f} Y{abs_park_y:.2f} Z{abs_park_z:.2f} F{JOG_SPEED_XY}")

        # Execute
        def run_seq():
            self.last_cmd_var.set("Running Rack Test Sequence...")
            self._send_lines_with_ok(full_sequence)
            self._wait_for_finish()
            # Update UI (Mark all as empty since we used them)
            self.reset_all_tips_empty()
            self.update_last_module("PARK")
            self.log_command("[SYSTEM] Rack Test Complete. Parked.")
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def test_96_plate_robustness_sequence(self):
        """
        Robustness Test:
        Iterates Rows A-H.
        For each row:
        1. Ejects existing tip (if any).
        2. Picks NEW tip.
        3. Fills pipette with 200uL Air (Moves to 200uL absolute position).
        4. Aspirates 800uL from Wash A (Moves to 1000uL absolute position).
        5. Dispenses 900uL into Col 1 (Moves to 100uL absolute position - Blowout).
        6. Transfers 800uL from Col 1 -> Col 2 -> ... -> Col 12.
           (Logic: Move to 200uL -> Asp 800 -> Disp 900).
        """
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        self.log_command("[SYSTEM] Starting 96 Plate Robustness Sequence...")

        # Calculate Z heights once
        wash_safe_z = self.resolve_coords(0, 0, WASH_RACK_CONFIG["Z_SAFE"])[2]
        wash_asp_z = self.resolve_coords(0, 0, WASH_RACK_CONFIG["Z_ASPIRATE"])[2]
        plate_safe_z = self.resolve_coords(0, 0, PLATE_CONFIG["Z_SAFE"])[2]
        plate_asp_z = self.resolve_coords(0, 0, PLATE_CONFIG["Z_ASPIRATE"])[2]
        plate_disp_z = self.resolve_coords(0, 0, PLATE_CONFIG["Z_DISPENSE"])[2]

        # Calculate E-Positions based on Absolute Volume
        # 0 is empty/home. 1000 is full.
        # E is negative. Larger volume = more negative E.
        vol_gap = 200.0
        vol_asp = 800.0
        vol_disp = 900.0

        # E positions corresponding to specific volumes inside the pipette
        e_gap_pos = -1 * vol_gap * STEPS_PER_UL  # 200uL mark
        e_full_pos = -1 * (vol_gap + vol_asp) * STEPS_PER_UL  # 1000uL mark
        e_blowout_pos = -1 * (vol_gap + vol_asp - vol_disp) * STEPS_PER_UL  # 100uL mark

        def run_seq():
            self.last_cmd_var.set("Starting Plate Robustness Test...")
            # Loop Rows A to H
            for row_idx, row_char in enumerate(self.plate_rows):
                self.log_line(f"[SYSTEM] Starting Row {row_char}...")
                self.last_cmd_var.set(f"Test: Row {row_char}...")

                # 1. Eject Existing Tip
                # We blindly run eject to ensure we are clean.
                self.log_line(f"[SYSTEM] Row {row_char}: Ejecting old tip...")
                self._send_lines_with_ok(self._get_eject_tip_commands())
                self.update_last_module("EJECT")

                # 2. Pick New Tip
                tip_key = self._find_next_available_tip()
                if not tip_key:
                    messagebox.showerror("No Tips", f"Ran out of tips at Row {row_char}.")
                    return

                self.log_line(f"[SYSTEM] Row {row_char}: Picking Tip {tip_key}...")
                self._send_lines_with_ok(self._get_pick_tip_commands(tip_key))
                self.tip_inventory[tip_key] = False
                self.update_last_module("TIPS")

                # Update UI immediately
                self.root.after(0, self.update_tip_grid_colors)

                # 3. Initial Charge (Wash A -> Col 1)
                self.log_line(f"[TEST] Row {row_char}: Initial Charge from Wash A -> {row_char}1")

                cmds_init = []

                # Move to 200uL (Air Gap)
                # We trust the global tracking or initialization. We just move to the absolute position.
                cmds_init.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")

                # Move to Wash A
                wx, wy = self.get_wash_coordinates("Wash A")
                cmds_init.extend(self._get_smart_travel_gcode("WASH", wx, wy, wash_safe_z))

                # Aspirate 800uL (Move to 1000uL mark)
                cmds_init.append(f"G0 Z{wash_asp_z:.2f} F{JOG_SPEED_Z}")
                cmds_init.append(f"G1 E{e_full_pos:.3f} F{PIP_SPEED}")
                cmds_init.append(f"G0 Z{wash_safe_z:.2f} F{JOG_SPEED_Z}")

                # Move to Plate Col 1
                p1_x, p1_y = self.get_well_coordinates(f"{row_char}1")
                cmds_init.extend(self._get_smart_travel_gcode("PLATE", p1_x, p1_y, plate_safe_z))

                # Dispense 900uL (Move to 100uL mark - Blowout)
                cmds_init.append(f"G0 Z{plate_disp_z:.2f} F{JOG_SPEED_Z}")
                cmds_init.append(f"G1 E{e_blowout_pos:.3f} F{PIP_SPEED}")
                cmds_init.append(f"G0 Z{plate_safe_z:.2f} F{JOG_SPEED_Z}")

                self._send_lines_with_ok(cmds_init)
                self.update_last_module("PLATE")
                # Update logical volume to 100uL
                self.current_pipette_volume = 100.0
                self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")
                self.live_vol_var.set(f"{self.current_pipette_volume:.1f}")

                # 4. Serial Transfer Loop (Col 1 -> 2, 2 -> 3 ... 11 -> 12)
                for col in range(1, 12):
                    self.last_cmd_var.set(f"Test: Row {row_char} Col {col}->{col + 1}")
                    src_well = f"{row_char}{col}"
                    dst_well = f"{row_char}{col + 1}"

                    cmds_xfer = []

                    # Retract to 200uL (Air Gap)
                    # Currently at 100uL. Move to 200uL.
                    cmds_xfer.append(f"G1 E{e_gap_pos:.3f} F{PIP_SPEED}")

                    # Move to Source
                    sx, sy = self.get_well_coordinates(src_well)
                    cmds_xfer.extend(self._get_smart_travel_gcode("PLATE", sx, sy, plate_safe_z))

                    # Aspirate 800uL (Move to 1000uL mark)
                    cmds_xfer.append(f"G0 Z{plate_asp_z:.2f} F{JOG_SPEED_Z}")
                    cmds_xfer.append(f"G1 E{e_full_pos:.3f} F{PIP_SPEED}")
                    cmds_xfer.append(f"G0 Z{plate_safe_z:.2f} F{JOG_SPEED_Z}")

                    # Move to Dest
                    dx, dy = self.get_well_coordinates(dst_well)
                    cmds_xfer.extend(self._get_smart_travel_gcode("PLATE", dx, dy, plate_safe_z))

                    # Dispense 900uL (Move to 100uL mark)
                    cmds_xfer.append(f"G0 Z{plate_disp_z:.2f} F{JOG_SPEED_Z}")
                    cmds_xfer.append(f"G1 E{e_blowout_pos:.3f} F{PIP_SPEED}")
                    cmds_xfer.append(f"G0 Z{plate_safe_z:.2f} F{JOG_SPEED_Z}")

                    self._send_lines_with_ok(cmds_xfer)
                    self.current_pipette_volume = 100.0
                    self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")
                    self.live_vol_var.set(f"{self.current_pipette_volume:.1f}")

                # End of Row Loop

            # End of All Rows
            self.log_line("[SYSTEM] All Rows Complete. Ejecting final tip...")
            self.last_cmd_var.set("Test: Final Eject...")
            self._send_lines_with_ok(self._get_eject_tip_commands())
            self.update_last_module("EJECT")

            # Park
            abs_park_x, abs_park_y, abs_park_z = self.resolve_coords(SAFE_CENTER_X_OFFSET, SAFE_CENTER_Y_OFFSET,
                                                                     GLOBAL_SAFE_Z_OFFSET)
            self._send_lines_with_ok([f"G0 X{abs_park_x:.2f} Y{abs_park_y:.2f} Z{abs_park_z:.2f} F{JOG_SPEED_XY}"])
            self.update_last_module("PARK")

            self.log_command("[SYSTEM] Robustness Sequence Finished.")
            self._wait_for_finish()
            self.last_cmd_var.set("Idle")

        threading.Thread(target=run_seq, daemon=True).start()

    def generic_move_sequence(self, module_name, target_pos):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        if not target_pos: return

        x, y = 0.0, 0.0
        rel_safe_z = 0.0

        # Coordinate Resolution
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

        # Resolve Safe Z
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