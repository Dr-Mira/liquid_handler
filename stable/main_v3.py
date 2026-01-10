import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import time
import queue
import serial
import serial.tools.list_ports
import re
import random
from datetime import datetime

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

# --- TIP RACK CONFIGURATION ---
TIP_RACK_CONFIG = {
    "A1_X": 178.65, "A1_Y": 136.5,
    "F4_X": 208.85, "F4_Y": 87.6,
    "Z_TRAVEL": 185.0,
    "Z_PICK": 95.0,
}

# --- 96 WELL PLATE CONFIGURATION ---
PLATE_CONFIG = {
    "A1_X": 4.0, "A1_Y": 218.0,
    "H12_X": 103.0, "H12_Y": 154.0,
    "Z_SAFE": 110.0,
    "Z_ASPIRATE": 77.30, "Z_DISPENSE": 97.30
}

# --- FALCON RACK CONFIGURATION ---
FALCON_RACK_CONFIG = {
    "15ML_A1_X": 13.65, "15ML_A1_Y": 120.5,
    "15ML_B3_X": 50.65, "15ML_B3_Y": 100.5,
    "50ML_X": 78.65, "50ML_Y": 110.5,
    "Z_SAFE": 195.0,
    "Z_ASPIRATE": 80.0, "Z_DISPENSE": 180.0
}

# --- WASH STATION CONFIGURATION ---
WASH_RACK_CONFIG = {
    "A1_X": 160.05, "A1_Y": 53.5,  # Wash A
    "B2_X": 194.05, "B2_Y": 16.5,  # Waste B
    "Z_SAFE": 195.0,
    "Z_ASPIRATE": 105, "Z_DISPENSE": 170.0
}

# --- 4 ML RACK CONFIGURATION ---
_4ML_RACK_CONFIG = {
    "A1_X": -4.80, "A1_Y": 69.7,
    "A8_X": 120.09, "A8_Y": 69.7,
    "Z_SAFE": 121.6,
    "Z_ASPIRATE": 80.0, "Z_DISPENSE": 100.0
}

# --- FILTER EPPI RACK CONFIGURATION (Row B) ---
FILTER_EPPI_RACK_CONFIG = {
    "B1_X": -4.8, "B1_Y": 53.7,
    "B8_X": 120.09, "B8_Y": 53.7,
    "Z_SAFE": 121.6,
    "Z_ASPIRATE": 93.6, "Z_DISPENSE": 110.0
}

# --- EPPI RACK CONFIGURATION (Row C) ---
EPPI_RACK_CONFIG = {
    "C1_X": -4.8, "C1_Y": 39.2,
    "C8_X": 120.09, "C8_Y": 39.2,
    "Z_SAFE": 121.6,
    "Z_ASPIRATE": 72.0, "Z_DISPENSE": 105.0
}

# --- HPLC VIAL RACK CONFIGURATION (Row D) ---
HPLC_VIAL_RACK_CONFIG = {
    "D1_X": -4.8, "D1_Y": 23.7,
    "D8_X": 120.09, "D8_Y": 23.7,
    "Z_SAFE": 121.6,
    "Z_ASPIRATE": 88.6, "Z_DISPENSE": 110.0
}

# --- HPLC VIAL INSERT RACK CONFIGURATION (Row E) ---
HPLC_VIAL_INSERT_RACK_CONFIG = {
    "E1_X": -4.8, "E1_Y": 7.7,
    "E8_X": 120.09, "E8_Y": 7.7,
    "Z_SAFE": 121.6,
    "Z_ASPIRATE": 105.5, "Z_DISPENSE": 110.0
}

# --- SCREWCAP VIAL RACK CONFIGURATION (Row F) ---
SCREWCAP_VIAL_RACK_CONFIG = {
    "F1_X": -4.8, "F1_Y": -8.3,
    "F8_X": 120.09, "F8_Y": -8.3,
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


# ==========================================
#           MAIN APPLICATION
# ==========================================

class LiquidHandlerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Liquid Handler Control (Smart Z Tracking)")
        self.root.geometry("1024x600")
        self.root.resizable(False, False)

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
        self.last_known_module = None

        # UI Variables
        self.port_var = tk.StringVar(value="COM5")
        self.baud_var = tk.StringVar(value="115200")
        self.status_var = tk.StringVar(value="Disconnected")
        self.coord_var = tk.StringVar(value="X: 0.00  Y: 0.00  Z: 0.00")

        # Calibration Variables
        self.current_vol_var = tk.StringVar()
        self.target_vol_var = tk.StringVar(value=str(int(DEFAULT_TARGET_UL)))

        # Manual Control Variables
        self.step_size_var = tk.DoubleVar(value=10.0)
        self.pipette_move_var = tk.StringVar(value="100")
        self.raw_gcode_var = tk.StringVar()

        self.current_pipette_volume = DEFAULT_TARGET_UL
        self.vol_display_var = tk.StringVar(value=f"{self.current_pipette_volume:.1f} uL")

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

    def attempt_auto_connect(self):
        target_port = "COM5"
        available_ports = [p.device for p in serial.tools.list_ports.comports()]
        if target_port in available_ports:
            self.port_var.set(target_port)
            self.log_line(f"[SYSTEM] Auto-connecting to {target_port}...")
            self.connect()

    def _build_ui(self):
        # --- Bottom Console (Packed FIRST to ensure visibility) ---
        bottom_frame = ttk.Frame(self.root)
        bottom_frame.pack(side="bottom", fill="x", padx=2, pady=2)

        self.log_notebook = ttk.Notebook(bottom_frame)
        self.log_notebook.pack(fill="x", expand=True)

        self.tab_sys_log = ttk.Frame(self.log_notebook)
        self.log_notebook.add(self.tab_sys_log, text=" System Log ")
        self.log = scrolledtext.ScrolledText(self.tab_sys_log, height=3, state="disabled", wrap="word",
                                             font=("Consolas", 8))
        self.log.pack(fill="both", expand=True)

        self.tab_cmd_log = ttk.Frame(self.log_notebook)
        self.log_notebook.add(self.tab_cmd_log, text=" Command History ")
        self.cmd_log = scrolledtext.ScrolledText(self.tab_cmd_log, height=3, state="disabled", wrap="word",
                                                 font=("Arial", 10))
        self.cmd_log.pack(fill="both", expand=True)

        footer_frame = ttk.Frame(bottom_frame)
        footer_frame.pack(fill="x", pady=(1, 0))

        self.coord_label = ttk.Label(footer_frame, textvariable=self.coord_var, font=("Consolas", 10, "bold"),
                                     foreground="#333")
        self.coord_label.pack(side="left", padx=5)

        self.status_icon_lbl = tk.Label(footer_frame, text="✘", font=("Arial", 12, "bold"), fg="red")
        self.status_icon_lbl.pack(side="right", padx=5)

        self.status_text_lbl = ttk.Label(footer_frame, textvariable=self.status_var, font=("Arial", 9))
        self.status_text_lbl.pack(side="right", padx=5)

        # --- Main Tabs (Packed SECOND to take remaining space) ---
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

        # 4. Maintenance Tab
        self.tab_tips = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_tips, text=" Maint. ")
        self._build_tip_tab(self.tab_tips)

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

    def _build_tip_tab(self, parent):
        container = ttk.Frame(parent)
        container.place(relx=0.5, rely=0.45, anchor="center")

        ttk.Label(container, text="Maintenance Mode", font=("Arial", 16, "bold")).pack(pady=(0, 10))

        # General Controls
        ttk.Button(container, text="Park Head (Safe)", command=lambda: self.send_home("XY")).pack(fill="x", pady=5)
        ttk.Button(container, text="UNPAUSE / RESUME (M108)", command=self.send_resume).pack(fill="x", pady=5, ipady=5)

        # --- TEST SECTION ---
        test_frame = ttk.LabelFrame(container, text="Module Tests", padding=10)
        test_frame.pack(fill="x", pady=15)

        ttk.Label(test_frame, text="Rack Tip Module Test", font=("Arial", 10, "bold")).pack(pady=(0, 5))
        ttk.Label(test_frame, text="Picks all 24 tips (Random Order),\nEjects them, then Parks.",
                  font=("Arial", 8), justify="center").pack(pady=(0, 5))

        # Runs the test sequence in a thread
        ttk.Button(test_frame, text="TEST: Rack Tip Module",
                   command=lambda: threading.Thread(target=self.test_rack_module_sequence, daemon=True).start()
                   ).pack(fill="x", pady=2)

        # --- NEW TEST 96 PLATE ---
        ttk.Label(test_frame, text="96 Plate Full Sequence", font=("Arial", 10, "bold")).pack(pady=(10, 5))
        ttk.Button(test_frame, text="TEST: 96 Plate Full Seq",
                   command=lambda: threading.Thread(target=self.test_96_plate_sequence, daemon=True).start()
                   ).pack(fill="x", pady=2)

    # ==========================================
    #           LOGIC & COMMS
    # ==========================================

    def log_line(self, text):
        self.log.configure(state="normal")
        self.log.insert("end", text + "\n")
        self.log.see("end")
        self.log.configure(state="disabled")

    def log_command(self, text):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.cmd_log.configure(state="normal")
        self.cmd_log.insert("end", f"[{timestamp}] {text}\n")
        self.cmd_log.see("end")
        self.cmd_log.configure(state="disabled")
        self.log_notebook.select(self.tab_cmd_log)

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
            self.coord_var.set("X: 0.00  Y: 0.00  Z: 0.00")

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
        self.log_line("[INIT] Waiting for printer boot...")
        time.sleep(2.0)
        self.log_line("[INIT] Sending Setup G-Code...")
        self._send_lines_with_ok(CALIBRATION_SETUP_GCODE)
        self._send_raw("M115\n")
        self.log_line("[INIT] Setup Complete.")
        self.last_known_module = None

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
            self.coord_var.set(f"X: {x}  Y: {y}  Z: {z}")

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
                ok = self.ok_event.wait(timeout=60.0)
                if not ok:
                    self.rx_queue.put(f"[HOST] Error: Timeout waiting for 'ok' on: {line}")
                    self.rx_queue.put("[HOST] Stopping sequence to prevent crash.")
                    return
        finally:
            self.is_sequence_running = False
            self.last_action_time = time.time()
            self.rx_queue.put("[HOST] Sequence Complete")

    # ==========================================
    #           COORDINATE MATH
    # ==========================================

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

    def get_tip_coordinates(self, tip_key):
        row_char = tip_key[0]
        col_num = int(tip_key[1])
        row_idx = self.tip_rows.index(row_char)
        col_idx = col_num - 1
        return self._get_interpolated_coords(col_idx, row_idx, 4, 6, TIP_RACK_CONFIG["A1_X"], TIP_RACK_CONFIG["A1_Y"],
                                             TIP_RACK_CONFIG["F4_X"], TIP_RACK_CONFIG["F4_Y"])

    def get_well_coordinates(self, well_key):
        row_char = well_key[0]
        col_num = int(well_key[1:])
        row_idx = self.plate_rows.index(row_char)
        col_idx = col_num - 1
        return self._get_interpolated_coords(col_idx, row_idx, 12, 8, PLATE_CONFIG["A1_X"], PLATE_CONFIG["A1_Y"],
                                             PLATE_CONFIG["H12_X"], PLATE_CONFIG["H12_Y"])

    def get_falcon_coordinates(self, falcon_key):
        if falcon_key == "50mL":
            return FALCON_RACK_CONFIG["50ML_X"], FALCON_RACK_CONFIG["50ML_Y"]
        row_char = falcon_key[0]
        col_num = int(falcon_key[1:])
        falcon_rows = ["A", "B"]
        if row_char not in falcon_rows: return 0.0, 0.0
        row_idx = falcon_rows.index(row_char)
        col_idx = col_num - 1
        return self._get_interpolated_coords(col_idx, row_idx, 3, 2, FALCON_RACK_CONFIG["15ML_A1_X"],
                                             FALCON_RACK_CONFIG["15ML_A1_Y"], FALCON_RACK_CONFIG["15ML_B3_X"],
                                             FALCON_RACK_CONFIG["15ML_B3_Y"])

    def get_wash_coordinates(self, wash_name):
        mapping = {"Wash A": (0, 0), "Wash B": (1, 0), "Waste A": (0, 1), "Waste B": (1, 1)}
        col_idx, row_idx = mapping.get(wash_name, (0, 0))
        return self._get_interpolated_coords(col_idx, row_idx, 2, 2, WASH_RACK_CONFIG["A1_X"], WASH_RACK_CONFIG["A1_Y"],
                                             WASH_RACK_CONFIG["B2_X"], WASH_RACK_CONFIG["B2_Y"])

    def get_4ml_coordinates(self, key):
        # 1 Row (A), 8 Columns
        if not key.startswith("A"): return 0.0, 0.0
        col_num = int(key[1:])
        col_idx = col_num - 1
        return self._get_interpolated_coords(col_idx, 0, 8, 1, _4ML_RACK_CONFIG["A1_X"], _4ML_RACK_CONFIG["A1_Y"],
                                             _4ML_RACK_CONFIG["A8_X"], _4ML_RACK_CONFIG["A8_Y"])

    def get_1x8_rack_coordinates(self, key, config, row_char):
        """
        Calculates coordinates for a 1x8 rack where the row is fixed (e.g., 'B', 'C', etc.)
        and columns range from 1 to 8.
        """
        if not key.startswith(row_char): return 0.0, 0.0
        try:
            col_num = int(key[1:])
        except ValueError:
            return 0.0, 0.0

        col_idx = col_num - 1
        # Get start and end coordinates from config using the row char
        # Example: if row_char is 'B', looks for 'B1_X' and 'B8_X'
        start_x = config[f"{row_char}1_X"]
        start_y = config[f"{row_char}1_Y"]
        end_x = config[f"{row_char}8_X"]
        end_y = config[f"{row_char}8_Y"]

        return self._get_interpolated_coords(col_idx, 0, 8, 1, start_x, start_y, end_x, end_y)

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
        self.last_known_module = None
        distance = self.step_size_var.get()
        move_val = distance * direction_sign
        feed_rate = JOG_SPEED_Z if axis == 'Z' else JOG_SPEED_XY
        commands = ["G91", f"G0 {axis}{move_val} F{feed_rate}", "G90"]
        self.log_line(f"[MANUAL] Jog {axis} {move_val}mm")
        threading.Thread(target=self._send_lines_with_ok, args=(commands,), daemon=True).start()

    def send_home(self, axes):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        self.last_known_module = None
        axes = axes.upper()
        if axes == "ALL":
            home_cmd = "G28"
        else:
            home_cmd = f"G28 {' '.join(list(axes))}"
        commands = [home_cmd, "M18 E", f"G0 Z{GLOBAL_SAFE_Z}"]
        self.log_line(f"[MANUAL] Homing {axes}...")
        threading.Thread(target=self._send_lines_with_ok, args=(commands,), daemon=True).start()

    def send_raw_gcode_command(self):
        cmd = self.raw_gcode_var.get().strip()
        if not cmd: return
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        self.last_known_module = None
        threading.Thread(target=self._send_lines_with_ok, args=([cmd],), daemon=True).start()
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
        threading.Thread(target=self._send_lines_with_ok, args=(commands,), daemon=True).start()
        self.current_pipette_volume = new_vol
        self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")

    # --- SMART PIPETTE LOGIC ---
    def smart_pipette_sequence(self, mode):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        if not self.last_known_module:
            messagebox.showerror("Unknown Position", "Move to a module first.")
            return
        if self.last_known_module in ["TIPS", "EJECT"]:
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

        z_action = cfg["Z_ASPIRATE"] if mode == "aspirate" else cfg["Z_DISPENSE"]
        z_safe = cfg["Z_SAFE"]

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
        self.log_line(f"[SMART] {mode.upper()} at {self.last_known_module} (Z={z_action})")
        self.log_command(f"Smart {mode}: {delta_ul}uL @ {self.last_known_module}")

        commands = [
            "G90",
            f"G0 Z{z_action} F{JOG_SPEED_Z}",
            f"G1 E{target_e_pos:.3f} F{PIP_SPEED}",
            f"G0 Z{z_safe} F{JOG_SPEED_Z}",
            "M18 E"
        ]

        def run_seq():
            self._send_lines_with_ok(commands)
            self.current_pipette_volume = new_vol
            self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")

        threading.Thread(target=run_seq, daemon=True).start()

    # --- MIX WELL LOGIC ---
    def _get_mix_commands(self, cfg):
        """
        Returns a list of G-Code commands to mix at the current location.
        Logic: 200uL -> Aspirate 800 (Total 1000) -> Dispense 900 (Total 100).
        """
        z_aspirate = cfg["Z_ASPIRATE"]
        z_dispense = cfg["Z_DISPENSE"]
        z_safe = cfg["Z_SAFE"]

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
            f"G0 Z{z_aspirate} F{JOG_SPEED_Z}",
            # 3. Aspirate 800uL
            f"G1 E{e_pos_asp:.3f} F{PIP_SPEED}",
            # 4. Move to Dispense Height
            f"G0 Z{z_dispense} F{JOG_SPEED_Z}",
            # 5. Dispense 900uL
            f"G1 E{e_pos_disp:.3f} F{PIP_SPEED}",
            # 6. Retract to Safe Z
            f"G0 Z{z_safe} F{JOG_SPEED_Z}",
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
        if self.last_known_module in ["TIPS", "EJECT"]:
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
            self._send_lines_with_ok(commands)
            self.current_pipette_volume = final_vol
            self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")

        threading.Thread(target=run_seq, daemon=True).start()

    # --- SMART Z HELPER ---
    def _get_smart_travel_gcode(self, target_module, target_x, target_y, module_safe_z):
        cmds = ["G90"]
        if self.last_known_module == target_module and self.last_known_module is not None:
            cmds.append(f"G0 Z{module_safe_z} F{JOG_SPEED_Z}")
            cmds.append(f"G0 X{target_x} Y{target_y} F{JOG_SPEED_XY}")
        else:
            cmds.append(f"G0 Z{GLOBAL_SAFE_Z} F{JOG_SPEED_Z}")
            cmds.append(f"G0 X{target_x} Y{target_y} F{JOG_SPEED_XY}")
            cmds.append(f"G0 Z{module_safe_z} F{JOG_SPEED_Z}")
        return cmds

    # --- HELPER: GET PICK TIP COMMANDS ---
    def _get_pick_tip_commands(self, tip_key):
        """Returns the list of G-code commands to pick a specific tip."""
        tx, ty = self.get_tip_coordinates(tip_key)
        rack_safe_z = TIP_RACK_CONFIG["Z_TRAVEL"]
        pick_z = TIP_RACK_CONFIG["Z_PICK"]
        commands = self._get_smart_travel_gcode("TIPS", tx, ty, rack_safe_z)
        commands.extend([f"G0 Z{pick_z} F500", f"G0 Z{rack_safe_z} F{JOG_SPEED_Z}"])
        return commands

    # --- HELPER: GET EJECT TIP COMMANDS ---
    def _get_eject_tip_commands(self):
        """Returns the list of G-code commands to eject a tip."""
        commands = self._get_smart_travel_gcode("EJECT", 190.0, 176.0, 190.0)
        commands.extend([f"G0 Z160.2 F{JOG_SPEED_Z}", f"G0 Y218 F250", f"G0 Z190 F250",
                         f"G0 Z{GLOBAL_SAFE_Z} F{JOG_SPEED_Z}"])
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
            self._send_lines_with_ok(commands)
            self.last_known_module = "EJECT"

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
            self._send_lines_with_ok(commands)
            self.last_known_module = "TIPS"
            self.tip_inventory[target_tip] = False
            self.root.after(0, self.update_tip_grid_colors)
            self.root.after(0, self.update_available_tips_combo)

        threading.Thread(target=run_seq, daemon=True).start()

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
        # Park at 110 110 195
        full_sequence.append(f"G0 X110 Y110 Z195 F{JOG_SPEED_XY}")

        # Execute
        self._send_lines_with_ok(full_sequence)

        # Update UI (Mark all as empty since we used them)
        self.reset_all_tips_empty()
        self.last_known_module = "PARK"
        self.log_command("[TEST] Rack Test Complete. Parked.")

    def test_96_plate_sequence(self):
        """
        1. Pick new tip.
        2. Set Pipette to 200uL (Air).
        3. Falcon A1: Aspirate 500uL.
        4. Plate A1: Dispense 600uL.
        5. Eject Tip.
        6. Pick new tip.
        7. Set Pipette to 200uL (Air).
        8. Wash A: Aspirate 500uL.
        9. Plate A1: Dispense 600uL.
        10. Mix Well.
        11. Eject Tip.
        12. Park.
        """
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        self.log_command("[TEST] Starting 96 Plate Sequence...")

        # --- HELPER FOR TRANSFER LOGIC ---
        def run_transfer(source_module, source_pos, dest_module, dest_pos, asp_vol, disp_vol):
            # 1. Ensure Pipette is at 200uL (Air Gap)
            # We use G92 to force the coordinate system to match our logical "200uL" starting point
            start_vol = 200.0
            start_e = -1 * start_vol * STEPS_PER_UL
            self._send_lines_with_ok([f"G92 E{start_e:.3f}"])
            self.current_pipette_volume = start_vol

            # 2. Move to Source & Aspirate
            # Get Coords
            if source_module == "FALCON":
                sx, sy = self.get_falcon_coordinates(source_pos)
                scfg = FALCON_RACK_CONFIG
            elif source_module == "WASH":
                sx, sy = self.get_wash_coordinates(source_pos)
                scfg = WASH_RACK_CONFIG
            else:
                return  # Should not happen in this specific test

            target_vol_asp = start_vol + asp_vol  # 200 + 500 = 700
            target_e_asp = -1 * target_vol_asp * STEPS_PER_UL

            cmds_asp = self._get_smart_travel_gcode(source_module, sx, sy, scfg["Z_SAFE"])
            cmds_asp.extend([
                f"G0 Z{scfg['Z_ASPIRATE']} F{JOG_SPEED_Z}",
                f"G1 E{target_e_asp:.3f} F{PIP_SPEED}",
                f"G0 Z{scfg['Z_SAFE']} F{JOG_SPEED_Z}"
            ])
            self._send_lines_with_ok(cmds_asp)
            self.last_known_module = source_module
            self.current_pipette_volume = target_vol_asp

            # 3. Move to Dest & Dispense
            # Get Coords (Plate)
            dx, dy = self.get_well_coordinates(dest_pos)
            dcfg = PLATE_CONFIG

            target_vol_disp = self.current_pipette_volume - disp_vol  # 700 - 600 = 100
            target_e_disp = -1 * target_vol_disp * STEPS_PER_UL

            cmds_disp = self._get_smart_travel_gcode(dest_module, dx, dy, dcfg["Z_SAFE"])
            cmds_disp.extend([
                f"G0 Z{dcfg['Z_DISPENSE']} F{JOG_SPEED_Z}",
                f"G1 E{target_e_disp:.3f} F{PIP_SPEED}",
                f"G0 Z{dcfg['Z_SAFE']} F{JOG_SPEED_Z}"
            ])
            self._send_lines_with_ok(cmds_disp)
            self.last_known_module = dest_module
            self.current_pipette_volume = target_vol_disp

        # --- STEP 1: FIRST TIP & FALCON TRANSFER ---
        tip1 = self._find_next_available_tip()
        if not tip1:
            messagebox.showerror("Error", "No tips available for Step 1.")
            return

        # Pick Tip 1
        self.log_line(f"[TEST] Step 1: Picking Tip {tip1}")
        self._send_lines_with_ok(self._get_pick_tip_commands(tip1))
        self.last_known_module = "TIPS"
        self.tip_inventory[tip1] = False
        self.root.after(0, self.update_tip_grid_colors)

        # Falcon Transfer
        self.log_line("[TEST] Step 2: Transfer Falcon -> Plate")
        run_transfer("FALCON", "A1", "PLATE", "A1", 500.0, 600.0)

        # Eject Tip 1
        self.log_line("[TEST] Step 3: Ejecting Tip 1")
        self._send_lines_with_ok(self._get_eject_tip_commands())
        self.last_known_module = "EJECT"

        # --- STEP 2: SECOND TIP & WASH TRANSFER ---
        tip2 = self._find_next_available_tip()
        if not tip2:
            messagebox.showerror("Error", "No tips available for Step 2.")
            return

        # Pick Tip 2
        self.log_line(f"[TEST] Step 4: Picking Tip {tip2}")
        self._send_lines_with_ok(self._get_pick_tip_commands(tip2))
        self.last_known_module = "TIPS"
        self.tip_inventory[tip2] = False
        self.root.after(0, self.update_tip_grid_colors)

        # Wash Transfer
        self.log_line("[TEST] Step 5: Transfer Wash -> Plate")
        run_transfer("WASH", "Wash A", "PLATE", "A1", 500.0, 600.0)

        # --- STEP 3: MIX WELL ---
        self.log_line("[TEST] Step 6: Mixing at Plate A1")
        # We are already at PLATE A1 from the previous step, but we ensure correct commands
        mix_cmds, final_vol = self._get_mix_commands(PLATE_CONFIG)
        self._send_lines_with_ok(mix_cmds)
        self.current_pipette_volume = final_vol

        # --- STEP 4: EJECT & PARK ---
        self.log_line("[TEST] Step 7: Eject & Park")
        self._send_lines_with_ok(self._get_eject_tip_commands())
        self.last_known_module = "EJECT"

        # Park
        self._send_lines_with_ok([f"G0 X{SAFE_CENTER_X} Y{SAFE_CENTER_Y} Z{GLOBAL_SAFE_Z} F{JOG_SPEED_XY}"])
        self.last_known_module = "PARK"

        self.log_command("[TEST] 96 Plate Sequence Complete.")

    def generic_move_sequence(self, module_name, target_pos):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return
        if not target_pos: return

        x, y = 0.0, 0.0
        safe_z = GLOBAL_SAFE_Z

        # Coordinate Resolution
        if module_name == "PLATE":
            x, y = self.get_well_coordinates(target_pos)
            safe_z = PLATE_CONFIG["Z_SAFE"]
        elif module_name == "FALCON":
            x, y = self.get_falcon_coordinates(target_pos)
            safe_z = FALCON_RACK_CONFIG["Z_SAFE"]
        elif module_name == "WASH":
            x, y = self.get_wash_coordinates(target_pos)
            safe_z = WASH_RACK_CONFIG["Z_SAFE"]
        elif module_name == "4ML":
            x, y = self.get_4ml_coordinates(target_pos)
            safe_z = _4ML_RACK_CONFIG["Z_SAFE"]
        elif module_name == "FILTER_EPPI":
            x, y = self.get_1x8_rack_coordinates(target_pos, FILTER_EPPI_RACK_CONFIG, "B")
            safe_z = FILTER_EPPI_RACK_CONFIG["Z_SAFE"]
        elif module_name == "EPPI":
            x, y = self.get_1x8_rack_coordinates(target_pos, EPPI_RACK_CONFIG, "C")
            safe_z = EPPI_RACK_CONFIG["Z_SAFE"]
        elif module_name == "HPLC":
            x, y = self.get_1x8_rack_coordinates(target_pos, HPLC_VIAL_RACK_CONFIG, "D")
            safe_z = HPLC_VIAL_RACK_CONFIG["Z_SAFE"]
        elif module_name == "HPLC_INSERT":
            x, y = self.get_1x8_rack_coordinates(target_pos, HPLC_VIAL_INSERT_RACK_CONFIG, "E")
            safe_z = HPLC_VIAL_INSERT_RACK_CONFIG["Z_SAFE"]
        elif module_name == "SCREWCAP":
            x, y = self.get_1x8_rack_coordinates(target_pos, SCREWCAP_VIAL_RACK_CONFIG, "F")
            safe_z = SCREWCAP_VIAL_RACK_CONFIG["Z_SAFE"]

        self.log_line(f"[SYSTEM] Moving to {module_name} : {target_pos}...")
        self.log_command(f"Move: {module_name} {target_pos}")
        commands = self._get_smart_travel_gcode(module_name, x, y, safe_z)

        def run_seq():
            self._send_lines_with_ok(commands)
            self.last_known_module = module_name

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
        self.log_line(f"[CALC] Calibration: {current_ul}uL -> {target_ul}uL")
        self.log_command(f"Calibrate Pipette: {current_ul} -> {target_ul}uL")
        commands = [f"G92 E{current_e_pos:.3f}", f"G1 E{target_e_pos:.3f} F{MOVEMENT_SPEED}"]
        commands.extend(CALIBRATION_SETUP_GCODE)
        self.current_pipette_volume = target_ul
        self.vol_display_var.set(f"{self.current_pipette_volume:.1f} uL")
        threading.Thread(target=self._send_lines_with_ok, args=(commands,), daemon=True).start()


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