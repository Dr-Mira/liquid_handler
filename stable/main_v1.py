import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import time
import queue
import serial
import serial.tools.list_ports
import re
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
    "A1_X": 178.65,  # Top-Left Hole
    "A1_Y": 136.5,  # Top-Left Hole
    "F4_X": 208.85,  # Bottom-Right Hole
    "F4_Y": 87.6,  # Bottom-Right Hole
    "Z_TRAVEL": 185.0,
    "Z_PICK": 95.0,  # Mechanical pick height
}

# --- 96 WELL PLATE CONFIGURATION ---
PLATE_CONFIG = {
    "A1_X": 4.0,  # Top-Left Well
    "A1_Y": 218.0,  # Top-Left Well
    "H12_X": 103.0,  # Bottom-Right Well
    "H12_Y": 154.0,  # Bottom-Right Well
    "Z_SAFE": 110.0,

    # Updated Z Heights
    "Z_ASPIRATE": 77.30,
    "Z_DISPENSE": 97.30
}

# --- FALCON RACK CONFIGURATION ---
FALCON_RACK_CONFIG = {
    # 15mL Grid Configuration (A1 to B3)
    "15ML_A1_X": 13.65,
    "15ML_A1_Y": 120.5,
    "15ML_B3_X": 50.65,
    "15ML_B3_Y": 100.5,

    # 50mL Single Tube Configuration
    "50ML_X": 78.65,
    "50ML_Y": 110.5,

    "Z_SAFE": 195.0,

    # Hardcoded Z Heights for Falcons
    "Z_ASPIRATE": 80.0,
    "Z_DISPENSE": 110.0
}

# --- WASH STATION CONFIGURATION ---
WASH_RACK_CONFIG = {
    "A1_X": 160.05,  # Center of 'Wash A'
    "A1_Y": 53.5,  # Center of 'Wash A'
    "B2_X": 194.05,  # Center of 'Waste B'
    "B2_Y": 16.5,  # Center of 'Waste B'
    "Z_SAFE": 195.0,

    # Hardcoded Z Heights for Wash/Waste
    "Z_ASPIRATE": 80.0,
    "Z_DISPENSE": 110.0
}

# --- 4 ML RACK CONFIGURATION ---
_4ML_RACK_CONFIG = {
    "A1_X": -4.80,
    "A1_Y": 69.7,
    "A8_X": 120.09,
    "A8_Y": 69.7,
    "Z_SAFE": 121.6,
}

# --- FILTER EPPI RACK CONFIGURATION ---
FILTER_EPPI_RACK_CONFIG = {
    "A1_X": 160.05,  # Center of 'Wash A'
    "A1_Y": 53.5,  # Center of 'Wash A'
    "B2_X": 194.05,  # Center of 'Waste B'
    "B2_Y": 16.5,  # Center of 'Waste B'
    "Z_SAFE": 195.0,

    # Hardcoded Z Heights for Wash/Waste
    "Z_ASPIRATE": 80.0,
    "Z_DISPENSE": 110.0
}

# --- EPPI RACK CONFIGURATION ---
EPPI_RACK_CONFIG = {
    "A1_X": 160.05,  # Center of 'Wash A'
    "A1_Y": 53.5,  # Center of 'Wash A'
    "B2_X": 194.05,  # Center of 'Waste B'
    "B2_Y": 16.5,  # Center of 'Waste B'
    "Z_SAFE": 195.0,

    # Hardcoded Z Heights for Wash/Waste
    "Z_ASPIRATE": 80.0,
    "Z_DISPENSE": 110.0
}

# --- HPLC VIAL RACK CONFIGURATION ---
HPLC_VIAL_RACK_CONFIG = {
    "A1_X": 160.05,  # Center of 'Wash A'
    "A1_Y": 53.5,  # Center of 'Wash A'
    "B2_X": 194.05,  # Center of 'Waste B'
    "B2_Y": 16.5,  # Center of 'Waste B'
    "Z_SAFE": 195.0,

    # Hardcoded Z Heights for Wash/Waste
    "Z_ASPIRATE": 80.0,
    "Z_DISPENSE": 110.0
}

# --- HPLC VIAL INSERT RACK CONFIGURATION ---
HPLC_VIAL_INSERT_RACK_CONFIG = {
    "A1_X": 160.05,  # Center of 'Wash A'
    "A1_Y": 53.5,  # Center of 'Wash A'
    "B2_X": 194.05,  # Center of 'Waste B'
    "B2_Y": 16.5,  # Center of 'Waste B'
    "Z_SAFE": 195.0,

    # Hardcoded Z Heights for Wash/Waste
    "Z_ASPIRATE": 80.0,
    "Z_DISPENSE": 110.0
}

# --- SCREWCAP VIAL RACK CONFIGURATION ---
SCREWCAP_VIAL_RACK_CONFIG = {
    "A1_X": 160.05,  # Center of 'Wash A'
    "A1_Y": 53.5,  # Center of 'Wash A'
    "B2_X": 194.05,  # Center of 'Waste B'
    "B2_Y": 16.5,  # Center of 'Waste B'
    "Z_SAFE": 195.0,

    # Hardcoded Z Heights for Wash/Waste
    "Z_ASPIRATE": 80.0,
    "Z_DISPENSE": 110.0
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
        # Keeps track of the last module the machine interacted with.
        # Values: "TIPS", "PLATE", "FALCON", "WASH", "EJECT", or None (Unknown)
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

        # Tip Selection Variable
        self.selected_tip_var = tk.StringVar()

        # Plate Selection Variable
        self.selected_well_var = tk.StringVar()

        # Falcon Selection Variable
        self.selected_falcon_var = tk.StringVar()

        # Wash Selection Variable
        self.selected_wash_var = tk.StringVar()

        # STATE TRACKING
        self.current_pipette_volume = 1000.0
        self.vol_display_var = tk.StringVar(value=f"{self.current_pipette_volume:.1f} uL")

        # --- TIP INVENTORY INITIALIZATION ---
        self.tip_rows = ["A", "B", "C", "D", "E", "F"]
        self.tip_cols = ["1", "2", "3", "4"]
        self.tip_inventory = {}
        self.tip_buttons = {}

        for r in self.tip_rows:
            for c in self.tip_cols:
                self.tip_inventory[f"{r}{c}"] = True

        # --- PLATE WELL INITIALIZATION ---
        self.plate_rows = ["A", "B", "C", "D", "E", "F", "G", "H"]
        self.plate_cols = [str(i) for i in range(1, 13)]
        self.plate_wells = []
        for r in self.plate_rows:
            for c in self.plate_cols:
                self.plate_wells.append(f"{r}{c}")

        # --- FALCON RACK INITIALIZATION ---
        self.falcon_positions = ["A1", "A2", "A3", "B1", "B2", "B3", "50mL"]

        # --- WASH RACK INITIALIZATION ---
        self.wash_positions = ["Wash A", "Wash B", "Waste A", "Waste B"]

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
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True, padx=2, pady=2)

        # 1. Initialization Tab
        self.tab_init = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_init, text=" Initialization ")
        self._build_initialization_tab(self.tab_init)

        # 2. Manual Control Tab
        self.tab_manual = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_manual, text=" Manual Control ")
        self._build_manual_tab(self.tab_manual)

        # 3. Maintenance Tab
        self.tab_tips = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_tips, text=" Maint. ")
        self._build_tip_tab(self.tab_tips)

        # --- Bottom Console (Split Tabs) ---
        bottom_frame = ttk.Frame(self.root)
        bottom_frame.pack(side="bottom", fill="x", padx=2, pady=2)

        # Log Notebook
        self.log_notebook = ttk.Notebook(bottom_frame)
        self.log_notebook.pack(fill="x", expand=True)

        # Tab 1: System Log
        self.tab_sys_log = ttk.Frame(self.log_notebook)
        self.log_notebook.add(self.tab_sys_log, text=" System Log ")
        # REDUCED HEIGHT TO 3
        self.log = scrolledtext.ScrolledText(self.tab_sys_log, height=3, state="disabled", wrap="word",
                                             font=("Consolas", 8))
        self.log.pack(fill="both", expand=True)

        # Tab 2: Command History
        self.tab_cmd_log = ttk.Frame(self.log_notebook)
        self.log_notebook.add(self.tab_cmd_log, text=" Command History ")
        # REDUCED HEIGHT TO 3
        self.cmd_log = scrolledtext.ScrolledText(self.tab_cmd_log, height=3, state="disabled", wrap="word",
                                                 font=("Arial", 10))
        self.cmd_log.pack(fill="both", expand=True)

        # Footer Status
        footer_frame = ttk.Frame(bottom_frame)
        footer_frame.pack(fill="x", pady=(1, 0))

        self.coord_label = ttk.Label(footer_frame, textvariable=self.coord_var,
                                     font=("Consolas", 10, "bold"), foreground="#333")
        self.coord_label.pack(side="left", padx=5)

        self.status_icon_lbl = tk.Label(footer_frame, text="✘", font=("Arial", 12, "bold"), fg="red")
        self.status_icon_lbl.pack(side="right", padx=5)

        self.status_text_lbl = ttk.Label(footer_frame, textvariable=self.status_var, font=("Arial", 9))
        self.status_text_lbl.pack(side="right", padx=5)

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

    def _build_manual_tab(self, parent):
        main_layout = ttk.Frame(parent, padding=2)
        main_layout.pack(fill="both", expand=True)

        left_col = ttk.Frame(main_layout)
        left_col.pack(side="left", fill="both", expand=True, padx=(0, 2))
        right_col = ttk.Frame(main_layout)
        right_col.pack(side="right", fill="both", expand=True, padx=(2, 0))

        # --- Left Col: Movement ---
        step_frame = ttk.LabelFrame(left_col, text="Step Size (mm)", padding=2)
        step_frame.pack(fill="x", pady=(0, 2))
        for val in [0.1, 0.5, 1.0, 5.0, 10.0, 50.0]:
            ttk.Radiobutton(step_frame, text=f"{float(val)}", variable=self.step_size_var, value=val).pack(
                side="left", expand=True)

        controls_frame = ttk.Frame(left_col)
        controls_frame.pack(expand=True, fill="both")

        xy_frame = ttk.LabelFrame(controls_frame, text="XY Axis", padding=2)
        xy_frame.pack(side="left", fill="both", expand=True, padx=(0, 2))

        ttk.Button(xy_frame, text="Y+", width=5, command=lambda: self.send_jog("Y", 1)).grid(row=0, column=1, pady=1)
        ttk.Button(xy_frame, text="X-", width=5, command=lambda: self.send_jog("X", -1)).grid(row=1, column=0, padx=1)
        ttk.Button(xy_frame, text="Home", width=5, command=lambda: self.send_home("XY")).grid(row=1, column=1, padx=1)
        ttk.Button(xy_frame, text="X+", width=5, command=lambda: self.send_jog("X", 1)).grid(row=1, column=2, padx=1)
        ttk.Button(xy_frame, text="Y-", width=5, command=lambda: self.send_jog("Y", -1)).grid(row=2, column=1, pady=1)

        z_frame = ttk.LabelFrame(controls_frame, text="Z Axis", padding=2)
        z_frame.pack(side="right", fill="both", expand=True, padx=(2, 0))
        ttk.Button(z_frame, text="Z+ (Up)", command=lambda: self.send_jog("Z", 1)).pack(pady=1, fill='x')
        ttk.Button(z_frame, text="Home Z", command=lambda: self.send_home("Z")).pack(pady=1, fill='x')
        ttk.Button(z_frame, text="Z- (Down)", command=lambda: self.send_jog("Z", -1)).pack(pady=1, fill='x')

        ttk.Button(left_col, text="HOME ALL (G28)", command=lambda: self.send_home("All")).pack(fill="x", pady=2)

        # --- Right Col: Pipette & Plate ---
        pip_frame = ttk.LabelFrame(right_col, text="Pipette Control", padding=5)
        pip_frame.pack(fill="both", expand=True)

        top_pip_frame = ttk.Frame(pip_frame)
        top_pip_frame.pack(fill="x", pady=(0, 2))
        ttk.Label(top_pip_frame, text="Current:", font=("Arial", 9)).pack(side="left")
        self.vol_display_lbl = ttk.Label(top_pip_frame, textvariable=self.vol_display_var, font=("Arial", 11, "bold"),
                                         foreground="blue")
        self.vol_display_lbl.pack(side="right")

        pip_entry_frame = ttk.Frame(pip_frame)
        pip_entry_frame.pack(fill="x", pady=(0, 5))
        ttk.Label(pip_entry_frame, text="Move (uL):", font=("Arial", 9)).pack(side="left")
        pip_entry = ttk.Entry(pip_entry_frame, textvariable=self.pipette_move_var, font=("Arial", 10), width=6,
                              justify="center")
        pip_entry.pack(side="right")

        # --- MANUAL BUTTONS ---
        act_btn_frame = ttk.Frame(pip_frame)
        act_btn_frame.pack(fill="x", pady=1)
        ttk.Button(act_btn_frame, text="ASPIRATE", command=lambda: self.manual_pipette_move("aspirate")).pack(
            side="left", fill="x", expand=True, padx=(0, 1))
        ttk.Button(act_btn_frame, text="DISPENSE", command=lambda: self.manual_pipette_move("dispense")).pack(
            side="right", fill="x", expand=True, padx=(1, 0))

        # --- SMART BUTTONS (AT POSITION) ---
        smart_btn_frame = ttk.Frame(pip_frame)
        smart_btn_frame.pack(fill="x", pady=1)
        # These buttons use the current module state to determine Z height
        ttk.Button(smart_btn_frame, text="ASP @ POS", command=lambda: self.smart_pipette_sequence("aspirate")).pack(
            side="left", fill="x", expand=True, padx=(0, 1))
        ttk.Button(smart_btn_frame, text="DISP @ POS", command=lambda: self.smart_pipette_sequence("dispense")).pack(
            side="right", fill="x", expand=True, padx=(1, 0))

        ttk.Separator(pip_frame, orient='horizontal').pack(fill='x', pady=5)
        ttk.Button(pip_frame, text="EJECT TIP", command=self.eject_tip_sequence).pack(fill="x", pady=1, ipady=2)

        ttk.Separator(pip_frame, orient='horizontal').pack(fill='x', pady=5)

        # 1. Take New Tip
        pick_frame = ttk.Frame(pip_frame)
        pick_frame.pack(fill="x", pady=1)
        ttk.Label(pick_frame, text="New Tip:", font=("Arial", 9, "bold")).pack(side="left")
        self.tip_combo = ttk.Combobox(pick_frame, textvariable=self.selected_tip_var, state="readonly", width=5)
        self.tip_combo.pack(side="left", padx=5)
        ttk.Button(pick_frame, text="PICK", command=self.pick_tip_sequence, width=6).pack(side="right", fill="x",
                                                                                          expand=True)
        self.update_available_tips_combo()

        ttk.Separator(pip_frame, orient='horizontal').pack(fill='x', pady=5)

        # 2. Go Above Well
        well_frame = ttk.Frame(pip_frame)
        well_frame.pack(fill="x", pady=1)
        ttk.Label(well_frame, text="Go Well:", font=("Arial", 9, "bold")).pack(side="left")

        self.well_combo = ttk.Combobox(well_frame, textvariable=self.selected_well_var, state="readonly", width=5,
                                       values=self.plate_wells)
        if self.plate_wells: self.well_combo.set(self.plate_wells[0])
        self.well_combo.pack(side="left", padx=5)

        ttk.Button(well_frame, text="GO", command=self.move_to_well_sequence, width=6).pack(side="right", fill="x",
                                                                                            expand=True)

        # 3. Go Falcon
        ttk.Separator(pip_frame, orient='horizontal').pack(fill='x', pady=5)

        falcon_frame = ttk.Frame(pip_frame)
        falcon_frame.pack(fill="x", pady=1)
        ttk.Label(falcon_frame, text="Go Falcon:", font=("Arial", 9, "bold")).pack(side="left")

        self.falcon_combo = ttk.Combobox(falcon_frame, textvariable=self.selected_falcon_var, state="readonly", width=5,
                                         values=self.falcon_positions)
        if self.falcon_positions: self.falcon_combo.set(self.falcon_positions[0])
        self.falcon_combo.pack(side="left", padx=5)

        ttk.Button(falcon_frame, text="GO", command=self.move_to_falcon_sequence, width=6).pack(side="right", fill="x",
                                                                                                expand=True)

        # 4. Go Wash
        ttk.Separator(pip_frame, orient='horizontal').pack(fill='x', pady=5)

        wash_frame = ttk.Frame(pip_frame)
        wash_frame.pack(fill="x", pady=1)
        ttk.Label(wash_frame, text="Go Wash:", font=("Arial", 9, "bold")).pack(side="left")

        self.wash_combo = ttk.Combobox(wash_frame, textvariable=self.selected_wash_var, state="readonly", width=5,
                                       values=self.wash_positions)
        if self.wash_positions: self.wash_combo.set(self.wash_positions[0])
        self.wash_combo.pack(side="left", padx=5)

        ttk.Button(wash_frame, text="GO", command=self.move_to_wash_sequence, width=6).pack(side="right", fill="x",
                                                                                            expand=True)

        # G-Code Input
        gcode_frame = ttk.LabelFrame(parent, text="Raw G-Code", padding=2)
        gcode_frame.pack(side="bottom", fill="x", padx=2, pady=(0, 2))
        self.gcode_entry = ttk.Entry(gcode_frame, textvariable=self.raw_gcode_var, font=("Consolas", 9))
        self.gcode_entry.pack(side="left", fill="x", expand=True, padx=(0, 2))
        self.gcode_entry.bind('<Return>', lambda event: self.send_raw_gcode_command())
        ttk.Button(gcode_frame, text="SEND", width=6, command=self.send_raw_gcode_command).pack(side="right")

    def _build_tip_tab(self, parent):
        container = ttk.Frame(parent)
        container.place(relx=0.5, rely=0.45, anchor="center")
        ttk.Label(container, text="Maintenance Mode", font=("Arial", 16, "bold")).pack(pady=(0, 10))
        ttk.Button(container, text="Park Head (Safe)", command=lambda: self.send_home("XY")).pack(fill="x", pady=5)
        ttk.Button(container, text="UNPAUSE / RESUME (M108)", command=self.send_resume).pack(fill="x", pady=5, ipady=5)

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

        return self._get_interpolated_coords(
            col_idx, row_idx, 4, 6,
            TIP_RACK_CONFIG["A1_X"], TIP_RACK_CONFIG["A1_Y"],
            TIP_RACK_CONFIG["F4_X"], TIP_RACK_CONFIG["F4_Y"]
        )

    def get_well_coordinates(self, well_key):
        row_char = well_key[0]
        col_num = int(well_key[1:])
        row_idx = self.plate_rows.index(row_char)
        col_idx = col_num - 1

        return self._get_interpolated_coords(
            col_idx, row_idx, 12, 8,
            PLATE_CONFIG["A1_X"], PLATE_CONFIG["A1_Y"],
            PLATE_CONFIG["H12_X"], PLATE_CONFIG["H12_Y"]
        )

    def get_falcon_coordinates(self, falcon_key):
        if falcon_key == "50mL":
            return FALCON_RACK_CONFIG["50ML_X"], FALCON_RACK_CONFIG["50ML_Y"]

        row_char = falcon_key[0]
        col_num = int(falcon_key[1:])
        falcon_rows = ["A", "B"]

        if row_char not in falcon_rows:
            return 0.0, 0.0

        row_idx = falcon_rows.index(row_char)
        col_idx = col_num - 1

        return self._get_interpolated_coords(
            col_idx, row_idx, 3, 2,
            FALCON_RACK_CONFIG["15ML_A1_X"], FALCON_RACK_CONFIG["15ML_A1_Y"],
            FALCON_RACK_CONFIG["15ML_B3_X"], FALCON_RACK_CONFIG["15ML_B3_Y"]
        )

    def get_wash_coordinates(self, wash_name):
        mapping = {
            "Wash A": (0, 0),
            "Wash B": (1, 0),
            "Waste A": (0, 1),
            "Waste B": (1, 1)
        }
        col_idx, row_idx = mapping.get(wash_name, (0, 0))

        return self._get_interpolated_coords(
            col_idx, row_idx, 2, 2,
            WASH_RACK_CONFIG["A1_X"], WASH_RACK_CONFIG["A1_Y"],
            WASH_RACK_CONFIG["B2_X"], WASH_RACK_CONFIG["B2_Y"]
        )

    # ==========================================
    #           TIP INVENTORY LOGIC
    # ==========================================

    def toggle_tip_state(self, key):
        self.tip_inventory[key] = not self.tip_inventory[key]
        self.update_tip_grid_colors()
        self.update_available_tips_combo()

    def reset_all_tips_fresh(self):
        for k in self.tip_inventory:
            self.tip_inventory[k] = True
        self.update_tip_grid_colors()
        self.update_available_tips_combo()

    def reset_all_tips_empty(self):
        for k in self.tip_inventory:
            self.tip_inventory[k] = False
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
        self.tip_combo['values'] = available
        if available:
            self.tip_combo.set(available[0])
        else:
            self.tip_combo.set("EMPTY")

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
            axis_tokens = " ".join(list(axes))
            home_cmd = f"G28 {axis_tokens}"

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
        if new_vol > MAX_PIPETTE_VOL:
            messagebox.showwarning("Range Error",
                                   f"Max Volume is {MAX_PIPETTE_VOL} uL.\nCurrent: {self.current_pipette_volume} uL")
            return
        if new_vol < MIN_PIPETTE_VOL:
            messagebox.showwarning("Range Error",
                                   f"Min Volume is {MIN_PIPETTE_VOL} uL.\nCurrent: {self.current_pipette_volume} uL")
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
        """
        Moves Z down to the module-specific height, aspirates/dispenses, then moves Z back up.
        Requires self.last_known_module to be set.
        """
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        # 1. Validate State
        if not self.last_known_module:
            messagebox.showerror("Unknown Position",
                                 "The machine does not know which module it is over.\n"
                                 "Please use the 'GO' buttons to move to a specific module first.")
            return

        if self.last_known_module in ["TIPS", "EJECT"]:
            messagebox.showerror("Unsafe Action",
                                 f"Cannot perform liquid handling in {self.last_known_module} area.")
            return

        # 2. Determine Z Heights based on module
        z_action = 0.0
        z_safe = GLOBAL_SAFE_Z

        if self.last_known_module == "PLATE":
            z_action = PLATE_CONFIG["Z_ASPIRATE"] if mode == "aspirate" else PLATE_CONFIG["Z_DISPENSE"]
            z_safe = PLATE_CONFIG["Z_SAFE"]
        elif self.last_known_module == "FALCON":
            z_action = FALCON_RACK_CONFIG["Z_ASPIRATE"] if mode == "aspirate" else FALCON_RACK_CONFIG["Z_DISPENSE"]
            z_safe = FALCON_RACK_CONFIG["Z_SAFE"]
        elif self.last_known_module == "WASH":
            z_action = WASH_RACK_CONFIG["Z_ASPIRATE"] if mode == "aspirate" else WASH_RACK_CONFIG["Z_DISPENSE"]
            z_safe = WASH_RACK_CONFIG["Z_SAFE"]
        else:
            messagebox.showerror("Error", f"No config found for module: {self.last_known_module}")
            return

        # 3. Calculate Volume
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

        if new_vol > MAX_PIPETTE_VOL:
            messagebox.showwarning("Range Error", f"Max Volume is {MAX_PIPETTE_VOL} uL.")
            return
        if new_vol < MIN_PIPETTE_VOL:
            messagebox.showwarning("Range Error", f"Min Volume is {MIN_PIPETTE_VOL} uL.")
            return

        target_e_pos = -1 * new_vol * STEPS_PER_UL

        self.log_line(f"[SMART] {mode.upper()} at {self.last_known_module} (Z={z_action})")
        self.log_command(f"Smart {mode}: {delta_ul}uL @ {self.last_known_module}")

        # 4. Build Sequence
        commands = [
            "G90",
            f"G0 Z{z_action} F{JOG_SPEED_Z}",  # Dive
            f"G1 E{target_e_pos:.3f} F{PIP_SPEED}",  # Action
            "G4 P200",  # Brief pause for pressure equalization
            f"G0 Z{z_safe} F{JOG_SPEED_Z}",  # Retract
            "M18 E"
        ]

        # 5. Execute
        def run_seq():
            self._send_lines_with_ok(commands)
            # Update volume state only after success
            self.current_pipette_volume = new_vol
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

    def eject_tip_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        self.log_line("[SYSTEM] Ejecting Tip...")
        self.log_command("Ejecting Tip")

        commands = self._get_smart_travel_gcode("EJECT", 188.25, 176.0, 190.0)
        commands.extend([
            f"G0 Z160.2 F{JOG_SPEED_Z}",
            f"G4 P500",
            f"G0 Y218 F250",
            f"G0 Z190 F250",
            f"G0 Z{GLOBAL_SAFE_Z} F{JOG_SPEED_Z}"
        ])

        def run_seq():
            self._send_lines_with_ok(commands)
            self.last_known_module = "EJECT"

        threading.Thread(target=run_seq, daemon=True).start()

    def pick_tip_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        target_tip = self.selected_tip_var.get()
        if not target_tip or target_tip == "EMPTY":
            messagebox.showwarning("No Tip", "No fresh tips available or selected.")
            return

        tx, ty = self.get_tip_coordinates(target_tip)
        rack_safe_z = TIP_RACK_CONFIG["Z_TRAVEL"]
        pick_z = TIP_RACK_CONFIG["Z_PICK"]

        self.log_line(f"[SYSTEM] Picking Tip {target_tip} at X{tx:.2f} Y{ty:.2f}...")
        self.log_command(f"Pick Tip: {target_tip}")

        commands = self._get_smart_travel_gcode("TIPS", tx, ty, rack_safe_z)
        commands.extend([
            f"G0 Z{pick_z} F500",
            "G4 P500",
            f"G0 Z{rack_safe_z} F{JOG_SPEED_Z}",
        ])

        def run_seq():
            self._send_lines_with_ok(commands)
            self.last_known_module = "TIPS"
            self.tip_inventory[target_tip] = False
            self.root.after(0, self.update_tip_grid_colors)
            self.root.after(0, self.update_available_tips_combo)

        threading.Thread(target=run_seq, daemon=True).start()

    def move_to_well_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        target_well = self.selected_well_var.get()
        if not target_well:
            return

        wx, wy = self.get_well_coordinates(target_well)
        plate_safe_z = PLATE_CONFIG["Z_SAFE"]

        self.log_line(f"[SYSTEM] Moving above Well {target_well}...")
        self.log_command(f"Move to Well: {target_well}")

        commands = self._get_smart_travel_gcode("PLATE", wx, wy, plate_safe_z)

        def run_seq():
            self._send_lines_with_ok(commands)
            self.last_known_module = "PLATE"

        threading.Thread(target=run_seq, daemon=True).start()

    def move_to_falcon_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        target_falcon = self.selected_falcon_var.get()
        if not target_falcon:
            return

        fx, fy = self.get_falcon_coordinates(target_falcon)
        falcon_safe_z = FALCON_RACK_CONFIG["Z_SAFE"]

        self.log_line(f"[SYSTEM] Moving above Falcon {target_falcon}...")
        self.log_command(f"Move to Falcon: {target_falcon}")

        commands = self._get_smart_travel_gcode("FALCON", fx, fy, falcon_safe_z)

        def run_seq():
            self._send_lines_with_ok(commands)
            self.last_known_module = "FALCON"

        threading.Thread(target=run_seq, daemon=True).start()

    def move_to_wash_sequence(self):
        if not self.ser or not self.ser.is_open:
            messagebox.showwarning("Not Connected", "Please connect to the printer first.")
            return

        target_wash = self.selected_wash_var.get()
        if not target_wash:
            return

        wx, wy = self.get_wash_coordinates(target_wash)
        wash_safe_z = WASH_RACK_CONFIG["Z_SAFE"]

        self.log_line(f"[SYSTEM] Moving above {target_wash}...")
        self.log_command(f"Move to Wash: {target_wash}")

        commands = self._get_smart_travel_gcode("WASH", wx, wy, wash_safe_z)

        def run_seq():
            self._send_lines_with_ok(commands)
            self.last_known_module = "WASH"

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

        commands = []
        commands.append(f"G92 E{current_e_pos:.3f}")
        commands.append(f"G1 E{target_e_pos:.3f} F{MOVEMENT_SPEED}")

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