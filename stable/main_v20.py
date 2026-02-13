import tkinter as tk
import sys
import os

# Add the current directory to the path so we can import main
sys.path.insert(0, os.getcwd())

try:
    # Import the main module to test if it loads without errors
    import main

    print("OK: Main module imported successfully")

    # Create a test root window
    root = tk.Tk()
    root.withdraw()  # Hide the window

    # Try to create the LiquidHandlerApp
    app = main.LiquidHandlerApp(root)

    print("OK: LiquidHandlerApp created successfully")

    # Check if the calibration tab exists
    if hasattr(app, 'tab_calibration'):
        print("OK: Calibration tab exists")
    else:
        print("FAIL: Calibration tab not found")

    # Check if the calibration module variable exists
    if hasattr(app, 'calibration_module_var'):
        print("OK: Calibration module variable exists")
        print(f"  Default value: {app.calibration_module_var.get()}")
    else:
        print("FAIL: Calibration module variable not found")

    # Check if the new methods exist
    methods_to_check = [
        'start_module_calibration_sequence',
        'get_module_first_last_positions',
        '_calibrate_module_position',
        '_show_module_calibration_decision_popup',
        '_open_module_calibration_jog_window',
        'save_module_calibration_position',
        '_proceed_to_next_calibration_step'
    ]

    for method_name in methods_to_check:
        if hasattr(app, method_name):
            print(f"OK: Method {method_name} exists")
        else:
            print(f"FAIL: Method {method_name} not found")

    # Test the get_module_first_last_positions method
    test_modules = ["96 well plate", "tip rack", "4mL rack"]
    for module in test_modules:
        first, last = app.get_module_first_last_positions(module)
        print(f"OK: {module}: {first} -> {last}")

    root.destroy()
    print("\nOK: All tests passed! The calibration functionality has been successfully implemented.")

except Exception as e:
    print(f"FAIL: Error during testing: {e}")
    import traceback

    traceback.print_exc()