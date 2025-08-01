#!/usr/bin/env python3
"""
Insurance Control System Runner
Main execution script for running the insurance data processing system.
"""

import os
import sys
from pathlib import Path

# Add the current directory to Python path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

# Import the main function from the complete system
try:
    from coba import main
except ImportError as e:
    print(f"Error importing main system: {e}")
    print("Make sure the insurance_control_system.py file is in the same directory")
    sys.exit(1)

def setup_paths():
    """Setup and validate file paths"""
    
    # Default path - update this to your actual file location
    default_path = r"D:\Run Control 3\IRCS3_build\Input Sheet_IRCS3.xlsx"
    
    # Alternative paths to try
    alternative_paths = [
        "Input Sheet_IRCS3.xlsx",  # Current directory
        "./Input Sheet_IRCS3.xlsx",  # Explicit current directory
        "../Input Sheet_IRCS3.xlsx",  # Parent directory
        "data/Input Sheet_IRCS3.xlsx",  # Data subdirectory
    ]
    
    # Try default path first
    if os.path.exists(default_path):
        return default_path
    
    # Try alternative paths
    for path in alternative_paths:
        if os.path.exists(path):
            return os.path.abspath(path)
    
    # If no file found, ask user for input
    print("Input file not found in default locations.")
    print("\nPlease enter the full path to your 'Input Sheet_IRCS3.xlsx' file:")
    print("(or drag and drop the file into this window)")
    
    while True:
        user_path = input("File path: ").strip().strip('"').strip("'")
        
        if os.path.exists(user_path):
            return user_path
        else:
            print(f"File not found: {user_path}")
            print("Please try again or press Ctrl+C to exit.")

def check_dependencies():
    """Check if required dependencies are installed"""
    required_packages = {
        'pandas': 'pandas',
        'openpyxl': 'openpyxl',
    }
    
    missing_packages = []
    
    for package_name, import_name in required_packages.items():
        try:
            __import__(import_name)
        except ImportError:
            missing_packages.append(package_name)
    
    if missing_packages:
        print("Missing required packages:")
        for package in missing_packages:
            print(f"  - {package}")
        print("\nPlease install them using:")
        print(f"pip install {' '.join(missing_packages)}")
        return False
    
    return True

def main_runner():
    """Main runner function"""
    print("="*60)
    print("Insurance Control System v3.0")
    print("="*60)
    
    # Check dependencies
    print("Checking dependencies...")
    if not check_dependencies():
        input("Press Enter to exit...")
        return
    
    # Setup paths
    print("Setting up file paths...")
    try:
        input_path = setup_paths()
        print(f"Using input file: {input_path}")
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        return
    except Exception as e:
        print(f"Error setting up paths: {e}")
        input("Press Enter to exit...")
        return
    
    # Run the main process
    try:
        print("\nStarting data processing...")
        main(input_path)
        print("\n" + "="*60)
        print("Processing completed successfully!")
        print("="*60)
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        print(f"\nError during processing: {e}")
        print("\nPlease check:")
        print("1. Input file exists and is not corrupted")
        print("2. Required sheets (FILTER_TRAD, FILTER_UL) exist in the Excel file")
        print("3. All referenced data files exist")
        print("4. You have write permissions to the input file")
    
    finally:
        input("\nPress Enter to exit...")

if __name__ == "__main__":
    main_runner()