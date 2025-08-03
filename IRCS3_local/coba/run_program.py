#!/usr/bin/env python3
"""
Main Program Runner for Insurance Control System
This is the main entry point that orchestrates the entire process.
"""

import os
import sys
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION - CHANGE THIS PATH TO YOUR INPUT FILE
# ============================================================================
input_sheet = r"C:\Users\YourName\Documents\Insurance_Control_Input.xlsx"

# Import modules
try:
    from coba import run_trad, run_ul
    from config_reader import setup_configuration
    from run import process_and_write_results, validate_excel_file
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Make sure all required files are in the same directory:")
    print("- coba.py")
    print("- config_reader.py") 
    print("- run.py")
    sys.exit(1)

def make_columns_case_insensitive(df):
    """Make DataFrame column names case-insensitive by converting to lowercase"""
    if df.empty:
        return df
    
    # Create a mapping of lowercase to original column names
    column_mapping = {col.lower(): col for col in df.columns}
    
    # Rename columns to lowercase
    df_lower = df.copy()
    df_lower.columns = df_lower.columns.str.lower()
    
    return df_lower, column_mapping

def restore_column_names(df, column_mapping):
    """Restore original column names after processing"""
    if df.empty:
        return df
    
    # Create reverse mapping
    reverse_mapping = {v.lower(): v for v in column_mapping.values()}
    
    # Restore original column names where possible
    new_columns = []
    for col in df.columns:
        if col in reverse_mapping:
            new_columns.append(reverse_mapping[col])
        else:
            new_columns.append(col)
    
    df.columns = new_columns
    return df

def load_and_normalize_excel_sheet(file_path, sheet_name, required_columns=None):
    """Load Excel sheet and make column names case-insensitive"""
    try:
        if not file_path or not os.path.exists(file_path):
            print(f"Warning: File not found: {file_path}")
            return pd.DataFrame(), {}
        
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
        
        if df.empty:
            return pd.DataFrame(), {}
        
        # Store original column mapping
        original_columns = df.columns.tolist()
        column_mapping = {col.lower(): col for col in original_columns}
        
        # Convert to lowercase
        df.columns = df.columns.str.lower()
        
        if required_columns:
            # Convert required columns to lowercase for comparison
            required_lower = [col.lower() for col in required_columns]
            missing_cols = [col for col in required_lower if col not in df.columns]
            
            if missing_cols:
                print(f"Warning: Missing columns {missing_cols} in {sheet_name}")
                return pd.DataFrame(), {}
            
            df = df[required_lower]
        
        return df, column_mapping
        
    except Exception as e:
        print(f"Error loading {sheet_name} from {file_path}: {str(e)}")
        return pd.DataFrame(), {}

def load_and_normalize_csv(file_path):
    """Load CSV file and make column names case-insensitive"""
    try:
        if not file_path or not os.path.exists(file_path):
            print(f"Warning: File not found: {file_path}")
            return pd.DataFrame(), {}
        
        df = pd.read_csv(file_path)
        
        if df.empty:
            return pd.DataFrame(), {}
        
        # Store original column mapping
        original_columns = df.columns.tolist()
        column_mapping = {col.lower(): col for col in original_columns}
        
        # Convert to lowercase
        df.columns = df.columns.str.lower()
        
        return df, column_mapping
        
    except Exception as e:
        print(f"Error loading CSV {file_path}: {str(e)}")
        return pd.DataFrame(), {}

def normalize_filter_params(params):
    """Normalize parameter keys to lowercase for case-insensitive processing"""
    normalized_params = {}
    for key, value in params.items():
        normalized_params[key.lower()] = value
    return normalized_params

def read_filter_config(excel_path, sheet_name):
    """Read filter configuration from Excel sheet with case-insensitive processing"""
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name, engine='openpyxl')
        if df.empty:
            return []
        
        # Make column names case-insensitive
        df.columns = df.columns.str.lower()
        
        configs = []
        for _, row in df.iterrows():
            config = {}
            for col in df.columns:
                config[col] = row[col] if pd.notna(row[col]) else ''
            configs.append(config)
        
        return configs
    except Exception as e:
        print(f"Error reading {sheet_name}: {str(e)}")
        return []

def run_single_config(config, product_type):
    """Run a single configuration with case-insensitive processing"""
    try:
        print(f"Running {product_type} configuration: {config.get('run', 'Unknown')}")
        
        # Normalize config parameters
        normalized_config = normalize_filter_params(config)
        
        if product_type == 'TRAD':
            result = run_trad(normalized_config)
        elif product_type == 'UL':
            result = run_ul(normalized_config)
        else:
            return {"error": f"Unknown product type: {product_type}"}
        
        return result
        
    except Exception as e:
        return {"error": f"Error running {product_type} config: {str(e)}"}

def run_all_configurations(excel_path):
    """Run all configurations from Excel file"""
    print("="*60)
    print("RUNNING ALL CONFIGURATIONS")
    print("="*60)
    
    results = {}
    
    # Read TRAD configurations
    trad_configs = read_filter_config(excel_path, 'FILTER_TRAD')
    ul_configs = read_filter_config(excel_path, 'FILTER_UL')
    
    if not trad_configs and not ul_configs:
        print("No configurations found in FILTER_TRAD or FILTER_UL sheets")
        return results
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_config = {}
        
        # Submit TRAD jobs
        for config in trad_configs:
            if config.get('run', ''):
                future = executor.submit(run_single_config, config, 'TRAD')
                future_to_config[future] = (config.get('run', 'Unknown_TRAD'), 'TRAD')
        
        # Submit UL jobs
        for config in ul_configs:
            if config.get('run', ''):
                future = executor.submit(run_single_config, config, 'UL')
                future_to_config[future] = (config.get('run', 'Unknown_UL'), 'UL')
        
        # Collect results
        for future in as_completed(future_to_config):
            run_name, product_type = future_to_config[future]
            try:
                result = future.result()
                results[run_name] = result
                
                if "error" in result:
                    print(f"❌ {run_name} ({product_type}): {result['error']}")
                else:
                    print(f"✅ {run_name} ({product_type}): Completed successfully")
                    
            except Exception as e:
                print(f"❌ {run_name} ({product_type}): Exception occurred: {str(e)}")
                results[run_name] = {"error": str(e)}
    
    return results

def main():
    """Main function"""
    print("="*60)
    print("INSURANCE CONTROL SYSTEM")
    print("="*60)
    print(f"Input file: {input_sheet}")
    print("="*60)
    
    # Check if input file exists
    if not os.path.exists(input_sheet):
        print(f"❌ Input file not found: {input_sheet}")
        print("\nPlease update the 'input_sheet' variable at the top of this script")
        print("with the correct path to your Excel file.")
        return False
    
    # Validate Excel file
    is_valid, message = validate_excel_file(input_sheet)
    if not is_valid:
        print(f"❌ File validation failed: {message}")
        
        # Try to setup configuration if validation fails
        print("\nAttempting to setup configuration...")
        setup_success = setup_configuration(input_sheet)
        if setup_success:
            print("Configuration setup completed. Retrying validation...")
            is_valid, message = validate_excel_file(input_sheet)
            if not is_valid:
                print(f"❌ Validation still failed: {message}")
                return False
        else:
            print("❌ Configuration setup failed")
            return False
    
    print(f"✅ {message}")
    
    # Run all configurations
    results = run_all_configurations(input_sheet)
    
    if not results:
        print("❌ No results to process")
        return False
    
    # Write results to Excel
    print("\n" + "="*60)
    print("WRITING RESULTS TO EXCEL")
    print("="*60)
    
    success = process_and_write_results(input_sheet, results)
    
    if success:
        print("✅ All processes completed successfully!")
        print(f"Check your results in: {input_sheet}")
    else:
        print("❌ Some processes failed. Check the error messages above.")
    
    return success

def show_menu():
    """Show interactive menu for user"""
    while True:
        print("\n" + "="*50)
        print("INSURANCE CONTROL SYSTEM - MENU")
        print("="*50)
        print("1. Run All Configurations")
        print("2. Setup Configuration from INPUT_SETTING")
        print("3. Validate Input File")
        print("4. Exit")
        print("="*50)
        
        choice = input("Select an option (1-4): ").strip()
        
        if choice == '1':
            main()
        elif choice == '2':
            setup_success = setup_configuration(input_sheet)
            if setup_success:
                print("✅ Configuration setup completed")
            else:
                print("❌ Configuration setup failed")
        elif choice == '3':
            is_valid, message = validate_excel_file(input_sheet)
            if is_valid:
                print(f"✅ {message}")
            else:
                print(f"❌ {message}")
        elif choice == '4':
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Please select 1-4.")

if __name__ == "__main__":
    # Check if running in interactive mode
    if len(sys.argv) > 1 and sys.argv[1] == '--menu':
        show_menu()
    else:
        # Run directly
        success = main()
        
        # Keep console open on Windows
        if os.name == 'nt':  # Windows
            input("\nPress Enter to exit...")
        
        sys.exit(0 if success else 1)