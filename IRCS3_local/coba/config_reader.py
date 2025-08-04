import pandas as pd
import os

def read_input_settings(excel_path):
    """Read settings from INPUT_SETTING sheet"""
    try:
        # Read the INPUT_SETTING sheet
        df = pd.read_excel(excel_path, sheet_name='INPUT_SETTING', engine='openpyxl')
        
        # Convert to dictionary format
        settings = {}
        for _, row in df.iterrows():
            if pd.notna(row.get('Category')) and pd.notna(row.get('Path')):
                category = str(row['Category']).strip()
                path = str(row['Path']).strip()
                settings[category] = path
        
        return settings
        
    except Exception as e:
        print(f"Error reading INPUT_SETTING sheet: {str(e)}")
        return {}

def convert_settings_to_filter_config(settings, run_name="Default_Run"):
    """Convert INPUT_SETTING format to filter configuration format"""
    
    # Extract paths
    base_path = settings.get('Output Path Trad', '') or settings.get('Output Path UL', '')
    if base_path:
        base_path = os.path.dirname(base_path)
    
    # Get valuation info
    val_month = settings.get('Valuation Month', '12')
    val_year = settings.get('Valuation Year', '2025') 
    fx_rate = settings.get('FX Rate Valdate', '16233')
    
    # Create filter configuration for TRAD
    trad_config = {
        'RUN': f"{run_name}_TRAD",
        'run_name': f"{run_name}_TRAD",
        'path_dv': '',  # Will be set by user
        'path_rafm': '',  # Will be set by user
        'USDIDR': float(fx_rate) if fx_rate and fx_rate.replace('.','').isdigit() else 16233.0,
        'only_channel': '',
        'exclude_channel': '',
        'only_currency': '',
        'exclude_currency': '',
        'only_portfolio': '',
        'exclude_portfolio': '',
        'only_cohort': '',
        'exclude_cohort': '',
        'only_period': '',
        'exclude_period': ''
    }
    
    # Create filter configuration for UL
    ul_config = {
        'RUN': f"{run_name}_UL",
        'run_name': f"{run_name}_UL", 
        'path_dv': '',  # Will be set by user
        'path_rafm': '',  # Will be set by user
        'path_uvsg': '',  # Optional - Will be set by user
        'USDIDR': float(fx_rate) if fx_rate and fx_rate.replace('.','').isdigit() else 16233.0,
        'only_channel': '',
        'exclude_channel': '',
        'only_currency': '',
        'exclude_currency': '',
        'only_portfolio': '',
        'exclude_portfolio': '',
        'only_cohort': '',
        'exclude_cohort': '',
        'only_period': '',
        'exclude_period': ''
    }
    
    return trad_config, ul_config

def create_filter_sheets_from_settings(excel_path, settings):
    """Create FILTER_TRAD and FILTER_UL sheets from INPUT_SETTING"""
    try:
        # Convert settings to filter configs
        trad_config, ul_config = convert_settings_to_filter_config(settings)
        
        # Create DataFrames
        trad_df = pd.DataFrame([trad_config])
        ul_df = pd.DataFrame([ul_config])
        
        # Write to Excel using ExcelWriter to add new sheets
        with pd.ExcelWriter(excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            trad_df.to_excel(writer, sheet_name='FILTER_TRAD', index=False)
            ul_df.to_excel(writer, sheet_name='FILTER_UL', index=False)
        
        print("✓ Created FILTER_TRAD and FILTER_UL sheets from INPUT_SETTING")
        return True
        
    except Exception as e:
        print(f"Error creating filter sheets: {str(e)}")
        return False

def validate_and_setup_input_file(excel_path):
    """Validate input file and setup filter sheets if needed"""
    try:
        # Check if file exists
        if not os.path.exists(excel_path):
            return False, f"File not found: {excel_path}"
        
        # Read existing sheets
        xl_file = pd.ExcelFile(excel_path)
        existing_sheets = xl_file.sheet_names
        
        # Check if filter sheets exist
        has_filter_trad = 'FILTER_TRAD' in existing_sheets
        has_filter_ul = 'FILTER_UL' in existing_sheets
        has_input_setting = 'INPUT_SETTING' in existing_sheets
        
        if has_filter_trad and has_filter_ul:
            return True, "Filter sheets already exist"
        
        if has_input_setting:
            print("Found INPUT_SETTING sheet. Creating filter sheets...")
            settings = read_input_settings(excel_path)
            
            if settings:
                success = create_filter_sheets_from_settings(excel_path, settings)
                if success:
                    return True, "Filter sheets created from INPUT_SETTING"
                else:
                    return False, "Failed to create filter sheets"
            else:
                return False, "Could not read INPUT_SETTING data"
        
        return False, "No INPUT_SETTING or FILTER sheets found"
        
    except Exception as e:
        return False, f"Error validating input file: {str(e)}"

def get_file_paths_from_user():
    """Get file paths from user for DV, RAFM, and UVSG files"""
    print("\n" + "="*50)
    print("FILE PATH SETUP")
    print("="*50)
    print("Please provide the paths to your data files:")
    
    paths = {}
    
    # Get DV file path
    while True:
        dv_path = input("DV file path (CSV or Excel): ").strip().strip('"').strip("'")
        if os.path.exists(dv_path):
            paths['dv'] = dv_path
            break
        else:
            print(f"File not found: {dv_path}")
            retry = input("Try again? (y/n): ").lower()
            if retry != 'y':
                return None
    
    # Get RAFM file path
    while True:
        rafm_path = input("RAFM file path (Excel): ").strip().strip('"').strip("'")
        if os.path.exists(rafm_path):
            paths['rafm'] = rafm_path
            break
        else:
            print(f"File not found: {rafm_path}")
            retry = input("Try again? (y/n): ").lower()
            if retry != 'y':
                return None
    
    # Get UVSG file path (optional)
    print("\nUVSG file is optional (for Unit Linked products)")
    uvsg_path = input("UVSG file path (Excel, press Enter to skip): ").strip().strip('"').strip("'")
    if uvsg_path and os.path.exists(uvsg_path):
        paths['uvsg'] = uvsg_path
    else:
        paths['uvsg'] = ''
        if uvsg_path:  # User provided path but file doesn't exist
            print(f"Warning: UVSG file not found: {uvsg_path}")
            print("Continuing without UVSG file...")
    
    return paths

def update_filter_sheets_with_paths(excel_path, file_paths):
    """Update filter sheets with actual file paths"""
    try:
        # Read existing filter sheets
        trad_df = pd.read_excel(excel_path, sheet_name='FILTER_TRAD')
        ul_df = pd.read_excel(excel_path, sheet_name='FILTER_UL')
        
        # Update paths
        trad_df['path_dv'] = file_paths['dv']
        trad_df['path_rafm'] = file_paths['rafm']
        
        ul_df['path_dv'] = file_paths['dv']
        ul_df['path_rafm'] = file_paths['rafm']
        ul_df['path_uvsg'] = file_paths.get('uvsg', '')
        
        # Write back to Excel
        with pd.ExcelWriter(excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            trad_df.to_excel(writer, sheet_name='FILTER_TRAD', index=False)
            ul_df.to_excel(writer, sheet_name='FILTER_UL', index=False)
        
        print("✓ Updated filter sheets with file paths")
        return True
        
    except Exception as e:
        print(f"Error updating filter sheets: {str(e)}")
        return False

# Main function that can be called by run_program.py
def setup_configuration(excel_path):
    """Main function to setup configuration from input file"""
    print("Setting up configuration...")
    
    # Validate and setup input file
    is_valid, message = validate_and_setup_input_file(excel_path)
    if not is_valid:
        print(f"Configuration setup failed: {message}")
        return False
    
    print(f"✓ {message}")
    
    # Get file paths from user
    file_paths = get_file_paths_from_user()
    if not file_paths:
        print("File path setup cancelled")
        return False
    
    # Update filter sheets with paths
    success = update_filter_sheets_with_paths(excel_path, file_paths)
    if not success:
        return False
    
    print("✓ Configuration setup completed successfully")
    return True

def validate_excel_file(file_path):
    try:
        import pandas as pd
        xl = pd.ExcelFile(file_path)
        if 'FILTER_TRAD' in xl.sheet_names and 'FILTER_UL' in xl.sheet_names:
            return True, "Validation successful"
        else:
            return False, "Required sheets missing"
    except Exception as e:
        return False, str(e)
