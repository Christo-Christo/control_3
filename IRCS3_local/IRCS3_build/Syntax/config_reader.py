import pandas as pd
import os

def read_input_settings(excel_path):
    try:
        df = pd.read_excel(excel_path, sheet_name='INPUT_SETTING', engine='openpyxl')
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

    base_path = settings.get('Output Path Trad', '') or settings.get('Output Path UL', '')
    if base_path:
        base_path = os.path.dirname(base_path)
    
    val_month = settings.get('Valuation Month', '12')
    val_year = settings.get('Valuation Year', '2025') 
    fx_rate = settings.get('FX Rate Valdate', '16233')
    trad_config = {
        'RUN': f"{run_name}_TRAD",
        'run_name': f"{run_name}_TRAD",
        'path_dv': '', 
        'path_rafm': '', 
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

    ul_config = {
        'RUN': f"{run_name}_UL",
        'run_name': f"{run_name}_UL", 
        'path_dv': '',
        'path_rafm': '',
        'path_uvsg': '',
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
    try:
        trad_config, ul_config = convert_settings_to_filter_config(settings)

        trad_df = pd.DataFrame([trad_config])
        ul_df = pd.DataFrame([ul_config])

        with pd.ExcelWriter(excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            trad_df.to_excel(writer, sheet_name='FILTER_TRAD', index=False)
            ul_df.to_excel(writer, sheet_name='FILTER_UL', index=False)
        
        print("✓ Created FILTER_TRAD and FILTER_UL sheets from INPUT_SETTING")
        return True
        
    except Exception as e:
        print(f"Error creating filter sheets: {str(e)}")
        return False

def validate_and_setup_input_file(excel_path):
    try:
        if not os.path.exists(excel_path):
            return False, f"File not found: {excel_path}"
        
        xl_file = pd.ExcelFile(excel_path)
        existing_sheets = xl_file.sheet_names
        
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
                    return True,
                else:
                    return False,
            else:
                return False,
        
        return False,
        
    except Exception as e:
        return False, f"Error validating input file: {str(e)}"

def get_file_paths_from_user():
    print("\n" + "="*50)
    print("FILE PATH SETUP")
    print("="*50)
    print("Please provide the paths to your data files:")
    
    paths = {}
    
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
    print("\nUVSG file is optional (for Unit Linked products)")
    uvsg_path = input("UVSG file path (Excel, press Enter to skip): ").strip().strip('"').strip("'")
    if uvsg_path and os.path.exists(uvsg_path):
        paths['uvsg'] = uvsg_path
    else:
        paths['uvsg'] = ''
        if uvsg_path:
            print(f"Warning: UVSG file not found: {uvsg_path}")
            print("Continuing without UVSG file...")
    
    return paths

def update_filter_sheets_with_paths(excel_path, file_paths):
    try:
        trad_df = pd.read_excel(excel_path, sheet_name='FILTER_TRAD')
        ul_df = pd.read_excel(excel_path, sheet_name='FILTER_UL')

        trad_df['path_dv'] = file_paths['dv']
        trad_df['path_rafm'] = file_paths['rafm']
        
        ul_df['path_dv'] = file_paths['dv']
        ul_df['path_rafm'] = file_paths['rafm']
        ul_df['path_uvsg'] = file_paths.get('uvsg', '')
        
        with pd.ExcelWriter(excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            trad_df.to_excel(writer, sheet_name='FILTER_TRAD', index=False)
            ul_df.to_excel(writer, sheet_name='FILTER_UL', index=False)
        
        print("✓ Updated filter sheets with file paths")
        return True
        
    except Exception as e:
        print(f"Error updating filter sheets: {str(e)}")
        return False

def setup_configuration(excel_path):

    print("Setting up configuration...")
    
    is_valid, message = validate_and_setup_input_file(excel_path)
    if not is_valid:
        print(f"Configuration setup failed: {message}")
        return False
    
    print(f"✓ {message}")

    file_paths = get_file_paths_from_user()
    if not file_paths:
        print("File path setup cancelled")
        return False

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
