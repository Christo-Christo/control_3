import os
import sys
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import datetime
import warnings
import xlsxwriter
warnings.filterwarnings('ignore')

input_sheet_path = r"D:\RUN 3\control_3\control_3\IRCS3_local\Input Sheet_IRCS3.xlsx"

class InputSheetConfig:
    def __init__(self, valuation_year, valuation_month, valuation_rate, tradfilter, ulfilter, output_trad, output_ul):
        self.valuation_year = valuation_year
        self.valuation_month = valuation_month
        self.valuation_rate = valuation_rate
        self.tradfilter = tradfilter
        self.ulfilter = ulfilter
        self.output_trad = output_trad
        self.output_ul = output_ul

try:
    from ul_trad import run_trad, run_ul
    from config_reader import setup_configuration, validate_excel_file
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

def get_output_file_paths(excel_path):
    try:
        df = pd.read_excel(excel_path, sheet_name='INPUT_SETTING', engine='openpyxl')
        path_trad, file_trad = '', ''
        path_ul, file_ul = '', ''

        for _, row in df.iterrows():
            cat = str(row.get('Category', '')).strip().lower()
            val = str(row.get('Path', '')).strip()
            if cat == 'output path trad':
                path_trad = val
            elif cat == 'output path ul':
                path_ul = val
            elif cat == 'output trad':
                file_trad = val
            elif cat == 'output ul':
                file_ul = val

        file_trad = file_trad + '.xlsx' if not file_trad.endswith('.xlsx') else file_trad
        file_ul = file_ul + '.xlsx' if not file_ul.endswith('.xlsx') else file_ul

        full_trad = os.path.join(path_trad, file_trad)
        full_ul = os.path.join(path_ul, file_ul)
        return full_trad, full_ul

    except Exception as e:
        print(f"❌ Error getting output paths: {e}")
        return None, None

def safe_get_dict(d, key):
    val = d.get(key)
    return val if isinstance(val, dict) else {}

def normalize_filter_params(params):
    return {k.lower(): v for k, v in params.items()}

def read_filter_config(excel_path, sheet_name):
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name, engine='openpyxl')
        if df.empty:
            return []
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

def get_valuation_info_and_filters(excel_path):
    """Membaca informasi valuation dan filter TRAD/UL sekaligus"""
    try:
        df_input_setting = pd.read_excel(excel_path, sheet_name='INPUT_SETTING', engine='openpyxl')
        # Ambil valuation year, month, rate dari INPUT_SETTING berdasarkan Category
        valuation_year = None
        valuation_month = None
        valuation_rate = None
        for _, row in df_input_setting.iterrows():
            cat = str(row.get('Category', '')).strip().lower()
            val = row.get('Value', None)
            if cat == 'valuation year':
                valuation_year = val
            elif cat == 'valuation month':
                valuation_month = val
            elif cat == 'valuation rate':
                valuation_rate = val

        tradfilter_configs = read_filter_config(excel_path, 'FILTER_TRAD')
        ulfilter_configs = read_filter_config(excel_path, 'FILTER_UL')

        # Ambil list run_name dari filter sebagai tradfilter dan ulfilter untuk penulisan excel nanti
        tradfilter_run_names = [c.get('run_name', '') for c in tradfilter_configs if c.get('run_name', '')]
        ulfilter_run_names = [c.get('run_name', '') for c in ulfilter_configs if c.get('run_name', '')]

        return InputSheetConfig(
            valuation_year=valuation_year,
            valuation_month=valuation_month,
            valuation_rate=valuation_rate,
            tradfilter=tradfilter_run_names,
            ulfilter=ulfilter_run_names,
            output_trad=None,
            output_ul=None
        )

    except Exception as e:
        print(f"❌ Error reading valuation info and filters: {e}")
        return None

def run_single_config(config, product_type):
    try:
        run_name = config.get('run_name', 'Unknown')
        print(f"Running {product_type} configuration: {run_name}")
        normalized_config = normalize_filter_params(config)
        if product_type == 'TRAD':
            return run_name, run_trad(normalized_config)
        elif product_type == 'UL':
            return run_name, run_ul(normalized_config)
        else:
            return run_name, {"error": f"Unknown product type: {product_type}"}
    except Exception as e:
        return run_name, {"error": f"Error running {product_type} config: {str(e)}"}

def run_all_configurations(excel_path):
    print("="*60)
    print("RUNNING ALL CONFIGURATIONS")
    print("="*60)

    trad_configs = read_filter_config(excel_path, 'FILTER_TRAD')
    ul_configs = read_filter_config(excel_path, 'FILTER_UL')

    trad_results = {}
    ul_results = {}

    max_workers = max(8, (os.cpu_count() or 1) * 4)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_type = {}

        for config in trad_configs:
            run_name = config.get('run_name', '')
            if run_name:
                future = executor.submit(run_single_config, config, 'TRAD')
                future_to_type[future] = 'TRAD'

        for config in ul_configs:
            run_name = config.get('run_name', '')
            if run_name:
                future = executor.submit(run_single_config, config, 'UL')
                future_to_type[future] = 'UL'

        for future in as_completed(future_to_type):
            product_type = future_to_type[future]
            try:
                run_name, result = future.result()
                if product_type == 'TRAD':
                    trad_results[run_name] = result
                else:
                    ul_results[run_name] = result

                if "error" in result:
                    print(f"❌ {run_name} ({product_type}): {result['error']}")
                else:
                    print(f"✅ {run_name} ({product_type}): Completed successfully")

            except Exception as e:
                print(f"❌ Exception occurred while processing {product_type}: {str(e)}")

    return trad_results, ul_results

def write_trad_results_to_excel(trad_results, input_config: InputSheetConfig):
    wb = xlsxwriter.Workbook(input_config.output_trad, {'nan_inf_to_errors': True})
    number_format = '_(* #,##0_);_(* (#,##0)_);_(* "-"_);_(@_)'

    header_sum_tablerow = ['DV', 'RAFM', 'Differences']
    header_sum_tablerow2 = ['Total', 'Trad-Life inc. BTPN', 'Trad-Health non-YRT', 'Trad-Health YRT', 'Trad-C']
    tablerow2_len = len(header_sum_tablerow2)

    ws = wb.add_worksheet('Control and Summary')
    ws.freeze_panes(0, 1)
    ws.set_column(0, 0, 20)
    ws.set_column(1, 12, 25)
    ws.set_column(13, 13, 30)

    bold = wb.add_format({'bold': True})
    yellow = wb.add_format({'bold': True, 'bg_color': 'yellow'})
    center_bold = wb.add_format({'bold': True, 'align': 'center'})
    green_underline = wb.add_format({'bold': True, 'underline': True, 'bg_color': 'green'})
    center_merge = wb.add_format({'bold': True, 'align': 'center'})

    ws.write(0, 0, 'Valuation Year', bold)
    ws.write(1, 0, 'Valuation Month', bold)
    ws.write(2, 0, 'FX Rate ValDate', bold)
    ws.write(4, 0, '# of Policies Check', green_underline)
    ws.write(5, 0, '# Run', green_underline)

    ws.write(0, 1, input_config.valuation_year, yellow)
    ws.write(1, 1, input_config.valuation_month, yellow)
    ws.write(2, 1, input_config.valuation_rate, yellow)

    for i, run_name in enumerate(input_config.tradfilter):
        ws.write(6 + i, 0, run_name, yellow)

    for c, item in enumerate(header_sum_tablerow):
        ws.merge_range(4, 1 + (tablerow2_len * c), 4, tablerow2_len + (tablerow2_len * c), item, center_merge)

    ws.merge_range(4, 16, 5, 16, 'Notes', center_merge)

    for i in range(len(header_sum_tablerow)):
        for c, item in enumerate(header_sum_tablerow2):
            ws.write(5, c + 1 + (tablerow2_len * i), item, center_bold)

    for i, run_name in enumerate(input_config.tradfilter):
        if run_name in trad_results and 'ctrlsum_dict' in trad_results[run_name]:
            ctrlsum = pd.DataFrame([trad_results[run_name]['ctrlsum_dict'].get(run_name, {})])
            for c, item_ in enumerate(ctrlsum.iloc[0]):
                ws.write(6 + i, c + 1, item_, wb.add_format({'num_format': number_format}))

    wb.add_worksheet('Diff Breakdown')
    wb.add_worksheet('>>')

    header_diff_tablerow = ['GOC', 'DV # of Policies', 'DV SA', 'RAFM # of Policies', 'RAFM SA', 'Diff # of Policies', 'Diff SA']
    tablecol_fmt = wb.add_format({'bold': True, 'underline': True, 'bg_color':'#92D050'})

    for run_name in input_config.tradfilter:
        if run_name not in trad_results:
            continue
        ws = wb.add_worksheet(f'{run_name}')
        tr = trad_results[run_name]

        df_list = [
            safe_get_dict(tr, 'tabel_total').get(run_name, pd.DataFrame()),
            safe_get_dict(tr, 'tabel_2').get(run_name, pd.DataFrame()),
            safe_get_dict(tr, 'tabel_3').get(run_name, pd.DataFrame()),
            safe_get_dict(tr, 'tabel_4').get(run_name, pd.DataFrame()),
            safe_get_dict(tr, 'tabel_5').get(run_name, pd.DataFrame()),
        ]

        sum_list = [
            safe_get_dict(tr, 'summary_total').get(run_name, pd.DataFrame()),
            safe_get_dict(tr, 'summary_tabel_2').get(run_name, pd.DataFrame()),
            safe_get_dict(tr, 'summary_tabel_3').get(run_name, pd.DataFrame()),
            safe_get_dict(tr, 'summary_tabel_4').get(run_name, pd.DataFrame()),
            safe_get_dict(tr, 'summary_tabel_5').get(run_name, pd.DataFrame()),
        ]
        print(f"Menulis worksheet untuk run: {run_name}")
        for idx, df in enumerate(df_list):
            print(f"  Tabel {idx}: shape {df.shape}")
        for idx, summary in enumerate(sum_list):
            print(f"  Summary {idx}: shape {summary.shape}")
        col_starts = [1, 9, 17, 25, 33]

        for idx, (df, summary) in enumerate(zip(df_list, sum_list)):
            ws.set_column(col_starts[idx], col_starts[idx] + 6, 20)
            ws.set_column(col_starts[idx], col_starts[idx], 40)
            for c, item in enumerate(header_diff_tablerow):
                ws.write(2, col_starts[idx] + c, item, wb.add_format({'bold': True, 'underline': True}))
            for r, item in enumerate(['Total All from DV', 'Grand Total Summary', 'Check']):
                ws.write(3 + r, col_starts[idx], item, tablecol_fmt)

            if idx > 0:
                label = ['Total BTPN', 'Total Health non-YRT', 'Total Health YRT', 'Total C'][idx - 1]
                ws.write(3, col_starts[idx], label, tablecol_fmt)

            for row in range(len(summary)):
                for c, item in enumerate(summary.iloc[row]):
                    ws.write(3 + row, col_starts[idx] + 1 + c, item, wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
            for row in range(len(df)):
                for c, item in enumerate(df.iloc[row]):
                    ws.write(6 + row, col_starts[idx] + c, item, wb.add_format({'num_format': number_format}))

    wb.close()

def write_ul_results_to_excel(ul_results, input_config: InputSheetConfig):
    wb = xlsxwriter.Workbook(input_config.output_ul, {'nan_inf_to_errors': True})
    number_format = '_(* #,##0_);_(* (#,##0)_);_(* "-"_);_(@_)'
    header_sum_tablerow = ['DV', 'RAFM', 'Differences']
    header_sum_tablerow2 = ['Total', 'UL & SH & PI', 'Tasbih', 'GS']

    ws = wb.add_worksheet('Control and Summary')
    ws.freeze_panes(0, 1)
    ws.set_column(0, 0, 20)
    ws.set_column(1, 12, 25)
    ws.set_column(13, 13, 30)

    bold = wb.add_format({'bold': True})
    yellow = wb.add_format({'bold': True, 'bg_color': 'yellow'})
    center_bold = wb.add_format({'bold': True, 'align': 'center'})
    green_underline = wb.add_format({'bold': True, 'underline': True, 'bg_color': 'green'})
    center_merge = wb.add_format({'bold': True, 'align': 'center'})

    ws.write(0, 0, 'Valuation Year', bold)
    ws.write(1, 0, 'Valuation Month', bold)
    ws.write(2, 0, 'FX Rate ValDate', bold)
    ws.write(4, 0, '# of Policies Check', green_underline)
    ws.write(5, 0, '# Run', green_underline)

    ws.write(0, 1, input_config.valuation_year, yellow)
    ws.write(1, 1, input_config.valuation_month, yellow)
    ws.write(2, 1, input_config.valuation_rate, yellow)

    for i, run_name in enumerate(input_config.ulfilter):
        ws.write(6 + i, 0, run_name, yellow)

    for c, item in enumerate(header_sum_tablerow):
        ws.merge_range(4, 1 + (4 * c), 4, 4 + (4 * c), item, center_merge)

    ws.merge_range(4, 13, 5, 13, 'Notes', center_merge)

    for i in range(len(header_sum_tablerow)):
        for c, item in enumerate(header_sum_tablerow2):
            ws.write(5, c + 1 + (4 * i), item, center_bold)

    for i, run_name in enumerate(input_config.ulfilter):
        if run_name in ul_results and 'ctrlsum_dict' in ul_results[run_name]:
            ctrlsum = pd.DataFrame([ul_results[run_name]['ctrlsum_dict'].get(run_name, {})])
            for c, item in enumerate(ctrlsum.iloc[0]):
                ws.write(6 + i, c + 1, item, wb.add_format({'num_format': number_format}))

    wb.add_worksheet('Diff Breakdown')
    wb.add_worksheet('>>')

    header_diff_tablerow = ['GOC', 'DV # of Policies', 'DV Fund Value', 'RAFM # of Policies', 'RAFM Fund Value', 'Diff # of Policies', 'Diff Fund Value']
    tablecol_fmt = wb.add_format({'bold': True, 'underline': True, 'bg_color': '#92D050'})

    for run_name in input_config.ulfilter:
        if run_name not in ul_results:
            continue
        ws = wb.add_worksheet(f'{run_name}')
        ul = ul_results[run_name]

        df_list = [
            safe_get_dict(ul, 'tabel_total').get(run_name, pd.DataFrame()),
            safe_get_dict(ul, 'tabel_2').get(run_name, pd.DataFrame()),
            safe_get_dict(ul, 'tabel_3').get(run_name, pd.DataFrame()),
        ]

        sum_list = [
            safe_get_dict(ul, 'summary_total').get(run_name, pd.DataFrame()),
            safe_get_dict(ul, 'summary_tabel_2').get(run_name, pd.DataFrame()),
            safe_get_dict(ul, 'summary_tabel_3').get(run_name, pd.DataFrame()),
        ]
        print(f"Menulis worksheet untuk run: {run_name}")
        for idx, df in enumerate(df_list):
            print(f"  Tabel {idx}: shape {df.shape}")
        for idx, summary in enumerate(sum_list):
            print(f"  Summary {idx}: shape {summary.shape}")

        col_starts = [1, 9, 17]

        for idx, (df, summary) in enumerate(zip(df_list, sum_list)):
            ws.set_column(col_starts[idx], col_starts[idx] + 6, 20)
            ws.set_column(col_starts[idx], col_starts[idx], 40)

            for c, item in enumerate(header_diff_tablerow):
                ws.write(2, col_starts[idx] + c, item, wb.add_format({'bold': True, 'underline': True}))
            for r, item in enumerate(['Total All from DV', 'Grand Total Summary', 'Check']):
                ws.write(3 + r, col_starts[idx], item, tablecol_fmt)

            if idx == 1:
                ws.write(3, col_starts[idx], 'Total Tasbih', tablecol_fmt)
            elif idx == 2:
                ws.write(3, col_starts[idx], 'Total Group Savings', tablecol_fmt)

            for row in range(len(summary)):
                for c, item in enumerate(summary.iloc[row]):
                    ws.write(3 + row, col_starts[idx] + 1 + c, item, wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))

            for row in range(len(df)):
                for c, item in enumerate(df.iloc[row]):
                    ws.write(6 + row, col_starts[idx] + c, item, wb.add_format({'num_format': number_format}))

    wb.close()

def main():
    start_time = time.time()

    print("="*60)
    print("INSURANCE CONTROL SYSTEM")
    print("="*60)
    print(f"Input file: {input_sheet_path}")
    print("="*60)

    if not os.path.exists(input_sheet_path):
        print(f"❌ Input file not found: {input_sheet_path}")
        return False

    is_valid, message = validate_excel_file(input_sheet_path)
    if not is_valid:
        print(f"❌ File validation failed: {message}")
        print("\nAttempting to setup configuration...")
        setup_success = setup_configuration(input_sheet_path)
        if setup_success:
            print("Configuration setup completed. Retrying validation...")
            is_valid, message = validate_excel_file(input_sheet_path)
            if not is_valid:
                print(f"❌ Validation still failed: {message}")
                return False
        else:
            print("❌ Configuration setup failed")
            return False

    print(f"✅ {message}")

    # Baca config valuation dan filters (run_name list)
    input_config = get_valuation_info_and_filters(input_sheet_path)
    if input_config is None:
        print("❌ Failed to read input configuration")
        return False

    # Baca output paths
    output_trad_path, output_ul_path = get_output_file_paths(input_sheet_path)
    if not output_trad_path or not output_ul_path:
        print("❌ Output paths not properly configured")
        return False
    input_config.output_trad = output_trad_path
    input_config.output_ul = output_ul_path

    trad_results, ul_results = run_all_configurations(input_sheet_path)

    if not trad_results and not ul_results:
        print("❌ No results to process")
        return False

    print("\n" + "="*60)
    print("WRITING RESULTS TO EXCEL")
    print("="*60)

    if trad_results:
        output_folder = os.path.dirname(output_trad_path)
        os.makedirs(output_folder, exist_ok=True)  # Pastikan folder ada
        print(f"\n📤 Menulis hasil TRAD ke: {output_trad_path}")
        write_trad_results_to_excel(trad_results, input_config)

    if ul_results:
        output_folder = os.path.dirname(output_ul_path)
        os.makedirs(output_folder, exist_ok=True)  # Pastikan folder ada
        print(f"\n📤 Menulis hasil UL ke: {output_ul_path}")
        write_ul_results_to_excel(ul_results, input_config)

    elapsed = time.time() - start_time
    formatted = str(datetime.timedelta(seconds=int(elapsed)))
    print(f"⏱️ Total runtime: {formatted}")

    return True

def show_menu():
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
            setup_success = setup_configuration(input_sheet_path)
            print("✅ Configuration setup completed" if setup_success else "❌ Configuration setup failed")
        elif choice == '3':
            is_valid, message = validate_excel_file(input_sheet_path)
            print(f"✅ {message}" if is_valid else f"❌ {message}")
        elif choice == '4':
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Please select 1-4.")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--menu':
        show_menu()
    else:
        success = main()
        if os.name == 'nt':
            input("\nPress Enter to exit...")
        sys.exit(0 if success else 1)
