import os
import sys
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import datetime
import warnings
import xlsxwriter
from xlsxwriter.utility import xl_col_to_name
warnings.filterwarnings('ignore')


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
    from Syntax.ul_trad import run_trad, run_ul
    from Syntax.config_reader import setup_configuration, validate_excel_file
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
    import pandas as pd
    df = pd.read_excel(excel_path, sheet_name=sheet_name, dtype=str)
    df = df.fillna('') 
    df.columns = df.columns.str.strip()
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].str.replace(r'\.0$', '', regex=True)
    return df.to_dict(orient='records')

def get_valuation_info_and_filters(excel_path):
    try:
        df_input_setting = pd.read_excel(excel_path, sheet_name='INPUT_SETTING', engine='openpyxl')
        valuation_year = None
        valuation_month = None
        valuation_rate = None
        
        for _, row in df_input_setting.iterrows():
            cat = str(row.get('Category', '')).strip()
            val = row.get('Path', None)
            
            if cat == 'Valuation Year':
                valuation_year = val
            elif cat == 'Valuation Month':
                valuation_month = val
            elif cat == 'FX Rate Valdate':
                valuation_rate = val

        tradfilter_configs = read_filter_config(excel_path, 'FILTER_TRAD')
        ulfilter_configs = read_filter_config(excel_path, 'FILTER_UL')

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

def convert_trad_result_to_standard(result):
    return {
        'tables': [
            result.get('tabel_total', pd.DataFrame()),
            result.get('tabel_2', pd.DataFrame()),
            result.get('tabel_3', pd.DataFrame()),
            result.get('tabel_4', pd.DataFrame()),
            result.get('tabel_5', pd.DataFrame())
        ],
        'summaries': [
            result.get('summary_total', pd.DataFrame()),
            result.get('summary_tabel_2', pd.DataFrame()),
            result.get('summary_tabel_3', pd.DataFrame()),
            result.get('summary_tabel_4', pd.DataFrame()),
            result.get('summary_tabel_5', pd.DataFrame())
        ]
    }

def write_trad_results_to_excel(trad_results, input_config: InputSheetConfig):
    wb = xlsxwriter.Workbook(input_config.output_trad, {'nan_inf_to_errors': True})
    number_format = '_(* #,##0_);_(* (#,##0)_);_(* "-"_);_(@_)'
    total_run = len(input_config.tradfilter)
    header_sum_tablerow = ['DV', 'RAFM', 'Differences']
    header_sum_tablerow2 = ['Total', 'Trad-Life inc. BTPN', 'Trad-Health non-YRT', 'Trad-Health YRT', 'Trad-C']
    tablerow2_len = len(header_sum_tablerow2)

    ws = wb.add_worksheet('Control and Summary')
    ws.freeze_panes(0, 1)
    ws.set_column(0, 0, 20)
    ws.set_column(1, 12, 25)
    ws.set_column(13, 17, 30)

    bold = wb.add_format({'bold': True})
    yellow = wb.add_format({'bold': True, 'bg_color': 'yellow'})
    center_bold = wb.add_format({'bold': True, 'align': 'center'})
    green_underline = wb.add_format({'bold': True, 'underline': True, 'bg_color': 'green'})
    center_merge = wb.add_format({'bold': True, 'align': 'center'})
    border_number = wb.add_format({'num_format': number_format, 'border': 1})

    ws.write(0, 0, 'Valuation Year', bold)
    ws.write(1, 0, 'Valuation Month', bold)
    ws.write(2, 0, 'FX Rate ValDate', bold)
    ws.write(4, 0, '# of Policies Check', green_underline)
    ws.write(5, 0, '# Run', green_underline)
    ws.write(8 + total_run, 0, 'SA Check', green_underline)
    ws.write(9 + total_run, 0, '# Run', green_underline)

    ws.write(0, 1, input_config.valuation_year, yellow)
    ws.write(1, 1, input_config.valuation_month, yellow)
    ws.write(2, 1, input_config.valuation_rate, yellow)

    for c, item in enumerate(header_sum_tablerow):
        ws.merge_range(4, 1 + (tablerow2_len * c), 4, tablerow2_len + (tablerow2_len * c), item, center_merge)

    ws.merge_range(4, 16, 5, 16, 'Notes', center_merge)

    for i in range(len(header_sum_tablerow)):
        for c, item in enumerate(header_sum_tablerow2):
            ws.write(5, c + 1 + (tablerow2_len * i), item, center_bold)

    for i, run_name in enumerate(input_config.tradfilter):
        row = 6 + i
        if not run_name:
            continue
        
        ws.write(row, 0, run_name, yellow)
        
        # === DV (kolom 1) ===
        ws.write_formula(row, 1, f'=SUM(C{row+1}:F{row+1})',border_number)

        # === RAFM (kolom 6) ===
        ws.write_formula(row, 6, f'=SUM(H{row+1}:K{row+1})', border_number)

        # === Differences (kolom 11–15) ===
        ws.write_formula(row, 11, f'=B{row+1}-G{row+1}', border_number)
        ws.write_formula(row, 12, f'=C{row+1}-H{row+1}', border_number)
        ws.write_formula(row, 13, f'=D{row+1}-I{row+1}', border_number)
        ws.write_formula(row, 14, f'=E{row+1}-J{row+1}', border_number)
        ws.write_formula(row, 15, f'=F{row+1}-K{row+1}', border_number)

        # === DV Detail (kolom 2–5) ===
        ws.write_formula(row, 2, f"='{run_name}'!C5 + '{run_name}'!K4", border_number)
        ws.write_formula(row, 3, f"='{run_name}'!S4", border_number)
        ws.write_formula(row, 4, f"='{run_name}'!AA4", border_number)
        ws.write_formula(row, 5, f"='{run_name}'!AI4", border_number)

        # === RAFM Detail (kolom 7–10) ===
        ws.write_formula(row, 7, f"='{run_name}'!E5 + '{run_name}'!M4", border_number)
        ws.write_formula(row, 8, f"='{run_name}'!U4", border_number)
        ws.write_formula(row, 9, f"='{run_name}'!AC4", border_number)
        ws.write_formula(row, 10, f"='{run_name}'!AK4", border_number)

    for c, item in enumerate(header_sum_tablerow):
        ws.merge_range(8 + total_run, 1 + (tablerow2_len * c), 8 + total_run, tablerow2_len + (tablerow2_len * c), item, center_merge)

    ws.merge_range(8 + total_run, 16, 9 + total_run, 16, 'Notes', center_merge)

    for i in range(len(header_sum_tablerow)):
        for c, item in enumerate(header_sum_tablerow2):
            ws.write(9 + total_run, c + 1 + (tablerow2_len * i), item, center_bold)

    for i, run_name in enumerate(input_config.tradfilter):
        row = 10 + total_run + i
        if not run_name:
            continue
        
        ws.write(row, 0, run_name, yellow)
        
        # === DV (kolom 1) ===
        ws.write_formula(row, 1, f'=SUM(C{row+1}:F{row+1})',border_number)

        # === RAFM (kolom 6) ===
        ws.write_formula(row, 6, f'=SUM(H{row+1}:K{row+1})', border_number)

        # === Differences (kolom 11–15) ===
        ws.write_formula(row, 11, f'=B{row+1}-G{row+1}', border_number)
        ws.write_formula(row, 12, f'=C{row+1}-H{row+1}', border_number)
        ws.write_formula(row, 13, f'=D{row+1}-I{row+1}', border_number)
        ws.write_formula(row, 14, f'=E{row+1}-J{row+1}', border_number)
        ws.write_formula(row, 15, f'=F{row+1}-K{row+1}', border_number)

        # === DV Detail (kolom 2–5) ===
        ws.write_formula(row, 2, f"='{run_name}'!D5 + '{run_name}'!L4", border_number)
        ws.write_formula(row, 3, f"='{run_name}'!T4", border_number)
        ws.write_formula(row, 4, f"='{run_name}'!AB4", border_number)
        ws.write_formula(row, 5, f"='{run_name}'!AJ4", border_number)

        # === RAFM Detail (kolom 7–10) ===
        ws.write_formula(row, 7, f"='{run_name}'!F5 + '{run_name}'!N4", border_number)
        ws.write_formula(row, 8, f"='{run_name}'!V4", border_number)
        ws.write_formula(row, 9, f"='{run_name}'!AD4", border_number)
        ws.write_formula(row, 10, f"='{run_name}'!AL4", border_number)

    wb.add_worksheet('Diff Breakdown')
    wb.add_worksheet('>>')

    header_diff_tablerow = ['GOC', 'DV # of Policies', 'DV SA', 'RAFM # of Policies', 'RAFM SA', 'Diff # of Policies', 'Diff SA']
    tablecol_fmt = wb.add_format({'bold': True, 'underline': True, 'bg_color':'#92D050'})
    fmt_number = wb.add_format({'num_format': number_format})

    summary_number_fmt = wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True})
    
    data_bold_fmt = wb.add_format({'bold': True})
    data_number_fmt = wb.add_format({'num_format': number_format})

    for run_name in input_config.tradfilter:
        if run_name not in trad_results:
            continue
        ws = wb.add_worksheet(f'{run_name}')
        tr = trad_results[run_name]

        standard = convert_trad_result_to_standard(tr)
        df_list = standard['tables']
        sum_list = standard['summaries']
        col_starts = [1, 9, 17, 25, 33]  

        for col in range(2, 8):
            col_letter = xl_col_to_name(col)
            col_plus_8 = xl_col_to_name(col + 8)
            col_plus_16 = xl_col_to_name(col + 16)
            col_plus_24 = xl_col_to_name(col + 24)
            col_plus_32 = xl_col_to_name(col + 32)
            
            formula = f"={col_letter}6-{col_plus_8}4-{col_plus_16}4-{col_plus_24}4-{col_plus_32}4"
            ws.write_formula(1, col, formula, fmt_number)

        for idx, (df, summary) in enumerate(zip(df_list, sum_list)):
            ws.set_column(col_starts[idx], col_starts[idx] + 6, 20)
            ws.set_column(col_starts[idx], col_starts[idx], 40)

            for c, item in enumerate(header_diff_tablerow):
                ws.write(2, col_starts[idx] + c, item, wb.add_format({'bold': True, 'underline': True}))

            for r, item in enumerate(['Total All from DV', 'Grand Total Summary', 'Check']):
                ws.write(3 + r, col_starts[idx], item, tablecol_fmt)

            if idx == 1:
                ws.write(3, col_starts[idx], 'Total BTPN', tablecol_fmt)
            elif idx == 2:
                ws.write(3, col_starts[idx], 'Total Health non-YRT', tablecol_fmt)
            elif idx == 3:
                ws.write(3, col_starts[idx], 'Total Health YRT', tablecol_fmt)
            elif idx == 4:
                ws.write(3, col_starts[idx], 'Total C', tablecol_fmt)

            if summary is not None and not summary.empty:
                for row in range(len(summary)):
                    for c, item in enumerate(summary.iloc[row]):
                        value = item if not (pd.isna(item) or item == '') else 0
                        ws.write(
                            3 + row,
                            col_starts[idx] + 1 + c,
                            value,
                            summary_number_fmt
                        )

            if df is not None and not df.empty:
                for row in range(len(df)):
                    goc_value = df.iloc[row, 0] if not pd.isna(df.iloc[row, 0]) else ''
                    ws.write(6 + row, col_starts[idx], goc_value, data_bold_fmt)

                    for c in range(1, len(df.columns)):
                        item = df.iloc[row, c]
                        value = item if not (pd.isna(item) or item == '') else 0
                        ws.write(6 + row, col_starts[idx] + c, value, data_number_fmt)

                data_start_row = 6
                data_end_row = 6 + len(df) - 1

                if idx == 0:
                    for col_offset in range(1, 7):
                        col_idx = col_starts[idx] + col_offset
                        col_letter = xl_col_to_name(col_idx)

                        sum_formula = f'=SUM({col_letter}{data_start_row + 1}:{col_letter}{data_end_row + 1})'
                        ws.write_formula(4, col_idx, sum_formula, summary_number_fmt)

                        diff_formula = f'={col_letter}4 - {col_letter}5'
                        ws.write_formula(5, col_idx, diff_formula, summary_number_fmt)

                else:
                    for col_offset in range(1, 7):
                        col_idx = col_starts[idx] + col_offset
                        col_letter = xl_col_to_name(col_idx)

                        sum_formula = f'=SUM({col_letter}{data_start_row + 1}:{col_letter}{data_end_row + 1})'
                        ws.write_formula(3, col_idx, sum_formula, summary_number_fmt)
    wb.close()


def convert_ul_result_to_standard(result):
    return {
        'tables': [
            result.get('tabel_total', pd.DataFrame()),
            result.get('tabel_2', pd.DataFrame()),
            result.get('tabel_3', pd.DataFrame())
        ],
        'summaries': [
            result.get('summary_total', pd.DataFrame()),
            result.get('summary_tabel_2', pd.DataFrame()),
            result.get('summary_tabel_3', pd.DataFrame())
        ]
    }

def write_ul_results_to_excel(ul_results, input_config: InputSheetConfig):
    wb = xlsxwriter.Workbook(input_config.output_ul, {'nan_inf_to_errors': True})
    number_format = '_(* #,##0_);_(* (#,##0)_);_(* "-"_);_(@_)'
    total_run = len(input_config.ulfilter)
    header_sum_tablerow = ['DV', 'RAFM', 'Differences']
    header_sum_tablerow2 = ['Total', 'UL & SH & PI', 'GS']

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
    border_number = wb.add_format({'num_format': number_format, 'border': 1})
    fmt_number = wb.add_format({'num_format': number_format})
    ws.write(0, 0, 'Valuation Year', bold)
    ws.write(1, 0, 'Valuation Month', bold)
    ws.write(2, 0, 'FX Rate ValDate', bold)
    ws.write(4, 0, '# of Policies Check', green_underline)
    ws.write(5, 0, '# Run', green_underline)
    ws.write(8 + total_run, 0, 'Fund Check', green_underline)
    ws.write(9 + total_run, 0, '# Run', green_underline)

    ws.write(0, 1, input_config.valuation_year, yellow)
    ws.write(1, 1, input_config.valuation_month, yellow)
    ws.write(2, 1, input_config.valuation_rate, yellow)

    for c, item in enumerate(header_sum_tablerow):
        ws.merge_range(4, 1 + (3 * c), 4, 3 + (3 * c), item, center_merge)
    ws.merge_range(4, 13, 5, 13, 'Notes', center_merge)

    for i in range(len(header_sum_tablerow)):
        for c, item in enumerate(header_sum_tablerow2):
            ws.write(5, c + 1 + (3 * i), item, center_bold)

    for i, run_name in enumerate(input_config.ulfilter):
        row = 6 + i
        if not run_name:
            continue

        ws.write(row, 0, run_name, yellow)

        ws.write_formula(row, 1, f'=SUM(C{row+1}:D{row+1})', border_number)
        ws.write_formula(row, 4, f'=SUM(F{row+1}:G{row+1})', border_number)
        ws.write_formula(row, 7, f'=B{row+1}-E{row+1}', border_number)
        ws.write_formula(row, 8, f'=C{row+1}-F{row+1}', border_number)
        ws.write_formula(row, 9, f'=D{row+1}-G{row+1}', border_number)
        ws.write_formula(row, 2, f"='{run_name}'!C5", border_number)
        ws.write_formula(row, 3, f"='{run_name}'!K4", border_number)
        ws.write_formula(row, 5, f"='{run_name}'!E5", border_number)
        ws.write_formula(row, 6, f"='{run_name}'!M4", border_number)

    for c, item in enumerate(header_sum_tablerow):
        ws.merge_range(8 + total_run, 1 + (3 * c), 8 + total_run, 3 + (3 * c), item, center_merge)
    ws.merge_range(8 + total_run, 13, 9 + total_run, 13, 'Notes', center_merge)

    for i in range(len(header_sum_tablerow)):
        for c, item in enumerate(header_sum_tablerow2):
            ws.write(9 + total_run, c + 1 + (3 * i), item, center_bold)

    for i, run_name in enumerate(input_config.ulfilter):
        row = 10 + total_run + i
        if not run_name:
            continue

        ws.write(row, 0, run_name, yellow)

        ws.write_formula(row, 1, f'=SUM(C{row+1}:D{row+1})', border_number)
        ws.write_formula(row, 4, f'=SUM(F{row+1}:G{row+1})', border_number)
        ws.write_formula(row, 7, f'=B{row+1}-E{row+1}', border_number)
        ws.write_formula(row, 8, f'=C{row+1}-F{row+1}', border_number)
        ws.write_formula(row, 9, f'=D{row+1}-G{row+1}', border_number)
        ws.write_formula(row, 2, f"='{run_name}'!D5", border_number)
        ws.write_formula(row, 3, f"='{run_name}'!L4", border_number)
        ws.write_formula(row, 5, f"='{run_name}'!F5", border_number)
        ws.write_formula(row, 6, f"='{run_name}'!N4", border_number)

    wb.add_worksheet('Diff Breakdown')
    wb.add_worksheet('>>')

    header_diff_tablerow = ['GOC', 'DV # of Policies', 'DV Fund Value', 'RAFM # of Policies', 'RAFM Fund Value', 'Diff # of Policies', 'Diff Fund Value']
    tablecol_fmt = wb.add_format({'bold': True, 'underline': True, 'bg_color': '#92D050'})
    summary_number_fmt = wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True})
    data_bold_fmt = wb.add_format({'bold': True})
    data_number_fmt = wb.add_format({'num_format': number_format})

    for run_name in input_config.ulfilter:
        if run_name not in ul_results:
            continue

        try:
            ws = wb.add_worksheet(f'{run_name}')
            ul = ul_results[run_name]
            standard = convert_ul_result_to_standard(ul)

            df_list = standard.get('tables', [])
            sum_list = standard.get('summaries', [])

            if not isinstance(df_list, list) or not isinstance(sum_list, list):
                print(f"[{run_name}] ❌ tables/summaries not list")
                continue

            if len(df_list) != len(sum_list):
                print(f"[{run_name}] ⚠️ len df_list ({len(df_list)}) ≠ sum_list ({len(sum_list)})")

            max_len = len(df_list)
            col_starts = [1 + 8 * i for i in range(max_len)]

            for col in range(2, 8):
                col_letter = xl_col_to_name(col)
                col_plus_8 = xl_col_to_name(col + 8)
                
                formula = f"={col_letter}6-{col_plus_8}4"
                ws.write_formula(1, col, formula, fmt_number)


            for idx in range(max_len):
                df = df_list[idx] if idx < len(df_list) else None
                summary = sum_list[idx] if idx < len(sum_list) else None

                if (df is None or df.empty) and (summary is None or summary.empty):
                    continue
                ws.set_column(col_starts[idx], col_starts[idx] + 6, 20)
                ws.set_column(col_starts[idx], col_starts[idx], 40)

                for c, item in enumerate(header_diff_tablerow):
                    ws.write(2, col_starts[idx] + c, item, wb.add_format({'bold': True, 'underline': True}))

                row_titles = ['Total All from DV', 'Grand Total Summary', 'Check']
                if idx == 1:
                    row_titles[0] = 'Total Group Savings'

                for r, title in enumerate(row_titles):
                    ws.write(3 + r, col_starts[idx], title, tablecol_fmt)

                if summary is not None and not summary.empty:
                    for row in range(len(summary)):
                        for c, item in enumerate(summary.iloc[row]):
                            value = item if not (pd.isna(item) or item == '') else 0
                            ws.write(3 + row, col_starts[idx] + 1 + c, value, summary_number_fmt)

                if df is not None and not df.empty:
                    for row in range(len(df)):
                        goc_value = df.iloc[row, 0] if not pd.isna(df.iloc[row, 0]) else ''
                        ws.write(6 + row, col_starts[idx], goc_value, data_bold_fmt)

                        for c in range(1, len(df.columns)):
                            try:
                                item = df.iloc[row, c]
                                value = item if not (pd.isna(item) or item == '') else 0
                                ws.write(6 + row, col_starts[idx] + c, value, data_number_fmt)
                            except IndexError:
                                print(f"[{run_name}] ❌ IndexError di df[{row},{c}]")
                                continue

                    data_start_row = 6
                    data_end_row = 6 + len(df) - 1

                    if idx == 0:
                        for col_offset in range(1, 7):  
                            col_idx = col_starts[idx] + col_offset
                            col_letter = xl_col_to_name(col_idx)

                            sum_formula = f'=SUM({col_letter}{data_start_row + 1}:{col_letter}{data_end_row + 1})'
                            diff_formula = f'={col_letter}4 - {col_letter}5'

                            ws.write_formula(4, col_idx, sum_formula, summary_number_fmt)  
                            ws.write_formula(5, col_idx, diff_formula, summary_number_fmt) 

                    elif idx == 1: 
                        for col_offset in range(1, 7):  
                            col_idx = col_starts[idx] + col_offset 
                            col_letter = xl_col_to_name(col_idx)

                            sum_formula = f'=SUM({col_letter}{data_start_row + 1}:{col_letter}{data_end_row + 1})'
                            ws.write_formula(3, col_idx, sum_formula, summary_number_fmt)  

        except Exception as e:
            print(f"[{run_name}] ❌ Error menulis worksheet: {e}")
            continue

    wb.close()

def main(input_sheet_path):
    start_time = time.time()

    print("="*60)
    print("CONTROL 3")
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

    input_config = get_valuation_info_and_filters(input_sheet_path)
    if input_config is None:
        print("❌ Failed to read input configuration")
        return False

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

    def write_trad_wrapper():
        output_folder = os.path.dirname(input_config.output_trad)
        os.makedirs(output_folder, exist_ok=True)
        print(f"\n📤 Output path trad: {input_config.output_trad}")
        write_trad_results_to_excel(trad_results, input_config)

    def write_ul_wrapper():
        output_folder = os.path.dirname(input_config.output_ul)
        os.makedirs(output_folder, exist_ok=True)
        print(f"\n📤 Output path ul: {input_config.output_ul}")
        write_ul_results_to_excel(ul_results, input_config)

    with ThreadPoolExecutor() as executor:
        futures = []
        if trad_results:
            futures.append(executor.submit(write_trad_wrapper))
        if ul_results:
            futures.append(executor.submit(write_ul_wrapper))

        for future in futures:
            try:
                future.result()
            except Exception as e:
                print(f"❌ Error saat menulis file: {e}")
                return False

    elapsed = time.time() - start_time
    formatted = str(datetime.timedelta(seconds=int(elapsed)))
    print(f"\n⏱️ Total runtime: {formatted}")

    return True