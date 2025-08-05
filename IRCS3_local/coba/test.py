import pandas as pd
import os
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
import xlsxwriter
from typing import Dict, Any
from pathlib import Path
from ul_trad import run_trad, run_ul

warnings.filterwarnings('ignore')

def safe_get_dict(d, key):
    val = d.get(key)
    return val if isinstance(val, dict) else {}

def read_input_config(input_sheet: str) -> Dict[str, Any]:
    df = pd.read_excel(input_sheet, sheet_name='INPUT_SETTING', engine='openpyxl')
    df.columns = ['Category', 'Path']
    config = dict(zip(df['Category'], df['Path']))

    for key in ['FX Rate Valdate', 'Valuation Month', 'Valuation Year']:
        try:
            if key in config:
                config[key] = int(config[key])
        except Exception:
            pass
    return config

def validate_and_prepare_paths(config: Dict[str, Any]) -> None:
    required_files = ['Output Path Trad', 'Output Path UL', 'Output Trad', 'Output UL']
    for key in required_files:
        if key not in config or not config[key]:
            raise ValueError(f"Konfigurasi '{key}' tidak ditemukan atau kosong.")

    trad_out_dir = Path(config['Output Path Trad'])
    ul_out_dir = Path(config['Output Path UL'])

    trad_out_dir.mkdir(parents=True, exist_ok=True)
    ul_out_dir.mkdir(parents=True, exist_ok=True)

    config['output_trad'] = str(trad_out_dir / f"{config['Output Trad']}.xlsx")
    config['output_ul'] = str(ul_out_dir / f"{config['Output UL']}.xlsx")



def write_trad_results_to_excel(trad_results: Dict[str, Any], input_config: Dict[str, Any]) -> None:
    wb = xlsxwriter.Workbook(input_config['output_trad'], {'nan_inf_to_errors': True})
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

    ws.write(0, 1, input_config.get('valuation_year', ''), yellow)
    ws.write(1, 1, input_config.get('valuation_month', ''), yellow)
    ws.write(2, 1, input_config.get('valuation_rate', ''), yellow)

    for i, run_name in enumerate(input_config.get('tradfilter', [])):
        ws.write(6 + i, 0, run_name, yellow)

    for c, item in enumerate(header_sum_tablerow):
        ws.merge_range(4, 1 + (tablerow2_len * c), 4, tablerow2_len + (tablerow2_len * c), item, center_merge)

    ws.merge_range(4, 16, 5, 16, 'Notes', center_merge)

    for i in range(len(header_sum_tablerow)):
        for c, item in enumerate(header_sum_tablerow2):
            ws.write(5, c + 1 + (tablerow2_len * i), item, center_bold)

    for i, run_name in enumerate(input_config.get('tradfilter', [])):
        if run_name in trad_results and 'summary_total' in trad_results[run_name]:
            ctrlsum = trad_results[run_name]['summary_total']
            for c, item_ in enumerate(ctrlsum.iloc[1]):  # baris index 1: Grand Total Summary
                ws.write(6 + i, c + 1, item_, wb.add_format({'num_format': number_format}))

    wb.add_worksheet('Diff Breakdown')
    wb.add_worksheet('>>')

    header_diff_tablerow = ['GOC', 'DV # of Policies', 'DV SA', 'RAFM # of Policies', 'RAFM SA', 'Diff # of Policies', 'Diff SA']
    tablecol_fmt = wb.add_format({'bold': True, 'underline': True, 'bg_color':'#92D050'})

    for run_name in input_config.get('tradfilter', []):
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


def write_ul_results_to_excel(ul_results: Dict[str, Any], input_config: Dict[str, Any]) -> None:
    wb = xlsxwriter.Workbook(input_config['output_ul'], {'nan_inf_to_errors': True})
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

    ws.write(0, 1, input_config.get('valuation_year', ''), yellow)
    ws.write(1, 1, input_config.get('valuation_month', ''), yellow)
    ws.write(2, 1, input_config.get('valuation_rate', ''), yellow)

    for i, run_name in enumerate(input_config.get('ulfilter', [])):
        ws.write(6 + i, 0, run_name, yellow)

    for c, item in enumerate(header_sum_tablerow):
        ws.merge_range(4, 1 + (4 * c), 4, 4 + (4 * c), item, center_merge)

    ws.merge_range(4, 13, 5, 13, 'Notes', center_merge)

    for i in range(len(header_sum_tablerow)):
        for c, item in enumerate(header_sum_tablerow2):
            ws.write(5, c + 1 + (4 * i), item, center_bold)

    for i, run_name in enumerate(input_config.get('ulfilter', [])):
        if run_name in ul_results and 'summary_total' in ul_results[run_name]:
            ctrlsum = ul_results[run_name]['summary_total']
            for c, item_ in enumerate(ctrlsum.iloc[1]):
                ws.write(6 + i, c + 1, item_, wb.add_format({'num_format': number_format}))

    wb.add_worksheet('Diff Breakdown')
    wb.add_worksheet('>>')

    header_diff_tablerow = ['GOC', 'DV # of Policies', 'DV Fund Value', 'RAFM # of Policies', 'RAFM Fund Value', 'Diff # of Policies', 'Diff Fund Value']
    tablecol_fmt = wb.add_format({'bold': True, 'underline': True, 'bg_color': '#92D050'})

    for run_name in input_config.get('ulfilter', []):
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


def run_all(input_sheet: str) -> None:
    print(f"Memulai proses dengan input sheet: {input_sheet}")
    config = read_input_config(input_sheet)
    validate_and_prepare_paths(config)

    filter_trad = pd.read_excel(input_sheet, sheet_name='FILTER_TRAD', engine='openpyxl')
    filter_ul = pd.read_excel(input_sheet, sheet_name='FILTER_UL', engine='openpyxl')

    tradfilter = filter_trad['run_name'].astype(str).str.strip().str.lower().tolist()
    ulfilter = filter_ul['run_name'].astype(str).str.strip().str.lower().tolist()

    trad_results: Dict[str, Any] = {}
    ul_results: Dict[str, Any] = {}

    with ThreadPoolExecutor() as executor:
        trad_futures = {
            executor.submit(run_trad, {**row.to_dict(), **config, 'run_name': str(row.get('run_name', f'run_trad_{i}')).strip().lower()}):
            str(row.get('run_name', f'run_trad_{i}')).strip().lower()
            for i, row in filter_trad.iterrows()
        }

        ul_futures = {
            executor.submit(run_ul, {**row.to_dict(), **config, 'run_name': str(row.get('run_name', f'run_ul_{i}')).strip().lower()}):
            str(row.get('run_name', f'run_ul_{i}')).strip().lower()
            for i, row in filter_ul.iterrows()
        }

        for future in as_completed(trad_futures):
            run_name = trad_futures[future]
            try:
                trad_results[run_name] = future.result()
                print(f"✅ run_trad selesai: {run_name}")
            except Exception as e:
                print(f"❌ Error run_trad: {run_name} - {e}")

        for future in as_completed(ul_futures):
            run_name = ul_futures[future]
            try:
                ul_results[run_name] = future.result()
                print(f"✅ run_ul selesai: {run_name}")
            except Exception as e:
                print(f"❌ Error run_ul: {run_name} - {e}")

    config['tradfilter'] = tradfilter
    config['ulfilter'] = ulfilter
    config['valuation_year'] = config.get('Valuation Year')
    config['valuation_month'] = config.get('Valuation Month')
    config['valuation_rate'] = config.get('FX Rate Valdate')

    print("Menulis hasil TRAD ke Excel...")
    write_trad_results_to_excel(trad_results, config)
    print("Menulis hasil UL ke Excel...")
    write_ul_results_to_excel(ul_results, config)

    print("\n✅ Semua proses selesai dan file sudah ditulis ke:")
    print(f"- TRAD: {config['output_trad']}")
    print(f"- UL:   {config['output_ul']}")

def main(input_sheet_path: str) -> None:
    try:
        run_all(input_sheet_path)
    except Exception as e:
        print(f"❌ Terjadi kesalahan saat proses utama: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Usage: python this_script.py path_to_input_sheet.xlsx")
