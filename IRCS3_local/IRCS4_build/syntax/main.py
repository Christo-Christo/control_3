import os
import pandas as pd
from xlsxwriter.utility import xl_col_to_name
import syntax.control_4_trad as trad
import syntax.control_4_ul as ul
import syntax.control_4_reas as reas
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import xlwings as xw
from openpyxl.utils import get_column_letter
import warnings
import shutil
import datetime
import psutil

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

cols_to_sum_dict = {
    'trad': trad.cols_to_compare,
    'ul': ul.columns_to_sum_argo,
    'reas': reas.cols_to_compare
}

# ============================
# 🔧 Tambahan: fungsi styling
# ============================

def auto_adjust_column_width(ws, df, max_width=50, sample_size=100):
    """Menyesuaikan lebar kolom otomatis berdasarkan data"""
    if not hasattr(df, 'columns'):
        return
    for i, col in enumerate(df.columns):
        try:
            sample_data = df[col].head(sample_size).astype(str)
            max_len = max(sample_data.str.len().max(), len(str(col)))
            adjusted_width = min(max_len + 2, max_width)
            ws.range((1, i + 1)).column_width = adjusted_width
        except Exception:
            ws.range((1, i + 1)).column_width = 12


def apply_number_formats(ws, df):
    """Terapkan format angka seperti di notes.py"""
    if not hasattr(df, 'columns'):
        return

    nrows = len(df) + 1
    for col_idx, col_name in enumerate(df.columns, 1):
        col_letter = get_column_letter(col_idx)
        col_name_lower = str(col_name).lower()

        if 'speed duration' in col_name_lower:
            number_format = '@'
        elif 'include year' in col_name_lower or 'exclude year' in col_name_lower:
            number_format = '0'
        else:
            number_format = '_-* #,##0_-;_-* (#,##0);_-* "-"_-;_-@_-'

        rng = ws.range(f"{col_letter}2:{col_letter}{nrows}")
        rng.number_format = number_format


def apply_border(ws, df):
    """Tambahkan border hitam di seluruh tabel"""
    nrows = len(df) + 1
    ncols = len(df.columns)
    if nrows < 1 or ncols < 1:
        return
    full_range = ws.range(f"A1:{get_column_letter(ncols)}{nrows}")
    full_range.api.Borders.LineStyle = 1  # xlContinuous
    full_range.api.Borders.Weight = 2     # xlThin


def apply_accounting_to_all(ws, df):
    """Format seluruh angka menjadi accounting (khusus Checking Summary)"""
    nrows = len(df) + 1
    ncols = len(df.columns)
    for col_idx in range(1, ncols + 1):
        col_letter = get_column_letter(col_idx)
        data_range = ws.range(f'{col_letter}2:{col_letter}{nrows}')
        data_range.number_format = '_-* #,##0_-;_-* (#,##0);_-* "-"_-;_-@_-'


# ======================================
# ⚙️ Bagian fungsi utama dari main.py
# ======================================

def kill_excel_processes():
    """Force close all Excel processes"""
    try:
        for proc in psutil.process_iter(['pid', 'name']):
            if proc.info['name'] and 'excel' in proc.info['name'].lower():
                print(f"    • Killing Excel PID {proc.info['pid']}")
                proc.kill()
        time.sleep(2)
    except Exception as e:
        print(f"    ⚠️ Error killing Excel: {e}")


def write_checking_summary_formulas_xlwings(ws, df_sheet, jenis, start_row=2):
    """Menulis formula Checking Summary (logika asli tetap)"""
    sheet_names = {
        'trad': {
            'cf_argo': 'CF ARGO AZTRAD',
            'cf_rafm': 'RAFM Output AZTRAD',
            'rafm_manual': 'RAFM Output Manual',
            'uvsg': 'RAFM Output AZUL_PI'
        },
        'ul': {
            'cf_argo': 'CF ARGO AZUL',
            'cf_rafm': 'RAFM Output AZUL',
            'rafm_manual': 'RAFM Output Manual'
        },
        'reas': {
            'cf_argo': 'CF ARGO REAS',
            'cf_rafm': 'RAFM Output REAS',
            'rafm_manual': 'RAFM Output Manual'
        }
    }

    nrows = len(df_sheet)
    ncols = len(df_sheet.columns)

    if jenis == 'trad':
        start_col_idx = 5
        cf_argo_col_offset = 3
        cf_rafm_col_offset = 7
        rafm_manual_col_offset = 7
        uvsg_col_offset = 7
    elif jenis == 'ul':
        start_col_idx = 4
        cf_argo_col_offset = 3
        cf_rafm_col_offset = 6
        rafm_manual_col_offset = 6
    else:
        start_col_idx = 4
        cf_argo_col_offset = 3
        cf_rafm_col_offset = 3
        rafm_manual_col_offset = 3

    for row_idx in range(nrows):
        row_excel = start_row + row_idx
        for col_idx in range(start_col_idx, ncols + 1):
            rel_offset = col_idx - start_col_idx
            if jenis == 'trad':
                cf_argo_col = get_column_letter(cf_argo_col_offset + rel_offset)
                cf_rafm_col = get_column_letter(cf_rafm_col_offset + rel_offset)
                rafm_manual_col = get_column_letter(rafm_manual_col_offset + rel_offset)
                uvsg_col = get_column_letter(uvsg_col_offset + rel_offset)
                formula = (
                    f"='{sheet_names['trad']['cf_argo']}'!{cf_argo_col}{row_excel}"
                    f"-'{sheet_names['trad']['cf_rafm']}'!{cf_rafm_col}{row_excel}"
                    f"+'{sheet_names['trad']['rafm_manual']}'!{rafm_manual_col}{row_excel}"
                    f"-'{sheet_names['trad']['uvsg']}'!{uvsg_col}{row_excel}"
                )
            elif jenis == 'ul':
                cf_argo_col = get_column_letter(cf_argo_col_offset + rel_offset)
                cf_rafm_col = get_column_letter(cf_rafm_col_offset + rel_offset)
                rafm_manual_col = get_column_letter(rafm_manual_col_offset + rel_offset)
                formula = (
                    f"='{sheet_names['ul']['cf_argo']}'!{cf_argo_col}{row_excel}"
                    f"-'{sheet_names['ul']['cf_rafm']}'!{cf_rafm_col}{row_excel}"
                    f"-'{sheet_names['ul']['rafm_manual']}'!{rafm_manual_col}{row_excel}"
                )
            else:
                cf_argo_col = get_column_letter(cf_argo_col_offset + rel_offset)
                cf_rafm_col = get_column_letter(cf_rafm_col_offset + rel_offset)
                rafm_manual_col = get_column_letter(rafm_manual_col_offset + rel_offset)
                formula = (
                    f"='{sheet_names['reas']['cf_argo']}'!{cf_argo_col}{row_excel}"
                    f"-'{sheet_names['reas']['cf_rafm']}'!{cf_rafm_col}{row_excel}"
                    f"+'{sheet_names['reas']['rafm_manual']}'!{rafm_manual_col}{row_excel}"
                )
            ws.range(f"{get_column_letter(col_idx)}{row_excel}").formula = formula


def add_sheets_to_rafm_manual(rafm_manual_path, result_dict, output_path, output_filename, jenis):
    """Modifikasi dengan styling dari notes.py tapi tetap pakai xlwings"""
    app = None
    wb = None
    try:
        os.makedirs(output_path, exist_ok=True)
        output_file = os.path.join(output_path, output_filename)
        if os.path.exists(output_file):
            os.remove(output_file)

        shutil.copy2(rafm_manual_path, output_file)
        time.sleep(0.5)

        app = xw.App(visible=False)
        app.display_alerts = False
        app.screen_updating = False
        wb = app.books.open(output_file)

        for sheet_name, df in result_dict.items():
            if sheet_name == 'RAFM Output Manual':
                continue
            if sheet_name in [s.name for s in wb.sheets]:
                wb.sheets[sheet_name].delete()

            ws = wb.sheets.add(sheet_name, after=wb.sheets[-1])

            if sheet_name == 'Control':
                ws.range('A1').value = df.values.tolist()
            else:
                ws.range('A1').value = [df.columns.tolist()] + df.values.tolist()

            # === Tambahkan styling seperti notes.py ===
            apply_number_formats(ws, df)
            apply_border(ws, df)
            auto_adjust_column_width(ws, df)

            # === Checking Summary khusus ===
            if sheet_name.lower().startswith("checking summary"):
                print(f"  • Writing formulas for {sheet_name}")
                write_checking_summary_formulas_xlwings(ws, df, jenis)
                apply_accounting_to_all(ws, df)

        wb.save()
        wb.close()
        app.quit()
        print(f"✅ Output disimpan di: {output_file}")
        return output_file
    except Exception as e:
        print(f"❌ Error add_sheets_to_rafm_manual: {e}")
    finally:
        if wb is not None:
            try:
                wb.close()
            except:
                pass
        if app is not None:
            try:
                app.quit()
            except:
                pass


def process_input_file(file_path):
    """Process satu file input"""
    filename = os.path.basename(file_path).lower()
    if 'trad' in filename:
        jenis = 'trad'
        result = trad.main({"input excel": file_path})
    elif 'ul' in filename:
        jenis = 'ul'
        result = ul.main({"input excel": file_path})
    elif 'reas' in filename:
        jenis = 'reas'
        result = reas.main({"input excel": file_path})
    else:
        print(f"❌ Jenis file tidak dikenali: {filename}")
        return

    df = pd.read_excel(file_path, sheet_name='File Path')
    df.columns = df.columns.str.strip()
    df['Name'] = df['Name'].astype(str).str.strip().str.lower()
    df['File Path'] = df['File Path'].astype(str).str.strip()

    output_path = df.loc[df['Name']=='output_path', 'File Path'].values[0]
    output_filename = df.loc[df['Name']=='output_filename', 'File Path'].values[0]
    rafm_manual_path = df.loc[df['Name']=='rafm manual', 'File Path'].values[0]

    add_sheets_to_rafm_manual(rafm_manual_path, result, output_path, output_filename, jenis)


def main(input_path):
    """Main sequential (xlwings mode)"""
    if os.path.isfile(input_path):
        files = [input_path]
    elif os.path.isdir(input_path):
        files = [
            os.path.join(input_path, f)
            for f in os.listdir(input_path)
            if f.endswith(".xlsx") and not f.startswith("~$")
        ]
    else:
        print("❌ Path tidak valid")
        return

    for f in files:
        process_input_file(f)


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Usage: python main.py <input_file_or_folder>")
