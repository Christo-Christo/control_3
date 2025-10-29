import os
import pandas as pd
from xlsxwriter.utility import xl_col_to_name
import syntax.control_4_trad as trad
import syntax.control_4_ul as ul
import syntax.control_4_reas as reas
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
from functools import lru_cache

cols_to_sum_dict = {
    'trad': trad.cols_to_compare,
    'ul': ul.columns_to_sum_argo,
    'reas': reas.cols_to_compare
}

def auto_adjust_column_width(worksheet, df_sheet, max_width=50, sample_size=100):
    if not hasattr(df_sheet, 'columns'):
        return
    
    for i, col in enumerate(df_sheet.columns):
        try:
            sample_data = df_sheet[col].head(sample_size).astype(str)
            
            max_len = max(
                sample_data.str.len().max(),
                len(str(col))
            )

            adjusted_width = min(max_len + 2, max_width)
            worksheet.set_column(i, i, adjusted_width)
        except Exception:
            worksheet.set_column(i, i, 12)


def apply_number_formats(workbook, worksheet, df_sheet, sheet_name):
    if sheet_name == 'Control':
        return
    
    format_accounting = workbook.add_format({
        'num_format': '_-* #,##0_-;_-* (#,##0);_-* "-"_-;_-@_-'
    })
    format_int = workbook.add_format({'num_format': '0'})
    format_no_format = workbook.add_format()
    
    if not hasattr(df_sheet, 'columns'):
        return
    
    # Batch apply formats per column type
    for col_idx, col_name in enumerate(df_sheet.columns):
        col_name_lower = str(col_name).lower()
        
        if 'speed duration' in col_name_lower:
            worksheet.set_column(col_idx, col_idx, None, format_no_format)
        elif 'include year' in col_name_lower or 'exclude year' in col_name_lower:
            worksheet.set_column(col_idx, col_idx, None, format_int)
        else:
            worksheet.set_column(col_idx, col_idx, None, format_accounting)


def write_checking_summary_formulas(worksheet, df_sheet, result, jenis, nrows, ncols):
    """
    Menulis formula di sheet Checking Summary sesuai jenis file:
      - trad : mulai dari kolom E2
      - ul   : mulai dari kolom D2
      - reas : mulai dari kolom D2
    Formula mengikuti semua baris dan kolom yang ada,
    menggunakan nama sheet sesuai logika lama.
    """

    # Nama sheet sesuai logika lama
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

    for row_idx in range(1, nrows):  # mulai dari baris ke-2 (Excel row 2)
        row_excel = row_idx + 1

        # Kolom mulai berbeda untuk trad / ul / reas
        if jenis == 'trad':
            start_col_idx = 4  # E (0-based index)
        else:
            start_col_idx = 3  # D (0-based index)

        for col_idx in range(start_col_idx, ncols):
            col_letter = xl_col_to_name(col_idx)

            if jenis == 'trad':
                formula = (
                    f"='{sheet_names['trad']['cf_argo']}'!C{row_excel}"
                    f"-'{sheet_names['trad']['cf_rafm']}'!G{row_excel}"
                    f"+'{sheet_names['trad']['rafm_manual']}'!C{row_excel}"
                    f"-'{sheet_names['trad']['uvsg']}'!G{row_excel}"
                )

            elif jenis == 'ul':
                formula = (
                    f"='{sheet_names['ul']['cf_argo']}'!C{row_excel}"
                    f"-'{sheet_names['ul']['cf_rafm']}'!F{row_excel}"
                    f"-'{sheet_names['ul']['rafm_manual']}'!C{row_excel}"
                )

            elif jenis == 'reas':
                formula = (
                    f"='{sheet_names['reas']['cf_argo']}'!C{row_excel}"
                    f"-'{sheet_names['reas']['cf_rafm']}'!C{row_excel}"
                    f"+'{sheet_names['reas']['rafm_manual']}'!C{row_excel}"
                )

            worksheet.write_formula(row_idx, col_idx, formula)


def process_input_file(file_path):
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

    print(f"\n📄 Memproses: {filename} (jenis: {jenis})")

    try:
        df = pd.read_excel(file_path, sheet_name='File Path')
    except Exception as e:
        print(f"⚠️ Tidak bisa membaca sheet 'File Path' dari {file_path}: {e}")
        return

    df.columns = df.columns.str.strip()
    df['Name'] = df['Name'].astype(str).str.strip().str.lower()
    df['File Path'] = df['File Path'].astype(str).str.strip()

    if 'output_path' not in df['Name'].values or 'output_filename' not in df['Name'].values:
        print(f"⚠️ output_path atau output_filename tidak ditemukan di sheet 'File Path' pada {file_path}")
        return

    output_path = df.loc[df['Name'] == 'output_path', 'File Path'].values[0]
    output_filename = df.loc[df['Name'] == 'output_filename', 'File Path'].values[0]

    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, output_filename)

    with pd.ExcelWriter(output_file, engine='xlsxwriter', 
                        engine_kwargs={'options': {'strings_to_numbers': False}}) as writer:
        workbook = writer.book
        
        for sheet_name, df_sheet in result.items():
            if sheet_name == 'Control':
                df_sheet.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
            else:
                df_sheet.to_excel(writer, sheet_name=sheet_name, index=False, header=True)

            worksheet = writer.sheets[sheet_name]

            auto_adjust_column_width(worksheet, df_sheet)
            
            apply_number_formats(workbook, worksheet, df_sheet, sheet_name)

            if sheet_name != 'Control':
                border_format = workbook.add_format({'border': 1, 'border_color': 'black'})
                nrows, ncols = df_sheet.shape
                worksheet.conditional_format(
                    0, 0, nrows, ncols - 1,
                    {'type': 'no_errors', 'format': border_format}
                )
            if sheet_name.lower().startswith("checking summary"):
                nrows, ncols = df_sheet.shape
                nomor_kolom = df_sheet.iloc[:, 0].dropna()
                
                if not nomor_kolom.empty:
                    nrows = int(nomor_kolom.max()) + 1
                else:
                    nrows = df_sheet.shape[0]
                
                write_checking_summary_formulas(worksheet, df_sheet, result, jenis, nrows, ncols)

    print(f"✅ Output disimpan di: {output_file}")


def main(input_path):
    start_time = time.time()

    if os.path.isfile(input_path):
        files = [input_path]
    elif os.path.isdir(input_path):
        files = [
            os.path.join(input_path, fname)
            for fname in os.listdir(input_path)
            if fname.endswith(".xlsx") and not fname.startswith("~$")
        ]
    else:
        print(f"❌ Path tidak ditemukan atau tidak valid: {input_path}")
        return

    if not files:
        print("📂 Tidak ada file .xlsx yang ditemukan.")
        return

    print(f"🔧 Memproses {len(files)} file...\n")

    if len(files) == 1:
        process_input_file(files[0])
    else:
        optimal_workers = min(os.cpu_count() or 4, len(files))
        
        with ProcessPoolExecutor(max_workers=optimal_workers) as executor:
            futures = [executor.submit(process_input_file, f) for f in files]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ Error saat memproses file: {e}")

    end_time = time.time()
    print(f"\n⏲️ Total waktu proses: {end_time - start_time:.2f} detik")