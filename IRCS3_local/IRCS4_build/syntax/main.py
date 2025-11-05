import os
import pandas as pd
from xlsxwriter.utility import xl_col_to_name
import syntax.control_4_trad as trad
import syntax.control_4_ul as ul
import syntax.control_4_reas as reas
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
from functools import lru_cache
from openpyxl import load_workbook
import xlwings as xw
import shutil

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

    for row_idx in range(1, nrows):
        row_excel = row_idx + 1

        if jenis == 'trad':
            start_col_idx = 4
            cf_argo_col_offset = 2
            cf_rafm_col_offset = 6
            rafm_manual_col_offset = 6
            uvsg_col_offset = 6

        elif jenis == 'ul':
            start_col_idx = 3
            cf_argo_col_offset = 2
            cf_rafm_col_offset = 5
            rafm_manual_col_offset = 5

        else:  # reas
            start_col_idx = 3
            cf_argo_col_offset = 2
            cf_rafm_col_offset = 2
            rafm_manual_col_offset = 2

        for col_idx in range(start_col_idx, ncols):
            relative_offset = col_idx - start_col_idx

            if jenis == 'trad':
                cf_argo_col = xl_col_to_name(cf_argo_col_offset + relative_offset)
                cf_rafm_col = xl_col_to_name(cf_rafm_col_offset + relative_offset)
                rafm_manual_col = xl_col_to_name(rafm_manual_col_offset + relative_offset)
                uvsg_col = xl_col_to_name(uvsg_col_offset + relative_offset)

                formula = (
                    f"='{sheet_names['trad']['cf_argo']}'!{cf_argo_col}{row_excel}"
                    f"-'{sheet_names['trad']['cf_rafm']}'!{cf_rafm_col}{row_excel}"
                    f"+'{sheet_names['trad']['rafm_manual']}'!{rafm_manual_col}{row_excel}"
                    f"-'{sheet_names['trad']['uvsg']}'!{uvsg_col}{row_excel}"
                )

            elif jenis == 'ul':
                cf_argo_col = xl_col_to_name(cf_argo_col_offset + relative_offset)
                cf_rafm_col = xl_col_to_name(cf_rafm_col_offset + relative_offset)
                rafm_manual_col = xl_col_to_name(rafm_manual_col_offset + relative_offset)

                formula = (
                    f"='{sheet_names['ul']['cf_argo']}'!{cf_argo_col}{row_excel}"
                    f"-'{sheet_names['ul']['cf_rafm']}'!{cf_rafm_col}{row_excel}"
                    f"-'{sheet_names['ul']['rafm_manual']}'!{rafm_manual_col}{row_excel}"
                )

            elif jenis == 'reas':
                cf_argo_col = xl_col_to_name(cf_argo_col_offset + relative_offset)
                cf_rafm_col = xl_col_to_name(cf_rafm_col_offset + relative_offset)
                rafm_manual_col = xl_col_to_name(rafm_manual_col_offset + relative_offset)

                formula = (
                    f"='{sheet_names['reas']['cf_argo']}'!{cf_argo_col}{row_excel}"
                    f"-'{sheet_names['reas']['cf_rafm']}'!{cf_rafm_col}{row_excel}"
                    f"+'{sheet_names['reas']['rafm_manual']}'!{rafm_manual_col}{row_excel}"
                )

            worksheet.write_formula(row_idx, col_idx, formula)


def insert_rafm_manual_with_formulas(src_path, dest_path, sheet_name="RAFM Output Manual", insert_position=None):
    try:
        if not os.path.exists(src_path):
            print(f"⚠️ File RAFM manual tidak ditemukan: {src_path}")
            return False
            
        if not os.path.exists(dest_path):
            print(f"⚠️ File output tidak ditemukan: {dest_path}")
            return False

        print(f"🔄 Menyalin sheet '{sheet_name}' dari RAFM manual...")
        
        with xw.App(visible=False) as app:
            # Buka kedua file
            src_wb = xw.Book(src_path)
            dest_wb = xw.Book(dest_path)
            
            # Hapus sheet lama jika ada
            if sheet_name in [s.name for s in dest_wb.sheets]:
                print(f"  ↳ Menghapus sheet '{sheet_name}' yang lama...")
                dest_wb.sheets[sheet_name].delete()
            
            # Copy sheet dari source
            # Ambil sheet pertama dari source file (biasanya Sheet1)
            src_sheet = None
            for s in src_wb.sheets:
                src_sheet = s
                break
            
            if src_sheet is None:
                print(f"⚠️ Tidak ada sheet di file source: {src_path}")
                src_wb.close()
                dest_wb.close()
                return False
            
            # Tentukan posisi insert
            if insert_position is not None and insert_position < len(dest_wb.sheets):
                target_sheet = dest_wb.sheets[insert_position]
                src_sheet.copy(after=target_sheet)
            else:
                # Insert di akhir
                src_sheet.copy(after=dest_wb.sheets[-1])
            
            # Rename sheet yang baru di-copy
            dest_wb.sheets[-1].name = sheet_name
            
            # Save dan close
            dest_wb.save()
            dest_wb.close()
            src_wb.close()
            
        print(f"✅ Sheet '{sheet_name}' berhasil disalin dengan formula utuh")
        return True
        
    except Exception as e:
        print(f"❌ Error saat menyalin sheet RAFM Manual: {e}")
        import traceback
        traceback.print_exc()
        return False


def get_sheet_insert_position(jenis):
    if jenis == 'trad':
        # Insert setelah: Control, Code, CF ARGO AZTRAD, RAFM Output AZTRAD
        return 3  # Posisi ke-4 (index 3)
    elif jenis == 'ul':
        # Insert setelah: Control, Code, CF ARGO AZUL, RAFM Output AZUL
        return 3  # Posisi ke-4 (index 3)
    elif jenis == 'reas':
        # Insert setelah: Control, Code, CF ARGO REAS, RAFM Output REAS
        return 3  # Posisi ke-4 (index 3)
    else:
        return None  # Insert di akhir


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
        print(f"⚠️ Tidak bisa membaca sheet 'File Path': {e}")
        return

    df.columns = df.columns.str.strip()
    df['Name'] = df['Name'].astype(str).str.strip().str.lower()
    df['File Path'] = df['File Path'].astype(str).str.strip()

    if 'output_path' not in df['Name'].values or 'output_filename' not in df['Name'].values:
        print(f"⚠️ output_path atau output_filename tidak ditemukan di sheet 'File Path'")
        return

    output_path = df.loc[df['Name']=='output_path','File Path'].values[0]
    output_filename = df.loc[df['Name']=='output_filename','File Path'].values[0]
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
                border_format = workbook.add_format({'border':1,'border_color':'black'})
                nrows, ncols = df_sheet.shape
                worksheet.conditional_format(0,0,nrows,ncols-1,{'type':'no_errors','format':border_format})

            if sheet_name.lower().startswith("checking summary"):
                nrows, ncols = df_sheet.shape
                nomor_kolom = df_sheet.iloc[:,0].dropna()
                if not nomor_kolom.empty:
                    nrows = int(nomor_kolom.max()) + 1
                write_checking_summary_formulas(worksheet, df_sheet, result, jenis, nrows, ncols)

    try:
        rafm_manual_path = df.loc[df['Name']=='rafm manual','File Path'].values[0]
        if os.path.exists(rafm_manual_path):
            insert_position = get_sheet_insert_position(jenis)
            success = insert_rafm_manual_with_formulas(
                src_path=rafm_manual_path,
                dest_path=output_file,
                sheet_name="RAFM Output Manual",
                insert_position=insert_position
            )
            if not success:
                print("⚠️ Gagal menyalin RAFM Output Manual, file output tetap dibuat tanpa sheet ini")
        else:
            print(f"⚠️ File RAFM manual tidak ditemukan: {rafm_manual_path}")
    except Exception as e:
        print(f"⚠️ Error saat mengambil path RAFM manual: {e}")

    print(f"✅ Output disimpan di: {output_file}")


def main(input_path):
    start_time = time.time()

    # Tentukan list file .xlsx
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

    # Proses file tunggal
    if len(files) == 1:
        process_input_file(files[0])
    else:
        # Proses paralel
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


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Usage: python main.py <input_file_or_folder>")