import os
import pandas as pd
from xlsxwriter.utility import xl_col_to_name
import syntax.control_4_trad as trad
import syntax.control_4_ul as ul
import syntax.control_4_reas as reas
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import xlwings as xw

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


def insert_rafm_manual_optimized(src_path, dest_path, sheet_name="RAFM Output Manual", insert_position=None):
    """
    OPTIMIZED: Copy sheet RAFM Manual dengan formula utuh (termasuk SharePoint links)
    menggunakan xlwings dengan optimasi kecepatan maksimal
    
    Optimasi yang diterapkan:
    1. Minimize Excel interactions
    2. Batch operations
    3. Disable recalculation saat copy
    4. Reuse Excel instance
    """
    try:
        if not os.path.exists(src_path):
            print(f"⚠️ File RAFM manual tidak ditemukan: {src_path}")
            return False
            
        if not os.path.exists(dest_path):
            print(f"⚠️ File output tidak ditemukan: {dest_path}")
            return False

        print(f"🔄 Menyalin sheet '{sheet_name}' (OPTIMIZED MODE)...")
        start_time = time.time()
        
        # Reuse existing Excel instance jika ada (lebih cepat)
        try:
            app = xw.apps.active
            if app is None:
                raise Exception("No active app")
        except:
            app = xw.App(visible=False, add_book=False)
        
        # KUNCI OPTIMASI: Matikan semua yang memperlambat
        app.display_alerts = False
        app.screen_updating = False
        app.calculation = 'manual'  # Matikan auto-calculate (PENTING!)
        app.enable_events = False
        
        try:
            # Buka files dengan minimal interaction
            print("  ↳ Membuka files...")
            src_wb = app.books.open(src_path, update_links=False, read_only=True)
            dest_wb = app.books.open(dest_path, update_links=False)
            
            # Hapus sheet lama jika ada
            if sheet_name in [s.name for s in dest_wb.sheets]:
                print(f"  ↳ Menghapus sheet lama...")
                dest_wb.sheets[sheet_name].delete()
            
            # Copy sheet (ini yang lama, tapi unavoidable untuk preserve links)
            src_sheet = src_wb.sheets[0]
            print(f"  ↳ Menyalin sheet (mohon tunggu)...")
            
            # Tentukan posisi
            if insert_position is not None and insert_position < len(dest_wb.sheets):
                target_sheet = dest_wb.sheets[insert_position]
                src_sheet.api.Copy(After=target_sheet.api)
            else:
                src_sheet.api.Copy(After=dest_wb.sheets[-1].api)
            
            # Rename
            dest_wb.sheets[-1].name = sheet_name
            print(f"  ↳ Sheet berhasil di-copy")
            
            # Save dengan calculation masih manual (lebih cepat)
            print("  ↳ Menyimpan file...")
            dest_wb.save()
            
            # Close files
            src_wb.close()
            dest_wb.close()
            
            elapsed = time.time() - start_time
            print(f"✅ Selesai dalam {elapsed:.2f} detik")
            print(f"   Formula SharePoint tetap utuh dan aktif")
            
            return True
            
        finally:
            # Restore calculation mode
            app.calculation = 'automatic'
            
    except Exception as e:
        print(f"❌ Error saat copy: {e}")
        import traceback
        traceback.print_exc()
        return False


def get_sheet_insert_position(jenis):
    """Tentukan posisi insert sheet RAFM Output Manual"""
    if jenis in ['trad', 'ul', 'reas']:
        return 3  # Setelah: Control, Code, CF ARGO, RAFM Output
    return None


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
        print(f"⚠️ output_path atau output_filename tidak ditemukan")
        return

    output_path = df.loc[df['Name']=='output_path','File Path'].values[0]
    output_filename = df.loc[df['Name']=='output_filename','File Path'].values[0]
    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, output_filename)

    # Tulis semua sheet hasil processing
    print("📝 Menulis hasil processing ke Excel...")
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

    print("✅ File dasar berhasil dibuat")

    # Insert RAFM Output Manual dengan optimasi
    try:
        rafm_manual_path = df.loc[df['Name']=='rafm manual','File Path'].values[0]
        if os.path.exists(rafm_manual_path):
            print("\n🚀 Memulai copy RAFM Output Manual...")
            insert_position = get_sheet_insert_position(jenis)
            success = insert_rafm_manual_optimized(
                src_path=rafm_manual_path,
                dest_path=output_file,
                sheet_name="RAFM Output Manual",
                insert_position=insert_position
            )
            if not success:
                print("⚠️ Gagal copy RAFM Output Manual")
        else:
            print(f"⚠️ File RAFM manual tidak ditemukan: {rafm_manual_path}")
    except Exception as e:
        print(f"⚠️ Error: {e}")

    print(f"\n✅ OUTPUT FINAL: {output_file}")


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
        print(f"❌ Path tidak valid: {input_path}")
        return

    if not files:
        print("📂 Tidak ada file .xlsx")
        return

    print(f"🔧 Memproses {len(files)} file...\n")

    # OPTIMASI: Proses sequential untuk reuse Excel instance
    # (Parallel processing justru lebih lambat karena multiple Excel instances)
    if len(files) == 1:
        process_input_file(files[0])
    else:
        # Untuk multiple files, tetap sequential untuk share Excel instance
        print("💡 Mode sequential (lebih cepat untuk multiple files)")
        for f in files:
            try:
                process_input_file(f)
            except Exception as e:
                print(f"❌ Error: {e}")

    end_time = time.time()
    print(f"\n⏲️ Total: {end_time - start_time:.2f} detik")


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Usage: python main.py <input_file_or_folder>")