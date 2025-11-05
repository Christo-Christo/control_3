import os
import pandas as pd
from xlsxwriter.utility import xl_col_to_name
import syntax.control_4_trad as trad
import syntax.control_4_ul as ul
import syntax.control_4_reas as reas
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import xlwings as xw
from openpyxl import load_workbook
from openpyxl.styles import Border, Side, Alignment
from openpyxl.utils import get_column_letter
import warnings
import shutil
import datetime
import tempfile
import psutil

# Suppress warnings untuk performa
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

cols_to_sum_dict = {
    'trad': trad.cols_to_compare,
    'ul': ul.columns_to_sum_argo,
    'reas': reas.cols_to_compare
}


def kill_excel_processes():
    """Force close all Excel processes"""
    try:
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                if proc.info['name'] and 'excel' in proc.info['name'].lower():
                    print(f"    • Killing Excel PID {proc.info['pid']}")
                    proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        time.sleep(2)
    except Exception as e:
        print(f"    ⚠️ Error killing Excel: {e}")


def write_checking_summary_formulas_xlsxwriter(worksheet, df_sheet, jenis, nrows, ncols):
    """
    Tulis formula checking summary menggunakan xlsxwriter
    Format sama dengan notes.py
    """
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

        # Tentukan kolom dasar dan offset per jenis
        if jenis == 'trad':
            start_col_idx = 4  # E (0-based index)
            cf_argo_col_offset = 2  # kolom C
            cf_rafm_col_offset = 6  # kolom G
            rafm_manual_col_offset = 6  # kolom G
            uvsg_col_offset = 6  # kolom G

        elif jenis == 'ul':
            start_col_idx = 3  # D (0-based index)
            cf_argo_col_offset = 2  # kolom C
            cf_rafm_col_offset = 5  # kolom F
            rafm_manual_col_offset = 5  # kolom F

        else:  # reas
            start_col_idx = 3  # D (0-based index)
            cf_argo_col_offset = 2  # kolom C
            cf_rafm_col_offset = 2  # kolom C
            rafm_manual_col_offset = 2  # kolom C

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


def auto_adjust_column_width(worksheet, df_sheet, max_width=50, sample_size=100):
    """Auto adjust column width based on content - dari notes.py"""
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
    """Apply number formats to worksheet - dari notes.py"""
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


def replace_rafm_output_manual_with_linked_sheet(src_path, dest_path, sheet_name="RAFM Output Manual"):
    """
    Replace RAFM Output Manual sheet dengan copy dari file original
    Preserves all SharePoint links and formulas
    """
    app = None
    src_wb = None
    dest_wb = None
    
    try:
        if not os.path.exists(src_path):
            print(f"    ⚠️ File source RAFM manual tidak ditemukan: {src_path}")
            return False
        
        if not os.path.exists(dest_path):
            print(f"    ⚠️ File destination tidak ditemukan: {dest_path}")
            return False
        
        print(f"    ↳ Replacing '{sheet_name}' with original (preserving links)...")
        
        # Use xlwings to copy sheet (preserves all links)
        app = xw.App(visible=False)
        app.display_alerts = False
        app.screen_updating = False
        
        src_wb = app.books.open(src_path)
        dest_wb = app.books.open(dest_path)
        
        # Delete existing sheet if present
        if sheet_name in [s.name for s in dest_wb.sheets]:
            dest_wb.sheets[sheet_name].delete()
        
        # Copy first sheet from source
        src_sheet = src_wb.sheets[0]
        src_sheet.api.Copy(After=dest_wb.sheets[-1].api)
        
        # Rename copied sheet
        dest_wb.sheets[-1].name = sheet_name
        
        # Save and close
        dest_wb.save()
        dest_wb.close()
        src_wb.close()
        
        app.quit()
        
        print(f"    ✓ Sheet '{sheet_name}' replaced successfully (links intact)")
        return True
        
    except Exception as e:
        print(f"    ⚠️ Gagal replace RAFM Output Manual: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Cleanup
        if dest_wb:
            try:
                dest_wb.close()
            except:
                pass
        if src_wb:
            try:
                src_wb.close()
            except:
                pass
        if app:
            try:
                app.quit()
            except:
                pass


def add_sheets_to_rafm_manual(rafm_manual_path, result_dict, output_path, output_filename, jenis):
    """
    🔧 HYBRID APPROACH:
    1. Copy RAFM Manual file → preserve SharePoint links
    2. Write new sheets using xlsxwriter → perfect formatting
    3. Replace RAFM Output Manual sheet using xlwings → preserve links
    
    Args:
        rafm_manual_path: Path ke file RAFM Output Manual original
        result_dict: Dictionary hasil processing {sheet_name: dataframe}
        output_path: Folder output
        output_filename: Nama file output baru
        jenis: 'trad', 'ul', atau 'reas'
    
    Returns:
        str: Path file output yang berhasil dibuat
    """
    try:
        if not os.path.exists(rafm_manual_path):
            print(f"❌ File RAFM Manual tidak ditemukan: {rafm_manual_path}")
            return None
        
        print(f"\n🚀 Menambahkan sheet ke RAFM Output Manual...")
        start_time = time.time()
        
        # Create output directory
        os.makedirs(output_path, exist_ok=True)
        output_file = os.path.join(output_path, output_filename)
        
        # 🔧 STEP 1: Handle existing output file
        if os.path.exists(output_file):
            print(f"  ↳ File sudah ada, mencoba hapus...")
            try:
                os.remove(output_file)
                print(f"  ✓ File lama berhasil dihapus")
            except PermissionError:
                print(f"  ⚠️ File sedang digunakan, force close Excel...")
                kill_excel_processes()
                time.sleep(2)
                
                try:
                    os.remove(output_file)
                    print(f"  ✓ File lama berhasil dihapus setelah force close")
                except PermissionError:
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    base_name = os.path.splitext(output_filename)[0]
                    ext = os.path.splitext(output_filename)[1]
                    output_filename = f"{base_name}_{timestamp}{ext}"
                    output_file = os.path.join(output_path, output_filename)
                    print(f"  ↳ Nama baru: {output_filename}")
        
        # 🔧 STEP 2: Copy original RAFM Manual file (preserves all metadata)
        print(f"  ↳ Copying original file (preserving all links)...")
        shutil.copy2(rafm_manual_path, output_file)
        print(f"  ✓ File copied successfully (SharePoint links intact)")
        
        time.sleep(0.5)  # Wait for file system
        
        # 🔧 STEP 3: Write new sheets using xlsxwriter (perfect formatting)
        print(f"  ↳ Adding {len(result_dict)} new sheets with xlsxwriter...")
        
        # Create temporary file for xlsxwriter output
        temp_file = os.path.join(tempfile.gettempdir(), f"temp_{output_filename}")
        
        with pd.ExcelWriter(temp_file, engine='xlsxwriter',
                            engine_kwargs={'options': {'strings_to_numbers': False}}) as writer:
            workbook = writer.book
            
            for sheet_name, df_sheet in result_dict.items():
                # Skip RAFM Output Manual - will be handled by xlwings
                if sheet_name == 'RAFM Output Manual':
                    print(f"    • {sheet_name}: SKIP (will preserve from original)")
                    continue
                
                print(f"    • Writing sheet: {sheet_name}")
                
                # Write data
                if sheet_name == 'Control':
                    df_sheet.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
                else:
                    df_sheet.to_excel(writer, sheet_name=sheet_name, index=False, header=True)
                
                worksheet = writer.sheets[sheet_name]
                
                # Auto adjust column width (dari notes.py)
                auto_adjust_column_width(worksheet, df_sheet)
                
                # Apply number formats (dari notes.py)
                apply_number_formats(workbook, worksheet, df_sheet, sheet_name)
                
                # Add borders (kecuali Control sheet)
                if sheet_name != 'Control':
                    border_format = workbook.add_format({
                        'border': 1,
                        'border_color': 'black'
                    })
                    nrows, ncols = df_sheet.shape
                    worksheet.conditional_format(
                        0, 0, nrows, ncols - 1,
                        {'type': 'no_errors', 'format': border_format}
                    )
                
                # Write formulas for Checking Summary
                if sheet_name.lower().startswith("checking summary"):
                    print(f"    • Writing formulas for {sheet_name}...")
                    nrows, ncols = df_sheet.shape
                    nomor_kolom = df_sheet.iloc[:, 0].dropna()
                    if not nomor_kolom.empty:
                        nrows = int(nomor_kolom.max()) + 1
                    write_checking_summary_formulas_xlsxwriter(worksheet, df_sheet, jenis, nrows, ncols)
        
        print(f"  ✓ Temporary file created with all sheets")
        
        # 🔧 STEP 4: Merge temp file sheets into output file using xlwings
        print(f"  ↳ Merging sheets into output file...")
        
        app = None
        temp_wb = None
        output_wb = None
        
        try:
            app = xw.App(visible=False)
            app.display_alerts = False
            app.screen_updating = False
            
            temp_wb = app.books.open(temp_file)
            output_wb = app.books.open(output_file)
            
            # Copy all sheets from temp to output (except RAFM Output Manual)
            for sheet in temp_wb.sheets:
                sheet_name = sheet.name
                
                # Delete if exists in output
                if sheet_name in [s.name for s in output_wb.sheets]:
                    output_wb.sheets[sheet_name].delete()
                
                # Copy sheet
                sheet.api.Copy(After=output_wb.sheets[-1].api)
                print(f"    • Copied sheet: {sheet_name}")
            
            # Save and close
            output_wb.save()
            output_wb.close()
            temp_wb.close()
            app.quit()
            
            print(f"  ✓ All sheets merged successfully")
            
        finally:
            if output_wb:
                try:
                    output_wb.close()
                except:
                    pass
            if temp_wb:
                try:
                    temp_wb.close()
                except:
                    pass
            if app:
                try:
                    app.quit()
                except:
                    pass
        
        # Clean up temp file
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except:
            pass
        
        elapsed = time.time() - start_time
        
        print(f"✅ Selesai dalam {elapsed:.2f} detik")
        print(f"   📁 Output: {output_file}")
        print(f"   ✅ Format perfect (xlsxwriter)")
        print(f"   ✅ SharePoint links PRESERVED")
        
        return output_file
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def process_input_file(file_path):
    """Process single input file"""
    filename = os.path.basename(file_path).lower()
    
    # Deteksi jenis
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

    print(f"\n{'='*60}")
    print(f"📄 PROCESSING: {filename}")
    print(f"   Jenis: {jenis.upper()}")
    print(f"{'='*60}")
    
    # Baca File Path sheet
    try:
        df = pd.read_excel(file_path, sheet_name='File Path')
    except Exception as e:
        print(f"⚠️ Tidak bisa membaca sheet 'File Path': {e}")
        return

    df.columns = df.columns.str.strip()
    df['Name'] = df['Name'].astype(str).str.strip().str.lower()
    df['File Path'] = df['File Path'].astype(str).str.strip()

    # Validasi required fields
    required = ['output_path', 'output_filename', 'rafm manual']
    missing = [r for r in required if r not in df['Name'].values]
    if missing:
        print(f"⚠️ Missing di File Path sheet: {missing}")
        return

    # Get paths
    output_path = df.loc[df['Name']=='output_path', 'File Path'].values[0]
    output_filename = df.loc[df['Name']=='output_filename', 'File Path'].values[0]
    rafm_manual_path = df.loc[df['Name']=='rafm manual', 'File Path'].values[0]

    # Proses: Tambahkan sheets ke RAFM Manual
    output_file = add_sheets_to_rafm_manual(
        rafm_manual_path=rafm_manual_path,
        result_dict=result,
        output_path=output_path,
        output_filename=output_filename,
        jenis=jenis
    )
    
    if output_file:
        print(f"\n🎉 SUCCESS: {os.path.basename(output_file)}")
    else:
        print(f"\n❌ FAILED: {filename}")


def main(input_path):
    """Main entry point"""
    print("\n" + "="*60)
    print("🔧 CONTROL 4 - RAFM OUTPUT PROCESSOR")
    print("="*60)
    
    start_time = time.time()

    # Deteksi input
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
        print("📂 Tidak ada file .xlsx ditemukan")
        return

    print(f"📊 Ditemukan {len(files)} file untuk diproses\n")

    # Process files - Sequential (xlwings limitation)
    print(f"📋 Mode: Sequential processing\n")
    
    success_count = 0
    fail_count = 0
    
    for idx, file_path in enumerate(files, 1):
        filename = os.path.basename(file_path)
        print(f"\n[{idx}/{len(files)}] Processing: {filename}")
        
        try:
            process_input_file(file_path)
            success_count += 1
            print(f"✅ [{idx}/{len(files)}] Completed: {filename}")
        except Exception as e:
            fail_count += 1
            print(f"❌ [{idx}/{len(files)}] Failed: {filename}")
            print(f"   Error: {e}")
            import traceback
            traceback.print_exc()

    # Summary
    elapsed = time.time() - start_time
    print("\n" + "="*60)
    print(f"📊 Summary:")
    print(f"   Total: {len(files)} file(s)")
    print(f"   ✅ Success: {success_count}")
    print(f"   ❌ Failed: {fail_count}")
    print(f"⏱️  TOTAL WAKTU: {elapsed:.2f} detik")
    if len(files) > 0:
        print(f"⚡ Avg: {elapsed/len(files):.2f} detik/file")
    print("="*60)


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Usage: python main.py <input_file_or_folder>")