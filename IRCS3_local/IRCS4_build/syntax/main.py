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
from openpyxl.utils.dataframe import dataframe_to_rows
import warnings
import shutil
import datetime
import tempfile

# Suppress warnings untuk performa
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

cols_to_sum_dict = {
    'trad': trad.cols_to_compare,
    'ul': ul.columns_to_sum_argo,
    'reas': reas.cols_to_compare
}


def write_checking_summary_formulas_openpyxl(ws, df_sheet, jenis, start_row=2):
    """
    Tulis formula checking summary menggunakan openpyxl
    
    Args:
        ws: openpyxl worksheet object
        df_sheet: DataFrame checking summary
        jenis: 'trad', 'ul', atau 'reas'
        start_row: Baris mulai data (default=2, karena row 1 = header)
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
    
    nrows = len(df_sheet)
    ncols = len(df_sheet.columns)
    
    # Tentukan kolom mulai formula dan offset
    if jenis == 'trad':
        start_col_idx = 5  # Kolom E (1-based: 5)
        cf_argo_col_offset = 3  # Kolom C
        cf_rafm_col_offset = 7  # Kolom G
        rafm_manual_col_offset = 7
        uvsg_col_offset = 7
    elif jenis == 'ul':
        start_col_idx = 4  # Kolom D
        cf_argo_col_offset = 3  # Kolom C
        cf_rafm_col_offset = 6  # Kolom F
        rafm_manual_col_offset = 6
    else:  # reas
        start_col_idx = 4  # Kolom D
        cf_argo_col_offset = 3  # Kolom C
        cf_rafm_col_offset = 3  # Kolom C
        rafm_manual_col_offset = 3
    
    # Loop per row (skip header)
    for row_idx in range(nrows):
        row_excel = start_row + row_idx
        
        # Loop per column (mulai dari kolom formula)
        for col_idx in range(start_col_idx, ncols + 1):
            relative_offset = col_idx - start_col_idx
            
            if jenis == 'trad':
                from openpyxl.utils import get_column_letter
                cf_argo_col = get_column_letter(cf_argo_col_offset + relative_offset)
                cf_rafm_col = get_column_letter(cf_rafm_col_offset + relative_offset)
                rafm_manual_col = get_column_letter(rafm_manual_col_offset + relative_offset)
                uvsg_col = get_column_letter(uvsg_col_offset + relative_offset)
                
                formula = (
                    f"='{sheet_names['trad']['cf_argo']}'!{cf_argo_col}{row_excel}"
                    f"-'{sheet_names['trad']['cf_rafm']}'!{cf_rafm_col}{row_excel}"
                    f"+'{sheet_names['trad']['rafm_manual']}'!{rafm_manual_col}{row_excel}"
                    f"-'{sheet_names['trad']['uvsg']}'!{uvsg_col}{row_excel}"
                )
            elif jenis == 'ul':
                from openpyxl.utils import get_column_letter
                cf_argo_col = get_column_letter(cf_argo_col_offset + relative_offset)
                cf_rafm_col = get_column_letter(cf_rafm_col_offset + relative_offset)
                rafm_manual_col = get_column_letter(rafm_manual_col_offset + relative_offset)
                
                formula = (
                    f"='{sheet_names['ul']['cf_argo']}'!{cf_argo_col}{row_excel}"
                    f"-'{sheet_names['ul']['cf_rafm']}'!{cf_rafm_col}{row_excel}"
                    f"-'{sheet_names['ul']['rafm_manual']}'!{rafm_manual_col}{row_excel}"
                )
            else:  # reas
                from openpyxl.utils import get_column_letter
                cf_argo_col = get_column_letter(cf_argo_col_offset + relative_offset)
                cf_rafm_col = get_column_letter(cf_rafm_col_offset + relative_offset)
                rafm_manual_col = get_column_letter(rafm_manual_col_offset + relative_offset)
                
                formula = (
                    f"='{sheet_names['reas']['cf_argo']}'!{cf_argo_col}{row_excel}"
                    f"-'{sheet_names['reas']['cf_rafm']}'!{cf_rafm_col}{row_excel}"
                    f"+'{sheet_names['reas']['rafm_manual']}'!{rafm_manual_col}{row_excel}"
                )
            
            ws.cell(row=row_excel, column=col_idx, value=formula)


def add_sheets_to_rafm_manual(rafm_manual_path, result_dict, output_path, output_filename, jenis):
    """
    Tambahkan sheet hasil processing ke file RAFM Output Manual existing,
    kemudian Save As dengan nama baru
    
    Args:
        rafm_manual_path: Path ke file RAFM Output Manual original
        result_dict: Dictionary hasil processing {sheet_name: dataframe}
        output_path: Folder output
        output_filename: Nama file output baru
        jenis: 'trad', 'ul', atau 'reas'
    
    Returns:
        str: Path file output yang berhasil dibuat
    """
    wb = None  # Initialize untuk finally block
    
    try:
        if not os.path.exists(rafm_manual_path):
            print(f"❌ File RAFM Manual tidak ditemukan: {rafm_manual_path}")
            return None
        
        print(f"\n🚀 Menambahkan sheet ke RAFM Output Manual...")
        start_time = time.time()
        
        # Buat output path
        os.makedirs(output_path, exist_ok=True)
        output_file = os.path.join(output_path, output_filename)
        
        # PRE-CHECK: Cek apakah output file bisa di-write
        if os.path.exists(output_file):
            print(f"  ↳ File sudah ada, mencoba hapus...")
            
            # Method 1: Direct delete
            try:
                os.remove(output_file)
                print(f"  ✓ File lama berhasil dihapus")
            except PermissionError:
                # Method 2: Force close Excel
                print(f"  ⚠️ File sedang digunakan, force close Excel...")
                kill_excel_processes_for_file(output_file)
                time.sleep(2)  # Wait for Excel to fully close
                
                try:
                    os.remove(output_file)
                    print(f"  ✓ File lama berhasil dihapus setelah force close")
                except PermissionError:
                    # Method 3: Use alternative name
                    print(f"  ⚠️ File masih terkunci, menggunakan nama alternatif...")
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    base_name = os.path.splitext(output_filename)[0]
                    ext = os.path.splitext(output_filename)[1]
                    output_filename = f"{base_name}_{timestamp}{ext}"
                    output_file = os.path.join(output_path, output_filename)
                    print(f"  ↳ Nama baru: {output_filename}")
        
        # Load workbook RAFM Manual menggunakan openpyxl
        print(f"  ↳ Membuka file RAFM Manual (formula tetap utuh)...")
        # OPTIMASI: read_only=False tapi data_only=False untuk preserve formula
        # keep_links=True untuk preserve external links
        wb = load_workbook(rafm_manual_path, data_only=False, keep_links=True)
        
        # RENAME 'Sheet1' menjadi 'RAFM Output Manual' jika ada
        if 'Sheet1' in wb.sheetnames:
            print(f"  ↳ Rename 'Sheet1' → 'RAFM Output Manual'")
            wb['Sheet1'].title = 'RAFM Output Manual'
        
        # Jika sudah ada 'RAFM Output Manual' tapi ada 'Sheet1' juga, hapus Sheet1
        if 'Sheet1' in wb.sheetnames and 'RAFM Output Manual' in wb.sheetnames:
            print(f"  ↳ Menghapus 'Sheet1' duplikat...")
            del wb['Sheet1']
        
        # Border style untuk formatting
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        # Order sheets berdasarkan jenis
        if jenis == 'trad':
            sheet_order = [
                'Control', 'Code', 
                'CF ARGO AZTRAD', 'RAFM Output AZTRAD', 
                'RAFM Output Manual',  # Sheet existing (sudah ada)
                'RAFM Output AZUL_PI',
                'Checking Summary AZTRAD'
            ]
        elif jenis == 'ul':
            sheet_order = [
                'Control', 'Code',
                'CF ARGO AZUL', 'RAFM Output AZUL',
                'RAFM Output Manual',  # Sheet existing
                'Checking Summary AZUL'
            ]
        else:  # reas
            sheet_order = [
                'Control', 'Code',
                'CF ARGO REAS', 'RAFM Output REAS',
                'RAFM Output Manual',  # Sheet existing
                'Checking Summary REAS'
            ]
        
        # Tambahkan sheet baru dari result_dict
        print(f"  ↳ Menambahkan {len(result_dict)} sheet baru...")
        for sheet_name, df in result_dict.items():
            # Skip RAFM Output Manual karena sudah ada di file original
            if sheet_name == 'RAFM Output Manual':
                print(f"    • {sheet_name}: SKIP (already exists)")
                continue
            
            print(f"    • Menambahkan sheet: {sheet_name}")
            
            # FIX: Convert <NA> values to None sebelum tulis ke Excel
            # OPTIMASI: Gunakan method paling cepat
            df = df.copy()  # Avoid SettingWithCopyWarning
            
            # Replace semua NA types dengan None
            df = df.replace({pd.NA: None, pd.NaT: None})
            df = df.where(pd.notna(df), None)  # Catch remaining NaN
            
            # Buat sheet baru
            if sheet_name in wb.sheetnames:
                del wb[sheet_name]  # Hapus jika sudah ada
            
            ws = wb.create_sheet(title=sheet_name)
            
            # OPTIMASI: Batch write menggunakan append (lebih cepat)
            if sheet_name == 'Control':
                # Control tanpa header
                for row in df.values:
                    # Convert row to list and handle any remaining NA
                    row_list = [None if pd.isna(v) else v for v in row]
                    ws.append(row_list)
            else:
                # Sheet lain dengan header
                # Write header first
                ws.append(list(df.columns))
                
                # Write data rows
                for row in df.values:
                    # Convert row to list and handle any remaining NA
                    row_list = [None if pd.isna(v) else v for v in row]
                    ws.append(row_list)
                
                # Apply formatting AFTER data written (lebih cepat)
                print(f"    • Applying formatting...")
                for row in ws.iter_rows(min_row=1, max_row=ws.max_row, 
                                       min_col=1, max_col=ws.max_column):
                    for cell in row:
                        cell.border = thin_border
                        
                        # Format accounting untuk numeric columns (skip header)
                        if cell.row > 1 and isinstance(cell.value, (int, float)):
                            cell.number_format = '_-* #,##0_-;_-* (#,##0);_-* "-"_-;_-@_-'
            
            # Tulis formula untuk Checking Summary
            if sheet_name.startswith("Checking Summary"):
                print(f"    • Menulis formula checking summary...")
                write_checking_summary_formulas_openpyxl(ws, df, jenis)
            
            # Auto-adjust column width
            for column in ws.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                ws.column_dimensions[column_letter].width = adjusted_width
        
        # Reorder sheets sesuai urutan yang diinginkan
        print(f"  ↳ Mengurutkan sheets...")
        existing_sheets = wb.sheetnames
        ordered_sheets = [s for s in sheet_order if s in existing_sheets]
        
        # Move sheets ke posisi yang benar
        for idx, sheet_name in enumerate(ordered_sheets):
            if sheet_name in wb.sheetnames:
                sheet = wb[sheet_name]
                wb.move_sheet(sheet, offset=idx - wb.index(sheet))
        
        # Save As dengan nama baru
        print(f"  ↳ Menyimpan sebagai: {output_filename}")
        
        # STRATEGY 1: Try direct save
        save_success = False
        try:
            wb.save(output_file)
            save_success = True
            print(f"  ✓ File berhasil disimpan")
        except PermissionError as e:
            print(f"  ⚠️ Permission error saat save langsung")
            
            # STRATEGY 2: Save to temp first, then move
            try:
                temp_file = os.path.join(tempfile.gettempdir(), output_filename)
                print(f"  ↳ Mencoba save ke temp: {temp_file}")
                
                wb.save(temp_file)
                wb.close()
                wb = None  # Mark as closed
                
                # Wait a bit for file system to release
                time.sleep(0.5)
                
                # Try to move from temp to destination
                if os.path.exists(output_file):
                    os.remove(output_file)
                
                shutil.move(temp_file, output_file)
                save_success = True
                print(f"  ✓ File berhasil disimpan via temp")
                
            except Exception as temp_err:
                print(f"  ⚠️ Gagal save via temp: {temp_err}")
                
                # STRATEGY 3: Save with timestamp
                try:
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    base_name = os.path.splitext(output_filename)[0]
                    ext = os.path.splitext(output_filename)[1]
                    output_filename_new = f"{base_name}_{timestamp}{ext}"
                    output_file_new = os.path.join(output_path, output_filename_new)
                    
                    print(f"  ↳ Mencoba dengan nama alternatif: {output_filename_new}")
                    wb.save(output_file_new)
                    output_file = output_file_new
                    save_success = True
                    print(f"  ✓ File disimpan dengan nama alternatif")
                    
                except Exception as final_err:
                    # STRATEGY 4: Keep in temp and inform user
                    print(f"  ❌ Semua strategi save gagal!")
                    print(f"  → File tersimpan di temp: {temp_file}")
                    print(f"  → Silakan copy manual ke: {output_file}")
                    output_file = temp_file
        
        # Close workbook if still open
        if wb is not None:
            wb.close()
            wb = None
        
        elapsed = time.time() - start_time
        
        if save_success:
            print(f"✅ Selesai dalam {elapsed:.2f} detik")
            print(f"   📁 Output: {output_file}")
            print(f"   ✓ Formula SharePoint di 'RAFM Output Manual' tetap utuh")
        
        return output_file if save_success else None
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    finally:
        # ALWAYS close workbook to prevent locks
        if wb is not None:
            try:
                wb.close()
            except:
                pass


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

    # Proses: Tambahkan sheets ke RAFM Manual dan Save As
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
    """Main entry point - OPTIMIZED dengan ThreadPoolExecutor"""
    print("\n" + "="*60)
    print("🔧 CONTROL 4 - RAFM OUTPUT PROCESSOR (TURBO MODE)")
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

    # OPTIMASI: Gunakan ThreadPoolExecutor untuk parallel processing
    # Thread lebih cocok untuk I/O bound operations (baca/tulis Excel)
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    # Determine optimal workers
    max_workers = min(len(files), os.cpu_count() or 4, 8)  # Max 8 threads
    
    if len(files) == 1:
        # Single file - langsung process
        print(f"📋 Mode: Single file\n")
        process_input_file(files[0])
    else:
        # Multiple files - parallel processing
        print(f"⚡ Mode: Parallel processing ({max_workers} workers)\n")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(process_input_file, f): f 
                for f in files
            }
            
            # Process as they complete
            for idx, future in enumerate(as_completed(future_to_file), 1):
                file_path = future_to_file[future]
                filename = os.path.basename(file_path)
                
                try:
                    future.result()
                    print(f"✅ [{idx}/{len(files)}] Completed: {filename}")
                except Exception as e:
                    print(f"❌ [{idx}/{len(files)}] Failed: {filename}")
                    print(f"   Error: {e}")
                    import traceback
                    traceback.print_exc()

    # Summary
    elapsed = time.time() - start_time
    print("\n" + "="*60)
    print(f"⏲️  TOTAL WAKTU: {elapsed:.2f} detik")
    print(f"📁 Processed: {len(files)} file(s)")
    print(f"⚡ Avg: {elapsed/len(files):.2f} detik/file")
    print("="*60)


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Usage: python main.py <input_file_or_folder>")