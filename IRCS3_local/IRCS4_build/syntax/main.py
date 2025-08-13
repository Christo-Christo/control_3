import os
import pandas as pd
from xlsxwriter.utility import xl_col_to_name
import syntax.control_4_trad as trad
import syntax.control_4_ul as ul
import syntax.control_4_reas as reas
from concurrent.futures import ProcessPoolExecutor, as_completed
import time

cols_to_sum_dict = {
    'trad': trad.cols_to_compare,
    'ul': ul.columns_to_sum_argo,
    'reas': reas.cols_to_compare
}


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

    print("Isi 'Name':", df['Name'].tolist())

    if 'output_path' not in df['Name'].values or 'output_filename' not in df['Name'].values:
        print(f"⚠️ output_path atau output_filename tidak ditemukan di sheet 'File Path' pada {file_path}")
        return

    output_path = df.loc[df['Name'] == 'output_path', 'File Path'].values[0]
    output_filename = df.loc[df['Name'] == 'output_filename', 'File Path'].values[0]

    print(f"output_path: {output_path}")
    print(f"output_filename: {output_filename}")

    cols_to_sum = cols_to_sum_dict.get(jenis, [])

    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, output_filename)

    process_start_time = time.time()

    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        for sheet_name, df_sheet in result.items():
            if sheet_name == 'Control':
                df_sheet.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
            else:
                df_sheet.to_excel(writer, sheet_name=sheet_name, index=False, header=True)

            workbook = writer.book
            worksheet = writer.sheets[sheet_name]

            # Skip formatting for 'Control' sheet
            if sheet_name != 'Control':
                format_accounting = workbook.add_format({
                    'num_format': '_-* #,##0_-;_-* (#,##0);_-* "-"_-;_-@_-'
                })
                format_int = workbook.add_format({'num_format': '0'})
                format_no_format = workbook.add_format() 

                if hasattr(df_sheet, 'columns'):
                    for col_idx, col_name in enumerate(df_sheet.columns):
                        col_name_lower = str(col_name).lower()
                        if ('include year' in col_name_lower or 
                            'exclude year' in col_name_lower or 
                            'speed duration' in col_name_lower):
                            if 'speed duration' in col_name_lower:
                                worksheet.set_column(col_idx, col_idx, None, format_no_format)
                            else:
                                worksheet.set_column(col_idx, col_idx, None, format_int)
                        else:
                            worksheet.set_column(col_idx, col_idx, None, format_accounting)
                else:
                    worksheet.set_column(0, df_sheet.shape[1] - 1, None, format_accounting)

            if sheet_name != 'Control':
                border_format = workbook.add_format({'border': 1, 'border_color': 'black'})
                nrows, ncols = df_sheet.shape
                worksheet.conditional_format(
                    0, 0, nrows, ncols - 1,
                    {'type': 'no_errors', 'format': border_format}
                )

            if sheet_name.lower().startswith("checking summary"):
                nrows, ncols = df_sheet.shape
                nomor_kolom = df_sheet.iloc[:, 0]

                nomor_kolom = nomor_kolom.dropna()
                if not nomor_kolom.empty:
                    nrows = int(nomor_kolom.max()) + 1 
                else:
                    nrows = df_sheet.shape[0]
                    
                if jenis == 'trad':
                    cf_sheet = 'CF ARGO AZTRAD'
                    rafm_sheet_1 = 'RAFM Output AZTRAD'
                    rafm_sheet_2 = 'RAFM Output AZUL_PI'
                    manual_sheet = 'RAFM Output Manual'

                    cf_df = result[cf_sheet]
                    rafm1_df = result[rafm_sheet_1]
                    rafm2_df = result[rafm_sheet_2]
                    manual_df = result[manual_sheet]
                    max_col_cf = xl_col_to_name(cf_df.shape[1] - 1)
                    max_col_rafm1 = xl_col_to_name(rafm1_df.shape[1] - 1)
                    max_col_rafm2 = xl_col_to_name(rafm2_df.shape[1] - 1)
                    max_col_manual = xl_col_to_name(manual_df.shape[1] - 1)

                    max_row_cf = cf_df.shape[0] + 1
                    max_row_rafm1 = rafm1_df.shape[0] + 1
                    max_row_rafm2 = rafm2_df.shape[0] + 1
                    max_row_manual = manual_df.shape[0] + 1

                    for row_idx in range(1, nrows):
                        for col_idx in range(4, ncols):
                            col_letter = xl_col_to_name(col_idx)
                            col_header = f"{col_letter}$1"
                            row_b = f"$B{row_idx+1}"
                            row_c = f"$C{row_idx+1}"
                            row_d = f"$D{row_idx+1}"

                            formula = (
                                f"=IFERROR(INDEX('{cf_sheet}'!$C$2:${max_col_cf}${max_row_cf}, MATCH({row_b}, '{cf_sheet}'!$B$2:$B${max_row_cf}, 0), MATCH({col_header}, '{cf_sheet}'!$C$1:${max_col_cf}$1, 0)),0)"
                                f"-IFERROR(INDEX('{rafm_sheet_1}'!$C$2:${max_col_rafm1}${max_row_rafm1}, MATCH({row_c}, '{rafm_sheet_1}'!$B$2:$B${max_row_rafm1}, 0), MATCH({col_header}, '{rafm_sheet_1}'!$C$1:${max_col_rafm1}$1, 0)),0)"
                                f"+IFERROR(INDEX('{manual_sheet}'!$C$2:${max_col_manual}${max_row_manual}, MATCH({row_c}, '{manual_sheet}'!$B$2:$B${max_row_manual}, 0), MATCH({col_header}, '{manual_sheet}'!$C$1:${max_col_manual}$1, 0)),0)"
                                f"-IFERROR(INDEX('{rafm_sheet_2}'!$C$2:${max_col_rafm2}${max_row_rafm2}, MATCH({row_d}, '{rafm_sheet_2}'!$B$2:$B${max_row_rafm2}, 0), MATCH({col_header}, '{rafm_sheet_2}'!$C$1:${max_col_rafm2}$1, 0)),0)"
                            )
                            worksheet.write_formula(row_idx, col_idx, formula)

                elif jenis == 'ul':
                    cf_sheet = 'CF ARGO AZUL'
                    rafm_sheet = 'RAFM Output AZUL'
                    manual_sheet = 'RAFM Output Manual'

                    cf_df = result[cf_sheet]
                    rafm_df = result[rafm_sheet]
                    manual_df = result[manual_sheet]
                    max_col_cf = xl_col_to_name(cf_df.shape[1] - 1)
                    max_col_rafm = xl_col_to_name(rafm_df.shape[1] - 1)
                    max_col_manual = xl_col_to_name(manual_df.shape[1] - 1)

                    max_row_cf = cf_df.shape[0] + 1
                    max_row_rafm = rafm_df.shape[0] + 1
                    max_row_manual = manual_df.shape[0] + 1

                    for row_idx in range(1, nrows):
                        for col_idx in range(3, ncols): 
                            col_letter = xl_col_to_name(col_idx)
                            col_header = f"{col_letter}$1"
                            row_b = f"$B{row_idx+1}"
                            row_c = f"$C{row_idx+1}"

                            formula = (
                                f"="
                                f"IF(OR(ISNUMBER(SEARCH(\"clm_base\",{col_header})),ISNUMBER(SEARCH(\"clm_pro\",{col_header})),ISNUMBER(SEARCH(\"clm_hth\",{col_header}))),"
                                f"IFERROR(INDEX('{cf_sheet}'!$C$2:${max_col_cf}${max_row_cf}, MATCH({row_b}, '{cf_sheet}'!$B$2:$B${max_row_cf}, 0), MATCH({col_header}, '{cf_sheet}'!$C$1:${max_col_cf}$1, 0))/3,0),"
                                f"IFERROR(INDEX('{cf_sheet}'!$C$2:${max_col_cf}${max_row_cf}, MATCH({row_b}, '{cf_sheet}'!$B$2:$B${max_row_cf}, 0), MATCH({col_header}, '{cf_sheet}'!$C$1:${max_col_cf}$1, 0)),0))"
                                f"-IFERROR(INDEX('{rafm_sheet}'!$C$2:${max_col_rafm}${max_row_rafm}, MATCH({row_c}, '{rafm_sheet}'!$B$2:$B${max_row_rafm}, 0), MATCH({col_header}, '{rafm_sheet}'!$C$1:${max_col_rafm}$1, 0)),0)"
                                f"-IFERROR(INDEX('{manual_sheet}'!$C$2:${max_col_manual}${max_row_manual}, MATCH({row_c}, '{manual_sheet}'!$B$2:$B${max_row_manual}, 0), MATCH({col_header}, '{manual_sheet}'!$C$1:${max_col_manual}$1, 0)),0)"
                                f"-IF(ISNUMBER(SEARCH(\"lrc_cl_inv_surr\",{col_header})),"
                                f"IFERROR(INDEX('{rafm_sheet}'!$C$2:${max_col_rafm}${max_row_rafm}, MATCH({row_c}, '{rafm_sheet}'!$B$2:$B${max_row_rafm}, 0), MATCH(\"tab_ph\", '{rafm_sheet}'!$C$1:${max_col_rafm}$1, 0)),0),0)"
                            )

                            worksheet.write_formula(row_idx, col_idx, formula)

                elif jenis == 'reas':
                    cf_sheet = 'CF ARGO REAS'
                    rafm_sheet = 'RAFM Output REAS'
                    manual_sheet = 'RAFM Output Manual'

                    cf_df = result[cf_sheet]
                    rafm_df = result[rafm_sheet]
                    manual_df = result[manual_sheet]
                    max_col_cf = xl_col_to_name(cf_df.shape[1] - 1)
                    max_col_rafm = xl_col_to_name(rafm_df.shape[1] - 1)
                    max_col_manual = xl_col_to_name(manual_df.shape[1] - 1)

                    max_row_cf = cf_df.shape[0] + 1
                    max_row_rafm = rafm_df.shape[0] + 1
                    max_row_manual = manual_df.shape[0] + 1

                    for row_idx in range(1, nrows):
                        for col_idx in range(3, ncols): 
                            col_letter = xl_col_to_name(col_idx)
                            col_header = f"{col_letter}$1"
                            row_b = f"$B{row_idx+1}"
                            row_c = f"$C{row_idx+1}"

                            formula = (
                                f"=IFERROR(INDEX('{cf_sheet}'!$C$2:${max_col_cf}${max_row_cf}, MATCH({row_b}, '{cf_sheet}'!$B$2:$B${max_row_cf}, 0), MATCH({col_header}, '{cf_sheet}'!$C$1:${max_col_cf}$1, 0)),0)"
                                f"-IFERROR(INDEX('{rafm_sheet}'!$C$2:${max_col_rafm}${max_row_rafm}, MATCH({row_c}, '{rafm_sheet}'!$B$2:$B${max_row_rafm}, 0), MATCH({col_header}, '{rafm_sheet}'!$C$1:${max_col_rafm}$1, 0)),0)"
                                f"-IFERROR(INDEX('{manual_sheet}'!$C$2:${max_col_manual}${max_row_manual}, MATCH({row_c}, '{manual_sheet}'!$B$2:$B${max_row_manual}, 0), MATCH({col_header}, '{manual_sheet}'!$C$1:${max_col_manual}$1, 0)),0)"
                            )
                            worksheet.write_formula(row_idx, col_idx, formula)


    print(f"✅ Output disimpan di: {output_file}")
    print("📋 Sheet yang diekspor:")
    for sheet in result:
        print(f"   - {sheet}")


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

    print(f"🔧 Memproses {len(files)} file secara paralel...\n")

    if len(files) == 1:
        process_input_file(files[0])
    else:
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(process_input_file, f) for f in files]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ Error saat memproses file: {e}")

    end_time = time.time()
    print(f"\n⏲️ Total waktu proses: {end_time - start_time:.2f} detik")