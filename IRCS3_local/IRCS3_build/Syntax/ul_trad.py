import pandas as pd
import re
import os
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import warnings
warnings.filterwarnings('ignore')

def parse_numeric_fast(val):
    if val is None or val == '':
        return None
    if isinstance(val, (int, float)):
        return float(val)

    if isinstance(val, str):
        s = val.strip()
        if not s or s.lower() in ['none', 'nan', 'n/a', '-', '--']:
            return None
        s = s.replace('\xa0', '').replace(' ', '').replace('\u202f', '')
        s = s.replace('−', '-')
        s = re.sub(r'[^\d,\.\-()%]', '', s)
        is_percent = s.endswith('%')
        if is_percent:
            s = s[:-1]
        is_negative = False
        if s.startswith('(') and s.endswith(')'):
            is_negative = True
            s = s[1:-1]

        comma_count = s.count(',')
        dot_count = s.count('.')

        try:
            if comma_count > 1 and dot_count == 1 and s.rfind('.') > s.rfind(','):
                result = float(s.replace(',', ''))
            elif comma_count == 1 and dot_count > 0 and s.rfind(',') > s.rfind('.'):
                result = float(s.replace('.', '').replace(',', '.'))
            elif dot_count == 0 and comma_count == 1:
                result = float(s.replace(',', '.'))
            elif dot_count > 1 and comma_count == 0:
                result = float(s.replace('.', ''))
            elif comma_count == 0 and dot_count <= 1:
                result = float(s)
            else:
                result = float(s.replace(',', '').replace('.', ''))

            if is_negative:
                result = -result
            if is_percent:
                result /= 100.0
            return result
        except ValueError:
            return None

    try:
        return float(val)
    except:
        return None

def clean_numeric_column(df, column_name):
    df_processed, _ = make_columns_case_insensitive(df)
    column_lower = column_name.lower()

    if column_lower not in df_processed.columns:
        return df

    original_col = next((col for col in df.columns if col.lower() == column_lower), None)
    if not original_col:
        return df

    col_data = df[original_col].astype(str).to_list()

    with ThreadPoolExecutor() as executor:
        parsed = list(executor.map(parse_numeric_fast, col_data))

    df[original_col] = np.nan_to_num(parsed, nan=0.0)
    return df

def make_columns_case_insensitive(df):
    if df is None or df.empty:
        return pd.DataFrame(), {}

    column_mapping = {}
    lowercase_cols = []
    
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in column_mapping:
            print(f"⚠️ Warning: Duplicate lowercase column detected: '{col}' conflicts with '{column_mapping[col_lower]}'")
        column_mapping[col_lower] = col
        lowercase_cols.append(col_lower)
    
    df_lower = df.copy()
    df_lower.columns = lowercase_cols
    
    return df_lower, column_mapping


def parse_multi_values(value):
    if pd.isna(value) or not value:
        return []
    parts = re.split(r'[,/]', str(value))
    return [p.strip() for p in parts if p.strip()]

def combine_filters(*args):
    combined = []
    for arg in args:
        combined.extend(arg)
    return combined

def apply_filters_dv(df, params):
    if df.empty:
        return df.copy()

    df_processed, column_mapping = make_columns_case_insensitive(df)
    
    produk_tertentu = combine_filters(
        parse_multi_values(params.get('only_channel', '')),
        parse_multi_values(params.get('only_currency', '')),
        parse_multi_values(params.get('only_portfolio', '')),
    )
    kecuali_produk = combine_filters(
        parse_multi_values(params.get('exclude_channel', '')),
        parse_multi_values(params.get('exclude_currency', '')),
        parse_multi_values(params.get('exclude_portfolio', '')),
    )

    only_cohort_list = parse_multi_values(params.get('only_cohort', ''))
    only_period_list = parse_multi_values(params.get('only_period', ''))

    tahun_tertentu = []
    if only_cohort_list and only_period_list:
        for c in only_cohort_list:
            for p in only_period_list:
                tahun_tertentu.append(f"{c}_{p}")
    elif only_cohort_list:
        tahun_tertentu.extend(only_cohort_list)
    elif only_period_list:
        tahun_tertentu.extend(only_period_list)

    exclude_cohort_list = parse_multi_values(params.get('exclude_cohort', ''))
    exclude_period_list = parse_multi_values(params.get('exclude_period', ''))

    kecuali_tahun = []
    if exclude_cohort_list and exclude_period_list:
        for c in exclude_cohort_list:
            for p in exclude_period_list:
                kecuali_tahun.append(f"{c}_{p}")
    elif exclude_cohort_list:
        kecuali_tahun.extend(exclude_cohort_list)
    elif exclude_period_list:
        kecuali_tahun.extend(exclude_period_list)

    mask = pd.Series(True, index=df_processed.index)

    goc_col = 'goc'
    if goc_col not in df_processed.columns:
        print(f"Warning: 'goc' column not found. Available columns: {df_processed.columns.tolist()}")
        return df.copy()

    if kecuali_tahun:
        pattern_exc = '|'.join(map(re.escape, kecuali_tahun))
        mask &= ~df_processed[goc_col].astype(str).str.contains(pattern_exc, case=False, na=False)

    if tahun_tertentu:
        pattern_inc = '|'.join(map(re.escape, tahun_tertentu))
        mask &= df_processed[goc_col].astype(str).str.contains(pattern_inc, case=False, na=False)

    if produk_tertentu:
        produk_mask = pd.Series(False, index=df_processed.index)
        for produk in produk_tertentu:
            pattern = rf'(^|_){re.escape(produk)}(_|$)'
            produk_mask |= df_processed[goc_col].astype(str).str.contains(pattern, case=False, na=False)
        mask &= produk_mask

    if kecuali_produk:
        for produk_exc in kecuali_produk:
            pattern = rf'(^|_){re.escape(produk_exc)}(_|$)'
            mask &= ~df_processed[goc_col].astype(str).str.contains(pattern, case=False, na=False)



    filtered_df = df_processed[mask].copy()
    filtered_df.columns = [column_mapping.get(col.lower(), col) for col in filtered_df.columns]
    return filtered_df

def apply_filters_rafm(df, params):
    if df.empty:
        return df.copy()

    df_processed, column_mapping = make_columns_case_insensitive(df)
    
    produk_tertentu = combine_filters(
        parse_multi_values(params.get('only_channel', '')),
        parse_multi_values(params.get('only_currency', '')),
        parse_multi_values(params.get('only_portfolio', '')),
    )
    kecuali_produk = combine_filters(
        parse_multi_values(params.get('exclude_channel', '')),
        parse_multi_values(params.get('exclude_currency', '')),
        parse_multi_values(params.get('exclude_portfolio', '')),
    )
    only_cohort_list = parse_multi_values(params.get('only_cohort', ''))
    exclude_cohort_list = parse_multi_values(params.get('exclude_cohort', ''))

    mask = pd.Series(True, index=df_processed.index)

    goc_col = 'goc'
    if goc_col not in df_processed.columns:
        print(f"Warning: 'goc' column not found. Available columns: {df_processed.columns.tolist()}")
        return df.copy()

    if only_cohort_list:
        pattern_inc = '|'.join(map(re.escape, only_cohort_list))
        mask &= df_processed[goc_col].astype(str).str.contains(pattern_inc, case=False, na=False)

    if exclude_cohort_list:
        pattern_exc = '|'.join(map(re.escape, exclude_cohort_list))
        mask &= ~df_processed[goc_col].astype(str).str.contains(pattern_exc, case=False, na=False)

    if produk_tertentu:
        produk_mask = pd.Series(False, index=df_processed.index)
        for produk in produk_tertentu:
            pattern = rf'(^|_){re.escape(produk)}(_|$)'
            produk_mask |= df_processed[goc_col].astype(str).str.contains(pattern, case=False, na=False)
        mask &= produk_mask

    if kecuali_produk:
        for produk_exc in kecuali_produk:
            pattern = rf'(^|_){re.escape(produk_exc)}(_|$)'
            mask &= ~df_processed[goc_col].astype(str).str.contains(pattern, case=False, na=False)



    filtered_df = df_processed[mask].copy()
    filtered_df.columns = [column_mapping.get(col.lower(), col) for col in filtered_df.columns]
    return filtered_df


def filter_goc_by_code(df, code):
    if df.empty:
        return df
    
    df_processed, _ = make_columns_case_insensitive(df)
    goc_col = 'goc'
    
    if goc_col not in df_processed.columns:
        return df
    
    mask = df_processed[goc_col].str.contains(code, case=False, na=False)
    return df[mask].copy()


def exclude_goc_by_code(df, code):
    """Exclude dataframe by GOC code with case-insensitive column handling"""
    if df.empty:
        return df
    
    df_processed, _ = make_columns_case_insensitive(df)
    goc_col = 'goc'
    
    if goc_col not in df_processed.columns:
        return df
        
    tokens = [k for k in code.split('_') if k]
    mask = df_processed[goc_col].apply(lambda x: all(token.lower() in str(x).lower() for token in tokens))
    
    return df[~mask].copy()

def load_excel_sheet_safely(file_path, sheet_name, required_columns=None, return_column_mapping=False):
    try:
        if not file_path or not os.path.exists(file_path):
            print(f"⚠️ File not found: {file_path}")
            return (pd.DataFrame(), {}) if return_column_mapping else pd.DataFrame()
        
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')

        column_mapping = {col.lower(): col for col in df.columns}

        if required_columns:
            df_columns_lower = [col.lower() for col in df.columns]
            required_lower = [col.lower() for col in required_columns]

            missing_cols = [col for col in required_lower if col not in df_columns_lower]
            if missing_cols:
                print(f"⚠️ Missing columns {missing_cols} in {sheet_name}")
                return (pd.DataFrame(), {}) if return_column_mapping else pd.DataFrame()

            selected_columns = [column_mapping[col.lower()] for col in required_columns]
            df = df[selected_columns]

        df.columns = [col.lower() for col in df.columns]

        return (df, column_mapping) if return_column_mapping else df

    except Exception as e:
        print(f"❌ Error loading {sheet_name} from {file_path}: {str(e)}")
        return (pd.DataFrame(), {}) if return_column_mapping else pd.DataFrame()


def run_trad(params):
    try:
        path_dv = params.get('path_dv', '')
        path_rafm = params.get('path_rafm', '')
        if not path_dv or not os.path.isfile(path_dv):
            return {"error": f"File DV tidak ditemukan atau path kosong: {path_dv}"}
        if not path_rafm or not os.path.isfile(path_rafm):
            return {"error": f"File RAFM tidak ditemukan atau path kosong: {path_rafm}"}

        try:
            dv_trad = pd.read_csv(path_dv)
        except:
            try:
                dv_trad = pd.read_excel(path_dv, engine='openpyxl')
            except Exception as e:
                return {"error": f"Gagal membaca file DV: {str(e)}"}
                
        dv_trad_processed, dv_column_mapping = make_columns_case_insensitive(dv_trad)
        
        dv_trad_total = apply_filters_dv(dv_trad_processed, params)
        
        columns_to_drop = ['product_group', 'pre_ann', 'loan_sa']
        columns_to_drop_lower = [col.lower() for col in columns_to_drop]
        
        existing_columns_to_drop = []
        for col in dv_trad_total.columns:
            if col.lower() in columns_to_drop_lower:
                existing_columns_to_drop.append(col)
        
        dv_trad_total = dv_trad_total.drop(columns=existing_columns_to_drop, errors='ignore')

        goc_column = None
        for col in dv_trad_total.columns:
            if col.lower() == 'goc':
                goc_column = col
                break
        
        if not goc_column:
            return {"error": "GOC column not found in DV data"}

        def get_sortir(params):
            def sortir(name):
                if not isinstance(name, str) or not name:
                    return ''

                def remove_trailing_q_and_if(parts):
                    while parts and (re.fullmatch(r'Q\d+', parts[-1], re.IGNORECASE) or parts[-1].upper() == 'IF'):
                        parts.pop()
                    return parts

                only_cohort = parse_multi_values(params.get('only_cohort', ''))
                only_period = parse_multi_values(params.get('only_period', ''))
                tahun_tertentu = [f"{c}_{p}" for c in only_cohort for p in only_period]

                if '____' in name:
                    double_underscore_parts = name.split('____')
                    if len(double_underscore_parts) > 1:
                        after_double = double_underscore_parts[-1]
                        after_parts = [p for p in after_double.split('_') if p]

                        year_index_after = -1
                        for i, part in enumerate(after_parts):
                            if re.fullmatch(r'\d{4}', part):
                                year_index_after = i
                                break

                        if year_index_after == -1:
                            return ''

                        if tahun_tertentu and any('Q' in t.upper() or 'IF' in t.upper() for t in tahun_tertentu):
                            filtered_parts = remove_trailing_q_and_if(after_parts[:year_index_after + 1])
                            return '_'.join(filtered_parts)

                        return '_'.join(after_parts[:year_index_after + 1])

                parts = [p for p in name.split('_') if p]
                year_index = -1
                for i, part in enumerate(parts):
                    if re.fullmatch(r'\d{4}', part):
                        year_index = i
                        break

                start_index = next((i for i, part in enumerate(parts) if part == 'AG'), 2)

                if year_index == -1:
                    return ''

                if tahun_tertentu and any('Q' in t.upper() or 'IF' in t.upper() for t in tahun_tertentu):
                    filtered_parts = remove_trailing_q_and_if(parts[start_index:year_index + 1])
                    return '_'.join(filtered_parts)

                return '_'.join(parts[start_index:year_index + 1])

            return sortir
        sortir_func = get_sortir(params)
        dv_trad_total[goc_column] = dv_trad_total[goc_column].apply(sortir_func)
        dv_trad_total[goc_column] = dv_trad_total[goc_column].apply(lambda x: 'H_IDR_NO_2025' if x == 'IDR_NO_2025' else x)

        with ThreadPoolExecutor() as executor:
            executor.map(lambda col: clean_numeric_column(dv_trad_total, col), ['pol_num', 'sum_assd'])

        dv_trad_total = dv_trad_total.groupby([goc_column], as_index=False).sum(numeric_only=True)

        params_lower = {k.lower(): v for k, v in params.items()}

        if 'usdidr' not in params_lower:
            print("❌ Parameter 'usdidr' tidak ditemukan dalam input")
        else:
            usd_rate = (params_lower['usdidr'])
            if isinstance(usd_rate, (np.ndarray, pd.Series)):
                usd_rate = usd_rate.astype(float)
            elif isinstance(usd_rate, str):
                usd_rate = float(usd_rate)

        sum_assd_column = None
        for col in dv_trad_total.columns:
            if col.lower() == 'sum_assd':
                sum_assd_column = col
                break
        
        if sum_assd_column:
            usd_mask = dv_trad_total[goc_column].astype(str).str.contains("USD", case=False, na=False)
            dv_trad_total.loc[usd_mask, sum_assd_column] = pd.to_numeric(
                dv_trad_total.loc[usd_mask, sum_assd_column], errors='coerce'
            )
            dv_trad_total.loc[usd_mask, sum_assd_column] *= usd_rate

        run_rafm_idr = load_excel_sheet_safely(path_rafm, 'extraction_IDR', ['GOC', 'period', 'cov_units', 'pol_b'])
        run_rafm_usd = load_excel_sheet_safely(path_rafm, 'extraction_USD', ['GOC', 'period', 'cov_units', 'pol_b'])

        if not run_rafm_idr.empty:
            run_rafm_idr = run_rafm_idr[run_rafm_idr['period'].astype(str) == '0']
            run_rafm_idr = run_rafm_idr.drop(columns=["period"])
        
        if not run_rafm_usd.empty:
            run_rafm_usd = run_rafm_usd[run_rafm_usd['period'].astype(str) == '0']
            run_rafm_usd = run_rafm_usd.drop(columns=["period"])

        run_rafm_only = pd.concat([run_rafm_idr, run_rafm_usd], ignore_index=True)
        run_rafm_only = apply_filters_rafm(run_rafm_only, params)
        if not run_rafm_only.empty:
            with ThreadPoolExecutor() as executor:
                executor.map(lambda col: clean_numeric_column(run_rafm_only, col), ['pol_b', 'cov_units'])
            goc_col_rafm = None
            for col in run_rafm_only.columns:
                if col.lower() == 'goc':
                    goc_col_rafm = col
                    break
            
            if goc_col_rafm:
                run_rafm = run_rafm_only.rename(columns={goc_col_rafm: goc_column})
            else:
                run_rafm = run_rafm_only.copy()
            
            merged = pd.merge(dv_trad_total, run_rafm, on=goc_column, how="outer")
            merged_cols = merged.columns.tolist()
            merged_cols[3], merged_cols[4] = merged_cols[4], merged_cols[3]
            merged = merged[merged_cols]
        else:
            merged = dv_trad_total.copy()
            merged['pol_b'] = 0
            merged['cov_units'] = 0

        merged.fillna(0, inplace=True)
        
        pol_num_col = None
        sum_assd_col = None
        for col in merged.columns:
            if col.lower().startswith('pol_num'):
                pol_num_col = col
            elif col.lower().startswith('sum_assd'):
                sum_assd_col = col
        
        if pol_num_col and 'pol_b' in merged.columns:
            merged['diff_policies'] = merged[pol_num_col] - merged['pol_b']
        if sum_assd_col and 'cov_units' in merged.columns:
            merged['diff_sa'] = merged[sum_assd_col] - merged['cov_units']

        tabel_total_l = filter_goc_by_code(merged, 'l')
        tabel_total_l = tabel_total_l[~tabel_total_l[goc_column].astype(str).str.contains("%", case=False, na=False)]

        def safe_sum(df, col_name):
            for col in df.columns:
                if col.lower() == col_name.lower():
                    return df[col].sum()
            return 0

        summary = pd.DataFrame({
            'DV_Policies': [
                safe_sum(dv_trad_total, 'pol_num'),
            ],
            'DV_SA': [
                safe_sum(dv_trad_total, 'sum_assd'),
            ],
            'RAFM_Policies': [
                safe_sum(merged, 'pol_b'),
            ],
            'RAFM_SA': [
                safe_sum(merged, 'cov_units'),
            ],
            'Diff_Policies': [
                safe_sum(dv_trad_total, 'pol_num') - safe_sum(merged, 'pol_b'),
            ],
            'Diff_SA': [
                safe_sum(dv_trad_total, 'sum_assd') - safe_sum(merged, 'cov_units'),
            ]
        })
        # TABEL 2: CC%
        tabel_2 = filter_goc_by_code(merged, 'CC%')
    
        # TABEL 3: H_IDR_NO
        produk_tertentu_list = combine_filters(
            parse_multi_values(params.get('only_channel', '')),
            parse_multi_values(params.get('only_currency', '')),
            parse_multi_values(params.get('only_portfolio', '')),
        )

        filter_code = 'H_IDR_NO'
        if any(p.upper() in ['SN', 'SI'] for p in produk_tertentu_list):
            filter_code = 'H_IDR'
        tabel_3 = filter_goc_by_code(merged, filter_code)
        if not tabel_3.empty:
            tabel_3_processed = tabel_3.copy()
            tabel_3_processed[goc_column] = tabel_3_processed[goc_column].apply(
                lambda x: '_'.join(str(x).split('_')[0:4]) if str(x).startswith(filter_code) else x
            )
            tabel_3_processed = tabel_3_processed.groupby([goc_column], as_index=False).sum(numeric_only=True)
            tabel_3 = tabel_3_processed

        # TABEL 4: YR
        tabel_4 = filter_goc_by_code(merged, 'YR')
        if not tabel_4.empty:
            tabel_4_processed = tabel_4.copy()
            tabel_4_processed[goc_column] = tabel_4_processed[goc_column].apply(
                lambda x: '_'.join(str(x).split('_')[1:5])
            )
            tabel_4_processed = tabel_4_processed.groupby([goc_column], as_index=False).sum(numeric_only=True)
            tabel_4 = tabel_4_processed

        # TABEL 5: _C_
        tabel_5 = filter_goc_by_code(merged, '_C_')
        if not tabel_5.empty:
            tabel_5_processed = tabel_5.copy()
            tabel_5_processed[goc_column] = tabel_5_processed[goc_column].apply(
                lambda x: '_'.join(str(x).split('_')[1:5])
            )
            tabel_5_processed = tabel_5_processed.groupby([goc_column], as_index=False).sum(numeric_only=True)
            tabel_5 = tabel_5_processed
        return {
            'product_type': 'TRAD',
            'tabel_total': tabel_total_l,
            'tabel_2': tabel_2,
            'tabel_3': tabel_3,
            'tabel_4': tabel_4,
            'tabel_5': tabel_5,
            'summary_total': summary,
            'run_name': params.get('run_name', params.get('run', ''))
        }
    
    except Exception as e:
        return {"error": f"Error in run_trad: {str(e)}"}

def run_ul(params):
    try:
        path_dv = params.get('path_dv', '')
        path_rafm = params.get('path_rafm', '')
        path_uvsg = params.get('path_uvsg', '')
        
        if not path_dv or not os.path.isfile(path_dv):
            return {"error": f"File DV tidak ditemukan atau path kosong: {path_dv}"}
        if not path_rafm or not os.path.isfile(path_rafm):
            return {"error": f"File RAFM tidak ditemukan atau path kosong: {path_rafm}"}

        # Load DV data
        try:
            dv_ul = pd.read_csv(path_dv)
        except:
            try:
                dv_ul = pd.read_excel(path_dv, engine='openpyxl')
            except Exception as e:
                return {"error": f"Gagal membaca file DV: {str(e)}"}
        
        if dv_ul.empty:
            return {"error": "File DV kosong atau tidak dapat dibaca"}
        
        dv_ul_processed, dv_column_mapping = make_columns_case_insensitive(dv_ul)
 
        dv_ul_total = apply_filters_dv(dv_ul_processed, params)
        columns_to_drop = ['product_group', 'pre_ann', 'sum_assur']
        columns_to_drop_lower = [col.lower() for col in columns_to_drop]
        
        existing_columns_to_drop = []
        for col in dv_ul_total.columns:
            if col.lower() in columns_to_drop_lower:
                existing_columns_to_drop.append(col)
        
        dv_ul_total = dv_ul_total.drop(columns=existing_columns_to_drop, errors='ignore')

        goc_column = None
        for col in dv_ul_total.columns:
            if col.lower() == 'goc':
                goc_column = col
                break
        
        if not goc_column:
            return {"error": "GOC column not found in DV data"}

        def sortir(name):
            if not isinstance(name, str) or not name:
                return ''
            parts = [p for p in str(name).split('_') if p]
            year_index = -1
            for i, part in enumerate(parts):
                if re.fullmatch(r'\d{4}', part):
                    year_index = i
                    break
            if year_index == -1:
                return ''
            start_index = None
            for i, part in enumerate(parts):
                if part == 'AG':
                    start_index = i
                    break
            if start_index is None:
                start_index = 2
            return '_'.join(parts[start_index:year_index+1])

        dv_ul_total[goc_column] = dv_ul_total[goc_column].apply(sortir)
        with ThreadPoolExecutor() as executor:
            executor.map(lambda col: clean_numeric_column(dv_ul_total, col), ['pol_num', 'total_fund'])

        dv_ul_total = dv_ul_total.groupby([goc_column], as_index=False).sum(numeric_only=True)

        params_lower = {k.lower(): v for k, v in params.items()}

        if 'usdidr' not in params_lower:
            print("❌ Parameter 'usdidr' tidak ditemukan dalam input")
        else:
            usd_rate = (params_lower['usdidr'])
            if isinstance(usd_rate, (np.ndarray, pd.Series)):
                usd_rate = usd_rate.astype(float)
            elif isinstance(usd_rate, str):
                usd_rate = float(usd_rate)
        total_fund_column = None
        for col in dv_ul_total.columns:
            if col.lower() == 'total_fund':
                total_fund_column = col
                break
        
        if total_fund_column:
            usd_mask = dv_ul_total[goc_column].astype(str).str.contains("USD", case=False, na=False)
            dv_ul_total.loc[usd_mask, total_fund_column] = pd.to_numeric(
                dv_ul_total.loc[usd_mask, total_fund_column], errors='coerce'
            )
            dv_ul_total.loc[usd_mask, total_fund_column] *= usd_rate

        run_rafm_idr = load_excel_sheet_safely(path_rafm, 'extraction_IDR', ['GOC', 'period', 'pol_b', 'RV_AV_IF'])
        run_rafm_usd = load_excel_sheet_safely(path_rafm, 'extraction_USD', ['GOC', 'period', 'pol_b', 'RV_AV_IF'])
        
        if not run_rafm_idr.empty:
            run_rafm_idr = run_rafm_idr[run_rafm_idr['period'].astype(str) == '0']
            run_rafm_idr = run_rafm_idr.drop(columns=["period"])
        
        if not run_rafm_usd.empty:
            run_rafm_usd = run_rafm_usd[run_rafm_usd['period'].astype(str) == '0']
            run_rafm_usd = run_rafm_usd.drop(columns=["period"])

        run_rafm_only = pd.concat([run_rafm_idr, run_rafm_usd], ignore_index=True)
        run_rafm_only = apply_filters_rafm(run_rafm_only, params)
        if not run_rafm_only.empty:
            with ThreadPoolExecutor() as executor:
                executor.map(lambda col: clean_numeric_column(run_rafm_only, col), ['pol_b', 'rv_av_if'])
            goc_col_rafm = None
            for col in run_rafm_only.columns:
                if col.lower() == 'goc':
                    goc_col_rafm = col
                    break
            
            if goc_col_rafm and goc_col_rafm != goc_column:
                run_rafm_only = run_rafm_only.rename(columns={goc_col_rafm: goc_column})

        run_rafm_no_gs = run_rafm_only[~run_rafm_only[goc_column].astype(str).str.contains('GS', case=False, na=False)] if not run_rafm_only.empty else pd.DataFrame()

        run_uvsg = pd.DataFrame()
        if path_uvsg and os.path.isfile(path_uvsg):
            print(f"Loading UVSG file: {path_uvsg}")
            run_uvsg_idr = load_excel_sheet_safely(path_uvsg, 'extraction_IDR', ['GOC', 'period', 'pol_b', 'rv_av_if'])
            run_uvsg_usd = load_excel_sheet_safely(path_uvsg, 'extraction_USD', ['GOC', 'period', 'pol_b', 'rv_av_if'])
            
            if not run_uvsg_idr.empty:
                run_uvsg_idr = run_uvsg_idr[run_uvsg_idr['period'].astype(str) == '0']
                run_uvsg_idr = run_uvsg_idr.drop(columns=["period"])
            
            if not run_uvsg_usd.empty:
                run_uvsg_usd = run_uvsg_usd[run_uvsg_usd['period'].astype(str) == '0']
                run_uvsg_usd = run_uvsg_usd.drop(columns=["period"])

            run_uvsg = pd.concat([run_uvsg_idr, run_uvsg_usd], ignore_index=True)
            if not run_uvsg.empty:
                with ThreadPoolExecutor() as executor:
                    executor.map(lambda col: clean_numeric_column(run_uvsg, col), ['pol_b', 'rv_av_if'])                
                
                # Find and standardize GOC column in UVSG
                goc_col_uvsg = None
                for col in run_uvsg.columns:
                    if col.lower() == 'goc':
                        goc_col_uvsg = col
                        break
                
                if goc_col_uvsg and goc_col_uvsg != goc_column:
                    run_uvsg = run_uvsg.rename(columns={goc_col_uvsg: goc_column})
        else:
            print("UVSG file not provided or not found - skipping UVSG processing")
        run_rafm = pd.concat([run_rafm_no_gs, run_uvsg], ignore_index=True) if not run_rafm_no_gs.empty or not run_uvsg.empty else pd.DataFrame()

        if not run_rafm.empty:
            merged = pd.merge(dv_ul_total, run_rafm, on=goc_column, how="outer")
        else:
            merged = dv_ul_total.copy()
            merged['pol_b'] = 0
            merged['rv_av_if'] = 0

        merged.fillna(0, inplace=True)
        def safe_get_col(df, col_name):
            for col in df.columns:
                if col.lower() == col_name.lower():
                    return col
            return None

        pol_num_col = safe_get_col(merged, 'pol_num')
        total_fund_col = safe_get_col(merged, 'total_fund')
        pol_b_col = safe_get_col(merged, 'pol_b')
        rv_av_if_col = safe_get_col(merged, 'rv_av_if')

        if not pol_num_col or not pol_b_col:
            return {"error": "Kolom pol_num atau pol_b tidak ditemukan"}
        if not total_fund_col or not rv_av_if_col:
            return {"error": "Kolom total_fund atau rv_av_if tidak ditemukan"}

        merged['diff_policies'] = merged[pol_num_col] - merged[pol_b_col]
        merged['diff_fund_value'] = merged[total_fund_col] - merged[rv_av_if_col]

        essential_columns = [goc_column, pol_num_col, total_fund_col, pol_b_col, rv_av_if_col, 'diff_policies', 'diff_fund_value']
        merged = merged[essential_columns]

        tabel_total_l = exclude_goc_by_code(merged, 'gs')

        def safe_sum(df, col_name):
            for col in df.columns:
                if col.lower() == col_name.lower():
                    return df[col].sum()
            return 0
        summary = pd.DataFrame({
            'DV # of Policies': [
                safe_sum(dv_ul_total, 'pol_num'),
            ],
            'DV Fund Value': [
                safe_sum(dv_ul_total, 'total_fund'),
            ],
            'RAFM # of Policies': [
                safe_sum(run_rafm, 'pol_b'),
            ],
            'RAFM Fund Value': [
                safe_sum(run_rafm, 'rv_av_if'),
            ],
            'Diff # of Policies': [
                safe_sum(dv_ul_total, 'pol_num') - safe_sum(run_rafm, 'pol_b'),
            ],
            'Diff Fund Value': [
                safe_sum(dv_ul_total, 'total_fund') - safe_sum(run_rafm, 'rv_av_if'),
            ]
        })

        # TABEL 2: GS (Group Savings)
        tabel_2 = pd.DataFrame()
        
        dv_gs = filter_goc_by_code(dv_ul_total, 'GS')
        rafm_gs = filter_goc_by_code(run_rafm_only, 'GS') if not run_rafm_only.empty else pd.DataFrame()

        if not dv_gs.empty or not rafm_gs.empty:
            tabel_2 = pd.merge(dv_gs, rafm_gs, on=goc_column, how="outer", suffixes=("", "_rafm"))
            tabel_2.fillna(0, inplace=True)

            pol_num_gs = safe_get_col(tabel_2, 'pol_num')
            total_fund_gs = safe_get_col(tabel_2, 'total_fund')
            pol_b_gs = safe_get_col(tabel_2, 'pol_b')
            rv_av_if_gs = safe_get_col(tabel_2, 'rv_av_if')

            if pol_num_gs and pol_b_gs:
                tabel_2['diff_policies'] = tabel_2[pol_num_gs] - tabel_2[pol_b_gs]
            else:
                tabel_2['diff_policies'] = 0
                
            if total_fund_gs and rv_av_if_gs:
                tabel_2['diff_fund_value'] = tabel_2[total_fund_gs] - tabel_2[rv_av_if_gs]
            else:
                tabel_2['diff_fund_value'] = 0

            tabel_2_essential = [goc_column, pol_num_gs, total_fund_gs, pol_b_gs, rv_av_if_gs, 'diff_policies', 'diff_fund_value']
            tabel_2_essential = [col for col in tabel_2_essential if col is not None and col in tabel_2.columns]
            tabel_2 = tabel_2[tabel_2_essential]

        return {
            'product_type': 'UL',
            'tabel_total': tabel_total_l,
            'tabel_2': tabel_2,
            'summary_total': summary,
            'run_name': params.get('run_name', params.get('run', ''))
        }
        
    except Exception as e:
        import traceback
        error_msg = f"Error in run_ul: {str(e)}\nTraceback: {traceback.format_exc()}"
        print(error_msg)
        return {"error": error_msg}