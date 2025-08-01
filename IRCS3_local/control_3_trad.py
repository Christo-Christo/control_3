import pandas as pd
import re
import os
#update

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

def load_runs_from_excel(file_path):
    df = pd.read_excel(file_path, sheet_name='FILTER_TRAD', engine='openpyxl')
    df.columns = df.columns.str.strip()
    runs = []
    for _, row in df.iterrows():
        run = {
            'run_name': row.get('run_name', ''),
            'path_dv': row.get('path_dv', ''),
            'path_rafm': row.get('path_rafm', ''),
            'USDIDR': row.get('USDIDR', 1.0),
            'only_channel': row.get('only_channel', ''),
            'exclude_channel': row.get('exclude_channel', ''),
            'only_currency': row.get('only_currency', ''),
            'exclude_currency': row.get('exclude_currency', ''),
            'only_portfolio': row.get('only_portfolio', ''),
            'exclude_portfolio': row.get('exclude_portfolio', ''),
            'only_cohort': row.get('only_cohort', ''),
            'exclude_cohort': row.get('exclude_cohort', ''),
            'only_period': row.get('only_period', ''),
            'exclude_period': row.get('exclude_period', ''),
            'tabel_2_aktif': row.get('tabel_2_aktif', True),
            'tabel_3_aktif': row.get('tabel_3_aktif', True),
            'tabel_4_aktif': row.get('tabel_4_aktif', True),
            'tabel_5_aktif': row.get('tabel_5_aktif', True),
        }
        runs.append(run)
    return runs

def apply_filters(df, params):
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
    for c in only_cohort_list:
        for p in only_period_list:
            tahun_tertentu.append(f"{c}_{p}")
    exclude_cohort_list = parse_multi_values(params.get('exclude_cohort', ''))
    exclude_period_list = parse_multi_values(params.get('exclude_period', ''))
    kecuali_tahun = []
    for c in exclude_cohort_list:
        for p in exclude_period_list:
            kecuali_tahun.append(f"{c}_{p}")

    mask = pd.Series(True, index=df.index)

    if kecuali_tahun:
        pattern_exc = '|'.join(map(re.escape, kecuali_tahun))
        mask &= ~df['goc'].astype(str).str.contains(pattern_exc, case=False, na=False)
    if tahun_tertentu:
        pattern_inc = '|'.join(map(re.escape, tahun_tertentu))
        mask &= df['goc'].astype(str).str.contains(pattern_inc, case=False, na=False)
    if produk_tertentu:
        produk_mask = pd.Series(False, index=df.index)
        for produk in produk_tertentu:
            produk_mask |= df['goc'].astype(str).str.contains(re.escape(produk), case=False, na=False)
        mask &= produk_mask
    if kecuali_produk:
        for produk_exc in kecuali_produk:
            mask &= ~df['goc'].astype(str).str.contains(re.escape(produk_exc), case=False, na=False)

    return df[mask].copy(), tahun_tertentu

def filter_goc_by_code(df, code):
    if df.empty:
        return df
    tokens = [k for k in code.split('_') if k]
    mask = df['goc'].apply(lambda x: all(token.lower() in str(x).lower() for token in tokens))
    return df[mask].copy()

def main(params):
    path_dv = params.get('path_dv', '')
    path_rafm = params.get('path_rafm', '')
    if not os.path.isfile(path_dv):
        raise FileNotFoundError(f"File DV tidak ditemukan: {path_dv}")
    if not os.path.isfile(path_rafm):
        raise FileNotFoundError(f"File RAFM tidak ditemukan: {path_rafm}")

    dv_trad = pd.read_csv(path_dv)
    dv_trad_total, tahun_tertentu = apply_filters(dv_trad, params)
    dv_trad_total = dv_trad_total.drop(columns=['product_group','pre_ann','loan_sa'], errors='ignore')

    def get_sortir(tahun_tertentu):
        def sortir(name):
            if not isinstance(name, str) or not name:
                return ''
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
                    if tahun_tertentu and any('Q1' in t.upper() for t in tahun_tertentu):
                        return after_double
                    if year_index_after == -1:
                        return ''
                    return '_'.join(after_parts[:year_index_after + 1])
            parts = [p for p in name.split('_') if p]
            year_index = -1
            for i, part in enumerate(parts):
                if re.fullmatch(r'\d{4}', part):
                    year_index = i
                    break
            start_index = next((i for i, part in enumerate(parts) if part == 'AG'), 2)
            if tahun_tertentu and any('Q1' in t.upper() for t in tahun_tertentu):
                return '_'.join(parts[start_index:])
            if year_index == -1:
                return ''
            return '_'.join(parts[start_index:year_index + 1])
        return sortir

    sortir_func = get_sortir(tahun_tertentu)
    dv_trad_total['goc'] = dv_trad_total['goc'].apply(sortir_func)
    dv_trad_total['goc'] = dv_trad_total['goc'].apply(lambda x: 'H_IDR_NO_2025' if x == 'IDR_NO_2025' else x)

    for col in ['pol_num', 'sum_assd']:
        dv_trad_total[col] = pd.to_numeric(
            dv_trad_total[col].astype(str).str.replace(",", ".", regex=False),
            errors="coerce"
        )

    dv_trad_total = dv_trad_total.groupby(["goc"], as_index=False).sum(numeric_only=True)

    usd_rate = float(params.get('USDIDR', 1.0))
    usd_mask = dv_trad_total["goc"].astype(str).str.contains("USD", case=False, na=False)
    dv_trad_total.loc[usd_mask, 'sum_assd'] = dv_trad_total.loc[usd_mask, 'sum_assd'] * usd_rate

    # Load RAFM sheets safely
    try:
        run_rafm_idr = pd.read_excel(path_rafm, sheet_name='extraction_IDR', engine='openpyxl')
        run_rafm_idr = run_rafm_idr[['GOC', 'period', 'cov_units', 'pol_b']]
        run_rafm_idr = run_rafm_idr[run_rafm_idr['period'].astype(str) == '0']
        run_rafm_idr = run_rafm_idr.drop(columns=["period"])
    except Exception:
        run_rafm_idr = pd.DataFrame()

    try:
        run_rafm_usd = pd.read_excel(path_rafm, sheet_name='extraction_USD', engine='openpyxl')
        run_rafm_usd = run_rafm_usd[['GOC', 'period', 'cov_units', 'pol_b']]
        run_rafm_usd = run_rafm_usd[run_rafm_usd['period'].astype(str) == '0']
        run_rafm_usd = run_rafm_usd.drop(columns=["period"])
    except Exception:
        run_rafm_usd = pd.DataFrame()

    run_rafm_only = pd.concat([run_rafm_idr, run_rafm_usd], ignore_index=True)
    if not run_rafm_only.empty:
        for col in ['pol_b', 'cov_units']:
            run_rafm_only[col] = pd.to_numeric(
                run_rafm_only[col].astype(str).str.replace(",", ".", regex=False), errors="coerce"
            )
        run_rafm = run_rafm_only.rename(columns={'GOC': 'goc'})
        merged = pd.merge(dv_trad_total, run_rafm, on="goc", how="outer", suffixes=("_trad", "_rafm"))
    else:
        merged = dv_trad_total.copy()
        merged['pol_b'] = 0
        merged['cov_units'] = 0

    merged.fillna(0, inplace=True)
    merged['diff policies'] = merged['pol_num'] - merged['pol_b']
    merged['diff sa'] = merged['sum_assd'] - merged['cov_units']

    # --- Table Total L
    tabel_total_l = filter_goc_by_code(merged, 'l')
    tabel_total_l = tabel_total_l[~tabel_total_l['goc'].astype(str).str.contains("%", case=False, na=False)]

    # --- Summary
    summary = pd.DataFrame({
        '': ['Total Trad All from DV', 'Grand Total Summary', 'Check'],
        'DV # of Policies': [
            dv_trad_total['pol_num'].sum(),
            tabel_total_l['pol_num'].sum(),
            dv_trad_total['pol_num'].sum() - tabel_total_l['pol_num'].sum()
        ],
        'DV SA': [
            dv_trad_total['sum_assd'].sum(),
            tabel_total_l['sum_assd'].sum(),
            dv_trad_total['sum_assd'].sum() - tabel_total_l['sum_assd'].sum()
        ],
        'RAFM # of Policies': [
            merged['pol_b'].sum(),
            tabel_total_l['pol_b'].sum(),
            merged['pol_b'].sum() - tabel_total_l['pol_b'].sum()
        ],
        'RAFM SA': [
            merged['cov_units'].sum(),
            tabel_total_l['cov_units'].sum(),
            merged['cov_units'].sum() - tabel_total_l['cov_units'].sum()
        ],
        'Diff # of Policies': [
            dv_trad_total['pol_num'].sum() - merged['pol_b'].sum(),
            tabel_total_l['pol_num'].sum() - tabel_total_l['pol_b'].sum(),
            (dv_trad_total['pol_num'].sum() - merged['pol_b'].sum()) - 
            (tabel_total_l['pol_num'].sum() - tabel_total_l['pol_b'].sum())
        ],
        'Diff SA': [
            dv_trad_total['sum_assd'].sum() - merged['cov_units'].sum(),
            tabel_total_l['sum_assd'].sum() - tabel_total_l['cov_units'].sum(),
            (dv_trad_total['sum_assd'].sum() - merged['cov_units'].sum()) - 
            (tabel_total_l['sum_assd'].sum() - tabel_total_l['cov_units'].sum())
        ]
    })

    # --- TABEL 2: CC%
    summary_tabel_2 = None
    if params.get('tabel_2_aktif', True):
        code_tabel_2 = 'CC%'
        tabel_2 = filter_goc_by_code(merged, code_tabel_2)
        dv_policies_tabel_2 = tabel_2['pol_num'].sum()
        dv_sa_tabel_2 = tabel_2['sum_assd'].sum()
        rafm_policies_tabel_2 = tabel_2['pol_b'].sum()
        rafm_sa_tabel_2 = tabel_2['cov_units'].sum()
        diff_policies_tabel_2 = dv_policies_tabel_2 - rafm_policies_tabel_2
        diff_sa_tabel_2 = dv_sa_tabel_2 - rafm_sa_tabel_2

        summary_tabel_2 = pd.DataFrame([{
            "DV": dv_policies_tabel_2,
            "DV SA": dv_sa_tabel_2,
            "RAFM Output": rafm_policies_tabel_2,
            "RAFM SA": rafm_sa_tabel_2,
            "Diff # of Policies": diff_policies_tabel_2,
            "Diff SA": diff_sa_tabel_2
        }])

    # --- TABEL 3: goc starts with H_IDR_NO
    summary_tabel_3 = None
    if params.get('tabel_3_aktif', True):
        code_tabel_3 = 'H_IDR_NO'
        tabel_3 = filter_goc_by_code(merged, code_tabel_3)
        if not tabel_3.empty:
            tabel_3 = tabel_3.copy()
            tabel_3['goc'] = tabel_3['goc'].apply(lambda x: '_'.join(str(x).split('_')[0:4]) if str(x).startswith('H_IDR_NO') else x)
            tabel_3 = tabel_3.groupby(['goc'], as_index=False).sum(numeric_only=True)

            dv_policies_tabel_3 = tabel_3['pol_num'].sum()
            dv_sa_tabel_3 = tabel_3['sum_assd'].sum()
            rafm_policies_tabel_3 = tabel_3['pol_b'].sum()
            rafm_sa_tabel_3 = tabel_3['cov_units'].sum()
            diff_policies_tabel_3 = dv_policies_tabel_3 - rafm_policies_tabel_3
            diff_sa_tabel_3 = dv_sa_tabel_3 - rafm_sa_tabel_3

            summary_tabel_3 = pd.DataFrame([{
                "DV": dv_policies_tabel_3,
                "DV SA": dv_sa_tabel_3,
                "RAFM Output": rafm_policies_tabel_3,
                "RAFM SA": rafm_sa_tabel_3,
                "Diff # of Policies": diff_policies_tabel_3,
                "Diff SA": diff_sa_tabel_3
            }])

        # --- TABEL 4: goc contains 'YR' ---
    if params.get('tabel_4_aktif', True):
        code_tabel_4 = 'YR'
        tabel_4 = filter_goc_by_code(merged, code_tabel_4)
        if not tabel_4.empty:
            tabel_4 = tabel_4.copy()
            tabel_4['goc'] = tabel_4['goc'].apply(lambda x: '_'.join(str(x).split('_')[1:5]))
            tabel_4 = tabel_4.groupby(['goc'], as_index=False).sum(numeric_only=True)

            dv_policies_tabel_4 = tabel_4['pol_num'].sum()
            dv_sa_tabel_4 = tabel_4['sum_assd'].sum()
            rafm_policies_tabel_4 = tabel_4['pol_b'].sum()
            rafm_sa_tabel_4 = tabel_4['cov_units'].sum()
            diff_policies_tabel_4 = dv_policies_tabel_4 - rafm_policies_tabel_4
            diff_sa_tabel_4 = dv_sa_tabel_4 - rafm_sa_tabel_4

            summary_tabel_4 = pd.DataFrame([{
                "DV": dv_policies_tabel_4,
                "DV SA": dv_sa_tabel_4,
                "RAFM Output": rafm_policies_tabel_4,
                "RAFM SA": rafm_sa_tabel_4,
                "Diff # of Policies": diff_policies_tabel_4,
                "Diff SA": diff_sa_tabel_4
            }])

    # --- TABEL 5: goc contains '_C_' ---
    if params.get('tabel_5_aktif', True):
        code_tabel_5 = '_C_'
        tabel_5 = filter_goc_by_code(merged, code_tabel_5)
        if not tabel_5.empty:
            tabel_5 = tabel_5.copy()
            tabel_5['goc'] = tabel_5['goc'].apply(lambda x: '_'.join(str(x).split('_')[1:5]))
            tabel_5 = tabel_5.groupby(['goc'], as_index=False).sum(numeric_only=True)

            dv_policies_tabel_5 = tabel_5['pol_num'].sum()
            dv_sa_tabel_5 = tabel_5['sum_assd'].sum()
            rafm_policies_tabel_5 = tabel_5['pol_b'].sum()
            rafm_sa_tabel_5 = tabel_5['cov_units'].sum()
            diff_policies_tabel_5 = dv_policies_tabel_5 - rafm_policies_tabel_5
            diff_sa_tabel_5 = dv_sa_tabel_5 - rafm_sa_tabel_5

            summary_tabel_5 = pd.DataFrame([{
                "DV": dv_policies_tabel_5,
                "DV SA": dv_sa_tabel_5,
                "RAFM Output": rafm_policies_tabel_5,
                "RAFM SA": rafm_sa_tabel_5,
                "Diff # of Policies": diff_policies_tabel_5,
                "Diff SA": diff_sa_tabel_5
            }])

    return {
        'tabel total':tabel_total_l,
        'tabel 2' : tabel_2,
        'tabel 3' : tabel_3,
        'tabel 4' : tabel_4,
        'tabel 5' : tabel_5,
        'summary_total': summary,
        'summary_tabel_2': summary_tabel_2,
        'summary_tabel_3': summary_tabel_3,
        'summary_tabel_4': summary_tabel_4,
        'summary_tabel_5': summary_tabel_5,
        'run_name': params.get('run_name', '')
    }

