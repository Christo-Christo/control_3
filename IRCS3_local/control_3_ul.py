import pandas as pd
import re
import time

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
    df = pd.read_excel(file_path, sheet_name='FILTER_UL', engine='openpyxl')
    df.columns = df.columns.str.strip()
    runs = []
    for _, row in df.iterrows():
        run = {
            'run_name': row.get('run_name', ''),
            'path_dv': row.get('path_dv', ''),
            'path_rafm': row.get('path_rafm', ''),
            'path_uvsg': row.get('path_uvsg', ''),
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
            'tabel_3_aktif': row.get('tabel_3_aktif', True)
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

    return df[mask].copy()

def main(params):
    dv_ul = pd.read_excel(params['path_dv'], engine='openpyxl')
    dv_ul_total = apply_filters(dv_ul, params)
    dv_ul_total = dv_ul_total.drop(columns=['product_group', 'pre_ann', 'sum_assur'], errors='ignore')

    def sortir(name):
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

    dv_ul_total['goc'] = dv_ul_total['goc'].apply(sortir)

    dv_ul_total["total_fund"] = (
        dv_ul_total["total_fund"].astype(str).str.replace(",", ".", regex=False)
    )
    dv_ul_total["total_fund"] = pd.to_numeric(dv_ul_total["total_fund"], errors="coerce")

    dv_ul_total = dv_ul_total.groupby(["goc"], as_index=False).sum(numeric_only=True)

    usd_rate = float(params.get('USDIDR', 1.0))
    usd_mask = dv_ul_total["goc"].astype(str).str.contains("USD", case=False, na=False)
    dv_ul_total.loc[usd_mask, 'total_fund'] = dv_ul_total.loc[usd_mask, 'total_fund'] * usd_rate

    try:
        run_rafm_idr = pd.read_excel(params['path_rafm'], sheet_name='extraction_IDR', engine='openpyxl')
        run_rafm_idr = run_rafm_idr[['GOC', 'period', 'pol_b', 'RV_AV_IF']]
        run_rafm_idr = run_rafm_idr[run_rafm_idr['period'].astype(str) == '0']
        run_rafm_idr = run_rafm_idr.drop(columns=["period"])
    except Exception:
        run_rafm_idr = pd.DataFrame()

    try:
        run_rafm_usd = pd.read_excel(params['path_rafm'], sheet_name='extraction_USD', engine='openpyxl')
        run_rafm_usd = run_rafm_usd[['GOC', 'period', 'pol_b', 'RV_AV_IF']]
        run_rafm_usd = run_rafm_usd[run_rafm_usd['period'].astype(str) == '0']
        run_rafm_usd = run_rafm_usd.drop(columns=["period"])
    except Exception:
        run_rafm_usd = pd.DataFrame()

    if not run_rafm_idr.empty or not run_rafm_usd.empty:
        run_rafm_only = pd.concat([run_rafm_idr, run_rafm_usd], ignore_index=True)
        run_rafm_only["pol_b"] = pd.to_numeric(run_rafm_only["pol_b"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
        run_rafm_only["RV_AV_IF"] = pd.to_numeric(run_rafm_only["RV_AV_IF"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
    else:
        run_rafm_only = pd.DataFrame()

    run_rafm_no_gs = run_rafm_only[~run_rafm_only['GOC'].astype(str).str.contains('GS', case=False, na=False)]

    try:
        run_uvsg_idr = pd.read_excel(params['path_uvsg'], sheet_name='extraction_IDR', engine='openpyxl')
        run_uvsg_idr = run_uvsg_idr[['GOC', 'period', 'pol_b', 'rv_av_if']]
        run_uvsg_idr = run_uvsg_idr[run_uvsg_idr['period'].astype(str) == '0']
        run_uvsg_idr = run_uvsg_idr.drop(columns=["period"])
    except Exception:
        run_uvsg_idr = pd.DataFrame()

    try:
        run_uvsg_usd = pd.read_excel(params['path_uvsg'], sheet_name='extraction_USD', engine='openpyxl')
        run_uvsg_usd = run_uvsg_usd[['GOC', 'period', 'pol_b', 'rv_av_if']]
        run_uvsg_usd = run_uvsg_usd[run_uvsg_usd['period'].astype(str) == '0']
        run_uvsg_usd = run_uvsg_usd.drop(columns=["period"])
    except Exception:
        run_uvsg_usd = pd.DataFrame()

    if not run_uvsg_idr.empty or not run_uvsg_usd.empty:
        run_uvsg = pd.concat([run_uvsg_idr, run_uvsg_usd], ignore_index=True)
        run_uvsg["pol_b"] = pd.to_numeric(run_uvsg["pol_b"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
        run_uvsg["rv_av_if"] = pd.to_numeric(run_uvsg["rv_av_if"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
        run_uvsg = run_uvsg.rename(columns={'rv_av_if': 'RV_AV_IF'})
    else:
        run_uvsg = pd.DataFrame()

    run_rafm = pd.concat([run_rafm_no_gs, run_uvsg], ignore_index=True)
    run_rafm = run_rafm.rename(columns={'GOC': 'goc'})

    merged = pd.merge(dv_ul_total, run_rafm, on="goc", how="outer", suffixes=("_uv_total", "run_rafm"))
    merged.fillna(0, inplace=True)

    merged['diff policies'] = merged.get('pol_num', 0) - merged.get('pol_b', 0)
    merged['diff sa'] = merged.get('total_fund', 0) - merged.get('RV_AV_IF', 0)

    def filter_goc_by_code(df, code):
        tokens = [k for k in code.split('_') if k]
        mask = df['goc'].apply(lambda x: all(token.lower() in str(x).lower() for token in tokens))
        return df[mask]

    def exclude_goc_by_code(df, code):
        tokens = [k for k in code.split('_') if k]
        mask = df['goc'].apply(lambda x: all(token.lower() in str(x).lower() for token in tokens))
        return df[~mask]

    tabel_total_l = exclude_goc_by_code(merged, 'gs')

    summary = pd.DataFrame({
        '': ['Total Trad All from DV', 'Grand Total Summary', 'Check'],
        'DV # of Policies': [dv_ul_total['pol_num'].sum() if 'pol_num' in dv_ul_total else 0,
                             tabel_total_l['pol_num'].sum() if 'pol_num' in tabel_total_l else 0,
                             (dv_ul_total['pol_num'].sum() if 'pol_num' in dv_ul_total else 0) - (tabel_total_l['pol_num'].sum() if 'pol_num' in tabel_total_l else 0)],
        'DV Fund Value': [dv_ul_total['total_fund'].sum() if 'total_fund' in dv_ul_total else 0,
                         tabel_total_l['total_fund'].sum() if 'total_fund' in tabel_total_l else 0,
                         (dv_ul_total['total_fund'].sum() if 'total_fund' in dv_ul_total else 0) - (tabel_total_l['total_fund'].sum() if 'total_fund' in tabel_total_l else 0)],
        'RAFM # of Policies': [run_rafm['pol_b'].sum() if 'pol_b' in run_rafm else 0,
                              tabel_total_l['pol_b'].sum() if 'pol_b' in tabel_total_l else 0,
                              (run_rafm['pol_b'].sum() if 'pol_b' in run_rafm else 0) - (tabel_total_l['pol_b'].sum() if 'pol_b' in tabel_total_l else 0)],
        'RAFM Fund Value': [run_rafm['RV_AV_IF'].sum() if 'RV_AV_IF' in run_rafm else 0,
                           tabel_total_l['RV_AV_IF'].sum() if 'RV_AV_IF' in tabel_total_l else 0,
                           (run_rafm['RV_AV_IF'].sum() if 'RV_AV_IF' in run_rafm else 0) - (tabel_total_l['RV_AV_IF'].sum() if 'RV_AV_IF' in tabel_total_l else 0)],
        'Diff # of Policies': [
            (dv_ul_total['pol_num'].sum() if 'pol_num' in dv_ul_total else 0) - (run_rafm['pol_b'].sum() if 'pol_b' in run_rafm else 0),
            (tabel_total_l['pol_num'].sum() if 'pol_num' in tabel_total_l else 0) - (tabel_total_l['pol_b'].sum() if 'pol_b' in tabel_total_l else 0),
            ((dv_ul_total['pol_num'].sum() if 'pol_num' in dv_ul_total else 0) - (run_rafm['pol_b'].sum() if 'pol_b' in run_rafm else 0)) -
            ((tabel_total_l['pol_num'].sum() if 'pol_num' in tabel_total_l else 0) - (tabel_total_l['pol_b'].sum() if 'pol_b' in tabel_total_l else 0))
        ],
        'Diff Fund Value': [
            (dv_ul_total['total_fund'].sum() if 'total_fund' in dv_ul_total else 0) - (run_rafm['RV_AV_IF'].sum() if 'RV_AV_IF' in run_rafm else 0),
            (tabel_total_l['total_fund'].sum() if 'total_fund' in tabel_total_l else 0) - (tabel_total_l['RV_AV_IF'].sum() if 'RV_AV_IF' in tabel_total_l else 0),
            ((dv_ul_total['total_fund'].sum() if 'total_fund' in dv_ul_total else 0) - (run_rafm['RV_AV_IF'].sum() if 'RV_AV_IF' in run_rafm else 0)) -
            ((tabel_total_l['total_fund'].sum() if 'total_fund' in tabel_total_l else 0) - (tabel_total_l['RV_AV_IF'].sum() if 'RV_AV_IF' in tabel_total_l else 0))
        ]
    })

    summary_tabel_2 = None
    if params.get('tabel_2_aktif', True):
        code_tabel_2 = 'AG_IDR_SH'
        tabel_2 = filter_goc_by_code(merged, code_tabel_2)
        dv_policies_tabel_2 = tabel_2['pol_num'].sum() if 'pol_num' in tabel_2 else 0
        dv_fund_tabel_2 = tabel_2['total_fund'].sum() if 'total_fund' in tabel_2 else 0
        rafm_policies_tabel_2 = tabel_2['pol_b'].sum() if 'pol_b' in tabel_2 else 0
        rafm_fund_tabel_2 = tabel_2['RV_AV_IF'].sum() if 'RV_AV_IF' in tabel_2 else 0
        diff_policies_tabel_2 = dv_policies_tabel_2 - rafm_policies_tabel_2
        diff_fund_tabel_2 = dv_fund_tabel_2 - rafm_fund_tabel_2
        summary_tabel_2 = pd.DataFrame([{
            "DV": dv_policies_tabel_2,
            "DV Fund": dv_fund_tabel_2,
            "RAFM Output": rafm_policies_tabel_2,
            "RAFM Fund": rafm_fund_tabel_2,
            'Diff # of Policies': diff_policies_tabel_2,
            'Diff fund': diff_fund_tabel_2
        }])

    summary_tabel_3 = None
    if params.get('tabel_3_aktif', True):
        code_tabel_3 = 'GS'
        tabel_gs = filter_goc_by_code(run_rafm_only, code_tabel_3) if 'run_rafm_only' in locals() else pd.DataFrame()
        tabel_gs = tabel_gs.rename(columns={'GOC': 'goc'}) if not tabel_gs.empty else tabel_gs
        dv_gs = filter_goc_by_code(dv_ul_total, code_tabel_3)

        tabel_3 = pd.merge(dv_gs, tabel_gs, on="goc", how="outer", suffixes=("_uv_total", "run_rafm"))
        tabel_3.fillna(0, inplace=True)

        tabel_3['diff policies'] = tabel_3.get('pol_num', 0) - tabel_3.get('pol_b', 0)
        tabel_3['diff sa'] = tabel_3.get('total_fund', 0) - tabel_3.get('RV_AV_IF', 0)

        dv_policies_tabel_3 = tabel_3['pol_num'].sum()
        dv_fund_tabel_3 = tabel_3['total_fund'].sum()
        rafm_policies_tabel_3 = tabel_3['pol_b'].sum()
        rafm_fund_tabel_3 = tabel_3['RV_AV_IF'].sum()
        diff_policies_tabel_3 = dv_policies_tabel_3 - rafm_policies_tabel_3
        diff_fund_tabel_3 = dv_fund_tabel_3 - rafm_fund_tabel_3

        summary_tabel_3 = pd.DataFrame([{
            "DV": dv_policies_tabel_3,
            "DV Fund": dv_fund_tabel_3,
            "RAFM Output": rafm_policies_tabel_3,
            "RAFM Fund": rafm_fund_tabel_3,
            "Diff # of Policies": diff_policies_tabel_3,
            "Diff Fund": diff_fund_tabel_3
        }])

    return {
        'tabel total':tabel_total_l,
        'tabel 2' : tabel_2,
        'tabel 3' : tabel_3,
        'summary_total': summary,
        'summary_tabel_2': summary_tabel_2,
        'summary_tabel_3': summary_tabel_3,
        'run_name': params.get('run_name', '')
    }
