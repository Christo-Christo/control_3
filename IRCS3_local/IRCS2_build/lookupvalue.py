import pandas as pd
import IRCS2_input as input_script
import UL
import trad
import numpy as np

# UL Data Processing
ul_dv = pd.read_csv(input_script.DV_AZUL_path)
ul_dv = ul_dv.drop(columns=["goc"])
ul_dv_final = ul_dv.groupby(["product_group"],as_index=False).sum(numeric_only=True)

code_ul = pd.read_excel(input_script.CODE_LIBRARY_path,sheet_name = ["UL"],engine="openpyxl")
code_ul = code_ul["UL"]

ul_dv_final[["product", "currency"]] = ul_dv_final["product_group"].str.extract(r"(\w+)_([\w\d]+)")
ul_dv_final = ul_dv_final.drop(columns="product_group")
 
a1 = (ul_dv_final[["product",'currency']]).copy()
convert = dict(zip(code_ul["Prophet Code"], code_ul["Flag Code"]))
ul_dv_final["product"] = ul_dv_final["product"].map(convert).fillna(ul_dv_final["product"])
a2 = (ul_dv_final[['product','currency']]).copy()
ul_dv_final["product_group"] = ul_dv_final["product"].str.cat(ul_dv_final["currency"], sep="_")

a1['product code'] = (
    a1['product']
      .str.rstrip('_')        
      .str.cat(a1['currency'], sep='_')
)

a2 ['product code'] =(
    a2['product'] + '_' + a2['currency']
) 

lookup_ul = pd.DataFrame({
    'Product code':        a1['product'],
    'Grouping DV':         a1['product code'],
    'product_group':   a2['product code']
})

merged_ul = (
    lookup_ul
      .groupby('product_group', sort=False)
      .agg({
         'Product code': '/'.join,
         'Grouping DV':  '/'.join
      })
      .reset_index()
)

# Merge with UL data
ul_lookup_table = pd.merge(merged_ul, UL.merged, on="product_group", how='right')

# Reorder columns
first_three = ['Product code', 'Grouping DV', 'product_group']
rest = [c for c in ul_lookup_table.columns if c not in first_three]
ul_lookup_table = ul_lookup_table[first_three + rest]

# Add blank column and currency
ul_lookup_table['New Blank'] = ''
ul_lookup_table['Currency'] = ul_lookup_table['product_group'].str[-3:]

# Currency totals for UL
ul_metrics = [
    'pol_num', 'pre_ann', 'sum_assur', 'total_fund',
    'POLICY_NO_Count', 'pre_ann_Sum', 'PR_SA_Sum', 'total_fund_Sum'
]

ul_currency_totals = (
    ul_lookup_table
      .groupby('Currency', sort=False)[ul_metrics]
      .sum()
      .reset_index()
)
ul_currency_totals['Currency'] = 'UL_' + ul_currency_totals['Currency']

# TRAD Data Processing - FIXED
# Get TRAD DV data - check actual columns first
print("TRAD DV columns:", trad.trad_dv_final.columns.tolist())

# Get available columns from trad_dv_final
trad_dv_metrics = trad.trad_dv_final.copy()

# Check if 'loan_sa' exists before dropping it
if 'loan_sa' in trad_dv_metrics.columns:
    trad_dv_metrics = trad_dv_metrics.drop(columns=['loan_sa'])

# Add total_fund_sum = 0 for TRAD (since it doesn't have this column)
trad_dv_metrics['total_fund_sum'] = 0

# Select available columns - adjust based on what actually exists
available_dv_cols = ['product_group']
for col in ['pol_num', 'sum_assd', 'pre_ann']:
    if col in trad_dv_metrics.columns:
        available_dv_cols.append(col)
available_dv_cols.append('total_fund_sum')  # We just added this

trad_dv_metrics = trad_dv_metrics[available_dv_cols]

# Get TRAD stat data - check actual columns first
print("TRAD stat columns:", trad.full_stat_total.columns.tolist())

trad_stat_metrics = trad.full_stat_total.copy()
# Add total_fund_sum = 0 for TRAD stat data too
trad_stat_metrics['total_fund_sum'] = 0

# Select available columns
available_stat_cols = ['product_group']
for col in ['POLICY_REF_Count', 'sum_assd_Sum', 'pre_ann_Sum']:
    if col in trad_stat_metrics.columns:
        available_stat_cols.append(col)
available_stat_cols.append('total_fund_sum')  # We just added this

trad_stat_metrics = trad_stat_metrics[available_stat_cols]

# Merge TRAD data
trad_merged = pd.merge(trad_dv_metrics, trad_stat_metrics, on='product_group', how='outer')
trad_merged = trad_merged.fillna(0)

# Create TRAD lookup
trad_code = trad.original_trad[['product', 'product_group']].copy()
trad_code.rename(columns={'product_group': 'grouping DV'}, inplace=True)
trad_code['product_group'] = trad.trad2['product_group'].copy()
trad_code_unique = trad_code.drop_duplicates(subset=['product_group'])

trad_lookup_table = pd.merge(trad_code_unique, trad_merged, on='product_group', how='right')
trad_lookup_table['remarks'] = ''
trad_lookup_table['currency'] = trad_lookup_table['product_group'].str[-3:]
trad_lookup_table.fillna(0, inplace=True)
trad_lookup_table.replace(np.inf, 0, inplace=True)

# Rename columns for consistency
trad_lookup_table = trad_lookup_table.rename(columns={
    'product': 'Product code',
    'grouping DV': 'Grouping DV'
})

# Reorder TRAD columns to match UL structure
trad_first_three = ['Product code', 'Grouping DV', 'product_group']
trad_rest = [c for c in trad_lookup_table.columns if c not in trad_first_three]
trad_lookup_table = trad_lookup_table[trad_first_three + trad_rest]

# Add blank column
trad_lookup_table['New Blank'] = ''

# Currency totals for TRAD - use only available columns
trad_available_metrics = []
for col in ['pol_num', 'sum_assd', 'pre_ann', 'total_fund_sum', 'POLICY_REF_Count', 'sum_assd_Sum', 'pre_ann_Sum']:
    if col in trad_lookup_table.columns:
        trad_available_metrics.append(col)

trad_currency_totals = trad_lookup_table.groupby('currency')[trad_available_metrics].sum(numeric_only=True).reset_index()
trad_currency_totals['currency'] = 'TRAD_' + trad_currency_totals['currency']

# Final merged table for display (combining both UL and TRAD)
# First, let's align column structures properly
ul_display_cols = [
    'Product code', 'Grouping DV', 'product_group', 
    'pol_num', 'sum_assur', 'pre_ann', 'total_fund',
    'POLICY_NO_Count', 'PR_SA_Sum', 'pre_ann_Sum', 'total_fund_Sum'
]

# For TRAD, use available columns and create mapping
trad_column_mapping = {
    'sum_assd': 'sum_assur',
    'total_fund_sum': 'total_fund', 
    'POLICY_REF_Count': 'POLICY_NO_Count',
    'sum_assd_Sum': 'PR_SA_Sum'
}

# Start with basic columns that should exist
trad_base_cols = ['Product code', 'Grouping DV', 'product_group']

# Add available columns with proper mapping
trad_display_cols = trad_base_cols.copy()
for orig_col, target_col in [
    ('pol_num', 'pol_num'),
    ('sum_assd', 'sum_assur'), 
    ('pre_ann', 'pre_ann'),
    ('total_fund_sum', 'total_fund'),
    ('POLICY_REF_Count', 'POLICY_NO_Count'),
    ('sum_assd_Sum', 'PR_SA_Sum'),
    ('pre_ann_Sum', 'pre_ann_Sum'),
    ('total_fund_sum', 'total_fund_Sum')  # Note: using same source for both fund columns
]:
    if orig_col in trad_lookup_table.columns:
        trad_display_cols.append(orig_col)

# Create TRAD display dataframe with available columns
trad_display = trad_lookup_table[trad_display_cols].copy()

# Rename columns to match UL naming
rename_dict = {}
for old_name, new_name in trad_column_mapping.items():
    if old_name in trad_display.columns:
        rename_dict[old_name] = new_name

trad_display = trad_display.rename(columns=rename_dict)

# Ensure both dataframes have the same columns by adding missing ones with 0
ul_display = ul_lookup_table[ul_display_cols].copy()

# Get all unique columns from both datasets
all_columns = set(ul_display.columns) | set(trad_display.columns)

# Add missing columns to both datasets
for col in all_columns:
    if col not in ul_display.columns:
        ul_display[col] = 0
    if col not in trad_display.columns:
        trad_display[col] = 0

# Reorder columns to match
final_columns = ul_display_cols
ul_display = ul_display[final_columns]
trad_display = trad_display[final_columns]

# Combine both datasets
combined_lookup_table = pd.concat([ul_display, trad_display], ignore_index=True)
combined_lookup_table = combined_lookup_table.fillna(0)

# Add currency column to combined table
combined_lookup_table['Currency'] = combined_lookup_table['product_group'].str[-3:]

# Combined currency totals
combined_metrics = [
    'pol_num', 'sum_assur', 'pre_ann', 'total_fund',
    'POLICY_NO_Count', 'PR_SA_Sum', 'pre_ann_Sum', 'total_fund_Sum'
]

# Separate UL and TRAD for currency totals
ul_combined = combined_lookup_table[combined_lookup_table['product_group'].str.contains('U|A', case=False, na=False)]
trad_combined = combined_lookup_table[~combined_lookup_table['product_group'].str.contains('U|A', case=False, na=False)]

ul_currency_summary = ul_combined.groupby('Currency')[combined_metrics].sum().reset_index()
ul_currency_summary['Currency'] = 'UL_' + ul_currency_summary['Currency']

trad_currency_summary = trad_combined.groupby('Currency')[combined_metrics].sum().reset_index()
trad_currency_summary['Currency'] = 'TRAD_' + trad_currency_summary['Currency']

# Export variables for use in main program
full_lookup_table = combined_lookup_table  # Main table for display
currency_totals = ul_currency_summary      # UL currency summaries
agg_all = trad_currency_summary           # TRAD currency summaries