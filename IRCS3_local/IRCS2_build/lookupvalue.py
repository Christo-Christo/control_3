import pandas as pd
import IRCS2_input as input_script
import xlsxwriter
import UL
import trad
import numpy as np

# UL Lookup processing (simplified)
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

lookup = pd.DataFrame({
    'Product code':        a1['product'],
    'Grouping DV':         a1['product code'],
    'product_group':   a2['product code']
})

merged = (
    lookup
      .groupby('product_group', sort=False)
      .agg({
         'Product code': '/'.join,
         'Grouping DV':  '/'.join
      })
      .reset_index()
)

# Create simplified UL lookup table using merged data from UL module
full_lookup_table = pd.merge(merged, UL.merged, on="product_group", how='right')

# 1) Pick your "fixed" first three
first_three = ['Product code', 'Grouping DV', 'product_group']

# 2) Grab the rest in their existing order
rest = [c for c in full_lookup_table.columns if c not in first_three]

# 3) Reindex into the new order
full_lookup_table = full_lookup_table[first_three + rest]

# 1) Add a blank column
full_lookup_table['New Blank'] = ''

# 2) Pull out the currency suffix from product_group
full_lookup_table['Currency'] = full_lookup_table['product_group'].str[-3:]

# Currency totals for UL
numeric_columns = [col for col in full_lookup_table.columns if full_lookup_table[col].dtype in ['int64', 'float64']]
currency_totals = (
    full_lookup_table
      .groupby('Currency', sort=False)[numeric_columns]
      .sum()
      .reset_index()
)

currency_totals['Currency'] = 'UL_' + currency_totals['Currency']

# TRAD processing (simplified)
trad_dv_metrics = trad.trad_dv_final.copy()
trad_dv_metrics = trad_dv_metrics.drop(columns=['loan_sa'])
trad_dv_metrics = trad_dv_metrics[
    ['product_group', 'pol_num', 'sum_assd', 'pre_ann']
]

# Add total_fund_sum column for consistency
trad_dv_metrics['total_fund_sum'] = 0

# Use merged data from trad module for TRAD lookup table
trad_code = trad.original_trad[['product', 'product_group']].copy()
trad_code.rename(columns={'product_group': 'grouping DV'}, inplace=True)
trad_code['product_group'] = trad.trad2['product_group'].copy()
trad_code_unique = trad_code.drop_duplicates(subset=['product_group'])

merged4 = pd.merge(trad_code_unique, trad.merged, on='product_group', how='right')
merged4['remarks'] = ''
merged4['currency'] = merged4['product_group'].str[-3:]
merged4.fillna(0, inplace=True)
merged4.replace(np.inf, 0, inplace=True)

agg_all = merged4.groupby('currency').sum(numeric_only=True).reset_index()
agg_all['currency'] = 'TRAD_' + agg_all['currency']