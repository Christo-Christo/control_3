import pandas as pd
import numpy as np
import IRCS2_input as input_script

code_ul = pd.read_excel(input_script.CODE_LIBRARY_path,sheet_name = ["UL"],engine="openpyxl")
code_ul = code_ul["UL"]

# DV AZUL CLEANUP
ul_dv = pd.read_csv(input_script.DV_AZUL_path)

ul_dv = ul_dv.drop(columns=["goc"])
ul_dv_final = ul_dv.groupby(["product_group"],as_index=False).sum(numeric_only=True)
ul_dv_final[["product", "currency"]] = ul_dv_final["product_group"].str.extract(r"(\w+)_([\w\d]+)")
ul_dv_final = ul_dv_final.drop(columns="product_group")
convert = dict(zip(code_ul["Prophet Code"], code_ul["Flag Code"]))
ul_dv_final["product"] = ul_dv_final["product"].map(convert).fillna(ul_dv_final["product"])
ul_dv_final["product_group"] = ul_dv_final["product"].str.cat(ul_dv_final["currency"], sep="_")

ul_dv_final["pol_num"] = (
    ul_dv_final["pol_num"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)        
)
 
ul_dv_final["pol_num"] = pd.to_numeric(
    ul_dv_final["pol_num"], errors="coerce"
)
 
ul_dv_final["pre_ann"] = (
    ul_dv_final["pre_ann"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)        
)
 
ul_dv_final["pre_ann"] = pd.to_numeric(
    ul_dv_final["pre_ann"], errors="coerce"
)
 
ul_dv_final["sum_assur"] = (
    ul_dv_final["sum_assur"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)        
)
 
ul_dv_final["sum_assur"] = pd.to_numeric(
    ul_dv_final["sum_assur"], errors="coerce"
)
 
ul_dv_final["total_fund"] = (
    ul_dv_final["total_fund"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)        
)
 
ul_dv_final["total_fund"] = pd.to_numeric(
    ul_dv_final["total_fund"], errors="coerce"
)
 
ul_dv_final = ul_dv_final.groupby(["product_group"],as_index=False).sum(numeric_only=True)
cols = list(ul_dv_final.columns)
pre_idx = cols.index('pre_ann')
sum_idx = cols.index('sum_assur')
cols[pre_idx], cols[sum_idx] = cols[sum_idx], cols[pre_idx]
ul_dv_final = ul_dv_final[cols]

mapping_dict = pd.read_excel(input_script.CODE_LIBRARY_path,sheet_name = ["SPEC UL"],engine="openpyxl")
mapping_dict = mapping_dict["SPEC UL"]

# IT AZUL CLEAN UP
full_stat = pd.read_csv(input_script.IT_AZUL_path, sep = ";")

full_stat["product_group"] = full_stat["PRODUCT_CODE"].str.replace("BASE_","",regex=False)+"_"+full_stat["PR_CURR"]
full_stat[["product", "currency"]] = full_stat["product_group"].str.extract(r"(\w+)_([\w\d]+)")
convert = dict(zip(mapping_dict["Old"], mapping_dict["New"]))
full_stat["product"] = full_stat["product"].map(convert).fillna(full_stat["product"])
full_stat["product_group"] = full_stat["product"].str.cat(full_stat["currency"], sep="_")
full_stat = full_stat.drop(columns=["PRODUCT_CODE","PR_CURR","product","currency"])

full_stat["POLICY_NO_Count"] = (
    full_stat["POLICY_NO_Count"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

full_stat["POLICY_NO_Count"] = pd.to_numeric(
    full_stat["POLICY_NO_Count"], errors="coerce"
)

full_stat["pre_ann_Sum"] = (
    full_stat["pre_ann_Sum"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

full_stat["pre_ann_Sum"] = pd.to_numeric(
    full_stat["pre_ann_Sum"], errors="coerce"
)

full_stat["PR_SA_Sum"] = (
    full_stat["PR_SA_Sum"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

full_stat["PR_SA_Sum"] = pd.to_numeric(
    full_stat["PR_SA_Sum"], errors="coerce"
)

full_stat["total_fund_Sum"] = (
    full_stat["total_fund_Sum"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

full_stat["total_fund_Sum"] = pd.to_numeric(
    full_stat["total_fund_Sum"], errors="coerce"
)

full_stat = full_stat.groupby(["product_group"],as_index=False).sum(numeric_only=True)
cols = list(full_stat.columns)
pre_idx = cols.index('pre_ann_Sum')
sum_idx = cols.index('PR_SA_Sum')
cols[pre_idx], cols[sum_idx] = cols[sum_idx], cols[pre_idx]
full_stat = full_stat[cols]

merged = pd.merge(ul_dv_final, full_stat, on="product_group", how="outer", 
                  suffixes=("_ul_dv_final", "_full_stat"))

merged.fillna(0, inplace=True)
def get_prophet_code(pg):
    if '_IDR' in pg:
        currency = '_IDR'
    elif '_USD' in pg:
        currency = '_USD'
    else:
        return np.nan 
    base_name = pg.replace(currency, '')
    match = code_ul.loc[code_ul['Flag Code'] == base_name, 'Prophet Code']
    if not match.empty:
        return match.iloc[0]
    else:
        return base_name

merged.insert(0, 'col1', merged['product_group'].apply(get_prophet_code))

def add_currency(row):
    if '_IDR' in row['product_group']:
        return f"{row['col1']}_IDR"
    elif '_USD' in row['product_group']:
        return f"{row['col1']}_USD"
    else:
        return row['col1']

merged.insert(1, 'col2', merged.apply(add_currency, axis=1))

ul_dv = ul_dv