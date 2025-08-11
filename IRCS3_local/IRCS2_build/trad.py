import pandas as pd
import numpy as np
import csv
from datetime import datetime
from functools import reduce
import IRCS2_input as input_sheet

code = pd.read_excel(input_sheet.CODE_LIBRARY_path,sheet_name = ["TRAD"],engine="openpyxl")
code = code["TRAD"]

trad_dv = pd.read_csv(input_sheet.DV_AZTRAD_path,sep = ",")

trad_dv1 = trad_dv.drop(columns=["goc"])
trad_dv_final = trad_dv1.groupby(["product_group"],as_index=False).sum(numeric_only=True)
trad_dv_final[["product", "currency"]] = trad_dv_final["product_group"].str.extract(r"(\w+)_([\w\d]+)")
trad_dv_final.drop(columns="product_group")

original_trad = trad_dv_final.copy()

convert = dict(zip(code["Prophet Code"], code["Flag Code"]))
trad_dv_final["product"] = trad_dv_final["product"].map(convert).fillna(trad_dv_final["product"])
trad_dv_final["product_group"]= trad_dv_final["product"].str.cat(trad_dv_final["currency"], sep="_")
trad2 = trad_dv_final.copy()

trad_dv_final["pol_num"] = (
    trad_dv_final["pol_num"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

trad_dv_final["pol_num"] = pd.to_numeric(
    trad_dv_final["pol_num"], errors="coerce"
)

trad_dv_final["pre_ann"] = (
    trad_dv_final["pre_ann"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

trad_dv_final["pre_ann"] = pd.to_numeric(
    trad_dv_final["pre_ann"], errors="coerce"
)

trad_dv_final["sum_assd"] = (
    trad_dv_final["sum_assd"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

trad_dv_final["sum_assd"] = pd.to_numeric(
    trad_dv_final["sum_assd"], errors="coerce"
)

trad_dv_final["loan_sa"] = (
    trad_dv_final["loan_sa"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

trad_dv_final["loan_sa"] = pd.to_numeric(
    trad_dv_final["loan_sa"], errors="coerce"
)

trad_dv_final = trad_dv_final.groupby(["product_group"],as_index=False).sum(numeric_only=True)
trad_dv_final = trad_dv_final[~(trad_dv_final["product_group"].str.startswith("A_"))]
cols = list(trad_dv_final.columns)
pre_idx = cols.index('pre_ann')
sum_idx = cols.index('sum_assd')
cols[pre_idx], cols[sum_idx] = cols[sum_idx], cols[pre_idx]
trad_dv_final = trad_dv_final[cols]
trad_dv_final['loan_sa'] = 0

full_stat = pd.read_csv(input_sheet.IT_AZTRAD_path, sep = ";")

full_stat["product_group"] = full_stat["PRODUCT_CODE"].str.replace("BASE_","",regex=False)+"_"+full_stat["CURRENCY1"]
full_stat = full_stat.drop(columns=["PRODUCT_CODE","CURRENCY1"])
trad_stat = full_stat.copy()

full_stat["POLICY_REF_Count"] = (
    full_stat["POLICY_REF_Count"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

full_stat["POLICY_REF_Count"] = pd.to_numeric(
    full_stat["POLICY_REF_Count"], errors="coerce"
)

full_stat["pre_ann_Sum"] = (
    full_stat["pre_ann_Sum"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

full_stat["pre_ann_Sum"] = pd.to_numeric(
    full_stat["pre_ann_Sum"], errors="coerce"
)

full_stat["sum_assd_Sum"] = (
    full_stat["sum_assd_Sum"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

full_stat["sum_assd_Sum"] = pd.to_numeric(
    full_stat["sum_assd_Sum"], errors="coerce"
)

full_stat = full_stat.groupby(["product_group"],as_index=False).sum(numeric_only=True)
full_stat = full_stat[~(full_stat["product_group"].str.startswith("A_") | full_stat["product_group"].str.startswith("NA_"))]

summary_it = pd.read_csv(input_sheet.SUMMARY_path, sep = ",")
summary_it["product_group"] = summary_it["prod_code_First"]+"_"+summary_it["currency_First"]
summary_it = summary_it.drop(columns=["prod_code_First","currency_First"])
summary_it = summary_it.rename(columns={"pol_num_Count":"POLICY_REF_Count" })

summary_it["POLICY_REF_Count"] = (
    summary_it["POLICY_REF_Count"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

summary_it["POLICY_REF_Count"] = pd.to_numeric(
    summary_it["POLICY_REF_Count"], errors="coerce"
)

summary_it["pre_ann_Sum"] = (
    summary_it["pre_ann_Sum"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

summary_it["pre_ann_Sum"] = pd.to_numeric(
    summary_it["pre_ann_Sum"], errors="coerce"
)

summary_it["sum_assd_Sum"] = (
    summary_it["sum_assd_Sum"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

summary_it["sum_assd_Sum"] = pd.to_numeric(
    summary_it["sum_assd_Sum"], errors="coerce"
)

summary_it = summary_it.groupby(["product_group"],as_index=False).sum(numeric_only=True)

mapping_dict = pd.read_excel(input_sheet.CODE_LIBRARY_path,sheet_name = ["SPEC TRAD"],engine="openpyxl")
mapping_dict = mapping_dict["SPEC TRAD"]

full_stat_total = pd.concat([full_stat,summary_it])
full_stat_total[["product", "currency"]] = full_stat_total["product_group"].str.extract(r"(\w+)_([\w\d]+)")
full_stat_total = full_stat_total.copy()
convert = dict(zip(mapping_dict["Old"], mapping_dict["New"]))
full_stat_total["product"] = full_stat_total["product"].map(convert).fillna(full_stat_total["product"])
full_stat_total["product_group"] = full_stat_total["product"].str.cat(full_stat_total["currency"], sep="_")
full_stat_total = full_stat_total.drop(columns=["product","currency"])
full_stat_total = full_stat_total.groupby(["product_group"],as_index=False).sum(numeric_only=True)
cols = list(full_stat_total.columns)
pre_idx = cols.index('pre_ann_Sum')
sum_idx = cols.index('sum_assd_Sum')
cols[pre_idx], cols[sum_idx] = cols[sum_idx], cols[pre_idx]
full_stat_total = full_stat_total[cols]

campaign = pd.read_csv(input_sheet.LGC_LGM_CAMPAIGN_path,sep=";")
campaign = campaign.drop(columns=["campaign_Period"])

def read_csv_fallback(path, **kwargs):
    """
    Attempt to read as UTF-8, and if that fails, retry as Latin-1.
    Persists all other kwargs (sep, engine, on_bad_lines, etc.).
    """
    for enc in ("utf-8", "latin-1"):
        try:
            return pd.read_csv(path, encoding=enc, **kwargs)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, encoding="utf-8", **kwargs)

tradcon_input = read_csv_fallback(
    input_sheet.TRADCONV_path,
    sep=";",
    engine="python",
    on_bad_lines="skip",
    quoting=3
)
tradcon_input = tradcon_input[['POLICY_REF','PRODUCT_CODE','COVER_CODE','SUM_INSURED','CURRENCY1','POLICY_START_DATE']]
tradcon_input = tradcon_input[tradcon_input['PRODUCT_CODE'].str.contains('lg[cm]', case=False, na=False)]
tradcon_input = tradcon_input.groupby(["POLICY_REF"]).first().reset_index()

def filter_by_month(input, reporting_month,financial_year):
    month = reporting_month
    year = financial_year
    
    if month == 1:
        cutoff = datetime(year, 2, 1)
    elif month == 2:
        cutoff = datetime(year, 3, 1) 
    elif month == 3:
        cutoff = datetime(year, 4, 1)  
    elif month == 4:
        cutoff = datetime(year, 5, 1) 
    elif month == 5:
        cutoff = datetime(year, 6, 1)
    elif month == 6:
        cutoff = datetime(year, 7, 1)
    elif month == 7:
        cutoff = datetime(year, 8, 1) 
    elif month == 8:
        cutoff = datetime(year, 9, 1)  
    elif month == 9:
        cutoff = datetime(year, 10, 1) 
    elif month == 10:
        cutoff = datetime(year, 11, 1)
    elif month == 11:
        cutoff = datetime(year, 12, 1)
    elif month == 12:
        cutoff = datetime(year + 1, 1, 1)

    input['POLICY_START_DATE'] = pd.to_datetime(input['POLICY_START_DATE'], format='%d-%b-%y')
    filtered = input[input['POLICY_START_DATE'] < cutoff]
    return filtered

tradcon_cleaned = filter_by_month(tradcon_input, input_sheet.reporting_month, input_sheet.financial_year)

tradsha_input = read_csv_fallback(
    input_sheet.TRADSHA_path,
    sep=";",
    engine="python",
    on_bad_lines="skip",
    quoting=3
)
tradsha_input = tradsha_input[['POLICY_REF','PRODUCT_CODE','COVER_CODE','SUM_INSURED','CURRENCY1','POLICY_START_DATE']]
tradsha_input = tradsha_input[tradsha_input['PRODUCT_CODE'].str.contains('lg[cm]', case=False, na=False)]
tradsha_input = tradsha_input.groupby(["POLICY_REF"]).first().reset_index()

tradsha_cleaned = filter_by_month(tradsha_input,input_sheet.reporting_month, input_sheet.financial_year)

tradcon = tradcon_cleaned
tradcon = tradcon.drop(columns=["POLICY_START_DATE"])

tradsha = tradsha_cleaned
tradsha = tradsha.drop(columns=["POLICY_START_DATE"])

merged_trad = pd.concat([tradcon,tradsha])
campaign_total = campaign.merge(merged_trad, 
                   left_on="Policy No", 
                   right_on="POLICY_REF", 
                   how="left")
campaign_total = campaign_total.fillna(0)
campaign_total = campaign_total.drop("POLICY_REF", axis=1)

lookup = pd.read_excel(input_sheet.CODE_LIBRARY_path,sheet_name = ["Campaign Lookup"],engine="openpyxl")
lookup = lookup["Campaign Lookup"]
campaign_total["SUM_INSURED"] = pd.to_numeric(campaign_total["SUM_INSURED"], errors="coerce")
lookup["Max Bonus"] = pd.to_numeric(lookup["Max Bonus"], errors="coerce")

campaign_total["key"] = campaign_total["campaign_type"].astype(str) + "_" + campaign_total["CURRENCY1"].astype(str)
bonus = campaign_total.merge(lookup[["key", "Max Bonus"]], on="key", how="left")

bonus["calculated_bonus"] = bonus["SUM_INSURED"] * 0.1
bonus["Bonus SA"] = np.where(
    bonus["Max Bonus"].notna(),
    np.minimum(bonus["calculated_bonus"], bonus["Max Bonus"]),
    0
)

bonus["SA After Bonus"] = bonus["SUM_INSURED"]+bonus["Bonus SA"]
bonus = bonus.drop(["key", "calculated_bonus","Max Bonus"], axis=1)

summary = bonus.drop(columns=["Policy No","campaign_type","product","PRODUCT_CODE"])
summary["Grouping Raw Data"] = summary["COVER_CODE"].str.replace("BASE_","",regex=False)+"_"+summary["CURRENCY1"]
summary = summary.groupby(["Grouping Raw Data"],as_index=False).sum(numeric_only=True)

summary[["product", "currency"]] = summary["Grouping Raw Data"].str.extract(r"(\w+)_([\w\d]+)")
convert = dict(zip(code["Flag Code"], code["Prophet Code"]))
summary["Grouping DV"] = summary["product"].map(convert).fillna(summary["product"])
summary = summary.drop(columns=["product","currency"])
cols = ["Grouping Raw Data", "Grouping DV"] + [col for col in summary.columns if col not in ["Grouping Raw Data", "Grouping DV"]]
summary = summary[cols]

campaign_sum = summary.copy()

bsi_raw = pd.read_excel(input_sheet.BSI_ATTRIBUSI_path, sheet_name = ["Export Worksheet"], engine="openpyxl")
bsi_raw = bsi_raw["Export Worksheet"]
bsi_raw = bsi_raw.drop(columns=["POLICY_NO","CP_PH_ID","CP_PH","PRODUCT_CODE","CP_INSURED_ID","LOANNO","CP_INSURED","POLICY_STATUS","UP_ATTR"])
bsi_raw = bsi_raw.rename(columns = {"COVER_CODE":"Cover_code",
                                    "PREM_ATTR":"anp"})
bsi_raw = bsi_raw.groupby(["Cover_code"],as_index=False).sum(numeric_only=True)
code_bsi = pd.read_excel(input_sheet.CODE_LIBRARY_path,sheet_name = ["Code BSI"],engine="openpyxl")
code_bsi = code_bsi["Code BSI"]
code_bsi = code_bsi.rename(columns = {'Grouping raw data':'product_group'})
bsi_merge = pd.merge(code_bsi,bsi_raw,on = 'Cover_code', how = 'left')
bsi_merge["product_group"] = bsi_merge["product_group"]+"_IDR"
bsi = bsi_merge.drop(columns = {'Cover_code'})

summary = summary.rename(columns = {"Grouping Raw Data" : "product_group","Bonus SA":"sum_assd"})
summary = summary.drop(columns = {"Grouping DV","SUM_INSURED","SA After Bonus"})

merged = pd.merge(trad_dv_final, full_stat_total, on="product_group", how="outer", 
                  suffixes=("_trad_dv_final", "_full_stat"))

merged.fillna(0, inplace=True)

def get_prophet_code(pg):
    if '_IDR' in pg:
        currency = '_IDR'
    elif '_USD' in pg:
        currency = '_USD'
    else:
        return np.nan 
    base_name = pg.replace(currency, '')
    match = code.loc[code['Flag Code'] == base_name, 'Prophet Code']
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

trad_dv = trad_dv 