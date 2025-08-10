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
 
pol_e_ul_dv_final = sum(ul_dv_final["pol_num"])
sa_if_m_ul_dv_final = sum(ul_dv_final["sum_assur"])
anp_if_m_ul_dv_final = sum(ul_dv_final["pre_ann"])
total_fund_sum_ul_dv_final = sum(ul_dv_final["total_fund"])

summary_ul_dv_final = pd.DataFrame([{
    "pol_e": pol_e_ul_dv_final,
    "sa_if_m": sa_if_m_ul_dv_final,
    "anp_if_m": anp_if_m_ul_dv_final,
    "total_fund_sum": total_fund_sum_ul_dv_final
}])


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

pol_e_full_stat_total = sum(full_stat["POLICY_NO_Count"])
sa_if_m_full_stat_total = sum(full_stat["PR_SA_Sum"])
anp_if_m_full_stat_total = sum(full_stat["pre_ann_Sum"])
total_fund_sum_full_stat_total = sum(full_stat["total_fund_Sum"])

summary_full_stat_total = pd.DataFrame([{
    "pol_e": pol_e_full_stat_total,
    "sa_if_m": sa_if_m_full_stat_total,
    "anp_if_m": anp_if_m_full_stat_total,
    "total_fund_sum": total_fund_sum_full_stat_total
}])

diff_pol_e = pol_e_ul_dv_final-pol_e_full_stat_total
diff_sa_if_m = sa_if_m_ul_dv_final-sa_if_m_full_stat_total
diff_anp_if_m = anp_if_m_ul_dv_final-anp_if_m_full_stat_total
diff_total_fund = total_fund_sum_ul_dv_final-total_fund_sum_full_stat_total

summary_diff_total = pd.DataFrame([{
    "pol_e": diff_pol_e,
    "sa_if_m": diff_sa_if_m,
    "anp_if_m": diff_anp_if_m,
    "total_fund_sum": diff_total_fund
}])



merged = pd.merge(ul_dv_final, full_stat, on="product_group", how="outer", 
                  suffixes=("_ul_dv_final", "_full_stat"))

merged.fillna(0, inplace=True)

result = pd.DataFrame()
result["product_group"] = merged["product_group"]
result["POLICY_NO_Count_diff"] = merged["pol_num"] - merged["POLICY_NO_Count"]
result["sum_assur_diff"] = merged["sum_assur"] - merged["PR_SA_Sum"]
result["pre_ann_diff"] = merged["pre_ann"] - merged["pre_ann_Sum"]
result["total_fund_diff"] = merged["total_fund"] - merged["total_fund_Sum"]

table1 = pd.merge(merged, result, on="product_group", how='outer')


merged_2 = pd.merge(result, full_stat, on="product_group", how="outer", 
                  suffixes=("_result", "_full_stat"))

merged_2.fillna(0, inplace=True)

result_percent = pd.DataFrame()
result_percent["product_group"] = merged_2["product_group"]
result_percent["policy_count_percent"] = merged_2["POLICY_NO_Count_diff"]/merged_2["POLICY_NO_Count"]*100
result_percent["sum_assur_percent"] = merged_2["sum_assur_diff"] /merged_2["PR_SA_Sum"]*100
result_percent["pre_ann_percent"] = merged_2["pre_ann_diff"]/merged_2["pre_ann_Sum"]*100
result_percent["total_fund_percent"] = merged_2["total_fund_diff"] /merged_2["total_fund_Sum"]*100

result_percent.replace(np.inf, 0, inplace=True)
table2 = pd.merge(table1, result_percent, on="product_group", how="outer")

policy_count = ((summary_diff_total["pol_e"]/summary_full_stat_total["pol_e"])*100) 
sa_if_m= (summary_diff_total["sa_if_m"]/summary_full_stat_total["sa_if_m"])*100
anp_if_m =(summary_diff_total["anp_if_m"]/summary_full_stat_total["anp_if_m"])*100
total_fund_sum= (summary_diff_total["total_fund_sum"]/summary_full_stat_total["total_fund_sum"])*100

Different_Percentage = pd.DataFrame([{  
    "policy_count": policy_count,
    "sa_if_m": sa_if_m,
    "anp_if_m": anp_if_m, 
    "total_fund_sum": total_fund_sum
}])

policy_count = (sum(result_percent["policy_count_percent"])/sum(full_stat["POLICY_NO_Count"]))
sa_if_m= (sum(result_percent["sum_assur_percent"])/sum(full_stat["PR_SA_Sum"]))
anp_if_m = (sum(result_percent["pre_ann_percent"])/sum(full_stat["pre_ann_Sum"]))
total_fund_sum= (sum(result_percent["total_fund_percent"])/sum(full_stat["total_fund_Sum"]))

Different_Percentage_of_Checking_Result_to_Raw_Data = pd.DataFrame([{
    "policy_count": policy_count,
    "sa_if_m": sa_if_m,
    "anp_if_m": anp_if_m, 
    "total_fund_sum": total_fund_sum
}])


