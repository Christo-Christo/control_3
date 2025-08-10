import xlsxwriter
from collections import defaultdict
from IRCS2_input import xlsx_output, IT_AZTRAD_path, SUMMARY_path
import UL
import time
import lookupvalue as tst
import numpy
import trad
import pandas as pd

def elapsed_time(start,end):
    if round((end - start),0) > 60:
        print(f"\n RUNTIME: {round((end_time - start_time) / 60, 2)} minutes")
    elif (end - start) < 1:
        print(f"\n RUNTIME: {round((end_time - start_time) * 1000, 2)} ms")
    else:
        print(f"\n RUNTIME: {round((end_time - start_time), 2)} second")

############### EXCEL FORMATTING
start_time = time.time()
wb = xlsxwriter.Workbook(xlsx_output, {'nan_inf_to_errors': True})
number_format = '_(* #,##0_);_(* (#,##0)_);_(* "-"_);_(@_)'

# Summary checking AZUL SHEET
ws = wb.add_worksheet('Summary_Checking_UL')

ws.freeze_panes(10, 4)

headers_summary = ['Items', 'Total Input from csv', 'Total output in summary', 'Diff','AZUL']

headers_sum_dict = defaultdict(int)
for h in headers_summary:
    headers_sum_dict[h] = len(h)
max_len = max(headers_sum_dict.items())[1]
ws.set_column(1, 19, max_len + 2)
ws.set_column(20, 20, max_len * 6)

for c, h in enumerate(headers_summary):
    ws.write(c + 1, 3, h, wb.add_format({'bold': True}))

headers_table = ["Product code", "Grouping DV", "Grouping Raw Data"]
for c, h in enumerate(headers_table):
    ws.merge_range(8, c + 1, 9, c + 1, h, wb.add_format({'bold': True, 'bg_color': '#002060', 
                                                  'pattern': 1, 'font_color': 'white', 
                                                  'align': 'center', 'valign': 'vcenter'}))

header_table_notfreezed1 = ["DV Output [1]", "Raw Data [2]", "Checking Results [1]-[2]", "Different Percentage of Checking Result to Raw Data"]
headers_table_notfreezed2 = ["pol_e", "sa_if_m", "anp_if_m", "total_fund_sum"]
header_table_notfreezed1_frm = wb.add_format({'bold': True, 'bg_color': '#002060', 
                                'pattern': 1, 'font_color': 'white', 
                                'align': 'center', 'valign': 'center',
                                'top': 2, 'top_color':'white', 'bottom': 2,
                                'bottom_color': 'white', 'left': 1,'left_color': 'white',
                                'right': 1,'right_color': 'white'})
header_table_notfreezed2_frm = wb.add_format({'bold': True, 'bg_color': '#3A3838', 
                                'pattern': 1, 'font_color': 'white', 
                                'align': 'center', 'valign': 'center',
                                'top': 2, 'top_color':'white', 'bottom': 2,
                                'bottom_color': 'white', 'left': 1,'left_color': 'white',
                                'right': 1,'right_color': 'white'})

for c,h in enumerate(header_table_notfreezed1):
    ws.merge_range(0, 4 * (c + 1), 0, (4 * (c + 1)) + 3, h, header_table_notfreezed1_frm)
    ws.merge_range(8, 4 * (c + 1), 8, (4 * (c + 1)) + 3, h, header_table_notfreezed1_frm)

for x in range(1, len(header_table_notfreezed1) + 1):
    for c,h in enumerate(headers_table_notfreezed2):
        ws.write(1, c + (4 * (x)), h, header_table_notfreezed2_frm)
        ws.write(9, c + (4 * (x)), h, header_table_notfreezed2_frm)

ws.write(9, 20, 'Remarks', header_table_notfreezed2_frm)

####################### DATA ENTRY ROW 3 (Keep existing raw data)
sum_ul_dv_raw = UL.ul_dv.sum()
clean_ul_dv_raw = sum_ul_dv_raw.iloc[1:].tolist()
clean_ul_dv_raw[1], clean_ul_dv_raw[2] = clean_ul_dv_raw[2], clean_ul_dv_raw[1]
for c, item in enumerate(clean_ul_dv_raw):
    ws.write(2, c + 4, item, wb.add_format({'num_format': number_format}))

sum_full_stat_raw = UL.full_stat.sum()
clean_stat_raw = sum_full_stat_raw.iloc[1:].tolist()
for c, item in enumerate(clean_stat_raw):
    ws.write(2, c + 4 * 2, item, wb.add_format({'num_format': number_format}))

sum_diff_raw = []
for i in range(len(clean_ul_dv_raw)):
    sum_diff_raw.append((clean_ul_dv_raw[i] - clean_stat_raw[i]).item())

for c, item in enumerate(sum_diff_raw):
    ws.write(2, c + 4 * 3, item, wb.add_format({'num_format': number_format}))

####################### DATA ENTRY ROW 4 - Excel Formulas
for col_idx in range(4):  # E, F, G, H columns (DV Output)
    col_letter = chr(69 + col_idx)
    formula = f'=SUM({col_letter}11:{col_letter}999)'
    ws.write_formula(3, 4 + col_idx, formula, wb.add_format({'num_format': number_format}))

for col_idx in range(4):  # I, J, K, L columns (Raw Data)
    col_letter = chr(73 + col_idx)
    formula = f'=SUM({col_letter}11:{col_letter}999)'
    ws.write_formula(3, 8 + col_idx, formula, wb.add_format({'num_format': number_format}))

# Checking Results formulas
checking_formulas = ['=E4-I4', '=F4-J4', '=G4-K4', '=H4-L4']
for idx, formula in enumerate(checking_formulas):
    ws.write_formula(3, 12 + idx, formula, wb.add_format({'num_format': number_format}))

######################## Diff row
for x in range(1, len(header_table_notfreezed1)):
    for y in range(len(header_table_notfreezed1)):
        unicode = chr(69 + (y + 4 * x) - 4)
        ws.write_formula(4, y + (4 * x), f'={unicode}3-{unicode}4', wb.add_format({'num_format': number_format,  'bg_color': '#92D050'}))

######################### Row 6 - Excel Formulas for UL data (rows with 'U')
for col_idx in range(12):  # E through P columns
    col_letter = chr(69 + col_idx)
    formula = f'=SUMIF(B11:B999,"U*",{col_letter}11:{col_letter}999)'
    ws.write_formula(5, 4 + col_idx, formula, wb.add_format({'num_format': number_format}))

######################## Diff percentage
diff_percent_formulas = [
    '=IFERROR(ROUND(M4/I4*100,1),0)',
    '=IFERROR(ROUND(N4/J4*100,1),0)', 
    '=IFERROR(ROUND(O4/K4*100,1),0)',
    '=IFERROR(ROUND(P4/L4*100,1),0)'
]
for c, formula in enumerate(diff_percent_formulas):
    ws.merge_range(2, 16 + c, 3, 16 + c, formula, wb.add_format({'num_format': '0.0%', 'bg_color': 'yellow', 'bold': True}))

######################## Combined Lookup table with Excel formulas
combined_table = tst.full_lookup_table
table_size = len(combined_table)

# Write the first 11 columns (Product code through total_fund_Sum)
for x in range(table_size):
    for c in range(11):  # Columns A through K
        item = combined_table.iloc[x, c]
        ws.write(10 + x, c + 1, item, wb.add_format({'num_format': number_format}))

# Write Excel formulas for difference columns M through P
for x in range(table_size):
    row_num = 11 + x
    diff_formulas = [
        f'=E{row_num}-I{row_num}',    # Column M
        f'=F{row_num}-J{row_num}',    # Column N
        f'=G{row_num}-K{row_num}',    # Column O
        f'=H{row_num}-L{row_num}'     # Column P
    ]
    for c, formula in enumerate(diff_formulas):
        ws.write_formula(10 + x, 12 + c, formula, wb.add_format({'num_format': number_format}))

# Write Excel formulas for percentage columns Q through T
for x in range(table_size):
    row_num = 11 + x
    percentage_formulas = [
        f'=IFERROR(M{row_num}/I{row_num},0)',
        f'=IFERROR(N{row_num}/J{row_num},0)',
        f'=IFERROR(O{row_num}/K{row_num},0)',
        f'=IFERROR(P{row_num}/L{row_num},0)'
    ]
    for c, formula in enumerate(percentage_formulas):
        ws.write_formula(10 + x, 16 + c, formula, wb.add_format({'num_format': '0.0%'}))

ws.conditional_format('Q11:T999', {
    'type':     'cell',
    'criteria': '>',
    'value':    0.02,
    'format':   wb.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'}),
})

# Summary Checking AZTRAD SUMMARY SHEET
wtrad = wb.add_worksheet('Summary_Checking_TRAD')

wtrad.freeze_panes(10, 4)

headers_summary = ['Items', 'Total Input from csv', 'Total output in summary', 'Diff', 'AZTRAD']

headers_sum_dict = defaultdict(int)
for h in headers_summary:
    headers_sum_dict[h] = len(h)
max_len = max(headers_sum_dict.items())[1]
wtrad.set_column(1, 19, max_len + 2)
wtrad.set_column(20, 20, max_len * 6)

for c, h in enumerate(headers_summary):
    if h != headers_summary[-1]:
        wtrad.write(c + 1, 3, h, wb.add_format({'bold': True}))
    else:
        wtrad.write(c + 1, 3, h, wb.add_format({'bold': True, 'bg_color': 'yellow'}))

headers_table = ["Product code", "Grouping DV", "Grouping Raw Data"]
for c, h in enumerate(headers_table):
    wtrad.merge_range(8, c + 1, 9, c + 1, h, wb.add_format({'bold': True, 'bg_color': '#002060', 
                                                  'pattern': 1, 'font_color': 'white', 
                                                  'align': 'center', 'valign': 'vcenter'}))

for c,h in enumerate(header_table_notfreezed1):
    wtrad.merge_range(0, 4 * (c + 1), 0, (4 * (c + 1)) + 3, h, header_table_notfreezed1_frm)
    wtrad.merge_range(8, 4 * (c + 1), 8, (4 * (c + 1)) + 3, h, header_table_notfreezed1_frm)

for x in range(1, len(header_table_notfreezed1) + 1):
    for c,h in enumerate(headers_table_notfreezed2):
        wtrad.write(1, c + (4 * (x)), h, header_table_notfreezed2_frm)
        wtrad.write(9, c + (4 * (x)), h, header_table_notfreezed2_frm)

wtrad.write(9, 20, 'Remarks', header_table_notfreezed2_frm)

###################### row 3 (Keep existing raw data for TRAD)
sum_trad_dv_raw = trad.trad_dv.sum()
clean_trad_dv_raw = sum_trad_dv_raw.iloc[1:len(sum_trad_dv_raw) - 1].tolist()
clean_trad_dv_raw[1], clean_trad_dv_raw[2] = clean_trad_dv_raw[2], clean_trad_dv_raw[1]
clean_trad_dv_raw.pop(0)
x = clean_trad_dv_raw.pop(0)
clean_trad_dv_raw.append(x)
clean_trad_dv_raw.append(0)
for c, item in enumerate(clean_trad_dv_raw):
    wtrad.write(2, c + 4, item, wb.add_format({'num_format': number_format}))

def clean_stat_sum(it_path, sum_path):
    # Keep existing function
    df_full = pd.read_csv(it_path, sep=";", encoding="utf-8", on_bad_lines="skip")
    df_sum = pd.read_csv(sum_path, sep=",", encoding="utf-8")

    # POLICY_REF_Count
    total_full = df_full["POLICY_REF_Count"].sum()
    total_summary = df_sum["pol_num_Count"].sum()
    exclude_base_na = df_full.loc[df_full["PRODUCT_CODE"].str.startswith("BASE_NA"), "POLICY_REF_Count"].sum()
    policy_ref = total_full + total_summary - exclude_base_na

    # pre_ann_Sum
    pre_ann_full = df_full["pre_ann_Sum"].sum()
    pre_ann_summary = df_sum["pre_ann_Sum"].sum()
    exclude_base_na2 = df_full.loc[df_full["PRODUCT_CODE"].str.startswith("BASE_NA"), "pre_ann_Sum"].sum()
    pre_ann = pre_ann_full + pre_ann_summary - exclude_base_na2

    # sum_assd_Sum
    assd_full = df_full["sum_assd_Sum"].sum()
    assd_summary = df_sum["sum_assd_Sum"].sum()
    exclude_base_na3 = df_full.loc[df_full["PRODUCT_CODE"].str.startswith("BASE_NA"), "sum_assd_Sum"].sum()
    sum_assured = assd_full + assd_summary - exclude_base_na3

    result = pd.DataFrame([{"policy_ref": policy_ref, "pre_ann_sum": pre_ann, "sum_assured": sum_assured}])
    return result 

sum_trad_stat_raw = clean_stat_sum(IT_AZTRAD_path, SUMMARY_path)
clean_trad_stat_raw_0 = sum_trad_stat_raw.values.tolist()
clean_trad_stat_raw = clean_trad_stat_raw_0[0].copy()
clean_trad_stat_raw[1], clean_trad_stat_raw[2] = clean_trad_stat_raw[2], clean_trad_stat_raw[1]
clean_trad_stat_raw.append(0)
for c, item in enumerate(clean_trad_stat_raw):
    wtrad.write(2, c + 4 * 2, item, wb.add_format({'num_format': number_format}))

sum_trad_diff_raw = []
for i in range(len(clean_trad_dv_raw)):
    sum_trad_diff_raw.append(clean_trad_dv_raw[i] - clean_trad_stat_raw[i])

for c, item in enumerate(sum_trad_diff_raw):
    wtrad.write(2, c + 4 * 3, item, wb.add_format({'num_format': number_format}))

####################### DATA ENTRY ROW 4 - Excel Formulas for TRAD
for col_idx in range(4):  # E, F, G, H columns (DV Output)
    col_letter = chr(69 + col_idx)
    formula = f'=SUM({col_letter}11:{col_letter}999)'
    wtrad.write_formula(3, 4 + col_idx, formula, wb.add_format({'num_format': number_format}))

for col_idx in range(4):  # I, J, K, L columns (Raw Data)
    col_letter = chr(73 + col_idx)
    formula = f'=SUM({col_letter}11:{col_letter}999)'
    wtrad.write_formula(3, 8 + col_idx, formula, wb.add_format({'num_format': number_format}))

# Checking Results formulas
checking_formulas_trad = ['=E4-I4', '=F4-J4', '=G4-K4', '=H4-L4']
for idx, formula in enumerate(checking_formulas_trad):
    wtrad.write_formula(3, 12 + idx, formula, wb.add_format({'num_format': number_format}))

######################### Row 6 - Excel Formulas for TRAD data (rows with 'C*' and 'WPCI77')
for col_idx in range(12):  # E through P columns
    col_letter = chr(69 + col_idx)
    formula = f'=SUMIFS({col_letter}11:{col_letter}999,B11:B999,"C*",B11:B999,"*WPCI77*")'
    wtrad.write_formula(5, 4 + col_idx, formula, wb.add_format({'num_format': number_format}))

######################## Diff percentage for TRAD
diff_percent_formulas_trad = [
    '=IFERROR(ROUND(M6/I4*100,1),0)',
    '=IFERROR(ROUND(N6/J4*100,1),0)',
    '=IFERROR(ROUND(O6/K4*100,1),0)',
    '=IFERROR(ROUND(P6/L4*100,1),0)'
]
for c, formula in enumerate(diff_percent_formulas_trad):
    wtrad.merge_range(2, 16 + c, 3, 16 + c, formula, wb.add_format({'num_format': '0.0%', 'bg_color': 'yellow', 'bold': True}))

################# DIFF ROW
for x in range(1, len(header_table_notfreezed1)):
    for y in range(len(header_table_notfreezed1)):
        unicode = chr(69 + (y + 4 * x) - 4)
        wtrad.write_formula(4, y + (4 * x), f'={unicode}3-{unicode}4', wb.add_format({'num_format': number_format,  'bg_color': '#92D050'}))

################# Combined Lookup Table for TRAD with same merged data
# Write the same combined table data to TRAD sheet
for x in range(table_size):
    for c in range(11):  # Columns A through K
        item = combined_table.iloc[x, c]
        wtrad.write(10 + x, c + 1, item, wb.add_format({'num_format': number_format}))

# Write Excel formulas for difference columns with VLOOKUP
for x in range(table_size):
    row_num = 11 + x
    diff_formulas_trad = [
        f'=E{row_num}-I{row_num}',    # Column M
        f'=F{row_num}-J{row_num}-IFERROR(INDEX(SUMMARY_CAMPAIGN.H:H,MATCH(D{row_num},SUMMARY_CAMPAIGN.B:B,0)),0)',    # Column N with CAMPAIGN lookup
        f'=G{row_num}-K{row_num}+IFERROR(INDEX("Summary BSI".C:C,MATCH(D{row_num},"Summary BSI".A:A,0)),0)',    # Column O with BSI lookup
        f'=H{row_num}-L{row_num}'     # Column P
    ]
    for c, formula in enumerate(diff_formulas_trad):
        wtrad.write_formula(10 + x, 12 + c, formula, wb.add_format({'num_format': number_format}))

# Write Excel formulas for percentage columns Q through T
for x in range(table_size):
    row_num = 11 + x
    percentage_formulas_trad = [
        f'=IFERROR(M{row_num}/I{row_num},0)',
        f'=IFERROR(N{row_num}/J{row_num},0)',
        f'=IFERROR(O{row_num}/K{row_num},0)',
        f'=IFERROR(P{row_num}/L{row_num},0)'
    ]
    for c, formula in enumerate(percentage_formulas_trad):
        wtrad.write_formula(10 + x, 16 + c, formula, wb.add_format({'num_format': '0.0%'}))

wtrad.conditional_format('Q11:T999', {
    'type':     'cell',
    'criteria': '>',
    'value':    0.02,
    'format':   wb.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'}),
})

# SUMMARY_CAMPAIGN SHEET
wcampaign = wb.add_worksheet("SUMMARY_CAMPAIGN")
wcampaign.set_column(1, 1, 14)
wcampaign.set_column(2, 2, 8)
wcampaign.set_column(3, 7, max_len)

header_campaign = ["PRODUCT_CD", "CURRENCY", "GROUPING RAW DATA", "GROUPING DV", "SUM_ASSURED", "Bonus SA", "SA After Bonus"]
header_campaign_frm = wb.add_format({'bold': True, 
                                    'align': 'left',
                                    'top': 1, 'top_color':'black', 'bottom': 1,
                                    'bottom_color': 'black', 'left': 1,'left_color': 'black',
                                    'right': 1,'right_color': 'black'})
header_campaign_frm_tail = wb.add_format({'bold': True, 'bg_color': "#8CA5D8", 'pattern': 1, 
                                    'align': 'left', 
                                    'top': 1, 'top_color':'black', 'bottom': 1,
                                    'bottom_color': 'black', 'left': 1,'left_color': 'black',
                                    'right': 1,'right_color': 'black'})

header_len = len(header_campaign)
for c, h in enumerate(header_campaign):
    wcampaign.write(1, c + 1, h, header_campaign_frm_tail)
for c, h in enumerate(header_campaign[:header_len - 2]):
    wcampaign.write(1,c + 1, h, header_campaign_frm)

campaign_sum = trad.campaign_sum
campaign_sum['Currency'] = campaign_sum['Grouping Raw Data'].str[-3:]
campaign_sum['Product_Cd'] = "BASE_" + campaign_sum['Grouping Raw Data'].str[0:-4]
cols = campaign_sum.columns.tolist()
new_order = ['Product_Cd', 'Currency'] + [c for c in cols if c not in ('Product_Cd', 'Currency')]
campaign_sum = campaign_sum[new_order]

for x in range(len(campaign_sum)):
    for c, item_ in enumerate(campaign_sum.iloc[x]):
        wcampaign.write(2 + x, c + 1, item_, wb.add_format({'num_format': number_format,'top': 1, 'top_color':'black', 'bottom': 1,
                                    'bottom_color': 'black', 'left': 1,'left_color': 'black',
                                    'right': 1,'right_color': 'black'}))

# SUMMARY SHEET WITH EXCEL FORMULAS
wsum = wb.add_worksheet("CONTROL_2_SUMMARY")
wsum.set_column(2, 17, max_len)
wsum.set_column(18, 18, max_len + 5)

for c,h in enumerate(header_table_notfreezed1):
    wsum.merge_range(1, 2 + 4 * c, 1, 2 + 4 * c + 3, h, header_table_notfreezed1_frm)

for x in range(len(header_table_notfreezed1)):
    for c,h in enumerate(headers_table_notfreezed2):
        wsum.write(2, c + 2 +  (4 * (x)), h, header_table_notfreezed2_frm)

wsum.merge_range(1, 18, 2, 18, 'Remarks', wb.add_format({'bold': True, 'bg_color': '#002060', 
                                'pattern': 1, 'font_color': 'white', 
                                'align': 'center', 'valign': 'center',
                                'top': 2, 'top_color':'white', 'bottom': 2,
                                'bottom_color': 'white', 'left': 1,'left_color': 'white',
                                'right': 1,'right_color': 'white', 'valign': 'vcenter'}))
 
wsum.merge_range(1,1,2,1, 'Grouping', wb.add_format({'bold': True, 'bg_color': '#002060', 
                                'pattern': 1, 'font_color': 'white', 
                                'align': 'center', 'valign': 'center',
                                'top': 2, 'top_color':'white', 'bottom': 2,
                                'bottom_color': 'white', 'left': 1,'left_color': 'white',
                                'right': 1,'right_color': 'white', 'valign': 'vcenter'}))

# UL Currency Summary with Excel formulas
ul_currency_data = tst.currency_totals
for x in range(len(ul_currency_data)):
    # Write currency name
    wsum.write(3 + x, 1, ul_currency_data.iloc[x, 0], wb.add_format({'num_format': number_format}))
    
    # Write data columns with Excel formulas for summing UL rows
    for col_idx in range(8):  # 8 data columns
        col_letter = chr(66 + col_idx)  # B, C, D, E, F, G, H, I
        currency_code = ul_currency_data.iloc[x, 0].replace('UL_', '')
        formula = f'=SUMIF("Summary_Checking_UL".D11:D999,"*{currency_code}","Summary_Checking_UL".{col_letter}11:{col_letter}999)'
        wsum.write_formula(3 + x, 2 + col_idx, formula, wb.add_format({'num_format': number_format}))

# Calculate checking results with formulas for UL
for y in range(len(ul_currency_data)):
    row_num = 4 + y
    checking_results_formulas = [
        f'=C{row_num}-F{row_num}',    # Column K
        f'=D{row_num}-G{row_num}',    # Column L  
        f'=E{row_num}-H{row_num}',    # Column M
        f'=F{row_num}-I{row_num}'     # Column N
    ]
    for idx, formula in enumerate(checking_results_formulas):
        wsum.write_formula(3 + y, 10 + idx, formula, wb.add_format({'num_format': number_format}))

# Percentage formulas for UL data
for y in range(len(ul_currency_data)):
    row_num = 4 + y
    percentage_formulas = [
        f'=IFERROR(ABS(K{row_num}/F{row_num}),0)',
        f'=IFERROR(ABS(L{row_num}/G{row_num}),0)',
        f'=IFERROR(ABS(M{row_num}/H{row_num}),0)',
        f'=IFERROR(ABS(N{row_num}/I{row_num}),0)'
    ]
    for idx, formula in enumerate(percentage_formulas):
        wsum.write_formula(3 + y, 14 + idx, formula, wb.add_format({'num_format': '0.0%'}))

# TRAD Currency Summary with Excel formulas
trad_currency_data = tst.agg_all
ul_rows = len(ul_currency_data)

for x in range(len(trad_currency_data)):
    # Write currency name
    wsum.write(3 + ul_rows + x, 1, trad_currency_data.iloc[x, 0], wb.add_format({'num_format': number_format}))
    
    # Write data columns with Excel formulas for summing TRAD rows
    for col_idx in range(8):  # 8 data columns
        col_letter = chr(66 + col_idx)  # B, C, D, E, F, G, H, I
        currency_code = trad_currency_data.iloc[x, 0].replace('TRAD_', '')
        formula = f'=SUMIF("Summary_Checking_TRAD".D11:D999,"*{currency_code}","Summary_Checking_TRAD".{col_letter}11:{col_letter}999)'
        wsum.write_formula(3 + ul_rows + x, 2 + col_idx, formula, wb.add_format({'num_format': number_format}))

# Calculate checking results with formulas for TRAD
for y in range(len(trad_currency_data)):
    row_num = 4 + ul_rows + y
    checking_results_formulas = [
        f'=C{row_num}-F{row_num}',    # Column K
        f'=D{row_num}-G{row_num}',    # Column L
        f'=E{row_num}-H{row_num}',    # Column M
        f'=F{row_num}-I{row_num}'     # Column N
    ]
    for idx, formula in enumerate(checking_results_formulas):
        wsum.write_formula(3 + ul_rows + y, 10 + idx, formula, wb.add_format({'num_format': number_format}))

# Percentage formulas for TRAD data
for y in range(len(trad_currency_data)):
    row_num = 4 + ul_rows + y
    percentage_formulas = [
        f'=IFERROR(ABS(K{row_num}/F{row_num}),0)',
        f'=IFERROR(ABS(L{row_num}/G{row_num}),0)',
        f'=IFERROR(ABS(M{row_num}/H{row_num}),0)',
        f'=IFERROR(ABS(N{row_num}/I{row_num}),0)'
    ]
    for idx, formula in enumerate(percentage_formulas):
        wsum.write_formula(3 + ul_rows + y, 14 + idx, formula, wb.add_format({'num_format': '0.0%'}))

wsum.conditional_format('O4:R999', {
    'type':     'cell',
    'criteria': '>',
    'value':    0.02,
    'format':   wb.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'}),
})

# BSI SUMMARY SHEET
w_bsi = wb.add_worksheet("Summary BSI")

w_bsi.set_column(0, 0, 14) 
w_bsi.set_column(1, 1, 18)   
w_bsi.set_column(2, 2, 14)   

header_format = wb.add_format({
    'bold': True,
    'bg_color': '#8CA5D8',
    'border': 1,
    'align': 'center'
})

cell_format_str = wb.add_format({'border': 1})
cell_format_num = wb.add_format({'num_format': '#,##0', 'border': 1})

headers = ['Cover_code', 'product_group', 'anp']
for col_num, header in enumerate(headers):
    w_bsi.write(0, col_num, header, header_format)

for row_num, row_data in trad.bsi_merge.iterrows():
    w_bsi.write(row_num + 1, 0, row_data['Cover_code'], cell_format_str)
    w_bsi.write(row_num + 1, 1, row_data['product_group'], cell_format_str)
    w_bsi.write(row_num + 1, 2, row_data['anp'], cell_format_num)

wb.close()
end_time = time.time()
elapsed_time(start_time, end_time)