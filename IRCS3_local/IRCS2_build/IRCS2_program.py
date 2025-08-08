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

####################### DATA ENTRY ROW 3
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


####################### DATA ENTRY ROW 4
sum_ul_dv = UL.summary_ul_dv_final
for c, item in enumerate(sum_ul_dv.iloc[0]):
    ws.write(3, c + 4, item, wb.add_format({'num_format': number_format}))

sum_full_stat = UL.summary_full_stat_total
for c, item in enumerate(sum_full_stat.iloc[0]):
    ws.write(3, c + 4 * 2, item, wb.add_format({'num_format': number_format}))

sum_diff_total = UL.summary_diff_total
for c, item in enumerate(sum_diff_total.iloc[0]):
    ws.write(3, c + 4 * 3, item, wb.add_format({'num_format': number_format}))
    
    
######################## Diff row
for x in range(1, len(header_table_notfreezed1)):
    for y in range(len(header_table_notfreezed1)):
        unicode = chr(69 + (y + 4 * x) - 4)
        ws.write_formula(4, y + (4 * x), f'={unicode}3-{unicode}4', wb.add_format({'num_format': number_format,  'bg_color': '#92D050'}))
    
######################### Row 6
sum_ul_dv = UL.summary_ul_dv_final
for c, item in enumerate(sum_ul_dv.iloc[0]):
    ws.write(5, c + 4, item, wb.add_format({'num_format': number_format}))

sum_full_stat = UL.summary_full_stat_total
for c, item in enumerate(sum_full_stat.iloc[0]):
    ws.write(5, c + 4 * 2, item, wb.add_format({'num_format': number_format}))

sum_diff_total = UL.summary_diff_total
for c, item in enumerate(sum_diff_total.iloc[0]):
    ws.write(5, c + 4 * 3, item, wb.add_format({'num_format': number_format}))

######################## Diff percentage
sum_diff_percent = UL.Different_Percentage_of_Checking_Result_to_Raw_Data
for c in range(len(header_table_notfreezed1)):
    unicode = chr(77 + c)
    formula = f'=IFERROR(abs({unicode}{4}/{chr(ord(unicode) - 4)}{4}),0)'
    ws.merge_range(2, 16 + c, 3, 16 + c, formula, wb.add_format({'num_format': '0.0', 'bg_color': 'yellow', 'bold': True}))

######################## Lookup table

table1 = tst.full_lookup_table.iloc[:,0:15]
table2 = tst.full_lookup_table.iloc[:,15:]
for x in range(len(table1)):
    for c, item in enumerate(table1.iloc[x]):
        ws.write(10 + x, c + 1, item, wb.add_format({'num_format': number_format}))
for x in range(len(table2)):
    for c, item in enumerate(table2.iloc[x]):
        if type(item) == numpy.float64:
            item = round(item,1)
        ws.write(10 + x, 16 + c, item, wb.add_format({'num_format': '%'}))

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
    wtrad.merge_range(0, 4 * (c + 1), 0, (4 * (c + 1)) + 3, h, header_table_notfreezed1_frm)
    wtrad.merge_range(8, 4 * (c + 1), 8, (4 * (c + 1)) + 3, h, header_table_notfreezed1_frm)

for x in range(1, len(header_table_notfreezed1) + 1):
    for c,h in enumerate(headers_table_notfreezed2):
        wtrad.write(1, c + (4 * (x)), h, header_table_notfreezed2_frm)
        wtrad.write(9, c + (4 * (x)), h, header_table_notfreezed2_frm)

wtrad.write(9, 20, 'Remarks', header_table_notfreezed2_frm)

###################### row 3

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
    
    # 1) Load the two tables
    df_full = pd.read_csv(
    it_path,  # path to your full‐stat CSV
    sep=";",                    # adjust if it’s not semicolon-delimited
    encoding="utf-8",
    on_bad_lines="skip",
    )
    df_sum = pd.read_csv(
    sum_path,              # path to your Summary sheet export
    sep=",",                    # adjust for your file’s delimiter
    encoding="utf-8",
    )

    # 2) Compute the three “SUM(…)+SUM(…)-SUMIFS(…)" values:

    # a) POLICY_REF_Count
    total_full       = df_full["POLICY_REF_Count"].sum()
    total_summary    = df_sum["pol_num_Count"].sum()
    exclude_base_na  = df_full.loc[
        df_full["PRODUCT_CODE"].str.startswith("BASE_NA"),
        "POLICY_REF_Count"
    ].sum()
    policy_ref = total_full + total_summary - exclude_base_na

    # b) pre_ann_Sum
    pre_ann_full     = df_full["pre_ann_Sum"].sum()
    pre_ann_summary  = df_sum["pre_ann_Sum"].sum()
    exclude_base_na2 = df_full.loc[
        df_full["PRODUCT_CODE"].str.startswith("BASE_NA"),
        "pre_ann_Sum"
    ].sum()
    pre_ann = pre_ann_full + pre_ann_summary - exclude_base_na2

    # c) sum_assd_Sum (sum assured)
    assd_full        = df_full["sum_assd_Sum"].sum()
    assd_summary     = df_sum["sum_assd_Sum"].sum()
    exclude_base_na3 = df_full.loc[
        df_full["PRODUCT_CODE"].str.startswith("BASE_NA"),
        "sum_assd_Sum"
    ].sum()
    sum_assured = assd_full + assd_summary - exclude_base_na3

    # 3) Pack into a one-row DataFrame
    result = pd.DataFrame([{
        "policy_ref":  policy_ref,
        "pre_ann_sum": pre_ann,
        "sum_assured": sum_assured
    }])

    return result 

sum_trad_stat_raw = clean_stat_sum(IT_AZTRAD_path, SUMMARY_path)

clean_trad_stat_raw_0 = sum_trad_stat_raw.values.tolist()
clean_trad_stat_raw = clean_trad_stat_raw_0[0].copy()
clean_trad_stat_raw[1], clean_trad_stat_raw[2] = clean_trad_stat_raw[2], clean_trad_stat_raw[1]
clean_trad_stat_raw.append(0)
for c, item in enumerate(clean_trad_stat_raw):
    wtrad.write(2, c + 4 * 2, item, wb.add_format({'num_format': number_format}))

for c in range(len(header_table_notfreezed1)):
    formula = f"=SUM({chr(73 + c)}11:{chr(73 + c)}897)"
    wtrad.write(3, c + 4 * 2, formula, wb.add_format({'num_format': number_format}))
    wtrad.write(5, c + 4 * 2, formula, wb.add_format({'num_format': number_format}))

sum_trad_dv_final = trad.summary_trad_dv_final.sum()
clean_trad_dv_final = sum_trad_dv_final.tolist()
clean_trad_dv_final.append(0)
for c, item in enumerate(clean_trad_dv_final):
    wtrad.write(3, c + 4, item, wb.add_format({'num_format': number_format}))
    wtrad.write(5, c + 4, item, wb.add_format({'num_format': number_format}))

for c in range(len(clean_trad_stat_raw)):
    formula = f"=SUM({chr(77 + c)}11:{chr(77 + c)}897)"
    wtrad.write(5,c + 4 * 3, formula, wb.add_format({'num_format': number_format}))

sum_trad_diff_raw = []
for i in range(len(clean_trad_dv_raw)):
    sum_trad_diff_raw.append(clean_trad_dv_raw[i] - clean_trad_stat_raw[i])

for c, item in enumerate(sum_trad_diff_raw):
    wtrad.write(2, c + 4 * 3, item, wb.add_format({'num_format': number_format}))

sum_diff_aztrad_output = trad.sum_diff_aztrad_output
for c, item in enumerate(sum_diff_aztrad_output.iloc[0]):
    wtrad.write(3, c + 4 * 3, item, wb.add_format({'num_format': number_format}))
    
sum_diff_trad_percent = trad.Different_Percentage
for c in range(len(header_table_notfreezed1)):
    unicode = chr(77 + c)
    formula = f'=IFERROR(abs({unicode}{6}/{chr(ord(unicode) - 4)}{4}),0)'
    wtrad.merge_range(2, 16 + c, 3, 16 + c, formula, wb.add_format({'num_format': '0.0', 'bg_color': 'yellow', 'bold': True}))


################# DIFF ROW
for x in range(1, len(header_table_notfreezed1)):
    for y in range(len(header_table_notfreezed1)):
        unicode = chr(69 + (y + 4 * x) - 4)
        wtrad.write_formula(4, y + (4 * x), f'={unicode}3-{unicode}4', wb.add_format({'num_format': number_format,  'bg_color': '#92D050'}))
    
################# LOOKUP TABLE
table1 = tst.merged4.iloc[:,0:15]
table2 = tst.merged4.iloc[:,15:]
for x in range(len(table1)):
    for c, item in enumerate(table1.iloc[x]):
        wtrad.write(10 + x, c + 1, item, wb.add_format({'num_format': number_format}))
for x in range(len(table2)):
    for c, item in enumerate(table2.iloc[x]):
        if type(item) == numpy.float64:
            item = round(item,1)
        wtrad.write(10 + x, 16 + c, item, wb.add_format({'num_format': '%'}))

wtrad.conditional_format('Q11:T999', {
    'type':     'cell',
    'criteria': '>',
    'value':    0.02,
    'format':   wb.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'}),
})

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

# SUMMARY SHEET
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

currency_summary = tst.currency_totals
for x in range(len(currency_summary)):
    for c, item in enumerate(currency_summary.iloc[x]):
        wsum.write(3 + x, c + 1, item, wb.add_format({'num_format': number_format}))

for y in range(len(currency_summary)):
    for x in range(len(header_table_notfreezed1)):
        unicode = chr(75 + x)
        formula = f'=IFERROR(abs({unicode}{4 + y}/{chr(ord(unicode) - 4)}{4 + y}),0)'
        wsum.write_formula(3 + y, 14 + x, formula, wb.add_format({'num_format': '0.0'}))
 
currency_summary_trad = tst.agg_all
for x in range(len(currency_summary_trad)):
    for c, item in enumerate(currency_summary_trad.iloc[x]):
        wsum.write(5 + x, c + 1, item, wb.add_format({'num_format': number_format}))

for y in range(len(currency_summary_trad)):
    for x in range(len(header_table_notfreezed1)):
        unicode = chr(75 + x)
        formula = f'=IFERROR(abs({unicode}{6 + y}/{chr(ord(unicode) - 4)}{6 + y}),0)'
        wsum.write_formula(5 + y, 14 + x, formula, wb.add_format({'num_format': '0.0'})) 
 
wsum.conditional_format('O4:R999', {
    'type':     'cell',
    'criteria': '>',
    'value':    0.02,
    'format':   wb.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'}),
})

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