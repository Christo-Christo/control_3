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
percentage_format = '0.0%'

# Summary checking AZUL SHEET
ws = wb.add_worksheet('Summary_Checking_UL')
merged_data = UL.merged 
ul_last_row = 10 + len(merged_data)
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

####################### DATA ENTRY ROW 3 (UL)
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

####################### DATA ENTRY ROW 4 (UL) - Replace with Excel formulas
# Row 4: Sum formulas from E11 to L (columns 4-11)
for col in range(4, 12):  # E to L (columns 4-11)
    col_letter = chr(ord('A') + col)
    formula = f'=SUM({col_letter}11:{col_letter}{ul_last_row})'
    ws.write_formula(3, col, formula, wb.add_format({'num_format': number_format}))

# Columns M to P (12-15): Difference formulas
ws.write_formula(3, 12, '=E4-I4', wb.add_format({'num_format': number_format}))  # M4
ws.write_formula(3, 13, '=F4-J4', wb.add_format({'num_format': number_format}))  # N4
ws.write_formula(3, 14, '=G4-K4', wb.add_format({'num_format': number_format}))  # O4
ws.write_formula(3, 15, '=H4-L4', wb.add_format({'num_format': number_format}))  # P4
    
######################## Diff row
for x in range(1, len(header_table_notfreezed1)):
    for y in range(len(header_table_notfreezed1)):
        unicode = chr(69 + (y + 4 * x) - 4)
        ws.write_formula(4, y + (4 * x), f'={unicode}3-{unicode}4', wb.add_format({'num_format': number_format,  'bg_color': '#92D050'}))
    
######################### Row 6 (UL) - Sum formulas with condition for names starting with "U"
# For columns E to P, sum where column B (product names) starts with "U"
for col in range(4, 12):
    col_letter = chr(ord('A') + col)
    formula = f'=SUMIF(B11:B{ul_last_row},"U*",{col_letter}11:{col_letter}{ul_last_row})'
    ws.write_formula(5, col, formula, wb.add_format({'num_format': number_format}))

for col in range(12, 16):
    col_letter = chr(ord('A') + col)
    formula = f'=SUM({col_letter}11:{col_letter}{ul_last_row})'
    ws.write_formula(5, col, formula, wb.add_format({'num_format': number_format}))

######################## Diff percentage (UL)
# Columns Q to T percentage formulas
ws.write_formula(2, 16, '=IFERROR(ROUND(M4/I4,3),0)', wb.add_format({'num_format': percentage_format, 'bg_color': 'yellow', 'bold': True}))
ws.write_formula(2, 17, '=IFERROR(ROUND(N4/J4,3),0)', wb.add_format({'num_format': percentage_format, 'bg_color': 'yellow', 'bold': True}))
ws.write_formula(2, 18, '=IFERROR(ROUND(O4/K4,3),0)', wb.add_format({'num_format': percentage_format, 'bg_color': 'yellow', 'bold': True}))
ws.write_formula(2, 19, '=IFERROR(ROUND(P4/L4,3),0)', wb.add_format({'num_format': percentage_format, 'bg_color': 'yellow', 'bold': True}))

# Merge the percentage cells
ws.merge_range(2, 16, 3, 16, '=IFERROR(ROUND(M4/I4,3),0)', wb.add_format({'num_format': percentage_format, 'bg_color': 'yellow', 'bold': True}))
ws.merge_range(2, 17, 3, 17, '=IFERROR(ROUND(N4/J4,3),0)', wb.add_format({'num_format': percentage_format, 'bg_color': 'yellow', 'bold': True}))
ws.merge_range(2, 18, 3, 18, '=IFERROR(ROUND(O4/K4,3),0)', wb.add_format({'num_format': percentage_format, 'bg_color': 'yellow', 'bold': True}))
ws.merge_range(2, 19, 3, 19, '=IFERROR(ROUND(P4/L4,3),0)', wb.add_format({'num_format': percentage_format, 'bg_color': 'yellow', 'bold': True}))

######################## Lookup table (UL)
for x in range(len(merged_data)):
    for c, item in enumerate(merged_data.iloc[x]):
        if c < 15:  # First 15 columns
            ws.write(10 + x, c + 1, item, wb.add_format({'num_format': number_format}))

for r in range(11, 11 + len(merged_data)):
    ws.write_formula(f'M{r}', f'=E{r}-I{r}', wb.add_format({'num_format': number_format}))
    ws.write_formula(f'N{r}', f'=F{r}-J{r}', wb.add_format({'num_format': number_format}))
    ws.write_formula(f'O{r}', f'=G{r}-K{r}', wb.add_format({'num_format': number_format}))
    ws.write_formula(f'P{r}', f'=H{r}-L{r}', wb.add_format({'num_format': number_format}))

    ws.write_formula(f'Q{r}', f'=IFERROR(M{r}/I{r},0)', wb.add_format({'num_format': percentage_format}))
    ws.write_formula(f'R{r}', f'=IFERROR(N{r}/J{r},0)', wb.add_format({'num_format': percentage_format}))
    ws.write_formula(f'S{r}', f'=IFERROR(O{r}/K{r},0)', wb.add_format({'num_format': percentage_format}))
    ws.write_formula(f'T{r}', f'=IFERROR(P{r}/L{r},0)', wb.add_format({'num_format': percentage_format}))


ws.conditional_format('Q11:T999', {
    'type':     'cell',
    'criteria': '!=',
    'value':    0,
    'format':   wb.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'}),
})

# Summary Checking AZTRAD SUMMARY SHEET
wtrad = wb.add_worksheet('Summary_Checking_TRAD')
merged_trad_data = trad.merged 
trad_last_row = 10 + len(merged_trad_data)
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

###################### row 3 (TRAD)
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
    df_full = pd.read_csv(
    it_path,
    sep=";",
    encoding="utf-8",
    on_bad_lines="skip",
    )
    df_sum = pd.read_csv(
    sum_path,
    sep=",",
    encoding="utf-8",
    )

    total_full       = df_full["POLICY_REF_Count"].sum()
    total_summary    = df_sum["pol_num_Count"].sum()
    exclude_base_na  = df_full.loc[
        df_full["PRODUCT_CODE"].str.startswith("BASE_NA"),
        "POLICY_REF_Count"
    ].sum()
    policy_ref = total_full + total_summary - exclude_base_na

    pre_ann_full     = df_full["pre_ann_Sum"].sum()
    pre_ann_summary  = df_sum["pre_ann_Sum"].sum()
    exclude_base_na2 = df_full.loc[
        df_full["PRODUCT_CODE"].str.startswith("BASE_NA"),
        "pre_ann_Sum"
    ].sum()
    pre_ann = pre_ann_full + pre_ann_summary - exclude_base_na2

    assd_full        = df_full["sum_assd_Sum"].sum()
    assd_summary     = df_sum["sum_assd_Sum"].sum()
    exclude_base_na3 = df_full.loc[
        df_full["PRODUCT_CODE"].str.startswith("BASE_NA"),
        "sum_assd_Sum"
    ].sum()
    sum_assured = assd_full + assd_summary - exclude_base_na3

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

sum_diff_raw = []
for i in range(len(clean_trad_dv_raw)):
    sum_diff_raw.append((clean_trad_dv_raw[i] - clean_trad_stat_raw[i]))

for c, item in enumerate(sum_diff_raw):
    wtrad.write(2, c + 4 * 3, item, wb.add_format({'num_format': number_format}))
####################### DATA ENTRY ROW 4 (TRAD) - Replace with Excel formulas
# Row 4: Sum formulas from E11 to L (columns 4-11)
for col in range(4, 12):  # E to L (columns 4-11)
    col_letter = chr(ord('A') + col)
    formula = f'=SUM({col_letter}11:{col_letter}{trad_last_row})'
    wtrad.write_formula(3, col, formula, wb.add_format({'num_format': number_format}))

# Columns M to P (12-15): Difference formulas
wtrad.write_formula(3, 12, '=E4-I4', wb.add_format({'num_format': number_format}))  # M4
wtrad.write_formula(3, 13, '=F4-J4', wb.add_format({'num_format': number_format}))  # N4
wtrad.write_formula(3, 14, '=G4-K4', wb.add_format({'num_format': number_format}))  # O4
wtrad.write_formula(3, 15, '=H4-L4', wb.add_format({'num_format': number_format}))  # P4

for col in range(4, 16):
    col_letter = chr(ord('A') + col)
    # formula: =E3 - E4 (dst)
    formula = f'={col_letter}3-{col_letter}4'
    wtrad.write_formula(4, col, formula, wb.add_format({'num_format': number_format,  'bg_color': '#92D050'}))

# ROW 6: SUMIFS dengan 2 kriteria: mulai "C" dan mengandung "WPCI77"
for col in range(4, 12):
    col_letter = chr(ord('A') + col)
    formula = (
        f'=SUMIF(B11:B{trad_last_row},"C*",{col_letter}11:{col_letter}{trad_last_row}) + '
        f'SUMIF(B11:B{trad_last_row},"*WPCI77*",{col_letter}11:{col_letter}{trad_last_row})'
    )
    wtrad.write_formula(5, col, formula, wb.add_format({'num_format': number_format}))

for col in range(12, 16):
    col_letter = chr(ord('A') + col)
    formula = f'=SUM({col_letter}11:{col_letter}{trad_last_row})'
    wtrad.write_formula(5, col, formula, wb.add_format({'num_format': number_format}))
    
######################## Diff percentage (TRAD)
# Columns Q to T percentage formulas - note different row reference for TRAD
wtrad.write_formula(2, 16, '=IFERROR(ROUND(M6/I4,3),0)', wb.add_format({'num_format': percentage_format, 'bg_color': 'yellow', 'bold': True}))
wtrad.write_formula(2, 17, '=IFERROR(ROUND(N6/J4,3),0)', wb.add_format({'num_format': percentage_format, 'bg_color': 'yellow', 'bold': True}))
wtrad.write_formula(2, 18, '=IFERROR(ROUND(O6/K4,3),0)', wb.add_format({'num_format': percentage_format, 'bg_color': 'yellow', 'bold': True}))
wtrad.write_formula(2, 19, '=IFERROR(ROUND(P6/L4,3),0)', wb.add_format({'num_format': percentage_format, 'bg_color': 'yellow', 'bold': True}))

# Merge the percentage cells
wtrad.merge_range(2, 16, 3, 16, '=IFERROR(ROUND(M6/I4,3),0)', wb.add_format({'num_format': percentage_format, 'bg_color': 'yellow', 'bold': True}))
wtrad.merge_range(2, 17, 3, 17, '=IFERROR(ROUND(N6/J4,3),0)', wb.add_format({'num_format': percentage_format, 'bg_color': 'yellow', 'bold': True}))
wtrad.merge_range(2, 18, 3, 18, '=IFERROR(ROUND(O6/K4,3),0)', wb.add_format({'num_format': percentage_format, 'bg_color': 'yellow', 'bold': True}))
wtrad.merge_range(2, 19, 3, 19, '=IFERROR(ROUND(P6/L4,3),0)', wb.add_format({'num_format': percentage_format, 'bg_color': 'yellow', 'bold': True}))
    
################# LOOKUP TABLE (TRAD)
for x in range(len(merged_trad_data)):
    for c, item in enumerate(merged_trad_data.iloc[x]):
        if c < 15:  # First 15 columns
            wtrad.write(10 + x, c + 1, item, wb.add_format({'num_format': number_format}))

for r in range(11, 11 + len(merged_trad_data)):
    wtrad.write_formula(f'M{r}', f'=E{r}-I{r}', wb.add_format({'num_format': number_format}))
    wtrad.write_formula(f'N{r}', f'=F{r}-J{r}-IFERROR(INDEX(SUMMARY_CAMPAIGN!G$3:G$1048576, MATCH(D{r}, SUMMARY_CAMPAIGN!D$3:D$1048576, 0)), 0)', wb.add_format({'num_format': number_format}))
    wtrad.write_formula(
        f'O{r}',
        f'=G{r}-K{r}+IFERROR(INDEX(\'Summary BSI\'!C2:C999, MATCH(D{r}, \'Summary BSI\'!B2:B999, 0)), 0)',
        wb.add_format({'num_format': number_format})
    )
    wtrad.write_formula(f'P{r}', f'=H{r}-L{r}', wb.add_format({'num_format': number_format}))

    wtrad.write_formula(f'R{r}', f'=IFERROR(N{r}/J{r},0)', wb.add_format({'num_format': percentage_format}))
    wtrad.write_formula(f'Q{r}', f'=IFERROR(M{r}/I{r},0)', wb.add_format({'num_format': percentage_format}))
    wtrad.write_formula(f'S{r}', f'=IFERROR(O{r}/K{r},0)', wb.add_format({'num_format': percentage_format}))
    wtrad.write_formula(f'T{r}', f'=IFERROR(P{r}/L{r},0)', wb.add_format({'num_format': percentage_format}))


# Add formulas for columns Q to T (percentage calculations)
for row in range(len(merged_trad_data)):
    wtrad.write_formula(10 + row, 16, f'=IFERROR(M{11+row}/I{11+row},0)', wb.add_format({'num_format': percentage_format}))  # Q
    wtrad.write_formula(10 + row, 17, f'=IFERROR(N{11+row}/J{11+row},0)', wb.add_format({'num_format': percentage_format}))  # R
    wtrad.write_formula(10 + row, 18, f'=IFERROR(O{11+row}/K{11+row},0)', wb.add_format({'num_format': percentage_format}))  # S
    wtrad.write_formula(10 + row, 19, f'=IFERROR(P{11+row}/L{11+row},0)', wb.add_format({'num_format': percentage_format}))  # T

wtrad.conditional_format('Q11:T999', {
    'type':     'cell',
    'criteria': '!=',
    'value':    0,
    'format':   wb.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'}),
})

# SUMMARY_CAMPAIGN sheet (unchanged)
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

# CONTROL_2_SUMMARY SHEET - Modified as requested
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

# Add the specific groupings as requested
groupings = ["UL_IDR", "UL_USD", "TRAD_IDR", "TRAD_USD"]
for row, group in enumerate(groupings):
    wsum.write(3 + row, 1, group, wb.add_format({'num_format': number_format}))

# UL_IDR row (row 4) - formulas to sum from Summary_Checking_UL where column D contains IDR
for col in range(2, 10):  # Columns C to J (2 to 9)
    col_letter_source = chr(ord('A') + col + 2)  # Maps to E, F, G, etc. in source sheet
    formula = f'=SUMIF(Summary_Checking_UL!D:D,"*IDR*",Summary_Checking_UL!{col_letter_source}:{col_letter_source})'
    wsum.write_formula(3, col, formula, wb.add_format({'num_format': number_format}))

# UL_USD row (row 5) - formulas to sum from Summary_Checking_UL where column D contains USD
for col in range(2, 10):  # Columns C to J (2 to 9)
    col_letter_source = chr(ord('A') + col + 2)  # Maps to E, F, G, etc. in source sheet
    formula = f'=SUMIF(Summary_Checking_UL!D:D,"*USD*",Summary_Checking_UL!{col_letter_source}:{col_letter_source})'
    wsum.write_formula(4, col, formula, wb.add_format({'num_format': number_format}))

# TRAD_IDR row (row 6) - formulas to sum from Summary_Checking_TRAD where column D contains IDR
for col in range(2, 10):  # Columns C to J (2 to 9)
    col_letter_source = chr(ord('A') + col + 2)  # Maps to E, F, G, etc. in source sheet
    formula = f'=SUMIF(Summary_Checking_TRAD!D:D,"*IDR*",Summary_Checking_TRAD!{col_letter_source}:{col_letter_source})'
    wsum.write_formula(5, col, formula, wb.add_format({'num_format': number_format}))

# TRAD_USD row (row 7) - formulas to sum from Summary_Checking_TRAD where column D contains USD
for col in range(2, 10):  # Columns C to J (2 to 9)
    col_letter_source = chr(ord('A') + col + 2)  # Maps to E, F, G, etc. in source sheet
    formula = f'=SUMIF(Summary_Checking_TRAD!D:D,"*USD*",Summary_Checking_TRAD!{col_letter_source}:{col_letter_source})'
    wsum.write_formula(6, col, formula, wb.add_format({'num_format': number_format}))

last_row = 7

for row in range(4, last_row + 1):  # Excel rows dimulai dari 1
    wsum.write_formula(f'K{row}', f'=C{row}-G{row}', wb.add_format({'num_format': number_format}))
    wsum.write_formula(f'L{row}', f'=D{row}-H{row}', wb.add_format({'num_format': number_format}))
    wsum.write_formula(f'M{row}', f'=E{row}-I{row}', wb.add_format({'num_format': number_format}))
    wsum.write_formula(f'N{row}', f'=F{row}-J{row}', wb.add_format({'num_format': number_format}))


# Add percentage formulas for columns O to R (14 to 17)
for row in range(4):  # 4 rows of data
    for col in range(4):  # 4 columns of percentages
        source_col = chr(ord('C') + col + 8)  
        base_col = chr(ord('C') + col + 4)  
        formula = f'=IFERROR(ROUND({source_col}{4+row}/{base_col}{4+row},3),0)'
        wsum.write_formula(3 + row, 14 + col, formula, wb.add_format({'num_format': percentage_format}))
 
wsum.conditional_format('O4:R999', {
    'type':     'cell',
    'criteria': '!=',
    'value':    0,
    'format':   wb.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'}),
})

# Summary BSI sheet (unchanged)
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