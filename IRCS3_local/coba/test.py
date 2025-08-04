from ul_trad import run_trad
import pandas as pd
import openpyxl

input_sheet = r"D:\RUN 3\control_3\control_3\IRCS3_local\Input Sheet_IRCS3.xlsx"

trad = pd.read_excel(input_sheet, sheet_name = 'FILTER_TRAD', engine = 'openpyxl')
params = trad.iloc[0].to_dict()

results = {}
for i, row in trad.iterrows():
    params = row.to_dict()
    res = run_trad(params)
    run_name = params.get('run_name', f'run_{i}')
    results[run_name] = res

print(trad[['run_name']])
print(results)