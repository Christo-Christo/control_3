import pandas as pd

CODE_LIBRARY_path = r"D:\Run Control 2\IRCS2_build\Input Sheet.xlsx"




















input_df = (pd.read_excel(CODE_LIBRARY_path, engine='openpyxl', sheet_name=['PATH INPUT']))['PATH INPUT']
path_map = dict(zip(input_df['Category'], input_df['Path']))

reporting_month    = path_map.get('Reporting Month')
financial_year       = path_map.get('Financial Year')
DV_AZTRAD_path       = path_map.get('DV_AZTRAD')
DV_AZUL_path         = path_map.get('DV_AZUL')
IT_AZTRAD_path       = path_map.get('IT_AZTRAD')
IT_AZUL_path         = path_map.get('IT_AZUL')
SUMMARY_path         = path_map.get('SUMMARY')
LGC_LGM_CAMPAIGN_path= path_map.get('LGC_LGM_Campaign')
BSI_ATTRIBUSI_path   = path_map.get('BSI Attribusi')
TRADCONV_path        = path_map.get('RESERVE_TRADCONV_RWNB_IFRS')
TRADSHA_path         = path_map.get('RESERVE_TRADSHA_RWNB_IFRS')
xlsx_filename        = path_map.get('Output filename')
xlsx_output          = "\\".join([x for x in DV_AZTRAD_path.split('\\')]
                                 [:len(DV_AZTRAD_path.split('\\')) - 1 ]) + "\\" + xlsx_filename + ".xlsx"



# tradconv_txt = path input here
# tradsha_txt = path input here

# cleaned_columns = ['POLICY_REF', 'PRODUCT_CODE', 'COVER_CODE', 'SUM_INSURED', 'CURRENCY1', 'POLICY_START_DATE']
# df = pd.read_csv(RESERVE_TRADCONV_RWNB_IFRS_2025_path, sep=';')
# print(df.columns)