import os
import pandas as pd
import glob
from pyxlsb import open_workbook as open_xlsb

file = "ATLEOS OCP IND POR_FEB-17-2025 v1"

path = r"E:\_Projects\IND POR\Report\\"
path_out = r"E:\_Projects\IND POR\changes\csv\\"

por = pd.read_excel(path + file + '.xlsb', sheet_name="Data",engine = "pyxlsb", keep_default_na='')
    
# Remove columns that contain '$' in suffix
por = por.loc[:, ~por.columns.astype(str).str.endswith('$')]

# Convert date columns into rows
por = por.melt(id_vars=[
    "Demand Stream", "Region1", "Area2", "Region", "Theatre", "Area", "Country Name", "Country Code", 
    "Bill to Country", "Zone", "Customer LOB", "Product LOB", "Product Range", "Parent Model", "Class", 
    "Description", "DTF", "DTF cutoff", "Start Date", "End Date", "DTF risk units", "DTF risk", 
    "Organization Code", "KeyAccount_Billing", "Key Account", "Master Customer", "Item Type", "ASP", 
    "Data Series", "Comments"], var_name="Date", value_name="qty")
# print(df_melted)

qty_ = 'qty'

por = por[(por[qty_] != '') & (por[qty_].notna())]

por.to_csv(path_out + file + '.csv',index=False)
        