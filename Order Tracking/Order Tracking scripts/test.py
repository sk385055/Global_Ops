
import pandas as pd
from pathlib import Path
from typing import Optional
from docx import Document  # python-docx is available in the environment

doc_file1 = 'Global DOC Atleos 20251103'
doc_file2 = 'Global DOC Atleos 20251110'
doc_file3 = 'Global DOC Atleos 20251117'
doc_file4 = 'Global DOC Atleos 20251124'

file_stems = [doc_file1, doc_file2, doc_file3, doc_file4]

path = 'E:\\_Projects\\regional_tls\\input\\'

def ord_alloc(ord):
    pid_counter = {}
    result = []
    
    for index, row in ord.iterrows():
        
        for _ in range(row["Supply Qty"]):
            result.append({
                **row.to_dict(),
                "Supply Qty": 1,
            })
    
    return pd.DataFrame(result)

for ord_file in file_stems:
    ord = pd.read_excel(path + ord_file + '.xlsb', sheet_name='Orders', engine='pyxlsb')
    ord = ord.sort_values(by=['PID', 'Whse', 'Ship Date'])
    ord_out = ord_alloc(ord)
    
    
    # Convert the columns to datetime format (DMY)
    ord_out["Booked Dt"] = pd.to_datetime(ord_out["Booked Dt"], origin='1899-12-30', unit='D').dt.strftime('%d-%m-%Y')
    ord_out["Request Dt"] = pd.to_datetime(ord_out["Request Dt"], origin='1899-12-30', unit='D').dt.strftime('%d-%m-%Y')
    ord_out["SAD"] = pd.to_datetime(ord_out["SAD"], origin='1899-12-30', unit='D').dt.strftime('%d-%m-%Y')
    ord_out["Ship Date"] = pd.to_datetime(ord_out["Ship Date"], origin='1899-12-30', unit='D').dt.strftime('%d-%m-%Y')
    ord_out["PRD"] = pd.to_datetime(ord_out["PRD"], origin='1899-12-30', unit='D').dt.strftime('%d-%m-%Y')
    ord_out["IO SSD"] = pd.to_datetime(ord_out["IO SSD"], origin='1899-12-30', unit='D').dt.strftime('%d-%m-%Y')
    ord_out["IO SAD"] = pd.to_datetime(ord_out["IO SAD"], origin='1899-12-30', unit='D').dt.strftime('%d-%m-%Y')
    ord_out["PD_PO"] = pd.to_datetime(ord_out["PD_PO"], origin='1899-12-30', unit='D').dt.strftime('%d-%m-%Y')
    ord_out["Plant Ship date"] = pd.to_datetime(ord_out["Plant Ship date"], origin='1899-12-30', unit='D').dt.strftime('%d-%m-%Y')
    ord_out['Plant Ship Month'] = pd.to_datetime(ord_out['Plant Ship date'], format='%d-%m-%Y').dt.strftime('%Y%m')

    # Generate ID for each item (PID) and reset ID for the next item
    ord_out['Concate'] = ord_out['Whse'] + ord_out['PID'] 
    ord_out['ID'] = ord_out.groupby('Concate').cumcount() + 1
    ord_out['ord flag'] = 'ord yes'
    ord_out.to_csv(r'E:\\_Projects\\regional_tls\\output\\' + ord_file + '.csv', index=False)

    

    
