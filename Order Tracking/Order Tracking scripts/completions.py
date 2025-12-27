import pandas as pd
from io import StringIO

#-----PARAMETER-------------------#
start_date = 202511

doc_file1 = 'Global DOC Atleos 20251103'
doc_file2 = 'Global DOC Atleos 20251110'
doc_file3 = 'Global DOC Atleos 20251117'
doc_file4 = 'Global DOC Atleos 20251124'

file_stems = [doc_file1, doc_file2, doc_file3, doc_file4]


# ---------------------------------------------------------------
path = 'E:\\_Projects\\regional_tls\\input\\'

# --------------------------------------------------------------
cmpln = pd.read_csv(path + 'completions_wsp.csv')

cmpln['ssd_mth'] = cmpln['ssd_mth'].astype(int)
cmpln = cmpln[cmpln['ssd_mth'] == start_date]

cmpln = cmpln.sort_values(by=['item', 'source', 'ssd'])

def cmpln_alloc(cmpln):
    pid_counter = {}
    result = []
    
    for index, row in cmpln.iterrows():
        
        for _ in range(row["quantity"]):
            result.append({
                **row.to_dict(),
                "quantity": 1,
            })
    
    return pd.DataFrame(result)

# Split the quantities by 1 on each line and generate IDs for each PID
cmpln_out = cmpln_alloc(cmpln)

cmpln_out['source'] = cmpln_out['source'].replace('CHS', 'CHE')
cmpln_out['source'] = cmpln_out['source'].replace('BUD', 'ENN')
cmpln_out = cmpln_out[cmpln_out['item_type'] == 'M']
cmpln_out['cmpln flag'] = 'cmpln yes'

# Generate ID for each item (PID) and reset ID for the next item
cmpln_out['Concate'] = cmpln_out['source']+cmpln_out['item']
cmpln_out['ID'] = cmpln_out.groupby('Concate').cumcount() + 1

cmpln_out.rename(columns={'source':'Whse',
                          'item':'PID'}, inplace=True)


# --------------------------------------------------------------
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

i = 0
combined = []  # list of DataFrames
for ord_file in file_stems:
    i = i+1
    ord = pd.read_excel(path + ord_file + '.xlsb', sheet_name='Orders', engine='pyxlsb')
    ord = ord.sort_values(by=['PID', 'Whse', 'Ship Date'])
    ord_out = ord_alloc(ord)
    print(ord_file)
    
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
    

    if len(cmpln_out) > 0:
        #print(i)
        df_ord = ord_out.merge(cmpln_out[["Whse", "PID","ID","cmpln flag"]], on=['Whse','PID','ID'], how="left")
        df_ord = df_ord[df_ord['cmpln flag'] == 'cmpln yes']
        combined.append(df_ord)
        
        cmpln_out = cmpln_out.merge(ord_out[["Whse", "PID","ID","ord flag"]], on=['Whse','PID','ID'], how="left")
        cmpln_out = cmpln_out[cmpln_out ['ord flag'].isna() | (cmpln_out['ord flag'].str.strip() == '')]
        
        cmpln_out = cmpln_out.drop(columns=['ID','ord flag'])
        cmpln_out = cmpln_out.sort_values(by=['PID', 'Whse', 'ssd'])
        cmpln_out['ID'] = cmpln_out.groupby('Concate').cumcount() + 1
        
        cmpln_out.to_csv(r'E:\\_Projects\\regional_tls\\output\\' + 'cmpln_out' + '.csv', index=False)
        #df_ord.to_csv(r'E:\\_Projects\\regional_tls\\output\\' + ord_file + '.csv', index=False)
        

df_all = pd.concat(combined, ignore_index=True)
df_all.to_csv(r'E:\\_Projects\\regional_tls\\output\\'  + 'combined.csv', index=False)

        








