import pandas as pd
from io import StringIO

#-----PARAMETER-------------------#

doc_file = 'Global DOC Atleos 20251103'
start_date = 202511

# -----------------------------------

path = 'E:\\_Projects\\regional_tls\\input\\'


ord = pd.read_excel(path + doc_file + '.xlsb', sheet_name='Orders', engine='pyxlsb')
#print(ord.astype(str))

cmpln = pd.read_csv(path + 'completions_wsp.csv')

cmpln['ssd_mth'] = cmpln['ssd_mth'].astype(int)
#cmpln = cmpln[cmpln['ssd_mth'] >= start_date]
cmpln = cmpln[cmpln['ssd_mth'] == start_date]

cmpln = cmpln.sort_values(by=['item', 'source', 'ssd'])
ord = ord.sort_values(by=['PID', 'Whse', 'Ship Date'])


# Function to split the quantities by 1 on each line and generate IDs for each PID
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

# Split the quantities by 1 on each line and generate IDs for each PID
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
cmpln_out['cmpln flag'] = 'yes'

# Generate ID for each item (PID) and reset ID for the next item
cmpln_out['Concate'] = cmpln_out['source']+cmpln_out['item']
cmpln_out['ID'] = cmpln_out.groupby('Concate').cumcount() + 1

# Export the result to a CSV file
#print(ord_out = split_quantities_and_generate_id(ord))
ord_out.to_csv(r'E:\\_Projects\\regional_tls\\output\\' + doc_file + '.csv', index=False)
cmpln_out.to_csv(r'E:\_Projects\regional_tls\output\cmpln_alloc.csv', index=False)








