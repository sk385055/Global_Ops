import pandas as pd
from io import StringIO

# Read the first Excel file
CHE = pd.read_excel('E:\\_Projects\\Discontinuation SDS\\rcv\\Raw data _CHE-CHS SDS.xlsx', sheet_name='Sheet1')
ENN = pd.read_excel('E:\\_Projects\\Discontinuation SDS\\rcv\\EN01 Weekly Atleos forecast.xlsx', sheet_name='Sheet1')
ENN_cost = pd.read_excel('E:\\_Projects\\Discontinuation SDS\\lookup\\enn_cost.xlsx')
#JAB = pd.read_excel('E:\\_Projects\\Discontinuation SDS\\rcv\\MRP List - DemandSupply(Excluded Planned Orders).xlsx')
#JAB_cost = pd.read_excel('E:\\_Projects\\Discontinuation SDS\\lookup\\jab_cost.xlsx')
discont_parts = pd.read_excel('E:\\_Projects\\Discontinuation SDS\\lookup\\part_list.xlsx')

# Rename the column at iloc 10 to 'CAT'
CHE.rename(columns={CHE.columns[19]: 'CAT'}, inplace=True)
CHE.rename(columns={' Past': 'Past'}, inplace=True)

#--------------------------------------ENNOCONN-------------------------------------------------
#-----------------------------------------------------------------------------------------------

# Convert date columns to numeric, replacing non-numeric values with 0
# Trim spaces from 'Inventory Item' column
ENN['Partnumber'] = ENN['Partnumber'].str.strip()
date_columns = ENN.columns[20:]
#print(date_columns)
ENN[date_columns] = ENN[date_columns].apply(pd.to_numeric, errors='coerce').fillna(0)

# Calculate the sum of all date columns and create a new 'demand' column
ENN['Final'] = ENN[date_columns].sum(axis=1)
ENN_1 = ENN

# Filter the data on CAT as FC
ENN_demand = ENN[ENN['CAT'] == 'FC']
ENN_po = ENN[ENN['CAT'] == 'PO']

# Create a new DataFrame with Partnumber and CAT columns
ENN_demand = ENN_demand[['Partnumber', 'Final']]
ENN_po = ENN_po[['Partnumber', 'Final']]

# Replace Final as Demand or PO
ENN_demand = ENN_demand.rename(columns=lambda x: x.replace('Final', 'Demand Forecast Qty'))
ENN_po = ENN_po.rename(columns=lambda x: x.replace('Final', 'On Order Qty'))

# Get Demand and PO
ENN = pd.merge(ENN, ENN_demand, on='Partnumber',how='left')
ENN = pd.merge(ENN, ENN_po, on='Partnumber',how='left')

# Create a new column with the value 'ENN'
ENN['Org Code'] = 'ENN'
ENN['Source Type'] = 'Buy'

ENN['CAT'] = ENN['CAT'].replace('FC', 'Demand Quantity')
ENN['CAT'] = ENN['CAT'].replace('PO', 'Purchase Order Quantity')
ENN['CAT'] = ENN['CAT'].replace('TQ', 'Balance With SS')

ENN.rename(columns={'TTL OH': 'OH - NCR','LT':'Item Eff. LT','MOQ':'Item MOQ',
                    'SafetyTime':'Safety Stock','Supplier':'Primary Supplier / Source Org',
                    'Buyer':'Buyer Name','Description':'Inventory Item Description',
                    'Partnumber': 'Inventory Item'}, inplace=True)

ENN_cost = ENN_cost[['ITEM', 'SUPPLIER_PRICE','SUPPLIER_CURRENCY']]

# Rename columns
ENN_cost.rename(columns={'ITEM': 'Inventory Item', 'SUPPLIER_PRICE': 'Std. Unit Cost Amount (USD)',
                         'SUPPLIER_CURRENCY':'CURRENCY'}, inplace=True)

# Get ENN_cost
ENN = pd.merge(ENN, ENN_cost, on='Inventory Item',how='left')

#ENN.to_csv('E:\\_Projects\\Discontinuation SDS\\Output\\enn_output.csv', index=False)

# Delete the columns 'Inventory Item' and 'Org Code'
ENN.drop(columns=['Customer', 'MC WhereUsed','Feat WhereUsed','Vendor Code','MSR comment',
                  'TransitTime','TTL Demand','RAW OH','FG OH','Past PO','FUT PO','Final'], inplace=True)


"""
#--------------------------------------JABIL----------------------------------------------------
#-----------------------------------------------------------------------------------------------
# Convert date columns to numeric, replacing non-numeric values with 0
JAB['Part'] = JAB['Part'].str.strip()
date_columns = JAB.columns[13:]
#print(date_columns)
JAB[date_columns] = JAB[date_columns].apply(pd.to_numeric, errors='coerce').fillna(0)

# Calculate the sum of all date columns and create a new 'demand' column
JAB['Final'] = JAB[date_columns].sum(axis=1)
JAB_1 = JAB

# Filter the data on CAT as FC
JAB_demand = JAB[JAB['CAT'] == 'Total Demand']
JAB_po = JAB[JAB['CAT'] == 'Available Supply']
JAB_OH = JAB[JAB['CAT'] == 'Balance']

# Create a new DataFrame with Partnumber and CAT columns
JAB_demand = JAB_demand[['Part', 'Final']]
JAB_po = JAB_po[['Part', 'Final']]
JAB_OH = JAB_OH[['Part', 'ON HAND']]

# Replace Final as Demand or PO
JAB_demand = JAB_demand.rename(columns=lambda x: x.replace('Final', 'Demand Forecast Qty'))
JAB_po = JAB_po.rename(columns=lambda x: x.replace('Final', 'On Order Qty'))
JAB_OH = JAB_OH.rename(columns=lambda x: x.replace('ON HAND', 'OH - NCR'))

# Get Demand and PO
JAB = pd.merge(JAB, JAB_demand, on='Part',how='left')
JAB = pd.merge(JAB, JAB_po, on='Part',how='left')
JAB = pd.merge(JAB, JAB_OH, on='Part',how='left')

# Create a new column with the value 'ENN'
JAB['Org Code'] = 'JAB'
JAB['Source Type'] = 'Buy'

JAB['CAT'] = JAB['CAT'].replace('Total Demand', 'Demand Quantity')
JAB['CAT'] = JAB['CAT'].replace('Available Supply', 'Purchase Order Quantity')
JAB['CAT'] = JAB['CAT'].replace('Balance', 'Balance With SS')

JAB.rename(columns={'Supplier':'Primary Supplier / Source Org',
                    'Buyer':'Buyer Name','Description':'Inventory Item Description',
                    'Part': 'Inventory Item'}, inplace=True)

JAB_cost = JAB_cost[['ITEM', 'SUPPLIER_PRICE','SUPPLIER_CURRENCY','LEAD_TIME']]

# Rename columns
JAB_cost.rename(columns={'ITEM': 'Inventory Item', 'SUPPLIER_PRICE': 'Std. Unit Cost Amount (USD)',
                         'SUPPLIER_CURRENCY':'CURRENCY','LEAD_TIME':'Item Eff. LT'}, inplace=True)

# Get ENN_cost
JAB = pd.merge(JAB, JAB_cost, on='Inventory Item',how='left')

#JAB.to_csv('E:\\_Projects\\Discontinuation SDS\\Output\\jab_output.csv', index=False)

# Delete the columns 'Inventory Item' and 'Org Code'
JAB.drop(columns=['CTB 12/30', 'CTB 01/20','Comments','NCR Comments','DSV STOCK',
                  'Status','ON HAND','Final'], inplace=True)

"""
# Concatenate the DataFrames
#SDS = pd.concat([CHE, ENN, JAB])
SDS = pd.concat([CHE, ENN])
SDS.to_csv('E:\\_Projects\\Discontinuation SDS\\Output\\temp.csv', index=False)

# Arrange column Currency and Future
cols = SDS.columns.tolist()
cols.insert(cols.index('Std. Unit Cost Amount (USD)') + 1, cols.pop(cols.index('CURRENCY')))
cols.insert(cols.index('Past') + 1, cols.pop(cols.index('Future')))
SDS = SDS[cols]

# Sort the Date columns
sorted_columns = list(SDS.columns[:23]) + sorted(SDS.columns[23:], key=lambda x: pd.to_datetime(x))
SDS = SDS[sorted_columns]

# Arrange Future
cols = SDS.columns.tolist()
cols.append(cols.pop(cols.index('Future')))
SDS = SDS[cols]

SDS['CAT'] = SDS['CAT'].replace('Balance With SS', 'Balance')


#--------------------------------------DISCONTINUATION----------------------------------------------
#---------------------------------------------------------------------------------------------------
discont_parts.rename(columns={'Part': 'Inventory Item'}, inplace=True)
discont_SDS = pd.merge(SDS, discont_parts, on='Inventory Item',how='left')
discont_SDS = discont_SDS[discont_SDS['Flag'] == 'Yes']

#--------------------------------------OUTPUT-------------------------------------------------------
#---------------------------------------------------------------------------------------------------
SDS.to_csv('E:\\_Projects\\Discontinuation SDS\\Output\\all_sds.csv', index=False)
discont_SDS.to_csv('E:\\_Projects\\Discontinuation SDS\\Output\\discontinuation_sds.csv', index=False)
