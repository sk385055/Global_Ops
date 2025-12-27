import os
import pandas as pd
import numpy as np
import glob
from datetime import datetime
from datetime import timedelta
from pyxlsb import open_workbook as open_xlsb

def main():
    User_Input = int(input('1) ia_consolidate | 2) order_metric | 3) int_order_consolidate | 4) int_order_metric | 5) stuart_order_tracking | 6) EXIT : '))
    
    if User_Input == 1:
        ia_consolidate()
    elif User_Input == 2:
        order_metric() 
    elif User_Input == 3:
         ia_consolidate_int_orders()
    elif User_Input == 4:
         int_order_metric()
    elif User_Input == 5:
         stuart_order_metric()
    else:
        print("Thank you...")
        return
    
def ia_consolidate():
    
    folder_path = r'E:\_Projects\_outputs\orders\snd'
    """"
    columns_to_read = ['Demand','Region','Org Code','Area',	'Org Name',	'DEMAND VAL','Line Number', 'Customer_End Customer',
                    'CDP',	'CIS',	'Booked Dt','Request Dt', 'PD_Order','Ship Date','SAD','Inv Trigger','Product Range',
                    'Class','PID',	'PID Desc', 'PID Type',	'Open Qty',	
                    'Res Qty',	'OH Qty',	'Supply Qty',	'ERP Line Status',	'Whse',	'IR No.',
                    'IO No.', 'PO No.', 'IO BOOKED DATE',	'IO SSD',	'IO SAD',	'Validation Flag',	
                    'Supply Type','Supply Detail',	'PD_PO','PRD',	'Order Type','Master Customer Number',
                    'Plant Ship date',	'Products',	'Offer PF',	'Plant','Plant SSD',
                    'PRM',	'PRQ',	'Key Account', 'Customer']
    """""
    
    columns_to_read = ['Demand','Region','Org Code','Area',	'Org Name',	'DEMAND VAL','Line Number', 'Customer_End Customer',
                    'CDP',	'CIS',	'Booked Dt','Request Dt', 'PD_Order','Ship Date','SAD','Inv Trigger','Product Range',
                    'Class','PID',	'PID Desc', 'PID Type',	'Open Qty',	
                    'Whse',	'IR No.',
                    'IO No.', 'PO No.', 'IO BOOKED DATE',	'IO SSD',	'IO SAD',	'Validation Flag',	
                    'Supply Type','Supply Detail',	'PD_PO','PRD',	'Order Type','Master Customer Number',
                    'Plant Ship date',	'Products',	'Offer PF',	'Plant','Plant SSD',
                    'Key Account', 'Customer']
    
    #workbench_data_eumocp_20250???_v2
    #workbench_data_eumocp_wPO_20250???_v2 'Vasanth
    files = glob.glob(folder_path+'\workbench_data_eumocp_20250???_v2.csv')
    #files = glob.glob(folder_path+'\workbench_data_eumocp_20250???_doc.csv')

    files.sort()

    consolidated_data = pd.DataFrame()
    for file in files:
        print(file)
        df=pd.read_csv(file, usecols=columns_to_read, encoding='utf-8', low_memory=False, keep_default_na='')
        
        df['filref'] = os.path.basename(file)
        #df['file'] = df['filref'].str[22:30] 'doc
        #df['file'] = df['filref'].str[26:34] 'WPO
        df['file'] = df['filref'].str[22:30]
        df['Report month'] = df['filref'].str[22:28]
        df['Report date'] = pd.to_datetime(df['file'], format='%Y%m%d')
        consolidated_data = pd.concat([consolidated_data,df],axis=0)

    OCP_IA = consolidated_data   
        
    values = ['Demand','Region','SO Ship Org','Org Code','Area','Org Name','DEMAND VAL','Line Number','Customer_End Customer','CDP','CIS','Booked Dt','Request Dt','PD_Order','Ship Date','SAD','Inv Trigger','Product Range','Class','PID','PID Desc','PID Type','Whse','IR No.','IO No.','IO BOOKED DATE','IO SSD','IO SAD','Validation Flag','Supply Type','Supply Detail','PD_PO','Buyer Name','Supplier','MakeBuy','Customer PO','Ship Set','PRD','Order Type','Master Customer Number','Plant Ship date','NEED BY DATE','SALES ORG','SALES ORG_DESC']

    OCP_IA = OCP_IA.drop_duplicates(subset=columns_to_read,keep='first')

    OCP_IA = OCP_IA[OCP_IA['PID Type'] == 'M']
    #OCP_IA = OCP_IA[OCP_IA['Demand'] == 'Order']

    prod_rng = ['Cash Dispense ATM', 'Multi Function ATM','PC Core Upgrade']
    OCP_IA = OCP_IA[OCP_IA['Product Range'].isin(prod_rng)]

    OCP_IA.to_csv(r'E:\_Projects\EUMOCP_Consolidate\input\ia_bundle_Orders.csv', index = False)
    
    print("\n")
    print("----------------------------------------")
    print("ia consolidation completed..")
    print("----------------------------------------")
    print("\n")
    main()

def ia_consolidate_int_orders():
    
    folder_path = r'E:\_Projects\_outputs\orders\snd'
    """"
    columns_to_read = ['Demand','Region','Org Code','Area',	'Org Name',	'DEMAND VAL','Line Number', 'Customer_End Customer',
                    'CDP',	'CIS',	'Booked Dt','Request Dt', 'PD_Order','Ship Date','SAD','Inv Trigger','Product Range',
                    'Class','PID',	'PID Desc', 'PID Type',	'Open Qty',	
                    'Res Qty',	'OH Qty',	'Supply Qty',	'ERP Line Status',	'Whse',	'IR No.',
                    'IO No.', 'PO No.', 'IO BOOKED DATE',	'IO SSD',	'IO SAD',	'Validation Flag',	
                    'Supply Type','Supply Detail',	'PD_PO','PRD',	'Order Type','Master Customer Number',
                    'Plant Ship date',	'Products',	'Offer PF',	'Plant','Plant SSD',
                    'PRM',	'PRQ',	'Key Account', 'Customer']
    """""
    
    columns_to_read = ['Demand','Region','Org Code','Area',	'Org Name',	'DEMAND VAL','Line Number', 'Customer_End Customer',
                    'CDP',	'CIS',	'Booked Dt','Request Dt', 'PD_Order','Ship Date','SAD','Inv Trigger','Product Range',
                    'Class','PID',	'PID Desc', 'PID Type',	'Open Qty',	
                    'Whse',	'IR No.',
                    'IO No.', 'PO No.', 'IO BOOKED DATE',	'IO SSD',	'IO SAD',	'Validation Flag',	
                    'Supply Type','Supply Detail',	'PD_PO','PRD',	'Order Type','Master Customer Number',
                    'Plant Ship date',	'Products',	'Offer PF',	'Plant','Plant SSD',
                    'Key Account', 'Customer']
    
    #workbench_data_eumocp_20250???_v2
    #workbench_data_eumocp_wPO_20250???_v2 'Vasanth
    files = glob.glob(folder_path+'\workbench_data_eumocp_20250???_v2.csv')
    #files = glob.glob(folder_path+'\workbench_data_eumocp_20250???_doc.csv')

    files.sort()

    consolidated_data = pd.DataFrame()
    for file in files:
        print(file)
        df=pd.read_csv(file, usecols=columns_to_read, encoding='utf-8', low_memory=False, keep_default_na='')
        
        df['filref'] = os.path.basename(file)
        #df['file'] = df['filref'].str[22:30] 'doc
        #df['file'] = df['filref'].str[26:34] 'WPO
        df['file'] = df['filref'].str[22:30]
        df['Report month'] = df['filref'].str[22:28]
        df['Report date'] = pd.to_datetime(df['file'], format='%Y%m%d')
        consolidated_data = pd.concat([consolidated_data,df],axis=0)

    OCP_IA = consolidated_data   
        
    values = ['Demand','Region','SO Ship Org','Org Code','Area','Org Name','DEMAND VAL','Line Number','Customer_End Customer','CDP','CIS','Booked Dt','Request Dt','PD_Order','Ship Date','SAD','Inv Trigger','Product Range','Class','PID','PID Desc','PID Type','Whse','IR No.','IO No.','IO BOOKED DATE','IO SSD','IO SAD','Validation Flag','Supply Type','Supply Detail','PD_PO','Buyer Name','Supplier','MakeBuy','Customer PO','Ship Set','PRD','Order Type','Master Customer Number','Plant Ship date','NEED BY DATE','SALES ORG','SALES ORG_DESC']

    OCP_IA = OCP_IA.drop_duplicates(subset=columns_to_read,keep='first')

    OCP_IA = OCP_IA[OCP_IA['Demand'] == 'Ord Int']
    pid_type = ['M','K']
    OCP_IA = OCP_IA[OCP_IA['PID Type'].isin(pid_type)]
   
    prod_rng = ['Int Ord CHS', 'Int Ord CHE','Int Ord GSL','Int Ord US']
    OCP_IA = OCP_IA[OCP_IA['Order Type'].isin(prod_rng)]

    OCP_IA.to_csv(r'E:\_Projects\EUMOCP_Consolidate\input\ia_bundle_int_orders.csv', index = False)
    
    print("\n")
    print("----------------------------------------")
    print("ia consolidation completed..")
    print("----------------------------------------")
    print("\n")
    main()




def order_metric():
    
    OCP_IA = pd.read_csv(r'E:\_Projects\EUMOCP_Consolidate\input\ia_bundle_Orders.csv', encoding='utf-8')
    
    sourcing = pd.read_excel(r'E:\_Projects\EUMOCP_Consolidate\input\Sourcing Tracker.xlsx', engine='openpyxl')
    
    OCP_IA['Metric Flag'] = np.where((OCP_IA['Demand'] == 'Order'), 'Order Scheduling' , '')
    
    OCP_IA['DEMAND VAL'] = OCP_IA['DEMAND VAL'].astype(str).str.strip()
    OCP_IA['Line Number'] = OCP_IA['Line Number'].astype(str).str.strip()
    OCP_IA['IR No.'] = OCP_IA['IR No.'].astype(str).str.strip()
    OCP_IA['IO No.'] =  OCP_IA['IO No.'].astype(str).str.strip()
    OCP_IA['PID'] = OCP_IA['PID'].astype(str).str.strip()
    
    #OCP_IA['UID'] = OCP_IA['DEMAND VAL'] + '|' + OCP_IA['Line Number'] + '|' + OCP_IA['IR No.'] + '|' + OCP_IA['IO No.'] + '|' + OCP_IA['PID']
    
    OCP_IA['UID'] = OCP_IA['DEMAND VAL'] + OCP_IA['PID']
    
    OCP_IA['Report date'] = pd.to_datetime(OCP_IA['Report date'], format='%d-%m-%Y')
    OCP_IA['Ship Date'] = pd.to_datetime(OCP_IA['Ship Date'], format='%d-%m-%Y')
    OCP_IA['Booked Dt'] = pd.to_datetime(OCP_IA['Booked Dt'], format='%d-%m-%Y')
    OCP_IA['SAD'] = pd.to_datetime(OCP_IA['SAD'], format='%d-%m-%Y')
    OCP_IA['PD_Order'] = pd.to_datetime(OCP_IA['PD_Order'], format='%d-%m-%Y')
    OCP_IA['IO BOOKED DATE'] = pd.to_datetime(OCP_IA['IO BOOKED DATE'], format='%d-%m-%Y')
    
    OCP_IA['weeknumber'] = OCP_IA['Report date'].dt.strftime('%U').astype(int)
     
    
    #-------------------------------------------------------------------------------------
    #--------------------------------- PO Count -----------------------------------------
    """
    order_count = (
        OCP_IA.groupby(["Report date", "UID"])
        .size()
        .reset_index(name="count")
    )
    
    order_count['Order Count'] = order_count.groupby('UID')['Report date'].transform('size')
     
    order_count = order_count[['UID','Order Count']]
    order_count.drop_duplicates(subset=['UID','Order Count'], keep='first',inplace=True)
    OCP_IA = pd.merge(OCP_IA, order_count, on='UID', how='left')
    
    #OCP_IA['total_order_count']=OCP_IA['UID'].nunique()
    
    #print(OCP_IA['total_order_count'])
    """
    #-------------------------------------------------------------------------------------
    #--------------------------------- Scheduling Date -----------------------------------
    
    #Scheduling date (EUMOCP IA Report - 1)
    min_report_date = OCP_IA.groupby('UID')['Report date'].min().reset_index()
    min_report_date.rename(columns={'Report date':'Scheduling date'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, min_report_date, on='UID', how='left')
    OCP_IA['First Report date'] = pd.to_datetime(OCP_IA['Scheduling date'], format='%d-%m-%Y')  - timedelta(days=0)
    OCP_IA['Scheduling date'] = pd.to_datetime(OCP_IA['Scheduling date'], format='%d-%m-%Y')  - timedelta(days=3)
    
    
    #-------------------------------------------------------------------------------------
    #--------------------------------- PO Status -----------------------------------------
    
    max_date = OCP_IA['Report date'].max()
    min_date = OCP_IA['Report date'].min()
    print(f"Latest report in dataset: {max_date}")
    
    #Maximun report date Of Order
    max_report_date = OCP_IA.groupby('UID')['Report date'].max().reset_index()
    max_report_date.rename(columns={'Report date':'last_report_date'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, max_report_date, on='UID', how='left')
    OCP_IA['last_report_date'] = pd.to_datetime(OCP_IA['last_report_date'], format='%d-%m-%Y')
    
    # Create 'order_status' column
    OCP_IA['order_status'] = OCP_IA['last_report_date'].apply(lambda x: 'Closed' if x < max_date else 'Open')
    
    #-------------------------------------------------------------------------------------------
    #--------------------------------- Promise Date calculation --------------------------------
    
    # Get First Promise Date
    OCP_IA['Flag'] = OCP_IA['Booked Dt'].apply(lambda x: 'Exclude' if x < min_date else 'Include')
    
    OCP_PD = OCP_IA.copy()
    
    OCP_PD = OCP_IA[OCP_IA['PD_Order'].notna()]   #OCP_PD.dropna(subset=['Upd PD'])
    OCP_PD.sort_values(by=['Report date','UID','PD_Order'])
    OCP_PD.drop_duplicates(subset=['UID'], keep='first',inplace=True)
    OCP_PD = OCP_PD[['UID','PD_Order','Report date']]
    OCP_PD.rename(columns={'PD_Order':'temp_FPD', 'Report date':'temp_FPD updated on'}, inplace=True)
    
    
    OCP_IA = pd.merge(OCP_IA, OCP_PD, on='UID', how='left')
  
    OCP_IA['FPD updated on'] = np.where((OCP_IA['order_status'] == 'Closed') & (OCP_IA['temp_FPD'].isna()), OCP_IA['First Report date'] , OCP_IA['temp_FPD updated on'])
    
    
    # Scheduling Date Vs PD date
    OCP_IA['Sch. dt vs PD Ageing'] =(pd.to_datetime(OCP_IA['FPD updated on']) - pd.to_datetime(OCP_IA['Scheduling date'])).dt.days

    con1 = OCP_IA['Sch. dt vs PD Ageing'] < 0
    con2 = OCP_IA['Sch. dt vs PD Ageing'] <= 5
    
    
    OCP_IA['Sch. dt vs PD Bucket'] = np.where(con1,'Error', \
                                 np.where(con2,'01-05days','05+ days'))
    
    OCP_IA['Sch. dt vs PD Bucket'] = np.where((OCP_IA['Sch. dt vs PD Ageing'].isna()), 'Yet to be Schedule', OCP_IA['Sch. dt vs PD Bucket'])
    
    
    #Scheduling Date Vs Booked Date
    OCP_IA['Sch. dt vs Booked dt Ageing'] =(pd.to_datetime(OCP_IA['Scheduling date']) - pd.to_datetime(OCP_IA['Booked Dt'])).dt.days
   
    
    con2 = OCP_IA['Sch. dt vs Booked dt Ageing'] <= 5
    
    OCP_IA['Sch. dt vs Booked dt Bucket'] = np.where(con2,'00-05days','05+ days')
    
    OCP_IA['Sch. dt vs Booked dt Bucket'] = np.where((OCP_IA['Sch. dt vs Booked dt Ageing'].isna()), 'Yet to be Schedule', OCP_IA['Sch. dt vs Booked dt Bucket'])
   
    #--------------------------------------------------------------------------------------------
    #--------------------------------- Sourcing -------------------------------------------------
   
    sourcing.rename(columns={'REQUESTED DATE and TIME':'REQUESTED DATE', 'SOURCING COMPLETION DATE and Time':'SOURCING COMPLETION DATE'}, inplace=True)
    sourcing['concate'] = sourcing['PARTNUMBER'] + sourcing['ORG NAME']
    sourcing = sourcing[['concate','REQUESTED DATE','SOURCING COMPLETION DATE','Buyer Confirm Date','Source Day Calc','Buyer Day Calc']]
    OCP_IA['concate'] = OCP_IA['PID'] + OCP_IA['Whse']
    OCP_IA = pd.merge(OCP_IA, sourcing, on='concate', how='left')
   
   
    #--------------------------------------------------------------------------------------------
    #--------------------------------- Missing Promise Date -------------------------------------

    OCP_IA['MPD Flag'] = np.where((OCP_IA['Demand'] == 'Order') & (OCP_IA['FPD updated on'].isna()), 'Yes - MPD', 'No')
    
    OCP_IA['MPD Ageing'] = (pd.to_datetime(OCP_IA['Report date']) - pd.to_datetime(OCP_IA['Scheduling date'])).dt.days
    OCP_IA['MPD Ageing'] = np.where((OCP_IA['MPD Flag'] == 'Yes - MPD'), OCP_IA['MPD Ageing'],0)
    
    # Create conditions and choices for ageing buckets
    con1 = OCP_IA['MPD Ageing'] <= 0 #Less 10
    con2 = OCP_IA['MPD Ageing'] <= 5
    
    OCP_IA['MPD Bucket'] = np.where(con1,'No MPD', \
                                 np.where(con2,'01-05 days','05+ days'))
    
    
    #--------------------------------------------------------------------------------------------
    #--------------------------------- Promise Date Vs SAD calculation --------------------------
    
    #Updated Promise date
    OCP_IA['Upd PD'] = np.where((OCP_IA['order_status'] == 'Closed') & (OCP_IA['PD_Order'].isna()), OCP_IA['last_report_date'], OCP_IA['PD_Order'])
    
    #Upd Promise date Vs SAD%
    OCP_IA['PD vs SAD Flag'] = np.where((OCP_IA['Upd PD'] < OCP_IA['SAD']), 'Bad - PD < SAD', 'Good - PD > SAD')    
    #--------------------------------------------------------------------------------------------
    #--------------------------------- Past Due -------------------------------------------------
    
    OCP_IA['Past Due SSD Flag'] = np.where((OCP_IA['Ship Date'] < OCP_IA['Report date']), 'Yes - Past Due SSD', 'No')
    
    OCP_IA['Past Due PD Flag'] = np.where((OCP_IA['PD_Order'] < OCP_IA['Report date']), 'Yes - Past Due PD', 'No')
    
    
    
    
    # --------------------------------IO Metric ---------------------------
     
   #Scheduling Date Vs IO Booked Date
    
    OCP_IA['IO No.'] = pd.to_numeric(OCP_IA['IO No.'], errors='coerce')
    OCP_IA['IO Booked Dt upd'] = np.where((OCP_IA['IO BOOKED DATE'].isna()) & (OCP_IA['IO No.'].notna()), OCP_IA['Booked Dt'], OCP_IA['IO BOOKED DATE'])
    
     # Get First Promise Date
    OCP_IA['IO Flag'] = OCP_IA['IO Booked Dt upd'].apply(lambda x: 'Exclude' if x < min_date else 'Include')
    
    OCP_IO = OCP_IA.copy()
    
    OCP_IO = OCP_IO[OCP_IO['IO No.'].notna()]   #OCP_PD.dropna(subset=['Upd PD'])
    OCP_IO.sort_values(by=['Report date','IO No.'])
    OCP_IO.drop_duplicates(subset=['IO No.'], keep='first',inplace=True)
    OCP_IO.to_csv(r'E:\_Projects\EUMOCP_Consolidate\output\io_order_tracking.csv', index = False)
    OCP_IO = OCP_IO[['IO No.','Report date']]
    OCP_IO.rename(columns={'Report date':'IO First upd'}, inplace=True)
    
    OCP_IA = pd.merge(OCP_IA, OCP_IO, on='IO No.', how='left')
    
    
    #condition = ((~OCP_IA['Order Type'].isin(['Int Ord CHS', 'Int Ord CHE', 'Int Ord US', 'Int Ord GSL'])))
    #OCP_IA.loc[condition, 'IO First upd'] = None  # Set to None/NaN
    

    OCP_IA['IO Report Date'] = np.where((OCP_IA['Order Type'].isin(['Int Ord CHS','Int Ord CHE','Int Ord US','Int Ord GSL'])), OCP_IA['IO First upd'], OCP_IA['First Report date'])

    OCP_IA['IO Ageing'] = (pd.to_datetime(OCP_IA['IO Booked Dt upd']) - pd.to_datetime(OCP_IA['IO Report Date'])).dt.days
    
    # Create conditions and choices for ageing buckets
    con1 = OCP_IA['IO Ageing'] < 0 #Less 10
    con2 = OCP_IA['IO Ageing'] <= 5
    
    OCP_IA['IO Buket'] = np.where(con1,'Error', \
                                 np.where(con2,'00-05 days','05+ days'))
    
    OCP_IA['IO Buket'] = np.where((OCP_IA['IO Ageing'].isna()), 'Yet to be Schedule', OCP_IA['IO Buket'])
   

    OCP_IA.to_csv(r'E:\_Projects\EUMOCP_Consolidate\output\ia_order_tracking.csv', index = False)
    
    print("\n")
    print("----------------------------------------")
    print("Order Metric created..")
    print("----------------------------------------")
    print("\n")
    main()



def int_order_metric():
    
    OCP_IA = pd.read_csv(r'E:\_Projects\EUMOCP_Consolidate\input\ia_bundle_int_orders.csv', encoding='utf-8')
    
    OCP_IA['DEMAND VAL'] = OCP_IA['DEMAND VAL'].astype(str).str.strip()
    OCP_IA['Line Number'] = OCP_IA['Line Number'].astype(str).str.strip()
    OCP_IA['IR No.'] = OCP_IA['IR No.'].astype(str).str.strip()
    OCP_IA['IO No.'] =  OCP_IA['IO No.'].astype(str).str.strip()
    OCP_IA['PID'] = OCP_IA['PID'].astype(str).str.strip()
    
    #OCP_IA['UID'] = OCP_IA['DEMAND VAL'] + '|' + OCP_IA['Line Number'] + '|' + OCP_IA['IR No.'] + '|' + OCP_IA['IO No.'] + '|' + OCP_IA['PID']
    
    OCP_IA['UID'] = OCP_IA['DEMAND VAL'] + OCP_IA['PID']
    
    OCP_IA['Report date'] = pd.to_datetime(OCP_IA['Report date'], format='%d-%m-%Y')
    OCP_IA['Ship Date'] = pd.to_datetime(OCP_IA['Ship Date'], format='%d-%m-%Y')
    OCP_IA['Booked Dt'] = pd.to_datetime(OCP_IA['Booked Dt'], format='%d-%m-%Y')
    OCP_IA['SAD'] = pd.to_datetime(OCP_IA['SAD'], format='%d-%m-%Y')
    OCP_IA['PD_Order'] = pd.to_datetime(OCP_IA['PD_Order'], format='%d-%m-%Y')
    OCP_IA['IO BOOKED DATE'] = pd.to_datetime(OCP_IA['IO BOOKED DATE'], format='%d-%m-%Y')
    
    OCP_IA['weeknumber'] = OCP_IA['Report date'].dt.strftime('%U').astype(int)
     
    #--------------------------------------------------------------------------------------------
    #--------------------------------- Past Due -------------------------------------------------
    
    OCP_IA['Past Due SSD Flag'] = np.where((OCP_IA['Ship Date'] < OCP_IA['Report date']), 'Yes - Past Due SSD', 'No')
    
    OCP_IA['Past Due PD Flag'] = np.where((OCP_IA['PD_Order'] < OCP_IA['Report date']), 'Yes - Past Due PD', 'No')
  
    OCP_IA.to_csv(r'E:\_Projects\EUMOCP_Consolidate\output\ia_int_order_tracking.csv', index = False)
    
    print("\n")
    print("----------------------------------------")
    print("Order Metric created..")
    print("----------------------------------------")
    print("\n")
    main()





def stuart_order_metric():
    
    OCP_IA = pd.read_csv(r'E:\_Projects\EUMOCP_Consolidate\input\ia_bundle.csv', encoding='utf-8')
    
    sourcing = pd.read_excel(r'E:\_Projects\EUMOCP_Consolidate\input\Sourcing actions tracker - Atleos.xlsx', engine='openpyxl', sheet_name='Data')
    
    OCP_IA = OCP_IA[OCP_IA['Demand'] == 'Order']

    
    OCP_IA['DEMAND VAL'] = OCP_IA['DEMAND VAL'].astype(str).str.strip()
    OCP_IA['Line Number'] = OCP_IA['Line Number'].astype(str).str.strip()
    OCP_IA['IR No.'] = OCP_IA['IR No.'].astype(str).str.strip()
    OCP_IA['IO No.'] =  OCP_IA['IO No.'].astype(str).str.strip()
    OCP_IA['PID'] = OCP_IA['PID'].astype(str).str.strip()
    
    #OCP_IA['UID'] = OCP_IA['DEMAND VAL'] + '|' + OCP_IA['Line Number'] + '|' + OCP_IA['IR No.'] + '|' + OCP_IA['IO No.'] + '|' + OCP_IA['PID']
    
    OCP_IA['UID'] = OCP_IA['DEMAND VAL'] + OCP_IA['PID']
    
    OCP_IA['Report date'] = pd.to_datetime(OCP_IA['Report date'], format='%d-%m-%Y')
    OCP_IA['Ship Date'] = pd.to_datetime(OCP_IA['Ship Date'], format='%d-%m-%Y')
    OCP_IA['Booked Dt'] = pd.to_datetime(OCP_IA['Booked Dt'], format='%d-%m-%Y')
    OCP_IA['SAD'] = pd.to_datetime(OCP_IA['SAD'], format='%d-%m-%Y')
    OCP_IA['PD_Order'] = pd.to_datetime(OCP_IA['PD_Order'], format='%d-%m-%Y')
    OCP_IA['IO BOOKED DATE'] = pd.to_datetime(OCP_IA['IO BOOKED DATE'], format='%d-%m-%Y')
    
    OCP_IA['weeknumber'] = OCP_IA['Report date'].dt.strftime('%U').astype(int)
     
    
    #-------------------------------------------------------------------------------------
    #--------------------------------- PO Count -----------------------------------------
    """
    order_count = (
        OCP_IA.groupby(["Report date", "UID"])
        .size()
        .reset_index(name="count")
    )
    
    order_count['Order Count'] = order_count.groupby('UID')['Report date'].transform('size')
     
    order_count = order_count[['UID','Order Count']]
    order_count.drop_duplicates(subset=['UID','Order Count'], keep='first',inplace=True)
    OCP_IA = pd.merge(OCP_IA, order_count, on='UID', how='left')
    
    #OCP_IA['total_order_count']=OCP_IA['UID'].nunique()
    
    #print(OCP_IA['total_order_count'])
    """
    #-------------------------------------------------------------------------------------
    #--------------------------------- Scheduling Date -----------------------------------
    
    #Scheduling date
    min_report_date = OCP_IA.groupby('UID')['Report date'].min().reset_index()
    min_report_date.rename(columns={'Report date':'Scheduling date'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, min_report_date, on='UID', how='left')
    OCP_IA['Scheduling date'] = pd.to_datetime(OCP_IA['Scheduling date'], format='%d-%m-%Y')  - timedelta(days=1)
    #OCP_IA['Scheduling date'] = pd.to_datetime(OCP_IA['Scheduling date'])  - timedelta(days=1)
    
    #-------------------------------------------------------------------------------------
    #--------------------------------- PO Status -----------------------------------------
    
    max_date = OCP_IA['Report date'].max()
    min_date = OCP_IA['Report date'].min()
    print(f"Latest report in dataset: {max_date}")
    
    #Maximun report date Of Order
    max_report_date = OCP_IA.groupby('UID')['Report date'].max().reset_index()
    max_report_date.rename(columns={'Report date':'last_report_date'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, max_report_date, on='UID', how='left')
    OCP_IA['last_report_date'] = pd.to_datetime(OCP_IA['last_report_date'], format='%d-%m-%Y')
    
    # Create 'order_status' column
    OCP_IA['order_status'] = OCP_IA['last_report_date'].apply(lambda x: 'Closed' if x < max_date else 'Open')
    
    #-------------------------------------------------------------------------------------------
    #--------------------------------- Promise Date calculation --------------------------------
    
    # Get First Promise Date
    OCP_IA['Flag'] = OCP_IA['Booked Dt'].apply(lambda x: 'Exclude' if x < min_date else 'Include')
    
    OCP_PD = OCP_IA.copy()
    
    OCP_PD = OCP_IA[OCP_IA['PD_Order'].notna()]   #OCP_PD.dropna(subset=['Upd PD'])
    OCP_PD.sort_values(by=['Report date','UID','PD_Order'])
    OCP_PD.drop_duplicates(subset=['UID'], keep='first',inplace=True)
    OCP_PD = OCP_PD[['UID','PD_Order','Report date']]
    OCP_PD.rename(columns={'PD_Order':'temp_FPD', 'Report date':'temp_FPD updated on'}, inplace=True)
    
    
    OCP_IA = pd.merge(OCP_IA, OCP_PD, on='UID', how='left')
    #OCP_PD.to_csv(r'E:\_Projects\EUMOCP_Consolidate\output\ia_bundle_FPD.csv', index = False)

    OCP_IA['FPD'] = np.where((OCP_IA['order_status'] == 'Closed') & (OCP_IA['temp_FPD'].isna()), OCP_IA['last_report_date'], OCP_IA['temp_FPD'])
    
    OCP_IA['FPD updated on'] = np.where((OCP_IA['order_status'] == 'Closed') & (OCP_IA['temp_FPD'].isna()), OCP_IA['Scheduling date'] + timedelta(days=1) , OCP_IA['temp_FPD updated on'])
    
    
    # Scheduling Date Vs PD date
    OCP_IA['Sch. dt vs PD Ageing'] =(pd.to_datetime(OCP_IA['FPD updated on']) - pd.to_datetime(OCP_IA['Scheduling date'])).dt.days

    con1 = OCP_IA['Sch. dt vs PD Ageing'] < 0
    con2 = OCP_IA['Sch. dt vs PD Ageing'] <= 5
    
    
    OCP_IA['Sch. dt vs PD Bucket'] = np.where(con1,'Error', \
                                 np.where(con2,'01-05days','05+ days'))
    
    OCP_IA['Sch. dt vs PD Bucket'] = np.where((OCP_IA['Sch. dt vs PD Ageing'].isna()), 'Yet to be Schedule', OCP_IA['Sch. dt vs PD Bucket'])
    
    
    #Scheduling Date Vs Booked Date
    OCP_IA['Sch. dt vs Booked dt Ageing'] =(pd.to_datetime(OCP_IA['Scheduling date']) - pd.to_datetime(OCP_IA['Booked Dt'])).dt.days
   
    con1 = OCP_IA['Sch. dt vs Booked dt Ageing'] < 0
    con2 = OCP_IA['Sch. dt vs Booked dt Ageing'] <= 5
    
    
    OCP_IA['Sch. dt vs Booked dt Bucket'] = np.where(con1,'Error', \
                                 np.where(con2,'01-05days','05+ days'))
    
    OCP_IA['Sch. dt vs Booked dt Bucket'] = np.where((OCP_IA['Sch. dt vs Booked dt Ageing'].isna()), 'Yet to be Schedule', OCP_IA['Sch. dt vs Booked dt Bucket'])
   
   
   #Scheduling Date Vs IO Booked Date
  
    OCP_IA['IO BOOKED DATE upd'] = np.where((OCP_IA['Region'] == 'NAMER') & (OCP_IA['IR No.'].notna()) & (OCP_IA['IO BOOKED DATE'].isna()), OCP_IA['Booked Dt'], OCP_IA['IO BOOKED DATE'])
    
    #OCP_IA['IO BOOKED DATE upd'] = np.where((OCP_IA['IR No.'].notna()) & (OCP_IA['IO BOOKED DATE'].isna()), OCP_IA['Booked Dt'], OCP_IA['IO BOOKED DATE'])
    
    OCP_IA['Flag IO'] = OCP_IA['IO BOOKED DATE upd'].apply(lambda x: 'Exclude' if x < min_date else 'Include')
    
    OCP_IA['Sch. dt vs IO Booked dt Ageing'] =(pd.to_datetime(OCP_IA['IO BOOKED DATE upd']) - pd.to_datetime(OCP_IA['Scheduling date'])).dt.days
   
    con1 = OCP_IA['Sch. dt vs IO Booked dt Ageing'] < 0
    con2 = OCP_IA['Sch. dt vs IO Booked dt Ageing'] <= 5
    
    
    OCP_IA['Sch. dt vs IO Booked dt Bucket'] = np.where(con1,'Early', \
                                 np.where(con2,'01-05days','05+ days'))
    
    OCP_IA['Sch. dt vs IO Booked dt Bucket'] = np.where((OCP_IA['Sch. dt vs IO Booked dt Ageing'].isna()), 'Yet to be Schedule', OCP_IA['Sch. dt vs IO Booked dt Bucket'])
   
   
    #--------------------------------------------------------------------------------------------
    #--------------------------------- Sourcing -------------------------------------------------
   
    sourcing.rename(columns={'Order Number':'DEMAND VAL', 'Comp Date':'OSS Lifted Date', 'Recv date':'OSS Request Date','Comp days':'OSS Ageing'}, inplace=True)
    sourcing = sourcing[['DEMAND VAL','OSS Request Date','OSS Lifted Date']]
    sourcing.drop_duplicates(subset=['DEMAND VAL','OSS Request Date','OSS Lifted Date'], keep='first',inplace=True)
    sourcing['DEMAND VAL'] = sourcing['DEMAND VAL'].astype(str)
    OCP_IA['DEMAND VAL'] = OCP_IA['DEMAND VAL'].astype(str)

    OCP_IA = pd.merge(OCP_IA, sourcing, on='DEMAND VAL', how='left')
   
   
    #--------------------------------------------------------------------------------------------
    #--------------------------------- Missing Promise Date -------------------------------------

    OCP_IA['MPD Flag'] = np.where((OCP_IA['Demand'] == 'Order') & (OCP_IA['FPD'].isna()), 'Yes - MPD', 'No')
    
    OCP_IA['MPD Ageing'] = (pd.to_datetime(OCP_IA['Report date']) - pd.to_datetime(OCP_IA['Scheduling date'])).dt.days
    OCP_IA['MPD Ageing'] = np.where((OCP_IA['MPD Flag'] == 'Yes - MPD'), OCP_IA['MPD Ageing'],0)
    
    # Create conditions and choices for ageing buckets
    con1 = OCP_IA['MPD Ageing'] <= 0 #Less 10
    con2 = OCP_IA['MPD Ageing'] <= 5
    
    OCP_IA['MPD Bucket'] = np.where(con1,'No MPD', \
                                 np.where(con2,'01-05 days','05+ days'))
    
    
    #--------------------------------------------------------------------------------------------
    #--------------------------------- Promise Date Vs SAD calculation --------------------------
    
    #Updated Promise date
    OCP_IA['Upd PD'] = np.where((OCP_IA['order_status'] == 'Closed') & (OCP_IA['PD_Order'].isna()), OCP_IA['last_report_date'], OCP_IA['PD_Order'])
    
    #Upd Promise date Vs SAD%
    OCP_IA['PD vs SAD Flag'] = np.where((OCP_IA['Upd PD'] < OCP_IA['SAD']), 'Bad - PD < SAD', 'Good - PD > SAD')

    OCP_IA['PD vs SAD Ageing'] = (pd.to_datetime(OCP_IA['SAD']) - pd.to_datetime(OCP_IA['Upd PD'])).dt.days
    OCP_IA.loc[OCP_IA['PD vs SAD Flag'] == 'Good - PD > SAD', 'PD vs SAD Ageing'] = 0 

    con1 = OCP_IA['PD vs SAD Ageing'] <= 0 #Less 10
    con2 = OCP_IA['PD vs SAD Ageing'] <= 10
    con3 = OCP_IA['PD vs SAD Ageing'] <= 20
    
    OCP_IA['PD vs SAD_Bucket'] = np.where(con1,'PD > SAD - Good', \
                                 np.where(con2,'01-10 days', \
                                 np.where(con3,'10-20 days','20+ days')))
    
    
    
    
    #--------------------------------------------------------------------------------------------
    #--------------------------------- Past Due -------------------------------------------------
    
    OCP_IA['Past Due SSD Flag'] = np.where((OCP_IA['Ship Date'] < OCP_IA['Report date']), 'Yes - Past Due SSD', 'No')
    
    OCP_IA['Past Due PD Flag'] = np.where((OCP_IA['PD_Order'] < OCP_IA['Report date']), 'Yes - Past Due PD', 'No')
  

    
    OCP_IA.to_csv(r'E:\_Projects\EUMOCP_Consolidate\output\ia_order_tracking.csv', index = False)
    
    print("\n")
    print("----------------------------------------")
    print("Order Metric created..")
    print("----------------------------------------")
    print("\n")
    main()


if __name__ == "__main__":
    main()