import os
import pandas as pd
import numpy as np
import glob
from datetime import datetime
from datetime import timedelta
from pyxlsb import open_workbook as open_xlsb

def main():
    User_Input = int(input('1) ia_consolidate | 2) order_metric | 3) int_order_metric | 4) EXIT : '))
    
    if User_Input == 1:
        ia_consolidate()
    elif User_Input == 2:
        order_metric() 
    elif User_Input == 3:
         int_order_metric()
    else:
        print("Thank you...")
        return
    
def ia_consolidate():
    
    start_month = "202501"
    end_month = "202505"
    
    folder_path = r'E:\_Projects\_outputs\orders\snd'
    """"
    columns_to_read = ['Demand','Region','SO Ship Org','Org Code','Area',	'Org Name',	'DEMAND VAL','Line Number', 'Customer_End Customer',
                    'CDP',	'CIS',	'Booked Dt','Request Dt', 'PD_Order','Ship Date','SAD','Inv Trigger','Product Range',
                    'Class','PID',	'PID Desc', 'PID Type',	'Open Qty',	
                    'Res Qty',	'OH Qty',	'Supply Qty',	'ERP Line Status',	'Whse',	'IR No.',
                    'IO No.', 'PO No.', 'IO BOOKED DATE',	'IO SSD',	'IO SAD',	'Validation Flag',	
                    'Supply Type','Supply Detail',	'PD_PO','PRD',	'Order Type','Master Customer Number',
                    'Plant Ship date',	'Products',	'Offer PF',	'Plant','Plant SSD',
                    'PRM',	'PRQ',	'Key Account', 'Customer']
    """""
    
    columns_to_read = ['Demand','Region','SO Ship Org','Org Code','Area',	'Org Name',	'DEMAND VAL','Line Number', 'Customer_End Customer',
                    'CDP',	'CIS',	'Booked Dt','Request Dt', 'PD_Order','Ship Date','SAD','Inv Trigger','Product Range',
                    'Class','PID',	'PID Desc', 'PID Type','Supply Qty',
                    'Whse',	'IR No.',
                    'IO No.', 'PO No.', 'IO BOOKED DATE',	'IO SSD',	'IO SAD',	'Validation Flag',	
                    'Supply Type','Supply Detail',	'PD_PO','PRD',	'Order Type','Master Customer Number',
                    'Plant Ship date',	'Products',	'Offer PF',	'Plant','Plant SSD',
                    'Key Account', 'Customer']
    
    #workbench_data_eumocp_20250???_v2
    #workbench_data_eumocp_wPO_20250???_v2 'Vasanth
    files = glob.glob(folder_path+'\workbench_data_eumocp_20250???_v2.csv')
    #files = glob.glob(folder_path+'\workbench_data_eumocp_20250???_doc.csv')
    #df['file'] = df['filref'].str[22:30] 'doc
    #df['file'] = df['filref'].str[26:34] 'WPO

    files.sort()

    consolidated_data = pd.DataFrame()
    for file in files:
        #print(file)
        
        df=pd.read_csv(file, usecols=columns_to_read, encoding='utf-8', low_memory=False, keep_default_na='')
                
        df['filref'] = os.path.basename(file)
        df['file'] = df['filref'].str[22:30]      
        df['Report month'] = df['filref'].str[22:28]
        df['Report date'] = pd.to_datetime(df['file'], format='%Y%m%d')
        
        fileref = os.path.basename(file)
        filename = fileref[22:28]
        #print(filename)
        if filename >= start_month:
            if filename <= end_month:
                consolidated_data = pd.concat([consolidated_data,df],axis=0)

    OCP_IA = consolidated_data   
        
    values = ['Demand','Region','SO Ship Org','Org Code','Area','Org Name','DEMAND VAL','Line Number','Customer_End Customer','CDP','CIS','Booked Dt','Request Dt','PD_Order','Ship Date','SAD','Inv Trigger','Product Range','Class','PID','PID Desc','PID Type','Whse','IR No.','IO No.','IO BOOKED DATE','IO SSD','IO SAD','Validation Flag','Supply Type','Supply Detail','PD_PO','PRD','Order Type','Master Customer Number','Plant Ship date']

    OCP_IA = OCP_IA.drop_duplicates(subset=values ,keep='first')
    
    OCP_order_scheduling = OCP_IA.copy()

    OCP_order_scheduling = OCP_order_scheduling[OCP_order_scheduling['PID Type'] == 'M']
    
    prod_rng = ['Cash Dispense ATM', 'Multi Function ATM','PC Core Upgrade']
    OCP_order_scheduling = OCP_order_scheduling[OCP_order_scheduling['Product Range'].isin(prod_rng)]

    OCP_order_scheduling.to_csv(r'E:\_Projects\EUMOCP_Consolidate\input\ia_bundle_Orders.csv', index = False)
    
    #---------------------------Order Int--------------------------
    OCP_order_int = OCP_IA.copy()
    
    OCP_order_int = OCP_order_int[OCP_order_int['Demand'] == 'Ord Int']
    pid_type = ['M','K']
    OCP_order_int = OCP_order_int[OCP_order_int['PID Type'].isin(pid_type)]
   
    prod_rng = ['Int Ord CHS', 'Int Ord CHE','Int Ord GSL','Int Ord US']
    OCP_order_int= OCP_order_int[OCP_order_int['Order Type'].isin(prod_rng)]

    OCP_order_int.to_csv(r'E:\_Projects\EUMOCP_Consolidate\input\ia_bundle_int_orders.csv', index = False)
    
    print("\n")
    print("----------------------------------------")
    print("ia consolidation completed..")
    print("----------------------------------------")
    print("\n")
    main()


def order_metric():
    
    OCP_IA = pd.read_csv(r'E:\_Projects\EUMOCP_Consolidate\input\ia_bundle_Orders.csv', encoding='utf-8')
    sourcing = pd.read_excel(r'E:\_Projects\EUMOCP_Consolidate\input\Sourcing Tracker.xlsx', engine='openpyxl')
    latest_IA = pd.read_excel(r'E:\_Projects\EUMOCP_Consolidate\input\OCP_IA_EUMOCP.xlsx', engine='openpyxl')
    azure = pd.read_excel(r'E:\_Projects\EUMOCP_Consolidate\input\Shipments.xlsx', engine='openpyxl')
    SIT_II = pd.read_excel(r'E:\_Projects\EUMOCP_Consolidate\input\Global_Plant_Sourcing_Tracker_Atleos.xlsb', engine='pyxlsb', sheet_name='SIT_Product LTs')
    SIT_II_Zone = pd.read_excel(r'E:\_Projects\EUMOCP_Consolidate\input\Global_Plant_Sourcing_Tracker_Atleos.xlsb', engine='pyxlsb', sheet_name='Cntries split Region and Zone')


    OCP_IA['Metric Flag'] = np.where((OCP_IA['Demand'] == 'Order'), 'Order Scheduling' , '')
    
    OCP_IA['DEMAND VAL'] = OCP_IA['DEMAND VAL'].astype(str).str.strip()
    OCP_IA['Line Number'] = OCP_IA['Line Number'].astype(str).str.strip()
    OCP_IA['PID'] = OCP_IA['PID'].astype(str).str.strip()
    
    latest_IA['DEMAND VAL'] = latest_IA['DEMAND VAL'].astype(str).str.strip()
    latest_IA['Line Number'] = latest_IA['Line Number'].astype(str).str.strip()
    latest_IA['PID'] = latest_IA['PID'].astype(str).str.strip()
    
    #OCP_IA['UID'] = OCP_IA['DEMAND VAL'] + '|' + OCP_IA['Line Number'] + '|' + OCP_IA['IR No.'] + '|' + OCP_IA['IO No.'] + '|' + OCP_IA['PID']
    
    OCP_IA['UID'] = OCP_IA['DEMAND VAL'] + OCP_IA['PID']
    
    OCP_IA['Report date'] = pd.to_datetime(OCP_IA['Report date'], format='%d-%m-%Y')
    OCP_IA['Ship Date'] = pd.to_datetime(OCP_IA['Ship Date'], format='%d-%m-%Y')
    OCP_IA['Booked Dt'] = pd.to_datetime(OCP_IA['Booked Dt'], format='%d-%m-%Y')
    OCP_IA['SAD'] = pd.to_datetime(OCP_IA['SAD'], format='%d-%m-%Y')
    OCP_IA['PD_Order'] = pd.to_datetime(OCP_IA['PD_Order'], format='%d-%m-%Y')
    OCP_IA['IO BOOKED DATE'] = pd.to_datetime(OCP_IA['IO BOOKED DATE'], format='%d-%m-%Y')
    
    #OCP_IA['weeknumber'] = OCP_IA['Report date'].dt.strftime('%U').astype(int)
    
    #-------------------------------------------------------------------------------------
    #--------------------------------- Scheduling Date -----------------------------------
    
    #Scheduling date (EUMOCP IA Report - 1)
    min_report_date = OCP_IA.groupby('UID')['Report date'].min().reset_index()
    min_report_date.rename(columns={'Report date':'Scheduling date'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, min_report_date, on='UID', how='left')
    OCP_IA['First Report date'] = pd.to_datetime(OCP_IA['Scheduling date'], format='%d-%m-%Y')  - timedelta(days=0)
    OCP_IA['Scheduling date'] = pd.to_datetime(OCP_IA['Scheduling date'], format='%d-%m-%Y')  - timedelta(days=3)
    
    
    #-------------------------------------------------------------------------------------------------
    #--------------------------------- PO Last Appeared Date -----------------------------------------
    
    max_date = OCP_IA['Report date'].max()
    min_date = OCP_IA['Report date'].min()
    print(f"Latest report in dataset: {max_date}")
    
    #Maximun report date Of Order
    max_report_date = OCP_IA.groupby('UID')['Report date'].max().reset_index()
    max_report_date.rename(columns={'Report date':'last_report_date'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, max_report_date, on='UID', how='left')
    OCP_IA['last_report_date'] = pd.to_datetime(OCP_IA['last_report_date'], format='%d-%m-%Y')
    
    #-------------------------------------------------------------------------------------------------
    #--------------------------------- PO Status Open / Closed-----------------------------------------
    #OCP_IA['order_status'] = OCP_IA['last_report_date'].apply(lambda x: 'Closed' if x < max_date else 'Open')
    
    latest_IA = latest_IA[['DEMAND VAL','Line Number','PID']]
    latest_IA['order_status'] = 'Open'
    latest_IA .drop_duplicates(subset=['DEMAND VAL','Line Number','PID'], keep='first',inplace=True)
    OCP_IA = pd.merge(OCP_IA, latest_IA, on=['DEMAND VAL','Line Number','PID'], how='left')
    
    OCP_IA['order_status'] = np.where((OCP_IA['order_status'].isna()), 'Closed' , OCP_IA['order_status'])
    
    
    #-------------------------------------------------------------------------------------------
    #--------------------------------- First Promise Date calculation --------------------------------
    
    # Get First Promise Date
    OCP_IA['Flag'] = OCP_IA['Booked Dt'].apply(lambda x: 'Exclude' if x < min_date else 'Include')
    OCP_IA['Flag'] = np.where((OCP_IA['Demand'] == 'Flow Stk'), 'Flow Stk', OCP_IA['Flag'])
    
    OCP_PD = OCP_IA.copy()
    
    OCP_PD = OCP_IA[OCP_IA['PD_Order'].notna()]   #OCP_PD.dropna(subset=['Upd PD'])
    OCP_PD.sort_values(by=['Report date','UID','PD_Order'])
    OCP_PD.drop_duplicates(subset=['UID'], keep='first',inplace=True)
    OCP_PD = OCP_PD[['UID','PD_Order','Report date']]
    OCP_PD.rename(columns={'PD_Order':'temp_FPD', 'Report date':'temp_FPD updated on'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, OCP_PD, on='UID', how='left')
  
    OCP_IA['FPD'] = np.where((OCP_IA['order_status'] == 'Closed') & (OCP_IA['temp_FPD'].isna()), OCP_IA['last_report_date'] , OCP_IA['temp_FPD'])
    
    OCP_IA['FPD updated on'] = np.where((OCP_IA['order_status'] == 'Closed') & (OCP_IA['temp_FPD'].isna()), OCP_IA['last_report_date'] , OCP_IA['temp_FPD updated on'])
    
    #--------------------------------- Scheduling Date Vs PD date --------------------------------
    # Scheduling Date Vs PD date
    OCP_IA['Sch. dt vs PD Ageing'] =(pd.to_datetime(OCP_IA['FPD updated on']) - pd.to_datetime(OCP_IA['Scheduling date'])).dt.days

    con1 = OCP_IA['Sch. dt vs PD Ageing'] < 0
    con2 = OCP_IA['Sch. dt vs PD Ageing'] <= 5
    
    
    OCP_IA['Sch. dt vs PD Bucket'] = np.where(con1,'Error', \
                                 np.where(con2,'01-05days','05+ days'))
    
    OCP_IA['Sch. dt vs PD Bucket'] = np.where((OCP_IA['Sch. dt vs PD Ageing'].isna()), 'Yet to be Schedule', OCP_IA['Sch. dt vs PD Bucket'])
    
    #--------------------------------- #Scheduling Date Vs Booked Date --------------------------------
    #Scheduling Date Vs Booked Date
    OCP_IA['Sch. dt vs Booked dt Ageing'] =(pd.to_datetime(OCP_IA['Scheduling date']) - pd.to_datetime(OCP_IA['Booked Dt'])).dt.days
   
    con2 = OCP_IA['Sch. dt vs Booked dt Ageing'] <= 5
    
    OCP_IA['Sch. dt vs Booked dt Bucket'] = np.where(con2,'01-05days','05+ days')
    
    OCP_IA['Sch. dt vs Booked dt Bucket'] = np.where((OCP_IA['Sch. dt vs Booked dt Ageing'].isna()), 'Yet to be Schedule', OCP_IA['Sch. dt vs Booked dt Bucket'])
   
    #--------------------------------------------------------------------------------------------
    # --------------------------------IO Metric -------------------------------------------------
     
   #Scheduling Date Vs IO Booked Date
    OCP_IA['IO No.'] = pd.to_numeric(OCP_IA['IO No.'], errors='coerce')
    OCP_IA['IO Booked Dt upd'] = np.where((OCP_IA['IO BOOKED DATE'].isna()) & (OCP_IA['IO No.'].notna()), OCP_IA['Booked Dt'], OCP_IA['IO BOOKED DATE'])
    
    OCP_IA['IO Booked Dt upd'] = np.where((OCP_IA['Demand'] == 'Ord Int') & (OCP_IA['IO BOOKED DATE'].isna()), OCP_IA['Booked Dt'], OCP_IA['IO Booked Dt upd'])
    
     # Get First Promise Date
    OCP_IA['IO Flag'] = ''
    OCP_IA['IO Flag'] = np.where((OCP_IA['Demand'] == 'Flow Stk'), 'Flow Stk', OCP_IA['IO Flag'])
    OCP_IA['IO Flag'] = np.where((OCP_IA['SO Ship Org'] == 'GF1') & (OCP_IA['Whse'] == 'GF1'), 'Excl', OCP_IA['IO Flag'])
    OCP_IA['IO Flag'] = np.where((OCP_IA['SO Ship Org'] == 'UF2') & (OCP_IA['Whse'] == 'UF2'), 'Excl', OCP_IA['IO Flag'])
    OCP_IA['IO Flag'] = np.where((OCP_IA['Validation Flag'].isin(['New/Rel PO','OH','OH-RS'])), 'Excl', OCP_IA['IO Flag'])

    
    
    OCP_IO = OCP_IA.copy()
    
    OCP_IO = OCP_IO[OCP_IO['IO Booked Dt upd'].notna()]   #OCP_PD.dropna(subset=['Upd PD'])
    OCP_IO.sort_values(by=['Report date','UID'])
    OCP_IO.drop_duplicates(subset=['UID'], keep='first',inplace=True)
    OCP_IO = OCP_IO[['UID','Report date']]
    OCP_IO.rename(columns={'Report date':'FIO updated on'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, OCP_IO, on='UID', how='left')
    
    # --------------------------------IO First updated on Vs First Report Date ------------------------------------
  
    OCP_IA['IO Ageing'] = (pd.to_datetime(OCP_IA['FIO updated on']) - pd.to_datetime(OCP_IA['First Report date'])).dt.days
    
    # Create conditions and choices for ageing buckets
    con1 = OCP_IA['IO Ageing'] < 0 #Less 10
    con2 = OCP_IA['IO Ageing'] <= 5
    
    OCP_IA['IO Buket'] = np.where(con1,'Early', \
                                 np.where(con2,'00-05 days','05+ days'))
    
    OCP_IA['IO Buket'] = np.where((OCP_IA['FIO updated on'].isna()), 'Yet to be Schedule', OCP_IA['IO Buket'])
    
    
    # --------------------------------IO Booked Date Vs First Report Date ------------------------------------
    """
    OCP_IA['IO Booked dt to Report Date'] = (pd.to_datetime(OCP_IA['IO Booked Dt upd']) - pd.to_datetime(OCP_IA['First Report date'])).dt.days
    
    # Create conditions and choices for ageing buckets
    con1 = OCP_IA['IO Booked dt to Report Date'] < 0 #Less 10
    con2 = OCP_IA['IO Booked dt to Report Date'] <= 5
    
    OCP_IA['IO Booked dt to Report Date Buket'] = np.where(con1,'00-05 days', \
                                 np.where(con2,'00-05 days','05+ days'))
    
    OCP_IA['IO Booked dt to Report Date Buket'] = np.where((OCP_IA['FIO updated on'].isna()), 'Yet to be Schedule', OCP_IA['IO Booked dt to Report Date Buket'])
    """
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
    
    
    #--------------------------------------------------------------------------------------------
    #--------------------------------- Sourcing -------------------------------------------------
   
    sourcing.rename(columns={'REQUESTED DATE and TIME':'REQUESTED DATE', 'SOURCING COMPLETION DATE and Time':'SOURCING COMPLETION DATE'}, inplace=True)
    sourcing['concate'] = sourcing['PARTNUMBER'] + sourcing['ORG NAME']
    sourcing = sourcing[['concate','REQUESTED DATE','SOURCING COMPLETION DATE','Buyer Confirm Date','Source Day Calc','Buyer Day Calc']]
    OCP_IA['concate'] = OCP_IA['PID'] + OCP_IA['Whse']
    OCP_IA = pd.merge(OCP_IA, sourcing, on='concate', how='left')
    
    #-----------------------------------------------------------------------
    #------------------------------Shipments--------------------------------
    
    order_status = azure.copy()
    order_status['Actual Ship Date'] = pd.to_datetime(order_status['Actual Ship Date'], dayfirst=True)
    order_status['Order Number'] = order_status['Order Number'].astype(str).str.strip()
    order_status['Product ID'] = order_status['Product ID'].astype(str).str.strip()
    
    order_status = order_status[order_status['Actual Ship Date'].notna()]   #OCP_PD.dropna(subset=['Upd PD'])
    order_status = order_status[['Order Number', 'Line Number', 'Shipment Number', 'Product ID','Actual Ship Date']]
    order_status.sort_values(by=['Order Number', 'Line Number', 'Shipment Number', 'Product ID','Actual Ship Date'])
    order_status.drop_duplicates(subset=['Order Number', 'Line Number', 'Shipment Number', 'Product ID'], keep='last',inplace=True)
   
    #order_status.to_csv(r'E:\_Projects\EUMOCP_Consolidate\output\shipments_tracking.csv', index = False)
    
    order_status['Line'] = order_status['Line Number'].astype(str).str.strip() +'.'+ order_status['Shipment Number'].astype(str).str.strip()
    
    order_status = order_status[['Order Number','Line', 'Product ID', 'Actual Ship Date']]
    order_status.rename(columns={'Order Number':'DEMAND VAL', 'Product ID':'PID', 'Line':'Line Number'}, inplace=True)
    
    OCP_IA = pd.merge(OCP_IA, order_status, on=['DEMAND VAL','Line Number', 'PID'], how='left')
    
    #------------------------------------------------------------------------------
    #------------------------------SIT II Lead Time--------------------------------
    
     
    
    SIT_II_Zone = SIT_II_Zone[['Country Code','Zone']]
    SIT_II_Zone.rename(columns={'Country Code':'Org Code'}, inplace=True)
    SIT_II_Zone.drop_duplicates(subset=['Org Code','Zone'], keep='first',inplace=True)
    OCP_IA = pd.merge(OCP_IA, SIT_II_Zone, on=['Org Code'], how='left')
   
    
    SIT_1 = SIT_II[['Country Code','Class','Plant']]
    SIT_1['Plant'] = SIT_1['Plant'] .replace({"CHS": "CHE"})
    SIT_1.drop_duplicates(subset=['Country Code','Class','Plant'], keep='first',inplace=True)
    SIT_1.rename(columns={'Country Code':'Org Code', 'Plant':'Plant by CC'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, SIT_1, on=['Org Code','Class'], how='left')
   
    SIT_2 = SIT_II[['Zone','Class','Plant']]
    SIT_2['Plant'] = SIT_2['Plant'] .replace({"CHS": "CHE"})
    SIT_2.drop_duplicates(subset=['Zone','Class'], keep='first',inplace=True)
    SIT_2 = SIT_2[SIT_2['Zone'].notna()]
    SIT_2.rename(columns={'Plant':'Plant by Zone'}, inplace=True)
    SIT_2.to_csv(r'E:\_Projects\EUMOCP_Consolidate\output\sit.csv', index = False)
    OCP_IA = pd.merge(OCP_IA, SIT_2, on=['Zone','Class'], how='left')
    
    
    OCP_IA['temp_whse'] = OCP_IA['Whse']
    OCP_IA['temp_whse'] = OCP_IA['temp_whse'] .replace({"GF1": "EC", "UF2": "JAB", "CHS": "CHE"})
   

    condition = ((~OCP_IA['temp_whse'].isin(['CHE','EC','JAB'])))
    OCP_IA.loc[condition, 'temp_whse'] = None  # Set to None/NaN
    
    
   
    OCP_IA['Final Plant'] = OCP_IA.apply(lambda row: row['temp_whse'] if pd.notnull(row['temp_whse']) 
                               else (row['Plant by CC'] if pd.notnull(row['Plant by CC']) 
                                                 else row['Plant by Zone']), axis=1)
    
    
    SIT_3 = SIT_II[['Class','Plant','Unit LT']]
    SIT_3['Plant'] = SIT_3['Plant'] .replace({"CHS": "CHE"})
    SIT_3.drop_duplicates(subset=['Class','Plant'], keep='first',inplace=True)
    SIT_3.rename(columns={'Plant':'Final Plant'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, SIT_3, on=['Final Plant','Class'], how='left')
    
    """
    """
    #--------------------------------------------------------------------------
    #------------------------------Final Export--------------------------------
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








if __name__ == "__main__":
    main()