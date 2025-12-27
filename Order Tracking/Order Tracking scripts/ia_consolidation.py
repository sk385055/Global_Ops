import os
import pandas as pd
import numpy as np
import glob
from datetime import datetime
from datetime import timedelta

from pyxlsb import open_workbook as open_xlsb

def main():
    User_Input = int(input('1) ia_consolidate | 2) order_metric | 3) weekly_metric | 4) int_order_metric | 5) EXIT : '))
    
    if User_Input == 1:
        ia_consolidate()
    elif User_Input == 2:
        order_metric() 
    elif User_Input == 3:
        Weekly_metric() 
    elif User_Input == 4:
         int_order_metric()
    else:
        print("Thank you...")
        return
    
def ia_consolidate():
    
    start_month = "202501"
    end_month = "202511"
    
    folder_path = r'E:\_Projects\_outputs\orders\snd'
    
    columns_to_read = ['Demand','Region','SO Ship Org','Org Code','Area',	'Org Name',	'DEMAND VAL','Line Number', 'Customer_End Customer',
                    'CDP',	'CIS',	'Booked Dt','Request Dt', 'PD_Order','Ship Date','SAD','Inv Trigger','Product Range',
                    'Class','PID',	'PID Desc', 'PID Type','LOB','Supply Qty',
                    'Whse',	'IR No.',
                    'IO No.', 'PO No.', 'IO BOOKED DATE',	'IO SSD',	'IO SAD', 'Seiban Number','Validation Flag',	
                    'Supply Type','Supply Detail',	'PD_PO','PRD',	'Order Type','Master Customer Number',
                    'Plant Ship date',	'Products',	'Offer PF',	'Plant','Plant SSD',
                    'Key Account', 'Customer']
    
    files = glob.glob(folder_path+'\workbench_data_eumocp_2025????_v2.csv')

    files.sort()

    consolidated_data = pd.DataFrame()
    for file in files:
        #print(file)
        
        df=pd.read_csv(file, usecols=columns_to_read, encoding='utf-8', low_memory=False, keep_default_na='')
                
        df['filref'] = os.path.basename(file)
        df['file'] = df['filref'].str[22:30]      
        df['Report month'] = df['filref'].str[22:28]
        df['Report date'] = pd.to_datetime(df['file'], format='%Y%m%d')
        df['week'] = df['Report date'].dt.strftime('%a')
        
        fileref = os.path.basename(file)
        filename = fileref[22:28]
        #print(filename)
        #consolidated_data = pd.concat([consolidated_data,df],axis=0)
        if filename <= end_month:
            print(file)
            consolidated_data = pd.concat([consolidated_data,df],axis=0)

    OCP_IA = consolidated_data   
        
    values = ['Demand','Region','SO Ship Org','Org Code','Area','Org Name','DEMAND VAL','Line Number','Customer_End Customer','CDP','CIS','Booked Dt','Request Dt','PD_Order','Ship Date','SAD','Inv Trigger','Product Range','Class','PID','PID Desc','PID Type', 'LOB','Whse','IR No.','IO No.','IO BOOKED DATE','IO SSD','IO SAD','Seiban Number','Validation Flag','Supply Type','Supply Detail','PD_PO','PRD','Order Type','Master Customer Number','Plant Ship date']

    OCP_IA = OCP_IA.drop_duplicates(subset=values ,keep='first')
    
    OCP_order_scheduling = OCP_IA.copy()
    
    item_type = ['M','K']
    
    OCP_order_scheduling = OCP_order_scheduling[OCP_order_scheduling['PID Type'].isin(item_type)]
    
    OCP_order_scheduling = OCP_order_scheduling[OCP_order_scheduling['LOB'] == 'FIN']
    
    weekly_metric = OCP_order_scheduling[OCP_order_scheduling['week'] == 'Mon']
    
    OCP_order_scheduling.to_csv(r'E:\_Projects\EUMOCP_Consolidate\input\ia_bundle_Orders.csv', index = False)
    
    weekly_metric.to_csv(r'E:\_Projects\EUMOCP_Consolidate\input\ia_weekly_metric.csv', index = False)
    
    #------------------------------------------------------------------------------
    #---------------------------Order Int---------------------------------------
    OCP_order_int = OCP_IA.copy()
    
    OCP_order_int = OCP_order_int[OCP_order_int['Demand'] == 'Ord Int']
    pid_type = ['M','K']
    OCP_order_int = OCP_order_int[OCP_order_int['PID Type'].isin(pid_type)]
   
    type = ['Int Ord CHS', 'Int Ord CHE','Int Ord GSL','Int Ord US']
    OCP_order_int= OCP_order_int[OCP_order_int['Order Type'].isin(type)]
    
    OCP_order_int = OCP_order_int[OCP_order_int['week'] == 'Mon']

    OCP_order_int.to_csv(r'E:\_Projects\EUMOCP_Consolidate\input\ia_bundle_int_orders.csv', index = False)
    
    print("\n")
    print("----------------------------------------")
    print("ia consolidation completed..")
    print("----------------------------------------")
    print("\n")
    main()
    
    
    
from datetime import datetime

def count_weekends(start_date, end_date):
        if pd.isna(start_date) or pd.isna(end_date) or start_date > end_date:
            return 0
        weekend_count = 0
        current_date = start_date
        # Iterate through each day between the dates
        while current_date <= end_date:
            # Check if current day is Saturday (5) or Sunday (6)
            if current_date.weekday() in [5, 6]:
                weekend_count += 1
            current_date += timedelta(days=1)
        return weekend_count
    
    
def try_parse_date(date_val):
        if pd.isna(date_val):
            return date_val  # Keep NaN as is
        date_str = str(date_val).strip()
        formats = ['%d-%m-%Y', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d %b %Y', '%d %B %Y']
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return date_val  # Return original if no format matches
    
def assign_count(row):
        if row['week_name'] == 'Mon':
            return row['First Report date'] - timedelta(days=4)
        elif row['week_name'] == 'Tue':
            return row['First Report date'] - timedelta(days=4)
        elif row['week_name'] == 'Wed':
            return row['First Report date'] - timedelta(days=3)
        else:
            return row['First Report date'] - timedelta(days=2)
    
def order_metric():
    
    OCP_IA = pd.read_csv(r'E:\_Projects\EUMOCP_Consolidate\input\ia_bundle_Orders.csv', encoding='utf-8')
    latest_IA = pd.read_excel(r'E:\_Projects\EUMOCP_Consolidate\input\Order_Status.xlsx', engine='openpyxl')
    CDP = pd.read_csv(r'E:\_Projects\EUMOCP_Consolidate\input\CDP_exceptions.csv', encoding='utf-8')
    cancelled = pd.read_excel(r'E:\_Projects\EUMOCP_Consolidate\input\Cancelled_Order.xlsx', engine='openpyxl')
    
    OCP_IA['DEMAND VAL'] = OCP_IA['DEMAND VAL'].astype(str).str.strip()
    OCP_IA['PID'] = OCP_IA['PID'].astype(str).str.strip()
    OCP_IA['Line Number'] = OCP_IA['Line Number'].astype(str).str.strip()
   
    OCP_IA['UID'] = OCP_IA['DEMAND VAL'] + OCP_IA['PID']

    OCP_IA['Report date'] = OCP_IA['Report date'].apply(try_parse_date)
    OCP_IA['Ship Date'] = OCP_IA['Ship Date'].apply(try_parse_date)
    OCP_IA['Booked Dt'] = OCP_IA['Booked Dt'].apply(try_parse_date)
    OCP_IA['SAD'] = OCP_IA['SAD'].apply(try_parse_date)
    OCP_IA['PD_Order'] = OCP_IA['PD_Order'].apply(try_parse_date)
    OCP_IA['IO BOOKED DATE'] = OCP_IA['IO BOOKED DATE'].apply(try_parse_date)
     
    #-------------------------------------------------------------------------------------
    #--------------------------------- Scheduling Date -----------------------------------
    
    #Scheduling date (EUMOCP IA Report - 1)
    min_report_date = OCP_IA.groupby('UID')['Report date'].min().reset_index()
    min_report_date.rename(columns={'Report date':'Scheduling date'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, min_report_date, on='UID', how='left')
    OCP_IA['First Report date'] = pd.to_datetime(OCP_IA['Scheduling date'], format='%d-%m-%Y')  - timedelta(days=0)
    
    # ------------------------------------- Add Days of the weeks -------------------------------------------
    
    OCP_IA['week_name'] = OCP_IA['First Report date'].dt.strftime('%a')
    OCP_IA['Scheduling date'] = OCP_IA.apply(assign_count, axis=1)
    
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
    
    latest_IA['Order Number'] = latest_IA['Order Number'].astype(str).str.strip()
    latest_IA['Product ID'] = latest_IA['Product ID'].astype(str).str.strip()
    latest_IA.rename(columns={'Product ID':'PID','Order Number':'DEMAND VAL'}, inplace=True)
    
    latest_IA['Line Number'] = latest_IA['Line Number'].astype(str) + '.' + latest_IA['Shipment Number'].astype(str)
    latest_IA['Line Number'] = latest_IA['Line Number'].astype(str).str.strip() 
    
      
    latest_IA = latest_IA[['DEMAND VAL','PID','Line Status']]
    latest_IA .drop_duplicates(subset=['DEMAND VAL','PID'], keep='first',inplace=True)
    
    OCP_IA = pd.merge(OCP_IA, latest_IA, on=['DEMAND VAL','PID'], how='left')
    
    
    #----------------------------------------------------------------------------------------------------------
    #--------------------------------- CANCELLED Orders -------------------------------------------------------
    
    cancelled['Order Number'] = cancelled['Order Number'].astype(str).str.strip()
    cancelled['Product ID'] = cancelled['Product ID'].astype(str).str.strip()
    cancelled['Line Number'] = cancelled['Line Number'].astype(str) + '.' + cancelled['Shipment Number'].astype(str)
    cancelled['Line Number'] = cancelled['Line Number'].astype(str).str.strip()
    
    cancelled.rename(columns={'Product ID':'PID','Order Number':'DEMAND VAL'}, inplace=True)
    
    cancelled = cancelled[['DEMAND VAL','PID','Cancel Line Date']]
    cancelled['Cancelled'] = 'Cancelled'
    
    cancelled.drop_duplicates(subset=['DEMAND VAL','PID'], keep='first',inplace=True)
    OCP_IA = pd.merge(OCP_IA, cancelled, on=['DEMAND VAL','PID'], how='left')

    OCP_IA['Cancelled'] = np.where((OCP_IA['Cancelled'].isna()), 'No' , OCP_IA['Cancelled'])
    
    OCP_IA['Line Status'] = np.where((OCP_IA['Cancelled']=='Cancelled'), 'Cancelled' , OCP_IA['Line Status'])
    
    #-------------------------------------------------------------------------------------------
    #--------------------------------- Flag Exclusion ------------------------------------------
    
    OCP_IA['Metric Flag'] = np.where((OCP_IA['Demand'] == 'Order'), 'Order Scheduling' , '')
    
    # Get First Promise Date
    OCP_IA['Flag'] = OCP_IA['Booked Dt'].apply(lambda x: 'Exclude' if x < min_date else 'Include')
    OCP_IA['Flag'] = np.where((OCP_IA['Demand'] == 'Flow Stk'), 'Flow Stk', OCP_IA['Flag'])
    
    OCP_IA = pd.merge(OCP_IA, CDP, on=['CDP'], how='left')
    OCP_IA['CDP Flag'] = OCP_IA['CDP Flag'].fillna('Include')
    #-------------------------------------------------------------------------------------------------
    #--------------------------------- #Scheduling Date Vs Booked Date --------------------------------
   
    OCP_IA['Booked Weekend'] = OCP_IA.apply(lambda row: count_weekends(row['Booked Dt'], row['Scheduling date']), axis=1)
    OCP_IA['Sch. dt vs Booked dt Ageing'] =(pd.to_datetime(OCP_IA['Scheduling date']) - pd.to_datetime(OCP_IA['Booked Dt'])).dt.days
    
    OCP_IA['Sch. dt vs Booked dt Ageing']  = OCP_IA['Sch. dt vs Booked dt Ageing'] - OCP_IA['Booked Weekend']
   
    con2 = OCP_IA['Sch. dt vs Booked dt Ageing'] <= 5
    
    OCP_IA['Sch. dt vs Booked dt Bucket'] = np.where(con2,'01-05days','05+ days')
    
    OCP_IA['Sch. dt vs Booked dt Bucket'] = np.where((OCP_IA['Sch. dt vs Booked dt Ageing'].isna()), 'Yet to be Schedule', OCP_IA['Sch. dt vs Booked dt Bucket'])
   
    #-------------------------------------------------------------------------------------------------
    #--------------------------------- First Promise Date calculation --------------------------------
    
    OCP_PD = OCP_IA.copy()
    
    OCP_PD = OCP_IA[OCP_IA['PD_Order'].notna()]   #OCP_PD.dropna(subset=['Upd PD'])
    OCP_PD.sort_values(by=['Report date','UID','PD_Order'])
    OCP_PD.drop_duplicates(subset=['UID'], keep='first',inplace=True)
    OCP_PD = OCP_PD[['UID','PD_Order','Report date']]
    OCP_PD.rename(columns={'PD_Order':'temp_FPD', 'Report date':'temp_FPD updated on'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, OCP_PD, on='UID', how='left')
  
  
    OCP_IA['FPD'] = np.where((OCP_IA['Line Status'].str.contains('Closed|Cancel')) & (OCP_IA['temp_FPD'].isna()), OCP_IA['last_report_date'] , OCP_IA['temp_FPD'])
    OCP_IA['FPD updated on'] = np.where((OCP_IA['Line Status'].str.contains('Closed|Cancel')) & (OCP_IA['temp_FPD'].isna()), OCP_IA['last_report_date'] , OCP_IA['temp_FPD updated on'])
    
    #-------------------------------------------------------------------------------------------------
    #--------------------------------- Scheduling Date Vs PD date -------------------------------------
    
    OCP_IA['First Report date'] = pd.to_datetime(OCP_IA['First Report date'], format='%d-%m-%Y')
    OCP_IA['FPD updated on'] = pd.to_datetime(OCP_IA['FPD updated on'], format='%d-%m-%Y')

    OCP_IA['PD Weekend'] = OCP_IA.apply(lambda row: count_weekends(row['First Report date'], row['FPD updated on']), axis=1)
    
    OCP_IA['Sch. dt vs PD Ageing'] =(pd.to_datetime(OCP_IA['FPD updated on']) - pd.to_datetime(OCP_IA['First Report date'])).dt.days
    OCP_IA['Sch. dt vs PD Ageing']  = OCP_IA['Sch. dt vs PD Ageing'] - OCP_IA['PD Weekend']
    
    con1 = OCP_IA['Sch. dt vs PD Ageing'] < 0
    con2 = OCP_IA['Sch. dt vs PD Ageing'] <= 5
    
    
    OCP_IA['Sch. dt vs PD Bucket'] = np.where(con1,'Error', \
                                 np.where(con2,'01-05days','05+ days'))
    
    OCP_IA['Sch. dt vs PD Bucket'] = np.where((OCP_IA['Sch. dt vs PD Ageing'].isna()), 'Yet to be Schedule', OCP_IA['Sch. dt vs PD Bucket'])
    
    #--------------------------------------------------------------------------
    #------------------------------Final Export--------------------------------
    OCP_IA.to_csv(r'E:\_Projects\EUMOCP_Consolidate\output\ia_order_tracking.csv', index = False)
    
    
    print("\n")
    print("----------------------------------------")
    print("Order Metric created..")
    print("----------------------------------------")
    print("\n")
    main()
    

def Weekly_metric(): 
    
    OCP_IA = pd.read_csv(r'E:\_Projects\EUMOCP_Consolidate\input\ia_weekly_metric.csv', encoding='utf-8')
    latest_IA = pd.read_excel(r'E:\_Projects\EUMOCP_Consolidate\input\Order_Status.xlsx', engine='openpyxl')
    CDP = pd.read_csv(r'E:\_Projects\EUMOCP_Consolidate\input\CDP_exceptions.csv', encoding='utf-8')
    cancelled = pd.read_excel(r'E:\_Projects\EUMOCP_Consolidate\input\Cancelled_Order.xlsx', engine='openpyxl')
    
    OCP_IA['Report date'] = OCP_IA['Report date'].apply(try_parse_date)
    OCP_IA['Ship Date'] = OCP_IA['Ship Date'].apply(try_parse_date)
    OCP_IA['Booked Dt'] = OCP_IA['Booked Dt'].apply(try_parse_date)
    OCP_IA['SAD'] = OCP_IA['SAD'].apply(try_parse_date)
    OCP_IA['PD_Order'] = OCP_IA['PD_Order'].apply(try_parse_date)
    OCP_IA['IO BOOKED DATE'] = OCP_IA['IO BOOKED DATE'].apply(try_parse_date)
    
    OCP_IA['DEMAND VAL'] =OCP_IA['DEMAND VAL'].astype(str).str.strip()
    OCP_IA['PID'] = OCP_IA['PID'].astype(str).str.strip()
    OCP_IA['Line Number'] = OCP_IA['Line Number'].astype(str).str.strip()
    
    OCP_IA['UID'] = OCP_IA['DEMAND VAL'] + OCP_IA['PID']
    
    
    #--------------------------------- Exclusions ----------------------------------------------
    OCP_IA['Metric Flag'] = np.where((OCP_IA['Demand'] == 'Order'), 'Order Scheduling' , '')
    OCP_IA = pd.merge(OCP_IA, CDP, on=['CDP'], how='left')
    OCP_IA['CDP Flag'] = OCP_IA['CDP Flag'].fillna('Include')
    
    #--------------------------------- PO Last Report Date -----------------------------------------
    
    max_date = OCP_IA['Report date'].max()
    print(f"Latest report in dataset: {max_date}")
    
    #Maximun report date Of Order
    max_report_date = OCP_IA.groupby('UID')['Report date'].max().reset_index()
    max_report_date.rename(columns={'Report date':'last_report_date'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, max_report_date, on='UID', how='left')
    OCP_IA['last_report_date'] = pd.to_datetime(OCP_IA['last_report_date'], format='%d-%m-%Y')
    
    #--------------------------------- PO Status Open / Closed-----------------------------------------
    
    latest_IA['Order Number'] = latest_IA['Order Number'].astype(str).str.strip()
    latest_IA['Product ID'] = latest_IA['Product ID'].astype(str).str.strip()
    latest_IA.rename(columns={'Product ID':'PID','Order Number':'DEMAND VAL'}, inplace=True)
    
    latest_IA['Line Number'] = latest_IA['Line Number'].astype(str) + '.' + latest_IA['Shipment Number'].astype(str)
    latest_IA['Line Number'] = latest_IA['Line Number'].astype(str).str.strip() 
    
    latest_IA = latest_IA[['DEMAND VAL','PID','Line Status']]
    latest_IA .drop_duplicates(subset=['DEMAND VAL','PID'], keep='first',inplace=True)
    
    OCP_IA = pd.merge(OCP_IA, latest_IA, on=['DEMAND VAL','PID'], how='left')
    
    
    #----------------------------------------------------------------------------------------------------------
    #--------------------------------- CANCELLED Orders -------------------------------------------------------
    
    cancelled['Order Number'] = cancelled['Order Number'].astype(str).str.strip()
    cancelled['Product ID'] = cancelled['Product ID'].astype(str).str.strip()
    cancelled['Line Number'] = cancelled['Line Number'].astype(str) + '.' + cancelled['Shipment Number'].astype(str)
    cancelled['Line Number'] = cancelled['Line Number'].astype(str).str.strip()
    cancelled.rename(columns={'Product ID':'PID','Order Number':'DEMAND VAL'}, inplace=True)
    
    cancelled = cancelled[['DEMAND VAL','PID','Cancel Line Date']]
    cancelled['Cancelled'] = 'Cancelled'
    
    cancelled.drop_duplicates(subset=['DEMAND VAL','PID'], keep='first',inplace=True)
    OCP_IA = pd.merge(OCP_IA, cancelled, on=['DEMAND VAL','PID'], how='left')

    OCP_IA['Cancelled'] = np.where((OCP_IA['Cancelled'].isna()), 'No' , OCP_IA['Cancelled'])
    
    OCP_IA['Line Status'] = np.where((OCP_IA['Cancelled']=='Cancelled'), 'Cancelled' , OCP_IA['Line Status'])
    
    #--------------------------------- Scheduling Date -----------------------------------
    
    #Scheduling date (EUMOCP IA Report - 1)
    min_report_date = OCP_IA.groupby('UID')['Report date'].min().reset_index()
    min_report_date.rename(columns={'Report date':'Scheduling date'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, min_report_date, on='UID', how='left')
    OCP_IA['First Report date'] = pd.to_datetime(OCP_IA['Scheduling date'], format='%d-%m-%Y')  - timedelta(days=0)
    
    # ------------------------------------- Add Days of the weeks -------------------------------------------
    
    OCP_IA['week_name'] = OCP_IA['First Report date'].dt.strftime('%a')
    OCP_IA['Scheduling date'] = OCP_IA.apply(assign_count, axis=1)
    
    #--------------------------------- First Promise Date calculation --------------------------------
    OCP_PD = OCP_IA.copy()
    
    OCP_PD = OCP_IA[OCP_IA['PD_Order'].notna()]   #OCP_PD.dropna(subset=['Upd PD'])
    OCP_PD.sort_values(by=['Report date','UID','PD_Order'])
    OCP_PD.drop_duplicates(subset=['UID'], keep='first',inplace=True)
    OCP_PD = OCP_PD[['UID','PD_Order','Report date']]
    OCP_PD.rename(columns={'PD_Order':'temp_FPD', 'Report date':'temp_FPD updated on'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, OCP_PD, on='UID', how='left')
  
  
    OCP_IA['FPD'] = np.where((OCP_IA['Line Status'].str.contains('Closed|Cancel')) & (OCP_IA['temp_FPD'].isna()), OCP_IA['last_report_date'] , OCP_IA['temp_FPD'])
    OCP_IA['FPD updated on'] = np.where((OCP_IA['Line Status'].str.contains('Closed|Cancel')) & (OCP_IA['temp_FPD'].isna()), OCP_IA['last_report_date'] , OCP_IA['temp_FPD updated on'])
    
    
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
    OCP_IA['Upd PD'] = np.where((OCP_IA['Line Status'].str.contains('Closed')) & (OCP_IA['PD_Order'].isna()), OCP_IA['last_report_date'], OCP_IA['PD_Order'])
    
    #Upd Promise date Vs SAD%
    OCP_IA['PD vs SAD Flag'] = np.where((OCP_IA['Upd PD'] < OCP_IA['SAD']), 'Bad - PD < SAD', 'Good - PD > SAD')    
    
    
    #--------------------------------------------------------------------------------------------
    #--------------------------------- Past Due -------------------------------------------------
    
    OCP_IA['Past Due SSD Flag'] = np.where((OCP_IA['Ship Date'] < OCP_IA['Report date']), 'Yes - Past Due SSD', 'No')
    
    OCP_IA['Past Due PD Flag'] = np.where((OCP_IA['PD_Order'] < OCP_IA['Report date']), 'Yes - Past Due PD', 'No')
   
    
    #--------------------------------------------------------------------------
    #------------------------------Final Export--------------------------------
    OCP_IA.to_csv(r'E:\_Projects\EUMOCP_Consolidate\output\ia_weekly_metric.csv', index = False)
    
    
    print("\n")
    print("----------------------------------------")
    print("Order Metric created..")
    print("----------------------------------------")
    print("\n")
    main()



def int_order_metric():
    
    OCP_IA = pd.read_csv(r'E:\_Projects\EUMOCP_Consolidate\input\ia_bundle_Orders.csv', encoding='utf-8',keep_default_na=False)
    int_ord = pd.read_csv(r'E:\_Projects\EUMOCP_Consolidate\input\ia_bundle_int_orders.csv', encoding='utf-8')
    Preferred = pd.read_csv(r'E:\_Projects\EUMOCP_Consolidate\input\preferred_offer.csv')
    latest_IA = pd.read_excel(r'E:\_Projects\EUMOCP_Consolidate\input\Order_Status.xlsx', engine='openpyxl')
    cancelled = pd.read_excel(r'E:\_Projects\EUMOCP_Consolidate\input\Cancelled_Order.xlsx', engine='openpyxl')
    
    OCP_IA = OCP_IA[(OCP_IA['Supply Qty'].astype(int))>0]

    OCP_IA['DEMAND VAL'] = OCP_IA['DEMAND VAL'].astype(str).str.strip()
    OCP_IA['PID'] = OCP_IA['PID'].astype(str).str.strip()
    OCP_IA['Line Number'] = OCP_IA['Line Number'].astype(str).str.strip()
    OCP_IA['Seiban Number'] = OCP_IA['Seiban Number'].astype(str).str.strip()
    OCP_IA['UID'] = OCP_IA['DEMAND VAL'] + OCP_IA['PID']
        
    OCP_IA['Report date'] = OCP_IA['Report date'].apply(try_parse_date)
    OCP_IA['Ship Date'] = OCP_IA['Ship Date'].apply(try_parse_date)
    OCP_IA['Booked Dt'] = OCP_IA['Booked Dt'].apply(try_parse_date)
    OCP_IA['SAD'] = OCP_IA['SAD'].apply(try_parse_date)
    OCP_IA['PD_Order'] = OCP_IA['PD_Order'].apply(try_parse_date)
    OCP_IA['IO BOOKED DATE'] = OCP_IA['IO BOOKED DATE'].apply(try_parse_date)
    
    
    Preferred = Preferred[['PID','Preferred_offer_Type']]
    Preferred.drop_duplicates(subset=['PID'], inplace=True)
    OCP_IA = pd.merge(OCP_IA, Preferred, on='PID', how='left')
    
    #-----------------------------------------------------------------------------------------------------
    #--------------------------------- IO Metric ---------------------------------------------------------
    
    min_date = OCP_IA['Report date'].min()
    OCP_IA['Flag'] = OCP_IA['Booked Dt'].apply(lambda x: 'Exclude' if x < min_date else 'Include')

    OCP_IA['IO Flag'] = 'Include'
    OCP_IA['IO Flag'] = np.where((OCP_IA['Demand'] == 'Flow Stk'), 'Flow Stk', OCP_IA['IO Flag'])
    OCP_IA['IO Flag'] = np.where((OCP_IA['SO Ship Org'] == 'GF1') & (OCP_IA['Whse'] == 'GF1'), 'Excl', OCP_IA['IO Flag'])
    OCP_IA['IO Flag'] = np.where((OCP_IA['SO Ship Org'] == 'UF2') & (OCP_IA['Whse'] == 'UF2'), 'Excl', OCP_IA['IO Flag'])
    OCP_IA['IO Flag'] = np.where((OCP_IA['SO Ship Org'].isin(['GI2','USL'])), 'Excl', OCP_IA['IO Flag'])
    OCP_IA['IO Flag'] = np.where((OCP_IA['Validation Flag'].isin(['New/Rel PO','OH','OH-RS'])), 'Excl', OCP_IA['IO Flag'])
    
    #OCP_IA['IO Flag'] = OCP_IA['IO Flag'].fillna('Include')
    
    #--------------------------------- Seiban Number ---------------------------------------------------------
    
    #--------------------------------- IO Booked Date updated On ----------------------------------------------
    
    Seiban = OCP_IA.copy()
    Seiban = Seiban[Seiban['Seiban Number'].notnull()]   #OCP_PD.dropna(subset=['Upd PD'])
    Seiban  = Seiban [Seiban ['Seiban Number'].str.strip() != ""]
    Seiban.sort_values(by=['Report date','UID', 'Seiban Number'])
    Seiban.drop_duplicates(subset=['UID'], keep='first',inplace=True)
    Seiban = Seiban[['UID','Seiban Number']]
    Seiban.rename(columns={'Seiban Number':'temp_Seiban Number'}, inplace=True)
    #Seiban.to_csv(r'E:\_Projects\EUMOCP_Consolidate\output\Seiban.csv', index = False)
    OCP_IA = pd.merge(OCP_IA, Seiban, on='UID', how='left')
    
    
    OCP_IA['Seiban Number upd'] = np.where((OCP_IA['Seiban Number'].str.strip() == ""), OCP_IA['temp_Seiban Number'], OCP_IA['Seiban Number'])
   
    # Remove letters to extract numeric part
    OCP_IA['Seiban Numeric'] = OCP_IA['Seiban Number upd'].str.extract(r'(\d+)')
    
    
    OCP_IA['Seiban Flag'] = OCP_IA.apply(
        lambda row: 'Include' if pd.isna(row['Seiban Numeric']) or str(row['DEMAND VAL']) == str(row['Seiban Numeric']) else 'Exclude',
        axis=1
    )

    
    OCP_IA['Seiban Flag'] = np.where((OCP_IA['Seiban Numeric'].isnull()), 'Include', OCP_IA['Seiban Flag'])

    
    #----------------------------------------------------------------------------------------------------------
    #--------------------------------- Order Status Closed / Open ---------------------------------------------
    latest_IA['Order Number'] = latest_IA['Order Number'].astype(str).str.strip()
    latest_IA['Product ID'] = latest_IA['Product ID'].astype(str).str.strip()
    latest_IA.rename(columns={'Product ID':'PID','Order Number':'DEMAND VAL'}, inplace=True)
    
    latest_IA['Line Number'] = latest_IA['Line Number'].astype(str) + '.' + latest_IA['Shipment Number'].astype(str)
    latest_IA['Line Number'] = latest_IA['Line Number'].astype(str).str.strip() 
     
    latest_IA = latest_IA[['DEMAND VAL','PID','Line Status']]
    latest_IA .drop_duplicates(subset=['DEMAND VAL','PID'], keep='first',inplace=True)
    
    OCP_IA = pd.merge(OCP_IA, latest_IA, on=['DEMAND VAL','PID'], how='left')
    
    #----------------------------------------------------------------------------------------------------------
    #--------------------------------- CANCELLED Orders -------------------------------------------------------
    
    cancelled['Order Number'] = cancelled['Order Number'].astype(str).str.strip()
    cancelled['Product ID'] = cancelled['Product ID'].astype(str).str.strip()
    cancelled['Line Number'] = cancelled['Line Number'].astype(str) + '.' + cancelled['Shipment Number'].astype(str)
    cancelled['Line Number'] = cancelled['Line Number'].astype(str).str.strip()
    cancelled.rename(columns={'Product ID':'PID','Order Number':'DEMAND VAL'}, inplace=True)
    
    cancelled = cancelled[['DEMAND VAL','PID','Cancel Line Date']]
    cancelled['Cancelled'] = 'Cancelled'
    
    cancelled.drop_duplicates(subset=['DEMAND VAL','PID'], keep='first',inplace=True)
    OCP_IA = pd.merge(OCP_IA, cancelled, on=['DEMAND VAL','PID'], how='left')

    OCP_IA['Cancelled'] = np.where((OCP_IA['Cancelled'].isna()), 'No' , OCP_IA['Cancelled'])
    
    OCP_IA['Line Status'] = np.where((OCP_IA['Cancelled']=='Cancelled'), 'Cancelled' , OCP_IA['Line Status'])

    #--------------------------------- First Report Date ---------------------------------------------------------
    min_report_date = OCP_IA.groupby('UID')['Report date'].min().reset_index()
    min_report_date.rename(columns={'Report date':'Scheduling date'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, min_report_date, on='UID', how='left')
    OCP_IA['First Report date'] = pd.to_datetime(OCP_IA['Scheduling date'], format='%d-%m-%Y')  - timedelta(days=0)
    
    #--------------------------------- IO Booked upd ---------------------------------------------------------
   
    OCP_IA['IO Booked Dt upd'] = np.where((OCP_IA['IO BOOKED DATE'].isna()) & (OCP_IA['IO No.'].notna()), OCP_IA['Booked Dt'], OCP_IA['IO BOOKED DATE'])

    #--------------------------------- IO Booked Date updated On ----------------------------------------------
    OCP_IO = OCP_IA.copy()
    OCP_IO = OCP_IO[OCP_IO['IO Booked Dt upd'].notna()]   #OCP_PD.dropna(subset=['Upd PD'])
    OCP_IO  = OCP_IO [OCP_IO ['IO Booked Dt upd']  != ""]
    OCP_IO.sort_values(by=['Report date','UID'])
    OCP_IO.drop_duplicates(subset=['UID'], keep='first',inplace=True)
    OCP_IO = OCP_IO[['UID','Report date']]
    OCP_IO.rename(columns={'Report date':'FIO updated on'}, inplace=True)
    OCP_IA = pd.merge(OCP_IA, OCP_IO, on='UID', how='left')
    
    #--------------------------------- If Pref Offer then FIO updated on as REport Date + 3  ---------------------------------------------------------
    
    OCP_IA.loc[OCP_IA['Preferred_offer_Type']=='CORE ATM','FIO updated on'] = OCP_IA['First Report date'] + pd.Timedelta(days=3)
      
    #--------------------------------- Weekend Count ---------------------------------------------------------
    
    OCP_IA['First Report date'] = pd.to_datetime(OCP_IA['First Report date'], format='%d-%m-%Y')
    OCP_IA['FIO updated on'] = pd.to_datetime(OCP_IA['FIO updated on'], format='%d-%m-%Y')
    
    OCP_IA['Weekend Count'] = OCP_IA.apply(lambda row: count_weekends(row['First Report date'], row['FIO updated on']), axis=1)
    
    OCP_IA['IO Ageing'] = (pd.to_datetime(OCP_IA['FIO updated on']) - pd.to_datetime(OCP_IA['First Report date'])).dt.days
    
    OCP_IA['IO Ageing']  = OCP_IA['IO Ageing'] - OCP_IA['Weekend Count'] 

    # Create conditions and choices for ageing buckets
    con1 = OCP_IA['IO Ageing'] < 0 #Less 10
    con2 = OCP_IA['IO Ageing'] <= 5

    OCP_IA['IO Buket'] = np.where(con1,'00-05 days', \
    np.where(con2,'00-05 days','05+ days'))

    OCP_IA['IO Buket'] = np.where((OCP_IA['FIO updated on'].isna()), 'Yet to be Schedule', OCP_IA['IO Buket'])

    OCP_IA.to_csv(r'E:\_Projects\EUMOCP_Consolidate\output\io_tracking.csv', index = False)
    
    #-----------------------------------------------------------------------------------------------------
    #--------------------------------- Past Due - IO SSD -------------------------------------------------
    
    int_ord['DEMAND VAL'] = int_ord['DEMAND VAL'].astype(str).str.strip()
    int_ord['PID'] = int_ord['PID'].astype(str).str.strip()
    int_ord['Line Number'] = int_ord['Line Number'].astype(str).str.strip()

    int_ord['UID'] = int_ord['DEMAND VAL'] + int_ord['PID']
    
    int_ord['Report date'] = int_ord['Report date'].apply(try_parse_date)
    int_ord['Ship Date'] = int_ord['Ship Date'].apply(try_parse_date)
    int_ord['Booked Dt'] = int_ord['Booked Dt'].apply(try_parse_date)
    int_ord['SAD'] = int_ord['SAD'].apply(try_parse_date)
    int_ord['PD_Order'] = int_ord['PD_Order'].apply(try_parse_date)
    int_ord['IO BOOKED DATE'] = int_ord['IO BOOKED DATE'].apply(try_parse_date)
    int_ord['IO SSD'] = int_ord['IO SSD'].apply(try_parse_date)
    
    int_ord['weeknumber'] = int_ord['Report date'].dt.strftime('%U').astype(int)
    
    #----------------------------------------------------------------------------------------------------------
    #--------------------------------- Order Status Closed / Open ---------------------------------------------
    latest_IA = latest_IA[['DEMAND VAL','PID','Line Status']]
    latest_IA.drop_duplicates(subset=['DEMAND VAL','PID'], keep='first',inplace=True)
    int_ord = pd.merge(int_ord, latest_IA, on=['DEMAND VAL','PID'], how='left')
    
    
    #----------------------------------------------------------------------------------------------------------
    #--------------------------------- CANCELLED Orders -------------------------------------------------------
    cancelled = cancelled[['DEMAND VAL','PID','Cancel Line Date']]
    cancelled['Cancelled'] = 'Cancelled'
    cancelled.drop_duplicates(subset=['DEMAND VAL','PID'], keep='first',inplace=True)
    int_ord = pd.merge(int_ord, cancelled, on=['DEMAND VAL','PID'], how='left')
    int_ord['Cancelled'] = np.where((int_ord['Cancelled'].isna()), 'No' , int_ord['Cancelled'])
    
    int_ord['Line Status'] = np.where((int_ord['Cancelled']=='Cancelled'), 'Cancelled' , int_ord['Line Status'])
    
    #----------------------------------------------------------------------------------------------------------
    #--------------------------------- Past Due - IO SSD Flag -------------------------------------------------
    
    int_ord['Past Due SSD Flag'] = np.where((int_ord['IO SSD'] < int_ord['Report date']), 'Yes - Past Due SSD', 'No')
    
    int_ord['Ageing Past Due SSD'] = (pd.to_datetime(int_ord['Report date']) - pd.to_datetime(int_ord['IO SSD'])).dt.days
    
    # Create conditions and choices for ageing buckets
    con1 = int_ord['Ageing Past Due SSD'] <= 0 #Less 10
    con2 = int_ord['Ageing Past Due SSD'] <= 2

    
    int_ord['Past Sue SSD Bucket'] = np.where(con1,'No', \
                                 np.where(con2,'No','Yes - Past Due SSD'))
    
    int_ord['Past Sue SSD Bucket'] = np.where((int_ord['Ageing Past Due SSD'].isna()), 'Yet to Sch.', int_ord['Past Sue SSD Bucket'])
    
    int_ord['Past Due SSD Flag'] = np.where((int_ord['Ageing Past Due SSD'].isna()), 'Yet to Sch.', int_ord['Past Due SSD Flag'])
    
    int_ord.to_csv(r'E:\_Projects\EUMOCP_Consolidate\output\ia_int_order_tracking.csv', index = False)
    
    print("\n")
    print("----------------------------------------")
    print("Order Metric created..")
    print("----------------------------------------")
    print("\n")
    main()



    #OCP_IA['Report date'] = pd.to_datetime(OCP_IA['Report date'], format='%d-%m-%Y')
    #OCP_IA['Ship Date'] = pd.to_datetime(OCP_IA['Ship Date'], format='%d-%m-%Y')
    #OCP_IA['Booked Dt'] = pd.to_datetime(OCP_IA['Booked Dt'], format='%d-%m-%Y')
    #OCP_IA['SAD'] = pd.to_datetime(OCP_IA['SAD'], format='%d-%m-%Y')
    #OCP_IA['PD_Order'] = pd.to_datetime(OCP_IA['PD_Order'], format='%d-%m-%Y')
    #OCP_IA['IO BOOKED DATE'] = pd.to_datetime(OCP_IA['IO BOOKED DATE'], format='%d-%m-%Y')




if __name__ == "__main__":
    main()