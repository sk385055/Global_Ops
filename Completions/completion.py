from asyncio.windows_events import NULL
import pandas as pd
import numpy as np 
from datetime import datetime,timedelta
from CTO_Shipment_mail import read_file,copy_mail,remove_mail
import sys
import os
import pyodbc
import time
import glob
import warnings
warnings.filterwarnings('ignore')

#today
dt = datetime.now()
date_str = dt.strftime("%Y%m%d")
#yesterday
dt_yest = datetime.now() - timedelta(days=1)
year_yest = dt_yest.strftime("%Y")
month_yest = dt_yest.strftime("%b").upper()
str_month = dt_yest.strftime("%Y%m")
#month in 
#------------------------------------------------------------------------------------------>start process
#def completions(name,file_name):
def completions(name,file_name):
    
    if name=='enn':
        out_file = 'completions_enn'  
    elif name=='jab':
        out_file = 'completions_jab'
        mail_in_path = r'.\CTO Shipments\mail_src'
        file_out_path = r'.\rcv'
        mail_out_path = r'.\CTO Shipments\mail_dst'
    elif name =='usi':
        out_file = 'completions_usi'
    elif name =='gut':
        out_file = 'completions_gut'
        gut_columns = ["Org", "ProductID","SystemClosedDate", "Qty"]
    elif name == 'zeb':
        out_file = 'completions_zeb'
    in_file=file_name 
    col_map_file_name ='completions_col_map' 
    prod_grp_file_name='class_offerpf_map'

    #--------------------------------------------------------------------------------------------------------

    # get the input file in rcv location 
    if name =='enn':
        data = pd.read_excel(r'.\rcv\\'+ in_file , sheet_name='Data_Source', engine='openpyxl') 
        data.columns = data.columns.str.strip( )
    elif name=='jab':
        in_file='jab_'+date_str
        #CTO_shipments mail scraping process(CTO_shipment_mail.py file in completion folder)------>start
        #reading eml in CTO Shipments\mail_src folder
        in_list_ctopath =glob.glob(mail_in_path+'\*.eml')

        for ctopath in in_list_ctopath:
            str_ctopath = str(ctopath)
            #get the file name only
            cto_file_name = str_ctopath[25:]
            #get the table inside the mail and convert to dataframe save in rcv folder
            read_file(cto_file_name,file_out_path,mail_in_path)
            #meanwhile take a copy file CTO Shipments\mail_dst folder
            copy_mail(cto_file_name,mail_in_path,mail_out_path)
            #remove a file CTO Shipments\mail_src folder
            remove_mail(cto_file_name,mail_in_path)
        #------------------------------------------------------------------------------------------------>end

        # CTO shipments data append the Daily jabil shipments --------------------------------------------->start
        data = pd.read_csv(r'.\rcv\CTO_shipement.csv',encoding='utf-8', low_memory=False, keep_default_na='')
        data = data.rename(columns={"MC":"PN","LastUpdated":"SHIPPED DATE"})
        #jabil update type column input
        catalog_sht = pd.read_excel(r'.\rcv\lookup\\jabil_type.xlsx', sheet_name='Catalog', engine='openpyxl')
       
       
        #---------------------------------------------------------------------------------------------->end
        
    elif name=='usi':
        #concat the data of  Data ground shipments sheet and Air & Sea Shipments into single sheet----------------------->start
        usi_sht=['Data ground shipments','Air & Sea Shipments']
        data = pd.read_excel(r'.\rcv\\'+ in_file, sheet_name=[usi_sht[0],usi_sht[1]], engine='openpyxl')
        #remove blant in this dataframe columns and values
        data[usi_sht[0]].columns = data[usi_sht[0]].columns.str.strip() 
        data[usi_sht[0]]['MODEL'] = data[usi_sht[0]]['MODEL'].str.strip()
        # drop unnamed column which are in this dataframe columns
        data[usi_sht[0]]=data[usi_sht[0]].loc[:,~data[usi_sht[0]].columns.str.match("Unnamed")]
        #change column names
        dict = {'HAWB': 'TRACKING',
        'Comments  / Comments Aditonal': 'Comments',
        }
        # # call rename () method column names
        data[usi_sht[1]].rename(columns=dict,inplace=True)
        data[usi_sht[1]]['Shipped Type']=usi_sht[1]
        data[usi_sht[0]]['Shipped Type']=usi_sht[0]
        #merge the 2 sheet into single dataframes
        data = pd.concat([data[usi_sht[0]],data[usi_sht[1]]],axis=0)
        #------------------------------------------------------------------------------------------------------------------>end
    elif name=='gut':
        #get the data in database------------------------------------------------------------------------------------------->start
       # data = pd.read_csv(r'E:\_Projects\completions\rcv\\ERPMFGGUT-'+month_yest+'-'+year_yest+'.csv',delimiter='|')
        #data = pd.read_csv(r'\\atm1502cifs.prod.local\cdunix\ERP\mpm\fsdmfg\prod\TOOLBOX\rcv\\ERPMFGGUT-'+month_yest+'-'+year_yest+'.csv',delimiter='|')
       

        # Assuming month_yest and year_yest are defined
        file_path = r'\\atm1502cifs.prod.local\cdunix\ERP\mpm\fsdmfg\prod\TOOLBOX\rcv\\ERPMFGGUT-' + month_yest + '-' + year_yest + '.csv'
        #file_path = r'E:\_Projects\completions\rcv\\ERPMFGGUT-' + month_yest + '-' + year_yest + '.csv'

        # Using on_bad_lines='skip' to handle lines with errors
        data = pd.read_csv(file_path, delimiter='|', on_bad_lines='skip')

        #print(data.head())



        print('ERPMFGGUT-'+month_yest+'-'+year_yest+'.csv')
        # print(data.shape)
        #for month end only
        # data = pd.read_csv(r'\\wtc1501cifs\cdunix\ERP\mpm\fsdmfg\prod\TOOLBOX\rcv\\ERPMFGGUT-APR-2023.csv',delimiter='|')
        data = data[pd.notnull(data['COMPLETION DATE'])]
        data = data.rename(columns={"ORGANIZATION": "Org","ASSEMBLY":"ProductID","COMPLETION DATE":"SystemClosedDate","COMPLETION QUANTITY":"Qty"})
        
        data = data[gut_columns]
        #----------------------------------------------------------------------------------------------------------------------->end
    elif name =='zeb':
        in_file='zeb_'+date_str
        data = pd.read_excel(r'.\rcv\\7895_Comp.xlsx' , sheet_name='Report1', engine='openpyxl') 
        data.columns = data.columns.str.strip( )
        data['Product ID'] = data['Product ID'].astype(str)
        #only fillter startwith 7895
        data = data[data['Product ID'].str.startswith("7895")]
        data['Product ID'] = data['Product ID'].str.slice(0,4)+'-'+data['Product ID'].str.slice(4,8)+'-'+data['Product ID'].str.slice(8,12)
        data = data[~data['Order Type Name'].isin(['INTERNAL','REQ INTERNAL','RMA NOCREDIT-SHIPPED ONLY','RMA NOCREDIT-INVOICED ONLY','RMA WITH CREDIT'])]
       
    #-------------------------------------------------------------------------------------------------------   
    # Reading lookup files----------------------------------------------------------------------------------------------->start        
    #get the input file in col_map_file
    col_map_file = pd.read_excel(r'.\rcv\lookup\\'+ col_map_file_name + '.xlsx', sheet_name='Sheet1', engine='openpyxl')
    #take a required columns
    col_map_file =col_map_file.iloc[:,0:13]
    #get the input file in prod_rng_file
    prod_rng_file = pd.read_csv(r'E:\_Projects\lookups\\'+ prod_grp_file_name + '.csv',encoding='utf-8', low_memory=False, keep_default_na='')
    #get the input file in bom_detail_file
    bom_detail_file = pd.read_csv(r'.\rcv\lookup\\bom_details.csv',encoding='utf-8', low_memory=False, keep_default_na='')
    #IPM for ss21
    ss21_file = pd.read_csv(r'.\rcv\lookup\\Inventory Parts Mgmt Report.csv',encoding='utf-8', low_memory=False, keep_default_na='')
    #7360 split for CHS
    bom_chs = pd.read_csv(r'.\rcv\lookup\\BOM Explosion.csv',encoding='utf-8', low_memory=False, keep_default_na='')

    #Read Historical file
    historical_file = pd.read_csv(r'E:\_Projects\_outputs\completion\\'+out_file+'_hist.csv',encoding='utf-8', low_memory=False, keep_default_na='')
      
    #--------------------------------------------------------------------------------------------------------------------end
    #data cook as per completion----------------------------------------------------------------------------------------->start
    data = pd.concat([data,col_map_file],axis=1)
    if name =='enn':
        data['Parent']= data['Parent'].astype(str)
        data['Parent'] = data['Parent'].str.strip()
        data['class'] = data['Parent'].str.slice(0,4)
        data['item']= data['Parent']
        data['site']="BUD"
        data['item_type']= np.where(data['Parent'].str.contains('M'),'M',data['item_type'])
        data['item_type']= np.where(data['Parent'].str.contains('MCC'),'MCC',data['item_type'])
        data['item_type']= np.where(data['Parent'].str.contains('-K'),'K',data['item_type'])
        data['item_type']= np.where(data['Parent'].str.contains('-C'),'C',data['item_type'])
        data['qty']=data['Quantity'].astype(int)
        data['Shipped']=data['Shipped'].astype('datetime64[ns]')
        data['ship_date']=data['Shipped']
        data['ship_date_str'] = data['Shipped'].dt.strftime("%Y%m%d")
    elif name == 'jab':
        data['PN']= data['PN'].astype(str)
        data['PN'] = data['PN'].str.strip()
        data['class'] = data['PN'].str.slice(0,4)
        data['item']= data['PN']
        data['site']="JAB"
        data['item_type']= np.where(data['PN'].str.contains('M'),'M',data['item_type'])
        data['item_type']= np.where(data['PN'].str.contains('MCC'),'MCC',data['item_type'])
        data['item_type']= np.where(data['PN'].str.contains('-K'),'K',data['item_type'])
        data['item_type']= np.where(data['PN'].str.contains('-C'),'C',data['item_type'])
        # data['SHIPPED DATE']= pd.to_datetime(data['SHIPPED DATE']).dt.date
        data['SHIPPED DATE']= pd.to_datetime(data['SHIPPED DATE'],format='mixed').dt.date
        data['SHIPPED DATE']= data['SHIPPED DATE'].astype('datetime64[ns]')
        data['ship_date']=data['SHIPPED DATE']
        data['qty']= data['Qty'].astype(int)
        data['ship_date_str'] = data['SHIPPED DATE'].dt.strftime("%Y%m%d")
            
    elif name =='usi':
        data['MODEL']= data['MODEL'].astype(str)
        data['MODEL'] = data['MODEL'].str.strip()
        data['class'] = data['MODEL'].str.slice(0,4)
        data['item']= data['MODEL']
        data['site']="USI"
        data['item_type']= np.where(data['MODEL'].str.contains('M'),'M',data['item_type'])
        data['item_type']= np.where(data['MODEL'].str.contains('MCC'),'MCC',data['item_type'])
        data['item_type']= np.where(data['MODEL'].str.contains('-K'),'K',data['item_type'])
        data['item_type']= np.where(data['MODEL'].str.contains('-C'),'C',data['item_type'])
        data['qty']=data['QTY'].astype(int)
        data['SHIPPING DATE']=data['SHIPPING DATE'].astype('datetime64[ns]')
        data['ship_date']=data['SHIPPING DATE']
        data['ship_date_str'] = data['SHIPPING DATE'].dt.strftime("%Y%m%d")
        
    elif name =='gut':
        data['ProductID'] = data['ProductID'].astype(str)
        data['ProductID'] = data['ProductID'].str.strip()
        data['class'] = data['ProductID'].str.slice(0,4)
        data['item']= data['ProductID']
        data['site']= data['Org']
        data['item_type']= np.where(data['ProductID'].str.contains('M'),'M',data['item_type'])
        data['item_type']= np.where(data['ProductID'].str.contains('MCC'),'MCC',data['item_type'])
        data['item_type']= np.where(data['ProductID'].str.contains('-K'),'K',data['item_type'])
        data['item_type']= np.where(data['ProductID'].str.contains('-C'),'C',data['item_type'])
        data['qty']=data['Qty']
        data['SystemClosedDate']=data['SystemClosedDate'].astype('datetime64[ns]')
        data['ship_date']=data['SystemClosedDate']
        data['ship_date_str'] = data['SystemClosedDate'].dt.strftime("%Y%m%d")
        # print(date_str)
        # data = data[~data['ship_date_str'].isin([date_str])]
        # data.drop(columns=gut_columns, axis=1,inplace=True)
        
    elif name == 'zeb':
        data['file_ref'] = in_file
        data['site']="ZEB"
        data['item'] = data['Product ID']
        data['item'] = data['item'].str.strip()
        data['class']= data['item'].str.slice(0,4)
        data['item_type']= np.where(data['item'].str.contains('-K'),'K','M')
        data['qty'] = data['Net Quantity']
        data['Actual Ship Date']  = data['Actual Ship Date'].astype('datetime64[ns]')
        data['ship_date'] = data['Actual Ship Date']
        data['ship_date_str'] = data['Actual Ship Date'].dt.strftime("%Y%m%d")
        data['unit']= np.where(data['item_type'] =='M','unit','non-unit')
    data['file_ref']=in_file
    data['unit']= np.where(data['item_type'] =='M','unit','non-unit')
    data['ship_date_str'] = data['ship_date_str'].astype(str)
    data['create_date']= date_str
    data['fyear'] = data['ship_date_str'].str.slice(0,4)
    data = data[~data['ship_date_str'].isin([date_str])]
    data = data[data['fyear'].isin([year_yest])]
    # print(data.shape)
    #------------------------------------------------------------------------------------------------------->end
    #update the week,month, qtr in this dataframe
    def upd_period(df, fld, fld_wk, fld_mon, fld_qtr):
        df[fld_wk] = pd.to_datetime(df[fld], errors='coerce').dt.isocalendar().week.astype(pd.Int64Dtype())
        df[fld_mon] = pd.to_datetime(df[fld], errors='coerce').dt.strftime('%Y%m')
        df[fld_qtr] = pd.to_datetime(df[fld], errors='coerce').dt.strftime('%Y') + 'Q' + pd.to_datetime(data[fld], errors='coerce').dt.quarter.astype(pd.Int64Dtype()).astype(str)    
    upd_period(data, 'ship_date_str','ship_date_wk','ship_date_mth','ship_date_qtr')
    data = data[data['ship_date_mth'].isin([str_month])]
    # print(data.shape)
   #--------------------------------------------------------------------------------------------
   # update product group/range
    rng_fld_col=['range','prod_grp_wb','offer_pf_wb']
    catalog_fld_col =['type']
    data = data.merge(prod_rng_file[['class']+rng_fld_col], on='class', how='left',validate='m:1')
    #ss21 for lpm
    ss21_file = ss21_file[ss21_file['Inventory Item Description'].str.startswith("SelfServ21") | ss21_file['Inventory Item Description'].str.startswith("SelfServ 21") ]
    ss21_file['ss21'] ='Y' 
    ss21_file = ss21_file.rename(columns={"Inventory Item": "item"})
    ss21_file = ss21_file[['item','ss21']]
    ss21_file = ss21_file.drop_duplicates()
    ss21_fld_col =['ss21']

    data = data.merge(ss21_file[['item']+ss21_fld_col], on='item', how='left',validate='m:1')
    
    data['ss21']= np.where(data['ss21']=="Y", data['ss21'],'N')
    

    #bom details file
    bom_detail_file =  bom_detail_file.rename(columns={"Dom/Export":"dom_exp","mcid":"item"})
    rng_fld_bom_col = ['dom_exp']
    data = data.merge(bom_detail_file[['item']+rng_fld_bom_col], on='item', how='left',validate='m:1')

    # update type column in completion data---------------------------------------------------------------------->start

    if name=='jab':
        catalog_sht =  catalog_sht.rename(columns={"MC": "PN","Type":"type"})
        data = data.merge(catalog_sht[['PN']+catalog_fld_col], on='PN', how='left',validate='m:1')
        data['type']= np.where(data['type']=='7360 Narrow','7360N',data['type'])
        data['type']= np.where(data['type']=='7360 Jarvis','7360J',data['type'])
        data['type'] = np.where(((data['class']=='7360') & (data['type'].isnull())),'7360S',data['type'])

        
    elif name =='gut':
        bom_chs =  bom_chs.rename(columns={"Top Parent Inventory Item": "item","Parent Inventory Item":"Assembly"})
        bom_chs =bom_chs[['item','Assembly']]
        bom_chs = bom_chs[bom_chs['Assembly'].isin(['7360-F001','7360-F002','7360-F003'])]  
        bom_chs = bom_chs[~bom_chs['item'].isin(['7360MC','7360MCC','7360M'])] 
        bom_chs = bom_chs.drop_duplicates()
        bom_chs['type'] = ''
        bom_chs['type']= np.where(bom_chs['Assembly']=='7360-F002','7360N',bom_chs['type'])
        bom_chs['type']= np.where(bom_chs['Assembly']=='7360-F003','7360J',bom_chs['type'])
        bom_chs['type']= np.where(bom_chs['Assembly']=='7360-F001','7360S',bom_chs['type'])
        data = data.merge(bom_chs[['item']+catalog_fld_col], on='item', how='left',validate='m:1')

    else:
        data[catalog_fld_col]=''
    # data.to_csv(r'.\snd\check.csv',index=False)
    # input("Break.....................")
    #------------------------------------------------------------------------------------------------------------>end
    #export the histroical file
    # delete dates which are in input file from histical and append it
    historical_file['ship_date_str'] = historical_file['ship_date_str'].astype(str)
    date_list = data['ship_date_str'].unique()
    historical_file = historical_file[~historical_file['ship_date_str'].isin(date_list)]
    historical_file = pd.concat([historical_file,data],axis=0)
    historical_file.to_csv(r'E:\_Projects\_outputs\completion' + '\\' + out_file +'_hist.csv',encoding='utf-8', index=False) 
    #historical_file.to_csv(r'\\Dayorg1\ORGSHARE\TEAMS\ERP Shared Folder\Global S&OP\workbench\completions' + '\\' + out_file +'.csv',encoding='utf-8', index=False) 
    #-----------------------------------------------------------------------------------------   
   
    # filter completion neccesary columns
    req_col = list(col_map_file.columns)+rng_fld_col+ss21_fld_col+rng_fld_bom_col+catalog_fld_col
    data= data[req_col]
    #-------------------------------------------------------------------------------------------
    # export into csv ->eg: name = completion_plantname.csv
    #local save
    data.to_csv(r'E:\_Projects\_outputs\completion' + '\\' + out_file +'.csv',encoding='utf-8', index=False)

  
# take some Example on gived last line                     
if __name__ == "__main__":
    print('Filter only '+str_month+' month records')
    sources =['enn','jab','gut']
    #get passing second arguments
    in_list = sys.argv[2:]
    #get passing first arguments
    in_date = sys.argv[1]
    #get the list of all xlsx file in below folder
    files_list = glob.glob(r'.\rcv\\'+'\*xlsx')
    #searching the file name as per you passing the plants arguments
    file_search_str ={"enn":"Shipped","jab":"CTO_shipement","gut":"gut","usi":"USI Outbound tracker","zeb":"zeb"}
    # if you pass the second arguments as 'all' it automatically run all the plant['enn','jab','usi','gut','zeb']------>start
    if in_list[0]=='all':
        for name in sources:
            if name == 'gut':
                file_name = name+date_str
                completions(name,file_name)

            else:  
                for file in files_list: 
                    str_file = str(file)
                    file_name = os.path.basename(file)
                    if file_name.__contains__(file_search_str[name]) & file_name.__contains__(in_date):
                        print(file_name)
                        completions(name,file_name)  
        #----------------------------------------------------------------------------------------------------------------->end 
        # if you pass the decond arguments as particullar plant it run you passing aruguments only ------------------------>start                 
    else:
        for name in in_list:
            if name == 'gut':
                file_name = name+date_str
                completions(name,file_name)
            elif name == 'zeb':
                file_name = name+date_str
                print(file_name)
                completions(name,file_name)
            elif name =='jab':
                file_name = name+date_str
                print(file_name)
                completions(name,file_name)

            else:
                for file in files_list:
                    str_file = str(file)
                    file_name = os.path.basename(file)
                    if file_name.__contains__(file_search_str[name]) & file_name.__contains__(in_date): 
                        print(file_name)
                        completions(name,file_name)
        #---------------------------------------------------------------------------------------------------------------------->end
    #How to pass the argument?
    #run the command prompt or power shell -> type the below text
    #1. first argument ->date ->should be 'Ymd' Eg: 20230123
    #2. second argument -> plant name ->should be [enn jab usi gut zeb] only
    # if you pass second argument all it run all palnt in default----->    eg: python completion.py 20230123 all
    # else you mention particular plants it run particular plant only-->   eg: python completion.py 20230123 enn jab usi zeb gut
    # full ex\mple-> python completion.py first argument second argument

                
                


                     
                    
      


    