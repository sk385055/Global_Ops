print('******** Choose Report to Run *********\n')
User_Input = int(input('1) POR Report | 2) STAT POR | 3) Ex-Inventory Report : '))

if User_Input ==1:
    import pandas as pd

    ########################################## 

    # This script run with original Country Code without any changes 

    ##########################################



    #Date = Ex.Date
    Date = input("Enter POR Date (eg:DD-MMM-YYYY) : ")
    #Date = '08-MAY-2024'

    file = 'NCR_OCP_IND_POR_'+Date+'.csv'
    Path ="E:\\_Projects\\IND POR\\Input\\"+file
    Server =r'\\wtc1501cifs.prod.local\cdunix\ERP\OCP\SNOP\DEMREPP\rcv\\'+file
    Input_Path = r'E:\_Projects\IND POR\Ex-Inventory\Input\\'
    Input_Location = r'E:\_Projects\IND POR\Input\\'


    from datetime import datetime

    def reformat_date(date_str):
       # Parse the date string
       dt = datetime.strptime(date_str, '%b-%Y')
       # Reformat the date string to the desired format
       formatted_date = dt.strftime('%b-%y')
       # Capitalize the first letter of the month
       return formatted_date.capitalize()
    # Example usage


    def reformat_date_Plan(date_str):
       # Parse the date string
       dt = datetime.strptime(date_str, '%y-%b')
       # Reformat the date string to the desired format
       formatted_date = dt.strftime('%b-%y')
       # Capitalize the first letter of the month
       return formatted_date.capitalize()



    try:
    #OCP = pd.read_csv(r'C:\Vasanthan\Python\NCR\IND POR\Input\NCR_OCP_IND_POR_25-DEC-2023.csv',na_values=[''],keep_default_na=False,low_memory=False)
        try:
            OCP = pd.read_csv(Path,na_values=[''],keep_default_na=False,low_memory=False)
        except:
            print('Checking IND POR in Server...!')
            OCP = pd.read_csv(Server,na_values=[''],keep_default_na=False,low_memory=False)
            print('POR Pulled from Server...!')
            
        col_name = list(OCP.columns)
        #print(len(col_name))
        count = len(col_name)

        for i in range(19, count ,1):
          # print(col_name[i])
           try:
             col_name[i] = reformat_date(col_name[i])
           except: 
             try:
                 col_name[i]  = pd.to_datetime(col_name[i] , format='%y-%b').strftime('%b-%y')
             except:
                 try:
                     col_name[i]  = pd.to_datetime(col_name[i] , format='%Y-%b').strftime('%b-%y')
                 except:
                     try:
                         col_name[i]  = pd.to_datetime(col_name[i] , format='mixed').strftime('%b-%y')
                     except:    
                         #print(output_date) 
                         print(col_name[i])

        OCP.columns = col_name
       # print(OCP.columns)
        
        print('*************POR AUTOMAION STARTED**************')
        #print(OCP.columns)
        Theater = pd.read_excel(r'E:\_Projects\IND POR\Input\Mapping.xlsx',sheet_name='Theatre Mapping')
        
        OCP.columns = OCP.columns.str.strip()
        
        
        ############# 
        
        #OCP['Total'] = OCP.iloc[:,19:].sum(axis=1).round(0)
        
        #OCP = OCP[OCP['Total']!=0]
        
        
        OCP = OCP[OCP['CLASS'].astype(str).str.isnumeric()]
        
        
        
        #OCP.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\OCP before lob.csv',index=False)
        
        ############### KEY ACCOUNT OTHER
        
        
        
        #OCP.loc[(OCP['KEY_ACCOUNT'].str[:6].isin(['OTHER-'])),'KEY_ACCOUNT']=('OTHER-'+ OCP['COUNTRY_CODE'])
        mask = OCP['KEY_ACCOUNT'].str[:6].isin(['OTHER-']) & OCP['COUNTRY_CODE'].notna()
        OCP.loc[mask, 'KEY_ACCOUNT'] = 'OTHER-' + OCP['COUNTRY_CODE']
        
        
        
        ######################### Class Separation ###################################
        Separation = pd.read_csv(r'E:\_Projects\IND POR\Input\Product Range Separation.csv', na_values=[''],keep_default_na=False)
        
        OCP.loc[OCP['CLASS'].isin(Separation['Class']),'Product Separation']='Commerce'
        
        OCP = OCP[OCP['Product Separation'] != ('Commerce')]
        
        OCP = OCP.filter(regex='^(?!.*Product Separation$)')
        
        #################################################################################
        values_to_exclude = ['SALES FORECAST', 'SHIPMENTS FORECAST', 'BASE TOTAL DEMAND']
        OCP = OCP[~OCP['DATA_SERIES'].isin(values_to_exclude)]
        
        
        ############## Bill to Key account
        
        OCP.loc[OCP['CUSTOMER_LOB'] == "MULTIPLE", 'CUSTOMER_LOB'] = OCP['PRODUCT_LOB']
        
        OCP.loc[OCP['CUSTOMER_LOB'].astype(str).str[:3] == "FIN", 'CUSTOMER_LOB'] = "FINANCIAL"
        OCP.loc[OCP['CUSTOMER_LOB'].astype(str).str[:3] == "HSR", 'CUSTOMER_LOB'] = "HOSPITALITY"
        
        values = ['Cash Dispense ATM','Multi Function ATM','PC Core Upgrade','Conf Upgrade','Branch Kiosk']
        
        OCP.loc[OCP['PRODUCT_RANGE'].isin(values),'CUSTOMER_LOB'] = 'FINANCIAL'
        
        OCP.loc[OCP['PRODUCT_RANGE'].isin(values),'CUSTOMER_LOB'] = 'FINANCIAL'

        #OCP.loc[OCP['COUNTRY_CODE']=='IQ','COUNTRY_CODE']=OCP['BILL_TO_COUNTRY']
        
        
        
       # OCP.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\NCR 1.csv',index=False)
        ############# Bill to Key account
        #OCP.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\NCR 1.csv',index=False)
        #OCP['KeyAccount_Billing']=None
        
        
        OCP['KeyAccount_Billing']=OCP['KEY_ACCOUNT']
        OCP.loc[((OCP['KEY_ACCOUNT'].str[:6].isin(['OTHER-'])) & (OCP['BILL_TO_COUNTRY']!='OTHER')),'KeyAccount_Billing']=('OTHER-'+OCP['BILL_TO_COUNTRY'])
        OCP.loc[((OCP['KEY_ACCOUNT'].str[:6].isin(['OTHER-'])) & (OCP['BILL_TO_COUNTRY']=='OTHER')),'KeyAccount_Billing']='OTHER-??'
        
        ################## DTF update
        #OCP.loc[((OCP['ITEM_TYPE']=='Unassigned') & (OCP['DATA_SERIES']=='NEW DEMAND')) & (OCP['DTF'].isnull()),'DTF']=260
        
        
        
        #OCP.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\OCP before lob.csv',index=False)
        
        ###################### KEY ACCOUNT CARDRONICS ASP = 0
        
        OCP.loc[OCP['KEY_ACCOUNT'].str.contains('CARDTRONIC'),'ASP']=0
        OCP.loc[OCP['KEY_ACCOUNT'].str.contains('ATMAAS',na=False,case=False),'ASP']=0
        
        
        ####################### LOB Region/LOB Area Update
        LOB_CC_KA = pd.read_excel(r'E:\_Projects\IND POR\Input\Mapping.xlsx',sheet_name='LOB_CC_KA', na_values=[''],keep_default_na=False)
        LOB_CC = pd.read_excel(r'E:\_Projects\IND POR\Input\Mapping.xlsx',sheet_name='LOB_CC', na_values=[''],keep_default_na=False)
        
        
        
        #OCP.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\OCP before LOB .csv',index=False)
        ################################################### LOB Area / LOB Region ###################$$
        
        LOB_CC_KA = LOB_CC_KA[['CONCAT','LOB REGION_1','LOB AREA_1']]
        
        ############## Mering ######################
        OCP['CONCAT']=OCP['COUNTRY_CODE']+OCP['KEY_ACCOUNT']
        
        # Merge the dataframes using the filtered LOB dataframe
        merged = pd.merge(OCP, LOB_CC_KA, on='CONCAT', how='left')
        OCP = pd.merge(merged, LOB_CC, on='COUNTRY_CODE', how='left')
        
        ################### Assign value to LOB Region and LOB Area
        OCP.loc[OCP['LOB REGION_1'].notnull(),'LOB REGION'] = OCP['LOB REGION_1'] 
        OCP.loc[OCP['LOB AREA_1'].notnull(),'LOB AREA'] = OCP['LOB AREA_1'] 
        
        OCP.loc[((OCP['LOB REGION'].isnull()) & (OCP['LOB REGION_2'].notnull())),'LOB REGION'] = OCP['LOB REGION_2'] 
        OCP.loc[((OCP['LOB AREA'].isnull()) & (OCP['LOB AREA_2'].notnull())),'LOB AREA'] = OCP['LOB AREA_2'] 
        
        
        ########### Delete Columns
        
        OCP = OCP.filter(regex='^(?!.*CONCAT$)')
        OCP = OCP.filter(regex='^(?!.*_2$)')
        OCP = OCP.filter(regex='^(?!.*_1$)')
        
        #OCP.to_csv(r'E:\_Projects\IND POR\OCP before ASP.csv',index=False)
        
        ######################################################### DTF Upldate ##########################
        
        from pandas.tseries.offsets import MonthEnd
        
        OCP['Start Date'] = (pd.to_datetime('today') + MonthEnd(0))
        
        OCP.loc[OCP['DTF'].notnull() & (OCP['DTF'] != 0), 'End Date'] = (OCP['Start Date'] + pd.to_timedelta(OCP['DTF'], unit='D')).dt.strftime('%m/%d/%Y')
        
        OCP.loc[(OCP['DTF'].isnull() | (OCP['DTF']==0)),'End Date'] = (pd.to_datetime('today') + MonthEnd(0)).strftime('%m/%d/%Y')
        
        OCP['Start Date'] = (pd.to_datetime('today') + MonthEnd(0)).strftime('%m/%d/%Y')
        
        OCP['DTF'] = OCP['DTF'].fillna(0)
        OCP['DTF cutoff']=pd.Timestamp.now() + OCP['DTF'].astype('timedelta64[D]')
        OCP['DTF cutoff']=OCP['DTF cutoff'].dt.strftime('%m-%d-%y')
        
        # Assuming 'df' is the DataFrame containing the 'End Date' column
        OCP['End_Date'] = pd.to_datetime(OCP['End Date'], format='%m/%d/%Y').dt.strftime('%b-%y')
        OCP['Start_Date'] = pd.to_datetime(OCP['Start Date'], format='%m/%d/%Y').dt.strftime('%b-%y')
    
        #OCP.to_csv(r'E:\_Projects\IND POR\Output\NCR 1.csv',index=False)
        
        
        #print(OCP.columns)
        ############################ DTF Risk Unit ###################################
        #print(OCP['End_Date'])
        # Define a function to get the index of a column
        def get_column_index(value):
            try:
                return OCP.columns.get_loc(value)
            except KeyError:
                return None
        
        # Apply the function to find the index of 'End Date' column
        OCP['index'] = OCP['End_Date'].apply(get_column_index) + 1
        
        OCP['S_index'] = OCP['Start_Date'].apply(get_column_index) - 1
        
        OCP.loc[OCP['DATA_SERIES']=='SHIP NOT INV','S_index'] = OCP['S_index']+1
        
       # OCP.to_csv(r'E:\_Projects\IND POR\Output\NCR 2.csv',index=False)

        
        # Create a new column 'DTF Risk' with the sum of values from index 19 to the 'index' column value
        #OCP['DTF risk units'] = OCP.apply(lambda row: row.iloc[19:int(row['index'])].sum(), axis=1)
        OCP['DTF risk units'] = OCP.apply(lambda row: row.iloc[int(row['S_index']):int(row['index'])].sum(), axis=1)
        
        
        OCP = OCP.filter(regex='^(?!.*End_Date$)')
        OCP = OCP.filter(regex='^(?!.*Start_Date$)')
        
        OCP.loc[OCP['DATA_SERIES']=='SHIPMENTS HISTORY','DTF risk units']=0
        
        OCP.to_csv(r'E:\_Projects\IND POR\Output\NCR Dft risk.csv',index=False)
        
        ##################################### DTF Risk ########################################
        
        
        OCP['DTF risk'] = OCP['DTF risk units'].apply(lambda x: 'Yes' if x > 0 else 'No')
        
        
        #################################### Remove unwanted Data #######################################
        OCP=OCP[~OCP['DATA_SERIES'].isin(['SHIPMENTS FORECAST','BASE TOTAL DEMAND','SALES FORECAST'])]
        
        
        OCP['Comments'] = '1_OCP Data Extract'
    
        Header={"REGION":"Region","AREA":"Area","LOB REGION":"LOB Region","LOB AREA":"LOB Area","COUNTRY":"Country Name","COUNTRY_CODE":"Country Code","BILL_TO_COUNTRY":"Bill to Country","ZONE":"Zone","CUSTOMER_LOB":"Customer LOB","PRODUCT_LOB":"Product LOB","PRODUCT_RANGE":"Product Range","PARENT_MODEL":"Parent Model","CLASS":"Class","DESCRIPTION":"Description","ORGANIZATION_CODE":"Organization Code","KEY_ACCOUNT":"Key Account","MASTER_CUSTOMER":"Master Customer","ITEM_TYPE":"Item Type","DATA_SERIES":"Data Series"}
        
        OCP = OCP.rename(columns=Header)
        
        #df.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\NCR final.csv', index=False)
        
        columns_to_move = [
            'Region', 'Area', 'LOB Region', 'LOB Area', 'Country Name', 'Country Code',
            'Bill to Country', 'Zone', 'Customer LOB', 'Product LOB', 'Product Range',
            'Parent Model', 'Class', 'Description', 'DTF', 'DTF cutoff', 'Start Date',
            'End Date', 'DTF risk units', 'DTF risk', 'Organization Code', 'KeyAccount_Billing',
            'Key Account', 'Master Customer', 'Item Type', 'ASP','Data Series'
        ]
        
        
        #df = df[columns_to_move + [col for col in df.columns if col not in columns_to_move]]
        
        remaining_columns = [col for col in OCP.columns if col not in columns_to_move]
        
        OCP = OCP[columns_to_move + remaining_columns]
        
        
        OCP = OCP.filter(regex='^(?!.*index$)')
        OCP = OCP.filter(regex='^(?!.*Total$)')
        
        
        Offline = pd.read_csv(r'E:\_Projects\IND POR\Input\Offlines.csv', na_values=[''],keep_default_na=False)
        Ex = pd.read_csv(r'E:\_Projects\IND POR\Input\EX-report.csv', na_values=[''],keep_default_na=False)
        Inv = pd.read_csv(r'E:\_Projects\IND POR\Input\Inventory-report.csv', na_values=[''],keep_default_na=False)
        Plan = pd.read_csv(r'E:\_Projects\IND POR\Input\Plan.csv', na_values=[''],keep_default_na=False)
        
        Ex.loc[Ex['Key Account'].str.contains('ATMAAS', case=False),'ASP']=0
        Offline.loc[Offline['Key Account'].str.contains('ATMAAS', case=False),'ASP']=0
        Inv.loc[Inv['Key Account'].str.contains('ATMAAS', case=False),'ASP']=0
        Plan.loc[Plan['Key Account'].str.contains('ATMAAS', case=False, na=False),'ASP']=0

        Ex.loc[Ex['Key Account'].str.contains('CARDTRONICS', case=False),'ASP']=0
        Offline.loc[Offline['Key Account'].str.contains('CARDTRONICS', case=False),'ASP']=0
        Inv.loc[Inv['Key Account'].str.contains('CARDTRONICS', case=False),'ASP']=0
        Plan.loc[Plan['Key Account'].str.contains('CARDTRONICS', case=False, na=False),'ASP']=0        
    ############################### Formate Change for all Demand month columns ##################
    ######## Ex ################
        col_name = list(Ex.columns)
        #print(len(col_name))
        count = len(col_name) - 1

        for i in range(28, count ,1):
            try:
                col_name[i]  = pd.to_datetime(col_name[i] , format='mixed').strftime('%b-%y')
            except: 
                try:
                    col_name[i]  = pd.to_datetime(col_name[i] , format='%y-%b').strftime('%b-%y')
                except:
                    try:
                        col_name[i]  = pd.to_datetime(col_name[i] , format='%Y-%b').strftime('%b-%y')
                    except:
                        abz="DUmmy"
                        #print(col_name[i])

        Ex.columns = col_name
        
    ######## In #############   
        col_name = list(Inv.columns)
        #print(len(col_name))
        count = len(col_name) - 1

        for i in range(28, count ,1):
            try:
                col_name[i]  = pd.to_datetime(col_name[i] , format='mixed').strftime('%b-%y')
            except: 
                try:
                    col_name[i]  = pd.to_datetime(col_name[i] , format='%y-%b').strftime('%b-%y')
                except:
                    try:
                        col_name[i]  = pd.to_datetime(col_name[i] , format='%Y-%b').strftime('%b-%y')
                    except:
                        abz="DUmmy"
                        #print(col_name[i])
        Inv.columns = col_name
        
    ######### Offline #############
    ##Remove column
    # List of columns to delete
        try:
            columns_to_delete = ['Demand Stream']
            # Drop the specified columns
            Offline.drop(columns=columns_to_delete, inplace=True)
        except: 
            pass
        col_name = list(Offline.columns)
        #print(len(col_name))
        count = len(col_name) - 1

        for i in range(28, count ,1):
            try:
                col_name[i]  = pd.to_datetime(col_name[i] , format='mixed').strftime('%b-%y')
            except: 
                try:
                    col_name[i]  = pd.to_datetime(col_name[i] , format='%y-%b').strftime('%b-%y')
                except:
                    try:
                        col_name[i]  = pd.to_datetime(col_name[i] , format='%Y-%b').strftime('%b-%y')
                    except:
                        abz="DUmmy"
        Offline.columns = col_name  

    ########## Plan ##############

        col_name = list(Plan.columns)
       # print(len(col_name))
        count = len(col_name) - 1

        for i in range(28, count ,1):
            try:
                col_name[i]  = pd.to_datetime(col_name[i] , format='mixed').strftime('%b-%y')
            except: 
                try:
                    col_name[i]  = pd.to_datetime(col_name[i] , format='%y-%b').strftime('%b-%y')
                except:
                    try:
                        col_name[i]  = pd.to_datetime(col_name[i] , format='%Y-%b').strftime('%b-%y')
                    except:
                        try:
                            output_date = reformat_date_Plan(col_name[i] )
                        except:
                            abz="DUmmy"
        Plan.columns = col_name    
        
        
        
        
        OCP = pd.concat([OCP,Offline,Ex,Inv,Plan],ignore_index=True)
        
        OCP = OCP.filter(regex='^(?!.*MCC$)')
        
        OCP = OCP.filter(regex='^(?!.*PID$)')
        OCP = OCP.filter(regex='^(?!.*Unnamed: 55$)')
        
        OCP['Concat']=OCP['Parent Model']+OCP['Country Code']+OCP['Key Account']

        #print(OCP.columns)
        
        ################################################### ASP/MCC update ##############################
        #print('ASP')
        
        OCP.rename(columns={'ASP': 'ASP org'}, inplace=True)
        
        ASP1 = pd.read_excel(r'E:\_Projects\IND POR\asp_template\ASP master.xlsx', sheet_name = 'L1', na_values=[''],keep_default_na=False)
        ASP2 = pd.read_excel(r'E:\_Projects\IND POR\asp_template\ASP master.xlsx', sheet_name = 'L2', na_values=[''],keep_default_na=False)
        ASP3 = pd.read_excel(r'E:\_Projects\IND POR\asp_template\ASP master.xlsx', sheet_name = 'L3', na_values=[''],keep_default_na=False)
        ASP4 = pd.read_excel(r'E:\_Projects\IND POR\asp_template\ASP master.xlsx', sheet_name = 'L4', na_values=[''],keep_default_na=False)
        ASP5 = pd.read_excel(r'E:\_Projects\IND POR\asp_template\ASP master.xlsx', sheet_name = 'Oride', na_values=[''],keep_default_na=False)
        
        ASP1.columns = ASP1.columns.str.strip()
        ASP2.columns = ASP2.columns.str.strip()
        ASP3.columns = ASP3.columns.str.strip()
        ASP4.columns = ASP4.columns.str.strip()
        ASP5.columns = ASP5.columns.str.strip()
        
        OCP['Concat1']=OCP['Class'].astype(str)+OCP['Country Code']+OCP['Key Account'].astype(str).str.upper()
        OCP['Concat2']=OCP['Class'].astype(str)+OCP['Country Code']
        OCP['Concat3']=OCP['Class'].astype(str)+OCP['Region'].astype(str).str.upper()
        OCP['Concat4']=OCP['Class'].astype(str)
        OCP['Concat5']=OCP['Class'].astype(str)+OCP['Key Account'].astype(str).str.upper()+OCP['Country Code']
        
        ASP1['Concat1'] = ASP1['Class'].astype(str)+ASP1['Country Code']+ASP1['Key Account'].astype(str).str.upper()
        ASP2['Concat2'] = ASP2['Class'].astype(str)+ASP2['Country Code']
        ASP3['Concat3'] = ASP3['Class'].astype(str)+ASP3['Region'].astype(str).str.upper()
        ASP4['Concat4'] = ASP4['Class'].astype(str)
        ASP5['Concat5'] = ASP5['Class'].astype(str)+ASP5['Key Account'].astype(str).str.upper()+ASP5['Country Code']
        
        ASP1 = ASP1.rename(columns=lambda x: x.replace('ASP', 'ASP_1'))
        ASP2 = ASP2.rename(columns=lambda x: x.replace('ASP', 'ASP_2'))
        ASP3 = ASP3.rename(columns=lambda x: x.replace('ASP', 'ASP_3'))
        ASP4 = ASP4.rename(columns=lambda x: x.replace('ASP', 'ASP_4'))
        ASP5 = ASP5.rename(columns=lambda x: x.replace('ASP', 'ASP_5'))
        
        ASP1 = ASP1[['Concat1','ASP_1']]
        ASP2 = ASP2[['Concat2','ASP_2']]
        ASP3 = ASP3[['Concat3','ASP_3']]
        ASP4 = ASP4[['Concat4','ASP_4']]
        ASP5= ASP5[['Concat5','ASP_5']]
        
        ASP1.drop_duplicates(inplace=True,subset=['Concat1'],keep='first')
        ASP2.drop_duplicates(inplace=True,subset=['Concat2'],keep='first')
        ASP3.drop_duplicates(inplace=True,subset=['Concat3'],keep='first')
        ASP4.drop_duplicates(inplace=True,subset=['Concat4'],keep='first')
        ASP5.drop_duplicates(inplace=True,subset=['Concat5'],keep='first')
        
        OCP = pd.merge(OCP, ASP1, on='Concat1',how='left')
        OCP = pd.merge(OCP, ASP2, on='Concat2',how='left')
        OCP = pd.merge(OCP, ASP3, on='Concat3',how='left')
        OCP = pd.merge(OCP, ASP4, on='Concat4',how='left')
        OCP = pd.merge(OCP, ASP5, on='Concat5',how='left')
       
        OCP['ASP'] = OCP.apply(lambda row: row['ASP_5'] if pd.notnull(row['ASP_5']) 
                               else (row['ASP_1'] if pd.notnull(row['ASP_1']) 
                                     else (row['ASP_2'] if pd.notnull(row['ASP_2']) 
                                           else (row['ASP_3'] if pd.notnull(row['ASP_3']) 
                                                 else row['ASP_4']))), axis=1)
        
        
        # Update ASP_org to ASP where Data Series is Plan
        OCP.loc[OCP['Data Series'] == 'Plan', 'ASP'] = OCP['ASP org']

        OCP.to_csv(r'E:\_Projects\IND POR\Output\ASP update.csv',index=False)
        
        # Remove columns containing 'ASP_'
        OCP = OCP.loc[:, ~OCP.columns.str.contains('^ASP_')]
        OCP = OCP.loc[:, ~OCP.columns.str.contains('^Concat')]
        
        OCP = OCP.filter(regex='^(?!.*Concat$)')
        OCP = OCP.filter(regex='^(?!.*ASP_$)')
        OCP = OCP.filter(regex='^(?!.*Override$)')
        
        # Relocate the 'ASP-Final' column after the 'ASP' column
        cols = OCP.columns.tolist()
        cols.insert(cols.index('ASP org') + 1, cols.pop(cols.index('ASP')))
        OCP = OCP[cols]
        
        # Remove columns containing 'ASP_'
        OCP = OCP.loc[:, ~OCP.columns.str.contains('^ASP org')]
        
        # Remove columns containing CARDTRONICS and ATMaas
        OCP.loc[OCP['Key Account'].str.contains('ATMaas', case=False, na=False), 'ASP'] = None
        OCP.loc[OCP['Key Account'].str.contains('Cardtronics', case=False, na=False), 'ASP'] = None
        
        #datatype conversion
        OCP['ASP'] = OCP['ASP'].replace('', '0').astype(float).fillna(0).astype(int)
    
        OCP.loc[(OCP['Country Code'].isin(['GG','IE','UK'])) | (OCP['Key Account'].str.contains('ROYAL BANK OF SCOTLAND')),['Country Code','Country Name']] = 'GB','Great Britain'
        
        OCP.loc[(OCP['Key Account'].isin(['OTHER-GG','OTHER-IE'])),'Key Account'] = "OTHER-GB"
        
        OCP.loc[OCP['Country Code']=='GB','Country Name']='Great Britain'
    
    
        OCP.to_csv(r'E:\_Projects\IND POR\Output\OCP.csv',index=False)
        
        ################################################# ASP Demand Value #####################
        
        ############################ Demand with ASP ####################
        ASP = OCP
        for col in ASP.columns[27:-1]:
            ASP[col] = ASP[col] * ASP['ASP']
            #column_name = col
            #print(f"The column name at index is: {column_name}")
            
        ############################ 6M Demand ###########################
        
        
        import datetime
        
        ASP = ASP.iloc[:,27:-1]
        
        # Get the current month and year
        current_month_year = datetime.datetime.now().strftime('%b-%y')
        
        # Initialize an empty list to store matching columns
        matching_columns = []
        
        # Iterate through the next 6 months and check if they exist in the DataFrame
        for i in range(0, 6):
           current_date = datetime.datetime.now() + datetime.timedelta(days=30*i)
           next_month_year = current_date.strftime('%b-%y')
           #print(next_month_year)
           
        
           # Check if the column exists in the DataFrame before adding it to matching_columns
           if next_month_year in ASP.columns:
               matching_columns.append(next_month_year)
               #column_name = next_month_year
               #print(f"The column name at index is: {column_name}")
               
               
        # Filter the DataFrame to only include the matching columns
        filtered_df = ASP[matching_columns]
        
        # Calculate the sum of values in the selected columns and assign it to the "6M POR" column
        ASP['6M POR'] = filtered_df.sum(axis=1)
        
        #OCP.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\POR.csv',index=False)
        
        ASP = ASP.filter(regex='^(?!.*Unnamed: 55$)')
        
        
        ################################ Quater Year ##############################
        QY = ASP.iloc[:,:-1]
        
        # Convert column names to datetime and group by quarter
        QY.columns = pd.to_datetime(QY.columns, format='%b-%y')
        
        quarters = QY.groupby(pd.PeriodIndex(QY.columns, freq='Q'), axis=1).sum()
        
        # Rename the columns to match your desired format
        quarters.columns = quarters.columns.strftime('Q%q-%Y')
        
        # Concatenate the original DataFrame and the quarters DataFrame
        ASP = pd.concat([ASP, quarters], axis=1)
        
        
        ######################### Total ###########################3
        
        years = set(col.split('-')[-1] for col in ASP.columns if 'Q' in col)
        
        # Calculate the total for each year and add new columns
        for year in years:
            year_columns = [col for col in ASP.columns if f'-{year}' in col]
            ASP[f'Total {year}'] = ASP[year_columns].sum(axis=1)
        
        print('ASP Completed')
        
        ################### Adding $ Symbole ######################
        
        
        ASP.columns = [col + ' $' for col in  ASP.columns]
        
        
        #ASP.to_csv(r'E:\_Projects\IND POR\Output\ASP POR.csv',index=False)
        
        ########################## Concatation ###########################
        
        OCP = pd.read_csv(r'E:\_Projects\IND POR\Output\OCP.csv', na_values=[''],keep_default_na=False)
        
        POR = pd.concat([OCP,ASP],axis=1)
        
        ########################## Separation ########################
        POR['Class'] = POR['Class'].astype(str)
        Separation['Class'] = Separation['Class'].astype(str)
        
        POR = pd.merge(POR, Separation,on='Class',how='left')
        
        POR = POR[POR['Product Separation'] != ('Commerce')]
        
        POR = POR.filter(regex='^(?!.*Product Separation$)')
        
        ######################################## Key Account Change from CARDTRONICS ###############
        
        
        #POR.loc[POR['Key Account'].astype(str).str.startswith('CARDTRONICS'),'LOB Region']='Cardtronics'
        
        ################################### IQ country code ###########################################
        
        

        
        ############################# LOB Region / LOB Area remapping #####################################
        
        
        ############## Mering ######################
        POR['CONCAT']=POR['Country Code']+POR['Key Account']
        
        # Merge the dataframes using the filtered LOB dataframe
        merged = pd.merge(POR, LOB_CC_KA, on='CONCAT', how='left')
        
        POR = pd.merge(merged, LOB_CC,left_on='Country Code', right_on='COUNTRY_CODE', how='left')
        
        #print(POR.columns)
        
        ################### Assign value to LOB Region and LOB Area
        POR.loc[POR['LOB REGION_1'].notnull(),'LOB Region'] = POR['LOB REGION_1'] 
        POR.loc[POR['LOB AREA_1'].notnull(),'LOB Area'] = POR['LOB AREA_1'] 
        
        POR.loc[((POR['LOB REGION_1'].isnull()) & (POR['LOB REGION_2'].notnull())),'LOB Region'] = POR['LOB REGION_2'] 
        POR.loc[((POR['LOB AREA_1'].isnull()) & (POR['LOB AREA_2'].notnull())),'LOB Area'] = POR['LOB AREA_2'] 
        
        #POR.to_csv(r"C:\Vasanthan\Python\NCR\IND POR\Output\ATLEOS OCP IND POR "+Date+"1.csv",index=False)
        
        ########### Delete Columns
        
        POR = POR.filter(regex='^(?!.*CONCAT$)')
        POR = POR.filter(regex='^(?!.*_2$)')
        POR = POR.filter(regex='^(?!.*_1$)')
        
        ############ Key Account 
        POR.loc[(POR['Country Code']=='US') & (POR['Key Account']=='CFI-CENTRAL'),'LOB AREA']='UNITED STATES'
        POR.loc[(POR['Country Code']=='US') & (POR['Key Account']=='CFI-CENTRAL'),'LOB REGION']='NAMER-CFI'

        
        
        ################## Theater Mapping #######################################################
        
        POR['LOB Region'] = POR['LOB Region'].fillna('Remove')
        
        POR = pd.merge(POR, Theater,on='LOB Region',how='left')
        
        POR.loc[POR['LOB Region']=='Remove','LOB Region']=''
        
        POR.loc[POR['LOB Region'].str.contains('NAMER'),'Theatre']='NAMER'
        
        ########################## Demand Stream ######################
        
        POR.loc[POR['Key Account'].str.upper().str[:10].isin(['CARDTRONIC']),'Demand Stream'] = 'ATMaaS_P&N'
        
        #POR.loc[POR['Key Account'].str.upper().str[:6].isin(['ATMAAS']),'Demand Stream'] = 'ATMaaS'
        
        POR.loc[POR['Key Account'].str.contains('ATMAAS',na=False,case=False),'Demand Stream'] = 'ATMaaS'
        
        POR['Demand Stream'] = POR['Demand Stream'].fillna('1-Time Rev.')
        
        POR.loc[POR['Demand Stream'].str.upper().str[:6].isin(['NAN']),'Demand Stream'] = '1-Time Rev.'
        
        
        #################### Move Theater columns######################
        
        
        POR.insert(0,'Demand Stream',POR.pop('Demand Stream'))
        
        POR.insert(2,'LOB Region',POR.pop('LOB Region'))
        
        POR.insert(3,'LOB Area',POR.pop('LOB Area'))
        
        POR.insert(3,'Theatre',POR.pop('Theatre'))
        
        POR.insert(2,'Area',POR.pop('Area'))
        
        
        POR = POR.iloc[:,:-3]
        
        #print('Line number 619 is runned')
        
        #Remove Plan from current file
        POR = POR[~POR['Customer LOB'].str.contains('T&T', na=False)]
        POR = POR[~POR['Customer LOB'].str.contains('OTHER', na=False)]
        POR = POR[~POR['Customer LOB'].str.contains('HOSPITALITY', na=False)]
        POR = POR[~POR['Customer LOB'].str.contains('RETAIL', na=False)]

        #print('Line number 625 is runned')

        ####### Other Financial
        Master = pd.read_excel(Input_Location+'Mapping.xlsx', sheet_name='Other_Finance')
        #print('Line number 629 is runned')
        POR = pd.merge(POR, Master,left_on='Parent Model',right_on='Other_Financial',how='left')
        #print('Line number 631 is runned')
        POR.loc[POR['PR_Update'].notnull(),'Product Range'] = POR['PR_Update']
        #print('Line number 633 is runned')
        
        POR = POR.filter(regex='^(?!.*PR_Update$)')
        POR = POR.filter(regex='^(?!.*Other_Financial$)')
        
        ##### DROP Duplciate CC & Area
        Master = pd.read_excel(Input_Location+'Mapping.xlsx', sheet_name='Duplicate_CC')
        
        POR = pd.merge(POR, Master,left_on=['Country Code','Area'],right_on=['Dup_CC','Dup_Area'],how='left')
        
        POR = POR[POR['Dup_Area'].isnull()]        
                
        POR = POR.filter(regex='^(?!.*Dup_CC$)')
        POR = POR.filter(regex='^(?!.*Dup_Area$)')
        
        
        
        #print(POR.columns)
        
        POR.to_csv(r"E:\_Projects\IND POR\Output\ATLEOS OCP IND POR "+Date+".csv",index=False)

        
        import tkinter as tk
        from tkinter import messagebox
        def show_success_message():
           messagebox.showinfo("Success", "POR Automation Completed!")
        root = tk.Tk()
        root.withdraw()  # Hide the main window
        # Call the function to display the dialog box
        show_success_message()
        # Keep the application running until the user closes the dialog
        root.mainloop()
    except FileNotFoundError:
        print('File Not Found')
        print('Check file availabe : ',file)


elif User_Input == 2:
    import pandas as pd
    
    ########################################## 
    
    # This script run with original Country Code without any changes 
    
    ##########################################
    
    
    
    #Date = Ex.Date
    Date = input("Enter POR Date (eg:DD-MMM-YYYY) : ")
    #Date = '08-MAY-2024'
    
    file = 'NCR_OCP_IND_POR_'+Date+'.csv'
    Path ="E:\\_Projects\\IND POR\\Input\\"+file
    Server =r'\\wtc1501cifs.prod.local\cdunix\ERP\OCP\SNOP\DEMREPP\rcv\\'+file
    Input_Path = r'E:\_Projects\IND POR\Ex-Inventory\Input\\'
    Input_Location = r'E:\_Projects\IND POR\Input\\'
    
    
    from datetime import datetime
    
    def reformat_date(date_str):
       # Parse the date string
       dt = datetime.strptime(date_str, '%b-%Y')
       # Reformat the date string to the desired format
       formatted_date = dt.strftime('%b-%y')
       # Capitalize the first letter of the month
       return formatted_date.capitalize()
    # Example usage
    
    
    def reformat_date_Plan(date_str):
       # Parse the date string
       dt = datetime.strptime(date_str, '%y-%b')
       # Reformat the date string to the desired format
       formatted_date = dt.strftime('%b-%y')
       # Capitalize the first letter of the month
       return formatted_date.capitalize()
    
    
    
    try:
    #OCP = pd.read_csv(r'C:\Vasanthan\Python\NCR\IND POR\Input\NCR_OCP_IND_POR_25-DEC-2023.csv',na_values=[''],keep_default_na=False,low_memory=False)
        try:
            OCP = pd.read_csv(Path,na_values=[''],keep_default_na=False,low_memory=False)
        except:
            print('Checking IND POR in Server...!')
            OCP = pd.read_csv(Server,na_values=[''],keep_default_na=False,low_memory=False)
            print('POR Pulled from Server...!')
            
        col_name = list(OCP.columns)
        #print(len(col_name))
        count = len(col_name)
    
        for i in range(19, count ,1):
          # print(col_name[i])
           try:
             col_name[i] = reformat_date(col_name[i])
           except: 
             try:
                 col_name[i]  = pd.to_datetime(col_name[i] , format='%y-%b').strftime('%b-%y')
             except:
                 try:
                     col_name[i]  = pd.to_datetime(col_name[i] , format='%Y-%b').strftime('%b-%y')
                 except:
                     try:
                         col_name[i]  = pd.to_datetime(col_name[i] , format='mixed').strftime('%b-%y')
                     except:    
                         #print(output_date) 
                         print(col_name[i])
    
        OCP.columns = col_name
       # print(OCP.columns)
        
        print('*************POR AUTOMAION STARTED**************')
        #print(OCP.columns)
        Theater = pd.read_excel(r'E:\_Projects\IND POR\Input\Mapping.xlsx',sheet_name='Theatre Mapping')
        
        OCP.columns = OCP.columns.str.strip()
        
        
        ############# 
        
        #OCP['Total'] = OCP.iloc[:,19:].sum(axis=1).round(0)
        
        #OCP = OCP[OCP['Total']!=0]
        
        
        OCP = OCP[OCP['CLASS'].astype(str).str.isnumeric()]
        
        
        
        #OCP.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\OCP before lob.csv',index=False)
        
        ############### KEY ACCOUNT OTHER
        
        
        
        #OCP.loc[(OCP['KEY_ACCOUNT'].str[:6].isin(['OTHER-'])),'KEY_ACCOUNT']=('OTHER-'+ OCP['COUNTRY_CODE'])
        mask = OCP['KEY_ACCOUNT'].str[:6].isin(['OTHER-']) & OCP['COUNTRY_CODE'].notna()
        OCP.loc[mask, 'KEY_ACCOUNT'] = 'OTHER-' + OCP['COUNTRY_CODE']
        
        
        
        ######################### Class Separation ###################################
        Separation = pd.read_csv(r'E:\_Projects\IND POR\Input\Product Range Separation.csv', na_values=[''],keep_default_na=False)
        
        OCP.loc[OCP['CLASS'].isin(Separation['Class']),'Product Separation']='Commerce'
        
        OCP = OCP[OCP['Product Separation'] != ('Commerce')]
        
        OCP = OCP.filter(regex='^(?!.*Product Separation$)')
        
        #################################################################################
        values_to_exclude = ['SHIPMENTS FORECAST']
        OCP = OCP[OCP['DATA_SERIES'].isin(values_to_exclude)]
        
        
        ############## Bill to Key account
        
        OCP.loc[OCP['CUSTOMER_LOB'] == "MULTIPLE", 'CUSTOMER_LOB'] = OCP['PRODUCT_LOB']
        
        OCP.loc[OCP['CUSTOMER_LOB'].astype(str).str[:3] == "FIN", 'CUSTOMER_LOB'] = "FINANCIAL"
        OCP.loc[OCP['CUSTOMER_LOB'].astype(str).str[:3] == "HSR", 'CUSTOMER_LOB'] = "HOSPITALITY"
        
        values = ['Cash Dispense ATM','Multi Function ATM','PC Core Upgrade','Conf Upgrade','Branch Kiosk']
        
        OCP.loc[OCP['PRODUCT_RANGE'].isin(values),'CUSTOMER_LOB'] = 'FINANCIAL'
        
        OCP.loc[OCP['PRODUCT_RANGE'].isin(values),'CUSTOMER_LOB'] = 'FINANCIAL'
    
        #OCP.loc[OCP['COUNTRY_CODE']=='IQ','COUNTRY_CODE']=OCP['BILL_TO_COUNTRY']
        
        
        
       # OCP.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\NCR 1.csv',index=False)
        ############# Bill to Key account
        #OCP.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\NCR 1.csv',index=False)
        #OCP['KeyAccount_Billing']=None
        
        
        OCP['KeyAccount_Billing']=OCP['KEY_ACCOUNT']
        OCP.loc[((OCP['KEY_ACCOUNT'].str[:6].isin(['OTHER-'])) & (OCP['BILL_TO_COUNTRY']!='OTHER')),'KeyAccount_Billing']=('OTHER-'+OCP['BILL_TO_COUNTRY'])
        OCP.loc[((OCP['KEY_ACCOUNT'].str[:6].isin(['OTHER-'])) & (OCP['BILL_TO_COUNTRY']=='OTHER')),'KeyAccount_Billing']='OTHER-??'
        
        ################## DTF update
        #OCP.loc[((OCP['ITEM_TYPE']=='Unassigned') & (OCP['DATA_SERIES']=='NEW DEMAND')) & (OCP['DTF'].isnull()),'DTF']=260
        
        
        
        #OCP.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\OCP before lob.csv',index=False)
        
        ###################### KEY ACCOUNT CARDRONICS ASP = 0
        
        OCP.loc[OCP['KEY_ACCOUNT'].str.contains('CARDTRONIC'),'ASP']=0
        OCP.loc[OCP['KEY_ACCOUNT'].str.contains('ATMAAS',na=False,case=False),'ASP']=0
        
        
        ####################### LOB Region/LOB Area Update
        LOB_CC_KA = pd.read_excel(r'E:\_Projects\IND POR\Input\Mapping.xlsx',sheet_name='LOB_CC_KA', na_values=[''],keep_default_na=False)
        LOB_CC = pd.read_excel(r'E:\_Projects\IND POR\Input\Mapping.xlsx',sheet_name='LOB_CC', na_values=[''],keep_default_na=False)
        
        
        
        #OCP.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\OCP before LOB .csv',index=False)
        ################################################### LOB Area / LOB Region ###################$$
        
        LOB_CC_KA = LOB_CC_KA[['CONCAT','LOB REGION_1','LOB AREA_1']]
        
        ############## Mering ######################
        OCP['CONCAT']=OCP['COUNTRY_CODE']+OCP['KEY_ACCOUNT']
        
        # Merge the dataframes using the filtered LOB dataframe
        merged = pd.merge(OCP, LOB_CC_KA, on='CONCAT', how='left')
        OCP = pd.merge(merged, LOB_CC, on='COUNTRY_CODE', how='left')
        
        ################### Assign value to LOB Region and LOB Area
        OCP.loc[OCP['LOB REGION_1'].notnull(),'LOB REGION'] = OCP['LOB REGION_1'] 
        OCP.loc[OCP['LOB AREA_1'].notnull(),'LOB AREA'] = OCP['LOB AREA_1'] 
        
        OCP.loc[((OCP['LOB REGION'].isnull()) & (OCP['LOB REGION_2'].notnull())),'LOB REGION'] = OCP['LOB REGION_2'] 
        OCP.loc[((OCP['LOB AREA'].isnull()) & (OCP['LOB AREA_2'].notnull())),'LOB AREA'] = OCP['LOB AREA_2'] 
        
        
        ########### Delete Columns
        
        OCP = OCP.filter(regex='^(?!.*CONCAT$)')
        OCP = OCP.filter(regex='^(?!.*_2$)')
        OCP = OCP.filter(regex='^(?!.*_1$)')
        
        ######################################################### DTF Upldate ##########################
        
        from pandas.tseries.offsets import MonthEnd
        
        
        
        OCP['Start Date'] = (pd.to_datetime('today') + MonthEnd(0))
        
        OCP.loc[OCP['DTF'].notnull() & (OCP['DTF'] != 0), 'End Date'] = (OCP['Start Date'] + pd.to_timedelta(OCP['DTF'], unit='D')).dt.strftime('%m/%d/%Y')
        
        OCP.loc[(OCP['DTF'].isnull() | (OCP['DTF']==0)),'End Date'] = (pd.to_datetime('today') + MonthEnd(0)).strftime('%m/%d/%Y')
        
        OCP['Start Date'] = (pd.to_datetime('today') + MonthEnd(0)).strftime('%m/%d/%Y')
        
        OCP['DTF'] = OCP['DTF'].fillna(0)
        OCP['DTF cutoff']=pd.Timestamp.now() + OCP['DTF'].astype('timedelta64[D]')
        OCP['DTF cutoff']=OCP['DTF cutoff'].dt.strftime('%m-%d-%y')
        
        
        
        # Assuming 'df' is the DataFrame containing the 'End Date' column
        OCP['End_Date'] = pd.to_datetime(OCP['End Date'], format='%m/%d/%Y').dt.strftime('%b-%y')
        OCP['Start_Date'] = pd.to_datetime(OCP['Start Date'], format='%m/%d/%Y').dt.strftime('%b-%y')
        
        
        
        #OCP.to_csv(r'E:\_Projects\IND POR\Output\NCR 1.csv',index=False)
        
        
        #print(OCP.columns)
        ############################ DTF Risk Unit ###################################
        #print(OCP['End_Date'])
        # Define a function to get the index of a column
        def get_column_index(value):
            try:
                return OCP.columns.get_loc(value)
            except KeyError:
                return None
        
        # Apply the function to find the index of 'End Date' column
        OCP['index'] = OCP['End_Date'].apply(get_column_index) + 1
        
        OCP['S_index'] = OCP['Start_Date'].apply(get_column_index) - 1
        
        OCP.loc[OCP['DATA_SERIES']=='SHIP NOT INV','S_index'] = OCP['S_index']+1
        
       # OCP.to_csv(r'E:\_Projects\IND POR\Output\NCR 2.csv',index=False)
    
        
        # Create a new column 'DTF Risk' with the sum of values from index 19 to the 'index' column value
        #OCP['DTF risk units'] = OCP.apply(lambda row: row.iloc[19:int(row['index'])].sum(), axis=1)
        OCP['DTF risk units'] = OCP.apply(lambda row: row.iloc[int(row['S_index']):int(row['index'])].sum(), axis=1)
        
        
        OCP = OCP.filter(regex='^(?!.*End_Date$)')
        OCP = OCP.filter(regex='^(?!.*Start_Date$)')
        
        OCP.loc[OCP['DATA_SERIES']=='SHIPMENTS HISTORY','DTF risk units']=0
        
        OCP.to_csv(r'E:\_Projects\IND POR\Output\NCR Dft risk.csv',index=False)
        
        ##################################### DTF Risk ########################################
        
        
        OCP['DTF risk'] = OCP['DTF risk units'].apply(lambda x: 'Yes' if x > 0 else 'No')
        
        
        #################################### Remove unwanted Data #######################################
        OCP=OCP[OCP['DATA_SERIES'].isin(['SHIPMENTS FORECAST'])]
        
        
        OCP['Comments'] = '1_OCP Data Extract'
        
        
        
        Header={"REGION":"Region","AREA":"Area","LOB REGION":"LOB Region","LOB AREA":"LOB Area","COUNTRY":"Country Name","COUNTRY_CODE":"Country Code","BILL_TO_COUNTRY":"Bill to Country","ZONE":"Zone","CUSTOMER_LOB":"Customer LOB","PRODUCT_LOB":"Product LOB","PRODUCT_RANGE":"Product Range","PARENT_MODEL":"Parent Model","CLASS":"Class","DESCRIPTION":"Description","ORGANIZATION_CODE":"Organization Code","KEY_ACCOUNT":"Key Account","MASTER_CUSTOMER":"Master Customer","ITEM_TYPE":"Item Type","DATA_SERIES":"Data Series"}
        
        OCP = OCP.rename(columns=Header)
        
        #df.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\NCR final.csv', index=False)
        
        columns_to_move = [
            'Region', 'Area', 'LOB Region', 'LOB Area', 'Country Name', 'Country Code',
            'Bill to Country', 'Zone', 'Customer LOB', 'Product LOB', 'Product Range',
            'Parent Model', 'Class', 'Description', 'DTF', 'DTF cutoff', 'Start Date',
            'End Date', 'DTF risk units', 'DTF risk', 'Organization Code', 'KeyAccount_Billing',
            'Key Account', 'Master Customer', 'Item Type', 'ASP','Data Series'
        ]
        
        
        #df = df[columns_to_move + [col for col in df.columns if col not in columns_to_move]]
        
        remaining_columns = [col for col in OCP.columns if col not in columns_to_move]
        
        OCP = OCP[columns_to_move + remaining_columns]
        
        
        OCP = OCP.filter(regex='^(?!.*index$)')
        OCP = OCP.filter(regex='^(?!.*Total$)')
        
        
     
        OCP = OCP.filter(regex='^(?!.*MCC$)')
        
        OCP = OCP.filter(regex='^(?!.*PID$)')
        OCP = OCP.filter(regex='^(?!.*Unnamed: 55$)')
        
        OCP['Concat']=OCP['Parent Model']+OCP['Country Code']+OCP['Key Account']
        #print(OCP.columns)
        
        ################################################### ASP/MCC update ##############################
        #print('ASP')
        
        OCP.rename(columns={'ASP': 'ASP org'}, inplace=True)
        
        ASP1 = pd.read_excel(r'E:\_Projects\IND POR\asp_template\ASP master.xlsx', sheet_name = 'L1', na_values=[''],keep_default_na=False)
        ASP2 = pd.read_excel(r'E:\_Projects\IND POR\asp_template\ASP master.xlsx', sheet_name = 'L2', na_values=[''],keep_default_na=False)
        ASP3 = pd.read_excel(r'E:\_Projects\IND POR\asp_template\ASP master.xlsx', sheet_name = 'L3', na_values=[''],keep_default_na=False)
        ASP4 = pd.read_excel(r'E:\_Projects\IND POR\asp_template\ASP master.xlsx', sheet_name = 'L4', na_values=[''],keep_default_na=False)
        ASP5 = pd.read_excel(r'E:\_Projects\IND POR\asp_template\ASP master.xlsx', sheet_name = 'Oride', na_values=[''],keep_default_na=False)
        
        ASP1.columns = ASP1.columns.str.strip()
        ASP2.columns = ASP2.columns.str.strip()
        ASP3.columns = ASP3.columns.str.strip()
        ASP4.columns = ASP4.columns.str.strip()
        ASP5.columns = ASP5.columns.str.strip()
        
        OCP['Concat1']=OCP['Class'].astype(str)+OCP['Country Code']+OCP['Key Account']
        OCP['Concat2']=OCP['Class'].astype(str)+OCP['Country Code']
        OCP['Concat3']=OCP['Class'].astype(str)+OCP['Region']
        OCP['Concat4']=OCP['Class'].astype(str)
        OCP['Concat5']=OCP['Class'].astype(str)+OCP['Key Account']+OCP['Country Code']
        
        ASP1['Concat1'] = ASP1['Class'].astype(str)+ASP1['Country Code']+ASP1['Key Account']
        ASP2['Concat2'] = ASP2['Class'].astype(str)+ASP2['Country Code']
        ASP3['Concat3'] = ASP3['Class'].astype(str)+ASP3['Region']
        ASP4['Concat4'] = ASP4['Class'].astype(str)
        ASP5['Concat5'] = ASP5['Class'].astype(str)+ASP5['Key Account']+ASP5['Country Code']
        
        ASP1 = ASP1.rename(columns=lambda x: x.replace('ASP', 'ASP_1'))
        ASP2 = ASP2.rename(columns=lambda x: x.replace('ASP', 'ASP_2'))
        ASP3 = ASP3.rename(columns=lambda x: x.replace('ASP', 'ASP_3'))
        ASP4 = ASP4.rename(columns=lambda x: x.replace('ASP', 'ASP_4'))
        ASP5 = ASP5.rename(columns=lambda x: x.replace('ASP', 'ASP_5'))
        
        ASP1 = ASP1[['Concat1','ASP_1']]
        ASP2 = ASP2[['Concat2','ASP_2']]
        ASP3 = ASP3[['Concat3','ASP_3']]
        ASP4 = ASP4[['Concat4','ASP_4']]
        ASP5= ASP5[['Concat5','ASP_5']]
        
        ASP1.drop_duplicates(inplace=True,subset=['Concat1'],keep='first')
        ASP2.drop_duplicates(inplace=True,subset=['Concat2'],keep='first')
        ASP3.drop_duplicates(inplace=True,subset=['Concat3'],keep='first')
        ASP4.drop_duplicates(inplace=True,subset=['Concat4'],keep='first')
        ASP5.drop_duplicates(inplace=True,subset=['Concat5'],keep='first')
        
        OCP = pd.merge(OCP, ASP1, on='Concat1',how='left')
        OCP = pd.merge(OCP, ASP2, on='Concat2',how='left')
        OCP = pd.merge(OCP, ASP3, on='Concat3',how='left')
        OCP = pd.merge(OCP, ASP4, on='Concat4',how='left')
        OCP = pd.merge(OCP, ASP5, on='Concat5',how='left')
       
        OCP['ASP'] = OCP.apply(lambda row: row['ASP_5'] if pd.notnull(row['ASP_5']) 
                               else (row['ASP_1'] if pd.notnull(row['ASP_1']) 
                                     else (row['ASP_2'] if pd.notnull(row['ASP_2']) 
                                           else (row['ASP_3'] if pd.notnull(row['ASP_3']) 
                                                 else row['ASP_4']))), axis=1)
        
        
        OCP.to_csv(r'E:\_Projects\IND POR\ASP update_jose.csv',index=False)
        
        # Remove columns containing 'ASP_'
        OCP = OCP.loc[:, ~OCP.columns.str.contains('^ASP_')]
        OCP = OCP.loc[:, ~OCP.columns.str.contains('^Concat')]
        
        OCP = OCP.filter(regex='^(?!.*Concat$)')
        OCP = OCP.filter(regex='^(?!.*ASP_$)')
        OCP = OCP.filter(regex='^(?!.*Override$)')
        
        # Relocate the 'ASP-Final' column after the 'ASP' column
        cols = OCP.columns.tolist()
        cols.insert(cols.index('ASP org') + 1, cols.pop(cols.index('ASP')))
        OCP = OCP[cols]
        
        # Remove columns containing 'ASP_'
        OCP = OCP.loc[:, ~OCP.columns.str.contains('^ASP org')]
        
        # Remove columns containing CARDTRONICS and ATMaas
        OCP.loc[OCP['Key Account'].str.contains('ATMaas', case=False, na=False), 'ASP'] = None
        OCP.loc[OCP['Key Account'].str.contains('Cardtronics', case=False, na=False), 'ASP'] = None
        
        #datatype conversion
        OCP['ASP'] = OCP['ASP'].replace('', '0').astype(float).fillna(0).astype(int)
        
        OCP.loc[(OCP['Country Code'].isin(['GG','IE','UK'])) | (OCP['Key Account'].str.contains('ROYAL BANK OF SCOTLAND')),['Country Code','Country Name']] = 'GB','Great Britain'
        
        OCP.loc[(OCP['Key Account'].isin(['OTHER-GG','OTHER-IE'])),'Key Account'] = "OTHER-GB"
        
        OCP.loc[OCP['Country Code']=='GB','Country Name']='Great Britain'
        
        
        
        OCP.to_csv(r'E:\_Projects\IND POR\Output\OCP.csv',index=False)
        
        
        ################################################# ASP Demand Value #####################
        
        ############################ Demand with ASP ####################
        ASP = OCP
        for col in ASP.columns[27:-1]:
            ASP[col] = ASP[col] * ASP['ASP']
            
        ############################ 6M Demand ###########################
        
        
        import datetime
        
        ASP = ASP.iloc[:,27:-1]
        
        
        # Get the current month and year
        current_month_year = datetime.datetime.now().strftime('%b-%y')
        
        # Initialize an empty list to store matching columns
        matching_columns = []
        
        # Iterate through the next 6 months and check if they exist in the DataFrame
        for i in range(0, 6):
           current_date = datetime.datetime.now() + datetime.timedelta(days=30*i)
           next_month_year = current_date.strftime('%b-%y')
        
           # Check if the column exists in the DataFrame before adding it to matching_columns
           if next_month_year in ASP.columns:
               matching_columns.append(next_month_year)
        
        # Filter the DataFrame to only include the matching columns
        filtered_df = ASP[matching_columns]
        
        # Calculate the sum of values in the selected columns and assign it to the "6M POR" column
        ASP['6M POR'] = filtered_df.sum(axis=1)
        
        #OCP.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\POR.csv',index=False)
        
        ASP = ASP.filter(regex='^(?!.*Unnamed: 55$)')
        
        
        ################################ Quater Year ##############################
        QY = ASP.iloc[:,:-1]
        
        # Convert column names to datetime and group by quarter
        QY.columns = pd.to_datetime(QY.columns, format='%b-%y')
        
        quarters = QY.groupby(pd.PeriodIndex(QY.columns, freq='Q'), axis=1).sum()
        
        # Rename the columns to match your desired format
        quarters.columns = quarters.columns.strftime('Q%q-%Y')
        
        # Concatenate the original DataFrame and the quarters DataFrame
        ASP = pd.concat([ASP, quarters], axis=1)
        
        
        ######################### Total ###########################3
        
        years = set(col.split('-')[-1] for col in ASP.columns if 'Q' in col)
        
        # Calculate the total for each year and add new columns
        for year in years:
            year_columns = [col for col in ASP.columns if f'-{year}' in col]
            ASP[f'Total {year}'] = ASP[year_columns].sum(axis=1)
        
        print('ASP Completed')
        
        ################### Adding $ Symbole ######################
        
        
        ASP.columns = [col + ' $' for col in  ASP.columns]
        
        
        #ASP.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\ASP POR.csv',index=False)
        
        ########################## Concatation ###########################
        
        OCP = pd.read_csv(r'E:\_Projects\IND POR\Output\OCP.csv', na_values=[''],keep_default_na=False)
        
        POR = pd.concat([OCP,ASP],axis=1)
        
        ########################## Separation ########################
        POR['Class'] = POR['Class'].astype(str)
        Separation['Class'] = Separation['Class'].astype(str)
        
        POR = pd.merge(POR, Separation,on='Class',how='left')
        
        POR = POR[POR['Product Separation'] != ('Commerce')]
        
        POR = POR.filter(regex='^(?!.*Product Separation$)')
        
        ######################################## Key Account Change from CARDTRONICS ###############
        
        
        #POR.loc[POR['Key Account'].astype(str).str.startswith('CARDTRONICS'),'LOB Region']='Cardtronics'
        
        ################################### IQ country code ###########################################
        
        
    
        
        ############################# LOB Region / LOB Area remapping #####################################
        
        
        ############## Mering ######################
        POR['CONCAT']=POR['Country Code']+POR['Key Account']
        
        # Merge the dataframes using the filtered LOB dataframe
        merged = pd.merge(POR, LOB_CC_KA, on='CONCAT', how='left')
        
        POR = pd.merge(merged, LOB_CC,left_on='Country Code', right_on='COUNTRY_CODE', how='left')
        
        #print(POR.columns)
        
        ################### Assign value to LOB Region and LOB Area
        POR.loc[POR['LOB REGION_1'].notnull(),'LOB Region'] = POR['LOB REGION_1'] 
        POR.loc[POR['LOB AREA_1'].notnull(),'LOB Area'] = POR['LOB AREA_1'] 
        
        POR.loc[((POR['LOB REGION_1'].isnull()) & (POR['LOB REGION_2'].notnull())),'LOB Region'] = POR['LOB REGION_2'] 
        POR.loc[((POR['LOB AREA_1'].isnull()) & (POR['LOB AREA_2'].notnull())),'LOB Area'] = POR['LOB AREA_2'] 
        
        #POR.to_csv(r"C:\Vasanthan\Python\NCR\IND POR\Output\ATLEOS OCP IND POR "+Date+"1.csv",index=False)
        
        ########### Delete Columns
        
        POR = POR.filter(regex='^(?!.*CONCAT$)')
        POR = POR.filter(regex='^(?!.*_2$)')
        POR = POR.filter(regex='^(?!.*_1$)')
        
        ############ Key Account 
        POR.loc[(POR['Country Code']=='US') & (POR['Key Account']=='CFI-CENTRAL'),'LOB AREA']='UNITED STATES'
        POR.loc[(POR['Country Code']=='US') & (POR['Key Account']=='CFI-CENTRAL'),'LOB REGION']='NAMER-CFI'
    
        
        
        ################## Theater Mapping #######################################################
        
        POR['LOB Region'] = POR['LOB Region'].fillna('Remove')
        
        POR = pd.merge(POR, Theater,on='LOB Region',how='left')
        
        POR.loc[POR['LOB Region']=='Remove','LOB Region']=''
        
        POR.loc[POR['LOB Region'].str.contains('NAMER'),'Theatre']='NAMER'
        
        ########################## Demand Stream ######################
        
        POR.loc[POR['Key Account'].str.upper().str[:10].isin(['CARDTRONIC']),'Demand Stream'] = 'ATMaaS_P&N'
        
        #POR.loc[POR['Key Account'].str.upper().str[:6].isin(['ATMAAS']),'Demand Stream'] = 'ATMaaS'
        
        POR.loc[POR['Key Account'].str.contains('ATMAAS',na=False,case=False),'Demand Stream'] = 'ATMaaS'
        
        POR['Demand Stream'] = POR['Demand Stream'].fillna('1-Time Rev.')
        
        POR.loc[POR['Demand Stream'].str.upper().str[:6].isin(['NAN']),'Demand Stream'] = '1-Time Rev.'
        
        
        #################### Move Theater columns######################
        
        
        POR.insert(0,'Demand Stream',POR.pop('Demand Stream'))
        
        POR.insert(2,'LOB Region',POR.pop('LOB Region'))
        
        POR.insert(3,'LOB Area',POR.pop('LOB Area'))
        
        POR.insert(3,'Theatre',POR.pop('Theatre'))
        
        POR.insert(2,'Area',POR.pop('Area'))
        
        
        POR = POR.iloc[:,:-3]
        
        #print('Line number 619 is runned')
        
        #Remove Plan from current file
        POR = POR[~POR['Customer LOB'].str.contains('T&T', na=False)]
        POR = POR[~POR['Customer LOB'].str.contains('OTHER', na=False)]
        POR = POR[~POR['Customer LOB'].str.contains('HOSPITALITY', na=False)]
        POR = POR[~POR['Customer LOB'].str.contains('RETAIL', na=False)]
    
        #print('Line number 625 is runned')
    
        ####### Other Financial
        Master = pd.read_excel(Input_Location+'Mapping.xlsx', sheet_name='Other_Finance')
        #print('Line number 629 is runned')
        POR = pd.merge(POR, Master,left_on='Parent Model',right_on='Other_Financial',how='left')
        #print('Line number 631 is runned')
        POR.loc[POR['PR_Update'].notnull(),'Product Range'] = POR['PR_Update']
        #print('Line number 633 is runned')
        
        POR = POR.filter(regex='^(?!.*PR_Update$)')
        POR = POR.filter(regex='^(?!.*Other_Financial$)')
        
        ##### DROP Duplciate CC & Area
        Master = pd.read_excel(Input_Location+'Mapping.xlsx', sheet_name='Duplicate_CC')
        
        POR = pd.merge(POR, Master,left_on=['Country Code','Area'],right_on=['Dup_CC','Dup_Area'],how='left')
        
        POR = POR[POR['Dup_Area'].isnull()]        
                
        POR = POR.filter(regex='^(?!.*Dup_CC$)')
        POR = POR.filter(regex='^(?!.*Dup_Area$)')
        
        
        
        #print(POR.columns)
        
        POR.to_csv(r"E:\_Projects\IND POR\Output\ATLEOS OCP STAT POR "+Date+".csv",index=False)
    
        
        import tkinter as tk
        from tkinter import messagebox
        def show_success_message():
           messagebox.showinfo("Success", "POR Automation Completed!")
        root = tk.Tk()
        root.withdraw()  # Hide the main window
        # Call the function to display the dialog box
        show_success_message()
        # Keep the application running until the user closes the dialog
        root.mainloop()
    except FileNotFoundError:
        print('File Not Found')
        print('Check file availabe : ',file)


elif User_Input == 3:
    import os
    import pandas as pd

    # Directory path where you want to search for files
    POR_Path = r'E:\_Projects\IND POR\Input\\'

    Input_Path = r'E:\_Projects\IND POR\Ex-Inventory\Input\\'
    Output_Path = r'E:\_Projects\IND POR\Ex-Inventory\Output\\'
    Server = r'\\wtc1501cifs.prod.local\cdunix\ERP\OCP\SNOP\DEMREPP\rcv\\'


    # Keywords to search for in file names
    keywords_ex = ['workbench_ex']
    keywords_inv = ['workbench_inv']


    ## POR Date

    POR_Date = input("Enter POR Date (eg:DD-MMM-YYYY) : ")

    # Get a list of files containing the specified keywords
    matching_files_ex = [filename for filename in os.listdir(Input_Path) if any(keyword in filename.lower() for keyword in keywords_ex)]
    matching_files_inv = [filename for filename in os.listdir(Input_Path) if any(keyword in filename.lower() for keyword in keywords_inv)]


    file_path_ex = os.path.join(Input_Path, matching_files_ex[0])
    file_path_inv = os.path.join(Input_Path, matching_files_inv[0])

    Ex = pd.read_csv(file_path_ex, low_memory=False)
    Inv = pd.read_csv(file_path_inv, low_memory=False)

    #Date = input('Enter POR Date : ')
    try:
        NCR_OCP = pd.read_csv(POR_Path+'NCR_OCP_IND_POR_'+POR_Date+'.csv', low_memory=False)
    except:
        print('IND POR Checking in  Server...!')
        NCR_OCP = pd.read_csv(Server+'NCR_OCP_IND_POR_'+POR_Date+'.csv', low_memory=False)
        print('***** POR Loaded Successfully *****')
        
        
    col_name = list(NCR_OCP.columns)
    #print(len(col_name))
    count = len(col_name)

    from datetime import datetime

    def reformat_date(date_str):
       # Parse the date string
       dt = datetime.strptime(date_str, '%b-%Y')
       # Reformat the date string to the desired format
       formatted_date = dt.strftime('%b-%y')
       # Capitalize the first letter of the month
       return formatted_date.capitalize()
    # Example usage


    for i in range(19, count ,1):
      # print(col_name[i])
       try:
         col_name[i]  = pd.to_datetime(col_name[i] , format='mixed').strftime('%b-%y')
       except: 
             try:
                 col_name[i]  = pd.to_datetime(col_name[i] , format='%y-%b').strftime('%b-%y')
             except:
                 try:
                     col_name[i]  = pd.to_datetime(col_name[i] , format='%Y-%b').strftime('%b-%y')
                 except:
                     col_name[i] = reformat_date(col_name[i])
                     
    NCR_OCP.columns = col_name


    # Now 'Ex' contains the data from the matching file
    print("DataFrame loaded successfully.")
    print('-----------Ex-Inventory Automation Started-----------')
    #Separation = pd.read_csv(r'C:\Vasanthan\Python\NCR\IND POR\Input\Product Range Separation.csv')
    Master = pd.read_excel(Input_Path+'Master file.xlsx', sheet_name='Region')



    ############################## Keep Only Class without Alphabet #########################

    # Use regular expressions to filter the 'goo' column
    Ex = Ex[Ex['Class'].str.contains(r'^[0-9]*$', regex=True)]

    Inv = Inv[Inv['Class'].str.contains(r'^[0-9]*$', regex=True)]



    ####################################################################################
    # Function to rename columns dynamically
    def rename_columns(Ex):
        for col in Ex.columns[14:]:
            year_month = col
            year = year_month[:4]
            month = year_month[4:]
            month_name = pd.to_datetime(month, format='%m').strftime('%b')
            new_col_name = f'{month_name}-{year[2:]}'
            Ex.rename(columns={col: new_col_name}, inplace=True)

    # Rename the columns
    rename_columns(Ex)


    # Function to rename columns dynamically
    def rename_columns(Inv):
        for col in Inv.columns[14:]:
            year_month = col
            year = year_month[:4]
            month = year_month[4:]
            month_name = pd.to_datetime(month, format='%m').strftime('%b')
            new_col_name = f'{month_name}-{year[2:]}'
            Inv.rename(columns={col: new_col_name}, inplace=True)

    # Rename the columns
    rename_columns(Inv)


    ########################### Commend ##########################################
    Ex['Comments'] = '3_EX'
    Inv['Comments'] = '4_Inventory'

    ############################# Concat #########################################

    Ex_Inv = pd.concat([Ex,Inv],ignore_index=True)

    comments_column = Ex_Inv.pop('Comments')

    # Insert the 'comments' column at index 15
    Ex_Inv.insert(14, 'Comments', comments_column)

    #print(Ex_Inv.columns)


    ####################################### Product Separation ##################################


    Separation = pd.read_csv(POR_Path+'Product Range Separation.csv', na_values=[''],keep_default_na=False)
        
    Ex_Inv.loc[Ex_Inv['Class'].isin(Separation['Class']),'Product Separation']='Commerce'
        
    Ex_Inv = Ex_Inv[Ex_Inv['Product Separation'] != ('Commerce')]
        
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*Product Separation$)')
      

    ####################################################### POR ###########################

    # Extract the columns you need
    OCP = NCR_OCP.iloc[:, 19:]


    OCP =='Comments'

    # Extract the column names as a DataFrame
    OCP_columns = OCP.columns


    column  = Ex_Inv.columns[:15].tolist() + [col for col in Ex_Inv.columns[15:-1] if col in OCP_columns]

    Ex_Inv = Ex_Inv[column]


    ########################## Move Commend column to Last ###############################

    if 'Comments' in Ex_Inv.columns:
        col = Ex_Inv.pop('Comments')
        Ex_Inv['Comments']=col

    ##########################################################################################


    ######################## 3)	Filter Item type as “Model” and rename as “Unassigned” and 4 dash PID’s to “Unit”

    Ex_Inv.loc[Ex_Inv['PID Type'].astype(str).str.contains('M'),'Item Type']='Unassigned'
    Ex_Inv.loc[Ex_Inv['PID Type'].astype(str).str.contains('U'),'Item Type']='UNIT'


    ######################### 4)	Filter “HU” in Ctry Cd column and replace as “XH”
    Ex_Inv = Ex_Inv.rename(columns=lambda x: x.replace('Org Code', 'Country Code'))

    Ex_Inv.loc[Ex_Inv['Country Code'].astype(str).str.contains('HU'),'Country Code']='XH'



    ############################ Key account Other ################################

    Ex_Inv.loc[Ex_Inv['Key Account']==('OTHER'),'Key Account']="OTHER-"+Ex_Inv['Country Code']




    ################################ Item Type ########################################
    Ex_Inv.loc[Ex_Inv['PID Type']=='M','Item Type']='Unassigned'
    Ex_Inv.loc[Ex_Inv['PID Type']=='U','Item Type']='Unit'


    #Ex_Inv.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\Ex-Inv.csv',index=False)
    ################################ Filter Unit and Unassgined #####################

    OCP = NCR_OCP

    OCP.columns = OCP.columns.str.strip()

    OCP['ITEM_TYPE']= OCP['ITEM_TYPE'].str.upper()

    OCP = OCP[OCP['ITEM_TYPE'].astype(str).isin(['UNASSIGNED','UNIT'])]


    OCP.to_csv(Output_Path+'OCP.csv',index=False)

    #############################################################################################################################################################

    ############################################################# MASTER FILE UPDATION ##########################################################################

    #############################################################################################################################################################


    ######################### Update Region/Area/Country Name ################################

    #OCP = pd.read_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\OCP.csv', low_memory=False)

    Ex_Inv['Country Code'] = Ex_Inv['Country Code'].astype(str)
    OCP['COUNTRY_CODE'] = OCP['COUNTRY_CODE'].astype(str)



    OCP = OCP[['COUNTRY_CODE','REGION','AREA','COUNTRY']]

    OCP = OCP.drop_duplicates(subset=['COUNTRY_CODE'], keep='first')

    Ex_Inv = pd.merge(Ex_Inv, OCP,left_on='Country Code',right_on='COUNTRY_CODE',how='left')

    Ex_Inv = pd.merge(Ex_Inv, Master,left_on='Country Code',right_on='Country Code_',how='left')


    ###################### Region
    Ex_Inv.loc[(Ex_Inv['COUNTRY_CODE'].fillna(0)) != 0, 'Region' ]= Ex_Inv['REGION']

    Ex_Inv.loc[((Ex_Inv['COUNTRY_CODE'].fillna(0)) == 0)&(Ex_Inv['Region_'].fillna(0)) != 0, 'Region' ]= Ex_Inv['Region_']

    ###################### Area
    Ex_Inv.loc[(Ex_Inv['COUNTRY_CODE'].fillna(0)) != 0, 'Area' ]= Ex_Inv['AREA']

    Ex_Inv.loc[((Ex_Inv['COUNTRY_CODE'].fillna(0)) == 0)&(Ex_Inv['Area_'].fillna(0)) != 0, 'Area' ]= Ex_Inv['Area_']

    ##################### Country Name
    Ex_Inv.loc[(Ex_Inv['COUNTRY_CODE'].fillna(0)) != 0, 'Org Name' ]=  Ex_Inv['COUNTRY']

    Ex_Inv.loc[((Ex_Inv['COUNTRY_CODE'].fillna(0)) == 0)&(Ex_Inv['Country Name_'].fillna(0)) != 0, 'Org Name' ]= Ex_Inv['Country Name_']


    ################### Remove the columns ###############

    Ex_Inv = Ex_Inv.filter(regex='^(?!.*Region_$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*Area_$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*Country Name_$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*Country Code_$)')

    Ex_Inv = Ex_Inv.filter(regex='^(?!.*COUNTRY_CODE$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*REGION$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*AREA$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*COUNTRY$)')


    #Ex_Inv.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\Ex-Inv.csv',index=False)

    ##############################################################################################
    ##############################################################################################

    #################### Product Range / LOB 

    Master = pd.read_excel(Input_Path+'Master file.xlsx', sheet_name='LOB')

    OCP = pd.read_csv(Output_Path+'OCP.csv', low_memory=False)

    OCP = OCP[['PRODUCT_LOB','PRODUCT_RANGE','CLASS']]

    OCP = OCP.drop_duplicates(subset=['CLASS'], keep='first')

    Ex_Inv['Class'] = Ex_Inv['Class'].astype(str)
    OCP['CLASS'] = OCP['CLASS'].astype(str)
    Master['CLASS_LOB_'] = Master['CLASS_LOB_'].astype(str)


    Ex_Inv = pd.merge(Ex_Inv, OCP,left_on='Class',right_on='CLASS',how='left')

    Ex_Inv = pd.merge(Ex_Inv, Master,left_on='Class',right_on='CLASS_LOB_',how='left')

    ###################### Product Range
    Ex_Inv.loc[(Ex_Inv['PRODUCT_RANGE'].fillna(0)) != 0, 'Product Range' ]= Ex_Inv['PRODUCT_RANGE']

    Ex_Inv.loc[((Ex_Inv['PRODUCT_RANGE'].fillna(0)) == 0)&(Ex_Inv['PRODUCT_RANGE_'].fillna(0)) != 0, 'Product Range' ]= Ex_Inv['PRODUCT_RANGE_']

    ###################### Product LOB
    Ex_Inv.loc[(Ex_Inv['PRODUCT_LOB'].fillna(0)) != 0, 'Product LOB' ]= Ex_Inv['PRODUCT_LOB']

    Ex_Inv.loc[((Ex_Inv['PRODUCT_LOB'].fillna(0)) == 0)&(Ex_Inv['PRODUCT_LOB_'].fillna(0)) != 0, 'Product LOB' ]= Ex_Inv['PRODUCT_LOB_']

    Ex_Inv.loc[(Ex_Inv['Product LOB'].fillna(0)) == 0, 'Product LOB' ]= Ex_Inv['LOB']

    Ex_Inv.loc[Ex_Inv['Product LOB'] == 'FIN', 'Product LOB' ]= 'FINANCE'

    Ex_Inv.loc[Ex_Inv['Product LOB'] == 'RET', 'Product LOB' ]= 'RETAIL'


    ################### Remove the columns ###############

    Ex_Inv = Ex_Inv.filter(regex='^(?!.*PRODUCT_RANGE_$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*PRODUCT_LOB_$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*CLASS_LOB_$)')

    Ex_Inv = Ex_Inv.filter(regex='^(?!.*PRODUCT_LOB$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*PRODUCT_RANGE$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*CLASS$)')


    ##############################################################################################
    ##############################################################################################

     
    ########################## Description 
    Master = pd.read_excel(Input_Path+'Master file.xlsx', sheet_name='Model')

    OCP = pd.read_csv(Output_Path+'OCP.csv', low_memory=False)

    OCP_New = OCP

    OCP = OCP[['CLASS','PARENT_MODEL']]

    OCP = OCP.drop_duplicates(subset=['CLASS'], keep='first')


    ###################### Parent Model
    #print(Ex_Inv.columns)

    Ex_Inv = pd.merge(Ex_Inv, OCP,left_on='Class',right_on='CLASS',how='left')
    Ex_Inv = pd.merge(Ex_Inv, Master,left_on='Class',right_on='CLASS_Model_',how='left')


    Ex_Inv.loc[(Ex_Inv['PID Type']== 'U'), 'Parent Model' ]= Ex_Inv['PID']
    Ex_Inv.loc[(Ex_Inv['PID Type']== 'M'), 'Parent Model' ]= Ex_Inv['PARENT_MODEL']
    Ex_Inv.loc[(Ex_Inv['Parent Model'].fillna(0)==0),'Parent Model'] = Ex_Inv['PARENT_MODEL_']
    Ex_Inv.loc[(Ex_Inv['Parent Model'].fillna(0)==0),'Parent Model'] = Ex_Inv['Parent']


    Ex_Inv = Ex_Inv.filter(regex='^(?!.*PARENT_MODEL_$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*PARENT_MODEL$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*CLASS_Model_$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*CLASS$)')


    #print(Ex_Inv)


    ############### Description 

    Master = pd.read_excel(Input_Path+'Master file.xlsx', sheet_name='Description')

    OCP = OCP_New

    OCP = OCP[['PARENT_MODEL','DESCRIPTION']]

    OCP = OCP.drop_duplicates(subset=['PARENT_MODEL'], keep='first')


    Ex_Inv = pd.merge(Ex_Inv, OCP,left_on='Parent Model',right_on='PARENT_MODEL',how='left')
    Ex_Inv = pd.merge(Ex_Inv, Master,left_on='Parent Model',right_on='PARENT_MODEL_',how='left')

    Ex_Inv.loc[Ex_Inv['DESCRIPTION'].fillna(0) != 0, 'Description' ]= Ex_Inv['DESCRIPTION']

    Ex_Inv.loc[((Ex_Inv['DESCRIPTION'].fillna(0)) == 0)&(Ex_Inv['DESCRIPTION_'].fillna(0)) != 0, 'Description' ]= Ex_Inv['DESCRIPTION_']


    ################### Remove the columns ###############

    Ex_Inv = Ex_Inv.filter(regex='^(?!.*DESCRIPTION_$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*PARENT_MODEL_$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*PARENT_MODEL$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*DESCRIPTION$)')

    ##############################################################################################


    ################################ LOB Region / LOB Area ########################

    LOB_CC_KA = pd.read_excel(POR_Path+'Mapping.xlsx',sheet_name='LOB_CC_KA', na_values=[''],keep_default_na=False)
    LOB_CC = pd.read_excel(POR_Path+'Mapping.xlsx',sheet_name='LOB_CC', na_values=[''],keep_default_na=False)


    ################################################### LOB Area / LOB Region ###################$$
    #print(Ex_Inv.columns)
        
    LOB_CC_KA = LOB_CC_KA[['CONCAT','LOB REGION_1','LOB AREA_1']]
        
        ############## Mering ######################
    Ex_Inv['CONCAT']=Ex_Inv['Country Code']+Ex_Inv['Key Account']
        
        # Merge the dataframes using the filtered LOB dataframe
    merged = pd.merge(Ex_Inv, LOB_CC_KA, on='CONCAT', how='left')
    Ex_Inv = pd.merge(merged, LOB_CC, left_on='Country Code' ,right_on='COUNTRY_CODE', how='left')
        
        ################### Assign value to LOB Region and LOB Area
    Ex_Inv.loc[Ex_Inv['LOB REGION_1'].notnull(),'LOB REGION'] = Ex_Inv['LOB REGION_1'] 
    Ex_Inv.loc[Ex_Inv['LOB AREA_1'].notnull(),'LOB AREA'] = Ex_Inv['LOB AREA_1'] 
        
    Ex_Inv.loc[((Ex_Inv['LOB REGION'].isnull()) & (Ex_Inv['LOB REGION_2'].notnull())),'LOB REGION'] = Ex_Inv['LOB REGION_2'] 
    Ex_Inv.loc[((Ex_Inv['LOB AREA'].isnull()) & (Ex_Inv['LOB AREA_2'].notnull())),'LOB AREA'] = Ex_Inv['LOB AREA_2'] 
        
        
        ########### Delete Columns
        
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*CONCAT$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*_2$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*_1$)')

    #Ex_Inv.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\Ex-Inv LOB.csv', index=False)  

    ############################ ASP #############################################

    OCP = pd.read_csv(Output_Path+'OCP.csv', low_memory=False)


    OCP = OCP[['PARENT_MODEL','COUNTRY_CODE','KEY_ACCOUNT','ASP']]
    OCP['Concat'] = OCP['PARENT_MODEL']+OCP['COUNTRY_CODE']+OCP['KEY_ACCOUNT']
    OCP = OCP[['Concat','ASP']]

    OCP = OCP.drop_duplicates(subset=['Concat'], keep='first')
     
    Ex_Inv['Concat1'] = Ex_Inv['Parent Model']+Ex_Inv['Country Code']+Ex_Inv['Key Account']


    Ex_Inv = pd.merge(Ex_Inv, OCP,left_on='Concat1',right_on='Concat',how='left')



    ########### Delete Columns

    if 'LOB' in Ex_Inv.columns:
        Ex_Inv.rename(columns={'LOB' :'ML'},inplace = True)
     
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*Concat1$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*Concat$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*_1$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*_2$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*_3$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*_4$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*Concat$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*Concat 1$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*Concat 2$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*Concat 3$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*Concat 4$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*ML$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*Parent$)')
    Ex_Inv = Ex_Inv.filter(regex='^(?!.*PID Type$)')

    ######################## Rename Columns

    Ex_Inv = Ex_Inv.rename(columns=lambda x: x.replace('Org Name','Country Name'))
    Ex_Inv = Ex_Inv.rename(columns=lambda x: x.replace('LOB REGION', 'LOB Region'))
    Ex_Inv = Ex_Inv.rename(columns=lambda x: x.replace('LOB AREA', 'LOB Area'))
    Ex_Inv = Ex_Inv.rename(columns=lambda x: x.replace('zone','Zone'))
    Ex_Inv = Ex_Inv.rename(columns=lambda x: x.replace('Master','Master Customer'))
    Ex_Inv = Ex_Inv.rename(columns=lambda x: x.replace('_Product_','Product LOB'))
    Ex_Inv = Ex_Inv.rename(columns=lambda x: x.replace( 'Customer_End Customer','Master Customer'))



    #Ex_Inv.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\Ex-Inv COUNTRY.csv', index=False)   
    ######################## Move columns 
    Ex_Inv['Bill to Country']=''
    Ex_Inv['DTF']=''
    Ex_Inv['DTF cutoff']=''
    Ex_Inv['Start Date']=''
    Ex_Inv['End Date']=''
    Ex_Inv['DTF risk units']=''
    Ex_Inv['DTF risk']=''
    Ex_Inv['Organization Code']=''
    Ex_Inv['KeyAccount_Billing']=''
    Ex_Inv['Customer LOB']=''


    Ex_Inv.insert(2,'LOB Region',Ex_Inv.pop('LOB Region'))
    Ex_Inv.insert(3,'LOB Area',Ex_Inv.pop('LOB Area'))  
    Ex_Inv.insert(4,'Country Name',Ex_Inv.pop('Country Name')) 
    Ex_Inv.insert(5,'Country Code',Ex_Inv.pop('Country Code')) 
    Ex_Inv.insert(6,'Bill to Country',Ex_Inv.pop('Bill to Country')) 
    Ex_Inv.insert(7,'Zone',Ex_Inv.pop('Zone')) 
    Ex_Inv.insert(8,'Customer LOB',Ex_Inv.pop('Customer LOB')) 
    Ex_Inv.insert(9,'Product LOB',Ex_Inv.pop('Product LOB')) 
    Ex_Inv.insert(10,'Product Range',Ex_Inv.pop('Product Range')) 
    Ex_Inv.insert(11,'Parent Model',Ex_Inv.pop('Parent Model')) 
    Ex_Inv.insert(12,'Class',Ex_Inv.pop('Class')) 
    Ex_Inv.insert(13,'Description',Ex_Inv.pop('Description')) 
    Ex_Inv.insert(14,'DTF',Ex_Inv.pop('DTF')) 
    Ex_Inv.insert(15,'DTF cutoff',Ex_Inv.pop('DTF cutoff')) 
    Ex_Inv.insert(16,'Start Date',Ex_Inv.pop('Start Date')) 
    Ex_Inv.insert(17,'End Date',Ex_Inv.pop('End Date')) 
    Ex_Inv.insert(18,'DTF risk units',Ex_Inv.pop('DTF risk units')) 
    Ex_Inv.insert(19,'DTF risk',Ex_Inv.pop('DTF risk')) 
    Ex_Inv.insert(20,'Organization Code',Ex_Inv.pop('Organization Code')) 
    Ex_Inv.insert(21,'KeyAccount_Billing',Ex_Inv.pop('KeyAccount_Billing')) 
    Ex_Inv.insert(22,'Key Account',Ex_Inv.pop('Key Account')) 
    Ex_Inv.insert(23,'Master Customer',Ex_Inv.pop('Master Customer')) 
    Ex_Inv.insert(24,'Item Type',Ex_Inv.pop('Item Type')) 
    Ex_Inv.insert(25,'ASP',Ex_Inv.pop('ASP')) 
    Ex_Inv.insert(26,'Data Series',Ex_Inv.pop('Data Series')) 
    Ex_Inv.insert(27,'PID',Ex_Inv.pop('PID')) 


    #Ex_Inv.to_csv(r'C:\Vasanthan\Python\NCR\IND POR\Output\Ex-Inv.csv', index=False)

    Ex_Inv = Ex_Inv.filter(regex='^(?!.*COUNTRY_CODE$)')

    Ex_Inv.drop_duplicates(inplace=True)



    Ex = Ex_Inv[Ex_Inv['Comments']=='3_EX']
    Inv = Ex_Inv[Ex_Inv['Comments']=='4_Inventory']

    Ex.to_csv(Output_Path+'EX-report.csv', index=False)
    Inv.to_csv(Output_Path+'Inventory-report.csv', index=False)

    import tkinter as tk
    from tkinter import messagebox
    def show_success_message():
       messagebox.showinfo("Success", "Ex-Invenotry Report Exported!")
    root = tk.Tk()
    root.withdraw()  # Hide the main window
    # Call the function to display the dialog box
    show_success_message()
    # Keep the application running until the user closes the dialog
    root.mainloop()



else:
    print("Enter Wrong Number")