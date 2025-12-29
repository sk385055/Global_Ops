import pandas as pd
import glob

#KA_Path  = r'E:\_Projects\DEP POR\Key Account\Input\\'

Path = r'E:\_Projects\DEP POR\Azure Vs IND POR\\'





############################ Load Files

POR = glob.glob(Path+'Input\ATLEOS OCP IND POR_???????????.csv')

#print(POR)


for POR in POR:
    IND_POR = pd.read_csv(POR,low_memory=False)
    
    
    IND_POR = IND_POR[IND_POR['Data Series'].str.contains('NEW DEMAND',na=False) & IND_POR['Item Type'].str.contains('Unassigned',na=False)]
    
    mask = IND_POR['Country Code'].isnull()

    IND_POR.loc[mask,'Country Code'] = 'NA'

    #IND_POR.to_csv(Path+'Output.csv')
    
    print(IND_POR.columns)



month=input("\n********************************************************** \n Enter Date from Header : ")

index = IND_POR.columns.get_loc('Comments')+1
IND_POR =IND_POR.iloc[:,0:index]

###############################################################
KA = pd.read_excel(Path+'Input\SQL.xlsx',keep_default_na=False)

Azure = pd.read_csv(Path+"Input\Azure.csv")

Region = pd.read_excel(Path+'Input\Region.xlsx')

mask = Region['COUNTRY_CODE'].isnull()

Region.loc[mask,'COUNTRY_CODE'] = 'NA'





Mix_Check_List = pd.read_excel(Path+'Input\Mix Check List.xlsx',sheet_name='Plant')


mask = Mix_Check_List['Country Code'].isnull()

Mix_Check_List.loc[mask,'Country Code'] = 'NA'

#print(Mix_Check_List)

#Mix_Check_List.to_csv(Path+'Output.csv')

################################################ AZURE ##################################################
############################## Remove Spaces

Azure['ISO Country Code'] = Azure['ISO Country Code'].str.strip()

mask = Azure['ISO Country Code'].isnull()

Azure.loc[mask,'ISO Country Code'] = 'NA'



Azure = pd.merge(Azure, Region[['COUNTRY_CODE','NEW_COUNTRY']],left_on='ISO Country Code',right_on='COUNTRY_CODE',how='left')

Azure.loc[Azure['NEW_COUNTRY'].notnull(),'ISO Country Code']=Azure['NEW_COUNTRY']

Azure = Azure.filter(regex='^(?!.*NEW_COUNTRY$)')
Azure = Azure.filter(regex='^(?!.*COUNTRY_CODE$)')

#################################### SSD & Ship Date filter

from dateutil.relativedelta import relativedelta
from datetime import datetime


Azure['Actual Ship Date'] = pd.to_datetime(Azure['Actual Ship Date'])
Azure['Scheduled Ship Date'] = pd.to_datetime(Azure['Scheduled Ship Date'])



# Create a new column 'SD_Date' with the month and year of the 'Actual Ship Date'
Azure['SD_Date'] = Azure['Actual Ship Date'].dt.strftime('%b-%y')

# Get the current date
latest_month = datetime.now()

# Create a list of the last 7 months
check_month = [latest_month - relativedelta(months=x) for x in range(7)]

# Convert the list of datetime objects to strings in the format '%b-%y'
check_month_str = [month.strftime('%b-%y') for month in check_month]

# Filter the Azure dataframe to only include rows where 'SD_Date' is in 'check_month_str'
Azure_Actual_Ship = Azure[Azure['SD_Date'].isin(check_month_str)]


# Create a list of the last 7 months
check_month = [latest_month + relativedelta(months=x) for x in range(13)]



# Convert the list of datetime objects to strings in the format '%b-%y'
check_month_str = [month.strftime('%b-%y') for month in check_month]


# Create a new column 'SD_Date' with the month and year of the 'Actual Ship Date'
Azure['SD_Date'] = Azure['Scheduled Ship Date'].dt.strftime('%b-%y')


# Filter the Azure dataframe to only include rows where 'SD_Date' is in 'check_month_str'
Azure_SSD = Azure[Azure['SD_Date'].isin(check_month_str) & Azure['Actual Ship Date'].isnull()]



Azure = pd.concat([Azure_Actual_Ship,Azure_SSD],axis=0)

Azure = Azure.filter(regex='^(?!.*SSD_Date$)')
Azure = Azure.filter(regex='^(?!.*SD_Date$)')



#######################################################################################
#################################### SSD & Ship Dt Validation process


print('\n')

print('*************************************************\n')

print('Checking Backward 6month Shipment & Foward 12 month SSD \n')

print('*************************************************\n')
####### Ship Date Validation
Azure['SD_Date'] = Azure['Actual Ship Date'].dt.strftime('%b-%y')

#print(Azure['SD_Date'])

Azure['SD_Date'] = Azure['SD_Date'].astype(str)

# Get the latest month in the 'month' column
latest_month = datetime.now()

# Calculate the month and year 4 months before the latest month
check_month = latest_month - relativedelta(months=6)

#print(type(check_month))

# Convert check_month to the same format as Azure['month']
check_month_str = check_month.strftime('%b-%y')

# Check if the calculated month exists in the 'month' column
is_present = (Azure['SD_Date'] == check_month_str).any()

print(f"Backward 6 Month Shipment Date       : {check_month_str} present? {is_present}")

############### SSD Validation


Azure['SSD_Date'] = Azure['Scheduled Ship Date'].dt.strftime('%b-%y')

#print(Azure['SSD_Date'])

Azure['SSD_Date'] = Azure['SSD_Date'].astype(str)

# Get the latest month in the 'month' column
latest_month = datetime.now()

# Calculate the month and year 4 months before the latest month
check_month = latest_month + relativedelta(months=12)

#print(type(check_month))

# Convert check_month to the same format as Azure['month']
check_month_str = check_month.strftime('%b-%y')

# Check if the calculated month exists in the 'month' column
is_present = (Azure['SSD_Date'] == check_month_str).any()

print(f"Forward 12 Month Schedule Ship Date  : {check_month_str} present? {is_present}")

Azure = Azure.filter(regex='^(?!.*SSD_Date$)')
Azure = Azure.filter(regex='^(?!.*SD_Date$)')


#Azure.to_csv(Path+'Output.csv')
print('*************************************************\n')
continues = input('Press Enter to Continue......... :')

#############################################################################
############################## Including KA 

KA = KA[['MASTER_CUST_NUM','KEY_ACCOUNT','MASTER_CUST_NAME']]

KA['MASTER_CUST_NUM'] = KA['MASTER_CUST_NUM'].astype(str)
Azure['Master Customer Number'] = Azure['Master Customer Number'].astype(str)

KA.drop_duplicates(inplace=True)

Azure = pd.merge(Azure, KA, left_on='Master Customer Number',right_on='MASTER_CUST_NUM',how='left')

Azure.loc[Azure['KEY_ACCOUNT'].isnull(),'KEY_ACCOUNT'] = "OTHER"


Azure.loc[Azure['KEY_ACCOUNT']=='OTHER','KEY_ACCOUNT'] = 'OTHER-'+Azure['ISO Country Code']




######################## Data Series

# Create a boolean mask for non-null 'Actual Ship Date'
mask = Azure['Actual Ship Date'].notnull()

# Update the 'Data Series' column where the mask is True
Azure.loc[mask, 'Data Series'] = 'SHIP'

# Update the '6M Ship' column where the mask is True
Azure.loc[mask, '6M Ship'] = Azure['Net Quantity']



mask = Azure['Scheduled Ship Date'].notnull() & Azure['Actual Ship Date'].isnull()

Azure.loc[mask,'Data Series'] = 'Order'

Azure.loc[mask,'6M Order'] = Azure['Net Quantity']

############################################
############################################

###### Keep w/o blank in Data series
Azure = Azure[Azure['Data Series'].notnull()]


################### Check list
Azure['MCID Class'] = Azure['MCID Class'].astype(int).round(0)

Azure['Concat'] = Azure['ISO Country Code']+Azure['KEY_ACCOUNT']+Azure['MCID Class'].astype(str)


Azure[['ISO Country Code','Order Number','Order Type Name','Line Number','Shipment Number','Master Customer Number','Master Customer Name','KEY_ACCOUNT','Offering Accounting Type Code','MCID Class','Product ID','Product Description','Order Booked Date','Line Request Date Time','Scheduled Ship Date','Scheduled Arrival Date','Actual Ship Date','Line Status','Data Series','6M Ship','6M Order','Net Quantity','Warehouse Name','Organization Code','PID type','MCID-Net Order Value-US','MCID- MCC-US','Purchase Order Number','Invoice Trigger','Functional Group Short Name','region_name','canceled_quantity']].to_csv(Path+'\Output\Azure.csv',index=False)

#Azure.to_csv(Path+'\Output\Azure_checklist.csv',index=False)

print('Azure Completed')

############################# 

Check_List = Azure[['Concat','Product ID']]


Check_List.drop_duplicates(inplace=True)

#Check_List.to_csv(Path+'Output\Checklist.csv',index=False)


#Check_List = pd.read_csv(Path+'Output\Checklist.csv')


Check_List = Check_List.groupby('Concat')['Product ID'].apply(','.join).reset_index()

Check_List.to_csv(Path+'\Output\Check List.csv',index=False)

print('Check list Completed')

####################################### IND POR #####################################################

########################### insert new publish flag

Mix_Check_List = pd.read_excel(Path+'Input\Mix Check List.xlsx',sheet_name='Publish Flag')

Mix_Check_List = Mix_Check_List[['Class','Publish flag']]

Mix_Check_List = Mix_Check_List.drop_duplicates(subset=['Class'],keep='first')

IND_POR = pd.merge(IND_POR, Mix_Check_List,on='Class',how='left')

#IND_POR = IND_POR[IND_POR['Publish flag'] == "Y"]

#print('line 119 : ',IND_POR.columns)

##########################





IND_POR['Concat'] = IND_POR['Country Code']+ IND_POR['Key Account'] +IND_POR['Class'].astype(str)

IND_POR = pd.merge(IND_POR, Check_List, on='Concat', how='left')


#IND_POR.to_csv(Path+'IND_POR.csv',index=False)

mask = IND_POR['Product ID'].isnull()

if mask.any():
    IND_POR['Concat_1'] = IND_POR['Bill to Country'] + IND_POR['KeyAccount_Billing'] + IND_POR['Class'].astype(str)
    
    IND_POR = pd.merge(IND_POR, Check_List, left_on='Concat_1', right_on='Concat', how='left')
    
    
mask = (IND_POR['Product ID_x'].isnull() & IND_POR['Product ID_y'].notnull())

if mask.any():
    IND_POR.loc[mask, 'Country Code'] = IND_POR.loc[mask, 'Bill to Country']
    IND_POR.loc[mask, 'Key Account'] = IND_POR.loc[mask, 'KeyAccount_Billing']
    IND_POR.loc[mask, 'Product ID_x'] = IND_POR.loc[mask, 'Product ID_y']
    

IND_POR.loc[IND_POR['Product ID_x'].notnull(),'Partial config']='No'
IND_POR.loc[IND_POR['Product ID_x'].isnull(),'Partial config']='Yes'

IND_POR.to_csv(Path+'dEMOD.csv',index=False)

IND_POR = IND_POR.rename(columns=lambda x: x.replace('Product ID_x','MCID'))
IND_POR = IND_POR.filter(regex='^(?!.*Product ID_y$)')
IND_POR = IND_POR.filter(regex='^(?!.*Concat_x$)')
IND_POR = IND_POR.filter(regex='^(?!.*Concat_1$)')
IND_POR = IND_POR.filter(regex='^(?!.*Concat_y$)')


#print('LIne number 150 :',IND_POR.columns)
###########################################################################################
############################## Update Plant

Mix_Check_List = pd.read_excel(Path+'Input\Mix Check List.xlsx',sheet_name='Plant')

mask = Mix_Check_List['Country Code'].isnull()

Mix_Check_List.loc[mask,'Country Code'] = 'NA'

#print(Mix_Check_List.columns)
  

Mix_Check_List = Mix_Check_List.drop_duplicates(subset=['Class','Country Code'],keep='first')


IND_POR = pd.merge(IND_POR, Mix_Check_List[['Country Code','Class','Plant']],on=['Class','Country Code'],how='left')


#####################
Mix_Check_List = pd.read_excel(Path+'input\Mix Check List.xlsx',sheet_name='Publish Flag',keep_default_na=False)


Mix_Check_List = Mix_Check_List[['As per Sourcing Matrix','Country Code']]

Mix_Check_List = Mix_Check_List.drop_duplicates(subset=['Country Code'],keep='first')



IND_POR = pd.merge(IND_POR, Mix_Check_List,on='Country Code',how='left')

#IND_POR.to_csv(Path+'IND_POR1.csv',index=False)

####################
Mix_Check_List = pd.read_excel(Path+'Input\Mix Check List.xlsx',sheet_name='Plant')

Mix_Check_List = Mix_Check_List.drop_duplicates(subset=['Country Code','Class'],keep='first')



mask = Mix_Check_List['Country Code'].isnull()

Mix_Check_List.loc[mask,'Country Code'] = 'NA'

#IND_POR.to_csv(Path+'IND_POR1.csv',index=False)

IND_POR = pd.merge(IND_POR, Mix_Check_List[['Country Code','Class','Plant']],left_on=['Class','As per Sourcing Matrix'],right_on=['Class','Country Code'],how='left')

#IND_POR.to_csv(Path+'IND_POR2.csv',index=False)



mask = (IND_POR['Plant_x'].isnull() & IND_POR['Plant_y'].notnull())

if mask.any():
    IND_POR.loc[mask, 'Plant_x'] = IND_POR.loc[mask, 'Plant_y']

IND_POR = IND_POR.rename(columns=lambda x: x.replace('Country Code_x', 'Country Code'))
IND_POR = IND_POR.rename(columns=lambda x: x.replace('Plant_x', 'Plant'))

IND_POR = IND_POR.filter(regex='^(?!.*Country Code_y$)')
IND_POR = IND_POR.filter(regex='^(?!.*Plant_y$)')

#print('Line 194 : ',IND_POR.columns)

##############################################################################
######################### Update DTF
Mix_Check_List = pd.read_excel(Path+'Input\Mix Check List.xlsx',sheet_name='Plant',keep_default_na=False)

Mix_Check_List = Mix_Check_List.drop_duplicates(subset=['Class.1', 'Plant.1'],keep='first')



IND_POR = pd.merge(IND_POR, Mix_Check_List[['Class.1', 'Plant.1', 'DTF']],left_on=['Class','Plant'],right_on=['Class.1', 'Plant.1'],how='left')

#print(Mix_Check_List.columns)
#IND_POR.to_csv(Path+'IND_POR2.csv',index=False)

from pandas.tseries.offsets import MonthEnd

IND_POR['Start Date'] = pd.to_datetime(IND_POR['Start Date'])

   
IND_POR['DTF_y'] = IND_POR['DTF_y'].fillna(0).astype(int)  


 
IND_POR.loc[IND_POR['DTF_y'].notnull() , 'End Date_New'] = (IND_POR['Start Date'] + pd.to_timedelta(IND_POR['DTF_y'].astype(int), unit='D')).dt.strftime('%m/%d/%Y')
  
IND_POR = IND_POR.rename(columns=lambda x: x.replace('DTF_y', 'DTF'))

IND_POR = IND_POR.filter(regex='^(?!.*Class.1$)')
IND_POR = IND_POR.filter(regex='^(?!.*Plant.1$)')
IND_POR = IND_POR.filter(regex='^(?!.*End Date$)')
IND_POR = IND_POR.filter(regex='^(?!.*DTF_x$)')

 
IND_POR = IND_POR.rename(columns=lambda x: x.replace('End Date_New', 'End Date'))

############ Transition Month

Mix_Check_List = pd.read_excel(Path+'Input\Mix Check List.xlsx',sheet_name='Plant')

Mix_Check_List = Mix_Check_List.drop_duplicates(subset=['Class','Country Code'],keep='first')


mask = Mix_Check_List['Country Code'].isnull()

Mix_Check_List.loc[mask,'Country Code'] = 'NA'

Mix_Check_List = Mix_Check_List[['Class','Country Code','Sea','Air']]

Mix_Check_List['Sea'] = Mix_Check_List['Sea'].fillna(0)
Mix_Check_List['Air'] = Mix_Check_List['Air'].fillna(0)



IND_POR['Class'] = IND_POR['Class'].astype(str)
Mix_Check_List['Class'] = Mix_Check_List['Class'].astype(str)

IND_POR = pd.merge(IND_POR, Mix_Check_List,on=['Class','Country Code'],how='left')



IND_POR = pd.merge(IND_POR, Mix_Check_List,right_on=['Class','Country Code'],left_on=['Class','As per Sourcing Matrix'],how='left')


#print(IND_POR.columns)

mask = IND_POR['Sea_x'].isnull() & IND_POR['Sea_y']
IND_POR.loc[mask,'Sea_x']=IND_POR['Sea_y']

mask = IND_POR['Air_x'].isnull() & IND_POR['Air_y']
IND_POR.loc[mask,'Air_x']=IND_POR['Air_y']

IND_POR = IND_POR.filter(regex='^(?!.*Air_y$)')
IND_POR = IND_POR.filter(regex='^(?!.*Sea_y$)')
IND_POR = IND_POR.filter(regex='^(?!.*Country Code_y$)')
IND_POR = IND_POR.filter(regex='^(?!.*As per Sourcing Matrix$)')

    
IND_POR = IND_POR.rename(columns=lambda x: x.replace('Sea_x', 'Sea'))
IND_POR = IND_POR.rename(columns=lambda x: x.replace('Air_x', 'Air'))
IND_POR = IND_POR.rename(columns=lambda x: x.replace('Country Code_x', 'Country Code'))



### MROUND

# Convert the 'Sea' column to numeric, and replace non-numeric values with NaN
IND_POR['Sea'] = pd.to_numeric(IND_POR['Sea'], errors='coerce')
IND_POR['Air'] = pd.to_numeric(IND_POR['Air'], errors='coerce')

# Fill NaN values with 0
IND_POR['Sea'] = IND_POR['Sea'].fillna(0)
IND_POR['Air'] = IND_POR['Air'].fillna(0)

# Define the mround function
def mround(x, base=30):
    return base * round(x/base)

# Apply the function to the 'Sea' column
IND_POR['Transit Month'] = IND_POR['Sea'].astype(int).apply(mround)


IND_POR.loc[IND_POR['Product Range'].str.contains('PC Core'),'Transit Month'] =IND_POR['Air'].astype(int).apply(mround)

IND_POR = IND_POR.filter(regex='^(?!.*Air$)')
IND_POR = IND_POR.filter(regex='^(?!.*Sea$)')

IND_POR_WO = IND_POR


IND_POR = IND_POR.rename(columns=lambda x: x.replace('Region','LOB Region'))
IND_POR = IND_POR.rename(columns=lambda x: x.replace('Area','LOB Area'))
IND_POR = IND_POR.rename(columns=lambda x: x.replace('Region1','Region'))
IND_POR = IND_POR.rename(columns=lambda x: x.replace('Area2','Area'))

columns_to_move = ['Demand Stream','Region1','Area2','Region','Theatre','Area','Country Name','Country Code','Bill to Country','Plant','Publish flag','Zone','Customer LOB','Product LOB','Product Range','Parent Model','Class','Description','DTF','DTF cutoff','Start Date','End Date','DTF risk units','DTF risk','Organization Code','MCID','Partial config','Transit Month','KeyAccount_Billing','Key Account','Master Customer','Item Type','ASP','Data Series']

remaining_columns = [col for col in IND_POR_WO.columns if col not in columns_to_move]
    
IND_POR_WO = IND_POR_WO[columns_to_move + remaining_columns]

#IND_POR_WO = IND_POR_WO.filter(regex='^(?!.*Comments$)')

IND_POR_WO.to_csv(Path+'\Output\IND_POR_WO.csv',index=False)


###################################### Sourcing Action 

print('*************************************************')
print('\n')

print('IND POR WO Sourcing Action Expoted in Output Folder\n')


print('*************************************************')
continues = input('Press Enter Sourcing Action Done : ')


###################################### Transit Month Updation
#IND_POR = pd.read_csv(Path+'\Output\IND_POR_WO.csv',keep_default_na=False)
IND_POR = pd.read_csv(Path+'\Output\IND_POR_WO.csv')



########################################
IND_POR['Country Code'] = IND_POR['Country Code'].fillna('NA')
IND_POR['Bill to Country'] = IND_POR['Bill to Country'].fillna('NA')




#print(IND_POR.columns)


Index_month = IND_POR.columns.get_loc(month)
Index_Comments = IND_POR.columns.get_loc('Comments')

Index_30_Start = Index_month -1
Index_30_End = Index_Comments-1

Index_60_Start = Index_month -2
Index_60_End = Index_Comments -2

Index_90_Start = Index_month -3
Index_90_End = Index_Comments -3


End_Month = IND_POR.columns[Index_30_End]
End_Month_60 = IND_POR.columns[Index_60_End]
End_Month_90 = IND_POR.columns[Index_90_End]


IND_POR['Transit Month'] = IND_POR['Transit Month'].astype(int)





condition_30 = (IND_POR['Data Series'].str.contains('NEW DEMAND',na=False)) & (IND_POR['Transit Month'] == 30)
condition_60 = (IND_POR['Data Series'].str.contains('NEW DEMAND',na=False)) & (IND_POR['Transit Month'] == 60)
condition_90 = (IND_POR['Data Series'].str.contains('NEW DEMAND',na=False)) & (IND_POR['Transit Month'] == 90)





# Check if any row meets the condition
if not IND_POR[condition_30].empty:
    
    cut_data = IND_POR.loc[condition_30].iloc[:, Index_month:Index_Comments].copy()
    
    IND_POR.loc[condition_30, IND_POR.columns[Index_30_Start:Index_30_End]] = cut_data.values

    IND_POR.loc[condition_30, End_Month] = ""

if not IND_POR[condition_60].empty:
    
    cut_data = IND_POR.loc[condition_60].iloc[:, Index_month:Index_Comments].copy()
    
    IND_POR.loc[condition_60, IND_POR.columns[Index_60_Start:Index_60_End]] = cut_data.values

    IND_POR.loc[condition_60, End_Month] = ""
    
    IND_POR.loc[condition_60, End_Month_60] = ""

if not IND_POR[condition_90].empty:
    
    cut_data = IND_POR.loc[condition_90].iloc[:, Index_month:Index_Comments].copy()
    
    IND_POR.loc[condition_90, IND_POR.columns[Index_90_Start:Index_90_End]] = cut_data.values

    IND_POR.loc[condition_90, End_Month] = ""
    
    IND_POR.loc[condition_90, End_Month_60] = ""

    IND_POR.loc[condition_90, End_Month_90] = ""
    
    
    

############### Column Alignments


columns_to_move = ['Demand Stream','Region1','Area2','Region','Theatre','Area','Country Name','Country Code','Bill to Country','Plant','Publish flag','Zone','Customer LOB','Product LOB','Product Range','Parent Model','Class','Description','DTF','DTF cutoff','Start Date','End Date','DTF risk units','DTF risk','Organization Code','MCID','Partial config','Transit Month','KeyAccount_Billing','Key Account','Master Customer','Item Type','ASP','Data Series']

remaining_columns = [col for col in IND_POR.columns if col not in columns_to_move]
    
IND_POR = IND_POR[columns_to_move + remaining_columns]

######################################## DTF End Date updation #########################
try:
    IND_POR['End Date'] = pd.to_datetime(IND_POR['End Date'])
except:
    try:
        IND_POR['End Date'] = pd.to_datetime(IND_POR['End Date'], format='%m/%d/%Y')
    except:
        try:
            IND_POR['End Date'] = pd.to_datetime(IND_POR['End Date'], format='mixed')
        except:
            IND_POR['End Date'] = pd.to_datetime(IND_POR['End Date'], format='%m-%d-%Y')


IND_POR['Month Adjustment'] = IND_POR['End Date'].dt.strftime('%b-%y')


for i, x in enumerate(IND_POR['Month Adjustment']):
    if pd.isnull(x) or x not in IND_POR.columns:  # Check if x is nan or not in columns
        continue  # Skip this iteration if x is nan or not a column name
    Index_month = IND_POR.columns.get_loc(x)
    IND_POR.iloc[i, 34:Index_month+1] = ""


    
    
#print(IND_POR['Month Adjustment'])


IND_POR = IND_POR.filter(regex='^(?!.*Month Adjustment$)')
IND_POR = IND_POR.filter(regex='^(?!.*Comments$)')

IND_POR.to_csv(Path+'\Output\IND_POR_W.csv',index=False)

print('\n')
print('*************************************************\n')
print('         IND POR With Changes Completed     ')
print('\n*************************************************')

##################################### Missing Configuation ##############################
POR = pd.read_csv(Path+'\Output\IND_POR_WO.csv')


###### Filter Publish flag / Partial config
POR = POR[POR['Partial config'].str.contains('Yes',na=False) & POR['Publish flag'].str.contains('Y',na=False)]

#POR.to_csv(Path+'\Output\Output.csv',index=False)




Start_index = POR.columns.get_loc(month)
End_index = Start_index + 9


# Define the columns to keep
columns_to_keep = ['Region', 'Class', 'Key Account', 'Country Code', 'Plant'] 

#POR = POR.groupby(columns_to_keep).agg({'Jul-24':'sum','Aug-24':'sum','Sep-24':'sum','Oct-24':'sum','Nov-24':'sum','Dec-24':'sum','Jan-25':'sum','Feb-25':'sum','Mar-25':'sum'})

POR = POR.groupby(columns_to_keep).agg({POR.columns[Start_index]:'sum',POR.columns[Start_index+1]:'sum',POR.columns[Start_index+2]:'sum',POR.columns[Start_index+3]:'sum',POR.columns[Start_index+4]:'sum',POR.columns[Start_index+5]:'sum',POR.columns[Start_index+6]:'sum',POR.columns[Start_index+7]:'sum',POR.columns[Start_index+8]:'sum'})

POR.to_csv(Path+'\Output\Missing Config.csv')

POR = pd.read_csv(Path+'\Output\Missing Config.csv')

# Sum the desired indices and store the result in a new column 'Total'
POR['Total'] = POR.iloc[:, 5:14].sum(axis=1)

POR = POR[POR['Total']>0]

POR['CONC'] = POR['Class'].astype(str) +POR['Country Code']+POR['Key Account']

####################### Load NFC
NFC_Input = '\\Input\\NFC Tracker.xlsx'

NFC = pd.read_excel(Path+NFC_Input,sheet_name='6M NFC')

NFC = NFC[['CONC','Config Status']]

NFC = NFC[NFC['Config Status'].str.contains('Completed',na=False)]

Missing_Config = pd.merge(POR, NFC,on='CONC',how='left')

Missing_Config = Missing_Config[Missing_Config['Config Status'].isnull()]

Missing_Config = Missing_Config.filter(regex='^(?!.*CONC$)')
Missing_Config = Missing_Config.filter(regex='^(?!.*Config Status$)')




Missing_Config.loc[Missing_Config['Total']>20,'Status'] = 'Missing Config'
Missing_Config.loc[Missing_Config['Total']<=20,'Status'] = 'Below 20'

Missing_Config = Missing_Config.sort_values(by='Total',ascending=False)


Missing_Config.to_csv(Path+'\Output\Missing Config.csv',index=False)


print('\n')
print('*************************************************\n')
print('         Missing Configuration Completed     ')
print('\n*************************************************')

###################################################################################
################# NEW Demand
###################################################################################

IND_POR.rename(columns={'Region':'LOB Region'},inplace=True)
IND_POR.rename(columns={'Area':'LOB Area'},inplace=True)
IND_POR.rename(columns={'Region1':'Region'},inplace=True)
IND_POR.rename(columns={'Area2':'Area'},inplace=True)

IND_POR['CC-Cls+Ctry+KA'] = IND_POR['Class'].astype(str) + IND_POR['Country Code'] + IND_POR['Key Account']


IND_POR = IND_POR[IND_POR['Publish flag'].str.contains('Y',na=False)]


Start_index = IND_POR.columns.get_loc(month)



# Define the columns to keep
columns_to_keep = ['CC-Cls+Ctry+KA','Region','Area','LOB Region','LOB Area','Country Name','Country Code','Plant','Publish flag','Zone','Customer LOB','Product LOB','Product Range','Parent Model','Class','MCID','Partial config','Key Account','Transit Month'] 



# Replace blank values with the keyword "blanks"
IND_POR[columns_to_keep] = IND_POR[columns_to_keep].replace('', 'dummy')
IND_POR[columns_to_keep] = IND_POR[columns_to_keep].fillna("dummy")

# Define the columns to sum
columns_to_sum = IND_POR.columns[Start_index:]

# Create a dictionary for aggregation
agg_dict = {col: 'sum' for col in columns_to_sum}

# Group by the columns to keep and aggregate the columns to sum
IND_POR = IND_POR.groupby(columns_to_keep).agg(agg_dict)

# Save the result to a csv file
IND_POR.to_csv(Path+'Output\\New Demand.csv')


New_Demand = pd.read_csv(Path+'Output\\New Demand.csv')

New_Demand[columns_to_keep] = New_Demand[columns_to_keep].replace('dummy', '')

# Get the column names for the specified slice
column_names = New_Demand.columns[19:26]

# Fill NaN values with 0 before converting to integer
for column in column_names:
    #print(column)
    New_Demand[column] = New_Demand[column].fillna(0).astype(int)

# Calculate the sum and store it in the new column '7M'
New_Demand['7M'] = New_Demand[column_names].sum(axis=1)


# Get the column names for the specified slice
column_names = New_Demand.columns[19:28]

# Fill NaN values with 0 before converting to integer
for column in column_names:
    New_Demand[column] = New_Demand[column].fillna(0).astype(int)

# Calculate the sum and store it in the new column '7M'
New_Demand['9M'] = New_Demand[column_names].sum(axis=1)


# Get the column names for the specified slice
column_names = New_Demand.columns[19:31]

# Fill NaN values with 0 before converting to integer
for column in column_names:
    New_Demand[column] = New_Demand[column].fillna(0).astype(int)

# Calculate the sum and store it in the new column '7M'
New_Demand['12M'] = New_Demand[column_names].sum(axis=1)

# Save the result to a csv file
New_Demand.to_csv(Path+'Output\\New Demand.csv',index=False)
    
print('\n')
print('*************************************************\n')
print('         New Demand Completed     ')
print('\n*************************************************')

###################################################################################
################################# EDW ########################################
###################################################################################


Azure = pd.read_csv(Path+'\Output\Azure.csv')

Mix_Check_List = pd.read_excel(Path+'Input\Mix Check List.xlsx',sheet_name='Plant')

mask = Mix_Check_List['Country Code'].isnull()

Mix_Check_List.loc[mask,'Country Code'] = 'NA'

mask = Azure['ISO Country Code'].isnull()

Azure.loc[mask,'ISO Country Code'] = 'NA'

#print(Mix_Check_List.columns)
  

Mix_Check_List = Mix_Check_List.drop_duplicates(subset=['Class','Country Code'],keep='first')


Azure = pd.merge(Azure, Mix_Check_List[['Country Code','Class','Plant']],left_on=['MCID Class','ISO Country Code'],right_on=['Class','Country Code'],how='left')


#####################
Mix_Check_List = pd.read_excel(Path+'input\Mix Check List.xlsx',sheet_name='Publish Flag',keep_default_na=False)


Mix_Check_List = Mix_Check_List[['As per Sourcing Matrix','Country Code']]

Mix_Check_List = Mix_Check_List.drop_duplicates(subset=['Country Code'],keep='first')



Azure = pd.merge(Azure, Mix_Check_List,left_on = 'ISO Country Code',right_on='Country Code',how='left')

#Azure.to_csv(Path+'Azure1.csv',index=False)

####################
Mix_Check_List = pd.read_excel(Path+'Input\Mix Check List.xlsx',sheet_name='Plant')

Mix_Check_List = Mix_Check_List.drop_duplicates(subset=['Country Code','Class'],keep='first')



mask = Mix_Check_List['Country Code'].isnull()

Mix_Check_List.loc[mask,'Country Code'] = 'NA'

#Azure.to_csv(Path+'Azure1.csv',index=False)

Azure = pd.merge(Azure, Mix_Check_List[['Country Code','Class','Plant']],left_on=['Class','As per Sourcing Matrix'],right_on=['Class','Country Code'],how='left')

#Azure.to_csv(Path+'Azure2.csv',index=False)


mask = (Azure['Plant_x'].isnull() & Azure['Plant_y'].notnull())

if mask.any():
    Azure.loc[mask, 'Plant_x'] = Azure.loc[mask, 'Plant_y']

Azure = Azure.rename(columns=lambda x: x.replace('Country Code_x', 'Country Code'))
Azure = Azure.rename(columns=lambda x: x.replace('Plant_x', 'Plant'))

Azure = Azure.filter(regex='^(?!.*Country Code_y$)')
Azure = Azure.filter(regex='^(?!.*Plant_y$)')


##################### Publish Flag


Mix_Check_List = pd.read_excel(Path+'Input\Mix Check List.xlsx',sheet_name='Publish Flag')

Mix_Check_List = Mix_Check_List[['Class','Publish flag']]

Mix_Check_List = Mix_Check_List.drop_duplicates(subset=['Class'],keep='first')

Azure = pd.merge(Azure, Mix_Check_List,left_on='MCID Class',right_on='Class',how='left')

Azure.rename(columns={'ISO Country Code':'ISO_CC'},inplace=True)


Azure = Azure.filter(regex='^(?!.*As per Sourcing Matrix$)')
Azure = Azure.filter(regex='^(?!.*Country Code$)')
Azure = Azure.filter(regex='^(?!.*Class_y$)')

Azure.rename(columns={'ISO_CC':'ISO Country Code'},inplace=True)

#print(Azure.columns)

############
Azure['CC-Cls+Ctry+KA'] = Azure['MCID Class'].astype(str) + Azure['ISO Country Code'] +Azure['KEY_ACCOUNT']

Azure.rename(columns={'KEY_ACCOUNT':'Key Account'},inplace=True)
Azure.rename(columns={'KEY_ACCOUNT':'Pblish Flag'},inplace=True)
Azure.rename(columns={'Data Series':'Data series'},inplace=True)
Azure.rename(columns={'6M Ship':'6M SHP'},inplace=True)
Azure.rename(columns={'KEY_ACCOUNT':'Key Account'},inplace=True)
Azure.rename(columns={'Plant':'Correct Org'},inplace=True)
Azure.rename(columns={'Publish flag':'Pblish Flag'},inplace=True)

Azure[['CC-Cls+Ctry+KA','ISO Country Code','Correct Org','Order Number','Order Type Name','Line Number','Shipment Number','Master Customer Number','Master Customer Name','Key Account','Pblish Flag','Offering Accounting Type Code','MCID Class','Product ID','Product Description','Order Booked Date','Line Request Date Time','Scheduled Ship Date','Scheduled Arrival Date','Actual Ship Date','Line Status','Data series','6M SHP','6M Order','Net Quantity','Warehouse Name','Organization Code','PID type','MCID-Net Order Value-US','MCID- MCC-US','Purchase Order Number','Invoice Trigger','Functional Group Short Name']].to_csv(Path+'\Output\EDW.csv',index=False)

print('\n')
print('*************************************************\n')
print('         EDW Completed     ')
print('\n*************************************************')




 