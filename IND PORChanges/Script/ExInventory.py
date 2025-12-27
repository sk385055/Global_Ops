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
print(Ex_Inv.columns)

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


print(Ex_Inv)


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
print(Ex_Inv.columns)
    
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
print('** Ex-Invenotry Report Exported Successfuly**')

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


