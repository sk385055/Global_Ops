from operator import index
from xml.dom import IndexSizeErr
import pandas as pd
import numpy as np
import cx_Oracle
import pyodbc
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from collections import Counter
import warnings
from sqlalchemy import false
import sys 
warnings.filterwarnings('ignore')




file_name = sys.argv[1]
#Todays date time
str_date = datetime.today().strftime('%d')
str_month_lower  = datetime.today().strftime('%b').lower()
str_year = datetime.today().strftime('%y').lower()
amm_filedate = datetime.now().strftime('%d-%b-%y')
'''
#fetching data base-------------------------------------------------
print('Fetching data into database.............')
conn_str = 'reports/reports@//153.84.74.45:1521/DEMREPP'
conn = cx_Oracle.connect(conn_str)
#for ammocp
if file_name == 'ammocp':
    print("File name is :" + file_name)
    fd = open('./sql/ocp_plan_det_v4_amm.sql', 'r')
    
elif file_name =='eumocp':
    #for eumocp
    print("File name is :" + file_name)
    fd = open('./sql/ocp_plan_det_v4_eum.sql', 'r')

sql_file = fd.read()
fd.close()
data_raw = pd.read_sql(sql_file, conn)

print('Fetching data completed')
print('cooking data.......................')
data_raw.to_csv(r'.\snd\DempRep.csv',encoding='utf-8', index=False)

'''
#-------------------------------------------------------------------------

#main data
print('Reading main data files')
data_raw = pd.read_csv(r'.\snd\DempRep.csv',encoding='utf-8', low_memory=False, keep_default_na='')

data_raw.rename(columns={'Supply Chain Order Type': 'Supply Chain Order Type Description',
                               'Inventory Item Number': 'Inventory Item Number Hyphenated',
                               'Supply Chain Transaction': 'Supply Chain Transaction Identifier'}, inplace=True)

data_raw.drop(columns={'Planner Code','Planner Name'},inplace=True)
#--------------------------------------------------------------------------
# data_raw = data_raw[data_raw['Inventory Item Number Hyphenated'].isin(['6688MC2580','7360MC2173'])]
#------------------------------------------------------------------------
#Rank file
rank_file = pd.read_excel(r'.\rcv\lookup\Rank_lookup.xlsx', engine='openpyxl')
rank_file['Order Type 2'] = rank_file['Order Type']
data_raw = data_raw.merge(rank_file[['Supply Chain Order Type Description','Order Type 2']], on='Supply Chain Order Type Description', how='left',validate='m:1')

# condition for ORDER TYPE
data_raw['Order Type'] =''
cond_pbo = data_raw['Supply Chain Order Number'].str.match('^([0-9]{5}|[0-9]{6}|[0-9]{7}|[0-9]{8})\.Pre-Build')
cond_int = (data_raw['Order Type']=='')&(data_raw['Supply Chain Order Type Description'].isin(['Transfer order demand']))
cond_plan_make = (data_raw['Order Type']=='')&(data_raw['Inventory Organization Code'] == data_raw['Source Organization Code']) &\
                  (data_raw['Action Description'].isin(['Release'])|data_raw['Action Description'].isin(['None']))
cond_int_req = (data_raw['Order Type']=='')&(data_raw['Supply Chain Order Type Description'].isin(['Transfer order']))
cond_ex = (data_raw['Order Type']=='')&(data_raw['Supply Chain Order Type Description'].isin(['Planned order'])) & (~data_raw['Source Organization Code'].str.startswith('EBS'))
cond_plan_irio = (data_raw['Order Type']=='')&(data_raw['Supply Chain Order Type Description'].isin(['Planned order'])) & (data_raw['Source Organization Code'].str.match('^EBS:[A-U,W-Z]'))
cond_flow = (data_raw['Order Type']=='')&(data_raw['Supply Chain Order Type Description'].isin(['Nonstandard work order']))
cond_final = data_raw['Order Type']==''

data_raw['Order Type'] = np.where(cond_pbo,'PBO',np.where(cond_int,'Int Order',\
        np.where(cond_plan_make,'Plan Make',np.where(cond_int_req,'Int Req',\
        np.where(cond_ex,'EX',np.where(cond_plan_irio,'Plan IR/IO',\
            np.where(cond_flow,'Flow/WIP',np.where(cond_final,data_raw['Order Type 2'],data_raw['Order Type']))))))))


# drop column order type 2
data_raw.drop(columns={'Order Type 2'},inplace=True)
col = data_raw.pop('Order Type')
data_raw.insert(16, col.name, col)
data_raw.to_csv(r'.\snd\order_type.csv',encoding='utf-8', index=False)
#------------------------------------------------------------------------------

#populate rank
rank_file.drop_duplicates(inplace=True,subset=['Order Type'],keep='first')
data_raw = data_raw.merge(rank_file[['Order Type','Rank']], on='Order Type', how='left',validate='m:1')
data_raw['Rank'] = np.where(data_raw['Rank'].isnull(),99,data_raw['Rank'])
#-------------------------------------------------------------------------------
# populate range
class_offerpf = pd.read_csv(r'E:\_Projects\lookups\\class_offerpf_map.csv',encoding='utf-8', low_memory=False, keep_default_na='')
class_offerpf.rename(columns={'range': 'Range', 'class': 'CLASS'}, inplace=True)
data_raw['CLASS'] = data_raw['CLASS'].astype(str)
data_raw = data_raw.merge(class_offerpf[['CLASS','Range']],on='CLASS',how='left',validate='m:1')

#-----------------------------------------------------------------------------------------------------------
data_raw['Project Name'] = np.where(data_raw['Project Name']=='','None',data_raw['Project Name'])
#----------------------------------------------------------------------------------------------------------
#contry code 
data_raw['Country Code'] = ''
cond_6 = (~data_raw['Inventory Organization Code'].str.match('^EBS:[A-Z]{3}'))
cond_1 = (data_raw['Project Name']!='None')
cond_2 = (data_raw['Inventory Organization Code'].str.match('^EBS:[A-Z]{3}'))&(data_raw['Supply Chain Order Number'].str.contains('CEE'))& (data_raw['Project Name'].isin(['None']))
cond_3 = (data_raw['Inventory Organization Code'].str.match('^EBS:[A-Z]{3}'))&(data_raw['Supply Chain Order Number'].str.contains('HUM'))& (data_raw['Project Name'].isin(['None']))
cond_4 = (data_raw['Inventory Organization Code'].str.match('^EBS:[A-Z]{3}'))&(data_raw['Country Code']=='')
cond_5 = (data_raw['Supply Chain Order Number'].str.contains('.ORDER'))&(data_raw['Project Name'].isin(['None']))&(data_raw['Country Code']=='')&(~data_raw['Supply Chain Order Number'].str.contains('CEE.ORDER'))&(~data_raw['Supply Chain Order Number'].str.contains('HUM.ORDER'))&(~data_raw['Supply Chain Order Number'].str.contains('Standard.ORDER'))
data_raw['Country Code'] =  np.where(cond_1,data_raw['Project Name'].str.slice(-2),\
                            np.where(cond_5,data_raw['Supply Chain Order Number'].str.split('.ORDER').str[0].str[-2:],\
                            np.where(cond_2,'CEE',np.where(cond_3,'HUM',\
                            np.where(cond_6,data_raw['Inventory Organization Code'].str.slice(4,6),\
                            data_raw['Country Code'])))))


# data_raw.to_csv(r'.\snd\raw_data.csv',index=False)

#back_up
data = data_raw.copy()
ir_io = data_raw.copy()
#filter only for plan IR/IO-----------------------------------------------
ir_io = ir_io[ir_io['Order Type'].isin(['Plan IR/IO'])]
ir_io = ir_io[['Inventory Organization Code','Inventory Item Number Hyphenated','Country Code','Customer Name','Plan Quantity']]
ir_io = ir_io.groupby(['Inventory Organization Code','Inventory Item Number Hyphenated','Country Code','Customer Name'])['Plan Quantity'].sum().reset_index()
ir_io.sort_values(by=['Plan Quantity'], ascending = False,inplace = True)

cond_new = ir_io['Country Code'].isin(['']) & ir_io['Inventory Organization Code'].str.match('^EBS:[A-Z]{3}')
cond_new2 = (ir_io['Country Code'].isin([''])) &(~ir_io['Inventory Organization Code'].str.match('^EBS:[A-Z]{3}'))
ir_io['Country Code'] =  np.where(cond_new,ir_io['Inventory Organization Code'].str.slice(4,7),\
                                np.where(cond_new2,ir_io['Inventory Organization Code'].str.slice(4,6),ir_io['Country Code'] ))


ir_io['Country Code'] = np.where(ir_io['Country Code'].str.match('^V..'),ir_io['Country Code'].str[-2:],ir_io['Country Code'])
ir_io['Country Code'] = np.where(ir_io['Country Code'].isin(['GF']),'GF1',ir_io['Country Code'])
ir_io['Country Code'] = np.where(ir_io['Country Code'].isin(['UF']),'UF2',ir_io['Country Code'])
ir_io.drop_duplicates(inplace=True,subset=['Inventory Item Number Hyphenated'],keep='first')
ir_io.to_csv(r'.\snd\country_plan_irio.csv',encoding='utf-8', index=False)
#----------------------------------------------------------------------------------------------
#country code save in csv only for PBO and sales orders
data_raw = data_raw[data_raw['Order Type'].isin(['PBO','Sales Orders'])]
data_raw = data_raw[['Inventory Item Number Hyphenated','Country Code','Customer Name','Plan Quantity']]
data_raw = data_raw.groupby(['Inventory Item Number Hyphenated','Country Code','Customer Name'])['Plan Quantity'].sum().reset_index()
data_raw.sort_values(by=['Plan Quantity'], ascending = True,inplace = True)
data_raw.to_csv(r'.\snd\country_code.csv',encoding='utf-8', index=False)
#update_multiple_country--------------------------------------------------
# data_raw = data_raw.groupby(['Inventory Item Number Hyphenated','Country Code'])['Plan Quantity'].sum().reset_index()
# data_raw['Country Code'] = np.where(data_raw['Country Code'].isin(['GF']),'GF1',data_raw['Country Code'])
# data_raw['Country Code'] = np.where(data_raw['Country Code'].isin(['UF']),'UF2',data_raw['Country Code'])
# data_raw['Country_new'] = data_raw.groupby('Inventory Item Number Hyphenated')['Country Code'].transform(lambda x: ','.join(x))
# data_raw.sort_values(by=['Plan Quantity'], ascending = True,inplace = True)
# data_raw.drop_duplicates(inplace=True,subset=['Inventory Item Number Hyphenated'],keep='first')
# data_raw.to_csv(r'.\snd\country_code_multiple.csv',encoding='utf-8', index=False)

#--------------------------------------------------------
#updating country code once again
data.drop(columns={'Country Code'},inplace=True)
country = pd.read_csv(r'.\snd\country_code.csv',encoding='utf-8', low_memory=False, keep_default_na='')
country.rename(columns = {'Customer Name':'Customer'}, inplace = True)
country_BK = country.copy()
country.drop_duplicates(inplace=True,subset=['Inventory Item Number Hyphenated'],keep='first')
data = data.merge(country[['Inventory Item Number Hyphenated','Country Code','Customer']],on ='Inventory Item Number Hyphenated', how='left',validate='m:1')

#update plan IR/IO country 
ir_io.rename(columns={'Country Code': 'Country Code2'}, inplace=True)
data = data.merge(ir_io[['Inventory Item Number Hyphenated','Country Code2']],on ='Inventory Item Number Hyphenated', how='left',validate='m:1')
data['Country Code'] = np.where(data['Order Type'].isin(['Plan IR/IO']),data['Country Code2'],data['Country Code'])
data.drop(columns={'Country Code2'},inplace=True)



cond_new = (data['Country Code'].isin([''])|data['Country Code'].isnull()) & (data['Inventory Organization Code'].str.match('^EBS:[A-Z]{3}'))
cond_new2 = (data['Country Code'].isin([''])|data['Country Code'].isnull()) &(~data['Inventory Organization Code'].str.match('^EBS:[A-Z]{3}'))
data['Country Code'] = np.where(cond_new,data['Inventory Organization Code'].str.slice(4,7),\
                                np.where(cond_new2,data['Inventory Organization Code'].str.slice(4,6),data['Country Code'] ))




#populate blanks record assigning country
#data['Country Code'] = np.where(data['Country Code']=='',data['Inventory Organization Code'].str.split(':', expand=True)[1],data['Country Code'])
data['Country Code'] = np.where(data['Country Code'].str.match('^V..'),data['Country Code'].str[-2:],data['Country Code'])
data['Country Code'] = np.where(data['Country Code'].isin(['GF']),'GF1',data['Country Code'])
data['Country Code'] = np.where(data['Country Code'].isin(['UF']),'UF2',data['Country Code'])
#-----------------------------------------------------------------------------------------------------------------
#filter data remove mcc and other pid
data_raw_cond1 = (~data['Inventory Item Number Hyphenated'].str.contains('MCC')) &(data['Inventory Item Number Hyphenated'].str.match('^[0-9]{4}M'))&\
                 (~data['Inventory Item Number Hyphenated'].str.endswith('MC')) & (~data['Inventory Item Number Hyphenated'].str.endswith('M'))
data_raw_cond_2 = data['Inventory Item Number Hyphenated'].str.match('^[0-9]{4}-[C][0-9]{3}')
data_raw_cond_4 = data['Inventory Item Number Hyphenated'].str.match('^[0-9]{4}-') & (data['Inventory Item Number Hyphenated'].str.contains('K'))
data_raw_cond_3 = data['Inventory Item Number Hyphenated'].str.match('^[0-9]{4}-[0-9]{4}-[0-9]{4}$')
data = data[data_raw_cond1 | data_raw_cond_2 | data_raw_cond_3 | data_raw_cond_4]
#------------------------------------------------------------------------------------------------------------------

#item type
data['Item Type'] = ''
cond_k = data['Inventory Item Number Hyphenated'].str.match('^[0-9]{4}-K[0-9]{3}-V[0-9]{3}')
cond_c = data['Inventory Item Number Hyphenated'].str.match('^[0-9]{4}-C[0-9]{3}-[0-9]{4}')
cond_m = data['Inventory Item Number Hyphenated'].str.match('^[0-9]{4}M')
cond_u = data['Inventory Item Number Hyphenated'].str.match('^[0-9]{4}-[0-9]{4}-[0-9]{4}$')

data['Item Type'] = np.where(cond_k,'K',\
                        np.where(cond_c,'C',\
                        np.where(cond_m,'M',\
                        np.where(cond_u,'U',data['Item Type']))))

col = data.pop('Item Type')
data.insert(5, col.name, col)
#----------------------------------------------------------------------------------------------------------
#update region column
#rename column name

# data = data.merge(srcs[['Country Code','Region']], on='Country Code', how='left',validate='m:1')

region_file = pd.read_csv(r'E:\_Projects\lookups\country_zone_region.csv', encoding='utf-8')
region_file.rename(columns={'region':'Region','country':'Country Code'}, inplace=True)
data = data.merge(region_file[['Country Code','Region']], on='Country Code', how='left',validate='m:1')
#-----------------------------------------------------------------------------------------------------------
#remove unwanted string in inventory item number hyphenated

cond_number = data['Inventory Item Number Hyphenated'].str.contains('/')
data['Inventory Item Number Hyphenated'] = np.where(cond_number,data['Inventory Item Number Hyphenated'].str.split('/').str[0],\
                                                   data['Inventory Item Number Hyphenated'])

#-----------------------------------------------------------------------------------------------------------------
#drop quantity 0
data= data[~data['Plan Quantity'].isin([0])]
data.rename(columns={'CLASS':'Class'}, inplace=True)

#-----------------------------------------------------------------------------------------------------------------
#Main plant det ocp **************************
data['Plan Quantity'] = data['Plan Quantity'].astype(float)
# data['Suggested Due Date'] = data['Suggested Due Date'][:10]
# data['Suggested Due Date'] = pd.to_datetime(data['Suggested Due Date'])
# data['Schedule Arrival Date'] = data['Schedule Arrival Date'][:10]
# data['Schedule Arrival Date'] = pd.to_datetime(data['Schedule Arrival Date'],format='').dt.date
data['Inventory Item Number Hyphenated'] = data['Inventory Item Number Hyphenated'].astype(str)
data['Source Organization Code'] = data['Source Organization Code'].astype(str)
data['Source Supplier Site Code'] = data['Source Supplier Site Code'].astype(str)
data['Suggested Due Date'] = data['Suggested Due Date'].astype('datetime64[ns]')
# data['Schedule Arrival Date'] = data['Schedule Arrival Date'].astype('datetime64[ns]')
data['OrgCodeNoVirtOrg'] = data['Inventory Organization Code'].str.replace(':V', ':')

#--------------------------------------------------------------------------------------------------------------------
#populate customer column
# data['Customer'] = data['Customer Name']

#-------------------------------------------------------------------------------------------------------------------
filt_supplier_cond = data['Source Supplier Site Code'].apply(lambda x: len(x) < 5)
data['Source Supplier Site Code'] = np.where(filt_supplier_cond,data['Source Organization Code'],\
                                             data['Source Supplier Site Code'])
#-------------------------------------------------------------------------------------------------------------------
#populate month column 
data['Ship Mth'] = data['Suggested Due Date'].dt.strftime("%Y%m")

#-----------------------------------------------------------------------------------------------------------------
#assign null as N/A for pivot purpose
data['Range'] = np.where(data['Range'].isnull(),'N/A',data['Range'] )
data['Customer'] = np.where(data['Customer'].isnull(),'N/A',data['Customer'])
data['Country Code'] = np.where(data['Country Code']=='','N/A',data['Country Code'])
data['Region'] = np.where(data['Region'].isnull(),'N/A',data['Region'] )

data['Supply Chain Order Type Description']=np.where(data['Supply Chain Order Type Description']=='','None',data['Supply Chain Order Type Description'])
data['Source Organization Code'] =  np.where(data['Source Organization Code']=='','None',data['Source Organization Code'])
data['Order Type'] = np.where(data['Order Type'].isnull(),'N/A',data['Order Type'])

#-----------------------------------------------------------------------------------------------------------------------
print('Saving main plant det files in snd folder')
# data.to_csv(r'.\snd\Plant_det_main.csv',encoding='utf-8', index=False)
#-----------------------------------------------------------------------------------------------------------------------
#reassign order type
filt = data['Supply Chain Order Type Description'] == 'Forecast'
data['Rank'] = np.where(filt, '1', data['Rank'])
data['Order Type'] = np.where(filt, 'Forecast', data['Order Type'])


#*************
filt_purchreq = data['Supply Chain Order Type Description'].isin(['Transfer order'])
filt_erp = data['Source Organization Code'].str.startswith('EBS')
filt_ord = data['Supply Chain Order Type Description'].str.contains('Transfer order demand')
filt_req = filt_purchreq & filt_erp

data['Rank'] = np.where(filt_ord, '1', data['Rank'])
data['Rank'] = np.where(filt_req, '4', data['Rank'])



conds = [filt_ord, filt_req]
choices = ['Int Order', 'Int Req']
data['Order Type'] = np.select(condlist=conds, choicelist=choices, default=data['Order Type'])

#******************************
filt1 = data['Supply Chain Order Type Description'].str.upper() == 'PLANNED ORDER'
filt2 = data['Source Organization Code'].str.startswith('EBS')
filt3 = ~data['Source Organization Code'].str.contains(':V')
filt4 = ~data['Source Organization Code'].str.contains(':V')
filt = filt1 & filt2 & filt3
data['Rank'] = np.where(filt, '6', data['Rank'])
data['Order Type'] = np.where(filt, 'Plan IR/IO', data['Order Type'])
#***********************
filt1 = data['Supply Chain Order Type Description'].str.upper() == 'PLANNED ORDER'
filt2 = data['Source Organization Code'] == data['Inventory Organization Code']
filt3 = ~data['Source Organization Code'].str.contains(':V')
filt = filt1 & filt2 & filt3
data['Rank'] = np.where(filt, '3', data['Rank'])
data['Order Type'] = np.where(filt, 'Plan Make', data['Order Type'])


#------------------------------------------------------------------------------------------------------------------------------
#plant det main data  onnly for raw data
#sourcing rule file
# srcs_zone = pd.read_csv(r'\\dayorg1\orgshare\TEAMS\ERP Shared Folder\Global S&OP\Supply Plan\EDL\lookups\sourcing_rules.csv', encoding='utf-8')
# srcs_zone.rename(columns={'country':'Country','zone':'Zone'}, inplace=True)
# srcs_zone.drop_duplicates(inplace=True,subset=['Country'],keep='first')
#zone
data_bk  = data.copy()
data_bk.rename(columns={'Country Code':'Country','Ship Mth':'Ship Qtr'}, inplace=True)
# data_bk = data_bk.merge(srcs_zone[['Country','Zone']], on='Country', how='left',validate='m:1')
#----------------------------------------------------------------------------------------------------------------------------------------
#Exclude Class in raw data
class_excl = pd.read_csv(r'.\rcv\lookup\class_exclude.csv',encoding='utf-8', low_memory=False, keep_default_na='')
#class_excl = class_excl['Class'].to_list()
class_excl['Class'] = class_excl['Class'].astype(str)
data_bk['Class Exclued'] = np.where(data_bk['Class'].isin(class_excl['Class']),'Exclude','')

#-------------------------------------------------------------------------------------------------------------------------------------------
output_file_name_main = f'Global AMMOCP Det.csv'
data_bk.to_csv('.\snd\Plantdet\\'+output_file_name_main, index=False, encoding='utf-8')


# data.to_csv('.\snd\Plantdet\\'+output_file_name_main, index=False)
#------------------------------------------------------------------------------------------------------------------------------
#create multiple dataframe
excess_cntry = data_bk.copy()
data_item = data_bk.copy()
data_po = data_bk.copy()
data_make = data_bk.copy()
data_int_req = data_bk.copy()
data_chennai_dummy = data_bk.copy()
data_firmed_exces = data_bk.copy()
#Excess Country
# excess_cntry = excess_cntry[['Inventory Item Number Hyphenated','Inventory Organization Code','Action Description']]
# cond = excess_cntry['Action Description'].isin(['None - Firmed Excess']) & excess_cntry['Inventory Organization Code'].str.match('^EBS:[A-Z]{3}') & excess_cntry['Inventory Organization Code'].str.contains(':V')
# excess_cntry = excess_cntry[cond]
# excess_cntry['country_excess'] = excess_cntry['Inventory Organization Code'].str.slice(-2)
# excess_cntry.drop_duplicates(inplace=True,subset=['Inventory Item Number Hyphenated','country_excess'],keep='first')
# excess_cntry['country_excess'] = excess_cntry.groupby('Inventory Item Number Hyphenated')['country_excess'].transform(lambda x: ','.join(x))
# excess_cntry.drop_duplicates(inplace=True,subset=['Inventory Item Number Hyphenated'],keep='first')
# excess_cntry.to_csv(r'.\snd\country_excess.csv', index=False)

#item type
data_item = data_item[['Inventory Item Number Hyphenated','Item Type']]
data_item.drop_duplicates(inplace=True,subset=['Inventory Item Number Hyphenated'],keep='first')
data_item.to_csv(r'.\snd\lockup_item_type.csv', index=False)

#filter ex for chennai dummy
data_chennai_dummy = data_chennai_dummy[['Inventory Item Number Hyphenated','Source Supplier Site Code','Order Type']]
cond = data_chennai_dummy['Source Supplier Site Code'].isin(['CHENNAI-DUMMY']) & data_chennai_dummy['Order Type'].isin(['EX'])
data_chennai_dummy = data_chennai_dummy[cond]
data_chennai_dummy.drop_duplicates(inplace=True,subset=['Inventory Item Number Hyphenated'],keep='first')
# data_chennai_dummy.to_csv(r'.\snd\chennai_dummy.csv', index=False)

#filter for firmed excess data
data_firmed_exces = data_firmed_exces[['Inventory Item Number Hyphenated','Source Supplier Site Code','Action Description']]
cond = data_firmed_exces['Action Description'].isin(['None - Firmed Excess'])
data_firmed_exces = data_firmed_exces[cond]
data_firmed_exces.drop_duplicates(inplace=True,subset=['Inventory Item Number Hyphenated'],keep='first')
# data_firmed_exces.to_csv(r'.\snd\firmed_excess.csv', index=False)


#data_po
data_po= data_po[['Inventory Item Number Hyphenated','Source Supplier Site Code','Order Type','Action Description']]
data_po = data_po[data_po['Order Type'].isin(['Purchase order'])]
splr = ['EDI-BUDAPES-USD','EDI-CHI-201-USD','P6DEL-GUAD','P6-HOL-201-USD']
data_po =data_po[data_po['Source Supplier Site Code'].isin(splr)]

plant_new=['EC','JAB','USL','USL']
data_po['Plant_new'] =''
data_po['Splr_new'] =''
for i in range(len(splr)):
    data_po['Plant_new'] = np.where(data_po['Source Supplier Site Code'].isin([splr[i]]) & data_po['Plant_new'].isin(['']) ,plant_new[i],data_po['Plant_new'])
    data_po['Splr_new'] = np.where(data_po['Source Supplier Site Code'].isin([splr[i]]) & data_po['Splr_new'].isin(['']),splr[i],data_po['Splr_new'])

data_po.sort_values(by=['Inventory Item Number Hyphenated', 'Action Description'], ascending=[True, False],inplace = True)
data_po.drop_duplicates(inplace=True,subset=['Inventory Item Number Hyphenated'],keep='first')
data_po['Data_ref'] = 'PO'
# data_po.to_csv('.\snd\po_raw_data.csv', index=False)

#Plan Make Sour
data_make= data_make[['Inventory Item Number Hyphenated','Source Supplier Site Code','Order Type','Source Organization Code']]
data_make = data_make[data_make['Order Type'].isin(['Plan Make'])]
plant = ['EBS:CHE','EBS:CHS','EBS:USL']
data_make =data_make[data_make['Source Organization Code'].isin(plant)]

data_make['Plant_new'] =''
data_make['Splr_new'] =''
for i in range(len(plant)):
    data_make['Plant_new'] = np.where(data_make['Source Organization Code'].isin([plant[i]]) & data_make['Plant_new'].isin(['']) ,plant[i][4:7],data_make['Plant_new'])
    data_make['Splr_new'] = np.where(data_make['Source Organization Code'].isin([plant[i]]) & data_make['Splr_new'].isin(['']),plant[i],data_make['Splr_new'])
    
data_make.drop_duplicates(inplace=True,subset=['Inventory Item Number Hyphenated'],keep='first')
data_make['Data_ref'] = 'Plan make'
# data_make.to_csv('.\snd\pl_make_raw_data.csv', index=False)
#Internal Req
data_int_req= data_int_req[['Inventory Item Number Hyphenated','Source Supplier Site Code','Order Type','Source Organization Code']]
data_int_req = data_int_req[data_int_req['Order Type'].isin(['Int Req'])]
plant = ['EBS:GF1','EBS:USL']
data_int_req =data_int_req[data_int_req['Source Organization Code'].isin(plant)]

data_int_req['Plant_new'] =''
data_int_req['Splr_new'] =''
for i in range(len(plant)):
    data_int_req['Plant_new'] = np.where(data_int_req['Source Organization Code'].isin([plant[i]]) & data_int_req['Plant_new'].isin(['']) ,plant[i][4:7],data_int_req['Plant_new'])
    data_int_req['Splr_new'] = np.where(data_int_req['Source Organization Code'].isin([plant[i]]) & data_int_req['Splr_new'].isin(['']),plant[i],data_int_req['Splr_new'])
    
data_int_req.drop_duplicates(inplace=True,subset=['Inventory Item Number Hyphenated'],keep='first')
data_int_req['Data_ref'] = 'Int Req'
# data_int_req.to_csv('.\snd\int_req_raw_data.csv', index=False)

#concat all
data_con = pd.concat([data_po,data_make,data_int_req],axis=0)
data_con.to_csv(r'.\snd\conct_lookup.csv', index=False)

#-------------------------------------------------------------------------------------------------------------------------------
#Create plant_New, Suppil_new
cond_1 = data_bk['Order Type']

#----------------------------------------------------------------------------------------------------------------------------
# data['Inventory Item Number Hyphenated BK'] = data['Inventory Item Number Hyphenated']
data['Inventory Item Number Hyphenated'] = data['Inventory Item Number Hyphenated'].str.replace('-','')
data['Inventory Item Number Hyphenated'] = data['Inventory Item Number Hyphenated']+'-'+data['Project Name'] 
#-------------------------------------------------------------------------------------------------------------------------------
#remove supply chain order type description == forcast in pos
'''
conn_str = (r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            r'DBQ=\\susday1322\RACE\RACE_GLOBAL\RACE_LookUP_GLOBAL.accdb;')
sql = "SELECT DISTINCT [Class], [Family] FROM [Product Family Map] WHERE [Family] = 'POS'"
conn = pyodbc.connect(conn_str)
pos_record = pd.read_sql(sql, conn)

'''

pos_record = pd.read_csv(r'E:\_Projects\plandet\rcv\lookup\RACE_LookUP_GLOBAL_product_family_map.csv',encoding='utf-8', low_memory=False, keep_default_na='')
pos_record = pos_record[pos_record['Family']=='POS']
pos_record['Class'] = pos_record['Class'].astype(str)

filt_pos = (data['Class'].astype(str).isin(pos_record['Class'])) & (data['Supply Chain Order Type Description'].str.strip().str.upper() == 'FORECAST')
data = data[~filt_pos]




#--------------------------------------------------------------------------------------------------------------------------------
#populate rank name column

ranks = {
        '1': 'Demand',
        '2': 'OH/IT',
        '3': 'Make',
        '4': 'Buy',
        '5': 'Internal',
        '6': 'Plan',
        '99': 'Not assigned'
    }
# remove_zero= data['Rank'].str.contains('.')
# data['Rank'] =  np.where(remove_zero,data['Rank'].str.split('.').str[0],data['Rank'])
data['Rank'] = pd.to_numeric(data['Rank']).astype(int).astype(str)
data['RankName'] = data['Rank'].map(ranks)
# input('break.....')
#----------------------------------------------------------------------------------------------------------------------------
#createing view
# create a ship month for current month to 8 and 14 month 
#no of mnths pasing the params
params = [7,13]
for param in params:
    print(f'Creating {param} months view ...........')
    #createing view
    # create a ship month for current month to 8 month
    day = 1
    mnth = datetime.now().month
    #pre month-----
    pre_mnth = datetime.now()
    pre_mnth = pre_mnth - timedelta(days=pre_mnth.day)
    pre_mnth = pre_mnth.strftime('%Y%m')
    pre_mnth = str(pre_mnth)
    #--------
    yr = datetime.now().year
    number_of_months = param
    current_date = datetime(yr, mnth, day)
    ship_qtr_months = []
    ship_qtr_months.append(pre_mnth)
    for month_offset in range(number_of_months):
        mth_offset = relativedelta(months=month_offset)
        ship_qtr_months.append((current_date + mth_offset).strftime('%Y%m'))
    
   
    # print(pre_mnth)
    
    #filter ship month and rank names
    ranks_name = ['Buy', 'Demand', 'Make', 'OH/IT', 'Internal', 'Plan']
    
    filt_cond1 = data['Ship Mth'].isin(ship_qtr_months)
    filt_cond2 = data['RankName'].isin(ranks_name)
    final_con = filt_cond1 & filt_cond2
    data_filter = data[final_con]
    print(data_filter['Ship Mth'].unique())
    #filter output
    filter_output_name = f'main_filter_{param}.csv'
    # data_filter.to_csv(r'.\snd\\'+filter_output_name,encoding='utf-8', index=False)

   
    #-------------------------------------------------------------------------------
    #creating pivot view

    pt_index=['Range', 'Class', 'Inventory Item Number Hyphenated', 'Customer', 'Country Code', 'Region']
    pt_columns = ['RankName', 'Order Type']
    pt_columns_totals = ['RankName']

    #new dataframe in pivot view
    pv_sum = data_filter.pivot_table(index=pt_index,columns=pt_columns,values='Plan Quantity',aggfunc='sum').reset_index()
    pv_sum.columns = [f'{i}|{j}' if j != '' else f'{i}' for i, j in pv_sum.columns]
    # pv_sum.to_csv(r'.\snd\\pv_sum.csv',encoding='utf-8', index=False)
 

    #new dataframe in pivot view
    pv_cum = data_filter.pivot_table(index=pt_index,columns=pt_columns_totals,values='Plan Quantity',aggfunc='sum').reset_index()
    # pv_cum.to_csv(r'.\snd\\pv_cum.csv',encoding='utf-8', index=False)


    #merge two dataframe
    res = pv_sum.merge(pv_cum, how='left', validate='1:1', indicator=True)



    #---------------------------------------------------------------------------------------------------------
    #count the Inventory Item Number Hyphenated new
    mcid= res['Inventory Item Number Hyphenated']
    res['MCID Count'] =  mcid.map(Counter(mcid.tolist()))
    # res.to_csv(r'.\snd\\res.csv',encoding='utf-8', index=False)

 
    #----------------------------------------------------------------------------
    #adding missing column set in 0
    v_vars = ['Buy|Purchase order', 'Buy|Purchase req', 'Buy|Int Req',
          'Make|Flow/WIP', 'Make|Plan Make', 'OH/IT|Intransit shipment',
          'OH/IT|On Hand', 'Plan|Plan IR/IO', 'Demand|PBO', 'Demand|Sales Orders',
          'Demand|Forecast', 'Demand|Int Order']

    for c in v_vars:
        if c not in res.columns:
            res[c]=0
    print(f'Creating pivot for {param} months............')
    #-----------------------------------------------------------------------------------------------------------
    #melt fields
    print(f'melt pivot data for {param} months............')
    var_name = 'desc'
    val_name = 'qty'
    i_vars = ['Range', 'Class', 'Inventory Item Number Hyphenated', 'Customer', 'Country Code', 'Region']
    
    df_new = res.melt(id_vars=i_vars,value_vars=v_vars,var_name=var_name,value_name=val_name).fillna(0)
    # df_new.to_csv(r'.\snd\\melt.csv',encoding='utf-8', index=False)



    # df_new['qty'] = np.where(df_new['qty'].isnull(),0,df_new['qty'])
    #----------------------------------------------------------------------------------------------------------
    #allocate rows
    new_rows = []
    for row in df_new.itertuples(index=False):
        for i in range(int(abs(row[-1]))):
            tmp = list(row).copy()
            new_rows.append(tmp)

    mt_new = pd.DataFrame(new_rows, columns=df_new.columns)
    mt_new['qty'] = mt_new['qty'].apply(lambda x: 1 if x > 0 else -1)
    #--------------------------------------------------------------------------------------------------------
    mt_new['group'] = mt_new['desc'].str.split('|').apply(lambda x: x[0])
    mt_new['type'] = mt_new['desc'].str.split('|').apply(lambda x: x[1])
    # mt_new.to_csv(r'.\snd\\melt2.csv',encoding='utf-8', index=False)

    #-----------------------------------------------------------------------------------------------------
    filt_dm = mt_new['group'] == 'Demand'
    filt_io = ((mt_new['group'] == 'Internal') & (mt_new['type'] == 'Int Order'))

    #creating 2 datafram for demand and supply
    plan_dem = mt_new[filt_dm | filt_io]
    plant_sup = mt_new[~filt_dm & ~filt_io]
    # plan_dem.to_csv(r'.\snd\\dem.csv',encoding='utf-8', index=False)
    # plant_sup.to_csv(r'.\snd\\sup.csv',encoding='utf-8', index=False)


    #---------------------------------------------------------------------------------------------------------  
    rank_type = {
        'Plan IR/IO': 9,
        'Int Order': 8,
        'Int Req': 7,
        'Purchase order': 6,
        'Purchase req': 5,
        'Flow/WIP': 4,
        'Plan Make': 3,
        'Intransit shipment': 2,
        'On Hand': 1}

    plant_sup['type_rank'] = plant_sup['type'].map(rank_type)
    sort_fields = ['Range', 'Class', 'Inventory Item Number Hyphenated','Customer', 'Country Code', 'Region','type_rank']
    plant_sup = plant_sup.sort_values(by=sort_fields)
    #------------------------------------------------------------------------------------------------------
    #outer join demand and supply
    join_key = list(plant_sup.columns[:6])
    plan_dem = plan_dem.join(plan_dem.groupby(join_key)['qty'].cumsum(), how='left', lsuffix='_x', rsuffix='_y')
    plan_dem['qty_y'] = abs(plan_dem['qty_y'])
    plant_sup = plant_sup.join(plant_sup.groupby(join_key)['qty'].cumsum(),how='left', lsuffix='_x', rsuffix='_y')
    join_key.append('qty_y')

    # plan_dem.to_csv(r'.\snd\\plant_dem.csv',encoding='utf-8', index=False)
    # plant_sup.to_csv(r'.\snd\\plant_sup.csv',encoding='utf-8', index=False)
    


    df = plant_sup.merge(plan_dem, how='outer', on=join_key,validate='m:1', indicator=True)
    # df.drop(columns=['type_rank_y'], inplace=True) #SIVA

    # df.to_csv(r'.\snd\\df.csv',encoding='utf-8', index=False)
    #---------------------------------------------------------------------------------------------------

    filt = df['desc_x'] == 'Plan|Plan IR/IO'
    calc_y_planirio = df['qty_x_x'].fillna(0) + 0
    calc_n_planirio = df['qty_x_x'].fillna(0) + df['qty_x_y'].fillna(0)
    df['qty'] = np.where(filt, calc_y_planirio, calc_n_planirio)
    has_demand = df[['Inventory Item Number Hyphenated','group_y']].dropna().drop_duplicates()
    df = df.merge(has_demand, how='left', on=['Inventory Item Number Hyphenated'], validate='m:1')
   
    #----------------------------------------------------------------------------------------------------
    is_zd_buy = (df['group_y_y'].isna()) & (df['group_x'] == 'Buy')
    is_zd_make = (df['group_y_y'].isna()) & (df['group_x'] == 'Make')
    is_zd_ohit = (df['group_y_y'].isna()) & (df['group_x'] == 'OH/IT')
    is_ex_buy = (~df['group_y_y'].isna()) & (df['group_x'] == 'Buy') & (df['qty'] > 0)
    is_ex_make = (~df['group_y_y'].isna()) & (df['group_x'] == 'Make') & (df['qty'] > 0)
    is_ex_ohit = (~df['group_y_y'].isna()) & (df['group_x'] == 'OH/IT') & (df['qty'] > 0)
    is_clean = (df['qty'] == 0)
    is_ex = (df['qty'] < 0)
    conds = [is_clean, is_ex, is_ex_buy, is_ex_make,is_ex_ohit, is_zd_buy, is_zd_make, is_zd_ohit]
    choice = ['clean', 'ex', 'excess_buy', 'excess_make', 'excess_ohit','zd_excess_buy', 'zd_excess_make', 'zd_excess_ohit']
    df['status'] = np.select(condlist=conds, choicelist=choice)

    # need to identfiy plan irio separately
    filt = (df['type_x'] == 'Plan IR/IO') & (df['status'] == '0')
    df['status'] = np.where(filt, 'ex', df['status'])

    # df.to_csv(r'.\snd\\df2.csv',encoding='utf-8', index=False)
    #------------------------------------------------------------------------------------------------------
    s_columns = ['Range', 'Class', 'Inventory Item Number Hyphenated',
                 'Customer', 'Country Code', 'Region','desc_x', 'group_x',
                 'type_x', 'status', 'qty_x_x', 'qty']
    d_columns = ['Range', 'Class', 'Inventory Item Number Hyphenated',
                 'Customer', 'Country Code', 'Region', 'desc_y', 'group_y_x',
                 'type_y', 'status', 'qty_x_y', 'qty']

    pd_demand = df[df['group_y_x'] == 'Demand'][d_columns]
    pd_supply = df[~df['group_x'].isna()][s_columns]

    s_cols = {'desc_x': 'Description',
              'group_x': 'Category',
              'type_x': 'Order Type',
              'qty_x_x': 'Qty',
              'qty': 'Check Qty'}

    d_cols = {'desc_y': 'Description',
              'group_y_x': 'Category',
              'type_y': 'Order Type',
              'qty_x_y': 'Qty',
              'qty': 'Check Qty'}

    pd_demand.rename(columns=d_cols, inplace=True)
    pd_supply.rename(columns=s_cols, inplace=True)

    pd_final = pd.concat([pd_demand, pd_supply])
    # pd_final.to_csv(r'.\snd\\pd_final.csv',encoding='utf-8', index=False)
    #---------------------------------------------------------------------------------------------
    #adjust final qty
    filt = (pd_final['Category'] != 'Demand') & (pd_final['status'] == 'clean')
    pd_final['Final Qty'] = np.where(filt, 0, abs(pd_final['Qty']))
    pd_final = pd_final.groupby(list(pd_final.columns[:10])).sum().reset_index()
    
    #--------------------------------------------------------------------------------
    #re call main data
    df_cols = ['Inventory Item Number Hyphenated', 'Source Supplier Site Code']
    filt = data_filter['Source Supplier Site Code'].str.len() > 0
    filt_by_order_type = data_filter['Order Type'].str.upper().isin(['EX', 'PURCHASE ORDER', 'PURCHASE REQ', 'PLAN IR/IO'])
    filt = filt & filt_by_order_type

    supplier = data_filter[df_cols][filt]

    mcids = supplier['Inventory Item Number Hyphenated'].copy()
    supplier['Inventory Item Number Hyphenated'] = [x.split('-')[0] for x in mcids]
    supplier.drop_duplicates(subset=['Inventory Item Number Hyphenated'], inplace=True)
    #-----------------------------------------------------------------------------------------------------------
    #mcid Tagging

    # MCId Tag master
    # RACE_PATH_TAG = r'\\susday1322\RACE\RACE_GLOBAL'
    # tags = pd.read_excel(RACE_PATH_TAG + '\\' + 'MCID_Tagging _Master.xlsx', engine='openpyxl')
    RACE_PATH_TAG = r'E:\_Projects\lookups'
    tags = pd.read_excel(RACE_PATH_TAG + '\\' + 'MCID_Tagging _Master.xlsx', engine='openpyxl')
    
    pd_final['MCID'] = [x.split('-')[0]for x in pd_final['Inventory Item Number Hyphenated']]
    pd_final = pd_final.merge(tags[['MCID', 'Tag Name']].drop_duplicates(subset=['MCID']),how='left', on='MCID', validate='m:1')
    pd_final.rename(columns={'Tag Name': 'MCID Tagging'}, inplace=True)

    #-----------------------------------------------------------------------------------------
    status = {
        'clean': 'clean',
        'ex': 'ex',
        'excess_buy': 'excess_buy',
        'excess_make': 'excess_make',
        'excess_ohit': 'excess_ohit',
        'zd_excess_buy': 'excess_buy',
        'zd_excess_make': 'excess_make',
        'zd_excess_ohit': 'excess_ohit'
    }

    # load preferred offers mapping table
    # mcid_tagging_path = r"\\Dayorg1\ORGSHARE\TEAMS\ERP Shared Folder\Global S&OP\Daily Order Cover - Global\preferred offers\preferred_offer.csv"
    mcid_tagging_path = r"E:\_Projects\lookups\preferred_offer.csv"
    preferred_offers= pd.read_csv(mcid_tagging_path ,encoding='utf-8', low_memory=False, keep_default_na='' ) 
    preferred_offers = preferred_offers[~preferred_offers['Type'].str.startswith('CATALOGUE')]
    filt1 = pd_final['MCID'].isin(preferred_offers['PID'])
    pd_final['PreferredOffer'] = ''
    pd_final['PreferredOffer'] = np.where(filt1, 'Y', 'N')
   
    pd_final['status_group'] = pd_final['status'].map(status)

    # split project name off mcid and tidy up
    mcids = pd_final['Inventory Item Number Hyphenated'].copy()
    pd_final['Inventory Item Number Hyphenated'] = [x.split('-')[0] for x in mcids]
    pd_final['Project Name'] = [x.split('-')[1] if len(x.split('-')) > 1 else '' for x in mcids]
    pd_final = pd_final.merge(supplier, how='left',on=['Inventory Item Number Hyphenated'], validate='m:1')
    cols = {'Source Supplier Site Code': 'Supplier','Class_x': 'Class'}
    pd_final.rename(columns=cols, inplace=True)
    #-----------------------------------------------------------------------------------------
    filt1 = pd_final['Order Type'].isin(['Int Req', 'Int Order'])
    filt2 = pd_final['status'] == 'clean'
    cond_final = filt1 & filt2
    pd_final['Final Qty'] =  np.where(cond_final,0,pd_final['Final Qty'])
    pd_final.rename(columns={'Country Code':'Country'},inplace=True)
    #--------------------------------------------------------------------------------------
    #update zone and plant
    #sourcing rule file
    srcs = pd.read_csv(r'E:\_Projects\lookups\sourcing_rules.csv', encoding='utf-8')
    # srcs = pd.read_csv(r'\\dayorg1\orgshare\TEAMS\ERP Shared Folder\Global S&OP\Supply Plan\EDL\lookups\sourcing_rules.csv', encoding='utf-8')
    srcs.rename(columns={'country':'Country','zone':'Zone','plant':'Plant'}, inplace=True)
    srcs_bk = srcs.copy()
    srcs.drop_duplicates(inplace=True,subset=['Country'],keep='first')
    #zone
    pd_final = pd_final.merge(srcs[['Country','Zone']], on='Country', how='left',validate='m:1')

    #plant
    pd_final['class country']= pd_final['Class']+pd_final['Country']
    pd_final['class region']= pd_final['Class']+pd_final['Region']
    #back for update null plant
    srcs_bk.rename(columns={'Country Code':'Country'},inplace=True)
    src_bk1 = srcs_bk.copy()
    src_bk1['Plant1'] = src_bk1['Plant']
    #remove duplicate
    srcs_bk.drop_duplicates(inplace=True,subset=['class country'],keep='first')
    pd_final = pd_final.merge(srcs_bk[['class country','Plant']], on='class country', how='left',validate='m:1')

    cond_plant = pd_final['Plant'].isnull()
    src_bk1.drop_duplicates(inplace=True,subset=['class region'],keep='first')
    pd_final = pd_final.merge(src_bk1[['class region','Plant1']], on='class region', how='left',validate='m:1')
    pd_final['Plant'] = np.where(cond_plant,pd_final['Plant1'],pd_final['Plant'])
    pd_final.drop(columns={'Plant1','class country','class region'},inplace=True)
    #---------------------------------------------------------------------------------------------

    plan_irio = ['plan_irio' for i in range(pd_final.shape[0])]
    pd_final['status'] = np.where(pd_final['Order Type'] == 'Plan IR/IO', 'plan_irio', pd_final['status'])
    #---------------------------------------------------------------------
    #add hyphens
    pd_final['Inventory Item Number Hyphenated'] = pd_final['Inventory Item Number Hyphenated'].apply(lambda x: '-'.join([x[i:i+4] for i in range(0, len(x), 4)]) if len(x) == 12 else x)
    #------------------------------------------------------------------------
    cond_che = (pd_final['Supplier'] == 'EDI-CHENNAI') & (pd_final['Plant'].isna())
    cond_jab = (pd_final['Supplier'] == 'EDI-CHI-201-USD') & (pd_final['Plant'].isna())
    cond_man = (pd_final['Supplier'] == 'EDI-MANAUS') & (pd_final['Plant'].isna())
    cond_gf1 = (pd_final['Supplier'] == 'EDI-BUDAPES-USD') & (pd_final['Plant'].isna())
    cond_all = pd_final['Plant'].notna()

    pd_final['Plant'] = np.where(cond_che,'CHE',pd_final['Plant'])  
    pd_final['Plant'] = np.where(cond_jab,'JAB',pd_final['Plant'])
    pd_final['Plant'] = np.where(cond_man,'MAN',pd_final['Plant'])
    pd_final['Plant'] = np.where(cond_gf1,'GF1',pd_final['Plant'])
    pd_final['Supplier'] = pd_final['Supplier'].fillna('Unknown')
    pd_final['MCID Tagging'] = pd_final['MCID Tagging'].fillna('None')
    pd_final['Plant'] = pd_final['Plant'].fillna('None')
    pd_final['Zone'] = pd_final['Zone'].fillna('None')

    #merging plant_new supplier new country new
    #country code
    # country_new = pd.read_csv(r'.\snd\country_code_multiple.csv',encoding='utf-8', low_memory=False, keep_default_na='')
    # pd_final = pd_final.merge(country_new[['Inventory Item Number Hyphenated','Country_new']],on ='Inventory Item Number Hyphenated', how='left',validate='m:1')
    # pd_final['Country_new'] = np.where(pd_final['Country_new'].isnull(),pd_final['Country'],pd_final['Country_new'])
    #Excess Country
    # excess_cntry = pd.read_csv(r'.\snd\country_excess.csv',encoding='utf-8', low_memory=False, keep_default_na='')
    # pd_final = pd_final.merge(excess_cntry[['Inventory Item Number Hyphenated','country_excess']],on ='Inventory Item Number Hyphenated', how='left',validate='m:1')
    # pd_final['country_excess'] = np.where(pd_final['country_excess'].isnull(),pd_final['Country'],pd_final['country_excess'])
    #supplier new and plant new
    new_data = pd.read_csv(r'.\snd\conct_lookup.csv',encoding='utf-8', low_memory=False, keep_default_na='')
    new_data.drop_duplicates(inplace=True,subset=['Inventory Item Number Hyphenated'],keep='first')
    pd_final = pd_final.merge(new_data[['Inventory Item Number Hyphenated','Plant_new','Splr_new','Data_ref']],on ='Inventory Item Number Hyphenated', how='left',validate='m:1')
    pd_final['Plant_new'] = np.where(pd_final['Plant_new'].isnull(),pd_final['Plant'],pd_final['Plant_new'])
    pd_final['Splr_new'] = np.where(pd_final['Splr_new'].isnull(),pd_final['Supplier'],pd_final['Splr_new'])
    #re-update supplier new
    # data_chennai_dummy = pd.read_csv(r'.\snd\chennai_dummy.csv',encoding='utf-8', low_memory=False, keep_default_na='')
    # data_firmed_exces = pd.read_csv(r'.\snd\firmed_excess.csv',encoding='utf-8', low_memory=False, keep_default_na='')
    data_firmed_exces.rename(columns={'Source Supplier Site Code':'Source Supplier Site Code upd'}, inplace=True)
    pd_final = pd_final.merge(data_firmed_exces[['Inventory Item Number Hyphenated','Source Supplier Site Code upd']],on ='Inventory Item Number Hyphenated', how='left',validate='m:1')
    pd_final['Splr_new'] = np.where(pd_final['Inventory Item Number Hyphenated'].isin(data_chennai_dummy['Inventory Item Number Hyphenated']),'CHENNAI-DUMMY',pd_final['Splr_new'])
    pd_final['Splr_new'] = np.where(pd_final['Inventory Item Number Hyphenated'].isin(data_firmed_exces['Inventory Item Number Hyphenated'])& pd_final['status_group'].isin(['excess_ohit','excess_buy','excess_make']),pd_final['Source Supplier Site Code upd'],pd_final['Splr_new'])
    pd_final.drop(columns={'Source Supplier Site Code upd'},inplace=True)
    #item type update
    item_type = pd.read_csv(r'.\snd\lockup_item_type.csv',encoding='utf-8', low_memory=False, keep_default_na='')
    pd_final = pd_final.merge(item_type[['Inventory Item Number Hyphenated','Item Type']],on ='Inventory Item Number Hyphenated', how='left',validate='m:1')
    # populate range
    class_offerpf = pd.read_csv(r'E:\_Projects\lookups\\class_offerpf_map.csv',encoding='utf-8', low_memory=False, keep_default_na='')
    # class_offerpf = pd.read_csv(r'\\dayorg1\orgshare\TEAMS\ERP Shared Folder\Global S&OP\Supply Plan\EDL\lookups\\class_offerpf_map.csv',encoding='utf-8', low_memory=False, keep_default_na='')
    class_offerpf.rename(columns={'prod_grp_wb': 'Range_grp', 'class': 'Class'}, inplace=True)
    pd_final = pd_final.merge(class_offerpf[['Class','Range_grp']],on='Class',how='left',validate='m:1')
    #apply blanks as unkown
    #pd_final['Country_new'] = pd_final['Country_new'].fillna('Unknown')
    #pd_final['country_excess'] = pd_final['country_excess'].fillna('Unknown')
    pd_final['Plant_new'] = pd_final['Plant_new'].fillna('Unknown')
    pd_final['Splr_new'] = pd_final['Splr_new'].fillna('Unknown')
    pd_final['Item Type'] = pd_final['Item Type'].fillna('Unknown')
    pd_final['Range_grp'] = pd_final['Range_grp'].fillna('N/A')
    #global_ammocp_det_24_feb_23_8mths_fcst_wo_org
    output_file_name_first = f'global_{param}mths.csv'
    pd_final.to_csv('.\snd\Plantdet\\'+output_file_name_first, index=False,encoding='utf-8')

    #---------------------------------------------------------------------------------------------------------

    #re assing pivot 
    pt_index = ['Range','Range_grp','Class', 'Inventory Item Number Hyphenated',
                'Region', 'Country','Customer', 'status', 'Supplier','Splr_new',
                'Project Name','PreferredOffer', 'status_group',
                'MCID Tagging', 'Zone', 'Plant','Plant_new','Item Type']

    sum_columns = ['Demand|Forecast', 'Demand|Int Order', 'Demand|PBO', 'Demand|Sales Orders',
                'OH/IT|Intransit shipment', 'OH/IT|On Hand',
                'Make|Flow/WIP', 'Make|Plan Make',
                'Buy|Int Req', 'Buy|Purchase order', 'Buy|Purchase req']

    sum_demand = ['Demand|Forecast', 'Demand|Int Order','Demand|PBO', 'Demand|Sales Orders']

    columns = pt_index + ['Qty', 'Demand|Forecast', 'Demand|Int Order',
                          'Demand|PBO', 'Demand|Sales Orders',
                          'OH/IT|Intransit shipment', 'OH/IT|On Hand',
                          'Make|Flow/WIP', 'Make|Plan Make',
                          'Buy|Int Req', 'Buy|Purchase order',
                          'Buy|Purchase req', 'Plan|Plan IR/IO']


    pt_columns = 'Description'
    pt_values = 'Qty'

    pv_new = pd_final.pivot_table(index=pt_index, columns=pt_columns,values=pt_values, aggfunc='sum',).reset_index()

    for c in columns:
        if c not in pv_new.columns:
            pv_new[c] = 0

    filt_clean = pv_new['status'] == 'clean'  
    filt_other = pv_new['Plan|Plan IR/IO'].fillna(0) == 0
    filt_irio = (pv_new['status'] == 'ex') & (pv_new['Plan|Plan IR/IO'].fillna(0) != 0)

    conds = [filt_clean, filt_other, filt_irio]
    choices = [abs(pv_new[sum_demand].sum(axis=1)), abs(
    pv_new[sum_columns].sum(axis=1)), pv_new['Plan|Plan IR/IO']]
    pv_new['Qty'] = np.select(condlist=conds, choicelist=choices)
    pv_new = pv_new[columns]
    pv_new['Class Exclued'] = np.where(pv_new['Class'].isin(class_excl['Class']),'Exclude','')
    print('Finishing view ')
    print(f'Saving {param} months files.....')
    #pv_global_ammocp_det_21_feb_23_14mths_fcst_wo_org
    #saving file
    output_file_name = f'pv_{param}mths.csv'
    pv_new.to_csv('.\snd\Plantdet\\'+output_file_name, index=False, encoding='utf-8')





