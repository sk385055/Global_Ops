
import pandas as pd
import numpy as np
from datetime import datetime
#import cx_Oracle
# from oracle import insert_data
from comp_po import consol_data
from pq_refersh import refersh_pivot

#\\Dayorg1\ORGSHARE\TEAMS\ERP Shared Folder\Global S&OP\Supply Plan\EDL\completions
#filename for colmap of completion and doc -----------
col_map_file_name ='completions_col_map'
doc_col_map_file_name = 'doc_col_map'
wsp_col_map_file_name = 'wsp_col_map'
#backup completion previous file----------------------
# pre_comp = pd.read_csv(r'.\snd' + '\\completion.csv',encoding='utf-8', low_memory=False, keep_default_na='')
# pre_comp.to_csv(r'.\snd' + '\\pre_completion.csv',encoding='utf-8', index=False)
# pre_comp.to_csv(r'\\Dayorg1\ORGSHARE\TEAMS\ERP Shared Folder\Global S&OP\workbench\completions' + '\\pre_completion.csv',encoding='utf-8', index=False)
# del pre_comp
#---------------------------------------------------------> 
#read completion historical file for all plant and lookup file in rcv folder 
col_map_file = pd.read_excel(r'.\rcv\lookup\\'+ col_map_file_name + '.xlsx', sheet_name='Sheet1', engine='openpyxl')
doc_col_map_file = pd.read_excel(r'.\rcv\lookup\\'+ doc_col_map_file_name + '.xlsx', sheet_name='Sheet1', engine='openpyxl')
wsp_col_map_file = pd.read_excel(r'.\rcv\lookup\\'+ wsp_col_map_file_name + '.xlsx', sheet_name='Sheet1', engine='openpyxl')
gut_lob = pd.read_csv(r'E:\_Projects\lookups\\gut_completions_lob_map.csv',encoding='utf-8', low_memory=False, keep_default_na='')
sco_head = pd.read_csv(r'E:\_Projects\lookups\\sco_heads.csv',encoding='utf-8', low_memory=False, keep_default_na='')
sco_head['item'] = sco_head['item'].astype(str)

#reading completion lookup file
jab = pd.read_csv(r'E:\_Projects\_outputs\completion\\completions_jab_hist.csv',encoding='utf-8', low_memory=False, keep_default_na='')
jab['PO']= jab['PurchaseOrder']
enn = pd.read_csv(r'E:\_Projects\_outputs\completion\\completions_enn_hist.csv',encoding='utf-8', low_memory=False, keep_default_na='')
enn['PO']= enn['PO / Order No']
usi = pd.read_csv(r'E:\_Projects\_outputs\completion\\completions_usi_hist.csv',encoding='utf-8', low_memory=False, keep_default_na='')
zeb = pd.read_csv(r'E:\_Projects\_outputs\completion\\completions_zeb_hist.csv',encoding='utf-8', low_memory=False, keep_default_na='')
zeb['PO']= zeb['Purchase Order Number']
gut = pd.read_csv(r'E:\_Projects\_outputs\completion\\completions_gut_hist.csv',encoding='utf-8', low_memory=False, keep_default_na='')
gut['PO']= ''
manual = pd.read_csv(r'.\map\\completions_man.csv',encoding='utf-8', low_memory=False, keep_default_na='')
# manual['ship_date'] = pd.to_datetime(manual['ship_date']).dt.date
manual['ship_date'] = manual['ship_date'].astype('datetime64[ns]')
manual_BK = manual.copy()
manual['PO']= ''
manual.drop(columns={'reason','req_by','req_date'},inplace=True)   
#consolidate completion and filter columns------------------------------------------------------------------------------------>start
req_col = list(col_map_file.columns)
req_col_po  = ['PO']+req_col
#-------------------------------------
data = pd.concat([jab[req_col_po],enn[req_col_po],usi[req_col_po],gut[req_col_po],zeb[req_col_po],manual[req_col_po]],axis=0)
#for save with po 
consol_data(req_col_po,data,sco_head)
#------------------------------------------------------------------
data.drop(columns={'PO'},inplace=True)
req_col.remove('qty')
data = data.groupby(req_col)['qty'].sum().reset_index()
data['ship_date'] = data['ship_date'].astype('datetime64[ns]')
#------------------------------------------------------------------------
#for sco head
data['item'] = data['item'].astype(str)
data = data.merge(sco_head[['item','sco_head']], on='item', how='left',validate='m:1')

# change class 2012 to 2021

data.loc[data['class']=='2012','class']='2021'
#----------------------------------------------------------------------------
data.to_csv(r'E:\_Projects\_outputs\completion' + '\\completion.csv',encoding='utf-8', index=False)
#data.to_csv(r'\\Dayorg1\ORGSHARE\TEAMS\ERP Shared Folder\Global S&OP\workbench\completions' + '\\completion.csv',encoding='utf-8', index=False)
data.drop(columns={'sco_head'},inplace=True)
#------------------------------------------------------------------------------------------------------------------------------>end
#refersh completion pivots
refersh_pivot()
#--------------------------------------------------------------------------------
#rename columns completion data to doc ----------------------------------------------------------------------------------------->start
# make a copy of completion
data_doc = data.copy()
data_doc = data_doc.rename(columns={"file_ref": "fileref","site":"org","ship_date_mth":"ssd_mth","ship_date_wk":"ssd_wk","ship_date_qtr":"ssd_qtr","prod_grp_wb":"Line Of Business","range":"Product Range","offer_pf_wb":"LOB New"})
#lookup and populated sco_head columns
data_doc['item'] = data_doc['item'].astype(str)
sco_head['item'] = sco_head['item'].astype(str)
data_doc = data_doc.merge(sco_head[['item','sco_head']], on='item', how='left',validate='m:1')
#lookup and populated LineOfBusiness columns
data_doc['class'] = data_doc['class'].astype(str)
gut_lob['class'] = gut_lob['class'].astype(str)
data_doc = data_doc.merge(gut_lob[['class','LineOfBusiness']], on='class', how='left',validate='m:1')
data_doc['fyear'] = data_doc['ssd_qtr'].str.slice(0,4)
data_doc['fquarter'] = data_doc['ssd_qtr'].str.slice(5,6)
data_doc['ssd_mth'] = data_doc['ssd_mth'].astype(str)
data_doc['fmonth'] = data_doc['ssd_mth'].str.slice(4,6)
data_doc['fweek'] = data_doc['ssd_wk']
#data_doc['class'] = np.where(data_doc['ss21'].isin(['Y']) & data_doc['class'].isin(['2012']),'2021',data_doc['class'])
doc_req_col = list(doc_col_map_file.columns)
data_doc = data_doc[doc_req_col]
#filter only M in itemtype
data_doc = data_doc[data_doc['item_type'].isin(['M'])]
data_doc.to_csv(r'E:\_Projects\_outputs\completion' + '\\completions_doc_test.csv',encoding='utf-8', index=False)
#data_doc.to_csv(r'\\Dayorg1\orgshare\TEAMS\ERP Shared Folder\Global S&OP\Supply Plan\EDL\completions\New' + '\\completions_doc_test.csv',encoding='utf-8', index=False)
#-------------------------------------------------------------------------------------------------------------------------------->end

#rename columns completions data to wsp------------------------------------------------------------------------------------------>start
# make a copy of completion
data_wsp = data.copy()
data_wsp = data_wsp.rename(columns={"file_ref": "fileref","site":"source","ship_date":"ssd","ship_date_mth":"ssd_mth","ship_date_wk":"ssd_wk","ship_date_qtr":"ssd_qtr","ss21":"IsSS21","dom_exp":"Dom/Export","qty":"quantity"})
wsp_req_col = list(wsp_col_map_file.columns)
data_wsp['serial_number']=''
#data_wsp['class'] = np.where(data_wsp['IsSS21'].isin(['Y']) & data_wsp['class'].isin(['2012']),'2021',data_wsp['class'])





#change file ref name
data_wsp['fileref']= np.where(data_wsp['source'].isin(['BUD']),'ENC',data_wsp['fileref'])
data_wsp['fileref']= np.where(data_wsp['source'].isin(['JAB']),'jab_for_daily_order_cover',data_wsp['fileref'])
data_wsp['fileref']= np.where(data_wsp['source'].isin(['ZEB']),'DART_AssemblyCompletions_CurQtr',data_wsp['fileref'])
data_wsp['fileref']= np.where(data_wsp['source'].isin(['USI']),'usi_for_daily_order_cover',data_wsp['fileref'])
data_wsp['fileref']= np.where(data_wsp['source'].isin(['USL','CHE','CHS']),'GUT',data_wsp['fileref'])
data_wsp = data_wsp[wsp_req_col]
data_wsp.to_csv(r'E:\_Projects\_outputs\completion' + '\\completions_wsp.csv',encoding='utf-8', index=False)
data_wsp.to_csv(r'E:\_Projects\Production Status' + '\\completions_wsp.csv',encoding='utf-8', index=False)


#data_wsp.to_csv(r'\\Dayorg1\orgshare\TEAMS\ERP Shared Folder\Global S&OP\Supply Plan\EDL\completions\New' + '\\completions_wsp.csv',encoding='utf-8', index=False)
#------------------------------------------------------------------------------------------------------------------------------->end
#manual_BK.to_csv(r'\\Dayorg1\ORGSHARE\TEAMS\ERP Shared Folder\Global S&OP\workbench\completions' + '\\completions_man.csv',encoding='utf-8', index=False)
#insert record in database------------------------------------------------------------------------------------------------------>start
#insert_data()#call oracle.py file
#------------------------------------------------------------------------------------------------------------------------------->end
import win32com.client
import time

## Completion refresh
xl = win32com.client.DispatchEx("Excel.Application")
xl.Visible = True
xl.DisplayAlerts = False
wb = xl.workbooks.open(r'E:\_Projects\Production Status\completions_v4.xlsx')
    #refersh all pivots

wb.RefreshAll()
    #wait for refersh complete
xl.CalculateUntilAsyncQueriesDone()
wb.Save()
wb.Close()
xl.Quit()

