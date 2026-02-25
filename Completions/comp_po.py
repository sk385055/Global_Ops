import pandas as pd
import numpy as np 
from datetime import datetime
dt = datetime.now()
date_str = dt.strftime("%Y%m%d")

def consol_data(req_col_po,comp_data,sco_head):
    req_col_po.remove('qty')
    comp_data = comp_data.groupby(req_col_po)['qty'].sum().reset_index()
    #comp_data['ship_date'] = comp_data['ship_date'].astype('datetime64[ns]')
    #Sco head---------------------------
    comp_data['item'] = comp_data['item'].astype(str)
    comp_data = comp_data.merge(sco_head[['item','sco_head']], on='item', how='left',validate='m:1')
    #------------------------------------------
    #remove PO .
    comp_data['PO'] = comp_data['PO'].astype(str)
    comp_data['PO'] = np.where(comp_data['PO'].str.contains('.'),comp_data['PO'].str.split('.',expand=True)[0],comp_data['PO'])
    # comp_data['PO'] = comp_data['PO'].str.split('.')[0]
    #--------------------------------------
    comp_data.to_csv(r'E:\_Projects\_outputs\completion\Completion PO' + '\\completion_po_'+date_str+'.csv',encoding='utf-8', index=False)
    #comp_data.to_csv(r'\\Dayorg1\ORGSHARE\TEAMS\ERP Shared Folder\Global S&OP\workbench\shipments' + '\\completion_po_'+date_str+'.csv',encoding='utf-8', index=False)



