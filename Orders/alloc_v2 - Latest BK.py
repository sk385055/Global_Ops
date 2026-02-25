from ast import arg
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import sys
import warnings
warnings.filterwarnings("ignore")


d = datetime.now().date()

def allocation(site,ocp_name,date_str):
    #inputs--------------------------------------------------------------------------->
    file_wb = r'workbench_data_'+ocp_name+'_'+date_str+'_v2.csv'
    #file_wb = r'workbench_data_eumocp_20240415_v2.csv'
    if site =='enn':
        file='enn'
        file_en = ['openpo_enn_v2_'+date_str+'.csv']
        
    elif site =='jab':
        file='jab'
#         file_en = ['openpo_jab_atm_v2_'+date_str+'.csv', 'openpo_jab_sco_v2_'+date_str+'.csv']
        file_en = ['openpo_jab_atm_v2_'+date_str+'.csv']
    elif site =='usi':
        file='usi'
        file_en = ['openpo_usi_v2_'+date_str+'.csv']
    file_out = d.strftime('%Y%m%d')
    sort_wb_by = ['PID', 'Plant Ship date']
    merge_wb_by = ['PID']
    sort_en_by = ['PID_', 'Ship Dt_']
    merge_en_by = ['PID_']
    #------------------------------------------------------------------------------------------------------------------>end
    #create empty dataframe
    data_en = pd.DataFrame()
    #conact two dataframe
    for fil in file_en:
         data_tmp = pd.read_csv(os.path.join(r'E:\_Projects\_outputs\orders\snd', fil),encoding='utf-8', keep_default_na='', low_memory=False)
         data_en = pd.concat([data_en, data_tmp], axis=0)
    #read ocp data
    data_wb = pd.read_csv(os.path.join(r'E:\_Projects\_outputs\orders\snd', file_wb),encoding='utf-8', keep_default_na='', low_memory=False)
    cols_en = ['File','SO','Booked Dt','PID','Qty','PO','Ship Dt','PD','Request Dt','Tot Qty','Org PD','Critical','Revenue', 'Priority','Comments-1','Comments-2','Customer','Prod Cate']
    data_en = data_en[cols_en]
    #add suffix in input file
    data_en = data_en.add_suffix('_')
    cols_en = list(data_en.columns)
    cols_wb = list(data_wb.columns)
    # filter required records/columns and formatting WB
    # use to get PID Desc
    data_wb_all = data_wb.copy()
    if file=='enn': 
        splr='ENNOCONN HUNGARY KFT'
    elif file=='jab':
        splr='JABIL CIRCUIT INC - CHIHUAHUA'
    else:
        splr='UNIVERSAL SCIENTIFIC INDUSTRIAL DE MEXICO S.A. DE C.V'
    # splr='ENNOCONN HUNGARY KFT' if file=='enn' else 'JABIL CIRCUIT INC - CHIHUAHUA'
    # splr=['ENNOCONN HUNGARY KFT','JABIL CIRCUIT INC - CHIHUAHUA']
    data_wb = data_wb.loc[(data_wb['Supplier'].str.contains(splr)) & (~data_wb['Demand'].str.contains('PO-'))  & ~(data_wb['Validation Flag']=='Plan')]
    # format input fields
    data_wb['Plant Ship date'] = pd.to_datetime(data_wb['Plant Ship date'], format='%Y-%m-%d')
    # data_wb['Plant Ship date'] = pd.to_datetime(data_wb['Plant Ship date'], format='%d-%m-%Y')
    condtn1 = data_wb['Supplier'].str.contains(splr)
    data_wb['Supply Detail'] = np.where(condtn1 , data_wb['Supply Detail'].str.split('(').str[0], data_wb['Supply Detail'])
    data_wb['Supply Detail'] = data_wb['Supply Detail'].astype(str)
    # formatting ENN
    data_en['Ship Dt_'] = pd.to_datetime(data_en['Ship Dt_'], format='%Y-%m-%d')
    data_en['PO_'] = data_en['PO_'].astype(str)
    # Sorting the datasets for allocation
    data_wb = data_wb.sort_values(by=sort_wb_by, ascending=True).reset_index(drop=True)
    data_en = data_en.sort_values(by=sort_en_by, ascending=True).reset_index(drop=True)
    # Creating multiple rows by qty
    data_wb = data_wb.loc[data_wb.index.repeat(data_wb['Supply Qty'])].reset_index(drop=True)   
    data_wb['Supply Qty']=1
    data_en = data_en.loc[data_en.index.repeat(data_en['Qty_'])].reset_index(drop=True)
    data_en['Qty_']=1  
    # Create ID field for every merge-fields change
    data_wb['id1'] = data_wb.groupby(merge_wb_by).cumcount()
    data_en['id0'] = data_en.groupby(merge_en_by).cumcount()
    # Merge the two datasets
    out = pd.merge(data_en, data_wb, left_on=merge_en_by + ['id0'], right_on=merge_wb_by + ['id1'], how='left')
    out.to_csv(r'E:\_Projects\_outputs\orders\snd\wb_test.csv', index=False) #hari commented
    #               suffixes=('','_drop')).filter(regex='^(?!.*_drop)')
    # Preparing field list
    out.drop(columns={'id0','id1'}, inplace=True)
    cols_out  = cols_en + cols_wb
    cols_grp = cols_out.copy()
    for x in (['Supply Qty','Qty_']):
        cols_grp.remove(x)
    # Group the output and sum the Qty
    # out = out.groupby(cols_grp, dropna=False).agg({'Supply Qty':'sum', 'Qty_':'sum'}).reset_index()
    out = out.groupby(cols_grp, dropna=False)['Supply Qty','Qty_'].sum().reset_index().copy()
    #--------------------------------------------------------------------------------------------------------------
    def upd_period(df, fld, fld_wk, fld_mon, fld_qtr):
            df[fld_wk] = pd.to_datetime(df[fld], errors='coerce').dt.isocalendar().week.astype(pd.Int64Dtype())
            df[fld_mon] = pd.to_datetime(df[fld], errors='coerce').dt.strftime('%Y%m')
            df[fld_qtr] = pd.to_datetime(df[fld], errors='coerce').dt.strftime('%Y') + 'Q' + pd.to_datetime(df[fld], errors='coerce').dt.quarter.astype(pd.Int64Dtype()).astype(str)
    posi = cols_out.index('Ship Dt_')
    for fld in ['Ship Wk_', 'Ship Mon_', 'Ship Qtr_']:
        cols_out.insert((posi+1), fld)
        out[fld]=''
    upd_period(out, 'Ship Dt_', 'Ship Wk_', 'Ship Mon_', 'Ship Qtr_')
    out['today'] = d
    out['Ship Mon_'] = np.where(out['Ship Dt_']<= out['today'], 'Past', out['Ship Mon_'])
    out['Ship Qtr_'] = np.where(out['Ship Dt_']<= out['today'], 'Past', out['Ship Qtr_'])
    
    out.drop(columns={'today'}, inplace=True) 
    #----------------------------------------------------------------------------------------------------
    # Populate WB fields on not allocated rows
    # Class, PID, PID Type  
    out['Class'] = out['PID_'].str[:4]
    out['PID'] = out['PID_']
    out['PID Type'] = out['PID Type'].astype(str)

    out['Prod Cate'] = out['Prod Cate_']
    # cols_out.remove(['Prod Cate_'])

    out['PID Type']=''
    condtn1 = ( (out['PID Type']=='') | (out['PID Type'].isnull()) )
    condtn2m = ((out['PID_'].str.contains('M')) & (~out['PID_'].str.contains('MCC')))
    condtn2k = (out['PID_'].str.contains('K'))   
    condtn2c = (out['PID_'].str.contains('C'))
    out['PID Type'] = np.where(condtn1 & condtn2k, 'K', out['PID Type'])
    out['PID Type'] = np.where(condtn1 & condtn2c, 'C', out['PID Type'])
    out['PID Type'] = np.where(condtn1 & condtn2m, 'M', out['PID Type'])  
    #-----------------------------------------------------------------------------------------------------
    # Supply Qty, Qty, Plant
    out['Supply Qty'] = np.where((out['Supply Qty'] == 0 | out['Supply Qty'].isnull()), out['Qty_'], out['Supply Qty'])
    out['Qty'] = out['Supply Qty']
    condtn = (file=='enn')
    if (file=='enn'):
        #out['Plant'] = np.where(out['Plant'].isnull(), 'GF1', out['Plant'])
        out['Plant'] = 'BUD'
    elif ((file=='jab-atm') | (file=='jab-sco')):
        #out['Plant'] = np.where(out['Plant'].isnull(), 'JAB', out['Plant'])
        out['Plant'] ='JAB'
    elif (file=='usi'):
        out['Plant']='USI'
    # Plant SSD, Wk, Mon, Qtr
    # out['Plant SSD'] = pd.to_datetime(out['Plant SSD'], errors='coerce')
    # out['Plant SSD'] = np.where(out['Plant SSD'].isnull(), out['Ship Dt_'], out['Plant SSD'])
    # out['Plant SSD Wk'] = np.where(out['Plant SSD Wk'].isnull(), out['Ship Wk_'], out['Plant SSD Wk'])
    # out['Plant SSD Mon'] = np.where(out['Plant SSD Mon'].isnull(), out['Ship Mon_'], out['Plant SSD Mon'])
    # out['Plant SSD Qtr'] = np.where(out['Plant SSD Qtr'].isnull(), out['Ship Qtr_'], out['Plant SSD Qtr'])
    # if (file == 'jab'):

    out['Plant SSD'] = out['Ship Dt_']   
    

    out['Today'] = pd.to_datetime('today').date()
    out['Plant SSD'] = np.where(pd.to_datetime(out['Plant SSD']) < pd.to_datetime(out['Today']), \
                             out['Today'].astype('datetime64[ns]'), out['Plant SSD'])
    
    upd_period(out, 'Plant SSD', 'Plant SSD Wk', 'Plant SSD Mon', 'Plant SSD Qtr')
    out.drop(columns={'Today'}, axis=1, inplace=True)
    # Products, Offer PF
    map_prod = pd.read_csv(r'E:\_Projects\lookups\class_offerpf_map.csv', encoding='utf8', low_memory=False)
    map_prod.drop_duplicates(['class'], inplace=True)
    out = pd.merge(out, map_prod[['class','prod_grp_wb','offer_pf_wb']], left_on='Class', right_on='class', how='left').reset_index()
    out['Products'] = out['prod_grp_wb']
    out['Offer PF'] = out['offer_pf_wb']
    out.drop(columns={'class','prod_grp_wb','offer_pf_wb'}, inplace=True)
    #---------------------------------------------------------------------------------------------
    # PID Desc
    data_wb_all = data_wb_all[['PID','PID Desc']].copy()
    data_wb_all.drop_duplicates(['PID'], inplace=True)
    data_wb_all.rename(columns={'PID':'PID_upd', 'PID Desc':'PID Desc_upd'}, inplace=True)
    out = pd.merge(out, data_wb_all, left_on=['PID'], right_on=['PID_upd'], how='left')
    out['PID Desc'] = np.where(~out['PID Desc_upd'].isnull(), out['PID Desc_upd'], out['PID Desc'])
    out.drop(columns={'PID_upd', 'PID Desc_upd'}, inplace=True)
    #-----------------------------------------------------------------------------------------------
    # Tag Name
    map_tag = pd.read_excel(r'E:\_Projects\lookups\\MCID_Tagging _Master.xlsx', sheet_name='MCID_Tagging__Master_TABLE' , engine='openpyxl')
    map_tag.rename(columns={'Tag Name':'Tag Name_upd'}, inplace=True)
    out = pd.merge(out, map_tag[['MCID','Tag Name_upd']], left_on='PID', right_on='MCID', how='left')
    out['Tag Name'] =  np.where(out['Tag Name'].isnull(),out['Tag Name_upd'], out['Tag Name'])
    out.drop(columns={'MCID','Tag Name_upd'}, axis=1, inplace=True)   
    out['PID Desc'] = np.where((out['PID Type']=='M') & out['Tag Name'].notnull(), out['Tag Name'], out['PID Desc'])
    #--------------------------------------------------------------------------------------------------------------------------
    # Key Account
    map_key = pd.read_excel(r'E:\_Projects\lookups\\7.3_Country-Key_Account_Heirarchy.xlsx', engine='openpyxl')
    map_key.drop_duplicates(['Country Code','Master Customer Number'], inplace=True)
    map_key.rename(columns={'Master Customer Number':'Master Customer Number_upd', 'Key Account':'Key Account_upd'}, inplace=True)
    out = pd.merge(out, map_key[['Country Code','Master Customer Number_upd','Key Account_upd']], left_on=['Org Code','Master Customer Number'],right_on=['Country Code','Master Customer Number_upd'], how='left')
    out['Key Account'] = out['Key Account_upd']
    out.drop(columns={'Master Customer Number_upd','Key Account_upd'}, inplace=True)
    #-----------------------------------------------------------------------------------------------------------------------------
    # Preferred Offer
    pref_offer = pd.read_csv(r'E:\_Projects\lookups\\preferred_offer.csv', encoding='utf-8', low_memory=False)
    pref_offer = pref_offer[['PID', 'Type']].copy()
    pref_offer.drop_duplicates(['PID','Type'], inplace=True)
    pref_offer.rename(columns={'PID':'PID_upd', 'Type':'Type_upd'}, inplace=True)
    out = pd.merge(out, pref_offer, left_on='PID', right_on='PID_upd', how='left')
    out['Preferred_Offer'] = out['Type_upd']
    out.drop(columns={'PID_upd','Type_upd'}, axis=1, inplace=True)
    # Populate UID for all unmapped PO's
    out['UID'] = np.where(out['UID'].isnull(), out['File_'] + out['PO_'] + out['PID_'], out['UID'])
    #--------------------------------------------------------------------------------------------------------
    # export the CSV
    cols_out.remove('Prod Cate_')
    # out[cols_out].to_csv('wb_' + file + '_allocation_v2_' + file_out + '.csv', index=False)
    #sri -  supply type blanks change to po
    out['Supply Type'] = np.where(out['Supply Type'].isnull(),'PO',out['Supply Type'])
    set_date_frmt = ['Booked Dt','Request Dt','PD_Order','Ship Date','SAD','IO BOOKED DATE','IO SSD','IO SAD','PD_PO','CRSD','PRD','Plant Ship date','NEED BY DATE','Plant SSD']
    for field in set_date_frmt:
        out[field] = pd.to_datetime(out[field])
    
    out = out[cols_out]
    out = out.replace('\n',';', regex=True)
    #out[cols_out].to_csv(r'\\dayorg1\orgshare\TEAMS\ERP Shared Folder\Global S&OP\workbench\allocation\wb_' + file + '_allocation_' + date_str + '.csv', index=False)
    out.to_csv(r'E:\_Projects\workbench\snd\wb_' + file + '_allocation_' + date_str + '.zip', compression={'method': 'zip', 'archive_name': 'workbench_po.csv'},index=False)
    out.to_csv(r'E:\_Projects\_outputs\orders\snd\wb_' + file + '_allocation_' + date_str + '.csv', index=False)
    #out.to_csv(r'E:\Sri\eumocp compare\allocation\wb_' + file + '_allocation_' + date_str + 'new.csv', index=False)   
    return out

    




if __name__ == "__main__":

    site = sys.argv[1]
    file_name = sys.argv[2]
    date_str = sys.argv[3]
    # print(site)
    # print(file_name)
    # print(date_str)
    # input parameters 
    data_all = pd.DataFrame() 
    site_list = []
    if site.upper() == 'ALL':
        site_list = ['enn','jab','usi']
    else:
        site_list.append(site)

    for v_site in site_list:
        print('site_name: '+v_site+'; data: '+file_name+'_'+date_str)
        #print(v_site)
        out = allocation(v_site,file_name,date_str)
        data_all = pd.concat([data_all,out],axis=0)
    
    if site.upper() =='ALL':
        #data_all.to_csv(r'E:\_Projects\orders\test\wb_all_allocation_' + date_str + '.csv', index=False)
        data_all.to_csv(r'E:\_Projects\_outputs\orders\snd\wb_all_allocation_' + date_str + '.zip', compression={'method': 'zip', 'archive_name': 'workbench_po.csv'},index=False)
    



        

    











