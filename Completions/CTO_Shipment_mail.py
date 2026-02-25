import pandas as pd
import os
from email import policy
from email.parser import BytesParser
import shutil

#filename for cto shipments
csv_file_name = 'CTO_shipement.csv'
 
def read_file(cto_file_name,file_out_path,mail_in_path):
    read_cto =  pd.read_csv(os.path.join(file_out_path,csv_file_name),encoding='utf-8', low_memory=False, keep_default_na='')
   
    with open(os.path.join(mail_in_path,cto_file_name), 'rb') as fp:  
        msg = BytesParser(policy=policy.default).parse(fp)
    body = msg.get_body(preferencelist=('html')).get_content()
    fp.close()
    # read_cto['LastUpdated']= pd.to_datetime(read_cto['LastUpdated'])
    read_cto['LastUpdated']= pd.to_datetime(read_cto['LastUpdated'],format='mixed')
    df = pd.read_html(body)
    #print(df)
    #-------------------------------------------
    #for Last updated date
    #date = df[1]
    date = df[0]
    
    date = date[date[0].isin(['Shipment Date'])]
    #date = date[date[0].isin(['Shipment Date'])]
    
    date_list =date[1].tolist()
    #date_list =date[0].tolist()
    
    date_list =str(date_list[0])
    #print('---------------- date List ----------------- \n ')
    #print(date_list)
    #-------------------------------------------
    df = df[3]
    #df = df[4]
    #print(df)
    df['Qty']=1
    df['LastUpdated'] = date_list
    df['LastUpdated']= pd.to_datetime(df['LastUpdated'],format='%b %d %Y %H:%M%p')
    df.rename(columns={'Class/Model': 'Class','MC/PartNumber':'MC','PO':'ProductionOrder'}, inplace=True)
    df = pd.concat([read_cto,df],axis=0)
    df.drop(columns={'ID'},inplace=True)
    
    df.drop_duplicates(inplace=True,subset=['SerialNumber'],keep='last')
    df.to_csv(os.path.join(file_out_path,csv_file_name),encoding='utf-8', index=False)
    

def copy_mail(cto_file_name,mail_in_path,mail_out_path):
    src = os.path.join(mail_in_path,cto_file_name)
    dst = os.path.join(mail_out_path,cto_file_name)
    shutil.copy(src,dst)
    


def remove_mail(cto_file_name,mail_in_path):
    os.remove(os.path.join(mail_in_path,cto_file_name))


        
   



    
            
    
        
   

    




    
                 

            


