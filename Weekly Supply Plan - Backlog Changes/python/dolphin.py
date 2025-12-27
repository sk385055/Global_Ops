import pandas as pd
import numpy as np
import datetime

import os
import glob


def run_dolphin_creation():
    rcv = r'C:\database\order_cover\dolphin\snd'
    # rcv = r'C:\Users\ja185222\OneDrive - NCR Corporation\Siva ETL_Inputs\Jabil'

    # rcv = os.path.join(os.path.join(os.environ['USERPROFILE']), 'OneDrive - NCR Corporation\ETL_Inputs\Jabil') 
    print('Onedrive:  ' + rcv)
    
    wrk = r'C:\database\order_cover\dolphin\wrk'
    snd = r'\\Dayorg1\ORGSHARE\TEAMS\ERP Shared Folder\Global S&OP\Daily Order Cover - Global\dolphin'

    # 1. Get ATM and SCO dolphin data
    # f-strings don't work in GLOB?!
    # * means all if need specific format then *.csv
    atm_xlsx = glob.glob(rcv+'\*atm*.xlsx')
    # * means all if need specific format then *.csv
    sco_xlsx = glob.glob(rcv+'\*sco*.xlsx')

    # 2. Parse ATM dolphin data
    xlsx = []
    # [-3:]
    for xl in atm_xlsx[-3:]:
        print(xl)
        xlsx.append([xl.split('\\')[-1].split('.')[0], pd.ExcelFile(xl)])

    atms = [xl[1].parse('Estimated Ship Dates') for xl in xlsx]
    filename = [xl[0] for xl in xlsx]

    for df, fileref in zip(atms, filename):
        df.columns = [str(x).upper() for x in df.columns.tolist()]
        column_names = []
        counter = 0
        for c in df.columns:  # brute force a fix for duplicate column names!!
            if c in column_names:
                c = c + '_' + str(counter)
                counter += 1
            column_names.append(c)
        df.columns = column_names
        df.rename(columns={'BUILD WEEK': 'BUILD WK'}, inplace=True)
        df['fileref'] = fileref

    # atms_com = pd.read_excel(f'{wrk}\\dolphin_atm_combined.xlsx')
    # atms.append(atms_com)
    print(pd.concat(atms, sort=False).shape)
    atms_out = pd.concat(atms, sort=False)
    atms_out = atms_out.drop_duplicates()
    print(atms_out.shape)

    atms_out.to_excel(f'{wrk}\\dolphin_atm_combined.xlsx', index=False)
    atms_out.to_excel(f'{snd}\\dolphin_atm_combined.xlsx', index=False)

    # 3. Parse SSCO dolphin data
    xlsx = []
    # [-3:]
    for xl in sco_xlsx[-3:]:
        print(xl)
        xlsx.append([xl.split('\\')[-1].split('.')[0], pd.ExcelFile(xl)])

    sheets = []
    for xl in xlsx:
        sheetname = ''
        for i, v in enumerate(xl[1].sheet_names):
            if 'SO List' in v or 'Estimated Ship Dates' in v:
                sheetname = v
        sheets.append(sheetname)

    scos = [xl[1].parse(s) for s, xl in zip(sheets, xlsx)]
    filename = [xl[0] for xl in xlsx]

    for df, fileref in zip(scos, filename):
        df.columns = [str(x).upper() for x in df.columns.tolist()]
        # latest version of SCO files has build week name as BUILD WK
        if datetime.datetime.now() >= datetime.datetime.strptime('20200615', "%Y%m%d"):
            df.rename(columns={'BUILD WK': 'BUILD WEEK',
                               'SO - ID': 'SO'}, inplace=True)
        df['fileref'] = fileref

    for i, sco in enumerate(scos):
        if any(sco.columns.duplicated()):
            scos[i] = pd.merge(
                scos[i].iloc[:, :-3], scos[i].iloc[:, -2:], left_index=True, right_index=True)

    # scos_com = pd.read_excel(f'{wrk}\\dolphin_sco_combined.xlsx')
    # scos.append(scos_com)
    try:
        scos_out = pd.concat(scos, sort=False)
        scos_out = scos_out.drop_duplicates()
    except ValueError:
        print('Check for duplicate columns in the headers')

    scos_out.to_excel(f'{wrk}\\dolphin_sco_combined.xlsx', index=False)
    scos_out.to_excel(f'{snd}\\dolphin_sco_combined.xlsx', index=False)

    # 4. Create formatted view for combined ATM and SSCO

    scos_out.rename(columns={'BUILD WEEK': 'BUILD WK',
                             'SO CREATE': 'PO RECEIPT DATE', 'SO - ID': 'SO'}, inplace=True)

    # ssco_1 = scos_out[scos_out['fileref'].str[-8:].astype('datetime64') < '2020-06-17'][['CLASS', 'MC', 'QTY', 'BUILD WK', 'fileref']]
    ssco_2 = scos_out[scos_out['fileref'].str[-8:].astype('datetime64') >= '2020-06-17'][[
        'CLASS', 'MC', 'QTY TO SHIP', 'PO RECEIPT DATE', 'BUILD WK', 'fileref']]

    ssco_2.rename(columns={'QTY TO SHIP': 'QTY'}, inplace=True)

    # scos_out = pd.concat([ssco_1, ssco_2], sort=False)
    scos_out = ssco_2

    atms_out[['CLASS', 'MC', 'QTY', 'PO RECEIPT DATE',  'BUILD WK', 'fileref']]
    scos_out[['CLASS', 'MC', 'QTY', 'PO RECEIPT DATE', 'BUILD WK', 'fileref']]

    print(atms_out[['CLASS', 'MC', 'QTY', 'PO RECEIPT DATE',
                    'BUILD WK', 'fileref']].head())
    print(scos_out[['CLASS', 'MC', 'QTY', 'PO RECEIPT DATE',
                    'BUILD WK', 'fileref']].head())

    scos_out['CLASS'] = scos_out['MC'].str[:4]
    dolphin_out = pd.concat([atms_out[['CLASS', 'MC', 'QTY', 'PO RECEIPT DATE',  'BUILD WK', 'fileref']], scos_out[[
                            'CLASS', 'MC', 'QTY', 'PO RECEIPT DATE',  'BUILD WK', 'fileref']]], sort=False)

    dolphin_out.to_excel(f'{wrk}\\dolphin_combined.xlsx', index=False)
    dolphin_out.to_excel(f'{snd}\\dolphin_combined.xlsx', index=False)


if __name__ == '__main__':
    run_dolphin_creation()
