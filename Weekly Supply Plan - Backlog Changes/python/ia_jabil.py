import pandas as pd
import datetime
import os
import shutil

# 1. establish the output columns required
# 2. get combined dolphin files 'NEED TO KNOW HOW THE FILES CREATED
# 3. update column naming
# 4. create date priority columns
# 5. filter for latest file
# 6. generate output
out_columns = ['SAP Material', 'MCID', 'Sales Doc.', 'LINE ITEM', 'ORIGINAL QTY', 'Created On', 'ORIGNAL REQUEST DLV.DATE', 'SHIPPED WEEK', 'Shipped Qty', 'Shipped date2', 'Shipped Qty',
               'Delivery', 'DELTA QTY TO SHIP', 'DELTA QTY TO BUILD', 'Prioridad', 'Date Priority', 'Date Priority2', 'BUILD YEAR', 'BUILD WEEK', 'PO\'s', 'IS THIS CANCEL?', 'Order Status']
out_columns = ['SAP Material', 'MCID', 'Sales Doc.', 'LINE ITEM', 'ORIGINAL QTY', 'Created On', 'ORIGNAL REQUEST DLV.DATE', 'SHIPPED WEEK', 'Shipped Qty', 'Shipped date2', 'Shipped Qty',
               'Delivery', 'DELTA QTY TO SHIP', 'DELTA QTY TO BUILD', 'Prioridad', 'Date Priority', 'Date Priority2', 'BUILD YEAR', 'BUILD WEEK', 'PO\'s', 'IS THIS CANCEL?', 'Order Status', 'PO RECEIPT DATE']


new_columns = ['Date Priority', 'Date Priority2']

dolphin_columns = ['CLASS', 'MC', 'SO', 'QTY', 'Date Priority',
                   'Date Priority2', 'PO', 'PO RECEIPT DATE', 'fileref']
dolphin_columns_sco = ['CLASS', 'MC', 'SO', 'QTY TO SHIP',
                       'Date Priority', 'Date Priority2', 'PO', 'PO RECEIPT DATE', 'fileref']

# ATM dolphin format changed after the 15th June
if datetime.datetime.now() > datetime.datetime.strptime('20200615', '%Y%m%d'):
    dolphin_columns_atm = ['CLASS', 'MC', 'SO', 'QTY', 'PO RECEIPT DATE',
                           'Date Priority', 'Date Priority2', 'PO', 'fileref']
else:
    dolphin_columns_atm = ['CLASS', 'MC', 'SO', 'DELTA', 'PO RECEIPT DATE',
                           'Date Priority', 'Date Priority2', 'PO', 'fileref']

dolphin_columns_rename_atm = ['SAP Material', 'MCID', 'Sales Doc.', 'DELTA QTY TO SHIP',
                              'PO RECEIPT DATE', 'Date Priority', 'Date Priority2', 'PO\'s', 'fileref']
dolphin_columns_rename_sco = ['SAP Material', 'MCID', 'Sales Doc.', 'DELTA QTY TO SHIP',
                              'Date Priority', 'Date Priority2', 'PO\'s', 'PO RECEIPT DATE', 'fileref']

#dolphin_path = r'C:\Users\CP185176\OneDrive - NCR Corporation\project\dolphin\wrk\dolphin_combined.xlsx'
#dolphin_atm_path = r'C:\Users\CP185176\OneDrive - NCR Corporation\project\dolphin\wrk\dolphin_atm_combined.xlsx'
#dolphin_sco_path = r'C:\Users\CP185176\OneDrive - NCR Corporation\project\dolphin\wrk\dolphin_sco_combined.xlsx'

#dolphin_path_out = r'C:\database\order_cover\dolphin\wrk\dolphin_combined.xlsx'
#dolphin_atm_path_out = r'C:\database\order_cover\dolphin\wrk\dolphin_atm_combined.xlsx'
#dolphin_sco_path_out = r'C:\database\order_cover\dolphin\wrk\dolphin_sco_combined.xlsx'
dolphin_path_out = r'\\Dayorg1\ORGSHARE\TEAMS\ERP Shared Folder\Global S&OP\Daily Order Cover - Global\dolphin\dolphin_combined.xlsx'
dolphin_atm_path_out = r'\\Dayorg1\ORGSHARE\TEAMS\ERP Shared Folder\Global S&OP\Daily Order Cover - Global\dolphin\dolphin_atm_combined.xlsx'
dolphin_sco_path_out = r'\\Dayorg1\ORGSHARE\TEAMS\ERP Shared Folder\Global S&OP\Daily Order Cover - Global\dolphin\dolphin_sco_combined.xlsx'
#--try:
#--    shutil.copyfile(dolphin_path, dolphin_path_out)
#    shutil.copyfile(dolphin_atm_path, dolphin_atm_path_out)
#    shutil.copyfile(dolphin_sco_path, dolphin_sco_path_out)
#except:
#    print('Error could not copy dolphin files to shared folder!')

# dac = dolphin_atm_combined
# dsc = dolphin_sco_combined

dac = pd.read_excel(dolphin_atm_path_out)
dsc = pd.read_excel(dolphin_sco_path_out)

if datetime.datetime.now() >= datetime.datetime.strptime('20210215', '%Y%m%d'):
    dac['Date Priority'] = dac['PD']
    dsc['Date Priority'] = dsc['SHIP DATE'].replace('TBC', '')
else:
    dac['Date Priority'] = dac['JABIL EXPEDITE DATE']
    dsc['Date Priority'] = dsc['JABIL EXPEDITE DATE']

dac['Date Priority2'] = dac['Date Priority']
dsc['Date Priority2'] = dsc['Date Priority']

# re-format Sales Doc. column to only include the first part of the SO column
dac['SO'] = [str(so).split('-')[0] for so in dac['SO']]
dsc['SO'] = [str(so).split('-')[0] for so in dsc['SO']]

# filter columns and rows
# 1. identify the latest fileref and filter data

print(dsc['fileref'].sort_values().drop_duplicates())
current_atm_fref = dac['fileref'].sort_values().drop_duplicates().iloc[-1]
current_sco_fref = dsc['fileref'].sort_values().drop_duplicates().iloc[-1]

print(current_atm_fref, current_sco_fref)

dac_filtered = dac[dolphin_columns_atm][dac['fileref'] == current_atm_fref]
# ATM dolphin format changed after the 15th June
if datetime.datetime.now() <= datetime.datetime.strptime('20200615', '%Y%m%d'):
    dac_filtered['DELTA'] = dac_filtered['DELTA'] * -1
    dac_filtered.rename(columns={'DELTA': 'QTY'}, inplace=True)

dsc.rename(columns={'SO CREATE': 'PO RECEIPT DATE'}, inplace=True)
dsc_filtered = dsc[dolphin_columns_sco][dsc['fileref'] == current_sco_fref]
dsc_filtered.rename(columns={'QTY TO SHIP': 'QTY'}, inplace=True)

dac_filtered.columns = dolphin_columns_rename_atm
dsc_filtered.columns = dolphin_columns_rename_sco

# align column names

for column in out_columns:
    if column not in dolphin_columns_rename_atm:
        dac_filtered[column] = ''
        dac_filtered[column] = ''

ia_jabil = pd.concat([dac_filtered, dsc_filtered], sort=False)

# add dates to Shipped Date2 to get formatting for database
ia_jabil['Shipped date2'] = datetime.datetime.now()
ia_jabil['PO\'s'] = ia_jabil['PO\'s'].fillna('N/A')
ia_jabil_out = ia_jabil[out_columns]
ia_jabil_out = ia_jabil_out.dropna(subset=['SAP Material']).copy()
ia_jabil_out.loc[:, 'SAP Material'] = [
    str(x)[:4] for x in ia_jabil_out['MCID']]
ia_jabil_out = ia_jabil_out[ia_jabil_out['DELTA QTY TO SHIP'] != 0].dropna(subset=[
                                                                           'SAP Material'])

ia_jabil_out['Date Priority2'] = ia_jabil_out['Date Priority']
ia_jabil_out['Date Priority2'] = ia_jabil_out['Date Priority']

ia_file = r'C:\database\dev\Inputs\IA-JABIL.xlsx'
ia_jabil_out.to_excel(ia_file, sheet_name='Sheet1', index=False)

# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(ia_file, engine='xlsxwriter',
                        datetime_format='dd/mm/yyyy', date_format='dd/mm/yyyy')
ia_jabil_out.to_excel(writer, sheet_name='OPN ORD', index=False)

# Get the xlsxwriter workbook and worksheet objects.
workbook = writer.book
worksheet = writer.sheets['OPN ORD']

# define worksheet formatting
fmt_txt = workbook.add_format({'num_format': '@'})
fmt_date = workbook.add_format({'num_format': 'dd/mm/yyyy'})
fmt_gen = workbook.add_format({'num_format': 'General'})

# dict of columns and format
col_fmt = {
    'A:A': fmt_txt,
    'C:C': fmt_gen,
    'J:J': fmt_date,
    'M:M': fmt_gen,
    'P:P': fmt_date,
    'Q:Q': fmt_date,
    'T:T': fmt_gen,
    'W:W': fmt_date,
}
# Set the column width and format.
for col, fmt in col_fmt.items():
    worksheet.set_column(col, 8, fmt)

# Close the Pandas Excel writer and output the Excel file.
writer.save()
