import pandas as pd
import numpy as np
from datetime import datetime
import os
 
def main():
    User_Input = int(input('1) POR Changes | 2) convert into CSV | 3) Prior Year | 4) EXIT : '))
    
    if User_Input == 1:
        por_changes()
    elif User_Input == 2:
        convert_into_csv()
    elif User_Input == 3:
        prior_year()  
    else:
        print("Thank you...")
        return
                
def list_files(directory):
    #files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and f.lower().endswith('.csv')]
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    if not files:
        print("No files found in the directory.")
        return None
    
    # Sort files by modification time (ascending)
    files.sort(key=lambda f: os.path.getmtime(os.path.join(directory, f)))

    return files

def display_files(files):
    """Display files with numbering"""
    print("\nAvailable files:")
    for i, file in enumerate(files, 1):
        print(f"{i}. {file}")

def get_files(files, prompt):
    print()
    selection = input(prompt).strip()
    file_num = int(selection)
    if 1<= file_num <= len(files):
        return files[file_num-1]
            
def por_changes():
    
    dir_path = r'E:\_Projects\IND POR\changes\csv\\'
    files = list_files(dir_path)
    
    display_files(files)
    
    prior_filename = get_files(files, "Enter prior file number: ")
    cur_filename = get_files(files, "Enter current file number: ")
    
    prior_file = prior_filename
    cur_file = cur_filename
    
    print("prior file - " + prior_file)
    print("current file - " + cur_file)
    
    columns_to_read = [ "Demand Stream", "Region1", "Area2", "Region", "Theatre", "Area", "Country Name", "Country Code", 
    "Bill to Country", "Zone", "Customer LOB", "Product LOB", "Product Range", "Parent Model", "Class", 
    "Description", "Organization Code", "KeyAccount_Billing", "Key Account", "Master Customer", "Item Type", "ASP", 
    "Data Series", "Comments","Date","qty"]
    
    #file_out name define
    from_date = prior_file[19:30]
    from_text = datetime.strptime(from_date, "%b-%d-%Y")
    from_text = from_text.strftime("%Y%m%d")
    
    to_date = cur_file[19:30]
    to_text = datetime.strptime(to_date, "%b-%d-%Y")
    to_text = to_text.strftime("%Y%m%d")
    
    print(from_text)
    print(to_text)
    
    #file_out = "DP Changes_OCP" + from_text +'_to_OCP_'+ to_text + '.csv'
    
    file_out = "OCP_" + from_text +'_to_OCP_'+ to_text + '_DataOnly.csv'
    
    compare_excel_files(prior_file, cur_file, file_out, columns_to_read)


def compare_excel_files(prior_file, cur_file, file_out, columns_to_read):
   """
   Compare two Excel files with similar columns, focusing on quantity comparison.
   Args:
       file1 (str): Path to the first Excel file (base file)
       file2 (str): Path to the second Excel file
       output_file (str): Path for the output comparison file
   """
   
   path = "E:\_Projects\IND POR\changes\csv\\"
   path_out = "E:\_Projects\IND POR\changes\out\\"
   
   # Read both Excel files
   PRIOR_DATA = pd.read_csv(path + prior_file, usecols=columns_to_read)
   CUR_DATA = pd.read_csv(path + cur_file, usecols=columns_to_read)
   
   PRIOR_YR = pd.read_csv('E:\_Projects\IND POR\changes\prior_yr\prior_year.csv')
   #print(PRIOR_YR)
   
   #Plan data
   PLAN_DATA = pd.read_csv(path + cur_file, usecols=columns_to_read)
   PLAN_DATA ['ASP'] = PLAN_DATA ['ASP'].replace([np.nan, np.inf, -np.inf], 0)
   PLAN_DATA ['ASP'] = PLAN_DATA ['ASP'].astype(int)
   PLAN_DATA = PLAN_DATA[PLAN_DATA['Data Series'] == 'Plan']
   PLAN_DATA['R_Plan'] = PLAN_DATA['ASP'] * PLAN_DATA['qty'].fillna(0)
   PLAN_DATA.rename(columns={'qty': 'Q_Plan'}, inplace=True)

    #Keywords to filter
   #dataseries = ['BACKLOG',	'ex','FINAL SHIPMENTS FORECAST','Neg Inv','NEW DEMAND','PRE-BUILD','SHIP NOT INV','UNSCHEDULED','SHIPMENTS HISTORY']
   #PRIOR_DATA = PRIOR_DATA[PRIOR_DATA['Data Series'].isin(dataseries)]
   #CUR_DATA = CUR_DATA[CUR_DATA['Data Series'].isin(dataseries)]
   
   PRIOR_DATA = PRIOR_DATA[PRIOR_DATA['Data Series'] != 'Plan']
   CUR_DATA = CUR_DATA[CUR_DATA['Data Series'] != 'Plan']
   
   #Convert ASP datatype
   PRIOR_DATA['ASP'] = PRIOR_DATA['ASP'].replace([np.nan, np.inf, -np.inf], 0)
   CUR_DATA['ASP'] = CUR_DATA['ASP'].replace([np.nan, np.inf, -np.inf], 0)

   PRIOR_DATA['ASP'] = PRIOR_DATA['ASP'].astype(int)
   CUR_DATA['ASP'] = CUR_DATA['ASP'].astype(int)
    
   # Find common columns (assuming one is 'qty' or similar)
   common_columns = set(PRIOR_DATA.columns) & set(CUR_DATA.columns)
   
   qty_columns = [col for col in common_columns if 'qty' in col.lower()]
   if not qty_columns:
       print("No quantity columns found (looking for columns containing 'qty')")
       return
   
   # Use the first quantity column found
   qty_col = qty_columns[0]
   #print(f"Using quantity column: '{qty_col}'")
   
   # Find the key columns (non-quantity common columns)
   key_columns = list(common_columns - set(qty_columns))
   if not key_columns:
       print("No common key columns found for merging")
       return
   
   # Merge the dataframes on key columns   
   merged = pd.merge(PRIOR_DATA, CUR_DATA, on=key_columns, how='outer',suffixes=('_file1', '_file2'))
   
   # Create comparison columns
   merged[f'{qty_col}_file1'] = merged.get(f'{qty_col}_file1', pd.NA)
   merged[f'{qty_col}_file2'] = merged.get(f'{qty_col}_file2', pd.NA)
   
   
   # Add comparison result column
   """
   merged['Comparison_Result'] = merged.apply(
       lambda row: 'Match' if row[f'{qty_col}_file1'] == row[f'{qty_col}_file2']
       else 'Mismatch' if not pd.isna(row[f'{qty_col}_file1']) and not pd.isna(row[f'{qty_col}_file2'])
       else 'Only in File1' if pd.isna(row[f'{qty_col}_file2'])
       else 'Only in File2', axis=1)
   """

   # Reorder columns to show keys first, then quantities, then result
   #columns_ordered = key_columns + [f'{qty_col}_file1', f'{qty_col}_file2', 'Comparison_Result']
   
   chgs = merged
   
   #Get result in chgs dataframe
   # Appending DataFrames using concat
   chgs = pd.concat([chgs, PLAN_DATA], ignore_index=True)
   
   #file_out qty define
   prior_mth = prior_file[19:25]
   cur_mth = cur_file[19:25]
   
   print(prior_mth)
   print(cur_mth)
   chgs['qty_file2'] = chgs['qty_file2'].fillna(0)
   chgs['qty_file1'] = chgs['qty_file1'].fillna(0)
   
   chgs['Q_V'] = chgs['qty_file2'].fillna(0) - chgs['qty_file1'].fillna(0) #cur - prev
   chgs['R_file1'] = chgs['ASP'] * chgs['qty_file1'].fillna(0)
   chgs['R_file2'] = chgs['ASP'] * chgs['qty_file2'].fillna(0)
   chgs['R_V'] = chgs['R_file2'].fillna(0) - chgs['R_file1'].fillna(0) #cur - prev
   
   chgs['Date'] = pd.to_datetime('1899-12-30') + pd.to_timedelta(chgs['Date'], unit = 'D')
   chgs['SAD_Qtr'] = chgs['Date'].dt.year.astype(str) + 'Q' + chgs['Date'].dt.quarter.astype(str)
   chgs['SAD_Mth'] = chgs['Date'].dt.strftime('%Y%m')
   
   
   #chgs.to_csv("E:/_Projects/IND POR/changes/out/test.csv", index=False)
   
   chgs = pd.concat([chgs, PRIOR_YR])
   
   columns_to_arrange = [ "Demand Stream", "Region1", "Area2", "Region", "Theatre", "Area", "Country Name", "Country Code", 
    "Bill to Country", "Zone", "Customer LOB", "Product LOB", "Product Range", "Parent Model", "Class", 
    "Description", "Organization Code", "KeyAccount_Billing", "Key Account", "Master Customer", "Item Type", "ASP", 
    "Data Series", "Comments","Date","qty_file1","qty_file2","Q_V","R_file1","R_file2","R_V","Q_Plan","R_Plan","Q_Prior","R_Prior","SAD_Qtr","SAD_Mth"]
   
   columns_ordered = columns_to_arrange
   remaining_columns = [col for col in chgs.columns if col not in columns_ordered]
   final_columns = remaining_columns + columns_ordered
   chgs = chgs[final_columns]
   
   prior_qty = "Q_" + prior_mth.replace("-","")
   cur_qty = "Q_" + cur_mth.replace("-","")
   prior_rev = "R_" + prior_mth.replace("-","")
   cur_rev = "R_" + cur_mth.replace("-","")
   
   
   
   chgs.rename(columns={'qty_file1': prior_qty, 'qty_file2': cur_qty, 'R_file1': prior_rev, 'R_file2':cur_rev,'Date':'SAD_Dt'}, inplace=True)
    
   chgs['SAD_Mth'] = chgs['SAD_Mth'].astype(str)

   
   # Save to new Excel file
   chgs.to_csv(path_out + file_out, index=False)
   print(f"Comparison completed. Output: {file_out}")
   main()


def convert_into_csv():
    print()
    
    #file = "ATLEOS OCP IND POR_FEB-17-2025 v1"
    path_out = r"E:\_Projects\IND POR\changes\csv\\"
    path = r"E:\_Projects\IND POR\Report\\"
    
    files = list_files(path)
    
    display_files(files)
    
    filename = get_files(files, "Enter file number to convert into csv: ")
    
    file = filename
    print("processing... " + file)
    por = pd.read_excel(path + file, sheet_name="Data",engine = "pyxlsb", keep_default_na='')
        
    # Remove columns that contain '$' in suffix
    #por = por.loc[:, ~por.columns.astype(str).str.endswith('$')]
    
    
    # Keep columns up to and including 'Comments'
    if 'Comments' in por.columns:
        por = por.loc[:, : 'Comments']
    else:
        print("'Comments' column not found.")

    # Convert date columns into rows
    por = por.melt(id_vars=[
        "Demand Stream", "Region1", "Area2", "Region", "Theatre", "Area", "Country Name", "Country Code", 
        "Bill to Country", "Zone", "Customer LOB", "Product LOB", "Product Range", "Parent Model", "Class", 
        "Description", "DTF", "DTF cutoff", "Start Date", "End Date", "DTF risk units", "DTF risk", 
        "Organization Code", "KeyAccount_Billing", "Key Account", "Master Customer", "Item Type", "ASP", 
        "Data Series", "Comments"], var_name="Date", value_name="qty")
    # print(df_melted)

     #Keywords to filter
    item_type = ['Unassigned',	'Unit']
    por = por[por['Item Type'].isin(item_type)]
    
    qty_ = 'qty'

    por = por[(por[qty_] != '') & (por[qty_].notna())]
    
    # Remove rows where 'Date' is '6M Total'
    #por = por[por['Date'] != '6M Total']
    
    
    # List of columns to delete
    columns_to_delete = [ "DTF", "DTF cutoff", "Start Date", "End Date", "DTF risk units", "DTF risk"]

    # Drop the columns from the DataFrame
    por.drop(columns=columns_to_delete, inplace=True)


    #por['Date'] = pd.to_datetime('1899-12-30') + pd.to_timedelta(por['Date'], unit = 'D')
     
    # Define columns to group by (excluding 'qty' and 'ASP')
    group_columns = [col for col in por.columns if col not in ['qty', 'ASP']]
    por = por.groupby(group_columns)[['qty', 'ASP']].sum().reset_index()

    
    file = os.path.splitext(file)[0] #remove its extension
    por.to_csv(path_out + file + '.csv',index=False)
    print("CSV conversion completed.. " + file)
    main()
    
    
def prior_year():
    print()
    
    dir_path = r'E:\_Projects\IND POR\changes\csv\\'
    files = list_files(dir_path)
    path_out = r'E:\_Projects\IND POR\changes\prior_yr\\'
    
    display_files(files)
    
    latest_filename = get_files(files, "Enter latest file number: ")
    
    file = latest_filename

    print("processing... " + file)
    columns_to_read = [ "Demand Stream", "Region1", "Area2", "Region", "Theatre", "Area", "Country Name", "Country Code", 
    "Bill to Country", "Zone", "Customer LOB", "Product LOB", "Product Range", "Parent Model", "Class", 
    "Description", "Organization Code", "KeyAccount_Billing", "Key Account", "Master Customer", "Item Type", "ASP", 
    "Data Series", "Comments","Date","qty"]
    
    PRIOR_YR = pd.read_csv(dir_path + file, usecols=columns_to_read)
    PRIOR_YR ['ASP'] = PRIOR_YR ['ASP'].replace([np.nan, np.inf, -np.inf], 0)
    PRIOR_YR ['ASP'] = PRIOR_YR ['ASP'].astype(int)
    PRIOR_YR = PRIOR_YR[PRIOR_YR['Data Series'] == 'SHIPMENTS HISTORY']
    
    PRIOR_YR['R_Prior'] = PRIOR_YR['ASP'] * PRIOR_YR['qty'].fillna(0)
    PRIOR_YR.rename(columns={'qty': 'Q_Prior'}, inplace=True)
    PRIOR_YR['Date'] = pd.to_datetime('1899-12-30') + pd.to_timedelta(PRIOR_YR['Date'], unit = 'D')
    
    PRIOR_YR['next_year_date'] = PRIOR_YR['Date'].apply(date_to_next_year) #convert date to next year
    PRIOR_YR['SAD_Qtr'] = PRIOR_YR['next_year_date'].dt.year.astype(str) + 'Q' + PRIOR_YR['next_year_date'].dt.quarter.astype(str)
    PRIOR_YR['SAD_Mth'] = PRIOR_YR['next_year_date'].dt.strftime('%Y%m')
    PRIOR_YR.drop('Date',axis=1, inplace=True) #delete Date columns
    PRIOR_YR.rename(columns={'next_year_date':'Date'}, inplace=True)
    
    
    current_date = datetime.today().strftime('%Y%m%d')
    print('prior_year_' + current_date + " file completed")
    
    # Save to new Excel file
    PRIOR_YR.to_csv(path_out + "prior_year_" + current_date + ".csv", index=False)
    main()
   

def date_to_next_year(d):
    try:
        # Try to return same date next year
        return d.replace(year=d.year + 1)
    except ValueError:
        # Handle February 29th for non-leap years by moving to March 1st
        return d + pd.Timedelta(days=365)


# Example usage
if __name__ == "__main__":
    main()
