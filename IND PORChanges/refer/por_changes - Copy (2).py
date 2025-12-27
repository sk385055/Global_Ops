import pandas as pd
from datetime import datetime


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
   prior_data = pd.read_csv(path + prior_file, usecols=columns_to_read)
   cur_data = pd.read_csv(path + cur_file, usecols=columns_to_read)

   
    #Keywords to filter
   dataseries = ['BACKLOG',	'ex','FINAL SHIPMENTS FORECAST','Neg Inv','NEW DEMAND','Plan','PRE-BUILD','SHIP NOT INV','UNSCHEDULED']
   
   prior_data = prior_data[prior_data['Data Series'].isin(dataseries)]
   cur_data = cur_data[cur_data['Data Series'].isin(dataseries)]
    
   # Find common columns (assuming one is 'qty' or similar)
   common_columns = set(prior_data.columns) & set(cur_data.columns)
   
   qty_columns = [col for col in common_columns if 'qty' in col.lower()]
   if not qty_columns:
       print("No quantity columns found (looking for columns containing 'qty')")
       return
   
   # Use the first quantity column found
   qty_col = qty_columns[0]
   print(f"Using quantity column: '{qty_col}'")
   
   # Find the key columns (non-quantity common columns)
   key_columns = list(common_columns - set(qty_columns))
   if not key_columns:
       print("No common key columns found for merging")
       return
   
   # Merge the dataframes on key columns   
   merged = pd.merge(prior_data, cur_data, on=key_columns, how='outer',suffixes=('_file1', '_file2'))

    #file_out qty define
   from_qty = prior_file[19:25]
   from_qty = from_qty.replace("-","")
   
   to_qty = cur_file[19:25]
   to_qty = to_qty.replace("-","")
   
   print(to_qty)

   # Create comparison columns
   merged[f'{qty_col}_file1'] = merged.get(f'{qty_col}_file1', pd.NA)
   merged[f'{qty_col}_file2'] = merged.get(f'{qty_col}_file2', pd.NA)
   
   
   # Add comparison result column
   merged['Comparison_Result'] = merged.apply(
       lambda row: 'Match' if row[f'{qty_col}_file1'] == row[f'{qty_col}_file2']
       else 'Mismatch' if not pd.isna(row[f'{qty_col}_file1']) and not pd.isna(row[f'{qty_col}_file2'])
       else 'Only in File1' if pd.isna(row[f'{qty_col}_file2'])
       else 'Only in File2', axis=1)
   
   # Reorder columns to show keys first, then quantities, then result
   columns_ordered = key_columns + [f'{qty_col}_file1', f'{qty_col}_file2', 'Comparison_Result']
   remaining_columns = [col for col in merged.columns if col not in columns_ordered]
   final_columns = columns_ordered + remaining_columns
   result = merged[final_columns]
   
   # Save to new Excel file
   result.to_csv(path_out + file_out, index=False)
   print(f"Comparison complete. Results saved to {file_out}")


# Example usage
if __name__ == "__main__":
   
   #file1_path = input("Enter path to first Excel file (base file): ").strip('"')
   #file2_path = input("Enter path to second Excel file: ").strip('"')
   #output_path = input("Enter output file path (press Enter for default): ").strip('"')
   
   columns_to_read = [ "Demand Stream", "Region1", "Area2", "Region", "Theatre", "Area", "Country Name", "Country Code", 
    "Bill to Country", "Zone", "Customer LOB", "Product LOB", "Product Range", "Parent Model", "Class", 
    "Description", "Organization Code", "KeyAccount_Billing", "Key Account", "Master Customer", "Item Type", "ASP", 
    "Data Series", "Comments","Date","qty"]
   
   prior_file = "ATLEOS OCP IND POR_FEB-17-2025 v1.csv"
   cur_file = "ATLEOS OCP IND POR_MAR-17-2025 v2.csv"
   
   
   #file_out name define
   from_date = prior_file[19:30]
   from_text = datetime.strptime(from_date, "%b-%d-%Y")
   from_text = from_text.strftime("%Y%m%d")
   
   to_date = cur_file[19:30]
   to_text = datetime.strptime(to_date, "%b-%d-%Y")
   to_text = to_text.strftime("%Y%m%d")
   
   print(from_text)
   print(to_text)
   
   file_out = "DP Changes_OCP" + from_text +'_to_OCP_'+ to_text + '.csv'
   
   compare_excel_files(prior_file, cur_file, file_out, columns_to_read)