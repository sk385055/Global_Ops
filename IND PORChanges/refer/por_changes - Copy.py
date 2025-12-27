import pandas as pd
def compare_excel_files(file1, file2, output_file='comparison_result.xlsx'):
   """
   Compare two Excel files with similar columns, focusing on quantity comparison.
   Args:
       file1 (str): Path to the first Excel file (base file)
       file2 (str): Path to the second Excel file
       output_file (str): Path for the output comparison file
   """
   # Read both Excel files
   df1 = pd.read_csv(file1)
   df2 = pd.read_csv(file2)
   
# Select specific columns
   df1 = df1.filter(items=[ "Demand Stream", "Region1", "Area2", "Region", "Theatre", "Area", "Country Name", "Country Code", 
    "Bill to Country", "Zone", "Customer LOB", "Product LOB", "Product Range", "Parent Model", "Class", 
    "Description", "Organization Code", "KeyAccount_Billing", "Key Account", "Master Customer", "Item Type", "ASP", 
    "Data Series", "Comments","Date","qty"])
   
# Select specific columns
   df2 = df2.filter(items=[ "Demand Stream", "Region1", "Area2", "Region", "Theatre", "Area", "Country Name", "Country Code", 
    "Bill to Country", "Zone", "Customer LOB", "Product LOB", "Product Range", "Parent Model", "Class", 
    "Description", "Organization Code", "KeyAccount_Billing", "Key Account", "Master Customer", "Item Type", "ASP", 
    "Data Series", "Comments","Date","qty"])
 
   # Find common columns (assuming one is 'qty' or similar)
   common_columns = set(df1.columns) & set(df2.columns)
   
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
   # Convert 'DTF risk units' to the same type
    
   
   merged = pd.merge(df1, df2, on=key_columns, how='outer',suffixes=('_file1', '_file2'))
   
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
   result.to_csv(output_file, index=False)
   print(f"Comparison complete. Results saved to {output_file}")


# Example usage
if __name__ == "__main__":
   
   #file1_path = input("Enter path to first Excel file (base file): ").strip('"')
   #file2_path = input("Enter path to second Excel file: ").strip('"')
   #output_path = input("Enter output file path (press Enter for default): ").strip('"')
   
   file1_path = "E:\_Projects\IND POR\changes\csv\ATLEOS OCP IND POR_FEB-17-2025 v1.csv"
   file2_path = "E:\_Projects\IND POR\changes\csv\ATLEOS OCP IND POR_MAR-17-2025 v2.csv"
   output_path = "E:\_Projects\IND POR\changes\csv\\output.csv"
   
   
   if not output_path:
       output_path = 'comparison_result.xlsx'
   compare_excel_files(file1_path, file2_path, output_path)