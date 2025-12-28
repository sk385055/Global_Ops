import pandas as pd

report_month = 202511
path_out = r"E:\_Projects\regional_tls\waterfall\out\\"
path = r"E:\_Projects\regional_tls\waterfall\rcv\\"
file = "template.xlsb"
file_out = "regional_tls"

por = pd.read_excel(path + file, sheet_name="data",engine = "pyxlsb", keep_default_na='')

# Convert date columns into rows
por = por.melt(id_vars=[
        "Plant", "Class_", "Data Series", "Region_", "Key Account", "Country Code"], var_name="Date", value_name="qty")

#print(por)

# Remove rows where qty is blank or zero
por = por[por['qty'].astype(str).str.strip() != '']  # Remove blanks
por = por[por['qty'].astype(int) != 0]  # Remove zeros
por['Report Month'] = report_month

por.to_csv(path_out + file_out + '.csv',index=False)
print("CSV conversion completed.. " + file)