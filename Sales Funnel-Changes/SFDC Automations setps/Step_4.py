import Step_3
import pandas as pd 
import numpy as np
from datetime import datetime, timedelta

TeamHeader = pd.read_csv("E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Team Header.csv" ,na_values=[''],keep_default_na=False, low_memory=False)
TeamDataOrders = pd.read_csv("E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Team Data Orders.csv" ,na_values=[''],keep_default_na=False, low_memory=False)
TEAM_DETAIL_RECEIPTS = pd.read_csv("E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\RACE Team Receipts.csv" ,na_values=[''],keep_default_na=False)


#Rename column in receipts
# rename columns
TEAM_DETAIL_RECEIPTS = TEAM_DETAIL_RECEIPTS.rename(columns={'Estimated Receipt Acct Period': 'Est Receipt Mth','Ref Opp-Line':'Ref-Opp-Line', 'Unassigned Receipt Units': 'Unassigned Rcpt Units'})
TEAM_DETAIL_RECEIPTS['Est Revenue Mth'] = None
TEAM_DETAIL_RECEIPTS['Receipt_Mth_Change_Flag'] = None
TEAM_DETAIL_RECEIPTS['Estimated Revenue Date'] = None
TEAM_DETAIL_RECEIPTS['Unit Value-USD'] = None
TEAM_DETAIL_RECEIPTS['Unit Value-Local'] = None
################################################################ Calculate new order values ################################################################

# Team_Update Order Value1

# SELECT [Team Data_Orders].[Opportunity Number], Sum([Team Data_Orders].[Estimated Order Value-US]) AS [SumOfEstimated Order Value-US], Sum([Team Data_Orders].[Estimated Order Value-Local]) AS [SumOfEstimated Order Value-Local] INTO Temp
# FROM [Team Data_Orders]
# WHERE (((Left([Top Line Product Name],2))<>"A-"))
# GROUP BY [Team Data_Orders].[Opportunity Number];

# Convert "MCS MCN" to object type and merge RaceTeamOrdersCountryCodeJoin and Load Team latest2a - xg country correction based on Master Customer Number

TeamDataOrders["Top Line Product Name"] = TeamDataOrders["Top Line Product Name"].astype(str)

# filter rows where Top Line Product Name does not start with "A-"
TeamDataOrders = TeamDataOrders[~TeamDataOrders['Top Line Product Name'].str.startswith('A-')]
TeamDataOrders = TeamDataOrders[TeamDataOrders["Top Line Product Name"] != "nan"]

# Group by Opportunity Number and calculate the sum of Estimated Order Value-US and Estimated Order Value-Local
TeamDataOrders = TeamDataOrders.groupby("Opportunity Number").agg({"Estimated Order Value-US": "sum", "Estimated Order Value-Local": "sum"})

# Reset the index to include the Opportunity Number column in the output
TeamDataOrders = TeamDataOrders.reset_index()

# rename columns
Temp = TeamDataOrders.rename(columns={'Estimated Order Value-US': 'SumOfEstimated Order Value-US', 'Estimated Order Value-Local': 'SumOfEstimated Order Value-Local'})

# write result to a temporary file
#Temp.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Temp 1.csv',index = False) 
####print('Calculate new order values')
################################################## Load new order values #############################################################

# Team_Update Order Value2
# UPDATE Temp INNER JOIN [TEAM Header] ON Temp.[Opportunity Number]=[TEAM Header].[Opportunity Number] SET [TEAM Header].[Total Order Value - US] = [SumOfEstimated Order Value-US], [TEAM Header].[Total Order Value - Local] = [SumOfEstimated Order Value-Local], [TEAM Header].[Total Order Value - US (Baseline)] = [SumOfEstimated Order Value-US]
# WHERE ((([TEAM Header].[Total Order Value - US (Baseline)])=0));
# Merge the two dataframes on 'Opportunity Number'

Temp = pd.merge(TeamHeader,Temp, on='Opportunity Number',how='left')

########## Increase Decimal value
Temp[['Total Order Value - US','Total Order Value - Local','Total Order Value - US (Baseline)']]=Temp[['Total Order Value - US','Total Order Value - Local','Total Order Value - US (Baseline)']].round(3)

Temp.loc[Temp['Total Order Value - US (Baseline)'] == 0, 'Total Order Value - US'] = Temp['SumOfEstimated Order Value-US']
Temp.loc[Temp['Total Order Value - US (Baseline)'] == 0, 'Total Order Value - Local'] = Temp['SumOfEstimated Order Value-Local']
Temp.loc[Temp['Total Order Value - US (Baseline)'] == 0, 'Total Order Value - US (Baseline)'] = Temp['SumOfEstimated Order Value-US']

# Save
#Temp.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Temp 2.csv', index=False) 

Temp[['Total Order Value - US','Total Order Value - Local','Total Order Value - US (Baseline)']] = Temp[['Total Order Value - US','Total Order Value - Local','Total Order Value - US (Baseline)']].replace('nan', '0')

####print('Load new order values')
############################################################ own Try #####################################################################

#TeamHeader = TeamHeader.drop_duplicates()
#Temp = Temp.drop_duplicates()

TeamHeader = pd.merge(TeamHeader, Temp, on="Opportunity Number" , how="left")

#TeamHeader = TeamHeader.rename(columns=lambda x: x.replace('_y', ''))

TeamHeader = TeamHeader.drop_duplicates()

TeamHeader.columns = [col.replace('_y','') for col in TeamHeader.columns]

TeamHeader = TeamHeader.filter(regex='^(?!.*_x$)')

####print('own Try')
################################################## Calculate revenue values - Karthik ##################################################

# Team_Update Revenue Value1

# SELECT [Team Data_Receipts].[Opportunity Number], Sum([Team Data_Receipts].[Estimated Receipt Value-US]) AS [SumOfEstimated Receipt Value-US], Sum([Team Data_Receipts].[Estimated Receipt Value-Local]) AS [SumOfEstimated Receipt Value-Local] INTO Temp_Rev
# FROM [Team Data_Receipts]
# WHERE (((Left([Top Line Product Name],2))<>"A-"))
# GROUP BY [Team Data_Receipts].[Opportunity Number];

TeamDataReceipt = pd.read_csv("E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\RACE Team Receipts.csv" ,na_values=[''],keep_default_na=False, low_memory=False)

# Replace NaN values in 'Top Line Product Name' column with empty strings
TeamDataReceipt['Top Line Product Name'] = TeamDataReceipt['Top Line Product Name'].fillna('')

# filter rows where Top Line Product Name does not start with "A-"
TeamDataReceipt = TeamDataReceipt[~TeamDataReceipt['Top Line Product Name'].str.startswith('A-')]

TeamDataReceipt = TeamDataReceipt[TeamDataReceipt["Top Line Product Name"] != ""]

TeamDataReceipt = TeamDataReceipt.groupby('Opportunity Number').agg({"Estimated Receipt Value-US":"sum", "Estimated Receipt Value-Local":"sum"})

# Reset the index to include the Opportunity Number column in the output
TeamDataReceipt = TeamDataReceipt.reset_index()


Temp_Rev = pd.DataFrame({'Opportunity Number':TeamDataReceipt['Opportunity Number'],'SumOfEstimated Receipt Value-US':TeamDataReceipt['Estimated Receipt Value-US'],'SumOfEstimated Receipt Value-Local':TeamDataReceipt['Estimated Receipt Value-Local']})

Temp_Rev = Temp_Rev.drop_duplicates()

# Save
#Temp_Rev.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Temp_Rev.csv',index = False)

####print(' Calculate revenue values - Karthik ')
################################################## Load Revenue Value in Team Header- Karthik  ##################################################
# Team_Update Revenue Value2

# UPDATE [TEAM Header] INNER JOIN Temp_Rev ON [TEAM Header].[Temp_Rev] = Temp_Rev.[Opportunity Number] SET [TEAM Header].[Total Revenue Value - US] = [SumOfEstimated Receipt Value-US], [TEAM Header].[Total Revenue Value - Local] = [SumOfEstimated Receipt Value-Local], [TEAM Header].[Total Revenue Value - US (Baseline)] = [SumOfEstimated Receipt Value-US]
# WHERE ((([TEAM Header].[Total Revenue Value - US (Baseline)])=0));

TeamHeader = pd.merge(TeamHeader, Temp_Rev, on="Opportunity Number" , how="left")

TeamHeader = TeamHeader.drop_duplicates()

TeamHeader.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Temp_Rev.csv',index = False)

TeamHeader = TeamHeader.rename(columns=lambda x: x.replace('_x', ''))

TeamHeader = TeamHeader.filter(regex='^(?!.*_y$)')

#####Convert to decimal
TeamHeader[['Total Revenue Value - US','Total Revenue Value - Local','Total Revenue Value - US (Baseline)']]=TeamHeader[['Total Revenue Value - US','Total Revenue Value - Local','Total Revenue Value - US (Baseline)']].round(3)

# Update the 'Total Revenue Value' fields in the merged dataframe
TeamHeader['Total Revenue Value - US'] = TeamHeader['SumOfEstimated Receipt Value-US']
TeamHeader['Total Revenue Value - Local'] = TeamHeader['SumOfEstimated Receipt Value-Local']
TeamHeader['Total Revenue Value - US (Baseline)'] = TeamHeader['SumOfEstimated Receipt Value-US']

# Update the 'TEAM Header' dataframe with the updated values
TeamHeader.update(TeamHeader[['Opportunity Number', 'Total Revenue Value - US', 'Total Revenue Value - Local', 'Total Revenue Value - US (Baseline)']])

TeamHeader = TeamHeader.drop_duplicates()

# Save
TeamHeader.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Team Headers 1.csv', index=False)
####print('Load Revenue Value in Team Header- Karthik')
TEAM_DETAIL_RECEIPTS.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\TEAM_DETAIL_RECEIPTS.csv', index=False)
################################################## Updates Est rev date in team_receipts - Karthik  ##################################################

# Updates Est rev date in team_receipts - Karthik

# Team_Update Detail RevDate1

# UPDATE [TEAM DETAIL_RECEIPTS] INNER JOIN [TEAM HEADER] ON [TEAM DETAIL_RECEIPTS].[Opportunity Number]=[TEAM HEADER].[Opportunity Number] SET [TEAM DETAIL_RECEIPTS].[Estimated Revenue Date] = [Estimated Receipt Date]
# WHERE ((([TEAM HEADER].[Revenue Trigger])="OTH" Or ([TEAM HEADER].[Revenue Trigger])="DEL" Or ([TEAM HEADER].[Revenue Trigger])="Other" Or ([TEAM HEADER].[Revenue Trigger])="Delivery"));


#MergedRECEIPTS4['Revenue Trigger'] = MergedRECEIPTS4['Revenue Trigger'].astype(str)

TEAM_DETAIL_RECEIPTS = pd.merge(TEAM_DETAIL_RECEIPTS,TeamHeader, on='Opportunity Number',how='left')

TEAM_DETAIL_RECEIPTS = TEAM_DETAIL_RECEIPTS.rename(columns=lambda x: x.replace('_x', ''))

TEAM_DETAIL_RECEIPTS = TEAM_DETAIL_RECEIPTS.filter(regex='^(?!.*_y$)')

TEAM_DETAIL_RECEIPTS.loc[
    TEAM_DETAIL_RECEIPTS['Revenue Trigger'].isin(['OTH', 'DEL', 'Other', 'Delivery']),
    'Estimated Revenue Date'
    ] = TEAM_DETAIL_RECEIPTS['Estimated Receipt Date']


# Save
#TEAM_DETAIL_RECEIPTS.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\TEAM DETAIL_RECEIPTS 1.csv', index=False)

####print('Updates Est rev date in team_receipts - Karthik ')
################################################## Updates Est rev date in team_receipts - Karthik  ##################################################

#Team_Update Detail RevDate2

#UPDATE [TEAM DETAIL_RECEIPTS] INNER JOIN [TEAM HEADER] ON [TEAM DETAIL_RECEIPTS].[Opportunity Number]=[TEAM HEADER].[Opportunity Number] SET [TEAM DETAIL_RECEIPTS].[Estimated Revenue Date] = IIf([Estimated Receipt Date]=#1/1/3000#,[Estimated Receipt Date],[TEAM DETAIL_RECEIPTS].[Estimated Receipt Date]+28)
#WHERE ((([TEAM HEADER].[Revenue Trigger])="INSTALL" Or ([TEAM HEADER].[Revenue Trigger])="CERT" Or ([TEAM HEADER].[Revenue Trigger])="Installation" Or ([TEAM HEADER].[Revenue Trigger])="Certification"));

# Define a function to update the 'Estimated Revenue Date' field based on the 'Revenue Trigger' value
def update_estimated_revenue_date(row):
    if row['Revenue Trigger'] in ['INSTALL', 'CERT', 'Installation', 'Certification']:
        if pd.isnull(row['Estimated Receipt Date']):
            return np.nan
        elif row['Estimated Receipt Date'] == '1/1/3000':
            return row['Estimated Receipt Date']
        else:
            date_string = str(row['Estimated Receipt Date'])
            date_format = '%m/%d/%Y'
            date_object = datetime.strptime(date_string, date_format)
            updated_date = date_object + timedelta(days=28)
            updated_date_string = datetime.strftime(updated_date, date_format)
            return updated_date_string
        
    else:
        return row['Estimated Revenue Date']


# Apply the update_estimated_revenue_date function to the merged dataframe
TEAM_DETAIL_RECEIPTS['Estimated Revenue Date'] = TEAM_DETAIL_RECEIPTS.apply(update_estimated_revenue_date, axis=1)


# save updated DataFrame to CSV file
#TEAM_DETAIL_RECEIPTS.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\TEAM DETAIL_RECEIPTS 2.csv', index=False)
####print('Updates Est rev date in team_receipts - Karthik ')
################################################## Updates Est rev date in team_receipts - Karthik  ##################################################

#Team_Update Detail RevDate3

#UPDATE [TEAM DETAIL_RECEIPTS] INNER JOIN [TEAM HEADER] ON [TEAM DETAIL_RECEIPTS].[Opportunity Number]=[TEAM HEADER].[Opportunity Number] SET [TEAM DETAIL_RECEIPTS].[Estimated Revenue Date] = IIf([Estimated Receipt Date]=#1/1/3000#,[Estimated Receipt Date],[TEAM DETAIL_RECEIPTS].[Estimated Receipt Date]-7)
#WHERE ((([TEAM HEADER].[Revenue Trigger])="SHIP" Or ([TEAM HEADER].[Revenue Trigger])="Shipment"));

# Define a function to update the 'Estimated Revenue Date' field based on the 'Revenue Trigger' value
def update_estimated_revenue_date3(row):
    if row['Revenue Trigger'] in ['SHIP','Shipment']:
        if pd.isnull(row['Estimated Receipt Date']):
            return np.nan
        elif row['Estimated Receipt Date'] == '1/1/3000':
            return row['Estimated Receipt Date']
        else:
            date_string = str(row['Estimated Receipt Date'])
            date_format = '%m/%d/%Y'
            date_object = datetime.strptime(date_string, date_format)
            updated_date = date_object - timedelta(days=7)
            updated_date_string = datetime.strftime(updated_date, date_format)
            return updated_date_string
        
    else:
        return row['Estimated Revenue Date']

# Apply the update_estimated_revenue_date function to the merged dataframe
TEAM_DETAIL_RECEIPTS['Estimated Revenue Date'] = TEAM_DETAIL_RECEIPTS.apply(update_estimated_revenue_date3, axis=1)


# save updated DataFrame to CSV file
#TEAM_DETAIL_RECEIPTS.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\TEAM DETAIL_RECEIPTS 3.csv', index=False)

####print('Updates Est rev date in team_receipts - Karthik ')
################################################## Updates Est rev date in team_receipts - Karthik  ##################################################

#Team_Update Detail RevDate4

#UPDATE [TEAM DETAIL_RECEIPTS] INNER JOIN [Fin Mth] ON [TEAM DETAIL_RECEIPTS].[Estimated Revenue Date]=[Fin Mth].Date SET [TEAM DETAIL_RECEIPTS].[Est Revenue Mth] = [Fin Mth].[Accounting Period];

FinMth = pd.read_csv('E:\_Projects\SFDC Automation\Sales Funnel input files\Fin Mth.csv' ,na_values=[''],keep_default_na=False)


TEAM_DETAIL_RECEIPTS = TEAM_DETAIL_RECEIPTS.merge(FinMth, left_on='Estimated Revenue Date', right_on='Date', how='left')

TEAM_DETAIL_RECEIPTS = TEAM_DETAIL_RECEIPTS.rename(columns=lambda x: x.replace('_x', ''))

TEAM_DETAIL_RECEIPTS = TEAM_DETAIL_RECEIPTS.filter(regex='^(?!.*_y$)')

TEAM_DETAIL_RECEIPTS['Est Revenue Mth'] = TEAM_DETAIL_RECEIPTS['Accounting Period']



# save updated DataFrame to CSV file
TEAM_DETAIL_RECEIPTS.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\TEAM DETAIL_RECEIPTS.csv', index=False)

####print('Updates Est rev date in team_receipts - Karthik')
################################################## Flag incomplete opps  ##################################################

#Team_Update Incomplete Opps
TeamHeader[['Total Revenue Value - US','Total Revenue Value - Local','Total Revenue Value - US (Baseline)']] = TeamHeader[['Total Revenue Value - US','Total Revenue Value - Local','Total Revenue Value - US (Baseline)']].replace('nan', '0')


#UPDATE [TEAM Header] SET [TEAM Header].[Incomplete Opps] = "Y"
#WHERE ((([TEAM Header].[Total Order Value - US])=0));

#TeamHeader.loc[TeamHeader['Total Order Value - US'] == 0 |TeamHeader['Total Order Value - US'].isnull , 'Incomplete Opps'] = 'Y'
TeamHeader.loc[(TeamHeader['Total Order Value - US'].fillna(0) == 0), 'Incomplete Opps'] = 'Y'


# save updated DataFrame to CSV file
#TeamHeader.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Team Header Before Xg.csv', index=False)

####print(' Flag incomplete opps ')
################################################## REGION****Update area name for US1  ##################################################

#R1_Team_Update Area1 

#INSERT INTO [Org Tables] ( [Org Code], [Country Code] )
#SELECT [TEAM Header].[Org Code], [TEAM Header].[Country Code]
#FROM [Org Tables] RIGHT JOIN [TEAM Header] ON [Org Tables].[Org Code] = [TEAM Header].[Org Code]
#GROUP BY [TEAM Header].[Org Code], [TEAM Header].[Country Code], [Org Tables].[Org Code]
#HAVING ((([TEAM Header].[Country Code])="US") AND (([Org Tables].[Org Code]) Is Null));


OrgTable = pd.read_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Org Tables.csv' ,na_values=[''],keep_default_na=False,low_memory=False)

# Merge "Org Tables" and "TEAM Header" DataFrames on "Org Code" column
merged_Org = pd.merge(OrgTable, TeamHeader, on="Org Code", how="right")

merged_Org = merged_Org.rename(columns=lambda x: x.replace('_x', ''))

merged_Org = merged_Org.filter(regex='^(?!.*_y$)')

# Filter merged DataFrame to select rows where "Country Code" is "US" and "Org Code" is null in "Org Tables"
filtered_df = merged_Org.loc[(merged_Org["Country Code"]=="US") & (merged_Org["Org Code"].isnull())]

# Group filtered DataFrame by "Org Code" and "Country Code" columns
grouped_df = filtered_df.groupby(["Org Code", "Country Code"])

# Create new DataFrame by selecting the first value in each group
new_df = grouped_df.first().reset_index()


# Insert new DataFrame to "Org Tables" DataFrame
OrgTable = pd.concat([OrgTable, new_df[["Org Code", "Country Code"]]], ignore_index=True)


# save updated DataFrame to CSV file
OrgTable.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Org Tables.csv', index=False)

####print('REGION****Update area name for US1')
################################################## REGION****Update area name for US2 ##################################################

#R1_Team_Update Area2

#UPDATE [Org Tables] INNER JOIN [TEAM Header] ON [Org Tables].[Org Code]=[TEAM Header].[Org Code] SET [TEAM Header].[Country Name] = IIf([Level 4] Is Null,"US",[Level 4]), [TEAM Header].[Area Name] = "US"
#WHERE ((([TEAM Header].[Country Code])="US"));

# Update the 'Country Name' column using a conditional statement
OrgTable['Country Name'] = np.where(OrgTable['Level 4'].isnull(), 'US', OrgTable['Level 4'])

# Select the rows where the 'Country Code' column is 'US'
OrgTable.loc[OrgTable['Country Code'] == 'Country Code', 'Area Name'] = 'US'


# Perform the join operation
TeamHeader = pd.merge(TeamHeader,OrgTable,  on='Org Code', how='left')

TeamHeader = TeamHeader.rename(columns=lambda x: x.replace('_x', ''))

TeamHeader = TeamHeader.filter(regex='^(?!.*_y$)')


# Update 'Country Name' column for rows where 'Country Code' is 'US'
TeamHeader.loc[TeamHeader['Country Code'] == 'US', 'Country Name'] = TeamHeader['Level 4'].apply(lambda x: 'US' if pd.isnull(x) else x)

#TeamHeader.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\org code.csv', index=False)

# save updated DataFrame to CSV file
#OrgTable.to_csv(r'C:\Vasanthan\Race\Global Study\Tables Exported Files\Race CSV\Org Tables.csv', index=False)
#####print(' REGION****Update area name for US2 ')
################################################## Open Query : Update-XG2 ##################################################

#Open Query : Update-XG2

#UPDATE [TEAM HEADER] INNER JOIN [XG-Ref] AS [XG-Ref_1] ON [TEAM HEADER].[Master Customer Number] = [XG-Ref_1].MCN SET [TEAM HEADER].[Country Code] = [CountryCode], [TEAM HEADER].[Country Name] = [CountryName], [TEAM HEADER].[Area Name] = [AreaName], [TEAM HEADER].[Region Code] = [RegionName]
#WHERE ((([TEAM HEADER].[Country Code])="XG"));

XgRef = pd.read_csv('E:\_Projects\SFDC Automation\Sales Funnel input files\XG-Ref.csv' ,na_values=[''],keep_default_na=False,low_memory=False)

XgRef['MCN'] = XgRef['MCN'].astype(str)

TeamHeader['Master Customer Number'].astype(str)

TeamHeader = pd.merge(TeamHeader, XgRef, how="left", left_on="Master Customer Number", right_on="MCN")

# save updated DataFrame to CSV file
#TeamHeader.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\TeamHeader Before xg joining.csv', index=False)

mask = (TeamHeader["Country Code"] == "XG") & (TeamHeader["CountryCode"].notnull()) & (TeamHeader["CountryCode"] != "")
TeamHeader.loc[mask, ["Country Code", "Country Name", "Area Name", "Region Code"]] = \
    TeamHeader.loc[mask, ["CountryCode", "CountryName", "AreaName", "RegionName"]].values   
    
TeamHeader.loc[TeamHeader["Country Code"] == "XG", ["Region Code"]] = \
    TeamHeader.loc[TeamHeader["Country Code"] == "XG", ["RegionName"]].values
     
#TeamHeader.drop("MCN", axis=1, inplace=True)  # drop the temporary column

# save updated DataFrame to CSV file
#TeamHeader.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\TeamHeader after xg.csv', index=False)

#####print('Open Query : Update-XG2 ')
################################################## KA00 - adjust HSR SolPF ##################################################
#Update KA00

#UPDATE [TEAM HEADER] SET [TEAM HEADER].[Sol PF Code] = "HSR"
#WHERE (((Left([Org Code],3)) In ("845","852","862","867","881")) AND (([TEAM HEADER].[Sol PF Code])<>"HSR"));

# convert Org Code column to string type
TeamHeader['Org Code'] = TeamHeader['Org Code'].astype(str)

# update the data using pandas
TeamHeader.loc[(TeamHeader['Org Code'].str[:3].isin(['845', '852', '862', '867', '881'])) & (TeamHeader['Sol PF Code'] != 'HSR'), 'Sol PF Code'] = 'HSR'


# save updated DataFrame to CSV file
#TeamHeader.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\TeamHeader 3.csv', index=False)

#####print('KA00 - adjust HSR SolPF')
################################################## KA01 - adjust key accounts ##################################################

#Update KA01

#UPDATE [TEAM HEADER] INNER JOIN [key account country map] ON ([TEAM HEADER].[Country Code]=[key account country map].[Country Code]) AND ([TEAM HEADER].[Master Customer Number]=[key account country map].[Master Customer Number]) SET [TEAM HEADER].[Key Account] = [key account country map]![KEY ACCOUNT]
#WHERE ((([key account country map].[Key Account])<>[TEAM HEADER]![KEY ACCOUNT]) AND ((NZ([Exclude],"N"))<>"Y"));

key_account_country_map = pd.read_csv('E:\_Projects\SFDC Automation\Sales Funnel input files\key account country map.csv' ,na_values=[''],keep_default_na=False)

# Perform left join on 'Country Code' and 'Master Customer Number'
TeamHeader = pd.merge(TeamHeader, key_account_country_map, on=['Country Code', 'Master Customer Number'], how='left')


TeamHeader = TeamHeader.drop_duplicates(subset=['Area Name','Opportunity Number','SumOfEstimated Order Value-US','SumOfEstimated Order Value-Local','Region Code','Master Customer Number'], keep='first')

#TeamHeader.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\TeamHeader 41.csv', index=False)

condition = (TeamHeader['Key Account_x'] != TeamHeader['Key Account_y']) & (np.where(pd.isnull(TeamHeader['Exclude']), 'N', TeamHeader['Exclude']) != 'Y') & (pd.notnull(TeamHeader['Key Account_y']))

# Update 'Key Account' column with values from 'Key Account_y' column where condition is met
TeamHeader.loc[condition, 'Key Account_x'] = TeamHeader.loc[condition, 'Key Account_y']

TeamHeader = TeamHeader.rename(columns=lambda x: x.replace('_x', ''))

TeamHeader = TeamHeader.filter(regex='^(?!.*_y$)')

# save updated DataFrame to CSV file
#TeamHeader.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\TeamHeader 4.csv', index=False)

#####print('KA01 - adjust key accounts')

##################################################KA01a - adjust key accounts ##################################################

#Update KA01a

#UPDATE [TEAM HEADER] LEFT JOIN [key account country map] ON ([TEAM HEADER].[Country Code]=[key account country map].[Country Code]) AND ([TEAM HEADER].[Master Customer Number]=[key account country map].[Master Customer Number]) SET [TEAM HEADER].[Key Account] = "OTHER"
#WHERE ((([TEAM HEADER].[Country Code]) In ("BR","SA","YY","ID")) AND (([TEAM HEADER].[Key Account])<>"OTHER") AND (([key account country map].[Key Account]) Is Null));

# Perform left join on 'Country Code' and 'Master Customer Number'
TeamHeader = pd.merge(TeamHeader, key_account_country_map, on=['Country Code', 'Master Customer Number'], how='left')

TeamHeader = TeamHeader.drop_duplicates(subset=['Opportunity Number'], keep='first')

# Update rows that meet the specified conditions
mask = ((TeamHeader["Country Code"].isin(["BR", "SA", "YY", "ID"])) & 
        (TeamHeader["Key Account_x"] != "OTHER") &
        (TeamHeader["Key Account_y"].isna()))

TeamHeader.loc[mask, "Key Account_x"] = "OTHER"

TeamHeader = TeamHeader.rename(columns=lambda x: x.replace('_x', ''))

TeamHeader = TeamHeader.filter(regex='^(?!.*_y$)')

# save updated DataFrame to CSV file
#TeamHeader.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\TeamHeader 5.csv', index=False)

#####print('KA01a - adjust key accounts')
################################################## KA02 - add industry for HSR SolPF ##################################################

#Update KA02

#UPDATE [TEAM HEADER] LEFT JOIN HSR_KA_IND_Map ON [TEAM HEADER].[Key Account]=HSR_KA_IND_Map.KEY_ACCOUNT SET [TEAM HEADER].[Sol PF Code] = IIf([Sol PF Code]="A2","RSG",IIf([Sol PF Code]="B4","FSG-SS",IIf([Sol PF Code]="B1","FSG-PAY",IIf(HSR_KA_IND_Map!KEY_ACCOUNT_DESCRIPTION Is Not Null,HSR_KA_IND_Map!KEY_ACCOUNT_DESCRIPTION,[Sol PF Code]))));


HSR_KA_IND_Map = pd.read_csv('E:\_Projects\SFDC Automation\Sales Funnel input files\HSR_KA_IND_Map.csv' ,na_values=[''],keep_default_na=False)

TeamHeader = TeamHeader.merge(HSR_KA_IND_Map, left_on='Key Account',right_on='KEY_ACCOUNT', how='left')

TeamHeader = TeamHeader.rename(columns=lambda x: x.replace('_x', ''))

TeamHeader = TeamHeader.filter(regex='^(?!.*_y$)')

TeamHeader['Sol PF Code'] = np.where(
    TeamHeader['Sol PF Code'] == 'A2',
    'RSG',
    np.where(
        TeamHeader['Sol PF Code'] == 'B4',
        'FSG-SS',
        np.where(
            TeamHeader['Sol PF Code'] == 'B1',
            'FSG-PAY',
            np.where(
                TeamHeader['KEY_ACCOUNT_DESCRIPTION'].notnull(),
                TeamHeader['KEY_ACCOUNT_DESCRIPTION'],
                TeamHeader['Sol PF Code']
            )
        )
    )
)

# save updated DataFrame to CSV file
#TeamHeader.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\TeamHeader 6.csv', index=False)

#####print('KA02 - add industry for HSR SolPF')

################################################## KA03 - adjust to RSG for non-845 ##################################################

#Update KA03

#UPDATE [TEAM HEADER] SET [TEAM HEADER].[Sol PF Code] = "RSG"
#WHERE ((([TEAM HEADER].[Sol PF Code])="HSR") AND ((Left([Org Code],3)) Not In ("845","852","862","867","881")));

TeamHeader.loc[
    (TeamHeader['Sol PF Code'] == 'HSR') & (~TeamHeader['Org Code'].astype(str).str[:3].isin(['845', '852', '862', '867', '881'])), 
    'Sol PF Code'
] = 'RSG'


# save updated DataFrame to CSV file
#TeamHeader.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\TeamHeader 7.csv', index=False)

#####print('KA03 - adjust to RSG for non-845')

################################################## XSELL00 ##################################################
#XSELL00

#SELECT [TEAM DETAIL_RECEIPTS].[Opportunity Number], "" AS XSELL INTO XSell_ref
#FROM ([TEAM HEADER] INNER JOIN [TEAM DETAIL_RECEIPTS] ON [TEAM HEADER].[Opportunity Number] = [TEAM DETAIL_RECEIPTS].[Opportunity Number]) INNER JOIN [Product Family Map] ON [TEAM DETAIL_RECEIPTS].[Top Line Product Name] = [Product Family Map].[Top Line Product]
#WHERE ((([Product Family Map].Family)<>"Do Not Report On"))
#GROUP BY [TEAM DETAIL_RECEIPTS].[Opportunity Number], "";

product_family_map = pd.read_csv('E:\_Projects\SFDC Automation\Input\Product Family Map.csv' ,na_values=[''],keep_default_na=False)


# Merge the tables on the  columns
merged = pd.merge(TeamHeader, TEAM_DETAIL_RECEIPTS, on='Opportunity Number')

merged = pd.merge(merged, product_family_map, left_on='Top Line Product Name', right_on='Top Line Product',how='left')

merged = merged.rename(columns=lambda x: x.replace('_x', ''))

merged = merged.filter(regex='^(?!.*_y$)')

# Filter out rows with Family = 'Do Not Report On'
merged = merged[merged['Family'] != 'Do Not Report On']

# Group by Opportunity Number and create a new dataframe
XSell_ref = merged.groupby('Opportunity Number').agg({'Opportunity Number': 'first'}).reset_index(drop=True)

XSell_ref['XSELL']=""

# save updated DataFrame to CSV file
#XSell_ref.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\XSell_ref 1.csv', index=False)

#####print('XSELL00')
################################################## XSELL01 ##################################################
#XSELL01

#UPDATE (([TEAM HEADER] INNER JOIN [TEAM DETAIL_RECEIPTS] ON [TEAM HEADER].[Opportunity Number]=[TEAM DETAIL_RECEIPTS].[Opportunity Number]) INNER JOIN [Product Family Map] ON [TEAM DETAIL_RECEIPTS].[Top Line Product Name]=[Product Family Map].[Top Line Product]) INNER JOIN XSell_ref ON [TEAM HEADER].[Opportunity Number]=XSell_ref.[Opportunity Number] SET XSell_ref.XSELL = "XSELL"
#WHERE (((Left([Org Code],3)) In ("845","852","862","867","881")) AND (([Product Family Map].[BU Code])="RSG") AND (([Product Family Map].Family)<>"Do Not Report On"));
# join the dataframes using inner join
df = pd.merge(pd.merge(pd.merge(TeamHeader, TEAM_DETAIL_RECEIPTS, on='Opportunity Number'), 
                       product_family_map, left_on='Top Line Product Name', right_on='Top Line Product', how='left'), 
              XSell_ref, on='Opportunity Number')


#df = df.rename(columns=lambda x: x.replace('_x', ''))

df.columns = [col.replace('_x','') for col in df.columns]

df = df.filter(regex='^(?!.*_y$)')


# filter the joined dataframe based on the conditions
df = df[df['Org Code'].astype(str).str[:3].isin(['845', '852', '862', '867', '881']) &
        (df['BU Code'] == 'RSG') &
        (df['Family'] != 'Do Not Report On')]

# update the XSELL column in the filtered dataframe
df.loc[:, 'XSELL'] = 'XSELL'

# update the XSell_ref dataframe with the updated values
XSell_ref.update(df[['Opportunity Number', 'XSELL']])


# save updated DataFrame to CSV file
#XSell_ref.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\XSell_ref 2.csv', index=False)

#####print('XSELL01')
################################################# XSELL02 ##################################################
#XSELL02

#UPDATE (([TEAM HEADER] INNER JOIN [TEAM DETAIL_RECEIPTS] ON [TEAM HEADER].[Opportunity Number] = [TEAM DETAIL_RECEIPTS].[Opportunity Number]) INNER JOIN [Product Family Map] ON [TEAM DETAIL_RECEIPTS].[Top Line Product Name] = [Product Family Map].[Top Line Product]) INNER JOIN XSell_ref ON [TEAM HEADER].[Opportunity Number] = XSell_ref.[Opportunity Number] SET XSell_ref.XSELL = "XSELL"
#WHERE ((([Product Family Map].Family)<>"Do Not Report On") AND ((Left([Org Code],3)) Not In ("845","852","862","867","881")) AND (([Product Family Map].[BU Code])="HSR"));


# update the XSell_ref.XSELL column
XSell_ref.loc[(merged['Family'] != 'Do Not Report On') & 
           (~merged['Org Code'].astype(str).str[:3].isin(['845', '852', '862', '867', '881'])) & 
           (merged['BU Code'] == 'HSR'), 'XSELL'] = 'XSELL'




# save updated DataFrame to CSV file
#XSell_ref.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\XSell_ref 3.csv', index=False)

#####print('XSELL02')
################################################# Div-Override ##################################################
#Div-Override

#UPDATE [TEAM HEADER] INNER JOIN [OPP-ORIDE VALUES] ON [TEAM HEADER].[Opportunity Number] = [OPP-ORIDE VALUES].Opportunity SET [TEAM HEADER].[Industry Group Code] = [INDUSTRY];


opp_oride_values = pd.read_csv('E:\_Projects\SFDC Automation\Sales Funnel input files\OPP-ORIDE VALUES.csv' ,na_values=[''],keep_default_na=False)

# create a boolean mask to identify rows in team_header where Opportunity Number matches Opportunity in opp_oride_values
mask = TeamHeader['Opportunity Number'].isin(opp_oride_values['Opportunity'])

# update the Industry Group Code column in team_header using the mask
TeamHeader.loc[mask, 'Industry Group Code'] = opp_oride_values.set_index('Opportunity').loc[TeamHeader.loc[mask, 'Opportunity Number'], 'Industry'].values

# save updated DataFrame to CSV file
#TeamHeader.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\TeamHeader 8.csv', index=False)

#####print(' Div-Override')
################################################# Div-Override2 ##################################################
#Div-Override2

#UPDATE [TEAM HEADER] INNER JOIN [OPP-ORIDE VALUES] ON [TEAM HEADER].[Master Customer Number] = [OPP-ORIDE VALUES].MCN SET [TEAM HEADER].[Industry Group Code] = [INDUSTRY];

TeamHeader['Master Customer Number'] = TeamHeader['Master Customer Number'].astype(str)

# create a boolean mask to identify rows in team_header where Opportunity Number matches Opportunity in opp_oride_values
mask = TeamHeader['Master Customer Number'].isin(opp_oride_values['MCN'])

# update the Industry Group Code column in team_header using the mask
TeamHeader.loc[mask, 'Industry Group Code'] = opp_oride_values.set_index('MCN').loc[TeamHeader.loc[mask, 'Master Customer Number'], 'Industry'].values


TeamHeader.loc[TeamHeader['Master Customer Number'].isin(('4638813','6634578')),'Industry Group Code']='FIN'
TeamHeader.loc[TeamHeader['Master Customer Number'].isin(('7424928','7349806')),'Industry Group Code']='RET'



# save updated DataFrame to CSV file
#TeamHeader.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\TeamHeader 9.csv', index=False)



####print('Div-Override2')
##################################################### Modification ####################################################

#TeamHeader[['Estimated Win Probability','Estimated Win Probability-Baseline','Total Order Value - US','Total Order Value - Local','Total Order Value - US (Baseline)','Total Receipt Value - US','Total Receipt Value - Local','Total Receipt Value - US (Baseline)','Total Revenue Value - US','Total Revenue Value - Local','Total Revenue Value - US (Baseline)','Solution Value-US','Solution Value-Local']]=TeamHeader[['Estimated Win Probability','Estimated Win Probability-Baseline','Total Order Value - US','Total Order Value - Local','Total Order Value - US (Baseline)','Total Receipt Value - US','Total Receipt Value - Local','Total Receipt Value - US (Baseline)','Total Revenue Value - US','Total Revenue Value - Local','Total Revenue Value - US (Baseline)','Solution Value-US','Solution Value-Local']].round(2)

TeamHeader[['Estimated Win Probability','Estimated Win Probability-Baseline','Total Receipt Value - US','Total Receipt Value - Local','Total Receipt Value - US (Baseline)']]=TeamHeader[['Estimated Win Probability','Estimated Win Probability-Baseline','Total Receipt Value - US','Total Receipt Value - Local','Total Receipt Value - US (Baseline)']].round(2)

TeamHeader[['Opportunity Value - US','Opportunity Value - Local','Opportunity Value - US (Baseline)']]=TeamHeader[['Opportunity Value - US','Opportunity Value - Local','Opportunity Value - US (Baseline)']].round(0)

TeamHeader['Record Date'] = pd.to_datetime(TeamHeader['Record Date']).dt.date


TeamHeader[['Total Order Value - US','Total Order Value - Local','Total Order Value - US (Baseline)','Total Revenue Value - US','Total Revenue Value - Local','Total Revenue Value - US (Baseline)']] = TeamHeader[['Total Order Value - US','Total Order Value - Local','Total Order Value - US (Baseline)','Total Revenue Value - US','Total Revenue Value - Local','Total Revenue Value - US (Baseline)']].replace('nan', '0')

# define a formatting function that formats each value to display only two decimal places
def format_value(value):
    return f'{value:.2f}'

# apply the formatting function to each column in the dataframe
TeamHeader[['Total Order Value - US','Total Order Value - Local','Total Order Value - US (Baseline)','Total Revenue Value - US','Total Revenue Value - Local','Total Revenue Value - US (Baseline)','Solution Value-US','Solution Value-Local','Annuity Value-US','Annuity Value-Local']]=TeamHeader[['Total Order Value - US','Total Order Value - Local','Total Order Value - US (Baseline)','Total Revenue Value - US','Total Revenue Value - Local','Total Revenue Value - US (Baseline)','Solution Value-US','Solution Value-Local','Annuity Value-US','Annuity Value-Local']].applymap(format_value)


# save updated DataFrame to CSV file
TeamHeader[['Sol PF Code', 'Region Code', 'Area Name', 'Country Code', 'Country Name', 'Key Account', 'Master Customer Number', 'Master Customer Name', 'Local Customer Name', 'Marketing Prog Name', 'Opportunity Number', 'Owner Name', 'Owner Manager Name', 'Opportunity Description', 'Phase Code-Current', 'Phase Code-Baseline', 'Phase Code Chg Date', 'Status Description', 'Status Description-Baseline', 'Status Chg Date', 'Date Created', 'Estimated Win Probability', 'Estimated Win Probability-Baseline', 'Est Win Prob Chg Dt', 'Currency Code', 'Opportunity Value - US', 'Opportunity Value - Local', 'Opportunity Value - US (Baseline)', 'Record Date', 'Org Code', 'Date Closed', 'Actual Close Mth', 'Estimated Close Mth', 'Estimated Order Mth', 'FSD DP Flag', 'Total Order Value - US', 'Total Order Value - Local', 'Total Order Value - US (Baseline)', 'Total Receipt Value - US', 'Total Receipt Value - Local', 'Total Receipt Value - US (Baseline)', 'Total Revenue Value - US', 'Total Revenue Value - Local', 'Total Revenue Value - US (Baseline)', 'Flag1', 'Incomplete Opps', 'Expected Book Date', 'Revenue Trigger', 'As of Date', 'Pull Forward Date', 'Include In Forecast Indicator', 'Primary Quote Number', 'Solution Value-US', 'Solution Value-Local', 'Annuity Value-US', 'Annuity Value-Local', 'Opportunity Type Desc', 'Advocated Solution ID', 'Industry Group Code', 'Customer Industry Code', 'Last Modified By User', 'Risk Comment Text', 'Top Deal Indicator', 'Last Modified Date Time', 'Risk Commit Color Text', 'Selling Stage Name', 'Executive Sponsor Description', 'Competitor Status Description', 'Competitor Comment Text', 'PBO Flag', 'PBO Order Number', 'Master Distributor Name', 'Fulfilled By Partner', 'Reseller Name', 'Primary Campaign Name', 'Contractual Commit']].to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Team Header Final.csv', index=False)

TeamHeader.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Team Header receipt.csv', index=False)

print("Step 4 is completed")