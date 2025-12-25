import Step_2
import pandas as pd

country_map = pd.read_csv("E:\_Projects\SFDC Automation\Sales Funnel input files\Country Map.csv", na_values=[''],keep_default_na=False)
TeamDataOrders = pd.read_csv("E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Team Data Orders.csv", na_values=[''],keep_default_na=False,low_memory=False)


# Join relevant tables
grouped_data = pd.merge(TeamDataOrders, country_map, left_on="Country Code", right_on="Country Code", how="left")


grouped_data.loc[grouped_data['CF_Region2011'].notnull() & (grouped_data['CF_Region2011'] != ''), 'Region code'] = grouped_data['CF_Region2011']

grouped_data.loc[grouped_data['CF_Area2011'].notnull() & (grouped_data['CF_Area2011'] != ''), 'Area Name'] = grouped_data['CF_Area2011']



grouped_data["Advocated Solution ID"] = None
grouped_data["Solution Portfolio Code"] = None
grouped_data["Top Deal Indicator"] = None
grouped_data["Product Line Number"] = None
grouped_data["Executive Sponsor Description"] = None
grouped_data["Competitor Status Description"] = None
grouped_data["Competitor Comment Text"] = None



grouped_data["Expr1"] = pd.Timestamp.today()
grouped_data["Expr2"] = grouped_data['Phase Code-Current'].astype(str) + '-' + grouped_data['Phase Description-Current']
grouped_data["Expr3"] = grouped_data['Phase Code-Current'].astype(str) + '-' + grouped_data['Phase Description-Current']
grouped_data["Expr4"] = "N"
grouped_data["Include In Forecast"] = 1

TeamHeader = pd.DataFrame()

# assign column from RaceTeamOrders 
TeamHeader['Sol PF Code'] = grouped_data['Solution Portfolio Code']
TeamHeader['Region Code'] = grouped_data['Region code']
TeamHeader['Area Name'] = grouped_data['Area Name']
TeamHeader['Country Name'] = grouped_data['Country Name']
TeamHeader['Country Code'] = grouped_data['Country Code']
TeamHeader['Key Account'] = grouped_data['Key Account']
TeamHeader['Master Customer Number'] = grouped_data['Master Customer Number']
TeamHeader['Master Customer Name'] = grouped_data['Master Customer Name']
TeamHeader['Opportunity Number'] = grouped_data['Opportunity Number']
TeamHeader['Owner Name'] = grouped_data['Owner Name']
TeamHeader['Opportunity Description'] = grouped_data['Opportunity Description']
TeamHeader['Phase Code-Current'] = grouped_data['Expr2']
TeamHeader['Date Created'] = grouped_data['Date Created']
TeamHeader['Estimated Win Probability'] = grouped_data['Estimated Win Probability']
TeamHeader['Record Date'] = grouped_data['Expr1']
TeamHeader['Org Code'] = grouped_data['Solution Team Code']
TeamHeader['Date Closed'] = grouped_data['Date Closed']
TeamHeader['Phase Code-Baseline'] = grouped_data['Expr3']
TeamHeader['Status Description-Baseline'] = grouped_data['Status Description']
TeamHeader['Estimated Win Probability-Baseline'] = grouped_data['Estimated Win Probability']
TeamHeader['Status Description'] = grouped_data['Status Description']
TeamHeader['Marketing Prog Name'] = grouped_data['Marketing Program Name']
TeamHeader['Local Customer Name'] = grouped_data['Master Customer Name-Local']
TeamHeader['Opportunity Value - US'] = grouped_data['Opportunity Value-US']
TeamHeader['Opportunity Value - Local'] = grouped_data['Opportunity Value-Local']
TeamHeader['Opportunity Value - US (Baseline)'] = grouped_data['Opportunity Value-US']
TeamHeader['Estimated Close Mth'] = grouped_data['Estimated Close Acct Period']
TeamHeader['Incomplete Opps'] = grouped_data['Expr4']
TeamHeader['Currency Code'] = grouped_data['Currency Code']
TeamHeader['Expected Book Date'] = grouped_data['Expected Book Date']
TeamHeader['Revenue Trigger'] = grouped_data['Revenue Trigger']
TeamHeader['As of Date'] = grouped_data['As Of Date']
TeamHeader['Estimated Order Mth'] = grouped_data['Estimated Close Acct Period']
TeamHeader['Solution Value-US'] = grouped_data['Solution Value-US']
TeamHeader['Solution Value-Local'] = grouped_data['Solution Value-Local']
TeamHeader['Annuity Value-US'] = grouped_data['Annuity Value-US']
TeamHeader['Annuity Value-Local'] = grouped_data['Annuity Value-Local']
TeamHeader['Opportunity Type Desc'] = grouped_data['Opportunity Type Desc']
TeamHeader['Advocated Solution ID'] = grouped_data['Advocated Solution ID']
TeamHeader['Industry Group Code'] = grouped_data['Industry Group Code']
TeamHeader['Customer Industry Code'] = grouped_data['Customer Industry Code']
TeamHeader['Last Modified By User'] = grouped_data['Last Modified By User']
TeamHeader['Risk Comment Text'] = grouped_data['Risk Comment Text']
TeamHeader['Top Deal Indicator'] = grouped_data['Top Deal Indicator']
TeamHeader['Last Modified Date Time'] = grouped_data['Last Modified Date Time']
TeamHeader['Risk Commit Color Text'] = grouped_data['Risk Commit Color Text']
TeamHeader['Selling Stage Name'] = grouped_data['Selling Stage Name']
TeamHeader['Executive Sponsor Description'] = grouped_data['Executive Sponsor Description']
TeamHeader['Competitor Status Description'] = grouped_data['Competitor Status Description']
TeamHeader['Competitor Comment Text'] = grouped_data['Competitor Comment Text']
TeamHeader['PBO Flag'] = grouped_data['PBO Flag']
TeamHeader['PBO Order Number'] = grouped_data['PBO Order Number']
TeamHeader['Master Distributor Name'] = grouped_data['Master Distributor Name']
TeamHeader['Fulfilled By Partner'] = grouped_data['Fulfilled By Partner']
TeamHeader['Reseller Name'] = grouped_data['Reseller Name']
TeamHeader['Include In Forecast Indicator'] = grouped_data['Include In Forecast']
TeamHeader['Primary Campaign Name'] = grouped_data['Primary Campaign Name']
TeamHeader['Contractual Commit'] = grouped_data['Contractual Commit']
TeamHeader['Owner Manager Name'] = grouped_data['Owner Manager Name']

#Add extra column with zero values
TeamHeader["Phase Code Chg Date"] = None
TeamHeader["Status Chg Date"] = None
TeamHeader["Est Win Prob Chg Dt"] = None
TeamHeader["Actual Close Mth"] = None
TeamHeader["FSD DP Flag"] = None
TeamHeader["Total Order Value - US"] = '0'
TeamHeader["Total Order Value - Local"] = '0'
TeamHeader["Total Order Value - US (Baseline)"] = '0'
TeamHeader["Total Receipt Value - US"] = '0'
TeamHeader["Total Receipt Value - Local"] = '0'
TeamHeader["Total Receipt Value - US (Baseline)"] = '0'
TeamHeader["Total Revenue Value - US"] = '0'
TeamHeader["Total Revenue Value - Local"] = '0'
TeamHeader["Total Revenue Value - US (Baseline)"] = '0'
TeamHeader["Flag1"] = None
TeamHeader["Pull Forward Date"] = None
TeamHeader["Primary Quote Number"] = '0'


# Filter rows
#TeamHeader = TeamHeader[(TeamHeader['Country Code'] != 'Country Code') & (TeamHeader['Opportunity Number'].isnull())]

# Group by relevant columns

grouped_df = TeamHeader.groupby([
    'Sol PF Code', 'Region Code', 'Area Name',  'Country Code','Country Name',
    'Key Account', 'Master Customer Number', 'Master Customer Name','Local Customer Name','Marketing Prog Name', 'Opportunity Number',
    'Owner Name', 'Owner Manager Name', 'Opportunity Description','Phase Code-Current' , 'Phase Code-Baseline','Phase Code Chg Date','Status Description','Status Description-Baseline',
    'Status Chg Date','Date Created', 'Estimated Win Probability', 'Estimated Win Probability-Baseline','Est Win Prob Chg Dt', 'Currency Code','Opportunity Value - US',
    'Opportunity Value - Local', 'Opportunity Value - US (Baseline)','Record Date','Org Code','Date Closed','Actual Close Mth','Estimated Close Mth','Estimated Order Mth',
    'FSD DP Flag','Total Order Value - US','Total Order Value - Local','Total Order Value - US (Baseline)','Total Receipt Value - US','Total Receipt Value - Local','Total Receipt Value - US (Baseline)',
    'Total Revenue Value - US','Total Revenue Value - Local','Total Revenue Value - US (Baseline)','Flag1','Incomplete Opps', 'Expected Book Date', 'Revenue Trigger', 'As of Date',
    'Pull Forward Date','Include In Forecast Indicator','Primary Quote Number','Solution Value-US', 'Solution Value-Local', 'Annuity Value-US', 'Annuity Value-Local',
    'Opportunity Type Desc', 'Advocated Solution ID', 'Industry Group Code', 'Customer Industry Code',
    'Last Modified By User', 'Risk Comment Text', 'Top Deal Indicator', 'Last Modified Date Time',
    'Risk Commit Color Text', 'Selling Stage Name', 'Executive Sponsor Description',
    'Competitor Status Description', 'Competitor Comment Text', 'PBO Flag', 'PBO Order Number',
    'Master Distributor Name', 'Fulfilled By Partner', 'Reseller Name','Primary Campaign Name', 'Contractual Commit'
    
    
      ]).groups
      
# Convert the grouped data to a DataFrame
TeamHeader = pd.DataFrame(list(grouped_df.keys()), columns=['Sol PF Code', 'Region Code', 'Area Name',  'Country Code','Country Name',
    'Key Account', 'Master Customer Number', 'Master Customer Name','Local Customer Name','Marketing Prog Name', 'Opportunity Number',
    'Owner Name', 'Owner Manager Name', 'Opportunity Description','Phase Code-Current' , 'Phase Code-Baseline','Phase Code Chg Date','Status Description','Status Description-Baseline',
    'Status Chg Date','Date Created', 'Estimated Win Probability', 'Estimated Win Probability-Baseline','Est Win Prob Chg Dt', 'Currency Code','Opportunity Value - US',
    'Opportunity Value - Local', 'Opportunity Value - US (Baseline)','Record Date','Org Code','Date Closed','Actual Close Mth','Estimated Close Mth','Estimated Order Mth',
    'FSD DP Flag','Total Order Value - US','Total Order Value - Local','Total Order Value - US (Baseline)','Total Receipt Value - US','Total Receipt Value - Local','Total Receipt Value - US (Baseline)',
    'Total Revenue Value - US','Total Revenue Value - Local','Total Revenue Value - US (Baseline)','Flag1','Incomplete Opps', 'Expected Book Date', 'Revenue Trigger', 'As of Date',
    'Pull Forward Date','Include In Forecast Indicator','Primary Quote Number','Solution Value-US', 'Solution Value-Local', 'Annuity Value-US', 'Annuity Value-Local',
    'Opportunity Type Desc', 'Advocated Solution ID', 'Industry Group Code', 'Customer Industry Code',
    'Last Modified By User', 'Risk Comment Text', 'Top Deal Indicator', 'Last Modified Date Time',
    'Risk Commit Color Text', 'Selling Stage Name', 'Executive Sponsor Description',
    'Competitor Status Description', 'Competitor Comment Text', 'PBO Flag', 'PBO Order Number',
    'Master Distributor Name', 'Fulfilled By Partner', 'Reseller Name','Primary Campaign Name', 'Contractual Commit'])



# Save file
TeamHeader.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Team Header.csv',index=False)


print('Step 3 is completed')