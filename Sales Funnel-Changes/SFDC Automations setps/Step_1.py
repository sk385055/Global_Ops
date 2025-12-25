import pandas as pd
from numpy import integer


# RaceTeamOrders = pd.read_csv("C:\Vasanthan\Race\Global Study\Tables Exported Files\Race CSV\RACE_TEAM_Orders.csv")
RaceTeamOrders = pd.read_csv("E:\_Projects\SFDC Automation\Input\RACE_TEAM_Orders.csv", dtype={'column44': str, 'column46': str},  na_values=[''],keep_default_na=False,low_memory=False)

RegionCodeMap2 = pd.read_csv("E:\_Projects\SFDC Automation\Sales Funnel input files\RegionCodeMap2.csv" ,na_values=[''],keep_default_na=False)

LoadTeamlatest2axgcountrycorrection = pd.read_csv("E:\_Projects\SFDC Automation\Sales Funnel input files\Load Team latest2a - xg country correction.csv" ,na_values=[''],keep_default_na=False)

#RaceTeamOrders[['Expected Book Date','Date Created','As Of Date','Date Closed','Last Modified Date Time']] = pd.to_datetime(RaceTeamOrders[['Expected Book Date','Date Created','As Of Date','Date Closed','Last Modified Date Time']]).dt.date

date_cols = ['Expected Book Date','Date Created','Expected Book Date','As Of Date']

for col in date_cols:
    RaceTeamOrders[col] = pd.to_datetime(RaceTeamOrders[col]).dt.date


# Merge RaceTeamOrders and RegionCodeMap2 based on Country Code
RaceTeamOrdersCountryCodeJoin = pd.merge(RaceTeamOrders, RegionCodeMap2, left_on="Country Code", right_on="Ctry Code", how="left")


# Convert "MCS MCN" to object type and merge RaceTeamOrdersCountryCodeJoin and Load Team latest2a - xg country correction based on Master Customer Number
LoadTeamlatest2axgcountrycorrection["MCS MCN"] = LoadTeamlatest2axgcountrycorrection["MCS MCN"].astype(str)

RaceTeamOrdersCountryCodeJoin['Master Customer Number'] = RaceTeamOrdersCountryCodeJoin['Master Customer Number'].astype(str)

# Merge RaceTeamOrders and Load Team latest2a - xg country correction based on Master Customer Number
RaceTeamOrders = pd.merge(RaceTeamOrdersCountryCodeJoin, LoadTeamlatest2axgcountrycorrection, left_on="Master Customer Number", right_on="MCS MCN", how="left")

# Filter out records where the first three characters of the "top line product name" field are "IPS"
RaceTeamOrders = RaceTeamOrders[~RaceTeamOrders['Top Line Product Name'].fillna('NA').str[:3].isin(['IPS']) & ~RaceTeamOrders['Top Line Product Name'].fillna('NA').str[:2].isin(['A-'])]

#RaceTeamOrders.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\RaceTeamOrders.csv', index=False)

def get_region(row):
    rows = row['MCS MCN']
    if isinstance(rows, str) and len(rows) > 0:
        return row['MCSreg']
    elif row['Theater Code'] == 'T0':
        return 'XG'
    else:
        return row['Region Code']

    
# Apply the function to create a new 'Region' column
RaceTeamOrders['Region'] = RaceTeamOrders.apply(get_region, axis=1)


# IIf(Len([mcs mcn])>0,[MCSCtry],[race_team_orders]![country code]) AS Ctry, 
def get_city(row):
    rows = row['MCS MCN']
    if isinstance(rows, str) and len(rows) > 0:
        return row['MCSCtry']
    else:
        return row['Country Code']

    
# Apply the function to create a new 'Region' column
RaceTeamOrders['Ctry'] = RaceTeamOrders.apply(get_city, axis=1)


# IIf([currency code ent]="USD",[opportunity value-ent],[opportunity value-us]) AS [Opp-US]
def get_opp(row):
    if row['Currency Code Ent'] == 'USD':
        
        return row['Opportunity Value-Ent']
    else:
        return row['Opportunity Value-US']

  
RaceTeamOrders['Opp-US'] = RaceTeamOrders.apply(get_opp, axis=1) 


# IIf([Extended Product Value-US] Is Null,0,[Extended Product Value-US]) AS [Order Val-US],
def get_Order_Val_US(row):
    if row['Extended Product Value-US'] is None or row['Extended Product Value-US']=='':
        return 0
    else:
        return row['Extended Product Value-US']


RaceTeamOrders['Order Val-US'] = RaceTeamOrders.apply(get_Order_Val_US, axis=1)
        

# IIf([Extended Product Value-Ent] Is Null,0,[Extended Product Value-Ent]) AS [Order Val-Ent], 
def get_Order_Ent(row):
    if row['Extended Product Value-Ent'] is None or row['Extended Product Value-Ent']=='':
       return 0
    else:
       return row['Extended Product Value-Ent']

        
RaceTeamOrders['Order Val-Ent'] = RaceTeamOrders.apply(get_Order_Ent, axis=1)


# IIf([Quantity] Is Null,0,[Quantity]) AS [Ord Units], 
RaceTeamOrders['Ord Units'] = RaceTeamOrders['Quantity'].fillna(0)


# IIf([currency code ent]="USD",[opportunity solution value-ent],[opportunity solution value-us]) AS [Opp-Sol-US],
def get_Opp_Sol_Us(row):
    if row['Currency Code Ent'] == 'USD':
        return row['Opportunity Solution Value-Ent']
    else:
        return row['Opportunity Solution Value-US']

        
RaceTeamOrders['Opp-Sol-US'] = RaceTeamOrders.apply(get_Opp_Sol_Us, axis=1)


# IIf([currency code ent]="USD",[opportunity annuity value-ent],[opportunity annuity value-us]) AS [Opp-Ann-US], 
def get_Opp_Ann_Us(row):
    if row['Currency Code Ent'] == 'USD':
        return row['Opportunity Annuity Value-Ent']
    else:
        return row['Opportunity Annuity Value-US']

        
RaceTeamOrders['Opp-Ann-US'] = RaceTeamOrders.apply(get_Opp_Ann_Us, axis=1)
  

#RACE_TEAM_Orders![Opportunity Number] & "-" & RACE_TEAM_Orders![Product Sequence Number] AS [Ref Opp Line],
RaceTeamOrders['Ref Opp Line'] = RaceTeamOrders['Opportunity Number'].astype(str) + '-' + RaceTeamOrders['Product Sequence Number'].astype(str)

# [Extended Product Value-US]/[Quantity] AS [Unit Value-US], 
RaceTeamOrders['Unit Value-US'] = RaceTeamOrders['Extended Product Value-US'] / RaceTeamOrders['Quantity']

# [Extended Product Value-Ent]/[Quantity] AS [Unit Value-Ent], 
RaceTeamOrders['Unit Value-Ent'] = RaceTeamOrders['Extended Product Value-Ent'] / RaceTeamOrders['Quantity']

#RaceTeamOrders.to_csv("E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\RaceTeamOrders.csv", index=False)
#print("Exported")

Team = pd.DataFrame()
# assign the 'Region' column from RaceTeamOrders to the 'Region code' column in Team
Team['Region code'] = RaceTeamOrders['Region']
Team['Country Code'] = RaceTeamOrders['Ctry']
Team['Key Account'] = RaceTeamOrders['Key Account Name']
Team['Marketing Program Name'] = RaceTeamOrders['Marketing Program Name']
Team['Master Customer Number'] = RaceTeamOrders['Master Customer Number']
Team['Master Customer Name'] = RaceTeamOrders['Master Customer Name']
Team['Master Customer Name-Local'] = RaceTeamOrders['Master Customer Name-Local']
Team['Opportunity Number'] = RaceTeamOrders['Opportunity Number']
Team['Product Sequence Number'] = RaceTeamOrders['Product Sequence Number']
Team['Owner Name'] = RaceTeamOrders['Owner Name']
Team['Owner Manager Name'] = RaceTeamOrders['Owner Manager Name']
Team['Top Line Product Name'] = RaceTeamOrders['Top Line Product Name']
Team['Product Group Name'] = RaceTeamOrders['Product Group Name']
Team['Phase Code-Current'] = RaceTeamOrders['Forecast Status Code-Current']
Team['Phase Description-Current'] = RaceTeamOrders['Forecast Status Description-Current'].str.upper()
Team['Estimated Close Acct Period'] = RaceTeamOrders['Expected Book Acct Period']
Team['Estimated Win Probability'] = RaceTeamOrders['Estimated Win Probability']
Team['Date Created'] = RaceTeamOrders['Date Created']
Team['Opportunity Value-US'] = RaceTeamOrders['Opp-US']
Team['Opportunity Value-Local'] = RaceTeamOrders['Opportunity Value-Ent']
Team['Estimated Order Value-US'] = RaceTeamOrders['Order Val-US']
Team['Estimated Order Value-Local'] = RaceTeamOrders['Order Val-Ent']
Team['Date Closed'] = RaceTeamOrders['Date Closed']
Team['Solution Team Code'] = RaceTeamOrders['Solution Team Code']
Team['Opportunity Description'] = RaceTeamOrders['Opportunity Description']
Team['Estimated Order Units'] = RaceTeamOrders['Ord Units']
Team['Estimated Order Acct Period'] = RaceTeamOrders['Expected Book Acct Period']
Team['Ref-Opp-Line'] = RaceTeamOrders['Ref Opp Line']
Team['Currency Code'] = RaceTeamOrders['Currency Code Ent']
Team['Expected Book Date'] = RaceTeamOrders['Expected Book Date']
Team['Revenue Trigger'] = RaceTeamOrders['Revenue Trigger']
Team['As Of Date'] = RaceTeamOrders['As Of Date']
Team['Status Code'] = RaceTeamOrders['Opportunity Status Code']
Team['Status Description'] = RaceTeamOrders['Opportunity Status Description']
Team['Solution Value-US'] = RaceTeamOrders['Opp-Sol-US']
Team['Solution Value-Local'] = RaceTeamOrders['Opportunity Solution Value-Ent']
Team['Annuity Value-US'] = RaceTeamOrders['Opp-Ann-US']
Team['Annuity Value-Local'] = RaceTeamOrders['Opportunity Annuity Value-Ent']
Team['Unit Value-US'] = RaceTeamOrders['Unit Value-US']
Team['Unit Value-Local'] = RaceTeamOrders['Unit Value-Ent']
Team['Opportunity Type Desc'] = RaceTeamOrders['Opportunity Type Desc']
Team['Industry Group Code'] = RaceTeamOrders['Industry Group Code']
Team['Customer Industry Code'] = RaceTeamOrders['Customer Industry Code']
Team['Last Modified By User'] = RaceTeamOrders['Last Modified By User']
Team['Last Modified Date Time'] = RaceTeamOrders['Last Modified Date Time']
Team['Risk Comment Text'] = RaceTeamOrders['Risk Comment Text']
Team['Risk Commit Color Text'] = RaceTeamOrders['Risk Commit Color Text']
Team['Selling Stage Name'] = RaceTeamOrders['Selling Stage Name']
Team['Master Distributor Name'] = RaceTeamOrders['Master Distributor Name']
Team['Fulfilled By Partner'] = RaceTeamOrders['Fulfilled by Partner']
Team['Reseller Name'] = RaceTeamOrders['Reseller Name']
Team['Primary Campaign Name'] = RaceTeamOrders['Primary Campaign Name']
Team['PBO Order Number'] = RaceTeamOrders['Pre Build Order Number']
Team['PBO Flag'] = RaceTeamOrders['Pre Build Flag']
Team['Contractual Commit'] = RaceTeamOrders['Contractual Commit Indicator']


# Save the updated DataFrame back
#Team.to_csv(r"E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Team Data Orders.csv", index=False)

##################################################Temp NSC fix - opps with close date that are not closed##########################################################
#Open Query : NSC-not closed
#UPDATE [Team Data_Orders] SET [Team Data_Orders].[Date Closed] = [As Of Date]
#WHERE ((([Team Data_Orders].[Date Closed]) Is Not Null) AND (([Team Data_Orders].[Status Description])<>"WON" And ([Team Data_Orders].[Status Description])<>"LOST" And ([Team Data_Orders].[Status Description])<>"DISCONTINUED"));


# Create a mask to filter the rows based on the specified conditions
NSCNotClosed = (Team["Date Closed"].notnull()) & (Team["Status Description"].isin(["WON", "LOST", "DISCONTINUED"]) == False)

# Update the "Date Closed" column with the "As Of Date" value for the filtered rows
Team.loc[NSCNotClosed, "Date Closed"] = Team['As Of Date']



# Save the updated DataFrame back
#Team.to_csv(r"E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Team Data Orders.csv", index=False)



################################################## Cloud change to eCOM ##########################################################
#Open Query : NSC-cloud
#UPDATE [Team Data_Orders] SET [Team Data_Orders].[Product Group Name] = "eCOM" WHERE ((([Team Data_Orders].[Product Group Name])="cloud"));

# Update the rows where Product Group Name is "cloud"
Team.loc[Team['Product Group Name'] == 'cloud', 'Product Group Name'] = 'eCOM'




# Save the updated DataFrame back
Team.to_csv(r"E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Team Data Orders.csv", index=False)



print('Step 1 is completed')





















