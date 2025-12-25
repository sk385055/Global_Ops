import Step_1
import pandas as pd
import numpy as np
from pandas._libs import index

# RaceTeamOrders = pd.read_csv("E:\_Projects\SFDC Automation\Sales Funnel input files\RACE_TEAM_Orders.csv")
RACE_Team_Receipts = pd.read_csv("E:\_Projects\SFDC Automation\Input\RACE_Team_Receipts.csv" ,na_values=[''],keep_default_na=False)

FinMth = pd.read_csv("E:\_Projects\SFDC Automation\Sales Funnel input files\Fin Mth.csv" ,na_values=[''],keep_default_na=False)

#RACE_Team_Receipts[['Estimated Receipt Date','Expected Delivery Date']] = pd.to_datetime(RACE_Team_Receipts[['Estimated Receipt Date','Expected Delivery Date']]).dt.date
###################################################### Load Receipts ######################################################
#Open Query : Load Team latest4

#INSERT INTO [Team Data_Receipts] ( [Ref Opp-Line], [Estimated Receipt Acct Period], [Estimated Receipt Units], [Estimated Receipt Value-US], [Estimated Receipt Value-Local], [Opportunity Number], [Product Sequence Number], [Top Line Product Name], [Product Line Number], [Estimated Receipt Date], [Expected Delivery Date], [Rollout Checkbox], [Currency Code], [Unassigned Receipt Units] )
#SELECT RACE_Team_Receipts![Opportunity Number] & "-" & RACE_Team_Receipts![Product Sequence Number] AS [Ref-OppLine], RACE_Team_Receipts.[Estimated Receipt Acct Period], CDbl(Nz([Estimated Receipt Units],0)) AS EstUnits, CDbl(Nz([Estimated Receipt Value-US],0)) AS EstValueUSD, CDbl(Nz([Estimated Receipt Value-Ent],0)) AS EstValueENT, RACE_Team_Receipts.[Opportunity Number], RACE_Team_Receipts.[Product Sequence Number], RACE_Team_Receipts.[Top Line Product Name], RACE_Team_Receipts.[Product Group Name], RACE_Team_Receipts.[Estimated Receipt Date], RACE_Team_Receipts.[Expected Delivery Date], RACE_Team_Receipts.[Rollout Checkbox], RACE_Team_Receipts.[Currency Code Ent], CDbl(Nz([Unassigned Receipt Qty],0)) AS UnnAssign
#FROM RACE_Team_Receipts
#WHERE (((Left(Nz([top line product name],"NA"),2))<>"A-") AND ((Left(Nz([top line product name],"NA"),3))<>"IPS"));

# Apply filter conditions
RACE_Team_Receipts = RACE_Team_Receipts[(~RACE_Team_Receipts['Top Line Product Name'].astype(str).str.startswith('A-')) & (~RACE_Team_Receipts['Top Line Product Name'].astype(str).str.startswith('IPS'))]

RACE_Team_Receipts = pd.DataFrame({
    'Ref Opp-Line': RACE_Team_Receipts['Opportunity Number'].astype(str) + '-' + RACE_Team_Receipts['Product Sequence Number'].astype(str),
    'Estimated Receipt Acct Period': RACE_Team_Receipts['Estimated Receipt Acct Period'],
    'Estimated Receipt Units': pd.to_numeric(RACE_Team_Receipts['Estimated Receipt Units'], errors='coerce').fillna(0),
    'Estimated Receipt Value-US': pd.to_numeric(RACE_Team_Receipts['Estimated Receipt Value-US'], errors='coerce').fillna(0),
    'Estimated Receipt Value-Local': pd.to_numeric(RACE_Team_Receipts['Estimated Receipt Value-Ent'], errors='coerce').fillna(0),
    'Opportunity Number': RACE_Team_Receipts['Opportunity Number'],
    'Product Sequence Number': RACE_Team_Receipts['Product Sequence Number'],
    'Top Line Product Name': RACE_Team_Receipts['Top Line Product Name'],
    'Product Line Number': RACE_Team_Receipts['Product Group Name'],
    'Estimated Receipt Date': RACE_Team_Receipts['Estimated Receipt Date'],
    'Expected Delivery Date': RACE_Team_Receipts['Expected Delivery Date'],
    'Rollout Checkbox': RACE_Team_Receipts['Rollout Checkbox'],
    'Currency Code': RACE_Team_Receipts['Currency Code Ent'],
    'Unassigned Receipt Units': pd.to_numeric(RACE_Team_Receipts['Unassigned Receipt Qty'], errors='coerce').fillna(0)
})



#RACE_Team_Receipts.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\\Load Team latest4.csv',index=False)

###################################################### Update Rcpt Dt to Delivery Dt where rollout = 0######################################################
#Open Query : Load Team latest4a

#UPDATE [Team Data_Receipts] SET [Team Data_Receipts].[Estimated Receipt Date] = [Expected Delivery Date]
#WHERE ((([Team Data_Receipts].[Estimated Receipt Date]) Is Null Or ([Team Data_Receipts].[Estimated Receipt Date])=#1/1/3000#) AND (([Team Data_Receipts].[Rollout Checkbox])=False));

# Filter the rows based on the specified conditions
mask = (RACE_Team_Receipts['Estimated Receipt Date'].isnull() | (RACE_Team_Receipts['Estimated Receipt Date'] == '1/1/3000')) & (RACE_Team_Receipts['Rollout Checkbox'] == 0)

# Update the values in the 'Estimated Receipt Date' column with values from the 'Expected Delivery Date' column
RACE_Team_Receipts.loc[mask, 'Estimated Receipt Date'] = RACE_Team_Receipts.loc[mask, 'Expected Delivery Date']


#RACE_Team_Receipts.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\\Load Team latest 5.csv',index=False)

###################################################### Update Rcpt Account Period ###################################################### 
#Open Query : Load Team latest4b

#UPDATE [Team Data_Receipts] INNER JOIN [Fin Mth] ON [Team Data_Receipts].[Estimated Receipt Date]=[Fin Mth].Date SET [Team Data_Receipts].[Estimated Receipt Acct Period] = [Fin Mth].[Accounting Period]
#WHERE ((([Team Data_Receipts].[Rollout Checkbox])=0));

###################This code is convert to finmth date to estimated rece and use due to series error 

# Join the two DataFrames and update the 'Estimated Receipt Acct Period' column
RACE_Team_Receipts = pd.merge(RACE_Team_Receipts, FinMth, left_on='Estimated Receipt Date', right_on='Date' ,how='left')

RACE_Team_Receipts.loc[(RACE_Team_Receipts['Rollout Checkbox'] == 0) & (~RACE_Team_Receipts['Accounting Period'].isnull()), 'Estimated Receipt Acct Period'] = RACE_Team_Receipts['Accounting Period']


#RACE_Team_Receipts.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\\Load Team latest 6.csv',index=False)

######################################################## Update Units and value where rollout = 0 ########################################################
#Open Query : Load Team latest4c

#UPDATE [Team Data_Receipts] INNER JOIN [Team Data_Orders] ON [Team Data_Receipts].[Ref Opp-Line] = [Team Data_Orders].[Ref-Opp-Line] SET [Team Data_Receipts].[Estimated Receipt Units] = [Team Data_Orders].[Estimated Order Units], [Team Data_Receipts].[Estimated Receipt Value-US] = [Team Data_Orders].[Estimated Order Value-US], [Team Data_Receipts].[Estimated Receipt Value-Local] = [Team Data_Orders].[Estimated Order Value-Local]
#WHERE ((([Team Data_Receipts].[Estimated Receipt Units])=0) AND (([Team Data_Receipts].[Rollout Checkbox])=False));
TeamDataOrders = pd.read_csv("E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Team Data Orders.csv" ,na_values=[''],keep_default_na=False,low_memory=False)

# Perform the join and updateRef-Opp-Line
RACE_Team_Receipts = pd.merge(TeamDataOrders,RACE_Team_Receipts, left_on='Ref-Opp-Line', right_on='Ref Opp-Line', how='right')


#RACE_Team_Receipts = RACE_Team_Receipts.merge(orders, left_on='Ref-OppLine', right_on='Ref-Opp-Line')

RACE_Team_Receipts = RACE_Team_Receipts.rename(columns=lambda x: x.replace('_x', ''))

RACE_Team_Receipts = RACE_Team_Receipts.filter(regex='^(?!.*_y$)')


RACE_Team_Receipts.loc[(RACE_Team_Receipts['Estimated Receipt Units']==0) & (RACE_Team_Receipts['Rollout Checkbox']==0), 'Estimated Receipt Value-US'] = RACE_Team_Receipts['Estimated Order Value-US']
RACE_Team_Receipts.loc[(RACE_Team_Receipts['Estimated Receipt Units']==0) & (RACE_Team_Receipts['Rollout Checkbox']==0), 'Estimated Receipt Value-Local'] = RACE_Team_Receipts['Estimated Order Value-Local']
RACE_Team_Receipts.loc[(RACE_Team_Receipts['Estimated Receipt Units']==0) & (RACE_Team_Receipts['Rollout Checkbox']==0), 'Estimated Receipt Units'] = RACE_Team_Receipts['Estimated Order Units']

#RACE_Team_Receipts[['Opportunity Number','Product Sequence Number','Top Line Product Name','Product Line Number','Estimated Receipt Acct Period','Estimated Receipt Units','Estimated Receipt Value-US','Estimated Receipt Value-Local','Ref Opp-Line','Currency Code','Estimated Receipt Date','Expected Delivery Date','Rollout Checkbox','Unassigned Receipt Units']].to_csv(r"E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\RACE Team Receipts 1.csv", index=False)

#RACE_Team_Receipts.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\\Load Team latest 7.csv',index=False)

###############################################################Update Units and value where rollout = 1 but data missing - populate with 1/1/3000 date#####################################
#Open Query : Load Team latest4d

#UPDATE [Team Data_Receipts] INNER JOIN [Team Data_Orders] ON [Team Data_Receipts].[Ref Opp-Line] = [Team Data_Orders].[Ref-Opp-Line] SET [Team Data_Receipts].[Estimated Receipt Units] = [Estimated Order Units], [Team Data_Receipts].[Estimated Receipt Value-US] = [Estimated Order Value-US], [Team Data_Receipts].[Estimated Receipt Value-Local] = [Estimated Order Value-Local], [Team Data_Receipts].[Estimated Receipt Date] = [Expected Delivery Date]
#WHERE ((([Team Data_Receipts].[Estimated Receipt Units])=0) AND (([Team Data_Receipts].[Rollout Checkbox])=True));

# set the values

RACE_Team_Receipts.loc[(RACE_Team_Receipts['Estimated Receipt Units'] == 0) & (RACE_Team_Receipts['Rollout Checkbox'] == 1), 'Estimated Receipt Value-US'] = RACE_Team_Receipts['Estimated Order Value-US']
RACE_Team_Receipts.loc[(RACE_Team_Receipts['Estimated Receipt Units'] == 0) & (RACE_Team_Receipts['Rollout Checkbox'] == 1), 'Estimated Receipt Value-Local'] = RACE_Team_Receipts['Estimated Order Value-Local']
RACE_Team_Receipts.loc[(RACE_Team_Receipts['Estimated Receipt Units'] == 0) & (RACE_Team_Receipts['Rollout Checkbox'] == 1), 'Estimated Receipt Date'] = RACE_Team_Receipts['Expected Delivery Date']
RACE_Team_Receipts.loc[(RACE_Team_Receipts['Estimated Receipt Units'] == 0) & (RACE_Team_Receipts['Rollout Checkbox'] == 1), 'Estimated Receipt Units'] = RACE_Team_Receipts['Estimated Order Units']

#RACE_Team_Receipts[['Opportunity Number','Product Sequence Number','Top Line Product Name','Product Line Number','Estimated Receipt Acct Period','Estimated Receipt Units','Estimated Receipt Value-US','Estimated Receipt Value-Local','Ref Opp-Line','Currency Code','Estimated Receipt Date','Expected Delivery Date','Rollout Checkbox','Unassigned Receipt Units']].to_csv(r"E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\RACE Team Receipts 2.csv", index=False)
    
#RACE_Team_Receipts.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\\Load Team latest 8.csv',index=False)

#####################################################################Update Rcpt Account Period to 1/1/3000##########################################################
#Open Query : Load Team latest4e

#UPDATE [Fin Mth] INNER JOIN [Team Data_Receipts] ON [Fin Mth].Date=[Team Data_Receipts].[Estimated Receipt Date] SET [Team Data_Receipts].[Estimated Receipt Acct Period] = [Accounting Period]
#WHERE ((([Team Data_Receipts].[Estimated Receipt Acct Period]) Is Null));

RACE_Team_Receipts = pd.merge(RACE_Team_Receipts, FinMth, left_on='Estimated Receipt Date', right_on='Date' ,how='left')


RACE_Team_Receipts.loc[RACE_Team_Receipts['Estimated Receipt Acct Period'].isnull() & (~RACE_Team_Receipts['Accounting Period_y'].isnull()), 'Estimated Receipt Acct Period'] = RACE_Team_Receipts['Accounting Period_y']

#RACE_Team_Receipts[['Opportunity Number','Estimated Receipt Acct Period','Estimated Receipt Units','Estimated Receipt Value-US','Estimated Receipt Value-Local','Ref Opp-Line','Currency Code','Estimated Receipt Date','Expected Delivery Date','Rollout Checkbox','Unassigned Receipt Units']].to_csv(r"E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\RACE Team Receipts 3.csv", index=False)

#RACE_Team_Receipts.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\\Load Team latest 9.csv',index=False)

############################################################## Update accounting period to 1/1/3000 when acc period is null ##########################################
#Open Query : Load Team latest4f

#UPDATE [Team Data_Receipts] SET [Team Data_Receipts].[Estimated Receipt Acct Period] = 300001
#WHERE ((([Team Data_Receipts].[Estimated Receipt Acct Period]) Is Null));


# update rows where 'Estimated Receipt Acct Period' is null
RACE_Team_Receipts.loc[RACE_Team_Receipts['Estimated Receipt Acct Period'].isnull(), 'Estimated Receipt Acct Period'] = 300001



#RACE_Team_Receipts.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\\Load Team latest 10.csv',index=False)

################################################################# Open Query : NSC rcpt date correction ######################################################
#Open Query : NSC rcpt date correction

#UPDATE [Team Data_Receipts] SET [Team Data_Receipts].[Estimated Receipt Acct Period] = 201508
#WHERE ((([Team Data_Receipts].[Estimated Receipt Acct Period])=201507) AND (([Team Data_Receipts].[Estimated Receipt Date])=#8/1/2015#) AND (([Team Data_Receipts].[Rollout Checkbox])=True));


RACE_Team_Receipts.loc[
    (RACE_Team_Receipts['Estimated Receipt Acct Period'] == 201507) &
    (RACE_Team_Receipts['Estimated Receipt Date'] == '08-01-2015') &
    (RACE_Team_Receipts['Rollout Checkbox'] == 1),
    'Estimated Receipt Acct Period'
] = 201508



##################################################### Save File######################################################################## 
#RACE_Team_Receipts.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Race Team Receipts all columns.csv',index=False)

#Save file
RACE_Team_Receipts[['Opportunity Number','Product Sequence Number','Top Line Product Name','Product Line Number','Estimated Receipt Acct Period','Estimated Receipt Units','Estimated Receipt Value-US','Estimated Receipt Value-Local','Ref Opp-Line','Currency Code','Estimated Receipt Date','Expected Delivery Date','Rollout Checkbox','Unassigned Receipt Units']].to_csv(r"E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\RACE Team Receipts.csv", index=False)





print('Step 2 is completed')