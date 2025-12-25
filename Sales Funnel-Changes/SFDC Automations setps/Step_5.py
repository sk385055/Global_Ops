import Step_4
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta

TEAM_HEADER = pd.read_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Team Header Final.csv' ,na_values=[''],keep_default_na=False, low_memory=False)

tblParent_MCC_Map = pd.read_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\tblParent_MCC_Map.csv' ,na_values=[''],keep_default_na=False, low_memory=False) 

tblSalesFunnel_ScheduleMth = pd.read_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\tblSalesFunnel_ScheduleMth.csv' ,na_values=[''],keep_default_na=False, low_memory=False)

tblNAMER_RetLOBMappingsByMCN = pd.read_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\tblNAMER_RetLOBMappingsByMCN.csv' ,na_values=[''],keep_default_na=False, low_memory=False)

tblNSC_LobRegion = pd.read_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\tblNSC_LobRegion.csv' ,na_values=[''],keep_default_na=False, low_memory=False)

RU_org_code_map = pd.read_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\RU_org_code_map.csv' ,na_values=[''],keep_default_na=False, low_memory=False)

Org_Tables_Local = pd.read_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Org Tables Local.csv' ,na_values=[''],keep_default_na=False, low_memory=False)

Product_Family_Map = pd.read_csv(r'E:\_Projects\SFDC Automation\Input\Product Family Map.csv' ,na_values=[''],keep_default_na=False, low_memory=False)

Country_Map = pd.read_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Country Map.csv', na_values=[''],keep_default_na=False, low_memory=False) 

Fin_Mth = pd.read_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Fin Mth.csv' ,na_values=[''],keep_default_na=False, low_memory=False) 

TEAM_DETAIL_RECEIPTS = pd.read_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\TEAM DETAIL_RECEIPTS.csv' ,na_values=[''],keep_default_na=False, low_memory=False) 

SQL_Master = pd.read_excel(r'E:\_Projects\SFDC Automation\Input\SQL.xlsx')

################################################# qry-L1-L2 #################################################################
# qry-L1-L2
#print('qry-l1-l2')
# SELECT [L1_L2_map].KeyAcct, IIf([L2] In ("Direct","Global"),"NAMER-" & [L2],[L1]) AS L1_, IIf([l3]="Other","VP Office",[l3]) AS L3_, [L1_L2_map].L3
# FROM [L1_L2_map]
# WHERE ((([L1_L2_map].L3) Not In ("Other")) AND ((Left([L1],5))="NAMER"));

L1_L2_map = pd.read_csv('E:\_Projects\SFDC Automation\Sales Funnel input files\L1-L2-map.csv' ,na_values=[''],keep_default_na=False, low_memory=False) 

# Filter the data based on the criteria specified in the WHERE clause
L1_L2_map = L1_L2_map[(L1_L2_map['L3'] != 'Other') & (L1_L2_map['L1'].str[:5] == 'NAMER')]

# Apply the transformations specified in the SELECT clause using pandas' apply() method
L1_L2_map['L1_'] = L1_L2_map.apply(lambda row: 'NAMER-' + row['L2'] if row['L2'] in ['Direct', 'Global'] else row['L1'], axis=1)
L1_L2_map['L3_'] = L1_L2_map['L3'].apply(lambda x: 'VP Office' if x == 'Other' else x)

# Select the desired columns
L1_L2_map = L1_L2_map[['KeyAcct', 'L1_', 'L3_', 'L3']]

# save updated DataFrame to CSV file
L1_L2_map.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\qry-L1-L2.csv', index=False)

qryL1L2 = pd.read_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\qry-L1-L2.csv' ,na_values=[''],keep_default_na=False, low_memory=False) 
################################################# qryOwnerManager #################################################################
# qryOwnerManager
#print('qryOwnerManager')
# SELECT DISTINCT [Team Data_Orders].[Opportunity Number], [Team Data_Orders].[Owner Manager Name]
# FROM [Team Data_Orders]
# WHERE [Team Data_Orders].[Owner Manager Name] NOT IN ('Ron  Braco','Jason Zou  Ji Ming');

TeamDataOrders = pd.read_csv("E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Team Data Orders.csv" ,na_values=[''],keep_default_na=False,low_memory=False)

# Apply the filter using the `isin` method to select values that are not in the list
filtered_TeamDataOrders = TeamDataOrders.loc[~TeamDataOrders['Owner Manager Name'].isin(['Ron Braco', 'Jason Zou Ji Ming'])]

# Select the desired columns
qryOwnerManager = filtered_TeamDataOrders[['Opportunity Number', 'Owner Manager Name']].drop_duplicates()

# save updated DataFrame to CSV file
qryOwnerManager.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\qryOwnerManager.csv', index=False)

##################################################### Creat Sales Report ###########################################################
# SELECT [TEAM HEADER].[Region Code] AS Region, [TEAM HEADER].[Area Name] AS Area, IIf([REGION]="NAMER" And [Org Tables Local].[Vertical] In ("Majors/CA","CFI","Public"),"NAMER-" & [Org Tables Local]![Vertical],IIf(Len([tblNAMER_RetLOBMappingsByMCN]![LOB_REGION])>0,[tblNAMER_RetLOBMappingsByMCN]![LOB_REGION],IIf([TEAM HEADER]![Industry Group Code]="RET" And [Region]="NAMER","NAMER-Other",IIf([TEAM HEADER]![INDUSTRY GROUP Code] In ("HSR","TLG") And Len([L1_])>0,[L1_],IIf([TEAM HEADER]![INDUSTRY GROUP Code] In ("HSR","TLG") And Len([Org Tables Local]![Vertical2])>0,[Org Tables Local]![Vertical2],[tblNSC_LobRegion]![LOB_REGION]))))) AS [LOB Region], IIf(Len([ru_org_code_map].[org code])>0,"Russia",IIf(Left([team header].[org code],3)="608" And [team header].[country code]="XC","Russia",IIf(Len([tblNAMER_RetLOBMappingsByMCN]![LOB_AREA])>0,[tblNAMER_RetLOBMappingsByMCN]![LOB_AREA],IIf([REGION CODE]="NAMER" And Len([Org Tables Local]![Vertical])>0,[Org Tables Local]![Vertical],IIf([team header]![industry group code] In ("HSR","TLG") And Len([L3_])>0,[L3_],IIf([team header]![industry group code] In ("HSR","TLG") And Len([Org Tables Local]![Vertical])>0,[Org Tables Local]![Vertical],[tblNSC_LobRegion]![LOB_AREA])))))) AS [LOB Area], [TEAM HEADER].[Country Name] AS Country, [TEAM HEADER].[Country Code], [TEAM HEADER].[Org Code] AS [Sales Org], Switch([TEAM HEADER].[Key Account] In ('OTHER'),[TEAM HEADER].[Key Account] & '-' & [TEAM HEADER].[Country Code],[TEAM HEADER].[Key Account] Not In ('OTHER'),[TEAM HEADER].[Key Account]) AS [Key Account], [TEAM HEADER].[Master Customer Name] AS [Customer Name], [TEAM HEADER].[Master Customer Number] AS [Customer Number], [TEAM HEADER].[Opportunity Description], [TEAM HEADER].[Opportunity Number], [TEAM HEADER].[Owner Name], qryOwnerManager.[Owner Manager Name], [Product Family Map].[HW or SW] AS [Product Type], [Product Family Map].UNIT_TYPE AS [S&OP Unit], [Product Family Map].Offer_PF AS [S&OP Offer Portolio], [Product Family Map].Family AS [Product Range], [Product Family Map].Class, [Product Family Map].[Parent Model], [TEAM DETAIL_RECEIPTS].[Product Sequence Number], [TEAM DETAIL_RECEIPTS].[Top Line Product Name] AS Topline, IIf(IsNull([Product Family Map].[Deposit Flag]),"Other","Deposit") AS [Deposit/Other], [Product Family Map].[Deposit Level 1] AS [Module Type], [Product Family Map].[CPM Level 1] AS [CPM Type], Mid([phase code-current],InStr([phase code-current],"-")+1,Len([phase code-current])) AS [Forecast Category], IIf([status description] In ("WON","LOST","DISCONTINUED"),0,IIf([phase code-current] In ("19-OMITTED","18-CLOSED"),0,IIf((CDbl(Nz([M1],0))+CDbl(Nz([M2],0))+CDbl(Nz([M3],0))+CDbl(Nz([M4],0))+CDbl(Nz([M5],0))+CDbl(Nz([M6],0))+CDbl(Nz([M7],0))+CDbl(Nz([M8],0))+CDbl(Nz([M9],0))+CDbl(Nz([M10],0))+CDbl(Nz([M11],0))+CDbl(Nz([M12],0)))=0,0,IIf([phase code-current]="6-PIPELINE" And (CDbl(Nz([M1],0))+CDbl(Nz([M2],0))+CDbl(Nz([M3],0))+CDbl(Nz([M4],0)))>0,0,1)))) AS [Forecast Flag], [TEAM HEADER].[Selling Stage Name] AS [Selling Stage], [TEAM HEADER].[Estimated Win Probability] AS [Est Win Prob], Left([Exp Book Mth],4) & "Q" & Round((Right([Exp Book Mth],2)+1)/3,0) AS [Exp Book Yr/Qtr], [TEAM HEADER].[Estimated Close Mth] AS [Exp Book Mth], [TEAM HEADER].[Expected Book Date] AS [Exp Book Date], [TEAM DETAIL_RECEIPTS].[Est Receipt Mth] AS [Del Schedule Mth], Left([TEAM DETAIL_RECEIPTS].[Est Receipt Mth],4) & "Q" & Round((Right([TEAM DETAIL_RECEIPTS].[Est Receipt Mth],2)+1)/3,0) AS [Del Schedule Yr/Qtr], [TEAM DETAIL_RECEIPTS].[Estimated Receipt Date], [TEAM DETAIL_RECEIPTS].[Expected Delivery Date] AS [Expected Delivery Start Date], [Expected Delivery Date]-[Exp Book Date] AS [Exp Del Time (Days)], tblSalesFunnel_ScheduleMth.[Schedule Mth], [TEAM DETAIL_RECEIPTS].[Estimated Receipt Units] AS [Oppty Qty], Switch([Forecast Category] In ('COMMIT'),[Oppty Qty]*1,[Forecast Category] Not In ('COMMIT'),[Oppty Qty]*[Est Win Prob]) AS [Weighted Qty], [Oppty Qty]*[Est Win Prob] AS [Win Adjusted Qty], Switch([Del Schedule Mth]=300001,"Yes",[Del Schedule Mth]<>300001,"No") AS Unscheduled, Switch((Left([Del Schedule Mth],4)-Year(Now()))*12+Right([Del Schedule Mth],2)-Month(Now())<-1,[Weighted Qty],(Left([Del Schedule Mth],4)-Year(Now()))*12+Right([Del Schedule Mth],2)-Month(Now())>=-1,0) AS [Past Due (>1 mth)], Switch((Left([Del Schedule Mth],4)-Year(Now()))*12+Right([Del Schedule Mth],2)-Month(Now())=-1,[Weighted Qty],(Left([Del Schedule Mth],4)-Year(Now()))*12+Right([Del Schedule Mth],2)-Month(Now())<>-1,0) AS [Past Due (=<1 mth)], IIf([Schedule Mth] In ('EXCLUDE','Unscheduled'),[Weighted Qty],Null) AS Exclude, IIf([Past Due (=<1 mth)] In ('Yes'),[Weighted Qty],Null) AS Before, IIf([Del Schedule Mth]=IIf(Month(Now())>12,Year(Now()),Year(Now())) & Format(IIf(Month(Now())>12,Month(Now())-12,Month(Now())),'00'),[Weighted Qty],Null) AS M1, IIf([Del Schedule Mth]=IIf(Month(Now())+1>12,Year(Now())+1,Year(Now())) & Format(IIf(Month(Now())+1>12,Month(Now())+1-12,Month(Now())+1),'00'),[Weighted Qty],Null) AS M2, IIf([Del Schedule Mth]=IIf(Month(Now())+2>12,Year(Now())+1,Year(Now())) & Format(IIf(Month(Now())+2>12,Month(Now())+2-12,Month(Now())+2),'00'),[Weighted Qty],Null) AS M3, IIf([Del Schedule Mth]=IIf(Month(Now())+3>12,Year(Now())+1,Year(Now())) & Format(IIf(Month(Now())+3>12,Month(Now())+3-12,Month(Now())+3),'00'),[Weighted Qty],Null) AS M4, IIf([Del Schedule Mth]=IIf(Month(Now())+4>12,Year(Now())+1,Year(Now())) & Format(IIf(Month(Now())+4>12,Month(Now())+4-12,Month(Now())+4),'00'),[Weighted Qty],Null) AS M5, IIf([Del Schedule Mth]=IIf(Month(Now())+5>12,Year(Now())+1,Year(Now())) & Format(IIf(Month(Now())+5>12,Month(Now())+5-12,Month(Now())+5),'00'),[Weighted Qty],Null) AS M6, IIf([Del Schedule Mth]=IIf(Month(Now())+6>12,Year(Now())+1,Year(Now())) & Format(IIf(Month(Now())+6>12,Month(Now())+6-12,Month(Now())+6),'00'),[Weighted Qty],Null) AS M7, IIf([Del Schedule Mth]=IIf(Month(Now())+7>12,Year(Now())+1,Year(Now())) & Format(IIf(Month(Now())+7>12,Month(Now())+7-12,Month(Now())+7),'00'),[Weighted Qty],Null) AS M8, IIf([Del Schedule Mth]=IIf(Month(Now())+8>12,Year(Now())+1,Year(Now())) & Format(IIf(Month(Now())+8>12,Month(Now())+8-12,Month(Now())+8),'00'),[Weighted Qty],Null) AS M9, IIf([Del Schedule Mth]=IIf(Month(Now())+9>12,Year(Now())+1,Year(Now())) & Format(IIf(Month(Now())+9>12,Month(Now())+9-12,Month(Now())+9),'00'),[Weighted Qty],Null) AS M10, IIf([Del Schedule Mth]=IIf(Month(Now())+10>12,Year(Now())+1,Year(Now())) & Format(IIf(Month(Now())+10>12,Month(Now())+10-12,Month(Now())+10),'00'),[Weighted Qty],Null) AS M11, IIf([Del Schedule Mth]=IIf(Month(Now())+11>12,Year(Now())+1,Year(Now())) & Format(IIf(Month(Now())+11>12,Month(Now())+11-12,Month(Now())+11),'00'),[Weighted Qty],Null) AS M12, IIf([Del Schedule Mth]=IIf(Month(Now())+12>12,Year(Now())+1,Year(Now())) & Format(IIf(Month(Now())+12>12,Month(Now())+12-12,Month(Now())+12),'00'),[Weighted Qty],Null) AS M13, IIf([Del Schedule Mth]=IIf(Month(Now())+13>12,Year(Now())+1,Year(Now())) & Format(IIf(Month(Now())+13>12,Month(Now())+13-12,Month(Now())+13),'00'),[Weighted Qty],Null) AS M14, IIf([Del Schedule Mth]=IIf(Month(Now())+14>12,Year(Now())+1,Year(Now())) & Format(IIf(Month(Now())+14>12,Month(Now())+14-12,Month(Now())+14),'00'),[Weighted Qty],Null) AS M15, IIf([Del Schedule Mth]=IIf(Month(Now())+15>12,Year(Now())+1,Year(Now())) & Format(IIf(Month(Now())+15>12,Month(Now())+15-12,Month(Now())+15),'00'),[Weighted Qty],Null) AS M16, [TEAM HEADER].[Revenue Trigger] AS [Invoice Trigger], [TEAM HEADER].[Status Description] AS [Oppty Status], [TEAM HEADER].[Date Closed], [TEAM HEADER].[Solution Value-US] AS [Solution $K], [Solution Value-US]*[Estimated Win Probability] AS [Solution Weighted $K], [TEAM DETAIL_RECEIPTS].[Estimated Receipt Value-US] AS [HW $K], Switch([Forecast Category] In ('8-COMMIT'),[Estimated Receipt Value-US]*1,[Forecast Category] Not In ('8-COMMIT'),[Estimated Receipt Value-US]*[Estimated Win Probability]) AS [HW Weighted $K], [Oppty Qty]*[Unit MCC USD]/1000 AS [HW MCC $K], [Weighted Qty]*[Unit MCC USD]/1000 AS [HW Weighted MCC $K], IIf(IsNull([Estimated Receipt Units]) Or [Estimated Receipt Units]=0,0,Round(([Estimated Receipt Value-US]/[Estimated Receipt Units])*1000,0)) AS [Unit Sell Price USD], Round(tblParent_MCC_Map.MCC,0) AS [Unit MCC USD], Switch([TEAM HEADER].[Industry Group Code] In ("HSR"),"HOSP",[TEAM HEADER].[Industry Group Code] Not In ("HSR"),[TEAM HEADER].[Industry Group Code]) AS [Industry Group], [TEAM HEADER].[Customer Industry Code] AS Industry, [TEAM HEADER].[Risk Commit Color Text] AS [Commitment Risk], [TEAM HEADER].[Opportunity Type Desc] AS [Oppty Record Type], [TEAM HEADER].[Date Created], [TEAM HEADER].[Last Modified Date Time], [TEAM HEADER].[PBO Flag], [TEAM HEADER].[PBO Order Number], [Product Family Map].[New Product] AS [NPI Flag], [TEAM HEADER].[Fulfilled By Partner], [TEAM HEADER].[Opportunity Type Desc] AS [Opportunity Type], "Need to add to query" AS [FinMktg Top 200 Customer], [TEAM HEADER].[Marketing Prog Name] AS [Marketing Program], [TEAM HEADER].[Primary Campaign Name] AS [Primary Campaign], "Add from EDW" AS [Competitor Name], [TEAM HEADER].[Risk Comment Text], [TEAM HEADER].[Contractual Commit]
# FROM ((((tblSalesFunnel_ScheduleMth RIGHT JOIN ([Product Family Map] RIGHT JOIN ((([Org Tables Local] RIGHT JOIN (tblNSC_LobRegion RIGHT JOIN (([Country Map] RIGHT JOIN [TEAM HEADER] ON [Country Map].[Country Code] = [TEAM HEADER].[Country Code]) LEFT JOIN [Fin Mth] ON [TEAM HEADER].[Expected Book Date] = [Fin Mth].Date) ON (tblNSC_LobRegion.LOB = [TEAM HEADER].[Industry Group Code]) AND (tblNSC_LobRegion.CNTY_CODE = [TEAM HEADER].[Country Code])) ON [Org Tables Local].[Org Code] = [TEAM HEADER].[Org Code]) LEFT JOIN tblNAMER_RetLOBMappingsByMCN ON [TEAM HEADER].[Master Customer Number] = tblNAMER_RetLOBMappingsByMCN.MASTER_CUSTOMER_NUMBER) LEFT JOIN [TEAM DETAIL_RECEIPTS] ON [TEAM HEADER].[Opportunity Number] = [TEAM DETAIL_RECEIPTS].[Opportunity Number]) ON [Product Family Map].[Top Line Product] = [TEAM DETAIL_RECEIPTS].[Top Line Product Name]) ON tblSalesFunnel_ScheduleMth.[Est Receipt Mth] = [TEAM DETAIL_RECEIPTS].[Est Receipt Mth]) LEFT JOIN qryOwnerManager ON [TEAM HEADER].[Opportunity Number] = qryOwnerManager.[Opportunity Number]) LEFT JOIN RU_org_code_map ON [TEAM HEADER].[Org Code] = RU_org_code_map.[Org Code]) LEFT JOIN tblParent_MCC_Map ON [Product Family Map].[Parent Model] = tblParent_MCC_Map.Parent_Model) LEFT JOIN [qry-L1-L2] ON [TEAM HEADER].[Key Account] = [qry-L1-L2].KeyAcct
# WHERE (((Nz([Family],"NA")) Not In ("Do Not Report On")) AND ((Nz([Product Line Number],"NA"))="Hardware"))
# ORDER BY [TEAM HEADER].[Region Code];
#print('Creat Sales Report')
Country_Map['Country Code'] = Country_Map['Country Code'].fillna('null')

# Merging
df1 = pd.merge( TEAM_HEADER, Country_Map,
               how="left", left_on="Country Code", right_on="Country Code")

# Convert the 'Expected Book Date' column in df1 to datetime
df1['Expected Book Date'] = pd.to_datetime(df1['Expected Book Date'])

# Extract the date from the 'Expected Book Date' column
df1['Expected Book Date'] = df1['Expected Book Date'].dt.date

# save updated DataFrame to CSV file
#df1.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\df1.csv', index=False)

# Convert the 'Date' column in Fin_Mth to datetime
Fin_Mth['Date'] = pd.to_datetime(Fin_Mth['Date'])

# Extract the date from the 'Date' column
Fin_Mth['Date'] = Fin_Mth['Date'].dt.date


df2 = pd.merge(df1, Fin_Mth,
               how="left", left_on="Expected Book Date", right_on="Date")

df2 = df2.rename(columns=lambda x: x.replace('_x', ''))

df2 = df2.filter(regex='^(?!.*_y$)')

# save updated DataFrame to CSV file
#df2.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\df2.csv', index=False)

df3 = pd.merge(df2, tblNSC_LobRegion,
               how="left" , left_on=["Industry Group Code", "Country Code"] , right_on=["LOB", "CNTY_CODE"])

df3 = df3.rename(columns=lambda x: x.replace('_x', ''))

df3 = df3.filter(regex='^(?!.*_y$)')

df3['Org Code'] = df3['Org Code'].astype('str')

# save updated DataFrame to CSV file
#df3.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\df3.csv', index=False)


df4 = pd.merge(df3,Org_Tables_Local,
               how="left", on="Org Code")

df4 = df4.rename(columns=lambda x: x.replace('_x', ''))

df4 = df4.filter(regex='^(?!.*_y$)')

# save updated DataFrame to CSV file
#df4.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\df4.csv', index=False)

df5 = pd.merge(df4,tblNAMER_RetLOBMappingsByMCN,
               how="left", right_on="MASTER_CUSTOMER_NUMBER", left_on="Master Customer Number")

df5 = df5.rename(columns=lambda x: x.replace('LOB_REGION_x', 'LOB_REGION_tblNSC_LobRegion'))

df5 = df5.rename(columns=lambda x: x.replace('LOB_REGION_y', 'LOB_REGION_tblNAMER_RetLOBMappingsByMCN'))

df5 = df5.rename(columns=lambda x: x.replace('LOB_AREA_x', 'LOB_AREA_tblNSC_LobRegion'))

df5 = df5.rename(columns=lambda x: x.replace('LOB_AREA_y', 'LOB_AREA_tblNAMER_RetLOBMappingsByMCN'))

# save updated DataFrame to CSV file
#df5.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\df5.csv', index=False)

df6 = pd.merge( df5,TEAM_DETAIL_RECEIPTS,
                how="left", on="Opportunity Number")

#df6 = df6.rename(columns=lambda x: x.replace('_x', ''))


df6.columns = [col.replace('_x','') for col in df6.columns]

df6 = df6.filter(regex='^(?!.*_y$)')

# save updated DataFrame to CSV file
#df6.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\df6.csv', index=False)

df7 = pd.merge( df6,Product_Family_Map,
               how="left", left_on="Top Line Product Name", right_on="Top Line Product")
               
df7 = df7.rename(columns=lambda x: x.replace('_x', ''))

df7 = df7.filter(regex='^(?!.*_y$)')
# save updated DataFrame to CSV file
#df7.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\df7.csv', index=False)

df8 = pd.merge( df7,tblSalesFunnel_ScheduleMth,
               how="left", on="Est Receipt Mth")

df8 = df8.rename(columns=lambda x: x.replace('_x', ''))

df8 = df8.filter(regex='^(?!.*_y$)')

# save updated DataFrame to CSV file
#df8.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\df8.csv', index=False)

df9 = pd.merge(df8, qryOwnerManager,
               how="left", left_on="Opportunity Number", right_on="Opportunity Number")

df9 = df9.rename(columns=lambda x: x.replace('_x', ''))

df9 = df9.filter(regex='^(?!.*_y$)')

# save updated DataFrame to CSV file
#df9.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\df9.csv', index=False)

RU_org_code_map['Org Code']=RU_org_code_map['Org Code'].astype(str)

df9['Org Code']=df9['Org Code'].astype(str)

df10 = pd.merge(RU_org_code_map, df9,
               how="right", left_on="Org Code", right_on="Org Code")

# save updated DataFrame to CSV file
#df10.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\df10.csv', index=False)

df11 = pd.merge( df10,tblParent_MCC_Map,
               how="left", right_on="Parent_Model", left_on="Parent Model")

df = pd.merge(df11, qryL1L2,
                how="left", left_on="Key Account", right_on="KeyAcct")
df = df.rename(columns=lambda x: x.replace('_x', ''))

df = df.filter(regex='^(?!.*_y$)')

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Sales Funnel Report.csv', index=False)

df = df[(df["Family"].fillna("NA") != "Do Not Report On") & (df["Product Line Number"].fillna("NA") == "Hardware")]

df = df.sort_values(by=["Region Code"])

# save updated DataFrame to CSV file
df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Sales Funnel Report.csv', index=False)


########################################################################## LOB Region ########################################################################
#print('LOB Region')

df = pd.read_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Sales Funnel Report.csv' ,na_values=[''],keep_default_na=False, low_memory=False)

def calculate_lob_region(row):
    if row['Region Code'] == 'NAMER' and isinstance(row['Vertical'], str) and row['Vertical'] in ['Majors/CA', 'CFI', 'Public']:
        return 'NAMER-' + row['Vertical']
    elif pd.notna(row['LOB_REGION_tblNAMER_RetLOBMappingsByMCN']) and isinstance(row['LOB_REGION_tblNAMER_RetLOBMappingsByMCN'], str):
        return row['LOB_REGION_tblNAMER_RetLOBMappingsByMCN']
    elif row['Industry Group Code'] == 'RET' and row['Region Code'] == 'NAMER':
        return 'NAMER-Other'
    elif row['Industry Group Code'] in ['HSR', 'TLG'] and isinstance(row['L1_'], str) and len(row['L1_']) > 0:
        return row['L1_']
    elif row['Industry Group Code'] in ['HSR', 'TLG'] and isinstance(row['Vertical2'], str) and len(row['Vertical2']) > 0:
        return row['Vertical2']
    elif pd.notna(row['LOB_REGION_tblNSC_LobRegion']) and isinstance(row['LOB_REGION_tblNSC_LobRegion'], str):
        return row['LOB_REGION_tblNSC_LobRegion']
    else:
        return None    


# Apply the custom function to generate the "LOB Region" column
df['LOB Region'] = df.apply(calculate_lob_region, axis=1)

##print('LOB Region')
# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\LOB Region.csv', index=False)

########################################LOB AREA##############################################
#print('Lob Area start')
df['Org Code'] = df['Org Code'].astype(str)
##print('String converstion')
# define the conditions and corresponding outputs
conditions = [
    (df['Org Code'].isin(['540100989','540110201','540110202','540110270','540120300','540120680','540120683','540120684'])),
    (df['Org Code'].str[:3] == '608') & (df['Country Code'] == 'XC'),
    (df['LOB_AREA_tblNAMER_RetLOBMappingsByMCN'].astype(str).str.len() > 0),
    (df['Region Code'] == 'NAMER') & (df['Vertical'].astype(str).str.len() > 0),
    (df['Industry Group Code'].astype(str).isin(['HSR', 'TLG'])) & (df['L3_'].astype(str).str.len() > 0),
    (df['Industry Group Code'].astype(str).isin(['HSR', 'TLG'])) & (df['Vertical'].astype(str).str.len() > 0),
]
outputs = [
    'Russia',
    'Russia',
    df['LOB_AREA_tblNAMER_RetLOBMappingsByMCN'],
    df['Vertical'],
    df['L3_'],
    df['Vertical'],
]

# use numpy.select() to calculate the LOB area
df['LOB Area'] = np.select(conditions, outputs, df['LOB_AREA_tblNSC_LobRegion'])

#print('LOB Area')
# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\LOB Area.csv', index=False)

#################################Key Account########################################
#print('Key Account')
# use numpy.where() to create new column
df['Key Account'] = np.where(
    df['Key Account'] == 'OTHER',
    df['Key Account'] + '-' + df['Country Code'],
    df['Key Account']
)

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Key Account.csv', index=False)
##print('Key Acc')
#############################Deposit Other############################################
#print('Deposite Other')
df["Deposit/Other"] = np.where(
    df["Deposit Flag"].isnull(),
    "Other",
    "Deposit"
)

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Deposit Other.csv', index=False)
##print('Deposit other')
######################################ForecastCategory##################################################
#print('ForecastCategory')
df["Phase Code-Current"] = df["Phase Code-Current"].astype(str)

# Assuming you have a DataFrame with a "phase code-current" column
df["Forecast Category"] = df["Phase Code-Current"].str.split("-").str[1]

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Forecast Category.csv', index=False)


######################################Exp Book Yr/Qtr##################################################
#print('Exp Book Yr/Qtr')
df["Estimated Close Mth"] = df["Estimated Close Mth"].astype(str)
df["Estimated Close Mth"] = df["Estimated Close Mth"].astype(str)

df['Estimated Close Mth'] = df['Estimated Close Mth'].replace('nan', '0')



# extract year and month
df["year"] = df["Estimated Close Mth"].str[:4]

df["month"] = df["Estimated Close Mth"].str[4:]

df['month'] = df['month'].replace('', '0')

# calculate quarter
df["quarter"] = np.ceil(df["month"].astype(float).astype(int) / 3).astype(int)

df['quarter'] = df['quarter'].replace('0', '')


# create new column with year and quarter
df["Exp Book Yr/Qtr"] = df["year"] + "Q" + df["quarter"].astype(str)

df['Exp Book Yr/Qtr'] = df['Exp Book Yr/Qtr'].replace('0Q0', 'Q')

# drop temporary columns
df = df.drop(columns=["year", "month", "quarter"])

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Exp Book Yr Qtr.csv', index=False)

######################################Del Schedule Yr/Qtr##################################################
#print('Del Schedule Yr/Qtr')

# Fill missing values in the Est Receipt Mth column with a default value

df['Est Receipt Mth'] = df['Est Receipt Mth'].astype(str)

df['Est Receipt Mth'] = df['Est Receipt Mth'].replace('nan', '0')



# extract year and month
df["year"] = df["Est Receipt Mth"].str[:4]

df["month"] = df["Est Receipt Mth"].str[4:]

df['month'] = df['month'].replace('', '0')

# calculate quarter
df["quarter"] = np.ceil(df["month"].astype(float).astype(int) / 3).astype(int)

df['quarter'] = df['quarter'].replace('0', '')


# create new column with year and quarter
df['Del Schedule Yr/Qtr'] = df["year"] + "Q" + df["quarter"].astype(str)

df['Del Schedule Yr/Qtr'] = df['Del Schedule Yr/Qtr'].replace('0Q0', 'Q')

# drop temporary columns
df = df.drop(columns=["year", "month", "quarter"])


# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Del Schedule Yr Qtr.csv', index=False)

######################################Exp Del Time (Days)##################################################
#print('Exp Del Time (Days)')
df['Expected Delivery Date'] = pd.to_datetime(df['Expected Delivery Date'], errors='coerce')
df['Exp Book Date'] = pd.to_datetime(df['Expected Book Date'], errors='coerce')

df['Exp Del Time (Days)'] = (df['Expected Delivery Date'] - df['Exp Book Date']).dt.days

# replace invalid timestamp values with NaT
df['Expected Delivery Date'] = df['Expected Delivery Date'].where(df['Expected Delivery Date'].notnull(), 'NaT')

df['Exp Book Date'] = df['Exp Book Date'].where(df['Exp Book Date'].notnull(), 'NaT')


# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Exp Del Time (Days).csv', index=False)

######################################Weighted Qty##################################################
#print('Weighted Qty')
df["Phase Code-Current"] = df["Phase Code-Current"].astype(str)

# Assuming you have a DataFrame with a "phase code-current" column
df["Forecast Category"] = df["Phase Code-Current"].str.split("-").str[1]

df['Weighted Qty'] = np.where(df['Forecast Category'] == 'COMMIT', df['Estimated Receipt Units'] * 1, df['Estimated Receipt Units'] * df['Estimated Win Probability'])

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Weighted Qty.csv', index=False)

######################################Win Adjusted Qty##################################################

df['Win Adjusted Qty'] = df['Estimated Receipt Units'] * df['Estimated Win Probability']

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Win Adjusted Qty.csv', index=False)

######################################Unscheduled##################################################

df['Unscheduled'] = np.where(df['Est Receipt Mth'] == 300001, 'Yes', 'No')

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Unscheduled.csv', index=False)


######################################Exclude##################################################
#print('Exclude')
# use numpy.where to create the new column
df['Exclude'] = np.where(df['Schedule Mth'].isin(['EXCLUDE', 'Unscheduled']),
                         df['Weighted Qty'], np.nan)

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Exclude.csv', index=False)

###################################### M1 ##################################################

# Get the current year and month in the format 'YYYYMM'
current_year_month = datetime.now().strftime('%Y%m')

df['Est Receipt Mth'] = df['Est Receipt Mth'].astype(float).astype(int)

df['Est Receipt Mth'] = df['Est Receipt Mth'].astype(str)

# Compare 'Del Schedule Mth' with the current year and month
df['M1'] = df['Weighted Qty'].where(df['Est Receipt Mth'] == current_year_month, None)

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\M1.csv', index=False)
###################################### M2 ##################################################
current_month = datetime.now().replace(day=1)

next_month = (current_month + relativedelta(months=1)).strftime('%Y%m')


# Compare 'Del Schedule Mth' with the current year and month
df['M2'] = df['Weighted Qty'].where(df['Est Receipt Mth'] == next_month, None)

# save updated DataFrame to CSV file
# M2.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\M2.csv', index=False)
###################################### M3 ##################################################
current_month = datetime.now().replace(day=1)

next_month = (current_month + relativedelta(months=2)).strftime('%Y%m')


# Compare 'Del Schedule Mth' with the current year and month
df['M3'] = df['Weighted Qty'].where(df['Est Receipt Mth'] == next_month, None)

# save updated DataFrame to CSV file
# M3.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\M3.csv', index=False)
###################################### M4 ##################################################
current_month = datetime.now().replace(day=1)

next_month = (current_month + relativedelta(months=3)).strftime('%Y%m')


# Compare 'Del Schedule Mth' with the current year and month
df['M4'] = df['Weighted Qty'].where(df['Est Receipt Mth'] == next_month, None)

# save updated DataFrame to CSV file
# M4.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\M4.csv', index=False)
###################################### M5 ##################################################
current_month = datetime.now().replace(day=1)

next_month = (current_month + relativedelta(months=4)).strftime('%Y%m')


# Compare 'Del Schedule Mth' with the current year and month
df['M5'] = df['Weighted Qty'].where(df['Est Receipt Mth'] == next_month, None)

# save updated DataFrame to CSV file
# M5.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\M5.csv', index=False)
###################################### M6 ##################################################
current_month = datetime.now().replace(day=1)

next_month = (current_month + relativedelta(months=5)).strftime('%Y%m')


# Compare 'Del Schedule Mth' with the current year and month
df['M6'] = df['Weighted Qty'].where(df['Est Receipt Mth'] == next_month, None)

# save updated DataFrame to CSV file
# M6.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\M6.csv', index=False)
###################################### M7 ##################################################
current_month = datetime.now().replace(day=1)

next_month = (current_month + relativedelta(months=6)).strftime('%Y%m')


# Compare 'Del Schedule Mth' with the current year and month
df['M7'] = df['Weighted Qty'].where(df['Est Receipt Mth'] == next_month, None)

# save updated DataFrame to CSV file
# M7.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\M7.csv', index=False)
###################################### M8 ##################################################
current_month = datetime.now().replace(day=1)

next_month = (current_month + relativedelta(months=7)).strftime('%Y%m')


# Compare 'Del Schedule Mth' with the current year and month
df['M8'] = df['Weighted Qty'].where(df['Est Receipt Mth'] == next_month, None)

# save updated DataFrame to CSV file
# M8.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\M8.csv', index=False)
###################################### M9 ##################################################
current_month = datetime.now().replace(day=1)

next_month = (current_month + relativedelta(months=8)).strftime('%Y%m')


# Compare 'Del Schedule Mth' with the current year and month
df['M9'] = df['Weighted Qty'].where(df['Est Receipt Mth'] == next_month, None)

# save updated DataFrame to CSV file
# M9.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\M9.csv', index=False)
###################################### M10 ##################################################
current_month = datetime.now().replace(day=1)

next_month = (current_month + relativedelta(months=9)).strftime('%Y%m')


# Compare 'Del Schedule Mth' with the current year and month
df['M10'] = df['Weighted Qty'].where(df['Est Receipt Mth'] == next_month, None)

# save updated DataFrame to CSV file
# M10.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\M10.csv', index=False)
###################################### M11 ##################################################
current_month = datetime.now().replace(day=1)

next_month = (current_month + relativedelta(months=10)).strftime('%Y%m')


# Compare 'Del Schedule Mth' with the current year and month
df['M11'] = df['Weighted Qty'].where(df['Est Receipt Mth'] == next_month, None)

# save updated DataFrame to CSV file
# M11.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\M11.csv', index=False)
###################################### M12 ##################################################
current_month = datetime.now().replace(day=1)

next_month = (current_month + relativedelta(months=11)).strftime('%Y%m')


# Compare 'Del Schedule Mth' with the current year and month
df['M12'] = df['Weighted Qty'].where(df['Est Receipt Mth'] == next_month, None)

# save updated DataFrame to CSV file
# M12.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\M12.csv', index=False)
###################################### M13 ##################################################
current_month = datetime.now().replace(day=1)

next_month = (current_month + relativedelta(months=12)).strftime('%Y%m')


# Compare 'Del Schedule Mth' with the current year and month
df['M13'] = df['Weighted Qty'].where(df['Est Receipt Mth'] == next_month, None)

# save updated DataFrame to CSV file
# M13.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\M13.csv', index=False)
###################################### M14 ##################################################
current_month = datetime.now().replace(day=1)

next_month = (current_month + relativedelta(months=13)).strftime('%Y%m')


# Compare 'Del Schedule Mth' with the current year and month
df['M14'] = df['Weighted Qty'].where(df['Est Receipt Mth'] == next_month, None)

# save updated DataFrame to CSV file
# M14.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\M14.csv', index=False)
###################################### M15 ##################################################
current_month = datetime.now().replace(day=1)

next_month = (current_month + relativedelta(months=14)).strftime('%Y%m')


# Compare 'Del Schedule Mth' with the current year and month
df['M15'] = df['Weighted Qty'].where(df['Est Receipt Mth'] == next_month, None)

# save updated DataFrame to CSV file
# M2.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\M15.csv', index=False)
###################################### M16 ##################################################
current_month = datetime.now().replace(day=1)

next_month = (current_month + relativedelta(months=15)).strftime('%Y%m')

# Compare 'Del Schedule Mth' with the current year and month
df['M16'] = df['Weighted Qty'].where(df['Est Receipt Mth'] == next_month, None)

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\M16.csv', index=False)
###################################### Forecast Flag ##################################################

df["Forecast Flag"] = np.where(df["Status Description"].isin(["WON","LOST","DISCONTINUED"]), 0,
                    np.where(df["Phase Code-Current"].isin(["19-OMITTED","18-CLOSED"]), 0,
                    np.where(np.sum(df[["M1","M2","M3","M4","M5","M6","M7","M8","M9","M10","M11","M12"]].fillna(0), axis=1)==0, 0,
                    np.where((df["Phase Code-Current"]=="6-PIPELINE") & (np.sum(df[["M1","M2","M3","M4"]].fillna(0), axis=1)>0), 0, 1))))

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Forecast Flag.csv', index=False)

###################################### Solution Weighted $K ##################################################

df['Solution Weighted $K'] = np.multiply(df['Solution Value-US'], df['Estimated Win Probability'])

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Solution Weighted $K.csv', index=False)

###################################### HW Weighted $K ##################################################

# assuming your dataframe is named "df"
df["HW Weighted $K"] = np.where(df["Forecast Category"] == "8-COMMIT",
                                df["Estimated Receipt Value-US"] * 1,
                                df["Estimated Receipt Value-US"] * df["Estimated Win Probability"])

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\HW Weighted $K.csv', index=False)
###################################### Exp Book Yr/Qtr ##################################################
# round the MCC column to 0 decimal places
df['Unit MCC USD'] = df['MCC'].round(0)

df['HW MCC $K'] = df['Estimated Receipt Units'] * df['Unit MCC USD'] / 1000

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\HW MCC $K.csv', index=False)

###################################### HW Weighted MCC $K ##################################################

# round the MCC column to 0 decimal places
df['Unit MCC USD'] = df['MCC'].round(0)

df['HW Weighted MCC $K'] = df['Weighted Qty'] * df['Unit MCC USD'] / 1000

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\HW Weighted MCC $K.csv', index=False)

###################################### Unit Sell Price USD ##################################################

# apply the logic using pandas functions and numpy vectorization
df['Unit Sell Price USD'] = np.where(
    pd.isnull(df['Estimated Receipt Units']) | (df['Estimated Receipt Units'] == 0),
    0,
    np.round((df['Estimated Receipt Value-US'] / df['Estimated Receipt Units']) * 1000)
)

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Unit Sell Price USD.csv', index=False)

###################################### Unit MCC USD ##################################################

df['Unit MCC USD'] = df['MCC'].round(0)

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Unit MCC USD.csv', index=False)

###################################### Past Due (>1 mth) ##################################################

df["Phase Code-Current"] = df["Phase Code-Current"].astype(str)

# Assuming you have a DataFrame with a "phase code-current" column
df["Forecast Category"] = df["Phase Code-Current"].str.split("-").str[1]

df['Weighted Qty'] = np.where(df['Forecast Category'] == 'COMMIT', df['Estimated Receipt Units'] * 1, df['Estimated Receipt Units'] * df['Estimated Win Probability'])

now = datetime.now()

df['Past Due (>1 mth)'] = np.where(((df['Est Receipt Mth'].str[:4].astype(float).astype(int) - pd.Timestamp.now().year) * 12 + 
                                     df['Est Receipt Mth'].str[-2:].astype(float).astype(int) - pd.Timestamp.now().month)<-1,
                                    df['Weighted Qty'], 0)
# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Past Due Greterthen 1 mth.csv', index=False)

###################################### Past Due (=<1 mth) ##################################################
df['Est Receipt Mth'] = df['Est Receipt Mth'].astype(str)

# Assuming the data is in a DataFrame called 'df'
df['Past Due (=<1 mth)'] = np.where(((df['Est Receipt Mth'].str[:4].astype(float).astype(int) - pd.Timestamp.now().year) * 12 + 
                                     df['Est Receipt Mth'].str[-2:].astype(float).astype(int) - pd.Timestamp.now().month).astype(str) == '-1',
                                    df['Weighted Qty'], 0)

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Past Due Greater .csv', index=False)

######################################Exp Book Yr/Qtr##################################################
#print('Exp Book Yr/Qtr')
# use numpy.where to create the new column
df['Before'] = np.where(df['Past Due (>1 mth)'].isin(['Yes']),
                         df['Weighted Qty'], np.nan)

# save updated DataFrame to CSV file
#df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Before.csv', index=False)

###################################### Industry Group ##################################################

# Apply the Switch statement logic using numpy.where
df['Industry Group Code'] = np.where(
    df['Industry Group Code'] == 'HSR', 'HOSP', df['Industry Group Code']
)
# save updated DataFrame to CSV file
df.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Sales Report Prefinal.csv', index=False)

###################################### Finaly Report ##################################################
df['Oppty Record Type']=df['Opportunity Type Desc']

df.rename(columns={'Expected Delivery Date':'Expected Delivery Start Date', 'UNIT_TYPE':'S&OP Unit', 'Org Code':'Sales Org', 'LOB_REGION':'LOB Region', 'LOB_AREA':'LOB Area', 'Region Code':'Region', 'Area Name':'Area', 'Country Name':'Country', 'Master Customer Name':'Customer Name', 'Master Customer Number':'Customer Number', 'HW or SW':'Product Type', 'UNIT':'S&OP Unit', 'Offer_PF':'S&OP Offer Portolio', 'Family':'Product Range', 'Top Line Product Name':'Topline', 'Deposit Level 1':'Module Type', 'CPM Level 1':'CPM Type', 'Selling Stage Name':'Selling Stage', 'Estimated Win Probability':'Est Win Prob', 'Estimated Close Mth':'Exp Book Mth', 'Expected Book Date':'Exp Book Date', 'Est Receipt Mth':'Del Schedule Mth','Estimated Receipt Units':'Oppty Qty', 'Revenue Trigger':'Invoice Trigger', 'Status Description':'Oppty Status', 'Solution Value-US':'Solution $K', 'Estimated Receipt Value-US':'HW $K', 'Industry Group Code':'Industry Group', 'Customer Industry Code':'Industry', 'Risk Commit Color Text':'Commitment Risk','Opportunity Type Desc':'Opportunity Type' ,'New Product':'NPI Flag', 'Marketing Prog Name':'Marketing Program', 'Primary Campaign Name':'Primary Campaign', 'Add from EDW':'Competitor Name'}, inplace=True)

df['FinMktg Top 200 Customer']='Need to add to query'
df['Competitor Name']='Add from EDW'


FinalReport = pd.DataFrame()

###################################### Finaly Report ##################################################
FinalReport = df[['Region', 'Area', 'LOB Region', 'LOB Area', 'Country', 'Country Code', 'Sales Org', 'Key Account', 'Customer Name', 'Customer Number', 'Opportunity Description', 'Opportunity Number', 'Owner Name', 'Owner Manager Name', 'Product Type', 'S&OP Unit', 'S&OP Offer Portolio', 'Product Range', 'Class', 'Parent Model' , 'Product Sequence Number', 'Topline'  , 'Deposit/Other' ,
      'Module Type', 'CPM Type', 'Forecast Category', 'Forecast Flag', 'Selling Stage', 'Est Win Prob', 'Exp Book Yr/Qtr', 'Exp Book Mth', 'Exp Book Date', 'Del Schedule Mth', 'Del Schedule Yr/Qtr', 'Estimated Receipt Date', 'Expected Delivery Start Date', 'Exp Del Time (Days)',
        'Schedule Mth', 'Oppty Qty', 'Weighted Qty', 'Win Adjusted Qty', 'Unscheduled' , 'Past Due (>1 mth)', 'Past Due (=<1 mth)', 'Exclude', 'Before', 'M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8', 'M9', 'M10', 'M11',
           'M12', 'M13', 'M14', 'M15', 'M16', 'Invoice Trigger', 'Oppty Status', 'Date Closed', 'Solution $K', 'Solution Weighted $K', 'HW $K', 'HW Weighted $K', 'HW MCC $K', 'HW Weighted MCC $K', 'Unit Sell Price USD', 'Unit MCC USD', 'Industry Group', 'Industry', 'Commitment Risk', 'Oppty Record Type', 'Date Created', 'Last Modified Date Time'
             , 'PBO Flag', 'PBO Order Number', 'NPI Flag', 'Fulfilled By Partner', 'Opportunity Type', 'FinMktg Top 200 Customer', 'Marketing Program', 'Primary Campaign', 'Risk Comment Text', 'Competitor Name', 'Contractual Commit','FinMktg Top 200 Customer','Competitor Name'
  ]]
 
FinalReport = FinalReport.loc[:, ~FinalReport.columns.duplicated()]

FinalReport.to_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Sales Funnel Report.csv', index=False)

FinalReport = pd.read_csv(r'E:\_Projects\SFDC Automation\Sales Funnel input files\Sales Funnel Output\Final Report\Sales Funnel Report.csv' ,na_values=[''],keep_default_na=False,low_memory=False)

FinalReport = FinalReport.drop_duplicates()


########################### Update KA


SQL_Master = SQL_Master[['MASTER_CUST_NUM','KEY_ACCOUNT']]

SQL_Master.drop_duplicates(inplace=True)

FinalReport['Customer Number']=  FinalReport['Customer Number'].astype(str)
SQL_Master['MASTER_CUST_NUM']=  SQL_Master['MASTER_CUST_NUM'].astype(str)

FinalReport = pd.merge(FinalReport,SQL_Master,left_on='Customer Number', right_on='MASTER_CUST_NUM',how='left')

mask = FinalReport['KEY_ACCOUNT'].isnull()
FinalReport.loc[mask,'KEY_ACCOUNT'] = "OTHER-" + FinalReport.loc[mask, 'Country Code']


FinalReport = FinalReport.filter(regex='^(?!.*Key Account$)')

FinalReport = FinalReport.rename(columns=lambda x: x.replace('KEY_ACCOUNT', 'Key Account'))

FinalReport = FinalReport[['Region', 'Area', 'LOB Region', 'LOB Area', 'Country', 'Country Code', 'Sales Org', 'Key Account', 'Customer Name', 'Customer Number', 'Opportunity Description', 'Opportunity Number', 'Owner Name', 'Owner Manager Name', 'Product Type', 'S&OP Unit', 'S&OP Offer Portolio', 'Product Range', 'Class', 'Parent Model' , 'Product Sequence Number', 'Topline'  , 'Deposit/Other' ,
      'Module Type', 'CPM Type', 'Forecast Category', 'Forecast Flag', 'Selling Stage', 'Est Win Prob', 'Exp Book Yr/Qtr', 'Exp Book Mth', 'Exp Book Date', 'Del Schedule Mth', 'Del Schedule Yr/Qtr', 'Estimated Receipt Date', 'Expected Delivery Start Date', 'Exp Del Time (Days)',
        'Schedule Mth', 'Oppty Qty', 'Weighted Qty', 'Win Adjusted Qty', 'Unscheduled' , 'Past Due (>1 mth)', 'Past Due (=<1 mth)', 'Exclude', 'Before', 'M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8', 'M9', 'M10', 'M11',
           'M12', 'M13', 'M14', 'M15', 'M16', 'Invoice Trigger', 'Oppty Status', 'Date Closed', 'Solution $K', 'Solution Weighted $K', 'HW $K', 'HW Weighted $K', 'HW MCC $K', 'HW Weighted MCC $K', 'Unit Sell Price USD', 'Unit MCC USD', 'Industry Group', 'Industry', 'Commitment Risk', 'Oppty Record Type', 'Date Created', 'Last Modified Date Time'
             , 'PBO Flag', 'PBO Order Number', 'NPI Flag', 'Fulfilled By Partner', 'Opportunity Type', 'FinMktg Top 200 Customer', 'Marketing Program', 'Primary Campaign', 'Risk Comment Text', 'Competitor Name', 'Contractual Commit','FinMktg Top 200 Customer','Competitor Name'
  ]]

print('Creating SFDC Rerport......')

FinalReport.to_csv(r'E:\_Projects\SFDC Automation\Output\Sales Funnel Report.csv', index=False)

print('Step 5 is completed')
print('Report Succesfuly Exported')


