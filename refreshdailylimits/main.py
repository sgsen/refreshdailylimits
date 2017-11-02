import jtdatafunctions as df
import jthelperfunctions as jthf
#import jtlimitslogicfunctions as jtlf
import pandas as pd
#%%
#Fetching Credentials
print("Fetching Credentials")
credentials=jthf.getUserCredentials()
print("Credentials fetched")
#Saving Credentials for use
rs_user_id=credentials.get('rs_user_id')
rs_password=credentials.get('rs_password')
googlesecretkey_location=credentials.get('googlesecretkey_location')

#%%
#Getting Customer Data
print("Fetching Customer data")
cust_data=df.get_customerdata(rs_user_id,rs_password)
cust_data.rename(index=str, columns={'custbid':'bid'}, inplace=True)
#Forming a list of all customers based on the data fetched to fetch cheque and credit details
#%%
print("Forming a list of all customers")
custList = cust_data.iloc[:,0:1]
custList.rename(index=str, columns={'custbid':'bid'}, inplace=True)

#%%
#all bids for cheque and credit data
print("Fetching cheque data")
cheque_data=df.get_jtchequedata(custList,googlesecretkey_location)

#%%
print("Fetching credit data")
credit_data=df.get_creditdata(custList,googlesecretkey_location)

#%%
def bouncedOutstanding(row):
    if row['currCreditOutsCount']>0 or row['currBouncedCount']>0 :
        return 'TRUE'
    else:
        return 'FALSE'
    
def deliver(row):
    if row['bouncedOutstanding']=='TRUE':
        return 'FALSE'
    else:
        return 'TRUE'
def takeCheque(row):
    if row['exceptions']=='BL' or row['deliver']=='FALSE':
        return 'FALSE'
    else:
        return 'TRUE'
def maxChequeAmountToday(row):
    if row['takeCheque']=='FALSE':
        return 0
    elif row['order_dates']>4:
        return 30000
    else:
        return 10000
    
creditData=pd.DataFrame(credit_data[['bid','currCreditOutsCount']])
chequeData=pd.DataFrame(credit_data[['bid','currBouncedCount']])
finalData=pd.DataFrame(cust_data[['bid','storename','exceptions','order_dates']])
finalData=finalData.merge(creditData, how='inner', on='bid')
finalData=finalData.merge(chequeData, how='inner', on='bid')
#finalData = finalData[['bid','storename','exceptions','currBouncedCount','currCreditOutsCount']]
finalData['bouncedOutstanding'] = finalData.apply(bouncedOutstanding, axis=1)
finalData['deliver']=finalData.apply(deliver, axis=1)
finalData['takeCheque']=finalData.apply(takeCheque, axis=1)
finalData['maxChequeAmountToday']=finalData.apply(maxChequeAmountToday, axis=1)
#finalData=finalData[['bid','storename','exceptions','deliver']]





