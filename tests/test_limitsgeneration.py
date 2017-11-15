import jtdatafunctions as jtdf
import jthelperfunctions as jthf
import jtlimitslogicfunctions as jtlf
#import pandas as pd
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
cust_data=jtdf.get_customerdata(rs_user_id,rs_password)
cust_data.rename(index=str, columns={'custbid':'bid'}, inplace=True)
#Forming a list of all customers based on the data fetched to fetch cheque and credit details
#%%
print("Forming a list of all customers")
custList = cust_data.iloc[:,0:1]
custList.rename(index=str, columns={'custbid':'bid'}, inplace=True)

#%%
#all bids for cheque and credit data
print("Fetching cheque data")
cheque_data=jtdf.get_jtchequedata(custList,googlesecretkey_location)

#%%
print("Fetching credit data")
credit_data=jtdf.get_creditdata(custList,googlesecretkey_location)

#%%
print('Refresh Limits...')
#refreshedData = jtlf.refreshLimits(cust_data, custList, cheque_data, credit_data)
#merge data sets
finalData = cust_data[['bid','storename','exceptions','order_dates', 'order_value','reattempt_pct']]
finalData = finalData.merge(cheque_data, how='left', on='bid')
finalData = finalData.merge(credit_data, how='left', on='bid')
finalData.fillna(0,inplace=True)

#%%
finalData['totalBouncedOutstanding'] = finalData.apply(jtlf.totalBouncedOutstanding, axis=1)

#%%
finalData['deliver']=finalData.apply(jtlf.deliver, axis=1)

#%%
finalData['takeCheque']=finalData.apply(jtlf.takeCheque, axis=1)

#%%
finalData['maxChequeAmountToday']=finalData.apply(jtlf.maxChequeAmountToday, axis=1)
 
#%%
finalData['takeCredit']=finalData.apply(jtlf.takeCredit, axis=1)
  
#%%
finalData['credit_limit_today']=finalData.apply(jtlf.credit_limit_today, axis=1)





