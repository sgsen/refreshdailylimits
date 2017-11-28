import jtdatafunctions as jtdf
import jthelperfunctions as jthf
import jtlimitslogicfunctions as jtlf
import pandas as pd

#%%
startTime = pd.Timestamp.now() 
print('Running Limits Refresh', startTime, '...')
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
#cheque_data=jtdf.get_jtchequedata(custList,googlesecretkey_location)
cheque_data=jtdf.get_jtchequedata2(custList,googlesecretkey_location)

#%%
print("Fetching credit data")
credit_data=jtdf.get_creditdata(custList,googlesecretkey_location)

#%%
print("Fetching credit customers data")
credit_cust = jtdf.get_CreditCustomers(googlesecretkey_location)


#%%
print('Refreshing Limits...')
refreshedData = jtlf.refreshLimits(cust_data, credit_cust, cheque_data, credit_data)


#%%
print('Clean final file, Create views for SCM and CD, and write to GS')
status = jtdf.publishLimits(refreshedData, googlesecretkey_location)
endTime = pd.Timestamp.now()
runTime = endTime - startTime
print(status, endTime, 'Total Time:', runTime)

#fixes
#credit_product is going to zero when it's missing instead of none
#done#credit_product is not factoring into takecredit()
#redo get_jtchequedata() to be faster
#figure out how to fix the 
#max checque doesn't grandfather in older clients
