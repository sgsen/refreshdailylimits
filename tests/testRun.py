# -*- coding: utf-8 -*-
# sgsen@jumbotail.com, puja@jumbotail.com
import refreshdailylimits.jtdatafunctions as jtdf
import refreshdailylimits.jthelperfunctions as jthf
import refreshdailylimits.jtlimitslogicfunctions as jtlf
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
cheque_data=jtdf.get_jtchequedata(custList,googlesecretkey_location)

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
print('Fetching orders to be delivered today...')
deliveriesToday = jtdf.get_ordersDeliveryToday(rs_user_id,rs_password)

#%%
print('Identifying Customers Who Will Exceed Cheque or Credit Limits Today...')
callExceededLimits = jtlf.idExceedLimits(refreshedData, deliveriesToday)

#%%
print('Clean final file, Create views for SCM and CD, and write to GS')
#status = jtdf.publishLimits(refreshedData, callExceededLimits, googlesecretkey_location)

status = jtdf.publishLimitsTest(refreshedData, callExceededLimits, googlesecretkey_location)

#%%
endTime = pd.Timestamp.now()
runTime = endTime - startTime
print(status, endTime, 'Total Time:', runTime)

#%% test_purposes 
#uploads the total dataset used to generate limits to GS
todStr=pd.to_datetime('today').strftime("%b_%d_%Y")
tkrFileName2="TEST_Check_Credit_Reference_ALLDATA_"+todStr
jthf.writeGsheet(refreshedData, 'A1',tkrFileName2,'Sheet1',googlesecretkey_location)

