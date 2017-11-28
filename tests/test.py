# -*- coding: utf-8 -*-


#%% 
'''
The goal here is to compare the newly generated limits tracker with the
one we already have generated by the R code. 

I would want to make sure: 
    - all the customers are there in the new sheet
    - we understand the differences in:
        - deliver
        - take check
        - cheque limit today
        - take credit
        - credit limit today
Note only run after both sheets have been generated and written to GoogleSheets
The new system sheets are titled: TEST_Check_Credit_Reference_mmm_dd_YYYY
The old system sheets are titled: Check_Credit_Reference_mmm_dd_YYYY
'''
#%% set up

import jthelperfunctions as jthf
#import jtdatafunctions as jtdf
import pandas as pd

#credentials=jthf.getUserCredentials()
gskl=googlesecretkey_location

#%% get both sheets and join by bid

todStr=pd.to_datetime('today').strftime("%b_%d_%Y")
newFileName="TEST_Check_Credit_Reference_"+todStr
currFileName="Check_Credit_Reference_"+todStr
#%%
new = jthf.getGsheet(newFileName,"Sheet1",gskl)
#%%
current = jthf.getGsheet(currFileName,currFileName,gskl)
#%%
combined = current.merge(new, how='left', on='bid')

#%% Are the customer counts the same?
print("Are the row counts the same?", new.shape[0] == current.shape[0])
print("Rows in New:", new.shape[0])
print("Rows in Current:", current.shape[0])

nullRows = combined[combined.isnull().any(axis=1)]
nullRows.shape


#%% Show me all the rows where delivery, take check, check limit, 
#take credit, credit limit

def fixDeliver(x):
    if x == 'no - call CD':
        return 'no'
    else:
        return 'yes'
    
combined['deliver_x'] = combined['deliver_x'].apply(lambda x: fixDeliver(x))

def fixtake_credit_x(x):
    if x == 0:
        return 'no'
    else:
        return 'yes'

combined['take_credit_x'] = combined['take_credit_x'].apply(lambda x: fixtake_credit_x(x))
#%%
def comparexy(x):
    var_x=x+'_x'
    var_y=x+'_y'
    test = combined[combined[var_x]!=combined[var_y]]
    print(test[[var_x, var_y]])
    print(test.shape)
    
#%% Compare Delivery
comparexy('deliver')
#%% Compare Take Chequue
comparexy('takecheck')
#%% Compare Take Credit
comparexy('take_credit')

#%% output all data for testing
todStr=pd.to_datetime('today').strftime("%b_%d_%Y")
tkrFileName2="TEST_Check_Credit_Reference_ALLDATA_"+todStr
jthf.writeGsheet(refreshedData, 'A1',tkrFileName2,'Sheet1',googlesecretkey_location)