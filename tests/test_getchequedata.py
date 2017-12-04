# -*- coding: utf-8 -*-
#%%
import refreshdailylimits.jthelperfunctions as jthf
import numpy as np
import re
import pandas as pd           

#
#Run main.py for credentials in system
gskl = googlesecretkey_location
cList = custList

#%%
googlesecretkey_location = gskl
custList = cList


#%%
#get tranasactions

print("Getting transactions data from google sheets")
chequeTransData = jthf.getGsheet("Cheque Payment & Exposure Tracker", "Master Data", googlesecretkey_location)

# choose the columns that are required for analysis
print("Using the info to get cheque data in the desired format")
selectCols = ['Date', 'BID', 'Amount', 'Final Status', 'Bounce Reason',
       'Replacement Days']
chequeTransData = chequeTransData[selectCols]
del selectCols
#rename the variables to be easy to work with
chequeTransData.rename(index=str, 
                       columns={'Date':'date', 'BID':'bid',
                                'Amount':'amount', 'Final Status':'finalstatus',
                                'Bounce Reason':'bouncereason', 'Replacement Days':
                                    'repldays'}, inplace=True)
# deal with the mixed data types in amount column
#convert all to string
chequeTransData['amount'] = chequeTransData['amount'].apply(lambda x: str(x))
#get rid of commas
chequeTransData['amount'] = chequeTransData['amount'].str.replace(',','')
#get rid of the rows that have empty amount cols
chequeTransData = chequeTransData[chequeTransData['amount'] != '']
#convert all of these to float type
chequeTransData['amount'] = chequeTransData['amount'].astype(float)

chequeTransData['repldays'] = chequeTransData.repldays.apply(lambda x: jthf.ensureNum(x))

chequeTransData['date'] = pd.to_datetime(chequeTransData.date)

#%%
brstrings = {
        'Insufficient Funds':'Insufficient Funds|[Ii]nsufficient',
        'Connectivity':'onnectivity',
        'Other Reasons':'[Oo]thers',
        'Signature Mismatch':'[Ss]ignature', 
        'Exceeds Arrangement':'(?i)exceed',
        'Words_Figures Differ':'mount|[Ww]ords|[Dd]iffer',
        'Wrong Date':'[Dd]ate',
        'Customer Blocked':'[Bb]lock',
        'Drawer Issue':'[Dd]raw'
        }

#import re

def fixBlanks(x):
    if x == '':
        return 'NA'
    else:
        return x

def fixBounceReason(x, namedict):
    for key, value in namedict.items():
        if re.search(value, x) is not None:
            #print('Found ', value, ' in ', x)
            return key
    #print('Didnt find',value,'in',x)
    return x

def custBounce(x):
    yes = 1
    no = 0
    if x in ['Insufficient Funds', 'Signature Mismatch', 'Exceeds Arrangement',
              'Drawer Issue', 'Customer Blocked']:
        return yes
    else:
        return no

breasons = chequeTransData.bouncereason.apply(lambda x: str(x))
breasons = breasons.apply(lambda x: fixBlanks(x))
breasons = breasons.apply(lambda x: fixBounceReason(x,namedict = brstrings))
chequeTransData.bouncereason = breasons
chequeTransData['custBounce'] = chequeTransData.bouncereason.apply(lambda x: custBounce(x))


#totalChequesValue
#avgRepayTime
aggs = {'amount':'sum','repldays':'mean'}
df1 = chequeTransData.groupby('bid', as_index=False).agg(aggs)
df1.rename(columns={'amount':'totalChequesValue','repldays_mean':'avgRepayTime'}, inplace=True)
custList = custList.merge(df1, on='bid', how='left')


#%%
#currOutsCount
#currOutsValue
df1 = chequeTransData[chequeTransData.finalstatus == 'Collected']
aggs = {'finalstatus':'count', 'amount':'sum'}
df1 = df1.groupby('bid', as_index=False).agg(aggs)
df1.rename(columns={'finalstatus':'currOutsCount', 'amount':'currOutsValue'},inplace=True)
custList = custList.merge(df1, on='bid', how='left')


#%%
#currBouncedCount
#currBouncedValue
df1 = chequeTransData[(chequeTransData.finalstatus == 'Bounced') & (chequeTransData.custBounce == 1)]
aggs = {'finalstatus':'count','amount':'sum'}
df1 = df1.groupby('bid', as_index=False).agg(aggs)
df1.rename(columns={'finalstatus':'currBouncedCount', 'amount':'currBouncedValue'},inplace=True)
custList = custList.merge(df1, on='bid', how='left')

#%%
#everBouncedCount
#everBouncedValue
df1 = chequeTransData[chequeTransData.custBounce == 1]
aggs = {'finalstatus':'count','amount':'sum'}
df1 = df1.groupby('bid', as_index=False).agg(aggs)
df1.rename(columns={'finalstatus':'everBouncedCount', 'amount':'everBouncedValue'},inplace=True)
custList = custList.merge(df1, on='bid', how='left')

#%%
#maxChequeAccepted,#grandfather_max
#we didn't have a max limit till April 7 at which point we dedided to
#accept cheque limits beyond the 30000 per day from existing customers who had
#given more than that before
df1 = chequeTransData[(chequeTransData.date < '2017-04-01') & (chequeTransData.custBounce == 0)]
aggs = {'amount':'max'}
df1 = df1.groupby('bid', as_index=False).agg(aggs)
df1.rename(columns={'amount':'grandpaMax'},inplace=True)
custList = custList.merge(df1, on='bid', how='left')
custList.fillna(0, inplace=True)
#%%
#chequeData.head()
print("Cheque Data Ready!")
#return chequeData
