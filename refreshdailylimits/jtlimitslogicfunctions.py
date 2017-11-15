# -*- coding: utf-8 -*-
def totalBouncedOutstanding(row):
    total = row.currBouncedCount + row.currCreditBouncedCount
    return total


def deliver(row):
    if row.totalBouncedOutstanding > 0:
        return 'no'
    else:
        return 'yes'

# this is not right; missing the total bounce count barrier
def takeCheque(row):
    totalEverBounced = row.creditEverBouncedCount + row.everBouncedCount
    if row['exceptions']=='BL' or row['deliver']=='no' or totalEverBounced > 5:
        return 'no'
    else:
        return 'yes'

def maxChequeAmountToday(row):
    if row['takeCheque']=='no':
        return 0
    elif row['order_dates']>4:
        return 30000
    else:
        return 10000

#take_credit()
def takeCredit(rw):
    totalEverBounced = rw.creditEverBouncedCount + rw.everBouncedCount
    if (rw.takeCheque == 'no' or rw.exceptions=='BL' or totalEverBounced > 5 or 
    rw.creditActive != 'yes'):
        return 'no'
    else:
        return 'yes'


#credit_limit_today()
def credit_limit_today(rw):
    if rw.takeCredit == 'no':
        return 0
    else:
        limit = rw.creditOverallLimit - rw.currCreditOutsValue
        if limit > rw.creditTransactionLimit:
            limit = rw.creditTransactionLimit
        return limit

#%%
def refreshLimits(cust_data, credit_cust, credit_data, cheque_data):
    print('Refresh Limits...')
    #refreshedData = jtlf.refreshLimits(cust_data, custList, cheque_data, credit_data)
    #merge data sets
    finalData = cust_data[['bid','storename','exceptions','order_dates', 'order_value','reattempt_pct']]
    finalData = finalData.merge(cheque_data, how='left', on='bid')
    finalData = finalData.merge(credit_data, how='left', on='bid')
    finalData = finalData.merge(credit_cust, how='left', on='bid')
    
    
    finalData.fillna(0,inplace=True)
    
    #%%
    finalData['totalBouncedOutstanding'] = finalData.apply(totalBouncedOutstanding, axis=1)
    
    #%%
    finalData['deliver']=finalData.apply(deliver, axis=1)
    
    #%%
    finalData['takeCheque']=finalData.apply(takeCheque, axis=1)
    
    #%%
    finalData['maxChequeAmountToday']=finalData.apply(maxChequeAmountToday, axis=1)
     
    #%%
    finalData['takeCredit']=finalData.apply(takeCredit, axis=1)
      
    #%%
    finalData['credit_limit_today']=finalData.apply(credit_limit_today, axis=1)

    return finalData
