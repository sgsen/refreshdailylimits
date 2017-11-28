# -*- coding: utf-8 -*-
def totalBouncedOutstanding(row):
    total = row.currBouncedCount + row.currCreditBouncedCount
    return total


def deliver(row):
    if row.totalBouncedOutstanding > 0:
        return 'no'
    else:
        return 'yes'

# if you can use pdc no cheque, max 5 bounces across products, blocked if
# bounced cheque outstanding
        
def takeCheque(row):
    totalEverBounced = row.creditEverBouncedCount + row.everBouncedCount
    if row.exceptions =='WL' and row.creditActive!='yes':
        return 'yes'
    elif (row.exceptions=='BL' or row.deliver =='no' or totalEverBounced > 5 or 
        (row.creditActive == 'yes' and 
         (row.creditProduct == 'FundsCorner-PDC' or 
          row.creditProduct == 'FundsCorner-CASH'))):
        return 'no'
    else:
        return 'yes'

def maxChequeAmountToday(row):
    import math
    
    if row['takeCheque']=='no':
        return 0
    elif row['grandpaMax'] > 30000:
        #grandfather clause
        gpaMax = row['grandpaMax']
        gpaMax = math.ceil(gpaMax/1000)*1000
        return (gpaMax)
    elif row['order_dates']>4:
        return 30000
    else:
        return 10000

#take_credit()
def takeCredit(rw):
    totalEverBounced = rw.creditEverBouncedCount + rw.everBouncedCount
    if rw.creditActive != 'yes':
        return 'no'
    elif rw.exceptions=='WL':
        return 'yes'
    elif (rw.deliver == 'no' or rw.exceptions=='BL' or totalEverBounced > 5):
        return 'no'
    else:
        return 'yes'

#cash credit transaction limited to 
#credit_limit_today()
def credit_limit_today(rw):
    if rw.takeCredit == 'no':
        return 0
    else:
        limit = rw.creditOverallLimit - rw.currCreditOutsValue
        if limit > rw.creditTransactionLimit:
            limit = rw.creditTransactionLimit
        return limit

def limitCashCreditTrans(rw):
    limit = rw.creditTransactionLimit
    if (rw.creditProduct == 'FundsCorner-CASH'):
        limit = 15000
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
    finalData['creditTransactionLimit'] = finalData.apply(limitCashCreditTrans, axis=1)
   
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
