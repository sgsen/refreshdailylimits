# -*- coding: utf-8 -*-
# sgsen@jumbotail.com, puja@jumbotail.com
#%%
def totalBouncedOutstanding(row):
    total = row.currBouncedCount + row.currCreditBouncedCount
    return total

#%%
def deliver(row):
    if row.totalBouncedOutstanding > 0:
        return 'no'
    else:
        return 'yes'

#%%
#Version 1
# max 5 bounces across products
# if you have credit/pdc no cheque
# if you reattempt more than 25% of the time in the last month, no cheque
# bounced cheque outstanding
# whitelist unlocks cheques only
        
def takeCheque_v1(row):
    totalEverBounced = row.creditEverBouncedCount + row.everBouncedCount
    if row.exceptions =='WL' and row.creditActive!='yes':
        return 'yes'
    elif (row.exceptions=='BL' or row.deliver =='no' or totalEverBounced > 5 or 
        row.creditActive == 'yes' or row.reattempt_pct > 0.25):
        return 'no'
    else:
        return 'yes'
#%%
#Version 0
#3 bounces you're out, 5 bounces total acx credit and cheque you're out
#can use both cheque and credit pdc at the same time
#whitelist unlocks cheque even if you have credit
def takeCheque_v0(row):
    totalEverBounced = row.creditEverBouncedCount + row.everBouncedCount
    if row.exceptions =='WL':
        return 'yes'
    elif (row.exceptions=='BL' or row.deliver =='no' or totalEverBounced > 5 or
          row.everBouncedCount >=3):
        return 'no'
    else:
        return 'yes'     

#%%
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
#%%
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
#%%

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
#cash credit transaction limited to 15000
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
    finalData['takeCheque']=finalData.apply(takeCheque_v0, axis=1)
    
    #%%
    finalData['maxChequeAmountToday']=finalData.apply(maxChequeAmountToday, axis=1)
     
    #%%
    finalData['takeCredit']=finalData.apply(takeCredit, axis=1)
      
    #%%
    finalData['credit_limit_today']=finalData.apply(credit_limit_today, axis=1)

    return finalData


#%% 
def idExceedLimits(refreshedData, deliveriesToday):
#%%
    #from refreshedData I want: bid, creditToday, chequeToday, 
    #chequeEverused, creditEverUsed, bouncedChequeOuts, bouncedChequeVal
    #pdcBouncedOutStanding, #pdcValue
    cols = ['bid', 'deliver','takeCheque', 'maxChequeAmountToday',
            'takeCredit', 'credit_limit_today', 'currBouncedCount', 
            'currBouncedValue', 'currCreditBouncedCount', 
            'currCreditBouncedValue', 'creditActive', 'creditProduct', 
            'creditEverUseCount', 'totalChequesEver','exceptions', 'order_dates']
    
    refDataSub = refreshedData[cols]
#%%
    refDataSub = refDataSub.reindex(columns = refDataSub.columns.tolist() + ['callRequired','callReason', 'cdName', 
                                     'calledTimestamp', 'custResponse']) 
    
#%%
    willExceedLimits = deliveriesToday.merge(refDataSub, on='bid', how='left')
    
#%%
    def callReasonExceedLimits(rw):
        
        rw.cdName = ''
        rw.calledTimestamp=''
        rw.custResponse=''
        rw.callReason = 'Check with Credit and Payments'
        rw.callRequired='yes'

        
        #if bounced cheques   
        if rw.deliver == 'no':
            rw.callRequired = 'yes'
            rw.callReason = 'Bounced Cheques Outstanding'
            return rw
        
        usesCheques = ''
        usesCredit = ''
        chequeExceeded = ''
        creditExceeded = ''
        bothChequeCreditExceeded = ''
        
        #usesCheque? has used more than 5 times or once before activation
        if rw.totalChequesEver >= 5:
            usesCheques = 'yes'
        elif (rw.totalChequesEver > 1) and (rw.order_dates<=5):
            usesCheques = 'yes'
        else:
            usesCheques = 'no'
        
        #usesCheque? has used more than 3 times or once before activation
        if (rw.creditEverUseCount >= 3) and (rw.creditActive=='yes'):
            usesCredit = 'yes'
        else:
            usesCredit = 'no'
        
        #chequeLimit Exceeded?
        if (rw.order_value>=rw.maxChequeAmountToday):
            chequeExceeded = 'yes'
        else:
            chequeExceeded = 'no'
        
        #creditLimitExceeded?
        if (rw.order_value>=rw.credit_limit_today):
            creditExceeded = 'yes'
        else:
            creditExceeded = 'no'
        
        #both
        if (rw.order_value>=(rw.credit_limit_today+rw.credit_limit_today)):
            bothChequeCreditExceeded = 'yes'
        else:
            bothChequeCreditExceeded = 'no'
        
        #set reason
        if (usesCheques == 'yes') and (usesCredit == 'yes') and (bothChequeCreditExceeded == 'yes'):
            rw.callRequired = 'yes'
            rw.callReason = 'Credit and Cheque Limits Insufficient'
        elif (usesCheques == 'yes') and (chequeExceeded == 'yes'):
            rw.callRequired = 'yes'
            rw.callReason = 'Cheque Limit Insufficient'
        elif (usesCredit == 'yes') and (creditExceeded == 'yes'):
            rw.callRequired = 'yes'
            rw.callReason = 'Credit Limit Insufficient'
        else:
            rw.callRequired = 'no'
            rw.callReason = 'Call Not Required'
        
        return rw    
  #%%
    willExceedLimits = willExceedLimits.apply(callReasonExceedLimits, axis=1)
    willExceedLimits = willExceedLimits[willExceedLimits.callRequired=='yes']
    
    return willExceedLimits
    
    
    
    