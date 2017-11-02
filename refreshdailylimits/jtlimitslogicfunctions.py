# -*- coding: utf-8 -*-
def bouncedOutstanding(row):
    if row['currCreditOutsCount']>0 or row['currBouncedCount']>0 :
        return 'TRUE'
    else:
        return 'FALSE'

def get_bouncedOutstanding(custData,creditData,chequeData):
    import pandas as pd
    finalData=pd.DataFrame(custData[['bid','storename','exceptions']])
    finalData=finalData.merge(creditData, how='inner', on='bid')
    finalData=finalData.merge(chequeData, how='inner', on='bid')
    finalData = finalData[['bid','storename','exceptions','currBouncedCount','currCreditOutsCount']]
    finalData['bouncedOutstanding'] = finalData.apply(bouncedOutstanding, axis=1)
    return finalData


def deliver(row):
    if row['bouncedOutstanding']=='TRUE':
        return 'FALSE'
    else:
        return 'TRUE'

#take_cheque()

#cheque_limit_today()

#max_cheque_limit()

#take_credit()

#credit_limit_today()

