# -*- coding: utf-8 -*-

#%%
def get_customerdata(rs_user_id,rs_password):
    import jthelperfunctions as jthf
    
    query='''
    WITH customer_details AS ( SELECT DISTINCT b.businessid AS custbid, b.displayname 
    AS storename, CASE WHEN bl.businessid IS NOT NULL THEN 'BL' WHEN wl.businessid IS NOT NULL
    THEN 'WL' ELSE 'NONE' END AS exceptions, DATE (b.onboarddatenew) 
    AS onboard_date, b.businesstype, b.businesssubtype, ad.addresslocality 
    FROM business_snapshot b LEFT JOIN basic_cheque_limit_exception ex 
    ON ex.businessid = b.businessid 
    LEFT JOIN cheque_whitelist wl ON b.businessid = wl.businessid 
    LEFT JOIN cheque_blacklist bl ON b.businessid = bl.businessid 
    LEFT JOIN address_snapshot_ ad ON ad.addresstype = 'SHIPPING' 
    AND ad.addressentityid = b.businessid ), order_details 
    AS ( SELECT c.businessid, COUNT(DISTINCT 
    (CASE WHEN quantity - cancelled_units - returned_units - return_requested_quantity > 0 
    THEN DATE (bo.src_created_time) END)) AS order_dates, 
    SUM((quantity - cancelled_units - returned_units - return_requested_quantity)*order_item_amount / quantity) 
    AS order_value, COUNT(CASE WHEN ei.reattempt_quantity > 0 
    THEN ei.orderitem_id END) AS reattempt_orderitems, 
    COUNT(CASE WHEN ei.return_quantity > 0 
    AND rc.isfillrate IS FALSE THEN ei.orderitem_id END) 
    AS return_orderitems, COUNT(DISTINCT (ei.orderitem_id)) as attempted_orderitems, 
    SUM(CASE WHEN distributed IS TRUE 
    THEN (quantity - cancelled_units - returned_units - return_requested_quantity)*order_item_amount / quantity END) 
    AS fmcg_sales FROM bolt_order_item_snapshot bo JOIN customer_snapshot_ c 
    ON c.customerid = bo.buyer_id JOIN business_snapshot b 
    ON b.businessid = c.businessid JOIN listing_snapshot li 
    ON li.listing_id = bo.listing_id JOIN sellerproduct_snapshot sp 
    ON sp.sp_id = li.sp_id JOIN product_snapshot_ p ON p.jpin = sp.jpin 
    JOIN category ca ON ca.pvid = p.pvid LEFT JOIN eod_item_level ei 
    ON bo.order_item_id = ei.orderitem_id LEFT JOIN eil_reason_code rc 
    ON rc.reason_code = ei.reason_code GROUP BY 1 ) select c.*,o.order_dates,o.order_value,
    case when attempted_orderitems=0 then 0 else reattempt_orderitems*1.00/attempted_orderitems end as reattempt_pct, 
    case when attempted_orderitems=0 then 0 else return_orderitems*1.00/attempted_orderitems end as return_pct, 
    case when o.order_value=0 then 0 else fmcg_sales*1.00/o.order_value end as fmcg_share 
    from customer_details c left join order_details o on o.businessid=c.custbid
    '''
    customerData=jthf.getDataFromRedshift(query,rs_user_id,rs_password)
    return customerData

#%%
def chequeCalcs_helper(row, transdf):
    cust_trans = transdf[transdf['bid'] == row['bid']]
    row['totalChequesCount'] = cust_trans.shape[0]
    
    if cust_trans.shape[0] > 0:
        row['totalChequesValue'] = cust_trans.amount.sum()
        
        row['currOutsCount'] = cust_trans.finalstatus[cust_trans['finalstatus'] == "Collected"].count()
        row['currOutsValue'] = cust_trans.amount[cust_trans['finalstatus'] == "Collected"].sum()
        
        row['currBouncedCount'] = cust_trans.finalstatus[cust_trans['finalstatus'] == "Bounced"].count()
        row['currBouncedValue'] = cust_trans.amount[cust_trans['finalstatus'] == "Bounced"].sum()
        
        row['everBouncedCount'] = cust_trans['bouncereason'][cust_trans['bouncereason']!=""].count()
        row['everBouncedValue'] = cust_trans['amount'][cust_trans['bouncereason']!=''].sum()
        row['avgRepayTime'] = cust_trans.repldays.mean()
    else:
        row['totalChequesValue'] = 0
            
        row['currOutsCount'] = 0
        row['currOutsValue'] = 0
            
        row['currBouncedCount'] = 0
        row['currBouncedValue'] = 0
            
        row['everBouncedCount'] = 0
        row['everBouncedValue'] = 0
        row['avgRepayTime'] = 0
        
    return row
 
#%%
#2. get_jtchequedata()
#current 
#bid, current outstanding number of cheques, current outstanding value of cheques,
    #bounced
#history
#everbounced_count, everbounced_value, repaychequebounce_attempts
#total cheques paid count, total cheques paid value, 
def get_jtchequedata(custList,googlesecretkey_location):
    import jthelperfunctions as jthf
    import numpy as np
    import re           

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

    # fix repldays to be an int
    #chequeTransData['repldays']
    
    chequeTransData['repldays'] = chequeTransData['repldays'].apply(lambda x: str(x))
    #chequeTransData['repldays']= chequeTransData['repldays'].str.replace(',','')
    chequeTransData['repldays']= chequeTransData['repldays'].apply(lambda x: re.sub('s+','0',x))
    chequeTransData['repldays'] = chequeTransData['repldays'].replace(np.NaN,'0')
    chequeTransData['repldays'] = chequeTransData['repldays'].replace('','0')
    chequeTransData['repldays']= chequeTransData['repldays'].str.strip('') 
    chequeTransData['repldays']=chequeTransData['repldays'].apply(lambda x: int(x))

    #
    chequeData = custList.apply(chequeCalcs_helper, args=(chequeTransData,), axis=1)
    #chequeData.head()
    print("Cheque Data Ready!")
    return chequeData

    

#
#1. creditProduct - done
#2. creditTransactionLimit - done
#3. creditOverallLimit - done
#4. currCreditOutsCount - done
#5. currCreditOutsValue - done
#6. creditEverBouncedCount - done
#7. creditEverBouncedValue - done
#8. creditAvgRepayDays - done
#9. creditAvgRepayAttemps - done
#10. creditEverUseCount - done
#11. creditEverUseValue - done
   

def get_creditdata(custList,googlesecretkey_location):
    import jthelperfunctions as jthf
    import pandas as pd

    #1. creditProduct - done
    #2. creditTransactionLimit - done
    #3. creditOverallLimit - done
    
    #Get Credit Details
    print("Getting Credit details for customers")
    creditCustData = jthf.getGsheet("customer_credit_details","Sheet1",googlesecretkey_location)
    
    creditCustData = creditCustData[creditCustData['Status'] == "ACTIVE"]
    creditCustData = creditCustData[['businessid', 'transactional_limit', 'overall_limit', 'product']]
    creditCustData.rename(index=str, 
                          columns={'businessid':'bid', 'transactional_limit':'creditTransactionLimit', 
                                   'overall_limit':'creditOverallLimit', 'product':'creditProduct'},
                                   inplace=True)
    # merge creditCustData with custList
    custList=pd.merge(custList, creditCustData, on='bid', how='left')
    
    #
    print("Connecting to [INTERNAL] FundsCorner <> Jumbotail | Collection Tracker")
    creditTranData = jthf.getGsheet("[INTERNAL] FundsCorner <> Jumbotail | Collection Tracker", "Final Sheet", googlesecretkey_location)
    #%credit transaction data, which columsn reqd for 
    print("Getting the data in the required shape and form")
    selCols = ['cust_id', 'net_collection_amount', 'JT_confirmed_cleared',
               'ever_bounced', 'days_to_repay', 'collection_attempts']
    
    creditTranData = creditTranData[selCols]
    
    creditTranData.rename(index=str,
                          columns={'cust_id':'bid', 'net_collection_amount':'amount',
                                   'JT_confirmed_cleared':'status', 
                                   'days_to_repay':'repayDays',
                                   'collection_attempts':'repayAttempts'}, inplace=True)
    
    creditTranData = creditTranData[creditTranData['bid']!=""]
    #ensure numeric columns are numeric
    creditTranData['amount'] = creditTranData.amount.apply(lambda x: jthf.ensureNum(x))
    creditTranData['repayDays'] = creditTranData.repayDays.apply(lambda x: jthf.ensureNum(x))
    creditTranData['repayAttempts'] = creditTranData.repayAttempts.apply(lambda x: jthf.ensureNum(x))
    
    #Calculations by customer
    grpCrTrnsData = creditTranData.groupby('bid', as_index=False)
    
    #grpCrTrnsData.apply(lambda x: x[x['status']!="Cleared"]['amount'].sum())
    
    # currCreditBouncedCount
    # currCreditBouncedValue
    df1 = creditTranData[creditTranData['status']=="Bounced"]
    aggs = {'currCreditBouncedCount':'count', 'currCreditBouncedValue':'sum'}
    currCrBounced = df1.groupby('bid', as_index=False).amount.agg(aggs)
    currCrBounced.head()
    
    #
    #4. currCreditOutsCount - done
    #5. currCreditOutsValue - done
    
    
    df1 = creditTranData[creditTranData['status']!="Cleared"]
    aggs = {'currCreditOutsCount':'count', 'currCreditOutsValue':'sum'}
    currCrOuts = df1.groupby('bid', as_index=False).amount.agg(aggs)
    currCrOuts.head()
    #
    #6. creditEverBouncedCount
    #7. creditEverBouncedValue
    
    df1 = creditTranData[creditTranData['ever_bounced']=="Yes"]
    aggs = {'creditEverBouncedCount':'count', 'creditEverBouncedValue':'sum'}
    everCrBounce = df1.groupby('bid', as_index=False).amount.agg(aggs)
    everCrBounce.head()
    
    
    
    #%%
    #8. creditAvgRepayDays
    #9. creditAvgRepayAttempts
    #10. creditEverUseCount
    #11. creditEverUseValue
    
#    aggs = {
#            'amount': {
#                    'creditEverUseCount':'count',
#                    'creditEverUseValue':'sum'},
#            'repayDays':{'creditAvgRepayDays':'mean'},
#            'repayAttempts':{'creditAvgRepayAttempts':'mean'}
#            }
    
    aggs = {'amount':['count','sum'],
            'repayDays':'mean',
            'repayAttempts':'mean'}
    crOverallUse = grpCrTrnsData.agg(aggs)
    crOverallUse.columns = ["_".join(x) for x in crOverallUse.columns.ravel()]
    crOverallUse.rename(columns={'bid_':'bid'}, inplace=True)
    crOverallUse.head()
    
    #%% bring together
    creditData = pd.merge(custList,currCrBounced, on="bid",how="left")
    creditData = pd.merge(creditData,currCrOuts, on="bid",how="left")
    creditData = pd.merge(creditData,everCrBounce, on="bid",how="left")
    creditData = pd.merge(creditData,crOverallUse, on="bid",how="left")
    
    
    #%%
    #rename
    creditData.rename(columns={'amount_count':'creditEverUseCount'},inplace=True)
    creditData.rename(columns={'amount_sum':'creditEverUseValue'},inplace=True)
    creditData.rename(columns={'repayDays_mean':'creditAvgRepayDays'},inplace=True)
    creditData.rename(columns={'repayAttempts_mean':'creditAvgRepayAttempts'},inplace=True)
    print("Credit Data is ready")
    return creditData

#%%
def publishLimits(refreshedData, googlesecretkey_location):
    import jthelperfunctions as jthf
    import pandas as pd
    #create view for CD
    #dummy vars to match current structure
    refreshedData['maxChequeAmountAccepted'] = refreshedData.maxChequeAmountToday
    refreshedData['paidPenalty'] = "Not Yet Available"
    refreshedData['overallCreditAvailable'] = "Remove"
    refreshedData['oldBid'] = "Remove"
    refreshedData['remainingtoclear'] = "Remove"
    #%%
    #select columns for CD View
    cdCols=['bid', 'storename', 'exceptions', 'deliver', 'takeCheque', 
            'maxChequeAmountToday', 'maxChequeAmountAccepted', 'currBouncedCount',
            'currBouncedValue','everBouncedCount','everBouncedValue','paidPenalty',
            'remainingtoclear','takeCredit', 'creditProduct','credit_limit_today',
            'currCreditBouncedCount',
            'currCreditOutsCount', 'currCreditOutsValue', 'overallCreditAvailable',
            'creditTransactionLimit', 'creditOverallLimit', 'oldBid']
    
    cdView = refreshedData[cdCols]
    
    #%%
    cdColNames = ['bid', 'storename', 'note', 'deliver', 'takecheck', 
                  'maxcheckamountaccepted_today', 'maxcheckamountaccepted', 
                  'bounced_check_outstanding', 'bounced_check_outstanding_amount', 
                  'total_everbounced', 'cum_bounce_amount', 'hasclearedbounce', 
                  'remainingtoclear', 'take_credit', 'credit_product', 
                  'credit_limit_today', 'pdc_bounced_cheques', 
                  'pdc_oustanding_count', 'pdc_oustanding_amount', 
                  'overall_credit_limit_available', 'credit_transaction_limit', 
                  'credit_overall_limit', 'oldbid']
    
    cdView.columns = cdColNames
    
    #%%
    #set filename
    #Check_Credit_Reference_Nov_11_2017
    todStr=pd.to_datetime('today').strftime("%b_%d_%Y")
    tkrFileName="TEST_Check_Credit_Reference_"+todStr
    print("Writing ", tkrFileName, " to Googlesheets...")
    #write CD view to GS
    cdGSheet = jthf.writeGsheet(cdView,'A1',tkrFileName,"Sheet1", googlesecretkey_location)
    #permission CD sheet and send
    cdGSheet.share('sohamgsen@gmail.com', role='commenter')
    #%%
    
    #create view for SCM
    
    #write SCM view to GS
    
    #permisison and send GS

    writeStatus = "Successful"
    return writeStatus

#%%
def get_CreditCustomers(googlesecretkey_location):
    import jthelperfunctions as jthf
    creditCustomers = jthf.getGsheet('customer_credit_details', 'Sheet1', googlesecretkey_location)
    creditCustomers.rename(columns={'bid':'oldbid', 'businessid':'bid' }, inplace = True)
    creditCustomers = creditCustomers[creditCustomers.Status == 'ACTIVE']   
    creditCustomers = creditCustomers[['bid']] 
    creditCustomers['creditActive'] = 'yes'
    return creditCustomers