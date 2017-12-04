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
def get_ordersDeliveryToday(rs_user_id,rs_password):
    import jthelperfunctions as jthf
    query ='''
        SELECT c.businessid,
        bs.displayname AS storename,
        c.phonenumber,
        CASE
        WHEN ch.cheque_usage > 5 THEN TRUE
        ELSE FALSE
        END AS cheque_customer,
        SUM((ord.quantity - ord.cancelled_units)*ord.order_item_amount / ord.quantity) AS order_value
        FROM bolt_order_item_snapshot ord
        JOIN customer_snapshot_ c ON ord.buyer_id = c.customerid
        LEFT JOIN business_snapshot bs ON bs.businessid = c.businessid
        LEFT JOIN promise_snapshot pr ON pr.promised_entity_id = ord.order_item_id
        LEFT JOIN (SELECT store_id AS businessid,
        COUNT(DISTINCT (CASE WHEN cheque IS NOT NULL THEN attempt_date END)) AS cheque_usage
        FROM eod_store_level
        GROUP BY 1) ch ON ch.businessid = c.businessid
        WHERE
        DATE (updated_promise_time) = CURRENT_DATE
        --DATE (updated_promise_time) = CURRENT_DATE-1
        AND   c.istestcustomer IS FALSE
        AND   ord.buyer_id != 'JC-1202555246'
        GROUP BY 1,
        2,
        3,
        4 
        '''
    ordersDeliveryToday = jthf.getDataFromRedshift(query,rs_user_id,rs_password)
    ordersDeliveryToday  = ordersDeliveryToday.iloc[:,[0,1,2,4]]
    ordersDeliveryToday.rename(columns={'businessid':'bid'}, inplace=True)
    return ordersDeliveryToday  
 
#%%
def get_CreditCustomers(googlesecretkey_location):
    import jthelperfunctions as jthf
    creditCustomers = jthf.getGsheet('customer_credit_details', 'Sheet1', googlesecretkey_location)
    creditCustomers.rename(columns={'bid':'oldbid', 'businessid':'bid' }, inplace = True)
    creditCustomers = creditCustomers[creditCustomers.Status == 'ACTIVE']   
    creditCustomers = creditCustomers[['bid']] 
    creditCustomers['creditActive'] = 'yes'
    return creditCustomers    

#%%
#2. get_jtchequedata()
#current 
#bid, current outstanding number of cheques, current outstanding value of cheques,
    #bounced
#history
#everbounced_count, everbounced_value, repaychequebounce_attempts
#total cheques paid count, total cheques paid value, 


def get_jtchequedata(custList, googlesecretkey_location):
    import jthelperfunctions as jthf
    import pandas as pd
    import re           

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
    aggs = {'date':'count','amount':'sum','repldays':'mean'}
    df1 = chequeTransData.groupby('bid', as_index=False).agg(aggs)
    df1.rename(columns={'date':'totalChequesEver','amount':'totalChequesValue','repldays':'avgRepayTime'}, inplace=True)
    custList = custList.merge(df1, on='bid', how='left')
    
    
    #currOutsCount
    #currOutsValue
    df1 = chequeTransData[chequeTransData.finalstatus == 'Collected']
    aggs = {'finalstatus':'count', 'amount':'sum'}
    df1 = df1.groupby('bid', as_index=False).agg(aggs)
    df1.rename(columns={'finalstatus':'currOutsCount', 'amount':'currOutsValue'},inplace=True)
    custList = custList.merge(df1, on='bid', how='left')
    
    
    #currBouncedCount
    #currBouncedValue
    df1 = chequeTransData[(chequeTransData.finalstatus == 'Bounced') & (chequeTransData.custBounce == 1)]
    aggs = {'finalstatus':'count','amount':'sum'}
    df1 = df1.groupby('bid', as_index=False).agg(aggs)
    df1.rename(columns={'finalstatus':'currBouncedCount', 'amount':'currBouncedValue'},inplace=True)
    custList = custList.merge(df1, on='bid', how='left')
    
    #everBouncedCount
    #everBouncedValue
    df1 = chequeTransData[chequeTransData.custBounce == 1]
    aggs = {'finalstatus':'count','amount':'sum'}
    df1 = df1.groupby('bid', as_index=False).agg(aggs)
    df1.rename(columns={'finalstatus':'everBouncedCount', 'amount':'everBouncedValue'},inplace=True)
    custList = custList.merge(df1, on='bid', how='left')
    
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
   
#    #firstChequeDate and Bounce
#    temp = chequeTransData.groupby('bid').date.min().to_frame('firstChequeDate')
#    chequeTransData = chequeTransData.merge(temp, how='left', left_on='bid',right_index=True)
#    def firstChequeBounce(rw):
#        if ((rw.date == rw.firstChequeDate) and rw.custBounce==1):
#            return 1
#        else:
#            return 0
#    chequeTransData['firstBounce']=chequeTransData.apply(firstChequeBounce, axis=1)
#    temp = chequeTransData.groupby('bid').firstBounce.agg(max).to_frame('bouncedFirstCheque')
#    custList = custList.merge(temp, how='left', left_on='bid',right_index=True)
#    #chequeData.head()
    print("Cheque Data Ready!")
    #return chequeData

    return custList



#%%
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
    selCols = ['cust_id', 'cumulative_amount', 'JT_confirmed_cleared',
               'ever_bounced', 'days_to_repay', 'collection_attempts']
    
    creditTranData = creditTranData[selCols]
    
    creditTranData.rename(index=str,
                          columns={'cust_id':'bid', 'cumulative_amount':'amount',
                                   'JT_confirmed_cleared':'status', 
                                   'days_to_repay':'repayDays',
                                   'collection_attempts':'repayAttempts'}, inplace=True)
    
    creditTranData = creditTranData[creditTranData['bid']!=""]
    #ensure numeric columns are numeric
    creditTranData['amount'] = creditTranData.amount.apply(lambda x: jthf.ensureNum(x))
    creditTranData['repayDays'] = creditTranData.repayDays.apply(lambda x: jthf.ensureNum(x))
    creditTranData['repayAttempts'] = creditTranData.repayAttempts.apply(lambda x: jthf.ensureNum(x))
    
    
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
    #8. creditAvgRepayDays
    #9. creditAvgRepayAttempts
    #10. creditEverUseCount
    #11. creditEverUseValue
    
    #Calculations by customer
    grpCrTrnsData = creditTranData.groupby('bid', as_index=False)
   
    aggs = {'amount':['count','sum'],
            'repayDays':'mean',
            'repayAttempts':'mean'}
    crOverallUse = grpCrTrnsData.agg(aggs)
    crOverallUse.columns = ["_".join(x) for x in crOverallUse.columns.ravel()]
    crOverallUse.rename(columns={'bid_':'bid'}, inplace=True)
    crOverallUse.head()
    
    creditData = pd.merge(custList,currCrBounced, on="bid",how="left")
    creditData = pd.merge(creditData,currCrOuts, on="bid",how="left")
    creditData = pd.merge(creditData,everCrBounce, on="bid",how="left")
    creditData = pd.merge(creditData,crOverallUse, on="bid",how="left")
    
    #rename
    creditData.rename(columns={'amount_count':'creditEverUseCount'},inplace=True)
    creditData.rename(columns={'amount_sum':'creditEverUseValue'},inplace=True)
    creditData.rename(columns={'repayDays_mean':'creditAvgRepayDays'},inplace=True)
    creditData.rename(columns={'repayAttempts_mean':'creditAvgRepayAttempts'},inplace=True)
    print("Credit Data is ready")
    return creditData

#%%
def publishLimits(refreshedData, callExceedLimits, googlesecretkey_location):
    import jthelperfunctions as jthf
    import pandas as pd
    
    todStr=pd.to_datetime('today').strftime("%b_%d_%Y")
    #%%
    #write permisison and send GS for exceed Calls
    #callExceedLimits
    tkrFileName="ChequeCreditLimitCalls_"+todStr
    print("Writing ", tkrFileName, " to Googlesheets...")
    exceededGSheet = jthf.writeGsheet(callExceedLimits,'A1',tkrFileName,"Sheet1", googlesecretkey_location)
    #permission send
    exceededGSheet.share('customerdelight@jumbotail.com', role='writer', is_group=True)
#%%
    #create view for CD
    #dummy vars to match current structure
    refreshedData['maxChequeAmountAccepted'] = refreshedData.maxChequeAmountToday
    refreshedData['paidPenalty'] = "NoPenaltyData"
    #refreshedData['everBouncedValue'] = "Removed"
    #refreshedData['oldBid'] = "Removed"
    #refreshedData['remainingtoclear'] = "Removed"
    #refreshedData['overallCreditAvailable'] = "Removed"
    #%%
    #select columns for CD View
    cdCols=['bid', 
            'storename', 
            'exceptions', 
            'deliver', 
            'takeCheque', 
            'maxChequeAmountToday', 
            'maxChequeAmountAccepted', 
            'currBouncedCount',
            'currBouncedValue',
            'paidPenalty',
            'totalChequesEver',
            'totalChequesValue',
            'everBouncedCount',
            'everBouncedValue',
            'takeCredit', 
            'creditProduct',
            'credit_limit_today',
            'currCreditBouncedCount',
            'currCreditBouncedValue',
            'currCreditOutsCount', 
            'currCreditOutsValue',
            'creditTransactionLimit', 
            'creditOverallLimit',
            'creditEverBouncedCount',
            'creditEverBouncedValue',
            'creditEverUseCount', 
            'creditEverUseValue']
    
    cdView = refreshedData[cdCols]
    
    #%%
    #cols for the old appsheets app
#    cdColNames = ['bid', 
#                  'storename', 
#                  'note', 
#                  'deliver', 
#                  'takecheck', 
#                  'maxcheckamountaccepted_today', 
#                  'maxcheckamountaccepted', 
#                  'bounced_check_outstanding', 
#                  'bounced_check_outstanding_amount', 
#                  'total_everbounced', 
#                  'cum_bounce_amount', 
#                  'hasclearedbounce', 
#                  'remainingtoclear', 
#                  'take_credit', 
#                  'credit_product', 
#                  'credit_limit_today', 
#                  'pdc_bounced_cheques', 
#                  'pdc_oustanding_count', 
#                  'pdc_oustanding_amount', 
#                  'overall_credit_limit_available',
#                  'credit_transaction_limit', 
#                  'credit_overall_limit', 
#                  'oldbid']
#    
#    cdView.columns = cdColNames
    
    #%%
    #set filename
    #Check_Credit_Reference_Nov_11_2017
    tkrFileName="Check_Credit_Reference_"+todStr
    print("Writing ", tkrFileName, " to Googlesheets...")
    #write CD view to GS
    cdGSheet = jthf.writeGsheet(cdView,'A1',tkrFileName,"Sheet1", googlesecretkey_location)
    #permission CD sheet and send
    cdGSheet.share('customerdelight@jumbotail.com', role='reader', is_group=True)
    #%%    #create view for SCM, mapping from refreshedData
    scmViewCols = [
            'BusinessId',
            'Business Name',
            'chequeProfileId',
            'creditProfileId',
            'eligibleForCheque',
            'currentChequeLimit',
            'maxChequeLimit',
            'outstandingBouncedCheques',
            'outstandingBouncedChequesAmount',
            'totalBouncedCheques',
            'eligibleForCredit',
            'creditProvider',
            'currentCreditLimit',
            'totalCreditLimit',
            'creditTransactionLimit',
            'bouncedPDC',
            'outstandingPDC',
            'outstandingPDCAmount',
            'cheque_createdTime',
            'cheque_lastUpdatedTime',
            'cheque_lastUpdatedBy',
            'credit_createdTime',
            'credit_lastUpdatedTime',
            'credit_lastUpdatedBy'] 
    
    scmView=pd.DataFrame(columns=scmViewCols, index=refreshedData.index)
    #%%
    scmView.loc[:,'BusinessId']=refreshedData.bid
    scmView.loc[:,'Business Name']=refreshedData.storename
    scmView.loc[:,'chequeProfileId']=''
    scmView.loc[:,'creditProfileId']=''
    scmView.loc[:,'eligibleForCheque']=refreshedData.takeCheque
    scmView.loc[:,'currentChequeLimit']=refreshedData.maxChequeAmountToday
    scmView.loc[:,'maxChequeLimit']=refreshedData.maxChequeAmountAccepted
    scmView.loc[:,'outstandingBouncedCheques']=refreshedData.currOutsCount
    scmView.loc[:,'outstandingBouncedChequesAmount']=refreshedData.currOutsValue
    scmView.loc[:,'totalBouncedCheques']=refreshedData.everBouncedCount
    scmView.loc[:,'eligibleForCredit']=refreshedData.takeCredit
    scmView.loc[:,'creditProvider']=refreshedData.creditProduct
    scmView.loc[:,'currentCreditLimit']=refreshedData.credit_limit_today
    scmView.loc[:,'totalCreditLimit']=refreshedData.creditOverallLimit
    scmView.loc[:,'creditTransactionLimit']=refreshedData.creditTransactionLimit
    scmView.loc[:,'bouncedPDC']=refreshedData.currCreditBouncedCount
    scmView.loc[:,'outstandingPDC']=refreshedData.currCreditOutsCount
    scmView.loc[:,'outstandingPDCAmount']=refreshedData.currCreditOutsValue
    scmView.loc[:,'cheque_createdTime']=''
    scmView.loc[:,'cheque_lastUpdatedTime']=''
    scmView.loc[:,'cheque_lastUpdatedBy']=''
    scmView.loc[:,'credit_createdTime']=''
    scmView.loc[:,'credit_lastUpdatedTime']=''
    scmView.loc[:,'credit_lastUpdatedBy']=''    
#%% convert yes/no to TRUE/FALSE and product types
    scmView.eligibleForCheque = scmView.eligibleForCheque.str.replace('yes','TRUE') 
    scmView.eligibleForCheque = scmView.eligibleForCheque.str.replace('no','FALSE')
    
    scmView.eligibleForCredit = scmView.eligibleForCredit.str.replace('yes','TRUE') 
    scmView.eligibleForCredit = scmView.eligibleForCredit.str.replace('no','FALSE')
    
    scmView.creditProvider = scmView.creditProvider.astype(str)
    scmView.creditProvider = scmView.creditProvider.str.replace('FundsCorner-CASH','FUNDSCORNER_CASH')
    scmView.creditProvider = scmView.creditProvider.str.replace('FundsCorner-PDC','FUNDSCORNER_PDC')
    scmView.creditProvider = scmView.creditProvider.str.replace('0','NO_CREDIT')
    
#%%    #write SCM view to GS
    tkrFileName="Check_Credit_Reference_SCM_"+todStr
    print("Writing ", tkrFileName, " to Googlesheets...")
    scmGSheet = jthf.writeGsheet(scmView,'A1',tkrFileName,"Sheet1", googlesecretkey_location)
    #permission send
    scmGSheet.share('rahul.kumar@jumbotail.com', role='reader')
   

#%%
    writeStatus = "Successful"
    return writeStatus

#%%
def publishLimitsTest(refreshedData, callExceedLimits, googlesecretkey_location):
    import jthelperfunctions as jthf
    import pandas as pd
    
    todStr=pd.to_datetime('today').strftime("%b_%d_%Y")
    #%%
    #write permisison and send GS for exceed Calls
    #callExceedLimits
    tkrFileName="TEST_ChequeCreditLimitCalls_"+todStr
    print("Writing ", tkrFileName, " to Googlesheets...")
    exceededGSheet = jthf.writeGsheet(callExceedLimits,'A1',tkrFileName,"Sheet1", googlesecretkey_location)
    #permission send
    exceededGSheet.share('credit@jumbotail.com', role='writer', is_group=True)
#%%
    #create view for CD
    #dummy vars to match current structure
    refreshedData['maxChequeAmountAccepted'] = refreshedData.maxChequeAmountToday
    refreshedData['paidPenalty'] = "NoPenaltyData"
    #refreshedData['everBouncedValue'] = "Removed"
    #refreshedData['oldBid'] = "Removed"
    #refreshedData['remainingtoclear'] = "Removed"
    #refreshedData['overallCreditAvailable'] = "Removed"
    #%%
    #select columns for CD View
    cdCols=['bid', 
            'storename', 
            'exceptions', 
            'deliver', 
            'takeCheque', 
            'maxChequeAmountToday', 
            'maxChequeAmountAccepted', 
            'currBouncedCount',
            'currBouncedValue',
            'paidPenalty',
            'totalChequesEver',
            'totalChequesValue',
            'everBouncedCount',
            'everBouncedValue',
            'takeCredit', 
            'creditProduct',
            'credit_limit_today',
            'currCreditBouncedCount',
            'currCreditBouncedValue',
            'currCreditOutsCount', 
            'currCreditOutsValue',
            'creditTransactionLimit', 
            'creditOverallLimit',
            'creditEverBouncedCount',
            'creditEverBouncedValue',
            'creditEverUseCount', 
            'creditEverUseValue']
    
    cdView = refreshedData[cdCols]
    
    #%%
    #cols for the old appsheets app
#    cdColNames = ['bid', 
#                  'storename', 
#                  'note', 
#                  'deliver', 
#                  'takecheck', 
#                  'maxcheckamountaccepted_today', 
#                  'maxcheckamountaccepted', 
#                  'bounced_check_outstanding', 
#                  'bounced_check_outstanding_amount', 
#                  'total_everbounced', 
#                  'cum_bounce_amount', 
#                  'hasclearedbounce', 
#                  'remainingtoclear', 
#                  'take_credit', 
#                  'credit_product', 
#                  'credit_limit_today', 
#                  'pdc_bounced_cheques', 
#                  'pdc_oustanding_count', 
#                  'pdc_oustanding_amount', 
#                  'overall_credit_limit_available',
#                  'credit_transaction_limit', 
#                  'credit_overall_limit', 
#                  'oldbid']
#    
#    cdView.columns = cdColNames
    
    #%%
    #set filename
    #Check_Credit_Reference_Nov_11_2017
    tkrFileName="TEST_Check_Credit_Reference_"+todStr
    print("Writing ", tkrFileName, " to Googlesheets...")
    #write CD view to GS
    cdGSheet = jthf.writeGsheet(cdView,'A1',tkrFileName,"Sheet1", googlesecretkey_location)
    #permission CD sheet and send
    cdGSheet.share('credit@jumbotail.com', role='reader', is_group=True)
    #%%    #create view for SCM, mapping from refreshedData
    scmViewCols = [
            'BusinessId',
            'Business Name',
            'chequeProfileId',
            'creditProfileId',
            'eligibleForCheque',
            'currentChequeLimit',
            'maxChequeLimit',
            'outstandingBouncedCheques',
            'outstandingBouncedChequesAmount',
            'totalBouncedCheques',
            'eligibleForCredit',
            'creditProvider',
            'currentCreditLimit',
            'totalCreditLimit',
            'creditTransactionLimit',
            'bouncedPDC',
            'outstandingPDC',
            'outstandingPDCAmount',
            'cheque_createdTime',
            'cheque_lastUpdatedTime',
            'cheque_lastUpdatedBy',
            'credit_createdTime',
            'credit_lastUpdatedTime',
            'credit_lastUpdatedBy'] 
    
    scmView=pd.DataFrame(columns=scmViewCols, index=refreshedData.index)
    #%%
    scmView.loc[:,'BusinessId']=refreshedData.bid
    scmView.loc[:,'Business Name']=refreshedData.storename
    scmView.loc[:,'chequeProfileId']=''
    scmView.loc[:,'creditProfileId']=''
    scmView.loc[:,'eligibleForCheque']=refreshedData.takeCheque
    scmView.loc[:,'currentChequeLimit']=refreshedData.maxChequeAmountToday
    scmView.loc[:,'maxChequeLimit']=refreshedData.maxChequeAmountAccepted
    scmView.loc[:,'outstandingBouncedCheques']=refreshedData.currOutsCount
    scmView.loc[:,'outstandingBouncedChequesAmount']=refreshedData.currOutsValue
    scmView.loc[:,'totalBouncedCheques']=refreshedData.everBouncedCount
    scmView.loc[:,'eligibleForCredit']=refreshedData.takeCredit
    scmView.loc[:,'creditProvider']=refreshedData.creditProduct
    scmView.loc[:,'currentCreditLimit']=refreshedData.credit_limit_today
    scmView.loc[:,'totalCreditLimit']=refreshedData.creditOverallLimit
    scmView.loc[:,'creditTransactionLimit']=refreshedData.creditTransactionLimit
    scmView.loc[:,'bouncedPDC']=refreshedData.currCreditBouncedCount
    scmView.loc[:,'outstandingPDC']=refreshedData.currCreditOutsCount
    scmView.loc[:,'outstandingPDCAmount']=refreshedData.currCreditOutsValue
    scmView.loc[:,'cheque_createdTime']=''
    scmView.loc[:,'cheque_lastUpdatedTime']=''
    scmView.loc[:,'cheque_lastUpdatedBy']=''
    scmView.loc[:,'credit_createdTime']=''
    scmView.loc[:,'credit_lastUpdatedTime']=''
    scmView.loc[:,'credit_lastUpdatedBy']=''    
#%% convert yes/no to TRUE/FALSE and product types
    scmView.eligibleForCheque = scmView.eligibleForCheque.str.replace('yes','TRUE') 
    scmView.eligibleForCheque = scmView.eligibleForCheque.str.replace('no','FALSE')
    
    scmView.eligibleForCredit = scmView.eligibleForCredit.str.replace('yes','TRUE') 
    scmView.eligibleForCredit = scmView.eligibleForCredit.str.replace('no','FALSE')
    
    scmView.creditProvider = scmView.creditProvider.astype(str)
    scmView.creditProvider = scmView.creditProvider.str.replace('FundsCorner-CASH','FUNDSCORNER_CASH')
    scmView.creditProvider = scmView.creditProvider.str.replace('FundsCorner-PDC','FUNDSCORNER_PDC')
    scmView.creditProvider = scmView.creditProvider.str.replace('0','NO_CREDIT')
    
#%%    #write SCM view to GS
    tkrFileName="TEST_Check_Credit_Reference_SCM_"+todStr
    print("Writing ", tkrFileName, " to Googlesheets...")
    scmGSheet = jthf.writeGsheet(scmView,'A1',tkrFileName,"Sheet1", googlesecretkey_location)
    #permission send
    scmGSheet.share('credit@jumbotail.com', role='reader', is_group=True)
   

#%%
    writeStatus = "Successful"
    return writeStatus

