# -*- coding: utf-8 -*-

#%%
def get_customerdata():
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
    credentials=jthf.getUserCredentials()
    customerData=jthf.getDataFromRedshift(query,credentials.get('rs_user_id'),credentials.get('rs_password'))
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
def get_jtchequedata():
    import jthelperfunctions as jthf
    import numpy as np
    import pandas as pd
    import re           
    #%%
    credentials=jthf.getUserCredentials()
    
    #all bids
    custdata = get_customerdata()
    custList = custdata.iloc[:,0:1]
    custList.rename(index=str, columns={'custbid':'bid'}, inplace=True)
    
    #get tranasactions
    chequeTransData = jthf.getGsheet("Cheque Payment & Exposure Tracker", "Master Data", 
                                     credentials.get('googlesecretkey_location'))
    del credentials
    
    #%% choose the columns that are required for analysis
    
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
    #%% deal with the mixed data types in amount column
    #convert all to string
    chequeTransData['amount'] = chequeTransData['amount'].apply(lambda x: str(x))
    #get rid of commas
    chequeTransData['amount'] = chequeTransData['amount'].str.replace(',','')
    #get rid of the rows that have empty amount cols
    chequeTransData = chequeTransData[chequeTransData['amount'] != '']
    #convert all of these to float type
    chequeTransData['amount'] = chequeTransData['amount'].astype(float)

    #%% fix repldays to be an int
    #chequeTransData['repldays']
    
    chequeTransData['repldays'] = chequeTransData['repldays'].apply(lambda x: str(x))
    #chequeTransData['repldays']= chequeTransData['repldays'].str.replace(',','')
    chequeTransData['repldays']= chequeTransData['repldays'].apply(lambda x: re.sub('s+','0',x))
    chequeTransData['repldays'] = chequeTransData['repldays'].replace(np.NaN,'0')
    chequeTransData['repldays'] = chequeTransData['repldays'].replace('','0')
    chequeTransData['repldays']= chequeTransData['repldays'].str.strip('') 
    chequeTransData['repldays']=chequeTransData['repldays'].apply(lambda x: int(x))

    #%%
    chequeData = custList.apply(chequeCalcs_helper, args=(chequeTransData,), axis=1)
    #chequeData.head()
    return chequeData
#%%

def get_creditdata():
    import jthelperfunctions as jthf
    credentials=jthf.getUserCredentials()
    creditTranData = jthf.getGsheet("[INTERNAL] FundsCorner <> Jumbotail | Collection Tracker", "Final Sheet", credentials.get('googlesecretkey_location'))
    #INCOMPLETE
    #need to manipulate the data coming in to have the appropriate cols and history
    #return creditData
