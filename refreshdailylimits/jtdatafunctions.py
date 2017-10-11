# -*- coding: utf-8 -*-


def get_customerdata ():
    import jthelperfunctions as hf
    query="WITH customer_details AS ( SELECT DISTINCT b.businessid AS custbid, b.displayname AS storename, CASE WHEN bl.businessid IS NOT NULL THEN 'BL' WHEN wl.businessid IS NOT NULL THEN 'WL' ELSE 'NONE' END AS exceptions, DATE (b.onboarddatenew) AS onboard_date, b.businesstype, b.businesssubtype, ad.addresslocality FROM business_snapshot b LEFT JOIN basic_cheque_limit_exception ex ON ex.businessid = b.businessid LEFT JOIN cheque_whitelist wl ON b.businessid = wl.businessid LEFT JOIN cheque_blacklist bl ON b.businessid = bl.businessid LEFT JOIN address_snapshot_ ad ON ad.addresstype = 'SHIPPING' AND ad.addressentityid = b.businessid ), order_details AS ( SELECT c.businessid, COUNT(DISTINCT (CASE WHEN quantity - cancelled_units - returned_units - return_requested_quantity > 0 THEN DATE (bo.src_created_time) END)) AS order_dates, SUM((quantity - cancelled_units - returned_units - return_requested_quantity)*order_item_amount / quantity) AS order_value, COUNT(CASE WHEN ei.reattempt_quantity > 0 THEN ei.orderitem_id END) AS reattempt_orderitems, COUNT(CASE WHEN ei.return_quantity > 0 AND rc.isfillrate IS FALSE THEN ei.orderitem_id END) AS return_orderitems, COUNT(DISTINCT (ei.orderitem_id)) as attempted_orderitems, SUM(CASE WHEN distributed IS TRUE THEN (quantity - cancelled_units - returned_units - return_requested_quantity)*order_item_amount / quantity END) AS fmcg_sales FROM bolt_order_item_snapshot bo JOIN customer_snapshot_ c ON c.customerid = bo.buyer_id JOIN business_snapshot b ON b.businessid = c.businessid JOIN listing_snapshot li ON li.listing_id = bo.listing_id JOIN sellerproduct_snapshot sp ON sp.sp_id = li.sp_id JOIN product_snapshot_ p ON p.jpin = sp.jpin JOIN category ca ON ca.pvid = p.pvid LEFT JOIN eod_item_level ei ON bo.order_item_id = ei.orderitem_id LEFT JOIN eil_reason_code rc ON rc.reason_code = ei.reason_code GROUP BY 1 ) select c.*,o.order_dates,o.order_value,case when attempted_orderitems=0 then 0 else reattempt_orderitems*1.00/attempted_orderitems end as reattempt_pct, case when attempted_orderitems=0 then 0 else return_orderitems*1.00/attempted_orderitems end as return_pct, case when o.order_value=0 then 0 else fmcg_sales*1.00/o.order_value end as fmcg_share from customer_details c left join order_details o on o.businessid=c.custbid"
    credentials=hf.getUserCredentials()
    customerData=hf.getDataFromRedshift(query,credentials.get('rs_user_id'),credentials.get('rs_password'))
    return customerData
##2. get_jtchequedata()
#current 
#bid, current outstanding number of cheques, current outstanding value of cheques, 
#history
#everbounced_count, everbounced_value,
#total cheques paid count, total cheques paid value, repaychequebounce_attempts


##3. get_creditdata()
#current stuff
#bid, product, overall limit, transaction limit, creditonboarddate 
#outstanding credit count, outstanding credit value
#history
#everbounced_count, everbounced_value,
#total credit paid count, total credit paid value, repaycreditbounce_attempts

