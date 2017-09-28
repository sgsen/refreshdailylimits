#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 21 10:00:31 2017

@author: sohamgsen
"""

def getcustinfo():
    print("Return datatable with customer bizid, storename, exceptions")
    
    #%%
    ## variables
    create_engine_string = ''
    
    query = '''SELECT DISTINCT b.businessid AS bid,
           displayname AS storename,
    CASE
    WHEN ex.businessid IS NULL THEN (
    CASE
    WHEN activation_date IS NULL THEN 10000
    ELSE 30000
    END 
    )
    ELSE ex.updated_limit
    END AS maxchequeamountaccepted,
    CASE
    WHEN bl.businessid IS NOT NULL THEN 'BL'
    WHEN wl.businessid IS NOT NULL THEN 'WL'
    ELSE 'NONE'
    END AS exceptions
    FROM business_snapshot b
    JOIN customer_milestone_ c ON c.bid = b.businessid
    LEFT JOIN basic_cheque_limit_exception ex ON ex.businessid = b.businessid
    LEFT JOIN cheque_whitelist wl ON b.businessid = wl.businessid
    LEFT JOIN cheque_blacklist bl ON b.businessid = bl.businessid
    '''    
    ## need to connect to a posgres database
        
    #%%
    from sqlalchemy import create_engine
    import pandas as padas
    engine = create_engine(create_engine_string)
    data_frame = padas.read_sql_query(query, engine)
    
    return data_frame
