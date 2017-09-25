#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 21 09:27:57 2017

@author: sgsen
"""

#
import basiccustinfo
import chequelimit
import creditlimit

#main function update limits
def update_limits():
    print("Update Limits")

    #get a list of all customer bizids, storename, ordercount 
    basiccustinfo.getcustinfo()


    #update daily cheque limit
    limit = chequelimit.genchequelimit()
    print(limit)

    #update daily credit limit
    print(creditlimit.gencreditlimit())
    

    # apply whitelist and blacklist


#create table for upload that has
#bid	storename	note	deliver	takecheck	maxcheckamountaccepted_today	
#maxcheckamountaccepted	bounced_check_outstanding	
#collected_not_cashed	total_everbounced	cum_bounce_amount	
#hasclearedbounce	remainingtoclear	take_credit	credit_product	
#credit_limit_today	pdc_bounced_cheques	pdc_oustanding_count	
#pdc_oustanding_amount	overall_credit_limit_available	
#credit_transaction_limit	credit_overall_limit	oldbid

#write table to googlesheets

update_limits()

## git, git ignore, and push to git hub
