# -*- coding: utf-8 -*-


#%% test get_customerdata()

import jthelperfunctions as jthf
import jtdatafunctions as jtdf

datatest = jtdf.get_customerdata()

credentials=jthf.getUserCredentials()

test = jthf.getGsheet("creditcosts","Sheet1",credentials.get('googlesecretkey_location'))


