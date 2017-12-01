
# coding: utf-8

# In[93]:

#
#from IPython.display import HTML
#
#HTML('''
#<script>
#code_show=true; 
#function code_toggle() {
# if (code_show){
# $('div.input').hide();
# } else {
# $('div.input').show();
# }
# code_show = !code_show
#} 
#$( document ).ready(code_toggle);
#</script>
#<form action="javascript:code_toggle()"><input type="submit" value="Click here to toggle on/off the raw code."></form>
#''')

# In[2]:


import pandas as pd
print("Pandas Version", pd.__version__)
print(pd.Timestamp.now())
#from matplotlib import pyplot as plt
#get_ipython().magic('matplotlib inline')
#import seaborn as sns

pd.set_option('max_columns', 50)

googlesecretkey_location="/home/sohamgsen/Dropbox/Jumbotail/projects/.config/pygsheets/google_secret.json"

def getGsheet(spreadsheetname, worksheetname, secretkeylocation):
    import pygsheets
    
    gc = pygsheets.authorize(outh_file=secretkeylocation)
    
    # Open spreadsheet and then workseet
    sh = gc.open(spreadsheetname)
    wks = sh.worksheet_by_title(worksheetname)
    dataframe = wks.get_as_df()
    return dataframe


import json
credentials = json.load(open("/home/sohamgsen/Dropbox/Jumbotail/soham_redshift.json"))

def getDataFromRedshift(query,rs_user,rs_password):
    print("Trying to fetch data from Redshift")
    from sqlalchemy import create_engine
    import pandas as pd
    dbname = 'datawarehousedb'
    host='datawarehouse-cluster.cgatqp75ezrh.ap-southeast-1.redshift.amazonaws.com'
    port= '5439'
    engine_str = 'postgresql://'+rs_user+':'+rs_password+'@'+host+':'+port+'/'+dbname
    engine = create_engine(engine_str)
    print("Connected to Redshift. Fetching data ...")
    data_frame = pd.read_sql_query(query, engine)
    print("Received Data")
    return data_frame

def writeGsheet(dataframe, cellstart, spreadsheetname, worksheetname, secretkeylocation):
    import pygsheets
    r = dataframe.shape[0] + 2
    c = dataframe.shape[1] + 1
    print("Authenticating...")
    gc = pygsheets.authorize(outh_file=secretkeylocation,no_cache=True)
    
    try:
        print("Spreadsheet Found...")
        sh = gc.open(spreadsheetname)
    except:
        print("Spreadsheet Not Found. Creating...")
        sh = gc.create(spreadsheetname)
    
    try:    
        print("Worksheet Found. Writing...")
        wks = sh.worksheet_by_title(worksheetname)
    except:
        print("Worksheet Not Found. Creating...")
        wks = sh.add_worksheet(worksheetname, rows = r, cols = c)
    
    wks.set_dataframe(dataframe, start = cellstart, fit = True)
    print('Write Completed')
    return sh



# In[95]:


todStr=pd.to_datetime('today').strftime("%b_%d_%Y")
todStr2=pd.to_datetime('today').strftime("%b_%-d_%Y")
newFileName="TEST_Check_Credit_Reference_"+todStr
currFileName="Check_Credit_Reference_"+todStr2
allFileName="TEST_Check_Credit_Reference_ALLDATA_"+todStr
testResultsFile = 'NewCreditChequeLogicTestResults'


# In[96]:


new = getGsheet(newFileName,"Sheet1",googlesecretkey_location)


# In[97]:


current = getGsheet(currFileName,currFileName,googlesecretkey_location)


# In[98]:


#merges the new and old output into the same dataframe for easier comparison
combined = current.merge(new, how='left', on='bid')


# In[99]:


#gets all the columns used for the new limits generator
allData = getGsheet(allFileName,"Sheet1",googlesecretkey_location)


# In[100]:


def fixDeliver(x):
    if x == 'no - call CD':
        return 'no'
    else:
        return 'yes'
    
combined['deliver_x'] = combined['deliver_x'].apply(lambda x: fixDeliver(x))

def fixtake_credit_x(x):
    if x == 0:
        return 'no'
    else:
        return 'yes'

combined['take_credit_x'] = combined['take_credit_x'].apply(lambda x: fixtake_credit_x(x))


# A key thing to think through is whether the inputs are basically the same. Then how those inputs are combined to produce outputs: deliver, takecheck, checklimit, takecredit, creditlimit

# In[103]:

colstocompare = new.columns.tolist()
colstocompare = colstocompare[1:len(colstocompare)]

def comparexy(x):
    var_x=x+'_x'
    var_y=x+'_y'
    test = combined[combined[var_x]!=combined[var_y]]
    #print(test[[var_x, var_y]])
    return test.shape[0]

colsdif = {}

for eachcol in colstocompare:
    colsdif[eachcol] = comparexy(eachcol)

colsdif = pd.DataFrame.from_dict(colsdif, orient='index')

print(colsdif)


# In[106]:


def showDiff(var):
    var1 = var +'_x'
    var2 = var +'_y'
    result = combined[combined[var1]!= combined[var2]][['bid', var1, var2]]
    return result


# In[108]:


#loads cheque data
chequeData = getGsheet("Cheque Payment & Exposure Tracker","Master Data",googlesecretkey_location)
 #convert all to string
chequeData['Amount'] = chequeData['Amount'].apply(lambda x: str(x))
    #get rid of commas
chequeData['Amount'] = chequeData['Amount'].str.replace(',','')
    #get rid of the rows that have empty amount cols
chequeData = chequeData[chequeData['Amount'] != '']
    #convert all of these to float type
chequeData['Amount'] = chequeData['Amount'].astype(float)


# In[119]:


#loads credit data
creditData = getGsheet("[INTERNAL] FundsCorner <> Jumbotail | Collection Tracker","Final Sheet",googlesecretkey_location)


# In[4]:


def creditDiff(bid):
    from IPython.display import display
    print('Current...')
    display(current[current.bid==bid])
    print('New...')
    display(new[new.bid==bid])

#creditDiff('BZID-1304472129')


# Feel pretty good about those who we are blocking from cheques. 
# Now just to confirm the take credit differences. 

# ### Whitelist
# Note on Whitelist question for credit - basically being on the whitelist doesn't give you credit unless you already have it. and being on WL still doesn't give you access to multiple products. Given the power of the Whitelist we should revisit it; do we really want these folks there?

# In[137]:


new[new.note == 'WL']


# ## 1. Take Cheque Differences
# Want to take a more robust approach. I know which variables are factors in take cheque:
# - takecheck
# - exceptions(note)
# - activeCredit (credit_product)
# - bounced_check_outstanding
# - pdc_bounced_cheques
# - totalEverBounced+ 
# - reattempt_pct+
# 
# Let's collect some pieces that are only in AllData
# Let's build a dataset from combinedPlus that compares each of these factors along with the high level take cheque.

# In[145]:


allData['totalEverBounced'] = allData.creditEverBouncedCount + allData.everBouncedCount
temp = allData[['bid','creditActive','totalEverBounced','reattempt_pct']]
temp[temp.totalEverBounced > 0].shape


# In[146]:


combinedPlus = combined.merge(temp, on='bid', how='left')


# In[147]:


combinedPlus.shape

#%%
def chequeDiffExplain_v0(rw):
    rw['takeChequeStatus'] = ''
    rw['why_Reason'] = ''
    
    if rw.takecheck_x == rw.takecheck_y:
        rw['takeChequeStatus'] = 'nochange'
        rw['why_Reason'] = 'nochange'
        return rw
    elif rw.takecheck_x == 'yes':
        rw['takeChequeStatus'] = 'yes->no'
    else:
        rw['takeChequeStatus'] = 'no->yes'
        
    if rw.note_x!=rw.note_y:
        rw['why_reason'] = 'changeInBL/WL'
    elif rw.bounced_check_outstanding_x!=rw.bounced_check_outstanding_y:
        rw['why_reason'] = 'BouncedChequeOutstanding'
    elif rw.pdc_bounced_cheques_x!=rw.pdc_bounced_cheques_y:
        rw['why_reason'] = 'BouncedCreditOutstanding'
    elif rw.totalEverBounced > 5:
        rw['why_reason'] = '5+BouncesCreditCheque'
    elif (rw.totalEverBounced >=3):
        rw['why_reason'] = 'Too Many Cheque Bounces'
    else:
        rw['why_reason'] = 'Unclear'
    
    return rw


# In[149]:


def chequeDiffExplain_v1(rw):
    rw['takeChequeStatus'] = ''
    rw['why_Reason'] = ''
    
    if rw.takecheck_x == rw.takecheck_y:
        rw['takeChequeStatus'] = 'nochange'
        rw['why_Reason'] = 'nochange'
        return rw
    elif rw.takecheck_x == 'yes':
        rw['takeChequeStatus'] = 'yes->no'
    else:
        rw['takeChequeStatus'] = 'no->yes'
        
    if rw.note_x!=rw.note_y:
        rw['why_reason'] = 'changeInBL/WL'
    elif rw.creditActive == 'yes':
        rw['why_reason'] = 'HasCreditNoCheque'
    elif rw.credit_product_x != rw.credit_product_y:
        rw['why_reason'] = 'creditProductChange'
    elif rw.bounced_check_outstanding_x!=rw.bounced_check_outstanding_y:
        rw['why_reason'] = 'BouncedChequeOutstanding'
    elif rw.pdc_bounced_cheques_x!=rw.pdc_bounced_cheques_y:
        rw['why_reason'] = 'BouncedCreditOutstanding'
    elif rw.totalEverBounced > 5:
        rw['why_reason'] = '5+BouncesCreditCheque'
    elif rw.reattempt_pct > .25:
        rw['why_reason'] = '25pctReattempt'
    elif (rw.totalEverBounced <=5 & rw.totalEverBounced >=3):
        rw['why_reason'] = 'RelaxedAllowedBounces'
    else:
        rw['why_reason'] = 'Unclear'
    
    return rw


# In[150]:

#needs to be changed depending on if jtlf.takeCheque_v0 is being used or 
    #jtlf.takeCheque_v1
chDiffResults = combinedPlus.apply(chequeDiffExplain_v0, axis=1)


# In[151]:


chDiffCols = ['bid','takeChequeStatus','why_reason', 'note_x' ,'note_y', 'credit_product_x', 
              'credit_product_y','bounced_check_outstanding_x','bounced_check_outstanding_y', 'creditActive',
              'pdc_bounced_cheques_x', 'pdc_bounced_cheques_y',
              'reattempt_pct', 'totalEverBounced']
chDiffResults = chDiffResults[chDiffCols] 
chDiffResults = chDiffResults[(chDiffResults.takeChequeStatus != 'nochange')]


# In[152]:


writeGsheet(chDiffResults, 'A1', testResultsFile, 'chequeStatusChanged', googlesecretkey_location)


# ### 1.b. Would be good to know how many of these cheque users actually use cheques and why

# What about those who will lose cheque cause of credit?

# In[153]:


willLoseCheque = chDiffResults[(chDiffResults.takeChequeStatus!='nochange') & (chDiffResults.why_reason == 'HasCreditNoCheque')]


# In[154]:


willLoseCheque.shape


# The question becomes, how many times in the last recent days - say 30 days - have these customers who will be turned off from cheque been using cheque? and for how much?

# In[155]:


willLoseChequeList = willLoseCheque.bid.values.tolist()


# In[156]:


willLoseChequeTrans = chequeData[chequeData.BID.isin(willLoseChequeList) & (chequeData.Date > "2017-10-28")]


# In[157]:


aggs = {'Amount':['count', 'sum']}
temp = willLoseChequeTrans.groupby('BID').agg(aggs)
dualCreditChequeUsers = temp.index.values.tolist()
willLoseChequeUse = temp[temp[('Amount','count')]>=1]
willLoseChequeUse


# In[158]:


willLoseChequeUse.shape


# ### We're talking about 30 cusotmers out of the 179 who actually have used cheque in the last 30 days 

# Have these customers also used credit in last thiry days?

# In[159]:


willLoseChequeCreditUse = creditData[creditData.cust_id.isin(dualCreditChequeUsers) & (creditData.cheque_collected_date > "2017-10-28")]
willLoseChequeCreditUse = willLoseChequeCreditUse.groupby('cust_id').agg({'net_collection_amount':['count','sum']})
willLoseCheque = willLoseChequeUse.merge(willLoseChequeCreditUse, how='left', left_index=True, right_index=True)
willLoseCheque.columns = ["_".join(x) for x in willLoseCheque.columns.ravel()]
willLoseCheque


# In[160]:


willLoseChequeDetails = allData[allData.bid.isin(dualCreditChequeUsers)]
willLoseChequeDetails = willLoseChequeDetails.merge(willLoseCheque, how='left', left_on='bid', right_index=True)
writeGsheet(willLoseChequeDetails, 'A1', testResultsFile, 'ChequeUsersWhoLose', googlesecretkey_location)


# #### There is a real question as to why these folks are using cheques? Can we call? Can we also see if credit limits are a factor?

# ## 2. Take Credit Differences
# Let's again look at which factors are considered for credit: 
# - take_credit
# - exceptions(note)
# - creditActive
# - bounced_check_outstanding
# - pdc_bounced_cheques
# - totalEverBounced
# - reattempt_pct

# In[161]:


def creditDiffExplain(rw):
    rw['takeCreditStatus'] = ''
    rw['why_Reason'] = ''
    
    if rw.take_credit_x == rw.take_credit_y:
        rw['takeCreditStatus'] = 'nochange'
        rw['why_Reason'] = 'nochange'
        return rw
    elif rw.take_credit_x == 'yes':
        rw['takeCreditStatus'] = 'yes->no'
    else:
        rw['takeCreditStatus'] = 'no->yes'
        
    if rw.note_x!=rw.note_y:
        rw['why_reason'] = 'changeInBL/WL'
    elif rw.creditActive != 'yes':
        rw['why_reason'] = 'Not Activated for Credit'
    elif rw.credit_product_x != rw.credit_product_y:
        rw['why_reason'] = 'creditProductChange'
    elif rw.bounced_check_outstanding_x!=rw.bounced_check_outstanding_y:
        rw['why_reason'] = 'BouncedChequeOutstanding'
    elif rw.pdc_bounced_cheques_x!=rw.pdc_bounced_cheques_y:
        rw['why_reason'] = 'BouncedCreditOutstanding'
    elif rw.totalEverBounced > 5:
        rw['why_reason'] = '5+BouncesCreditCheque'
    elif rw.reattempt_pct > .25:
        rw['why_reason'] = '25pctReattempt'
    else:
        rw['why_reason'] = 'Unclear'
    
    return rw


# In[168]:


crDiffResults = combinedPlus.apply(creditDiffExplain, axis=1)

crDiffCols = ['bid','takeCreditStatus', 'take_credit_x', 'take_credit_y', 
              'why_reason', 'note_x' ,'note_y', 'creditActive', 
              'credit_product_x', 'credit_product_y','bounced_check_outstanding_x','bounced_check_outstanding_y', 
              'pdc_bounced_cheques_x', 'pdc_bounced_cheques_y', 'totalEverBounced', 'reattempt_pct', ]
crDiffResults = crDiffResults[crDiffCols] 


# In[169]:


crDiffResults[(crDiffResults.takeCreditStatus != 'nochange')].why_reason.value_counts()


# In[170]:


crDiffResults = crDiffResults[(crDiffResults.takeCreditStatus != 'nochange')&(crDiffResults.why_reason != 'Not Activated for Credit')]
crDiffResults


# In[171]:


writeGsheet(crDiffResults, 'A1', testResultsFile, 'creditStatusChanged', googlesecretkey_location)

#%% output all data for testing
