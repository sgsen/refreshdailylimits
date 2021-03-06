# -*- coding: utf-8 -*-
# sgsen@jumbotail.com, puja@jumbotail.com
def getGsheet(spreadsheetname, worksheetname, secretkeylocation):
    import pygsheets
    
    gc = pygsheets.authorize(outh_file=secretkeylocation,no_cache=True, retries=3)
    
    # Open spreadsheet and then workseet
    sh = gc.open(spreadsheetname)
    wks = sh.worksheet_by_title(worksheetname)
    dataframe = wks.get_as_df()
    return dataframe
#%%
def writeGsheet(dataframe, cellstart, spreadsheetname, worksheetname, secretkeylocation):
    import pygsheets
    r = dataframe.shape[0] + 2
    c = dataframe.shape[1] + 1
    print("Authenticating...")
    gc = pygsheets.authorize(outh_file=secretkeylocation,no_cache=True, retries=3)
    
    spsheet_status = ''
    wsheet_status = ''
    write_status =''
    try:
        sh = gc.open(spreadsheetname)
        spsheet_status = "Spreadsheet Found..."
    except pygsheets.RequestError:
        spsheet_status = "Spreadsheet Request Error. Skip writing "+spreadsheetname+"..."
    except pygsheets.SpreadsheetNotFound:
        sh = gc.create(spreadsheetname)
        spsheet_status = "Spreadsheet Not Found. Creating..."
    except:
        spsheet_status = "Some error besides request and file not found. Skip writing "+spreadsheetname+"..."
    print(spsheet_status)
    
    try:    
        wks = sh.worksheet_by_title(worksheetname)
        wsheet_status = "Worksheet Found. Writing..."
        wks.set_dataframe(dataframe, start = cellstart, fit = True)
        write_status='Write Completed'
    except pygsheets.WorksheetNotFound:
        wks = sh.add_worksheet(worksheetname, rows = r, cols = c)
        wsheet_status = "Worksheet Not Found. Creating and Writing..."
        wks.set_dataframe(dataframe, start = cellstart, fit = True)
        write_status='Write Completed'
    except:
        wsheet_status = "Some error besides sheet not found. Skip writing "+worksheetname+"..."
        write_status='Write Failed'
    
    print(wsheet_status, write_status)
    return sh
#%%
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
    
#%%
def getUserCredentials():
    import json
    try:
        credentials = json.load(open("__pycache__/credentials.txt"))
    except:
        rs_user_id= input('Give me your redshift userid: ')
        rs_password=input('Give me your redshift password: ')
        googlesecretkey_location=input('Path for google_secret.json file: ')
        credentials={'rs_user_id':rs_user_id,'rs_password':rs_password,'googlesecretkey_location':googlesecretkey_location }
        json.dump(credentials, open("__pycache__/credentials.txt",'w'))
    return credentials

#%%
def ensureNum(a):
    import re
    
    x = str(a)
    x = re.sub('\D','',x)
    x = re.sub('^','0',x)
    return int(x)

