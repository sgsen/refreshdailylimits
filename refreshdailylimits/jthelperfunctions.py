# -*- coding: utf-8 -*-

def getGsheet(spreadsheetname, worksheetname, secretkeylocation):
    import pygsheets
    
    gc = pygsheets.authorize(outh_file=secretkeylocation)
    
    # Open spreadsheet and then workseet
    sh = gc.open(spreadsheetname)
    wks = sh.worksheet_by_title(worksheetname)
    dataframe = wks.get_as_df()
    return dataframe

def writeGsheet(dataframe, cellstart, spreadsheetname, worksheetname, secretkeylocation):
    import pygsheets
    
    gc = pygsheets.authorize(outh_file=secretkeylocation)
    sh = gc.open(spreadsheetname)
    wks = sh.worksheet_by_title(worksheetname)
    wks.set_dataframe(dataframe, start = cellstart, fit = True)


# get file from AWS
def getDataFromRedshift(query,rs_user_id,rs_password):
    print("Trying to fetch data from Redshift")
    import psycopg2
    con=psycopg2.connect(dbname= 'datawarehousedb', host='datawarehouse-cluster.cgatqp75ezrh.ap-southeast-1.redshift.amazonaws.com', 
    port= '5439', user= rs_user_id, password= rs_password)
    cur = con.cursor()
    print("Connected to Redshift")
    cur.execute(query)
    data_frame=cur.fetchall()
    return data_frame

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




