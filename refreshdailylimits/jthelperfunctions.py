# -*- coding: utf-8 -*-


import pygsheets

gc = pygsheets.authorize(outh_file='/home/sohamgsen/Dropbox/Jumbotail/projects/.config/pygsheets/google_secret.json')

# Open spreadsheet and then workseet
sh = gc.open('test')
wks = sh.sheet1
data = wks.get_as_df()
data.head()
sh.add_worksheet("test_sheet2")

## gold
sheet2 = sh.worksheet_by_title("test_sheet2")

sheet2.set_dataframe(data, 'A2')

###
oct5 = gc.open('Check_Credit_Reference_Oct_5_2017')
oct5_sht = oct5.worksheet_by_title("Check_Credit_Reference_Oct_5_2017")
data = oct5_sht.get_as_df()
data.head()

#sh.del_worksheet(testoct5)

sh.add_worksheet("oct5", 5000, 30)
testoct5 = sh.worksheet_by_title("oct5")
testoct5.set_dataframe(data, 'A3')


# share the sheet with your friend
sh.share("sohamgsen@gmail.com")

#write to gs

#get file from gs

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
        credentials = json.load(open("credentials.txt"))
    except:
        rs_user_id= input('Give me your redshift userid: ')
        rs_password=input('Give me your redshift password: ')
        credentials={'rs_user_id':rs_user_id,'rs_password':rs_password}
        json.dump(credentials, open("credentials.txt",'w'))
    return credentials




