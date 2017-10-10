# -*- coding: utf-8 -*-

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




