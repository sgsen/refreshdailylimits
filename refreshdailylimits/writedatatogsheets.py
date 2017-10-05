# -*- coding: utf-8 -*-

#Puja can you try to make a live version of this?
#https://www.dataiku.com/learn/guide/code/python/export-a-dataset-to-google-spreadsheets.html

#Oct 5, 2017
## using gpread-pandas package

#%%
from __future__ import print_function
import pandas as pd

#%%
#this doesn NOT work
from gspread_pandas import Spread

# 'Example Spreadsheet' needs to already exist and your user must have access to it
spread = Spread('work', 'Example Spreadsheet')
# This will ask to authenticate if you haven't done so before for 'example_user'

# Display available worksheets
spread.sheets


file_name = "http://www.ats.ucla.edu/stat/data/binary.csv"
df = pd.read_csv(file_name)

# Save DataFrame to worksheet 'New Test Sheet', create it first if it doesn't exist

spread.df_to_sheet(df, index=False, sheet='New Test Sheet', start='A2', replace=True)
spread.update_cells((1,1), (1,2), ['Created by:', spread.email])

print(spread)
# <gspread_pandas.client.Spread - User: '<example_user>@gmail.com', Spread: 'Example Spreadsheet', Sheet: 'New Test Sheet'>

#%%
#this works 
from gsheets import Sheets

sheets = Sheets.from_files('~/.config/gspread_pandas/google_secret.json', '~/.config/gspread_pandas/storage.json')
sheets  #doctest: +ELLIPSIS

url = 'https://docs.google.com/spreadsheets/d/1s-FFmQQdOJ1DdJsJneTWanF096REzaXRxD7dpKAZr_s/edit#gid=0'
s = sheets.get(url)
s
data = s.find('Sheet1').to_frame(index_col='businessid')

data.head()


#%%
# This also works
import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds']
s_file = "/home/sohamgsen/.config/gspread/google_secret.json"


credentials = ServiceAccountCredentials.from_json_keyfile_name(s_file, scope)

gc = gspread.authorize(credentials)

wks = gc.open("customer_credit_details")


sheet = wks.sheet1
dataframe = pd.DataFrame(sheet.get_all_records())

dataframe.head()

#%%

## GOLD!! ##

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

