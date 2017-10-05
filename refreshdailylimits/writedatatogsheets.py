# -*- coding: utf-8 -*-

#Puja can you try to make a live version of this?
#https://www.dataiku.com/learn/guide/code/python/export-a-dataset-to-google-spreadsheets.html

#Oct 5, 2017
## using gpread-pandas package

#%%
from __future__ import print_function
import pandas as pd

#%%
#this doesn NOT
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
# THIS DOES NOT WORK
import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds']
s_file = "~/.config/gspread/google_secret.json"

#fails here
credentials = ServiceAccountCredentials.from_json_keyfile_name(s_file, scope)
#failes here

gc = gspread.authorize(credentials)

wks = gc.open("customer_credit_details").Sheet1