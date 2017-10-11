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

