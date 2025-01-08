###TOPIC: Get data from Database to Google Sheets

# Import necessary librabries
import os
import re
import sys
import pandas as pd
import pygsheets
import psycopg2
import datetime
from sqlalchemy import create_engine
import logging
import gspread
from google.oauth2.service_account import Credentials

host = 'host_name'
name = 'name'
passwd = 'password'
db = 'database_name'


def QueryPostgre(sql, host, passwd, name, db='data_warehouse'):
    logging.info("Start db connection")
    db_connection = create_engine(f'postgresql://{name}:{passwd}@{host}:5432/{db}')
    with db_connection.connect() as con, con.begin():
        # df.to_sql('temp_amz_ads_profile_info', con,  schema='y4a_analyst',if_exists='append', index=False)
        df = pd.read_sql(sql, con)
        logging.info("Finish querying the data")
    return df

# Connect to Google Sheets
scopes = [
    "https://www.googleapis.com/auth/spreadsheets"
]
creds = Credentials.from_service_account_file('api_json_path', scopes=scopes) # Replace the api-json-path
client = gspread.authorize(creds)

# Get data from Google Sheet into df
spreadsheets_id = "spreadsheets_id" # Input the spreadsheets_id
sheet = client.open_by_key(spreadsheets_id)
sheet_name = 'name' # Input the spreadsheets_name
worksheet = sheet.worksheet(sheet_name)

# class Gsheet_working:
#     def __init__(spreadsheet_key, sheet_name, json_file):
#         spreadsheet_key = spreadsheet_key
#         sheet_name = sheet_name
#         gc = pygsheets.authorize(service_file=json_file)
#         sh = gc.open_by_key(spreadsheet_key)
#         wks = sh.worksheet_by_title(sheet_name)


#     def Update_dataframe(dataframe, row_num, col_num, clear_sheet=True, copy_head=True,empty_value=''):
#         wks = wks
#         sheet_name = sheet_name
#         if clear_sheet:
#             print('clear all data in %s'%sheet_name)
#             wks.clear()
#         else:
#             pass


#         total_rows = int(len(dataframe))
#         print('Start upload data to %s' % sheet_name)
#         if len(dataframe) >= 1:
#             dataframe.fillna('', inplace=True)
#             wks.set_dataframe(dataframe, (row_num, col_num), copy_head=copy_head,nan=empty_value)
#             print('Upload successful {} lines'.format(len(dataframe)))
#         else:
#             print('%s not contain value. Check again' % sheet_name)


script = '''Input SQL Query'''

sql = QueryPostgre(sql= script , host=host, passwd=passwd, name=name, db=db)

json_path = 'api_json_path' # Replace the api-json-path

spreadsheets_id = "spreadsheets_id" # Input the spreadsheets_id
spreadsheets_name = 'sheet_name' # Input the spreadsheets_name

si_to_gg = Gsheet_working(spreadsheets_id, 'Accural est', json_file = json_path)
si_to_gg.Update_dataframe(sql, row_num = 1, col_num = 1, clear_sheet=True, copy_head = True )

