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

host = 'localhost'
name = 'postgres'
passwd = 'matkhau2908'
db = 'postgres'


def QueryPostgre(sql, host, passwd, name, db):
    logging.info("Start db connection")
    db_connection = create_engine(f'postgresql://{name}:{passwd}@{host}:5432/{db}')
    with db_connection.connect() as con, con.begin():
        df = pd.read_sql(sql, con)
        logging.info("Finish querying the data")
    return df

script = '''SELECT * FROM etl.sales_invoice_etl'''

sql = QueryPostgre(sql= script , host=host, passwd=passwd, name=name, db=db)

# Connect to Google Sheets
# scopes = [
#     "https://www.googleapis.com/auth/spreadsheets"
# ]
json_path = r'C:\Users\USER\Documents\GitHub\Apache_Airflow\Bot API\credentials.json'
# creds = Credentials.from_service_account_file(json_path, scopes=scopes) # Replace the api-json-path
# client = gspread.authorize(creds)


spreadsheet_key = "17-ASdHfkd82GUAzd3M10lSLqBBNQ0gJ_8skPvKgordw" # Input the spreadsheets_id
# sheet = client.open_by_key(spreadsheets_id)
sheet_name = 'Hieu' # Input the spreadsheets_name
# worksheet = sheet.worksheet(sheet_name)


class Gsheet_working:
    def __init__(self, spreadsheet_key, sheet_name, json_file):
        self.spreadsheet_key = spreadsheet_key
        self.sheet_name = sheet_name
        gc = pygsheets.authorize(service_file=json_file)
        sh = gc.open_by_key(spreadsheet_key)
        self.wks = sh.worksheet_by_title(sheet_name)


    def Update_dataframe(self, dataframe, row_num, col_num, clear_sheet=True, copy_head=True,empty_value=''):
        wks = self.wks
        sheet_name = self.sheet_name
        if clear_sheet:
            print('clear all data in %s'%sheet_name)
            wks.clear()
        else:
            pass


        total_rows = int(len(dataframe))
        print('Start upload data to %s' % sheet_name)
        if len(dataframe) >= 1:
            dataframe.fillna('', inplace=True)
            wks.set_dataframe(dataframe, (row_num, col_num), copy_head=copy_head,nan=empty_value)
            print('Upload successful {} lines'.format(len(dataframe)))
        else:
            print('%s not contain value. Check again' % sheet_name)

si_to_gg = Gsheet_working(spreadsheet_key, sheet_name, json_file = json_path)
si_to_gg.Update_dataframe(sql, row_num = 1, col_num = 1, clear_sheet=True, copy_head = True )
