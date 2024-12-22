from sqlalchemy import create_engine
from datetime import datetime,timedelta
import pandas as pd
import os
import re
import sys
import glob
import time
import psycopg2
import numpy as np
from psycopg2 import sql
abspath = os.path.dirname(__file__) # get directory path of current file
main_cwd = re.sub('hieult_pycode.*','hieult_pycode',abspath) # modify the path  
os.chdir(main_cwd) # change current working directory to the modified path
sys.path.append(main_cwd) # add the modified path to system path

ct = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # get the time run 

print("current time: ",ct)

# set-up connect credentials
host = 'localhost'
name = 'postgres'
passwd = 'matkhau2908'
db = 'postgres'

# Specify the path to your Excel file
path = r'C:\Users\USER\Desktop\data\SI'

# List all files in the folder
files_in_folder = os.listdir(path)

excel_files = [file for file in files_in_folder if file.endswith('.xlsx')]
# Check if there are any Excel files in the folder
if excel_files:
    # Get the most recent Excel file
    latest_excel_file = max(excel_files, key=lambda x: os.path.getmtime(os.path.join(path, x)))

    # Construct the full path to the most recent Excel file
    excel_file_path = os.path.join(path, latest_excel_file)

    # Read the Excel file into a DataFrame
    df = pd.read_excel(excel_file_path)
    
df.columns = df.columns.str.lower().str.replace(' ', '_')
current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
df['data_updated_time'] = current_timestamp
df['data_updated_time'] = pd.to_datetime(df['data_updated_time'])

engine = create_engine(f'postgresql+psycopg2://{name}:{passwd}@{host}/{db}')

with engine.connect() as con,con.begin():

    df.to_sql(name = 'sales_invoice_raw',
                con = engine,
                if_exists = 'append',
                index = False,
                schema= 'raw')
    print('Ingest to raw.sales_invoice_raw')
    
time.sleep(3)

