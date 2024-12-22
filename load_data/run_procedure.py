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

def call_stored_procedure(host, name, db, schema, procedure_name):
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(host=host, database=db, user=name, password=passwd)
    
    try:
        # Create a cursor object to execute SQL queries
        with conn.cursor() as cursor:
            # Build the SQL query to call the stored procedure
            query = sql.SQL("CALL {}.{}()").format(sql.Identifier(schema), sql.Identifier(procedure_name))
            
            # Execute the stored procedure
            cursor.execute(query)
            
            # Commit the transaction
            conn.commit()
            
            print(f"Stored procedure '{schema}.{procedure_name} executed successfully'")
    except Exception as e:
        print(f"Error calling stored procedure '{schema}.{procedure_name}': {e}")
    finally:
        # Close the database connection
        conn.close()
        
schema = 'etl'
procedure_name = 'prod_ingest_data_sales_invoice_etl'
call_stored_procedure(host, name, db, schema, procedure_name)
