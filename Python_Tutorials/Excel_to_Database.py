###TOPIC: Get data from Database to Google Sheets

# Import necessary librabries
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd
import os
import re
import sys
import glob
import time
import psycopg2


# Set-up database connection credentials
host = 'host_name'
name = 'name'
passwd = 'password'
db = 'database_name'


path = 'path_to_file' # Specify the path to your Excel file


# List all files in the folder
files_in_folder = os.listdir(path)


# Filter the Excel files (assuming the file has a .xlsx extension)
excel_files = [file for file in files_in_folder if file.endswith('.xlsx')]
if len(excel_files) == 1:
    # Get the Excel file path
    excel_file_path = os.path.join(path, excel_files[0])


    # Read the Excel file
    df = pd.read_excel(excel_file_path,skiprows=3)

# Ingest data to database
engine = create_engine(f'postgresql+psycopg2://{name}:{passwd}@{host}:5432/{db}')

try:
    with engine.connect() as con, con.begin():
        df.to_sql(
            name='table_name',
            con=con,
            if_exists='append',
            index=False,
            schema='schema_name'
        )
        print('Ingest to schema_name.table_name successful')
except Exception as e:
    print(f"Error: {e}")
finally:
    time.sleep(3)