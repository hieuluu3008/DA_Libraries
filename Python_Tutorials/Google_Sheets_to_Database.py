###TOPIC: Read data from Google Sheet and ingest into PostgreSQL database

# Import necessary librabries
import pandas as pd
import time
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, types, Boolean
import gspread
from google.oauth2.service_account import Credentials

# Get access to Google Sheets by Google API
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
all_values = worksheet.get_all_values()
df = pd.DataFrame(all_values[1:], columns=all_values[0])
df_report = df.copy()

# Set-up database connection credentials
host = 'host_name'
name = 'name'
passwd = 'password'
db = 'database_name'

# Create delete data from table function 
def delete_data_from_table(table_name, condition):
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            host=host,
            database=db,
            user=name,
            password=passwd
        )
        # Create a cursor
        cursor = connection.cursor()
        # Build the DELETE query
        delete_query = f"DELETE FROM {table_name} WHERE {condition};"
        # Execute the DELETE query
        cursor.execute(delete_query)
        # Commit the transaction
        connection.commit()
        print("Data deleted successfully.")
    except (Exception, psycopg2.Error) as error:
        print("Error while deleting data:", error)
    finally:
        # Close the cursor and connection
        if connection:
            cursor.close()
            connection.close()
            print("Database connection closed.")


delete_data_from_table(table_name='etl.sales_invoice_etl',condition='1=1')


df_report.columns =  df_report.columns.str.lower().str.replace(' \ ','_').str.replace(' / ','_').str.replace(' ','_').str.replace('\n','_')


# Ingest data from df to table in database
engine = create_engine(f'postgresql+psycopg2://{name}:{passwd}@{host}:5432/{db}')

try:
    with engine.connect() as con, con.begin():
        df_report.to_sql(
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
    
    
# How to get Google Sheets API: https://www.youtube.com/watch?v=zCEJurLGFRk

