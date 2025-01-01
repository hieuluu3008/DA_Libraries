###TOPIC: Call Stored procedure in database

# Import necessary librabries
import datetime
import os
import re
import sys
import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine

# Set-up database connection credentials
host = 'host_name'
name = 'name'
passwd = 'password'
db = 'database_name'

# Create call Stored procedure function 
def call_stored_procedure(host, name, passwd, db, schema, procedure_name):
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(host=host, user=name, password=passwd, database=db)
   
    try:
        # Create a cursor object to interact with the database
        with conn.cursor() as cursor:
            # Build the SQL query to call the stored procedure without parameters
            query = sql.SQL("CALL {}.{}()").format(sql.Identifier(schema), sql.Identifier(procedure_name))
           
            # Execute the query
            cursor.execute(query)
           
            # Commit the transaction
            conn.commit()
           
            print(f"Stored procedure '{schema}.{procedure_name}' executed successfully.")
   
    except Exception as e:
        print(f"Error calling stored procedure '{schema}.{procedure_name}': {e}")
   
    finally:
        # Close the database connection
        conn.close()

# Run procedure to transfer data
procedure_schema_1 = 'schema_name_1'
procedure_name_1 = 'procedure_name_1'
procedure_schema_2 = 'schema_name_2'
procedure_name_2 = 'procedure_name_2'

call_stored_procedure(host, name, passwd, db,  procedure_schema_1, procedure_name_1)
call_stored_procedure(host, name, passwd, db,  procedure_schema_2, procedure_name_2)

