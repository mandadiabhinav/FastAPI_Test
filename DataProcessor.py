from fastapi import FastAPI, Query, File, UploadFile, HTTPException, Body
import pyodbc
import pandas as pd
from fast_to_sql import fast_to_sql as fts
from sqlalchemy import create_engine
import time
import sqlalchemy
import time
import logging
from typing import Dict, List
import io
import os

from sqlalchemy.exc import SQLAlchemyError

from fastapi.encoders import jsonable_encoder
import logging
#function to configure the logs
def configure_logger(log_file='example.log', log_level=logging.INFO):
    # Configure the logging system
    logging.basicConfig(
        filename=log_file,        # Specify the log file name
        level=log_level,          # Set the logging level
        format='%(asctime)s - %(levelname)s - %(message)s'  # Define the log message format
    )

    # Create and return a logger object
    logger = logging.getLogger('my_logger')
    return logger
    
logger = configure_logger("Status.log")


app = FastAPI()

class DataProcessor:
    # Function to create an Azure SQL connection
    @staticmethod
    def create_sql_connection(sql_server, database, sql_user, sql_password):

        ODBC_DRIVER = 'ODBC Driver 18 for SQL Server'
        TIMEOUT = 240
        RETRY_ATTEMPTS = 3
        RETRY_DELAY = 60  # Initial delay between retries (in seconds)

        if not all([sql_server, database, sql_user, sql_password]):
            raise ValueError("All parameters (sql_server, database, sql_user, sql_password) must be provided.")

        sql_conn_str = (
            f"Driver={{{ODBC_DRIVER}}};"
            f"Server=tcp:{sql_server},1433;"
            f"Database={database};"
            f"Uid={sql_user};"
            f"Pwd={sql_password};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            f"Connection Timeout={TIMEOUT};"
        )
        print(sql_conn_str)
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                logger.info(f"Attempt {attempt} to connect to SQL Server.")
                return pyodbc.connect(sql_conn_str)
            except pyodbc.OperationalError as e:
                logger.error(f"OperationalError on attempt {attempt}: {e}")
            except pyodbc.InterfaceError as e:
                logger.error(f"InterfaceError on attempt {attempt}: {e}")
            except pyodbc.DatabaseError as e:
                logger.error(f"DatabaseError on attempt {attempt}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt}: {e}")

            if attempt < RETRY_ATTEMPTS:
                logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
                RETRY_DELAY *= 2  # Exponential backoff

        raise ConnectionError("Failed to connect to SQL Server after several attempts.")

    @staticmethod
    def create_sql_engine(sql_server, database, sql_user, sql_password):

        ODBC_DRIVER = 'ODBC Driver 18 for SQL Server'
        TIMEOUT = 240
        RETRY_ATTEMPTS = 3
        RETRY_DELAY = 60  # Initial delay between retries (in seconds)

        if not all([sql_server, database, sql_user, sql_password]):
            raise ValueError("All parameters (sql_server, database, sql_user, sql_password) must be provided.")

        database_uri = sqlalchemy.engine.url.URL.create(
            drivername="mssql+pyodbc",
            username=sql_user,
            password=sql_password,
            host=sql_server,
            port="1433",  # Default port for SQL Server
            database=database,
            query={"driver": ODBC_DRIVER}
        )

        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                logger.info(f"Attempt {attempt} to create SQL engine.")
                return create_engine(database_uri)
            except sqlalchemy.exc.OperationalError as e:
                logger.error(f"OperationalError on attempt {attempt}: {e}")
            except sqlalchemy.exc.InterfaceError as e:
                logger.error(f"InterfaceError on attempt {attempt}: {e}")
            except sqlalchemy.exc.SQLAlchemyError as e:
                logger.error(f"SQLAlchemyError on attempt {attempt}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt}: {e}")

            if attempt < RETRY_ATTEMPTS:
                logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
                RETRY_DELAY *= 2  # Exponential backoff

        raise ConnectionError("Failed to create SQL engine after several attempts.")
        
@app.post("/DataProcessor/process_and_upload_data")
async def process_and_upload_data(
    sql_server: str = Query(..., description="SQL Server address."),
    database: str = Query(..., description="Database name."),
    sql_user: str = Query(..., description="SQL user."),
    sql_password: str = Query(..., description="SQL password."),
    data: UploadFile = File(..., description="CSV file with data to upload."),
    table_name: str = Query(..., description="Table name for data upload."),
    if_exists: str = Query(default="replace", description="Action if the table already exists.")
):
    """Process and upload data to SQL database."""
    sql_conn = None
    try:
        
        # Read the file contents
        contents = await data.read()
        
        data = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        
        # Create SQL connection
        sql_conn = DataProcessor.create_sql_connection(sql_server, database, sql_user, sql_password)
        
        if data.empty:
            return {"message": "No data to upload"}

        # Determine column types based on maximum length
        string_columns = data.select_dtypes(include=["object"]).columns
        column_types = {
            col: "VARCHAR(max)" for col in string_columns
            if data[col].apply(lambda x: len(str(x))).max() > 225
        }

        # Upload data in chunks
        total_rows = len(data)
        chunk_size = 50000
        insert_mode = if_exists

        for start_index in range(0, total_rows, chunk_size):
            end_index = min(start_index + chunk_size, total_rows)
            chunk_data = data[start_index:end_index]
            fts(chunk_data, table_name, sql_conn, if_exists=insert_mode, custom=column_types)
            insert_mode = "append"  # Subsequent chunks will append to the table

        sql_conn.commit()
        return {"message": "Data uploaded successfully"}

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")
    finally:
        if sql_conn is not None:
            sql_conn.close()

@app.get("/DataProcessor/execute_query")
def execute_query(
    sql_server: str = Query(..., description="SQL Server address."),
    database: str = Query(..., description="Database name."),
    sql_user: str = Query(..., description="SQL user."),
    sql_password: str = Query(..., description="SQL password."),
    command: str = Query(..., description="SQL command to execute.")
) -> Dict[str, str]:
    """Execute a SQL command and return a success or error message."""
    try:
        # Establish database connection
        conn = DataProcessor.create_sql_connection(sql_server, database, sql_user, sql_password)
        with conn.cursor() as cursor:
            cursor.execute(command)
            conn.commit()
        return {"message": f"Executed query successfully: {command}"}
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")
    finally:
        conn.close()
        
@app.get("/DataProcessor/get_query_output")
def get_query_output(
    command: str = Query(..., description="SQL command to execute."),
    sql_server: str = Query(..., description="SQL Server address."),
    database: str = Query(..., description="Database name."),
    sql_user: str = Query(..., description="SQL user."),
    sql_password: str = Query(..., description="SQL password.")
):
    # Validate required parameters
    for param, name in [(command, "SQL command"), (sql_server, "SQL Server"), 
                        (database, "Database name"), (sql_user, "SQL user"), 
                        (sql_password, "SQL password")]:
        if not param.strip():
            raise HTTPException(status_code=400, detail=f"{name} must not be empty.")
    
    try:
        engine = DataProcessor.create_sql_engine(sql_server, database, sql_user, sql_password)
        df = pd.read_sql(command, engine)
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"SQL error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")

    return jsonable_encoder(df.to_dict(orient="records"))