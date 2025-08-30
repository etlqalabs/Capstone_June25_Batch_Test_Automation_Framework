import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
from Configuration.config import *
import paramiko
import logging

# Logging configuration
logging.basicConfig(
    filename="LogFile/etljob.log",
    filemode='a', # a for append , w = overwrite
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
)
logger = logging.getLogger(__name__)



# Create database connection strings

oracle_conn = create_engine(f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}")

mysql_conn = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")



def check_for_duplicate_rows_in_file(file_path,file_type):
    logger.info(f"Duplicate check started ...")
    try:
        if file_type =='csv':
            df = pd.read_csv(file_path)
        elif file_type =='json':
            df = pd.read_json(file_path)
        elif file_type =='xml':
            df = pd.read_xml(file_path,xpath='.//item')
        else:
            raise ValueError(f"Usupported file type passed {file_type}")
        logger.info(f"Expected data is {df}")

        if df.duplicated().any():
            return True
        else:
            return False
    except Exception as e:
        print("error encountered while reading or writing..",e)

def check_for_duplicate_column_in_file(file_path,file_type,column_name):
    logger.info(f"Duplicate check started ...")
    try:
        if file_type =='csv':
            df = pd.read_csv(file_path)
        elif file_type =='json':
            df = pd.read_json(file_path)
        elif file_type =='xml':
            df = pd.read_xml(file_path,xpath='.//item')
        else:
            raise ValueError(f"Usupported file type passed {file_type}")
        logger.info(f"Expected data is {df}")

        if df[column_name].duplicated().any():
            return True
        else:
            return False
    except Exception as e:
        print("error encountered while reading or writing..",e)

