import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
from Configuration.config import *
import paramiko
import logging
import os.path

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


def check_for_Empty_or_NULL_values_in_file(file_path,file_type):
    logger.info(f" Nulls check started ...")
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

        if df.isnull().values.any():
            return True
        else:
            return False
    except Exception as e:
        print("error encountered while reading or writing..",e)

def check_for_Empty_or_NULL_values_in_specific_column_in_file(file_path,file_type,column_name):
    logger.info(f" Nulls check started column {column_name}  ...")
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

        if df[column_name].isnull().values.any():
            return True
        else:
            return False
    except Exception as e:
        print("error encountered while reading or writing..",e)


# File existence checks
def check_file_exists(file_path):
    try:
        if os.path.isfile(file_path):
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"file {file_path} does not exist {e}")

# File size checks ( zero byte file check )
def check_file_size(file_path):
    try:
        if os.path.getsize(file_path) != 0:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"file {file_path} is having zero size {e}")


# Implement this fucntion so we can use it across various tests
def refential_integrity_check(table1,table2):
    pass