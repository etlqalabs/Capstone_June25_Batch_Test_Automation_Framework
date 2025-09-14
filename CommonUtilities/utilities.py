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
    filemode='w', # a for append , w = overwrite
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


# Compare data between file as expected and databse table as actual.
def verify_expected_as_file_to_actual_as_db(file_path,file_type,db_connection_fixture,table_name):
        if file_type =='csv':
            df_expected = pd.read_csv(file_path)
        elif file_type =='json':
            df_expected = pd.read_json(file_path)
        elif file_type =='xml':
            df_expected = pd.read_xml(file_path,xpath='.//item')
        else:
            raise ValueError(f"Usupported file type passed {file_type}")
        logger.info(f"Expected data is {df_expected}")

        # Read the actual data from staging table
        query_actual = f"select * from {table_name}"
        df_actual = pd.read_sql(query_actual, db_connection_fixture)
        logger.info(f"Actaul data is {df_actual}")

        assert df_actual.equals(df_expected), "data between source and staging not matching"

# Compare data between  table as expected and databse table as actual.
def verify_expected_as_db_to_actual_as_db(db_engine_expected,query_expected,db_engine_actual,query_actual):
     # Read expected data
     df_expected = pd.read_sql(query_expected,db_engine_expected)


     # Read expected data
     df_actual = pd.read_sql(query_actual,db_engine_actual)

     # Compare between expected and actual data
     assert df_actual.equals(df_expected),f"expected data {query_expected} " \
                                          f"does not match with actual data {query_actual}"



def download_sales_file_from_Linux():
    logger.info("Sales file download started....")
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname,username=user_name,password=pass_word)
        sftp = ssh_client.open_sftp()
        sftp.get(linux_file_path,project_file_path)
        sftp.close()
        ssh_client.close()
        logger.info("Sales file download completed....")
    except Exception as e:
        logger.error(f"download of sales file failed {e}",exc_info=True)
    logger.info("Sales file download completed....")


import boto3
from io import StringIO

# initialize the connection
s3 = boto3.client("s3")
def read_file_from_s3(bucket_name,file_key):
    # fetch the csv file from S3
    try:
        response = s3.get_object(Bucket=bucket_name,Key=file_key)
        csv_content = response['Body'].read().decode('utf-8')
        data = StringIO(csv_content)
        df = pd.read_csv(data)
        return df
    except Exception as e:
        logger.error(f"exception raised while reading from S3 {e}", exc_info=True)

# Compare data between s3 and  databse table as actual.
def verify_expected_as_s3_to_actual_as_db(bucket_name,file_key,db_engine_actual,query_actual):
    # read from S3 as expected data
    df_expected = read_file_from_s3(bucket_name,file_key)

    # Read expected data
    df_actual = pd.read_sql(query_actual, db_engine_actual)

    # Compare between expected and actual data
    assert df_actual.equals(df_expected), f"expected data {df_expected} " \
                                          f"does not match with actual data {df_actual}"
