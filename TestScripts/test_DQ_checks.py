import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle

from CommonUtilities.utilities import check_for_duplicate_rows_in_file, check_for_duplicate_column_in_file
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




def test_sales_data_duplicate_check():
    is_duplicate = check_for_duplicate_rows_in_file("TestData/sales_data_linux.csv", "csv")
    assert is_duplicate == False ,"There are duplicates in the file"


def test_sales_data_duplicate_sales_id_check():
    is_duplicate = check_for_duplicate_column_in_file("TestData/sales_data_linux.csv", "csv","sales_id")
    assert is_duplicate == False ,"There are duplicates in sales_id column in the file"
