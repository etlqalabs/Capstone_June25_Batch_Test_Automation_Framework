import pandas as pd
import pytest
from sqlalchemy import create_engine
import cx_Oracle

from CommonUtilities.utilities import check_for_duplicate_rows_in_file, check_for_duplicate_column_in_file, \
    check_for_Empty_or_NULL_values_in_file, check_for_Empty_or_NULL_values_in_specific_column_in_file, \
    check_file_exists, check_file_size
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



## Duplicate Value Checks:

# Test to check row level duplication in sales_data.csv file
@pytest.mark.DQ
def test_sales_data_duplicate_check():
    is_duplicate = check_for_duplicate_rows_in_file("TestData/sales_data_remote.csv", "csv")
    assert is_duplicate == False ,"There are duplicates in the file"


# Test to check column level ( sales_id) duplication in sales_data.csv file
@pytest.mark.DQ
def test_sales_data_duplicate_sales_id_check():
    is_duplicate = check_for_duplicate_column_in_file("TestData/sales_data_remote.csv", "csv","sales_id")
    assert is_duplicate == False ,"There are duplicates in sales_id column in the file"

# Write some more tests for other columns in sales_data.csv ile


# Test to check row level duplication in product_data.csv file and
@pytest.mark.skip
def test_product_data_duplicate_check():
    pass



# Test to check column level ( product_id )duplication in product_data.csv file and
@pytest.mark.skip
def test_product_id_data_duplicate_check():
    pass


# Test to check row level duplication in supplier_data.json file and
@pytest.mark.skip
def test_supplier_data_duplicate_check():
    pass


# Test to check column level ( supplier_id )duplication in supplier_data.json file and
@pytest.mark.skip
def test_supplier_id_in_supplier_data_duplicate_check():
    pass



# Test to check row level duplication in inventory_data.xml file and
@pytest.mark.skip
def test_inventory_data_duplicate_check():
    pass


# Test to check column level ( product_id )duplication in inventory_data.xml file and
@pytest.mark.skip
def test_product_id_in_inventory_data_duplicate_check():
    pass


# Test to check row level duplication in stores data from oracle
@pytest.mark.skip
def test_stores_data_duplicate_check():
    pass


# Test to check column level duplication in stores table on store_id from oracle
@pytest.mark.skip
def test_DQ_store_id_in_stores_oracle_table_duplicate_check():
    pass


## NULL Value Checks:
@pytest.mark.DQ
def test_DQ_sales_data_file_NULL_values_check():
    try:
        logger.info("NULL values check strted in sales data file...")
        is_null = check_for_Empty_or_NULL_values_in_file("TestData/sales_data_remote.csv", "csv")
        assert is_null == False, "There are NUll values in the file"
        logger.info("NULL values check completed in sales data file...")
    except Exception as e:
        logger.error("Error while checking for the nulll values")
        pytest.fail("There are null values in the sales file")

@pytest.mark.DQ
def test_sales_data_file_NULL_values_for_region_column_check():
    try:
        logger.info("NULL values check strted in sales data file...")
        is_null = check_for_Empty_or_NULL_values_in_specific_column_in_file("TestData/sales_data_remote.csv", "csv","region")
        assert is_null == False, "There are NUll values in the file"
        logger.info(f"NULL values check completed in sales data file for region column")
    except Exception as e:
        logger.error("Error while checking for the null values")
        pytest.fail("There are null values in the sales file")


 # Implement the NULL values check sfor all the files and orcale source( stores) table
 # a) ON the file level
 # b) on column levels


# File existence check related test cases
@pytest.mark.DQ
def test_DQ_sales_data_file_availability():
    try:
        logger.info("sales data file availability check started....")
        does_file_exist = check_file_exists("TestData/sales_data_remote.csv")
        assert does_file_exist == True, "Sales file doesn not exist at the source location"
        logger.info("sales data file availability check completed....")
    except Exception as e:
        logger.error("Error while file availability checks")
        pytest.fail("sales file doesn not exist at the mentioned location")

# Please implement thes etest cases
@pytest.mark.skip
def test_DQ_product_data_file_availability():
    pass

@pytest.mark.skip
def test_DQ_inventory_data_file_availability():
    pass

@pytest.mark.skip
def test_DQ_supplier_data_file_availability():
    pass


# File size check related test cases
@pytest.mark.DQ
def test_DQ_sales_data_file_size():
    try:
        logger.info("sales data file size check started....")
        non_zero_file_size = check_file_size("TestData/sales_data_remote.csv")
        assert non_zero_file_size == True, "Sales file is empty"
        logger.info("sales data file size check completed....")
    except Exception as e:
        logger.error("Error while file size checks")
        pytest.fail("sales file is empty")


# Please implement thse file size checks
def test_DQ_product_data_file_size():
    pass


def test_DQ_inventory_data_file_size():
    pass


def test_DQ_supplier_data_file_size():
    pass


# Referential integrity Checks


# Ref integrity check for sales_id in fact_sales target table
@pytest.mark.DQ
def test_DQ_ReferentialIntegrity_check_between_stag_sales_and_fact_sales_target():
    query_expected = """select sales_id from stag_sales"""
    df_expected = pd.read_sql(query_expected,mysql_conn)
    query_actual = """select sales_id from fact_sales"""
    df_actual = pd.read_sql(query_actual, mysql_conn)
    df_mismatched = df_actual[~df_actual['sales_id'].isin(df_expected['sales_id'])]
    df_matched = df_actual[df_actual['sales_id'].isin(df_expected['sales_id'])]
    df_matched.to_csv("Data_matched/valid_sales_id_in_fact_sales.csv", index=False)
    if df_mismatched.empty != True:
        df_mismatched.to_csv("Data_Differences/extra_sales_id_in_fact_sales.csv",index=False)
    assert df_mismatched.empty,"There are extra sales_id in target - please invetigate"


# Implement these and any other between stag to stag as well
def test_DQ_ReferentialIntegrity_check_between_stag_stores_and_fact_sales_target():
    pass

def test_DQ_ReferentialIntegrity_check_between_stag_product_and_fact_sales_target():
    pass