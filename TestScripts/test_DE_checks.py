import pandas as pd
import pytest
from sqlalchemy import create_engine
import cx_Oracle

import paramiko
import logging

# Logging configuration
from CommonUtilities.utilities import verify_expected_as_file_to_actual_as_db, verify_expected_as_db_to_actual_as_db, \
    verify_expected_as_s3_to_actual_as_db

logging.basicConfig(
    filename="LogFiles/etljob.log",
    filemode='a', # a for append , w = overwrite
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
)
logger = logging.getLogger(__name__)


# supplier data extraction test case
def test_DE_supplier_data_to_staging(conect_to_mysql_database):
    try:
        logger.info("Test case for supplier data extraction has started..")
        verify_expected_as_file_to_actual_as_db("TestData/supplier_data.json","json",conect_to_mysql_database,"stag_supplier")
        logger.info("Test case for supplier data extraction has completed..")
    except Exception as e:
        logger.error("Test case for supplier data extraction caused problem..")
        pytest.fail("Test case for supplier data has failed..")

# Inventory data extraction test case

def test_DE_inventory_data_to_staging(conect_to_mysql_database):
    try:
        logger.info("Test case for inventory data extraction has started..")
        verify_expected_as_file_to_actual_as_db("TestData/inventory_data.xml", "xml", conect_to_mysql_database,
                                                "stag_inventory")
        logger.info("Test case for inventory data extraction has completed..")
    except Exception as e:
        logger.error("Test case for inventory data extraction caused problem..")
        pytest.fail("Test case for inventory data has failed..")

# Stores data extraction test case
def test_DE_stores_data_to_staging(conect_to_oracle_database,conect_to_mysql_database):
    try:
        logger.info("Test case for Stores data extraction has started..")
        query_expected ="""select * from stores"""
        query_actual = """select * from stag_stores"""
        verify_expected_as_db_to_actual_as_db(conect_to_oracle_database, query_expected, conect_to_mysql_database, query_actual)
        logger.info("Test case for Stores data extraction has completed..")
    except Exception as e:
        logger.error("Test case for Stores data extraction caused problem..")
        pytest.fail("Test case for Stores data has failed..")



#User_June_Batch
#s3://jun-proj-caps-bkt/product_data/product_data.csv

# product data extraction test case
def test_DE_product_data_to_staging(conect_to_mysql_database):
    try:
        logger.info("Test case for product data extraction has started..")

        bucket_name = "jun-proj-caps-bkt"
        file_key = "product_data/product_data.csv"
        query_actual = """select * from stag_product"""
        verify_expected_as_s3_to_actual_as_db(bucket_name, file_key, conect_to_mysql_database, query_actual)
        logger.info("Test case for product data extraction has completed..")
    except Exception as e:
        logger.error("Test case for product data extraction caused problem..")
        pytest.fail("Test case for product data has failed..")

