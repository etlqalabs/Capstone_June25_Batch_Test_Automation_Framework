import pandas as pd
import pytest
from sqlalchemy import create_engine
import cx_Oracle

import paramiko
import logging

# Logging configuration
from CommonUtilities.utilities import verify_expected_as_file_to_actual_as_db, verify_expected_as_db_to_actual_as_db, \
    verify_expected_as_s3_to_actual_as_db, download_sales_file_from_Linux

logging.basicConfig(
    filename="LogFiles/etljob.log",
    filemode='w', # a for append , w = overwrite
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
)
logger = logging.getLogger(__name__)


def test_DL_Sales_summary_checks(conect_to_mysql_database):
    try:
        logger.info("Test case for Sales summary data load check has started..")
        query_expected ="""select * from monthly_sales_summary_source order by product_id"""
        query_actual = """select * from monthly_sales_summary  order by product_id"""
        verify_expected_as_db_to_actual_as_db(conect_to_mysql_database, query_expected, conect_to_mysql_database, query_actual)
        logger.info("Test case for Sales summary data load check has completed..")
    except Exception as e:
        logger.error("Test case for  Sales summary data load check caused problem..",e)
        pytest.fail("Test case for  Sales summary data load check has failed..")


def test_DL_fact_sales_checks(conect_to_mysql_database):
    try:
        logger.info("Test case for fact sales check has started..")
        query_expected ="""select s.sales_id,s.product_id,s.store_id,s.quantity,s.total_sales_amount as total_sales,s.sale_date from sales_with_details as s"""
        query_actual = """select sales_id,product_id,store_id,quantity,total_sales,sale_date from fact_sales"""
        verify_expected_as_db_to_actual_as_db(conect_to_mysql_database, query_expected, conect_to_mysql_database, query_actual)
        logger.info("Test case for fact sales check has completed..")
    except Exception as e:
        logger.error("Test case for fact sales check caused problem..")
        pytest.fail("Test case for fact sales check has failed..")


def test_DL_fact_inventory_checks(conect_to_mysql_database):
    try:
        logger.info("Test case for fact inventory check has started..")
        query_expected ="""select product_id,store_id,quantity_on_hand,last_updated from stag_inventory"""
        query_actual = """select product_id,store_id,quantity_on_hand,last_updated from fact_inventory"""
        verify_expected_as_db_to_actual_as_db(conect_to_mysql_database, query_expected, conect_to_mysql_database, query_actual)
        logger.info("Test case for fact inventory check has completed..")
    except Exception as e:
        logger.error("Test case for fact inventory check caused problem..")
        pytest.fail("Test case for fact inventory check has failed..")


def test_DL_inventory_level_by_store_checks(conect_to_mysql_database):
    try:
        logger.info("Test case for  inventory_level_by_store check has started..")
        query_expected ="""select store_id,total_inventory from aggregated_inventory_level"""
        query_actual = """select store_id,total_inventory from inventory_levels_by_store"""
        verify_expected_as_db_to_actual_as_db(conect_to_mysql_database, query_expected, conect_to_mysql_database, query_actual)
        logger.info("Test case for inventory_level_by_store check has completed..")
    except Exception as e:
        logger.error("Test case for inventory_level_by_storecheck caused problem..")
        pytest.fail("Test case for inventory_level_by_store check has failed..")

