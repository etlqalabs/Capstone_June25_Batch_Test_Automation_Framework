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
    filename="LogFiles/data_transformation_test.log",
    filemode='a', # a for append , w = overwrite
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
)
logger = logging.getLogger(__name__)


@pytest.mark.smoke
def test_DT_Filter_checks(conect_to_mysql_database):
    try:
        logger.info("Test case for Filter transfromaiton check has started..")
        query_expected ="""select * from stag_sales where sale_date>='2024-09-10'"""
        query_actual = """select * from filtered_sales"""
        verify_expected_as_db_to_actual_as_db(conect_to_mysql_database, query_expected, conect_to_mysql_database, query_actual)
        logger.info("Test case for Filter transfromaiton check has completed..")
    except Exception as e:
        logger.error("Test case for Filter transfromaiton check caused problem..")
        pytest.fail("Test case for Filter transfromaiton check has failed..")

@pytest.mark.smoke
def test_DT_Router_High_checks(conect_to_mysql_database):
    try:
        logger.info("Test case for Router High transfromaiton check has started..")
        query_expected ="""select * from filtered_sales where region='High'"""
        query_actual = """select * from high_sales"""
        verify_expected_as_db_to_actual_as_db(conect_to_mysql_database, query_expected, conect_to_mysql_database, query_actual)
        logger.info("Test case for Router High transfromaiton check has completed..")
    except Exception as e:
        logger.error("Test case for Router High transfromaiton check caused problem..")
        pytest.fail("Test case for Router High transfromaiton check has failed..")


@pytest.mark.smoke
def test_DT_Router_Low_checks(conect_to_mysql_database):
    try:
        logger.info("Test case for Router Low transfromaiton check has started..")
        query_expected ="""select * from filtered_sales where region='Low'"""
        query_actual = """select * from low_sales"""
        verify_expected_as_db_to_actual_as_db(conect_to_mysql_database, query_expected, conect_to_mysql_database, query_actual)
        logger.info("Test case for Router Low transfromaiton check has completed..")
    except Exception as e:
        logger.error("Test case for Router Low transfromaiton check caused problem..")
        pytest.fail("Test case for Router Low transfromaiton check has failed..")

@pytest.mark.smoke
def test_DT_Aggregator_sales_data_checks(conect_to_mysql_database):
    try:
        logger.info("Test case for Aggregator - Sales transfromaiton check has started..")
        query_expected ="""select product_id,month(sale_date) as month,year(sale_date) as year,sum(price*quantity) as total_sales from filtered_sales
                group by product_id,month(sale_date),year(sale_date) """
        query_actual = """select * from monthly_sales_summary_source"""
        verify_expected_as_db_to_actual_as_db(conect_to_mysql_database, query_expected, conect_to_mysql_database, query_actual)
        logger.info("Test case for Aggregator - Sales transfromaiton check has completed..")
    except Exception as e:
        logger.error("Test case for Aggregator - Sales transfromaiton check caused problem..")
        pytest.fail("Test case for Aggregator - Sales transfromaiton check has failed..")

@pytest.mark.smoke
def test_DT_Joiner_sales_product_stores_checks(conect_to_mysql_database):
    try:
        logger.info("Test case for Joiner_sales_product_stores transfromaiton check has started..")
        query_expected ="""select fs.sales_id,fs.quantity,fs.price,fs.quantity*fs.price as total_sales_amount,fs.sale_date,
                p.product_id,p.product_name,
                s.store_id,s.store_name
                from filtered_sales as fs 
                inner join stag_product as p on fs.product_id = p.product_id
                inner join stag_stores as s on s.store_id = fs.store_id"""

        query_actual = """select * from sales_with_details"""
        verify_expected_as_db_to_actual_as_db(conect_to_mysql_database, query_expected, conect_to_mysql_database, query_actual)
        logger.info("Test case for Joiner_sales_product_stores transfromaiton check has completed..")
    except Exception as e:
        logger.error("Test case for Joiner_sales_product_stores transfromaiton check caused problem..")
        pytest.fail("Test case for Joiner_sales_product_stores transfromaiton check has failed..")

def test_DT_Aggregator_Inventory_data_checks(conect_to_mysql_database):
    try:
        logger.info("Test case for Aggregator - Inventory transfromaiton check has started..")
        query_expected ="""select store_id, sum(quantity_on_hand) as total_inventory from stag_inventory group by store_id """
        query_actual = """select * from aggregated_inventory_level"""
        verify_expected_as_db_to_actual_as_db(conect_to_mysql_database, query_expected, conect_to_mysql_database, query_actual)
        logger.info("Test case for Aggregator - Inventory transfromaiton check has completed..")
    except Exception as e:
        logger.error("Test case for Aggregator - Inventory transfromaiton check caused problem..")
        pytest.fail("Test case for Aggregator - Inventory transfromaiton check has failed..")
