import pandas as pd
import pytest
from sqlalchemy import create_engine
import cx_Oracle
from Configuration.config import *
import paramiko
import logging

# Logging configuration
logging.basicConfig(
    filename="LogFiles/conftest.log",
    filemode='a', # a for append , w = overwrite
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
)
logger = logging.getLogger(__name__)


@pytest.fixture()
def conect_to_oracle_database():
    logger.info("Oracle connection getting established...")
    oracle_conn = create_engine(
        f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}").connect()
    logger.info("Oracle connection has been established...")
    yield oracle_conn
    oracle_conn.close()
    logger.info("Oracle connection has been closed...")


@pytest.fixture()
def conect_to_mysql_database():
    logger.info("Mysql connection getting established...")
    mysql_conn = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}").connect()
    logger.info("mysql connection has been established...")
    yield mysql_conn
    mysql_conn.close()
    logger.info("mysql connection has been closed...")
