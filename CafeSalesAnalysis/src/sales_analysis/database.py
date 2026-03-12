"""Functions handling the connection of the data to postgres
Create your database ahead of time to use anything here.

Before you're able to connect to the db, do this in your terminal:
    psql -U postgres
    CREATE DATABASE cafe_sales;.
Create a file ".env" in src/.
Write a single line to the file:
    CS=postgresql://postgres:{password}@localhost:5432/cafe_sales
Be sure to replace {password} with your actual password."""

import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
import sqlalchemy as sa
from src.sales_analysis import logger, exceptions as ex

logger = logger.setup_logger(__name__, "debug")

def connect_db() -> sa.Engine:
    """Sets up connection to db"""
    logger.debug("Attempting db connection")
    try:        
        load_dotenv()
        CS = os.getenv("CS")
        engine = sa.create_engine(CS)
    except Exception as e:
        e = ex.DatabaseConnectionError(e)
        logger.error(e)
    else:
        logger.info(f"Successfully connected to the database")
        return engine

def drop_table(table="sales") -> None:
    """Removes table from the db"""
    logger.debug("Attempting to clear the database")

    engine = connect_db()
    query = f"DROP TABLE {table}"

    try:
        with engine.connect() as conn:
            conn.execute(sa.text(query))
            conn.commit()
    except Exception as e:
        e = ex.DatabaseExeError(query, e)
        logger.error(e)
    else:
        logger.info(f"Successfully cleared table {table}")
    finally:
        engine.dispose()
        logger.debug("End of drop_table()")

def write_from_dataframe(df: pd.DataFrame, db = "cafe_sales", table = "sales") -> None:
    """Write the content of a dataframe to specified table"""
    pass

def read_as_dataframe(db = "cafe_sales", table = "sales") -> pd.DataFrame:
    """Read from the specified table as a dataframe"""
    pass

if __name__ == "__main__":
    pass
