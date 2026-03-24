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

logger = logger.setup_logger(__name__, "error")

def connect_db() -> sa.Engine:
    """Sets up connection to db"""
    logger.debug("Attempting db connection")
    try:        
        load_dotenv("src/.env")

        CS = os.getenv("CS")
        logger.debug(f"CS: {CS}")
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

def write_from_dataframe(df: pd.DataFrame, table = "sales") -> None:
    """Write the content of a dataframe to specified table.
    If the table doesn't exist, it creates a new one."""
    logger.debug("Attempting to write to the database")
    engine = connect_db()

    try:
        if df.empty:
            logger.warning("An empty dataframe is being used, table will be created with no entries")
        with engine.connect() as conn:
            df.to_sql(table, conn)
    except Exception as e:
        logger.error(e)
    else:
        logger.info(f"Successfully wrote to table {table}")
    finally:
        engine.dispose()
        logger.debug("End of write_from_dataframe")


def read_as_dataframe(table = "sales") -> pd.DataFrame:
    """Read all rows from the specified table as a dataframe"""
    logger.debug(f"Attempting to read from table {table}")

    engine = connect_db()
    query = f"SELECT * FROM {table}"

    df = ""
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
    except Exception as e:
        e = ex.DatabaseExeError(query, e)
        logger.error(e)
    else:
        logger.info(f"Successfully read table {table}")
        return df
    finally:
        engine.dispose()
        logger.debug("End of read_as_dataframe")

def execute_sql(query: str, table = "sales") -> pd.DataFrame:
    """Risky method for handling any other sql queries. Probably don't use this."""
    logger.debug(f"Attempting query on table: {table}, query: {query}")

    engine = connect_db()

    df = ""
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
    except Exception as e:
        e = ex.DatabaseExeError(query, e)
        logger.error(e)
    else:
        logger.info(f"Successfully ran query: {query}")
        return df
    finally:
        engine.dispose()
        logger.debug("End of execute_sql")


# For debugging purposes
if __name__ == "__main__":
    drop_table()
    df = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
    'age': [28, 34, 25, 31, 29],
    'salary': [55000, 72000, 48000, 68000, 62000]
})
    write_from_dataframe(df)
#     query = "SELECT * FROM sales WHERE sales.Salary > 60000;"
#     df = execute_sql(query)
#     print(df.tail(5))
    