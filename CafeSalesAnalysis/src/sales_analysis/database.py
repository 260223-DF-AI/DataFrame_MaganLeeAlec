"""Functions handling the connection of the data to postgres"""

import pandas as pd
import psycopg2
from dotenv import load_dotenv

"""Create your database ahead of time.
Before you're able to connect to the db, do this in your terminal:
    psql -U postgres
    CREATE DATABASE cafe_sales;
Then, in CafeSalesAnalysis, create a new folder ".env".
Within the folder, create a single file "connection_string.txt"
Within the file, add your connection string on the first and only line:
    C://postgres:{password}@localhost:5432/cafe_sales
Be sure to replace {password} with your actual password.
"""
def connect_db(db = "cafe_sales") -> None:
    """Sets up connection to db"""
    
    pass

def clear_db(db = "cafe_sales") -> None:
    """Removes everything from the db"""
    pass

def write_from_dataframe(df: pd.DataFrame, db = "cafe_sales", table = "sales") -> None:
    """Write the content of a dataframe to specified table"""
    pass

def read_as_dataframe(db = "cafe_sales", table = "sales") -> pd.DataFrame:
    """Read from the specified table as a dataframe"""
    pass
