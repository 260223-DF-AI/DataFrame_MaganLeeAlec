"""Functions for reading csv & json files and storing as DataFrame"""

import pandas as pd
from src.paths import DATA_DIR
import src.sales_analysis.logger as logger, src.sales_analysis.exceptions as ex

logger = logger.setup_logger(__name__, "warning")

def read_csv_full(filepath: str) -> pd.DataFrame:
    """Reads csv file in src/data and returns it as a DataFrame"""
    logger.debug(f"Attempting to read csv file: {filepath}")
    filepath = DATA_DIR / filepath
    df = ""
    try:
        df = pd.read_csv(filepath)
    except FileNotFoundError as e:
        logger.error(e)
    except Exception as e:
        # if some other error occured, log as FileReadError
        e = ex.FileReadError(e)
        logger.error(e)
    else:
        logger.info(f"Successfully read file as dataframe: {filepath}")
        return df

def read_csv_nlines(filepath: str, nlines = 1):
    """Read csv by nlines at a time, nlines = 1 by default.
    Returns an iterable, so to use it, you can use a for loop. 
    ex. 'for line in read_csv_nlines(filepath):'
    """
    logger.debug(f"Attempting to read csv file as generator function: {filepath}")
    filepath = DATA_DIR / filepath
    df = ""
    try:
        df = pd.read_csv(filepath, on_bad_lines='skip', chunksize = nlines)
    except FileNotFoundError as e:
        logger.error(e)
    except Exception as e:
        # if some other error occured, log as FileReadError
        e = ex.FileReadError(e)
        logger.error(e)
    return df
        
def read_json_full(filepath: str) -> pd.DataFrame:
    """Reads json file in src/data and returns it as a DataFrame"""
    logger.debug(f"Attempting to read json file: {filepath}")
    filepath = DATA_DIR / filepath
    df = ""
    try:
        df = pd.read_json(filepath)
    except FileNotFoundError as e:
        logger.error(e)
    except Exception as e:
        # if some other error occured, log as FileReadError
        e = ex.FileReadError(e)
        logger.error(e)
    else:
        logger.info(f"Successfully read file as dataframe: {filepath}")
        return df

def read_json_nlines(filepath: str, nlines = 1):
    logger.debug(f"Attempting to read json file as generator function: {filepath}")
    filepath = DATA_DIR / filepath
    df = ""
    try:
        df = pd.read_json(filepath, lines=True, encoding_errors='ignore', chunksize = nlines)
    except FileNotFoundError as e:
        logger.error(e)
    except Exception as e:
        # if some other error occured, log as FileReadError
        e = ex.FileReadError(e)
        logger.error(e)
    return df

def print_df_preview(df: pd.DataFrame) -> None:
    print(df.tail(5))


# for debugging purposes
if __name__ == "__main__":
    csv = "dirty_cafe_sales.csv"
    json = "dirty_cafe_sales.json"
    for line in read_csv_nlines(json, nlines=1000):
        print_df_preview(line)