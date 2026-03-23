"""Functions for reading/writing csv, json, and parquet files and storing as DataFrame"""

import pandas as pd
from src.paths import DATA_DIR
import src.sales_analysis.logger as logger, src.sales_analysis.exceptions as ex

logger = logger.setup_logger(__name__, "debug")

def read_csv_full(file_name: str) -> pd.DataFrame:
    """Reads csv file in src/data and returns it as a DataFrame"""
    logger.debug(f"Attempting to read csv file: {file_name}")
    file_name = DATA_DIR / file_name
    df = ""
    try:
        df = pd.read_csv(file_name)
    except FileNotFoundError as e:
        logger.error(e)
    except Exception as e:
        # if some other error occured, log as FileReadError
        e = ex.FileReadError(e)
        logger.error(e)
    else:
        logger.info(f"Successfully read file as dataframe: {file_name}")
        return df

def read_csv_nlines(file_name: str, nlines = 1):
    """Read csv by nlines at a time, nlines = 1 by default.
    Returns an iterable, so to use it, you can use a for loop. 
    ex. 'for line in read_csv_nlines(file_name):'
    """
    logger.debug(f"Attempting to read csv file as pd.TextFileReader object: {file_name}")
    file_name = DATA_DIR / file_name
    df = ""
    try:
        df = pd.read_csv(file_name, on_bad_lines='skip', chunksize = nlines)
    except FileNotFoundError as e:
        logger.error(e)
    except Exception as e:
        # if some other error occured, log as FileReadError
        e = ex.FileReadError(e)
        logger.error(e)
    return df
        
def read_json_full(file_name: str) -> pd.DataFrame:
    """Reads json file in src/data and returns it as a DataFrame"""
    logger.debug(f"Attempting to read json file: {file_name}")
    file_name = DATA_DIR / file_name
    df = ""
    try:
        df = pd.read_json(file_name)
    except FileNotFoundError as e:
        logger.error(e)
    except Exception as e:
        # if some other error occured, log as FileReadError
        e = ex.FileReadError(e)
        logger.error(e)
    else:
        logger.info(f"Successfully read file as dataframe: {file_name}")
        return df

def read_json_nlines(file_name: str, nlines = 1):
    logger.debug(f"Attempting to read json file as pd.TextFileReader object: {file_name}")
    file_name = DATA_DIR / file_name
    df = ""
    try:
        df = pd.read_json(file_name, lines=True, encoding_errors='ignore', chunksize = nlines)
    except FileNotFoundError as e:
        logger.error(e)
    except Exception as e:
        # if some other error occured, log as FileReadError
        e = ex.FileReadError(e)
        logger.error(e)
    return df

def read_parquet_full(file_name: str) -> pd.DataFrame:
    """Reads parquet file in src/data and returns it as a DataFrame"""
    logger.debug(f"Attempting to read parquet file: {file_name}")
    file_name = DATA_DIR / file_name
    df = ""
    try:
        df = pd.read_parquet(file_name)
    except FileNotFoundError as e:
        logger.error(e)
    except Exception as e:
        # if some other error occured, log as FileReadError
        e = ex.FileReadError(e)
        logger.error(e)
    else:
        logger.info(f"Successfully read file as dataframe: {file_name}")
        return df

def write_parquet(dataframe: pd.DataFrame, file_name: str) -> None:
    logger.debug(f"Attempting to write dataframe as parquet file named {file_name}")
    file_path = DATA_DIR / f"{file_name}.parquet"
    try:
        dataframe.to_parquet(path=file_path)
    except Exception as e:
        # if some other error occured, log as FileReadError
        e = ex.FileReadError(e)
        logger.error(e)
    else:
        logger.info(f"Successfully read parquet file {file_name} as dataframe")

def print_df_preview(df: pd.DataFrame) -> None:
    print(df.tail(5))

# for debugging purposes
if __name__ == "__main__":
    sales_batch = "dummy_sales_batch_1.csv"
    df = read_csv_full(sales_batch)
    write_parquet(df, "dummy_sales_batch")
    print_df_preview(read_parquet_full("dummy_sales_batch.parquet"))
