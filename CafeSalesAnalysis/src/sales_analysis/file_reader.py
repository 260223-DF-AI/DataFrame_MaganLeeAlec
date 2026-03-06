import pandas as pd
import logger, exceptions as ex

logger = logger.setup_logger(__name__, "debug", log=False)

def read_csv_full(filepath: str) -> pd.DataFrame:
    """Reads csv file and returns it as a DataFrame"""
    logger.debug(f"Attempting to read csv file: {filepath}")
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
    df = ""
    try:
        df = pd.read_csv(filepath, chunksize = nlines)
    except FileNotFoundError as e:
        logger.error(e)
    except Exception as e:
        # if some other error occured, log as FileReadError
        e = ex.FileReadError(e)
        logger.error(e)
    return df
        

        
if __name__ == "__main__":
    sales_data = "data/test.csv"
    read_csv_full(sales_data)
    # count, n = 0, 5
    # for i in range(n):
    #     for chunk in read_csv_nlines(sales_data, nlines=5):
    #         print(chunk)
    #         break