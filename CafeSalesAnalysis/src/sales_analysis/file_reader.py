import pandas as pd
import logger, exceptions as ex

logger = logger.setup_logger(__name__, "debug")

def read_csv(filepath: str) -> pd.DataFrame:
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
        print(df.tail(5))

        
if __name__ == "__main__":
    df = read_csv("src/data/dirty_cafe_sales.csv")