from fastapi import FastAPI, Body
import json
from src.sales_analysis import file_reader, logger

app = FastAPI()
logger = logger.setup_logger(__name__, "debug", console=False)

# Send an HTTP `POST` request to trigger `.csv` to `.parquet` conversion pipelines.
@app.post("/csv_parquet")
def csv_to_parquet():
    logger.debug("HTTP request recieved. Attempting to convert csv to parquet...")

    df1, df2, df3, df4, df5 = 0, 0, 0, 0, 0
    # read all csv batch files
    try:
        df1 = file_reader.read_csv_full("dummy_sales_batch_1.csv")
        df2 = file_reader.read_csv_full("dummy_sales_batch_2.csv")
        df3 = file_reader.read_csv_full("dummy_sales_batch_3.csv")
        df4 = file_reader.read_csv_full("dummy_sales_batch_4.csv")
        df5 = file_reader.read_csv_full("dummy_sales_batch_5.csv")
    except FileNotFoundError as e:
        logger.error(f"File not found error: {e}")
    except Exception as e:
        logger.error(e)
    else:
        logger.debug("Successfully read all sales batch files")
    
    # TODO: clean/validate

    # write all batch files as parquet files
    # for now, as seperate files. if needed can be changed to write as one file
    logger.debug("Attempting to write parquet files")
    try: 
        file_reader.write_parquet(df1, "dummy_sales_batch_1")
        file_reader.write_parquet(df2, "dummy_sales_batch_2")
        file_reader.write_parquet(df3, "dummy_sales_batch_3")
        file_reader.write_parquet(df4, "dummy_sales_batch_4")
        file_reader.write_parquet(df5, "dummy_sales_batch_5")
    except FileExistsError as e:
        logger.error(f"File already exists: {e}")
    except Exception as e:
        logger.error(e)
    else:
        logger.debug("Successfully wrote all batches as parquet files")

    #TODO: trigger upload of files to GCS
    



# Send an HTTP `GET` request specifying metric parameters to trigger a **BigQuery** query and fetch row JSONs safely.
@app.get("/")
def temp():
    return "connected"


