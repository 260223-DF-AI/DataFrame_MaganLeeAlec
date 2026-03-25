from fastapi import FastAPI, Body, HTTPException, UploadFile
import json
import glob
from src.sales_analysis import file_reader, logger,  validation
from google.cloud import storage
from datetime import datetime
import os
from dotenv import load_dotenv

app = FastAPI()
logger = logger.setup_logger(__name__, "debug", console=False)
load_dotenv()
# Send an HTTP `POST` request to trigger `.csv` to `.parquet` conversion pipelines.
@app.post("/convert")
async def csv_to_parquet():
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
    logger.debug("Validating and cleaning data...")
    df1_valid, df2_valid, df3_valid, df4_valid, df5_valid = 0, 0, 0, 0, 0
    try:
        # clean all the sales data
        df1_tuple = validation.clean_sales_data(df1)
        df2_tuple = validation.clean_sales_data(df2)
        df3_tuple = validation.clean_sales_data(df3)
        df4_tuple = validation.clean_sales_data(df4)
        df5_tuple = validation.clean_sales_data(df5)
        
        # separate valid/rejects, validate them
        df1_valid, df1_rejects = validation.validate_chunk_dtypes(df1_tuple[0]), 0#validation.validate_chunk_dtypes(df1_tuple[1])
        df2_valid, df2_rejects = validation.validate_chunk_dtypes(df2_tuple[0]), 0#validation.validate_chunk_dtypes(df2_tuple[1])
        df3_valid, df3_rejects = validation.validate_chunk_dtypes(df3_tuple[0]), 0#validation.validate_chunk_dtypes(df3_tuple[1])
        df4_valid, df4_rejects = validation.validate_chunk_dtypes(df4_tuple[0]), 0#validation.validate_chunk_dtypes(df4_tuple[1])
        df5_valid, df5_rejects = validation.validate_chunk_dtypes(df5_tuple[0]), 0#validation.validate_chunk_dtypes(df5_tuple[1])
    except Exception as e:
        logger.error(e)
    else:
        logger.debug("Successfully validated data")

    # write all batch files as parquet files
    # for now, as seperate files. if needed can be changed to write as one file
    logger.debug("Attempting to write parquet files")
    try: 
        # write valid
        file_reader.write_parquet(df1_valid, "dummy_sales_batch_1")
        file_reader.write_parquet(df2_valid, "dummy_sales_batch_2")
        file_reader.write_parquet(df3_valid, "dummy_sales_batch_3")
        file_reader.write_parquet(df4_valid, "dummy_sales_batch_4")
        file_reader.write_parquet(df5_valid, "dummy_sales_batch_5")

        # write invalid
        # file_reader.write_parquet(df1_rejects, "reject_sales_batch_1")
        # file_reader.write_parquet(df2_rejects, "reject_sales_batch_2")
        # file_reader.write_parquet(df3_rejects, "reject_sales_batch_3")
        # file_reader.write_parquet(df4_rejects, "reject_sales_batch_4")
        # file_reader.write_parquet(df5_rejects, "reject_sales_batch_5")
    except FileExistsError as e:
        logger.error(f"File already exists: {e}")
    except Exception as e:
        logger.error(e)
    else:
        logger.debug("Successfully wrote all batches as parquet files")
    
    logger.debug("Testing parquet write...")
    logger.debug(f"{file_reader.read_parquet_full("dummy_sales_batch_1.parquet").tail(5)}")

    #TODO: trigger upload of files to GCS

    
    try:
        bucket_name = 'sales-data-project-2'
        storage_client = storage.Client("project-93fc0424-6808-4472-bff")
        bucket = storage_client.bucket(bucket_name)
        new_bucket = storage_client.create_bucket(bucket, location='US')
        gcs_path = "stg_sales/year=2026/month=03/test_data"
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename('src/data/dummy_sales_batch_1.parquet')
    except HTTPException as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
        
    return f"uploaded to {bucket_name}/stg_sales/year=2026/month=03/test_data"



# Send an HTTP `GET` request specifying metric parameters to trigger a **BigQuery** query and fetch row JSONs safely.
@app.get("/")
def temp():
    return "connected"


