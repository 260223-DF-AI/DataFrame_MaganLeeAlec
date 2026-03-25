from fastapi import FastAPI, Body, HTTPException, UploadFile
import json
import glob
from src.sales_analysis.gcs import upload_dir_to_gcs
from src.sales_analysis import file_reader, logger,  validation
from google.cloud import storage
from pydantic import BaseModel
import os
from dotenv import load_dotenv
import time

# Pydantic model for passing a csv filepath into POST request body
# class CSV_File(BaseModel):
#     csv_filepath: str

app = FastAPI()
logger = logger.setup_logger(__name__, "debug", console=False)

# Create a .env file with the following values: GCP_BUCKET_NAME, GCP_PROJECT_ID, ROOT_PATH
# ROOT_PATH can be "stg_sales"
load_dotenv()
GCP_BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
ROOT_PATH = os.environ.get("ROOT_PATH")
TOKEN = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
# Send an HTTP `POST` request to trigger `.csv` to `.parquet` conversion pipelines.

@app.post("/convert")
def csv_to_parquet():
    """Converts all .csv files into .parquet files. Then italidates, cleans, and uploads them to GCS"""
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
    storage_client = storage.Client(GCP_PROJECT_ID)
    try:
        bucket = storage_client.get_bucket(GCP_BUCKET_NAME)
        logger.warning(f"Bucket {GCP_BUCKET_NAME} already exists")
    except Exception as e:
        print(e)
        bucket = storage_client.create_bucket(GCP_BUCKET_NAME, location='US')
        logger.info(f"{bucket.name} created in {bucket.location}")
        
    # bucket = None

    # if bucket is not None:
    #     logger.warning(f"Bucket {bucket.name} already exists")
    # else:
    #     bucket = storage_client.create_bucket(GCP_BUCKET_NAME, location='US')
    #     logger.info(f"{bucket.name} created in {bucket.location}")
        
    current_time = time.time()
    upload_dir_to_gcs(df1_valid, bucket.name, ROOT_PATH, GCP_PROJECT_ID)
    upload_dir_to_gcs(df2_valid, bucket.name, ROOT_PATH, GCP_PROJECT_ID)
    upload_dir_to_gcs(df3_valid, bucket.name, ROOT_PATH, GCP_PROJECT_ID)
    upload_dir_to_gcs(df4_valid, bucket.name, ROOT_PATH, GCP_PROJECT_ID)
    upload_dir_to_gcs(df5_valid, bucket.name, ROOT_PATH, GCP_PROJECT_ID)
    total_time = time.time() - current_time
    
    logger.info(f"Time taken to upload files into GCS: {total_time:.02f}s")
    return "Success"
    
@app.post("/convert_csv")
def convert_csv_upload(csv_filepath: str):
	"""Convert a local CSV into parquet, with partitions, and upload to Google Cloud Storage"""
	# Validation and Cleaning
	df = file_reader.read_csv_full(csv_filepath)
	df = validation.clean_sales_data(df).valid
	validation.validate_chunk_dtypes(df)
	logger.info(f"{df.dtypes}")
 
	# Create a client by passing in the Google Cloud project ID
	storage_client = storage.Client(GCP_PROJECT_ID)
	
	# Get the bucket by name - this function will raise error if not found
	try:
		bucket = storage_client.bucket(GCP_BUCKET_NAME)
	except Exception as e:
		print(e)
		bucket = None
	# Set the bucket to None if it wasn't found. If it is found, report that it already exists
	if bucket.exists():
		logger.warning(f"Bucket {bucket.name} already exists")

	else:
		try:
			bucket = storage_client.create_bucket(GCP_BUCKET_NAME, location='US')
			logger.info(f"{bucket.name} created in {bucket.location}")
			
		except Exception as e:
			logger.error(e)

	# Pass bucket details into function to upload to Google Cloud Storage
	# Warning: If the bucket is deleted and then the same bucket is recreated quickly, an error will be raised
	current_time = time.time()
	upload_dir_to_gcs(df, bucket.name, ROOT_PATH, GCP_PROJECT_ID)
	total_time = time.time() - current_time
 
	logger.info(f"Time taken to upload {csv_filepath} file into GCS: {total_time:.02f}s")
	return "Success"


# Send an HTTP `GET` request specifying metric parameters to trigger a **BigQuery** query and fetch row JSONs safely.
@app.get("/")
def temp():
    return "connected"


