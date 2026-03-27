from fastapi import FastAPI, Query
from src.sales_analysis.gcs import upload_dir_to_gcs, write_local_partition, upload_parquet_if_not_exists
from src.sales_analysis import file_reader, logger, validation
from google.cloud import storage, bigquery
from google.api_core.exceptions import Conflict, Forbidden
import os
from dotenv import load_dotenv
import time
import matplotlib.pyplot as plt # generate chart
import pandas as pd #convert query results to dataframe
import io # in memory buffer for chart image
import base64 # encode image to send via API
import glob
from pathlib import Path

# Pydantic model for passing a csv filepath into POST request body
# class CSV_File(BaseModel):
#     csv_filepath: str
from src.paths import SALES_ANA_DIR

app = FastAPI()
logger = logger.setup_logger(__name__, "debug", console=False)

# Create a .env file with the following values: GCP_BUCKET_NAME, GCP_PROJECT_ID, ROOT_PATH
# ROOT_PATH can be "stg_sales"
load_dotenv()
GCP_BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
ROOT_PATH = os.environ.get("ROOT_PATH")
TOKEN = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

most_recent_upload_time = 0
most_recent_query_execution_time = 0
most_recent_query_access_duration = 0

# Send an HTTP `POST` request to trigger `.csv` to `.parquet` conversion pipelines.
@app.post("/convert")
def csv_to_parquet():
    """Follows these steps:
    1. Read all csvs as dataframes
    2. Validate and clean data
    3. Convert to parquet files
    4. Upload to GCS
    5. Create or update BigQuery external table"""

    # clear the local log before executing
    try:
        log_path = SALES_ANA_DIR / "app.log"
        with open(log_path, "w") as f:
            f.write("")
    except Exception as e:
        logger.error(f"Error clearing the log: {e}")
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
    
    # clean/validate
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

	#trigger upload of files to GCS
    storage_client = storage.Client(GCP_PROJECT_ID)
    try:
        bucket = storage_client.create_bucket(GCP_BUCKET_NAME, location='US')
        logger.warning(f"Bucket {GCP_BUCKET_NAME} was created")
    except Conflict as e:
        logger.error(f'Bucket already exists. Error: {e}')
    except Forbidden as e:
        logger.error(f'Bucket name was taken by another Cloud Storage user. Error: {e}')
    except Exception as e:
        logger.error(f'Generic exception: {e}')
        
    write_local_partition()
    
    directory_path = "src/data/stg_sales/**/" 
    files = glob.glob(os.path.join(directory_path, '*.parquet'), recursive=True)
    logger.debug(files)
    for file in files:
        full_path = Path(file)
        remaining_path = full_path.parts[2:]
        desired_path = Path(*remaining_path)
        
        upload_parquet_if_not_exists(GCP_BUCKET_NAME, storage_client.bucket(GCP_BUCKET_NAME), Path(file), full_path)
        logger.debug(desired_path)
    
    # current_time = time.time()
    # upload_dir_to_gcs(df1_valid, GCP_BUCKET_NAME, ROOT_PATH, GCP_PROJECT_ID)
    # upload_dir_to_gcs(df2_valid, GCP_BUCKET_NAME, ROOT_PATH, GCP_PROJECT_ID)
    # upload_dir_to_gcs(df3_valid, GCP_BUCKET_NAME, ROOT_PATH, GCP_PROJECT_ID)
    # upload_dir_to_gcs(df4_valid, GCP_BUCKET_NAME, ROOT_PATH, GCP_PROJECT_ID)
    # upload_dir_to_gcs(df5_valid, GCP_BUCKET_NAME, ROOT_PATH, GCP_PROJECT_ID)
    # total_time = time.time() - current_time
    # most_recent_upload_time = total_time
    
    # logger.info(f"Time taken to upload files into GCS: {total_time:.02f}s")
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
		bucket = storage_client.create_bucket(GCP_BUCKET_NAME)
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
# BigQuery client
client = bigquery.Client()

# Environment variables
BQ_TABLE = os.getenv("BQ_TABLE")  # format: project.dataset.table
GCP_BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
ROOT_PATH = os.environ.get("ROOT_PATH")
TOKEN = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

def generate_chart(rows: list[dict], metric: str, group_by: str) -> str | None:
    """
    Creates a simple bar chart from query results and returns it as a base64 PNG string
    - converts query output into a visual for presentation/demo
    - keeps everything in memory (no file system needed)
    """

    # if no data, dont try to build the chart
    if not rows:
        return None
    # convert query results into a dataframe
    df = pd.DataFrame(rows)
    # ensure required columns exist
    if "dimension" not in df.columns or "metic_value" not in df.columns:
        return None
    # keep only the top 10 rows for readability
    df = df.head(10).copy()
    # convert to safe plotting types
    df["dimension"] = df["dimension"].astype(str)
    df["metric_value"] = pd.to_numeric(df["metric_value"], errors = "coerce")
    # drop invalid numeric rows
    df = df.dropna(subset = ["metric_value"])
    if df.empty:
        return None
    # create chart
    fig, ax = plt.subplots(figsize = (10,6))
    ax.bar(df["dimension"], df["metric_value"])
    # add labels for clarity
    ax.set_title(f"{metric} by {group_by}")
    ax.set_xlabel(group_by)
    ax.set_ylabel(metric)
    # rotate labels for readability
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    # save chart to memory buffer instead of a file
    buffer = io.BytesIO()
    plt.savefig(buffer, format="png", bbox_inches="tight")
    plt.close(fig)
    buffer.seek(0)
    # convert image to base64 string (so API can return it)
    encoded = base64.b64encode(buffer.read()).decode("utf-8")
    return encoded



@app.get("/")
def root():
    return {"message": "BigQuery API running"}


@app.get("/query")
def run_query(
    metric: str = Query(..., description="Metric to calculate"),
    group_by: str = Query(..., description="Column to group by"),
    transaction_id: str = None,
    start_date: str = None,
    end_date: str = None,
    store_id: str = None,
    store_location: str = None,
    region: str = None,
    state: str = None,
    customer_id: str = None,
    customer_name: str = None,
    segment: str = None,
    product_id: str = None, 
    product_name: str = None,
    category: str = None,
    subcategory: str = None,
    quantity: int = None,
    unit_price: float = None,
    discount_percent: float = None,
    tax_amount: float = None,
    shipping_cost: float = None,
    total_amount: float = None,
    limit: int = 10
):
    """Run a BigQuery query and return row JSONs safely."""

    # Map allowed metrics
    metric_map = {
        "total_sales": "SUM(TotalAmount)",
        "order_count": "COUNT(*)",
        "avg_order_value": "AVG(TotalAmount)",
        "units_sold": "SUM(Quantity)"
    }

    if metric not in metric_map:
        return {"error": f"Invalid metric: {metric}"}

    metric_sql = metric_map[metric]

    # Base query
    query = f"""
        SELECT
            {group_by} AS dimension,
            {metric_sql} AS metric_value
        FROM `{BQ_TABLE}`
        WHERE 1 = 1 
    """

    # Optional filters- add a where clause with the value if it was specified
    if transaction_id:
        query += f" AND TransactionID = '{transaction_id}'"
    if start_date:
        query += f" AND Date >= '{start_date}'"
    if end_date:
        query += f" AND Date <= '{end_date}'"
    if store_id:
        query += f" AND StoreID = '{store_id}'"
    if store_location:
        query += f" AND StoreLocation = '{store_location}'"
    if region:
        query += f" AND Region = '{region}'"
    if state:
        query += f" AND State = '{state}'"
    if customer_id:
        query += f" AND CustomerID = '{customer_id}'"
    if customer_name:
        query += f" AND CustomerName = '{customer_name}'"
    if segment:
        query += f" AND Segment = '{segment}'"
    if product_id:
        query += f" AND ProductID = '{product_id}'"
    if product_name:
        query += f" AND ProductName = '{product_name}'"
    if category:
        query += f" AND Category = '{category}'"
    if subcategory:
        query += f" AND Subcategory = '{subcategory}'"
    if quantity:
        query += f" AND Quantity = {quantity}"
    if unit_price:
        query += f" AND UnitPrice = {unit_price}"
    if discount_percent:
        query += f" AND DiscountPercent = {discount_percent}"
    if tax_amount:
        query += f" AND TaxAmount = {tax_amount}"
    if shipping_cost:
        query += f" AND ShippingCost = {shipping_cost}"
    if total_amount:
        query += f" AND TotalAmount = {total_amount}"


    # Grouping + ordering
    query += f"""
        GROUP BY {group_by}
        ORDER BY metric_value DESC
        LIMIT {limit}
    """

    # Execute query
    try:
        start_time = time.time()
        results = client.query(query).result()
        access_duration = time.time() - start_time
        execution_time = (results.ended - results.started).total_seconds()
        most_recent_query_access_duration = access_duration
        most_recent_query_execution_time = execution_time
        logger.debug(f"Most recent query access time: {most_recent_query_access_duration}s")
        logger.debug(f"Most recent query execution time: {most_recent_query_execution_time}s")
    except Exception as e:
         logger.error(f"An error occured with the query {query}: {e}")
    else:
        rows = [dict(row) for row in results]
        #added to generate chart from query results
        chart_base64 = generate_chart(rows, metric, group_by)
        time.sleep(0.1)
        logger.info(f"Successfully ran query: {query}")  
        return {
            "table": BQ_TABLE,
            "metric": metric,
            "group_by": group_by,
            "row_count": len(rows),
            "rows": rows,
            # added to include the chart in the response
            "chart": {
                "type": "bar",
                "image_base64": chart_base64,
                "mime_type": "image/png"
            }
        }
    

    logger.info("End of run_query")

@app.get("/risky_query")
def risky_query(query: str):
    """Run any other SQL queries with no protections"""
    try:
        results = client.query(query).result()
    except Exception as e:
        logger.error(f"An error occured with the query {query}: \n{e}")
    else:
        rows = [dict(row) for row in results]
        time.sleep(0.1)
        logger.info(f"Successfully ran query: {query}")  
        return {
            "table": BQ_TABLE,
            "query": query,
            "row_count": len(rows),
            "rows": rows
        }
    logger.info("End of risky_query")
    

@app.delete("/reset_GCS")
def reset_GCS():
    """Delete all data from GCS"""
    logger.debug("Attempting to delete previous data from GCS")
    try:
        storage_client = storage.Client(GCP_PROJECT_ID)
        logger.debug("Successfully connected to GCS")
        bucket = storage_client.get_bucket(GCP_BUCKET_NAME)
        logger.debug("Successfully connected to GCS bucket")
        blobs = bucket.list_blobs()
        logger.debug("Deleting data, this may take a moment")
        for blob in blobs:
            blob.delete()
    except Exception as e:
        logger.error(f"An error occured: {e}")
    else:
        logger.info("Successfully deleted all data from GCS")
    logger.info("End of reset_GCS")

from src.sales_analysis import report_writer
@app.get("/benchmark_metrics")
def benchmark_metrics():
    try:
        metrics = report_writer.create_benchmark_metrics(
            upload_speed_s=most_recent_upload_time,
            query_access_duration_s=most_recent_query_access_duration,
            query_execution_time_s=most_recent_query_execution_time
        )

        report_writer.write_benchmark_report("benchmark_report.txt", metrics)
        with open("benchmark_report.txt", "r") as file:
            report = file.read()
        return {"report": report}
    except Exception as e:
        logger.error(f"An error occured: {e}")
    logger.info("End of benchmark_metrics")
