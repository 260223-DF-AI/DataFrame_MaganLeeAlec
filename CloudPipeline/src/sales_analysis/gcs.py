from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pandas as pd
import gcsfs
import os
from dotenv import load_dotenv
from .logger import setup_logger
import zlib
import uuid

logger = setup_logger(__name__, 'debug')

def upload_dir_to_gcs(df: pd.DataFrame, bucket_name: str, root_path: str, gcp_project_id: str):
    """Accepts a data frame, bucket name, root path inside the bucket, and the project id.
        This function will partition data by date and send to GCS"""
        
    # Partition the files by using Date column
    # Make a temporary column in the data frame for Year and Month from the Date column
    df['Year'] = df['Date'].dt.year
    df['Year'] = df['Year'].astype(str).str.zfill(4)
    df['Month'] = df['Date'].dt.month
    df['Month'] = df['Month'].astype(str).str.zfill(2)

    # Create a pyarrow table to pass into Pyarrow parquet function
    table = pa.Table.from_pandas(df)
    
    # Establish the file system using the project id. token=None will find the credentials automatically
    fs = gcsfs.GCSFileSystem(project=gcp_project_id, token=None)
    
    # Set the URI by using bucket and root
    gcs_uri = f'gs://{bucket_name}/{root_path}'
    
    # Use Pyarrow.Parquet to write the table to parquet format, partitioning by date
    pq.write_to_dataset(
        table,
        root_path=gcs_uri,
        filesystem=fs,
        partition_cols=['Year', 'Month'],
        compression='snappy'
    )
    
    return f"Data converted to Parquet and sent to {gcs_uri}"

def write_local_partition():
    # Look in this root directory for the parquet files
    files_dir = './src/data'
    
    # Load the parquet files into a pyarrow dataset
    local_dataset = ds.dataset(files_dir, format='parquet', exclude_invalid_files=True, ignore_prefixes=['stg'])
    logger.debug(local_dataset.files)
    
    # Create a pyarrow table to load into write_to_dataset funciton
    table = local_dataset.to_table()
    basename_template = f'part_{{i}}_{uuid.uuid4().hex}.parquet'
    pq.write_to_dataset(
        table,
        root_path='src/data/stg_sales',
        partition_cols=['Year', 'Month'],
        compression='snappy',
        existing_data_behavior='delete_matching',
        basename_template=basename_template
    )

def upload_parquet_if_not_exists(bucket_name, bucket, source_file_path, destination_blob_name):
    """
    Uploads a local parquet file to a GCS bucket only if the destination blob 
    does not already exist.

    :param bucket_name: Name of the GCS bucket (e.g., 'my-awesome-bucket')
    :param source_file_path: Local path to the file to upload (e.g., './data/file.parquet')
    :param destination_blob_name: The full path for the blob in GCS 
                                  (e.g., 'partitioned_data/date=2024-01-01/file.parquet')
    """
    
    blob = bucket.blob(str(destination_blob_name))
    
    if blob is None:
        print(f"Blob {destination_blob_name} already exists in bucket {bucket_name}. Skipping upload.")
    else:
        # Optional: set a generation-match precondition to avoid race conditions
        # if other processes might upload concurrently.
        # generation_match_precondition = 0 # for a new object
        
        blob.upload_from_filename(source_file_path)
        print(f"File {source_file_path} uploaded to gs://{bucket_name}/{destination_blob_name}.")