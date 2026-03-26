from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from pyarrow import compute as pc
from pyarrow.fs import GcsFileSystem
import pandas as pd
import gcsfs
from dotenv import load_dotenv
from .logger import setup_logger

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

def local_parq_to_gcs(bucket_name: str, root_path: str, gcp_project_id: str):
    files_dir = './src/data'
    dataset =  ds.dataset(files_dir, format='parquet', exclude_invalid_files=True)
    logger.debug(dataset.files)
    table = dataset.to_table()



    # table = table.append_column(
    #     "year_str",
    #     pc.strftime(table["Date"], format="%Y")
    # )

    # table = table.append_column(
    #     "month_str",
    #     pc.strftime(table["Date"], format="%m")
    # )
    fs = gcsfs.GCSFileSystem(project=gcp_project_id, token=None)
    gcs_uri = f'gs://{bucket_name}/{root_path}'

    ds.write_dataset(
        data=table,
        base_dir=gcs_uri,
        filesystem=fs,
        format='parquet',
        partitioning=['year', 'month'],
        existing_data_behavior='overwrite_or_ignore'
        )