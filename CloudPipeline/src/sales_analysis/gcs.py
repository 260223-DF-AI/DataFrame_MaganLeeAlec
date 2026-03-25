from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import gcsfs
from dotenv import load_dotenv

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