from google.cloud import storage
from dotenv import load_dotenv
import os

load_dotenv()
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

def create_bucket(bucket_name: str, location : str ="US") -> None:
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    new_bucket = storage_client.create_bucket(bucket, location=location)

create_bucket("sales-data-mla")

