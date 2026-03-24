from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
client = bigquery.Client()

for dataset in client.list_datasets():
  print(f"- {dataset.dataset_id}")