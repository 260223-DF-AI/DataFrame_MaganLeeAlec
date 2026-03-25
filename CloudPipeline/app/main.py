#FastAPI app (API endpoints)
from fastapi import FastAPI
from google.cloud import bigquery

app = FastAPI()

@app.get("/")
def run_query():
    client = bigquery.Client()  # <-- NO credentials needed, but can put project = 'projectid'

    query = "SELECT 1 AS test_value"
    rows = client.query(query).result()

    return {"result": [row.test_value for row in rows]}

