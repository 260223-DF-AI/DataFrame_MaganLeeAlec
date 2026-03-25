from google.cloud import bigquery

YOUR_PROJECT_ID = ""

def main():
    client = bigquery.Client(project=YOUR_PROJECT_ID)

    query = "SELECT 1 AS test_value"
    rows = client.query(query).result()

    for row in rows:
        print("Connected to BigQuery:", row.test_value)

if __name__ == "__main__":
    main()