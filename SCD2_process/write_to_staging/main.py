from flask import Request
import json

# Import Cloud Libraries
from google.cloud import storage, bigquery


PROJECT_ID = 'gcp-workflows-463821'
RAW_DATA_BUCKET = 'e-commerce-project-raw'

DATA_SET = 'ecommerce_data'
TABLE_NAME = 'ecommerce_staging_table'

GCS_client = storage.Client(project=PROJECT_ID)
BQ_client = bigquery.Client(project=PROJECT_ID)

def big_query_write(project_id, dataset, table, BQ_client, records:list):

    table_id = f'{project_id}.{dataset}.{table}'

    try:
        # Delete data in stagin table for an proper overwrite
        query = f"TRUNCATE TABLE `{table_id}`"
        BQ_client.query(query).result()

        errors = BQ_client.insert_rows_json(table_id, records)

        print(f'Succesfully wrote data in {table_id}')
        
        if errors:
            print("❌ Errors occurred while inserting:", errors)    
        else:
            print("✅ All rows inserted successfully.")

    except Exception as e:
       print( f"❌ Error writing to BigQuery: {e}")


def read_json_file(request:Request):

    try:
        # Try to get file_name from query string
        file_name = request.args.get("file_name")
        # If not found, try to get it from JSON body
        if not file_name:
            data = request.get_json(silent=True)
            file_name = data.get("file_name") if data else None
        if not file_name:
            return "Missing 'file_name' parameter in query or body", 400
        
        bucket = GCS_client.bucket(RAW_DATA_BUCKET)
        blob = bucket.blob(file_name)
        json_string = blob.download_as_text(encoding="utf-8")
        
        records = [json.loads(line) for line in json_string.strip().split('\n')]
        
        # Change dictionary to a string

        for d in records:
            for key in d.keys():
                if key == 'details':
                    d[key] = str(d[key])
                else:
                    pass
        # Write to Staging Table
        big_query_write(PROJECT_ID, DATA_SET, TABLE_NAME, BQ_client=BQ_client, records=records)

        return  f"Succesfully read file {file_name} and wrote to Staging Table f'{PROJECT_ID}.{DATA_SET}.{TABLE_NAME}'", 200
    
    except Exception as e:
        return f"An error occurred while reading file {file_name}: {e}", 500