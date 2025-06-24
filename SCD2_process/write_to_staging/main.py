from flask import Request
import json

# Import Cloud Libraries
from google.cloud import storage


PROJECT_ID = 'gcp-workflows-463821'
RAW_DATA_BUCKET = 'e-commerce-project-raw'

GCS_client = storage.Client(project=PROJECT_ID)

def read_json_file(request:Request):

    try:
        file_name = request.args.get("file_name")
        bucket = GCS_client.bucket(RAW_DATA_BUCKET)
        blob = bucket.blob(file_name)
        json_string = blob.download_as_text(encoding="utf-8")
        records = [json.loads(line) for line in json_string.strip().split('\n')]
        print(records[0:3])
        return 200, f"Succesfully read file {file_name}"
    except Exception as e:
        return 500, f"An error occurred while reading file {file_name}: {e}"