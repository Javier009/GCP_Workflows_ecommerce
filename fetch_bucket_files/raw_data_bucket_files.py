from google.cloud import storage

BUCKET_NAME = 'e-commerce-project-raw'

def list_gcs_files(bucket_name, prefix=None):
    files = []
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        files.append(f'gs://{BUCKET_NAME}/{blob.name}')
    
    return files