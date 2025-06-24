import random
import datetime
from faker import Faker
from flask import Request
from datetime import datetime
import json
import time

# Import Cloud Libraries
from google.cloud import storage, pubsub_v1

PROJECT_ID = 'gcp-workflows-463821'
RAW_DATA_BUCKET = 'e-commerce-project-raw'
PUBSUB_TOPIC = 'ecommerce-raw-new-files-created'

# GCS 
GCS_client = storage.Client(project=PROJECT_ID)

# PubSub
pubsub_client = pubsub_v1.PublisherClient()
topic_path = pubsub_client.topic_path(PROJECT_ID, PUBSUB_TOPIC)

# Get a random sample of users from a base
BASE_USER_BASE = random.randint(1000,2000)
SAMPLE_SIZE = random.randint(round(BASE_USER_BASE * 0.2), round(BASE_USER_BASE * 0.6))
SAMPLE_OF_USERS = random.sample([u for u in range(1, BASE_USER_BASE)], SAMPLE_SIZE)
NUMBER_OF_ITERATIONS = random.randint(1,5)

# Generate dicts for every user
fake = Faker()
category_prefixes = ['Home', 'Electronics', 'Beauty', 'Health', 'Fashion', 'Sports', 'Books', 'Toys', 'Grocery', 'Automotive']

for iteration in range(NUMBER_OF_ITERATIONS):
    users_data = []
    event_time_stamp = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    event_time_stamp_file_name_format = datetime.today().strftime('%Y-%m-%d-%H-%M-%S')

    for user in SAMPLE_OF_USERS:
        
        data = {
            'user_id' : f'USER_{user}',
            'action': random.choice(['home-page', f'catalog-page-{random.choice(category_prefixes)}', 'check_out', 'confirmation-page']),
            'timestamp': event_time_stamp,
            'details' : {} 
        }

        if data['action'].startswith('catalog-page'):
            data['details'] = {
                'product_id': f'PROD_{fake.uuid4()}',
                'product_name': fake.word().title(),
                'price': round(random.uniform(10, 500), 2)
            }
        elif data['action'] == 'check_out':
            data['details'] = {
                'cart_value': round(random.uniform(20, 1000), 2),
                'num_items': random.randint(1, 10)
            }
        elif data['action'] == 'confirmation-page':
            data['details'] = {
                'order_id': f'ORDER_{fake.uuid4()}',
                'shipping_eta': fake.date_between(start_date="+1d", end_date="+7d").isoformat()
        }
        
        users_data.append(data)

    # Upload JSON file to GCS 
    json_string = "\n".join(json.dumps(record) for record in users_data)
    bucket = GCS_client.bucket(RAW_DATA_BUCKET)
    destination_blob_name = f'raw_data_for_{event_time_stamp_file_name_format}.json'
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(json_string, content_type='application/json')
    print(f"✅ File uploaded to gs://{RAW_DATA_BUCKET}/{destination_blob_name}")

    time.sleep(2)

# Send a message to Pub/SUb to trigger GCP workflow

message_data = {"event": f"new_files_uploaded to {RAW_DATA_BUCKET}"}

future = pubsub_client.publish(topic_path, data=json.dumps(message_data).encode("utf-8"))
print(f"📣 Notified Pub/Sub of new files in {RAW_DATA_BUCKET} (message ID: {future.result()})")



    


