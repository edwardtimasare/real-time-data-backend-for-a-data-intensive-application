
import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for Airflow
default_args = {
    'owner': 'edward',
    'start_date': datetime(2024, 8, 1, 9, 00)
}

# Fetch random user data from the randomuser.me API
def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    return res['results'][0]

# Format the user data to include e-commerce details
def format_data(res):
    import random

    # Predefined product list and categories
    products = [
        {"name": "Laptop", "category": "Electronics", "price_range": (500, 1500)},
        {"name": "Shoes", "category": "Apparel", "price_range": (50, 200)},
        {"name": "Smartphone", "category": "Electronics", "price_range": (300, 1000)},
    ]
    product = random.choice(products)

    # Format e-commerce transaction data
    data = {}
    location = res['location']
    data['order_id'] = str(uuid.uuid4())  # Unique order ID
    data['product_name'] = product['name']
    data['category'] = product['category']
    data['price'] = round(random.uniform(*product['price_range']), 2)
    data['quantity'] = random.randint(1, 5)
    data['total_amount'] = round(data['price'] * data['quantity'], 2)
    data['timestamp'] = datetime.now().isoformat()
    data['customer_name'] = f"{res['name']['first']} {res['name']['last']}"
    data['purchase_location'] = f"{location['city']}, {location['state']}, {location['country']}"

    return data

# Stream e-commerce transactions to Kafka
def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:  # Run for 1 minute
            break
        try:
            res = get_data()  # Fetch user data
            transaction = format_data(res)  # Generate e-commerce transaction

            # Send data to Kafka
            producer.send('orders_created', json.dumps(transaction).encode('utf-8'))
            logging.info(f"Sent transaction: {transaction}")
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue

# Define the Airflow DAG
with DAG('ecommerce_transaction_streaming',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_ecommerce_transactions',
        python_callable=stream_data
    )
