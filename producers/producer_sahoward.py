""""
producer_sahoward.py

Read a file to be able to study Netflix customer churn.

Example JSON message
{
   "customer_id": "a9b75100-82a8-427a-a208-72f24052884a",
   "age": 51,
   "gender": "Female",
   "subscription_type": "Basic",
   "watch_hours": 14.73,
   "last_login_days": 29,
   "region": "Africa",
   "device": "TV",
   "monthly_fee": 8.99,
   "churned": 1,
   "payment_method": "Gift Card",
   "number_of_profiles": 1,
   "avg_watch_time_per_day": 0.49,
   "favorite_genre": "Action"
}

"""


#####################################
# Import Modules
#####################################

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv
import pathlib

# Import Kafka only if available
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# Import logging utility
from utils.utils_logger import logger

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for Environment Variables
#####################################

def get_message_interval() -> int:
    return int(os.getenv("PROJECT_INTERVAL_SECONDS", 1))

def get_kafka_topic() -> str:
    return os.getenv("PROJECT_TOPIC", "buzzline-topic")

def get_kafka_server() -> str:
    return os.getenv("KAFKA_SERVER", "localhost:9092")

#####################################
# Set up Paths
#####################################

BOOTSTRAP_SERVERS = ['localhost:9092'] 
TOPIC_NAME = 'netflix_customer_data'
CSV_FILE = 'C:/Repos/buzzline-06-sahoward/data/netflix_customer_churn.csv'
STREAMING_DELAY_SECONDS = 0.1 

def json_serializer(data):
    """
    Serializer function to convert Python dictionary to JSON bytes for Kafka.
    """
    return json.dumps(data).encode('utf-8')

def stream_data_to_kafka():
    """
    Reads the CSV data, initializes the Kafka producer, and streams
    customer records to the configured topic.
    """
    try:
        # 1. Load the data using the correct path
        df = pd.read_csv(CSV_FILE)
        records = df.to_dict(orient='records')
        
        print(f"Loaded {len(records)} records from {CSV_FILE}.")

        # 2. Initialize the Kafka Producer
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=json_serializer,
            client_id='netflix_customer_producer' 
        )
        
        print(f"Kafka Producer initialized. Targeting topic: {TOPIC_NAME}")
        
        # 3. Stream records
        for i, record in enumerate(records):
            key = str(record.get('customer_id')).encode('utf-8') 
            
            #Print the JSON message before sending
            print(f"\nSending record {i + 1}:")
            print(json.dumps(record, indent=2))  # Pretty-print JSON

            future = producer.send(
                topic=TOPIC_NAME,
                key=key,
                value=record
            )
            
            if i % 100 == 0:
                print(f"Record {i + 1} sent: Key={key.decode()}")
                
            time.sleep(STREAMING_DELAY_SECONDS)
        
        # 4. Finalize
        producer.flush()
        print("\nAll records have been sent successfully.")
        
    except KafkaError as e:
        print(f"An error occurred with Kafka: {e}")
    except FileNotFoundError:
        print(f"Error: The file '{CSV_FILE}' was not found. Please ensure the file is in the correct directory.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if 'producer' in locals():
            producer.close()
            print("Producer connection closed.")


if __name__ == "__main__":
    stream_data_to_kafka()