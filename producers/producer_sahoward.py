import pandas as pd
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

# --- Configuration ---
# Update these with your Kafka broker and topic details
BOOTSTRAP_SERVERS = ['localhost:9092'] 
TOPIC_NAME = 'netflix_customer_data'
CSV_FILE = 'netflix_customer_churn.csv'
STREAMING_DELAY_SECONDS = 0.1 # Pause between sending each record (0.1s = 10 messages/second)

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
        # 1. Load the data
        df = pd.read_csv(CSV_FILE)
        
        # Convert DataFrame to a list of dictionaries (one dictionary per row)
        records = df.to_dict(orient='records')
        
        print(f"Loaded {len(records)} records from {CSV_FILE}.")

        # 2. Initialize the Kafka Producer
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=json_serializer,
            # Optional: Add a client ID for easier tracking
            client_id='netflix_customer_producer' 
        )
        
        print(f"Kafka Producer initialized. Targeting topic: {TOPIC_NAME}")
        
        # 3. Stream records
        for i, record in enumerate(records):
            # Use 'customer_id' as the Kafka key for partitioning/ordering
            key = str(record.get('customer_id')).encode('utf-8') 
            
            # Asynchronously send the record
            future = producer.send(
                topic=TOPIC_NAME,
                key=key,
                value=record
            )
            
            # Optional: Check for send success/failure after a short delay
            if i % 100 == 0:
                print(f"Record {i + 1} sent: Key={key.decode()}")
                
            time.sleep(STREAMING_DELAY_SECONDS)
        
        # 4. Wait for all outstanding messages to be delivered and close the producer
        producer.flush()
        print("\nAll records have been sent successfully.")
        
    except KafkaError as e:
        print(f"An error occurred with Kafka: {e}")
    except FileNotFoundError:
        print(f"Error: The file '{CSV_FILE}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Ensure the producer is closed if it was initialized
        if 'producer' in locals():
            producer.close()
            print("Producer connection closed.")


if __name__ == "__main__":
    stream_data_to_kafka()