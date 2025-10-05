import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# --- Configuration ---
# Must match the producer configuration
BOOTSTRAP_SERVERS = ['localhost:9092'] 
TOPIC_NAME = 'netflix_customer_data'

def json_deserializer(m_bytes):
    """
    Deserializer function to convert JSON bytes from Kafka back into a Python dictionary.
    """
    if m_bytes is None:
        return None
    try:
        return json.loads(m_bytes.decode('utf-8'))
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None

def consume_data_from_kafka():
    """
    Initializes the Kafka consumer and processes messages from the topic.
    """
    try:
        # 1. Initialize the Kafka Consumer
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest', # Start reading at the earliest message if no offset is committed
            enable_auto_commit=True,      # Automatically commit offsets
            group_id='netflix-data-processor-group', # A unique ID for this consumer group
            value_deserializer=json_deserializer,
            key_deserializer=lambda m_bytes: m_bytes.decode('utf-8') if m_bytes else None
        )
        
        print(f"Kafka Consumer initialized. Listening to topic: {TOPIC_NAME}...")
        print("Press Ctrl+C to stop the consumer.")

        # 2. Start consuming messages
        for message in consumer:
            # The message value is already a Python dictionary because of json_deserializer
            customer_record = message.value
            key = message.key
            
            if customer_record:
                # You can now process the data here. For example, check for churned customers:
                
                churn_status = "CHURNED" if customer_record.get('churned') == 1 else "ACTIVE"
                
                print("-" * 50)
                print(f"Key: {key}, Partition: {message.partition}, Offset: {message.offset}")
                print(f"Customer ID: {customer_record.get('customer_id')}")
                print(f"Age: {customer_record.get('age')}, Subscription: {customer_record.get('subscription_type')}")
                print(f"Status: **{churn_status}**")
        
    except KafkaError as e:
        print(f"An error occurred with Kafka: {e}")
    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # 3. Close the consumer connection
        if 'consumer' in locals():
            consumer.close()
            print("Consumer connection closed.")

if __name__ == "__main__":
    consume_data_from_kafka()