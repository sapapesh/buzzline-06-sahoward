# utils/utils_consumer.py

from kafka import KafkaConsumer
import os

def create_kafka_consumer(topic: str, group_id: str) -> KafkaConsumer:
    """
    Creates a Kafka consumer with proper settings.
    Ensures new consumer groups read from the beginning of the topic.
    """
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset='earliest',  # Read from beginning if no committed offsets
        enable_auto_commit=True
    )
    return consumer
