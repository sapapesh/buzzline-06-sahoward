from kafka import KafkaConsumer
from kafka.errors import KafkaError
import os
from .utils_logger import logger  # relative import

def create_kafka_consumer(topic: str, group_id: str, bootstrap_servers: str = None) -> KafkaConsumer:
    """
    Create a Kafka consumer that automatically decodes messages from bytes to JSON.
    Returns a KafkaConsumer instance.
    """
    try:
        if not bootstrap_servers:
            bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: m.decode("utf-8"),  # decode bytes to string
        )
        logger.info(f"Kafka consumer created for topic '{topic}', group '{group_id}', server '{bootstrap_servers}'")
        return consumer

    except KafkaError as e:
        logger.error(f"Error creating Kafka consumer: {e}")
        raise
