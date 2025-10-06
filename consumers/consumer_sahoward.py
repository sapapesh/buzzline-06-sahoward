"""consumer_sahoward.py"""

import os
import json
import threading
from collections import defaultdict
import matplotlib
matplotlib.use("TkAgg")  # for interactive plotting
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

load_dotenv()

# Shared data with thread lock
subscription_type_avg = defaultdict(lambda: {"sum": 0.0, "count": 0})
lock = threading.Lock()

def get_kafka_topic():
    return os.getenv("PROJECT_TOPIC", "netflix_customer_data")

def get_kafka_consumer_group_id():
    return os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")

# Kafka consumer thread
def kafka_consumer_thread(consumer):
    logger.info("Kafka consumer thread started.")
    try:
        for message_str in consumer:
            try:
                data = json.loads(message_str)
                logger.info(f"Received message: {data}")  # log every message

                sub_type = data.get("subscription_type", "unknown")
                watch_hours = float(data.get("watch_hours", 0.0))

                with lock:
                    subscription_type_avg[sub_type]["sum"] += watch_hours
                    subscription_type_avg[sub_type]["count"] += 1
                    avg = subscription_type_avg[sub_type]["sum"] / subscription_type_avg[sub_type]["count"]
                    logger.info(f"{sub_type}: avg watch hours = {avg:.2f}")

            except Exception as e:
                logger.error(f"Error processing message: {e}")
    except Exception as e:
        logger.error(f"Kafka consumer thread error: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer thread ended.")

def main():
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    consumer = create_kafka_consumer(topic, group_id)

    # Start consumer thread
    thread = threading.Thread(target=kafka_consumer_thread, args=(consumer,), daemon=True)
    thread.start()

    # Setup live chart
    plt.ion()
    fig, ax = plt.subplots(figsize=(8,5))
    plt.tight_layout()
    plt.show(block=False)

    def update_chart(frame):
        ax.clear()
        with lock:
            sub_types = list(subscription_type_avg.keys())
            avg_hours = [
                subscription_type_avg[s]["sum"]/subscription_type_avg[s]["count"]
                if subscription_type_avg[s]["count"] > 0 else 0
                for s in sub_types
            ]
        ax.bar(range(len(sub_types)), avg_hours, color="blue", edgecolor="black")
        ax.set_xticks(range(len(sub_types)))
        ax.set_xticklabels(sub_types, rotation=45, ha="right")
        ax.set_ylim(0, max(avg_hours+[10]))
        ax.set_xlabel("Subscription Type")
        ax.set_ylabel("Average Watch Hours")
        ax.set_title("Real-Time Average Watch Hours Per Subscription Type")
        plt.tight_layout()

    ani = FuncAnimation(fig, update_chart, interval=1000, cache_frame_data=False)

    try:
        plt.show()
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    finally:
        consumer.close()
        plt.ioff()
        plt.show()
        logger.info("END consumer.")

if __name__ == "__main__":
    main()
