# consumer_chart.py

import os
import json
import threading
from collections import defaultdict
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
import time
import numpy as np

load_dotenv()

# ----------------------------
# Shared data
# ----------------------------
subscription_type_avg = defaultdict(lambda: {"sum": 0.0, "count": 0})
lock = threading.Lock()

# ----------------------------
# Kafka topic and group
# ----------------------------
def get_kafka_topic():
    return os.getenv("PROJECT_TOPIC", "netflix_customer_data")

def get_kafka_consumer_group_id():
    # Unique group ID for this chart to read all messages independently
    return os.getenv("BUZZ_CONSUMER_GROUP_ID", f"chart_group_{int(time.time())}")

# ----------------------------
# Kafka consumer thread
# ----------------------------
def kafka_consumer_thread(consumer):
    logger.info("Kafka consumer thread started.")
    try:
        for message in consumer:
            try:
                data = json.loads(message.value)

                sub_type = data.get("subscription_type", "unknown")
                watch_hours = float(data.get("watch_hours", 0.0))
                churn_raw = data.get("churned", 0)
                churned = int(churn_raw) if str(churn_raw).strip() in ["0", "1"] else 0

                key = (sub_type, churned)

                with lock:
                    subscription_type_avg[key]["sum"] += watch_hours
                    subscription_type_avg[key]["count"] += 1
                    avg = subscription_type_avg[key]["sum"] / subscription_type_avg[key]["count"]
                    logger.info(f"Processed: {sub_type} | {'Churned' if churned else 'Not Churned'} -> avg={avg:.2f}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    except Exception as e:
        logger.error(f"Kafka consumer thread error: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer thread ended.")

# ----------------------------
# Chart update
# ----------------------------
def update_chart(frame, ax):
    ax.clear()
    with lock:
        if not subscription_type_avg:
            ax.set_title("Waiting for Kafka data...")
            ax.set_xlabel("Subscription Type")
            ax.set_ylabel("Average Watch Hours")
            return

        # Step 1: get unique subscription types
        subscription_types = sorted(set(sub_type for (sub_type, _) in subscription_type_avg.keys()))
        churn_status = [0, 1]  # 0 = Not churned, 1 = Churned
        width = 0.35  # width of each bar

        labels = subscription_types
        x = np.arange(len(labels))  # the label locations

        # Step 2: prepare averages for each churn status
        averages_churned = []
        averages_not_churned = []

        for sub_type in labels:
            stats_not_churned = subscription_type_avg.get((sub_type, 0), {"sum":0, "count":0})
            avg_not = stats_not_churned["sum"]/stats_not_churned["count"] if stats_not_churned["count"]>0 else 0
            averages_not_churned.append(avg_not)

            stats_churned = subscription_type_avg.get((sub_type, 1), {"sum":0, "count":0})
            avg_ch = stats_churned["sum"]/stats_churned["count"] if stats_churned["count"]>0 else 0
            averages_churned.append(avg_ch)

        # Step 3: plot grouped bars
        ax.bar(x - width/2, averages_not_churned, width, color="blue", label="Not Churned")
        ax.bar(x + width/2, averages_churned, width, color="red", label="Churned")

        # Step 4: labels and formatting
        ax.set_xlabel("Subscription Type")
        ax.set_ylabel("Average Watch Hours")
        ax.set_title("Real-Time Average Watch Hours per Subscription Type & Churn")
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.set_ylim(0, max(averages_churned + averages_not_churned + [10]))
        ax.legend()

# ----------------------------
# Main
# ----------------------------
def main():
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    consumer = create_kafka_consumer(topic, group_id)

    # Start consumer thread
    thread = threading.Thread(target=kafka_consumer_thread, args=(consumer,), daemon=True)
    thread.start()

    # Live chart
    fig, ax = plt.subplots(figsize=(10, 6))
    plt.tight_layout()
    ani = FuncAnimation(fig, lambda frame: update_chart(frame, ax), interval=1000, cache_frame_data=False)

    try:
        plt.show()
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    finally:
        consumer.close()
        plt.close(fig)
        logger.info("Consumer ended.")

if __name__ == "__main__":
    main()
