import json
import glob
import os
import sys
import time

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = glob.glob("*.json")[0]

from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

# Configuration from environment variables
project_id = os.environ.get("GCP_PROJECT", "steam-collector-485722-i5")
subscription_id = os.environ.get("FILTER_SUB_ID", "filter-reading-sub")
topic_name = os.environ.get("TOPIC_NAME", "smartmeter")

# Create publisher for forwarding valid messages
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

def callback(message):
    """Filter out readings that contain None values."""
    try:
        data = json.loads(message.data.decode("utf-8"))

        # Check if temperature, humidity, or pressure is None
        if data.get("temperature") is None or data.get("humidity") is None or data.get("pressure") is None:
            print(f"FILTERED OUT (contains None): profile={data.get('profile_name')}, "
                  f"temp={data.get('temperature')}, humd={data.get('humidity')}, pres={data.get('pressure')}")
            message.ack()
            return

        # Forward valid reading with attribute function="filtered reading"
        message_data = json.dumps(data).encode("utf-8")
        future = publisher.publish(
            topic_path,
            message_data,
            function="filtered reading"
        )
        future.result()
        print(f"PASSED: profile={data.get('profile_name')}, "
              f"temp={data.get('temperature'):.2f}, humd={data.get('humidity'):.2f}, pres={data.get('pressure'):.3f}")
        message.ack()

    except Exception as e:
        print(f"Error processing message: {e}")
        message.ack()

# Create subscription with filter
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
filter_str = 'attributes.function = "raw reading"'

try:
    topic_full = f"projects/{project_id}/topics/{topic_name}"
    subscriber.create_subscription(
        request={
            "name": subscription_path,
            "topic": topic_full,
            "filter": filter_str
        }
    )
    print(f"Created subscription: {subscription_path}")
except AlreadyExists:
    print(f"Subscription already exists: {subscription_path}")
except Exception as e:
    print(f"Subscription note: {e}")

# Start listening
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"FilterReading service started.")
print(f"Listening for messages on {subscription_path}..\n")

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    streaming_pull_future.result()
    print("FilterReading service stopped.")
