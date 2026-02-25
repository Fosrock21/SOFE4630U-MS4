from google.cloud import pubsub_v1
import glob
import json
import os
import random
import numpy as np
import time

# Search the current directory for the JSON file
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set the project_id with your project ID
project_id = "steam-collector-485722-i5"
topic_name = "smartmeter"

# create a publisher and get the topic path
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages to {topic_path}.")

# device normal distributions profile used to generate random data
DEVICE_PROFILES = {
    "boston": {'temp': (51.3, 17.7), 'humd': (77.4, 18.7), 'pres': (1.019, 0.091)},
    "denver": {'temp': (49.5, 19.3), 'humd': (33.0, 13.9), 'pres': (1.512, 0.341)},
    "losang": {'temp': (63.9, 11.7), 'humd': (62.8, 21.8), 'pres': (1.215, 0.201)},
}
profileNames = ["boston", "denver", "losang"]

while True:
    profile_name = profileNames[random.randint(0, 2)]
    profile = DEVICE_PROFILES[profile_name]

    # get random values within a normal distribution
    temp = max(0, np.random.normal(profile['temp'][0], profile['temp'][1]))
    humd = max(0, min(np.random.normal(profile['humd'][0], profile['humd'][1]), 100))
    pres = max(0, np.random.normal(profile['pres'][0], profile['pres'][1]))

    # create dictionary
    msg = {
        "time": time.time(),
        "profile_name": profile_name,
        "temperature": temp,
        "humidity": humd,
        "pressure": pres
    }

    # randomly eliminate some measurements (10% chance each)
    if random.randrange(0, 10) < 1:
        msg['temperature'] = None
    if random.randrange(0, 10) < 1:
        msg['humidity'] = None
    if random.randrange(0, 10) < 1:
        msg['pressure'] = None

    record_value = json.dumps(msg).encode('utf-8')

    try:
        # MODIFIED: Added function="raw reading" attribute for Pub/Sub filtering
        future = publisher.publish(topic_path, record_value, function="raw reading")
        future.result()
        print("Published: {}".format(msg))
    except:
        print("Failed to publish the message")

    time.sleep(0.5)
