import paho.mqtt.client as mqtt
import json
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from zoneinfo import ZoneInfo

import os

MQTT_BROKER = os.environ.get("MQTT_BROKER")
MQTT_PORT = os.environ.get("MQTT_PORT")
MQTT_USER = os.environ.get("MQTT_USER")
MQTT_PASS = os.environ.get("MQTT_PASS")
MQTT_TOPIC = os.environ.get("MQTT_TOPIC")

MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = os.environ.get("DB_NAME")
COLLECTION_NAME = os.environ.get("COLLECTION_NAME")

print("DB_NAME =", DB_NAME)

# --- MongoDB client ---
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]

collection.create_index([("sensorId", 1), ("timestamp", 1)], unique=True)

# --- MQTT Callbacks ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to HiveMQ Cloud")
        client.subscribe(MQTT_TOPIC)
        print(f"📡 Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"❌ Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)

        # Ensure timestamp is in datetime format
        if "timestamp" in data:
            try:
                data["timestamp"] = datetime.fromisoformat(
                    data["timestamp"].replace("Z", "+00:00")
                )

                local_time = data["timestamp"].astimezone(ZoneInfo("Europe/Amsterdam"))

                data["timestamp_local"] = local_time.isoformat()

                
            except Exception:
                pass

        # Insert into MongoDB
        try:
            collection.insert_one(data)
            print(f"💾 Saved to MongoDB: {data}")
        except DuplicateKeyError:
            print("Duplicate message, skipping")
        

    except Exception as e:
        print(f"⚠️ Error processing message: {e}")

# --- MQTT client setup ---
client = mqtt.Client()
client.username_pw_set(MQTT_USER, MQTT_PASS)

# Use TLS (without certificate validation)
client.tls_set()          # enables TLS
client.tls_insecure_set(True)  # skip certificate verification for testing

client.on_connect = on_connect
client.on_message = on_message

# Connect and loop forever
client.connect(MQTT_BROKER, MQTT_PORT)
client.loop_forever()
