import json
import requests
import os
from kafka import KafkaProducer

# --- Configuration ---
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = 'raw_events' 
API_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson"

def fetch_and_produce():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"Fetching data from {API_URL}...")
    try:
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return
    
    if 'features' in data:
        record_count = 0
        for feature in data['features']:
            producer.send(TOPIC_NAME, value=feature)
            record_count += 1
        
        print(f"Successfully produced {record_count} records to topic: {TOPIC_NAME}")
    
    producer.flush()
    producer.close()

if __name__ == '__main__':
    fetch_and_produce()
