# producer.py
from kafka import KafkaProducer
import json
import os

is_docker = os.getenv('DB_HOST') == 'postgres'
KAFKA_SERVER = 'kafka:29092' if is_docker else 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

data = {
    "title": "Test news",
    "source": "test",
    "url": "http://test.com"
}

producer.send("news-topic", data) 
producer.flush()

print("message envoyé")