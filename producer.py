from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

data = {
    "title": "Test news",
    "source": "test",
    "url": "http://test.com"
}

producer.send("news_topic", data)
producer.flush()

print("message envoyé")