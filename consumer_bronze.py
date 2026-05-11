from kafka import KafkaConsumer
import json
import os
from datetime import datetime

consumer = KafkaConsumer(
    'news_topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
)

BRONZE_PATH = "data_lake/bronze/articles.json"

os.makedirs("data_lake/bronze", exist_ok=True)

def load_data():
    if os.path.exists(BRONZE_PATH):
        with open(BRONZE_PATH, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except:
                return []
    return []

def save_data(data):
    with open(BRONZE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

print("🚀 Consumer started...")

for msg in consumer:
    article = msg.value

    article["ingested_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    data = load_data()
    data.append(article)

    save_data(data)

    print("📥 Saved:", article.get("title", "NO TITLE"))