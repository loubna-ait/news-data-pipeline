from kafka import KafkaConsumer
import json
import os
from datetime import datetime

# ---------------- Kafka Consumer ----------------
consumer = KafkaConsumer(
    'news-topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# ---------------- Storage ----------------
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

# ---------------- LOOP ----------------
print("🚀 Consumer started...")

for msg in consumer:
    article = msg.value

    # ✅ NORMALISATION (IMPORTANT)
    normalized_article = {
        "title": article.get("title", ""),
        "author": "Unknown",
        "date": article.get("scraped_at", ""),
        "category": "General",
        "content": article.get("content", ""),
        "source": article.get("source", ""),
        "url": article.get("url", ""),
        "ingested_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    }

    # Load + Save
    data = load_data()
    data.append(normalized_article)
    save_data(data)

    print("📥 Saved:", normalized_article["title"])