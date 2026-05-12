from kafka import KafkaConsumer
import json
import os
from datetime import datetime

# ---------------- MinIO ----------------
from minio import Minio
from io import BytesIO

is_docker = os.getenv('DB_HOST') == 'postgres'
KAFKA_SERVER = 'kafka:29092' if is_docker else 'localhost:9092'
MINIO_SERVER = 'minio:9000' if is_docker else 'localhost:9000'

client = Minio(
    MINIO_SERVER,
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

BUCKET = "news-bronze"

# ---------------- Kafka Consumer ----------------
consumer = KafkaConsumer(
    'news-topic',
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# ---------------- LOCAL BACKUP (optionnel) ----------------
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

# ---------------- MINIO UPLOAD ----------------
def upload_to_minio(article):
    data = json.dumps(article).encode("utf-8")
    file_name = f"{article['id']}.json"

    client.put_object(
        BUCKET,
        file_name,
        data=BytesIO(data),
        length=len(data),
        content_type="application/json"
    )

    print("📦 Stored in MinIO:", file_name)

# ---------------- MAIN LOOP ----------------
print("🚀 Consumer started...")

for msg in consumer:
    article = msg.value

    # timestamp ingestion
    article["ingested_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # DEBUG
    print("📥 message reçu :", article)

    # 1. Save local file (backup)
    data = load_data()
    data.append(article)
    save_data(data)

    # 2. Save in MinIO (DATA LAKE)
    upload_to_minio(article)

    print("✅ Saved:", article.get("title", "NO TITLE"))