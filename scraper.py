from kafka import KafkaProducer
import requests
from bs4 import BeautifulSoup
import json
import hashlib
from datetime import datetime

# ---------------- Producer Kafka ----------------
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ---------------- Utils ----------------
def generate_id(text):
    return hashlib.md5(text.encode()).hexdigest()

def send_to_kafka(data):
    producer.send("news-topic", data)
    print(f"📤 Sent: {data['source']}")

# ---------------- BBC SCRAPER ----------------
def scrape_bbc():
    url = "https://www.bbc.com/news"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")

    for article in soup.find_all("h3"):
        title = article.get_text(strip=True)

        if title:
            send_to_kafka({
                "id": generate_id(title),
                "title": title,
                "source": "BBC",
                "url": url,
                "scraped_at": str(datetime.now())
            })

# ---------------- HESPRESS SCRAPER ----------------
def scrape_hespress():
    url = "https://www.hespress.com/"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")

    seen = set()

    for tag in soup.select("h2, h3"):
        title = " ".join(tag.get_text(strip=True).split())

        if title and len(title) > 10 and title not in seen:
            seen.add(title)

            send_to_kafka({
                "id": generate_id(title),
                "title": title,
                "source": "Hespress",
                "url": url,
                "scraped_at": str(datetime.now())
            })

# ---------------- MAIN ----------------
if __name__ == "__main__":
    print("🚀 START SCRAPING")

    scrape_bbc()
    scrape_hespress()

    producer.flush()
    producer.close()

    print("✅ DONE")