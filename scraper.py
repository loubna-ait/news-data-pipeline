import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
import json
import hashlib
from datetime import datetime

# ======================
# KAFKA PRODUCER
# ======================
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_id(title, url):
    return hashlib.md5((title + url).encode()).hexdigest()

def send_to_kafka(article):
    article["id"] = generate_id(article["title"], article["url"])
    article["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    producer.send("news_topic", article)
    print("📤 Sent:", article["title"])


# ======================
# SCRAPING BBC (exemple simple)
# ======================
def scrape_bbc():
    url = "https://www.bbc.com/news"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")

    articles = []

    for a in soup.find_all("a"):
        title = a.get_text(strip=True)
        link = a.get("href")

        if title and link and "/news" in link:
            if not link.startswith("http"):
                link = "https://www.bbc.com" + link

            articles.append({
                "title": title,
                "source": "BBC",
                "url": link
            })

    return articles


# ======================
# MAIN PIPELINE
# ======================
if __name__ == "__main__":
    print("🚀 START SCRAPING")

    articles = scrape_bbc()

    for article in articles:
        send_to_kafka(article)

    producer.flush()
    print("✅ DONE")