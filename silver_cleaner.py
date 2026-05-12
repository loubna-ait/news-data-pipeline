import json
import pandas as pd
from bs4 import BeautifulSoup
from langdetect import detect
import os

BRONZE_FILE = "data_lake/bronze/articles.json"
SILVER_PATH = "data_lake/silver/"
os.makedirs(SILVER_PATH, exist_ok=True)

def clean_html(text):
    return BeautifulSoup(text, "html.parser").get_text() if text else ""

def normalize(text):
    return " ".join(text.lower().split())

def detect_lang(text):
    try:
        return detect(text) if text and len(text) > 20 else "unknown"
    except:
        return "unknown"

if not os.path.exists(BRONZE_FILE):
    print("Fichier bronze introuvable. need scraper")
    exit()

with open(BRONZE_FILE, "r", encoding="utf-8") as f:
    articles = json.load(f)

cleaned = []
for article in articles:
    title = article.get("title", "")
    content = article.get("content", "")
    text_to_clean = (title + " " + content).strip()
    clean_content = normalize(clean_html(text_to_clean))
    cleaned.append({
        "title":         article.get("title", ""),
        "source":        article.get("source", ""),
        "url":           article.get("url", ""),
        "date":          article.get("date", article.get("ingested_at", "")),
        "author":        article.get("author", ""),
        "category":      article.get("category", ""),
        "clean_content": clean_content,
        "language": detect_lang(clean_content or article.get("title", ""))
    })

df = pd.DataFrame(cleaned)
df.to_json(SILVER_PATH + "silver_articles.json", orient="records", force_ascii=False)
print(f"Silver OK — {len(df)} articles nettoyés")