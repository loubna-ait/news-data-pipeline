import pandas as pd
import os
from collections import Counter

os.makedirs("data_lake/gold", exist_ok=True)

df = pd.read_json("data_lake/silver/silver_with_quality.json")

# ── Stopwords multilingues ─────────────────────────────────────────
stopwords = {
    "the","and","for","that","with","this","from","are","was","have",
    "been","they","their","will","its","but","not","also","more","has",
    "le","la","les","des","une","dans","que","qui","pour","par","sur",
    "est","sont","avec","plus","tout","bien","aussi","comme","aux","du",
    "من","على","في","الى","عن","هذا","هذه","التي","الذي","كان","وقد"
}

# ── 1. Articles par source ─────────────────────────────────────────
articles_by_source = df.groupby("source").size().reset_index(name="total")
articles_by_source.to_csv("data_lake/gold/articles_by_source.csv", index=False)

# ── 2. Articles par date ───────────────────────────────────────────
articles_by_date = df.groupby("date").size().reset_index(name="total")
articles_by_date.to_csv("data_lake/gold/articles_by_date.csv", index=False)

# ── 3. Articles par langue ─────────────────────────────────────────
articles_by_language = df.groupby("language").size().reset_index(name="total")
articles_by_language.to_csv("data_lake/gold/articles_by_language.csv", index=False)

# ── 4. Articles par catégorie ──────────────────────────────────────
if "category" in df.columns:
    articles_by_category = df[df["category"].str.strip() != ""] \
        .groupby("category").size().reset_index(name="total") \
        .sort_values("total", ascending=False)
    articles_by_category.to_csv("data_lake/gold/articles_by_category.csv", index=False)

# ── 5. Articles par auteur (top 10) ───────────────────────────────
if "author" in df.columns:
    articles_by_author = df[df["author"].str.strip() != ""] \
        .groupby("author").size().reset_index(name="total") \
        .sort_values("total", ascending=False).head(10)
    articles_by_author.to_csv("data_lake/gold/articles_by_author.csv", index=False)

# ── 8. Top mots clés (avec stopwords) ─────────────────────────────
words = [
    w.lower() for w in " ".join(df["clean_content"].fillna("")).split()
    if w.lower() not in stopwords and len(w) > 3
]
top_words = Counter(words).most_common(20)
top_words_df = pd.DataFrame(top_words, columns=["word", "count"])
top_words_df.to_csv("data_lake/gold/top_words.csv", index=False)

print("Gold OK")
print(articles_by_source.to_string(index=False))