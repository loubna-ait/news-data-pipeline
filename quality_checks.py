import pandas as pd
import os
import sys

SILVER_FILE = "data_lake/silver/silver_articles.json"

if not os.path.exists(SILVER_FILE):
    print("Silver file not found")
    sys.exit(0)

df = pd.read_json(SILVER_FILE)

if df.empty:
    print("Silver file is empty")
    sys.exit(0)


def check_quality(row):
    if not row["title"] or str(row["title"]).strip() == "":
        return "missing_title"
    if not row["date"] or str(row["date"]).strip() == "":
        return "missing_date"
    if len(str(row["clean_content"])) < 50:
        return "short_content"
    return "ok"

df["quality_flag"] = df.apply(check_quality, axis=1)
df.to_json("data_lake/silver/silver_with_quality.json", orient="records", force_ascii=False)

print("Quality OK")
print(df["quality_flag"].value_counts().to_string())