import os
import json
import pandas as pd
from tqdm import tqdm

STOCK_PATH = "/app/outputs/stock_data"
NEWS_PATH = "/app/outputs/news"
COMPANY_PATH = "/app/outputs/company_data"
OUTPUT_PATH = "/app/outputs/merged_source"

os.makedirs(OUTPUT_PATH, exist_ok=True)

def save_doc(company, filename, data):
    folder = os.path.join(OUTPUT_PATH, company)
    os.makedirs(folder, exist_ok=True)

    out_path = os.path.join(folder, filename)
    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)


def process_stock():
    print("Processing stock data...")
    for file in tqdm(os.listdir(STOCK_PATH)):
        if not file.endswith(".csv"):
            continue

        company = file.replace(".csv", "")
        df = pd.read_csv(os.path.join(STOCK_PATH, file))

        for _, row in df.iterrows():
            doc = {
                "company": company,
                "type": "stock",
                "date": row.get("date"),
                "text": (
                    f"On {row.get('date')}, {company} stock closed at "
                    f"{row.get('close')} with a high of {row.get('high')} "
                    f"and a low of {row.get('low')}. "
                )
            }

            filename = f"stock_{row.get('date')}.json"
            save_doc(company, filename, doc)


def process_news():
    print("Processing news data...")
    for file in tqdm(os.listdir(NEWS_PATH)):
        if not file.endswith(".json"):
            continue

        company = file.replace(".json", "")

        with open(os.path.join(NEWS_PATH, file)) as f:
            articles = json.load(f)

        for idx, art in enumerate(articles):
            doc = {
                "company": company,
                "type": "news",
                "date": art.get("date"),
                "source": art.get("source"),
                "title": art.get("title"),
                "text": art.get("content"),
            }

            filename = f"news_{idx+1}.json"
            save_doc(company, filename, doc)


def process_fundamentals():
    print("Processing fundamentals...")
    for file in tqdm(os.listdir(COMPANY_PATH)):
        if not file.endswith(".csv"):
            continue

        company = file.replace(".csv", "")
        df = pd.read_csv(os.path.join(COMPANY_PATH, file))

        # convert entire table to paragraph
        text = df.to_string(index=False)

        doc = {
            "company": company,
            "type": "fundamentals",
            "text": f"Financial summary for {company}:\n{text}"
        }

        filename = "fundamentals.json"
        save_doc(company, filename, doc)


if __name__ == "__main__":
    process_stock()
    process_news()
    process_fundamentals()
    print("✓ Merging completed.")
