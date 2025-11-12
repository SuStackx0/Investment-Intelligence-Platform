import os
import json
import pandas as pd
from datetime import datetime

BASE = "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/services/data_ingestion/outputs"
STOCK_PATH = os.path.join(BASE, "stock_data")
NEWS_PATH = os.path.join(BASE, "news")
COMPANY_PATH = os.path.join(BASE, "company_data")
OUTPUT_PATH = os.path.join(BASE, "merged_source")  # final jsons
os.makedirs(OUTPUT_PATH, exist_ok=True)

def load_existing_json(company):
    """Load existing JSON if it exists, else return empty structure."""
    file_path = os.path.join(OUTPUT_PATH, f"{company}.json")
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "company": company,
        "last_updated": None,
        "data": {"stock": [], "news": [], "fundamentals": {}}
    }

def save_json(company_data):
    """Write updated company JSON."""
    company = company_data["company"]
    company_data["last_updated"] = datetime.now().isoformat()
    out_path = os.path.join(OUTPUT_PATH, f"{company}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(company_data, f, indent=2, ensure_ascii=False)
    print(f"💾 Saved {out_path}")


def load_stock_data():
    stock_entries = {}
    for file in os.listdir(STOCK_PATH):
        if not file.endswith(".csv"): continue
        company = file.replace(".csv", "")
        df = pd.read_csv(os.path.join(STOCK_PATH, file))
        if "Date" not in df.columns or "Close" not in df.columns:
            continue

        entries = []
        for _, row in df.iterrows():
            entries.append({
                "date": str(row["Date"]),
                "close": row.get("Close"),
                "open": row.get("Open"),
                "high": row.get("High"),
                "low": row.get("Low"),
                "volume": row.get("Volume"),
                "summary": f"On {row['Date']}, {company} closed at ₹{row['Close']:.2f} "
                           f"(Open: ₹{row.get('Open', 'NA')}, High: ₹{row.get('High', 'NA')}, "
                           f"Low: ₹{row.get('Low', 'NA')}, Volume: {row.get('Volume', 'NA')})."
            })
        stock_entries[company] = entries
    return stock_entries

def load_news_data():
    news_entries = {}
    for file in os.listdir(NEWS_PATH):
        if not file.endswith(".json"): continue
        company = file.replace(".json", "")
        with open(os.path.join(NEWS_PATH, file), "r", encoding="utf-8") as f:
            articles = json.load(f)

        entries = []
        for art in articles:
            entries.append({
                "date": art.get("published", ""),
                "title": art.get("title", ""),
                "desc": art.get("desc", ""),
                "link": art.get("link"),
                "media": art.get("media"),
                "source": art.get("source")
            })
        news_entries[company] = entries
    return news_entries

def summarize_dataframe(df, name):
    """Generate short summaries for numeric columns."""
    if df.empty:
        return "No data available."
    summary = f"Summary of {name}:\n"
    for col in df.columns[:5]:
        mean_val = df[col].mean() if pd.api.types.is_numeric_dtype(df[col]) else None
        summary += f"- {col}: {mean_val if mean_val is not None else 'varied values'}\n"
    return summary.strip()

def load_fundamentals():
    fund_entries = {}
    for company in os.listdir(COMPANY_PATH):
        company_folder = os.path.join(COMPANY_PATH, company)
        if not os.path.isdir(company_folder): continue
        fund_entries[company] = {}

        for file in ["info.csv", "income_statement.csv", "balance_sheet.csv", "cashflow.csv"]:
            file_path = os.path.join(company_folder, file)
            if not os.path.exists(file_path): continue
            try:
                df = pd.read_csv(file_path)
                summary = summarize_dataframe(df, file.replace(".csv", ""))
                fund_entries[company][file.replace(".csv", "")] = {"summary": summary}
            except Exception:
                continue
    return fund_entries

print("📊 Loading data...")
stock_data = load_stock_data()
news_data = load_news_data()
fund_data = load_fundamentals()

all_companies = set(stock_data.keys()) | set(news_data.keys()) | set(fund_data.keys())

print(f"🏢 Found {len(all_companies)} companies")

for company in all_companies:
    existing = load_existing_json(company)

    if company in stock_data:
        existing["data"]["stock"].extend(stock_data[company])

    if company in news_data:
        existing["data"]["news"].extend(news_data[company])

    if company in fund_data:
        existing["data"]["fundamentals"].update(fund_data[company])

    if existing["data"]["stock"]:
        seen = set()
        unique = []
        for s in existing["data"]["stock"]:
            key = (s["date"], s.get("close"))
            if key not in seen:
                unique.append(s)
                seen.add(key)
        existing["data"]["stock"] = unique

    if existing["data"]["news"]:
        seen = set()
        unique = []
        for n in existing["data"]["news"]:
            key = (n["date"], n.get("title"))
            if key not in seen:
                unique.append(n)
                seen.add(key)
        existing["data"]["news"] = unique

    save_json(existing)

print("✅ All company JSONs updated successfully.")
