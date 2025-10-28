# import os
# import json
# import datetime
# from GoogleNews import GoogleNews

# # -------------------------------
# # CONFIG
# # -------------------------------
# START_DATE = "2025-01-01"
# END_DATE = datetime.date.today().strftime("%Y-%m-%d")

# OUTPUT_FOLDER = "new_output"
# os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# NIFTY50 = [
#     "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "HINDUNILVR", "ITC", "SBIN",
#     "BHARTIARTL", "BAJFINANCE", "ASIANPAINT", "KOTAKBANK", "LT", "M&M", "MARUTI",
#     "NESTLEIND", "NTPC", "POWERGRID", "TITAN", "SUNPHARMA", "ULTRACEMCO", "WIPRO",
#     "ONGC", "ADANIENT", "ADANIPORTS", "AXISBANK", "COALINDIA", "BAJAJFINSV",
#     "HCLTECH", "TECHM", "GRASIM", "TATASTEEL", "JSWSTEEL", "CIPLA", "DIVISLAB",
#     "DRREDDY", "EICHERMOT", "HDFCLIFE", "HEROMOTOCO", "BPCL", "BRITANNIA", "INDUSINDBK",
#     "APOLLOHOSP", "BAJAJ-AUTO", "SBILIFE", "TATACONSUM", "HINDALCO", "UPL", "SHRIRAMFIN",
#     "TRENT"
# ]

# # -------------------------------
# # FETCH FUNCTION
# # -------------------------------
# def fetch_company_news(company_name):
#     """Fetch recent news for a given company"""
#     print(f"📰 Fetching news for {company_name}...")

#     file_path = os.path.join(OUTPUT_FOLDER, f"{company_name}.json")

#     # Load old data if available
#     existing_data = []
#     if os.path.exists(file_path):
#         with open(file_path, "r") as f:
#             try:
#                 existing_data = json.load(f)
#             except json.JSONDecodeError:
#                 existing_data = []

#     existing_titles = {item["title"] for item in existing_data if "title" in item}

#     # ✅ Initialize GoogleNews with date range
#     news = GoogleNews(lang='en', region='IN', encode='utf-8')
#     news.set_time_range(START_DATE, END_DATE)
#     news.search(f"{company_name} stock news India")

#     results = news.result()

#     new_results = []
#     for item in results:
#         title = item.get("title", "")
#         if title and title not in existing_titles:
#             new_results.append({
#                 "title": title,
#                 "link": item.get("link", ""),
#                 "published": item.get("date", ""),
#                 "media": item.get("media", ""),
#                 "desc": item.get("desc", "")
#             })

#     if new_results:
#         print(f"✅ {len(new_results)} new articles found for {company_name}")
#         updated_data = existing_data + new_results
#         with open(file_path, "w") as f:
#             json.dump(updated_data, f, indent=4)
#     else:
#         print(f"⚠️ No new articles found for {company_name}")

# # -------------------------------
# # MAIN
# # -------------------------------
# if __name__ == "__main__":
#     print(f"📆 Fetching NIFTY50 news from {START_DATE} to {END_DATE}\n")
#     for company in NIFTY50:
#         try:
#             fetch_company_news(company)
#         except Exception as e:
#             print(f"❌ Error fetching news for {company}: {e}")
#!/usr/bin/env python3
import os
import json
import datetime
import time
import requests
import feedparser
from bs4 import BeautifulSoup
from GoogleNews import GoogleNews
from urllib.parse import urlparse
from dateutil import parser
from datetime import timedelta
import re
import sys

# -------------------------------
# CONFIG
# -------------------------------
START_DATE = "2025-01-01"
END_DATE = datetime.date.today().strftime("%Y-%m-%d")

OUTPUT_FOLDER = "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/data_cleaning/outputs"
NEWS_FOLDER = os.path.join(OUTPUT_FOLDER, "news")
os.makedirs(NEWS_FOLDER, exist_ok=True)

# NIFTY50 short list
NIFTY50 = [
    "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "HINDUNILVR", "ITC", "SBIN",
    "BHARTIARTL", "BAJFINANCE", "ASIANPAINT", "KOTAKBANK", "LT", "M&M", "MARUTI",
    "NESTLEIND", "NTPC", "POWERGRID", "TITAN", "SUNPHARMA", "ULTRACEMCO", "WIPRO",
    "ONGC", "ADANIENT", "ADANIPORTS", "AXISBANK", "COALINDIA", "BAJAJFINSV",
    "HCLTECH", "TECHM", "GRASIM", "TATASTEEL", "JSWSTEEL", "CIPLA", "DIVISLAB",
    "DRREDDY", "EICHERMOT", "HDFCLIFE", "HEROMOTOCO", "BPCL", "BRITANNIA", "INDUSINDBK",
    "APOLLOHOSP", "BAJAJ-AUTO", "SBILIFE", "TATACONSUM", "HINDALCO", "UPL", "SHRIRAMFIN",
    "TRENT"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
}

# -------------------------------
# DATE NORMALIZER
# -------------------------------
def normalize_date(date_str):
    """
    Convert things like '3 hours ago' or 'Mar 3, 2024' to ISO (YYYY-MM-DD)
    """
    if not date_str:
        return None
    ds = date_str.strip().lower()
    now = datetime.datetime.now()

    if "hour" in ds:
        num = int(re.findall(r"\d+", ds)[0]) if re.findall(r"\d+", ds) else 0
        return (now - timedelta(hours=num)).strftime("%Y-%m-%d")
    elif "minute" in ds:
        num = int(re.findall(r"\d+", ds)[0]) if re.findall(r"\d+", ds) else 0
        return (now - timedelta(minutes=num)).strftime("%Y-%m-%d")
    elif "day" in ds:
        num = int(re.findall(r"\d+", ds)[0]) if re.findall(r"\d+", ds) else 0
        return (now - timedelta(days=num)).strftime("%Y-%m-%d")
    elif "yesterday" in ds:
        return (now - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        return parser.parse(date_str).strftime("%Y-%m-%d")
    except Exception:
        return None

# -------------------------------
# HELPERS
# -------------------------------
def load_existing(file_path):
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def save_json(file_path, data):
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def standardize_article(title, link, published, media, desc, source):
    return {
        "title": title.strip() if title else "",
        "link": link.strip() if link else "",
        "published_raw": published.strip() if published else "",
        "published": normalize_date(published),
        "media": media.strip() if media else "",
        "desc": desc.strip() if desc else "",
        "source": source
    }

# -------------------------------
# SCRAPER
# -------------------------------
def scrape_article(url, max_chars=4000):
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
    except Exception:
        return None
    if res.status_code != 200:
        return None

    soup = BeautifulSoup(res.text, "html.parser")
    domain = urlparse(url).netloc.lower()

    if "economictimes" in domain:
        candidates = soup.select("div.artText p, div[itemprop='articleBody'] p, article p")
    elif "moneycontrol" in domain:
        candidates = soup.select("div.article p, .MCContent p, article p")
    elif "livemint" in domain or "mint" in domain:
        candidates = soup.select("div.article p, article p, .story p")
    elif "thehindu" in domain:
        candidates = soup.select("div.article p, article p")
    else:
        candidates = soup.find_all("p")

    paragraphs = [p.get_text(" ", strip=True) for p in candidates if len(p.get_text(strip=True)) > 50]
    text = " ".join(paragraphs).replace("\xa0", " ").strip()
    return text[:max_chars] if len(text) > 150 else None

# -------------------------------
# FETCHERS
# -------------------------------
def fetch_googlenews(company):
    try:
        gn = GoogleNews(lang='en', region='IN', encode='utf-8')
        queries = [
            f"{company} stock news India",
            f"{company} share price",
            f"{company} quarterly results",
            f"{company} acquisition OR merger OR deal"
        ]
        items = []
        for q in queries:
            gn.search(q)
            time.sleep(0.5)
            items.extend(gn.result())

        articles = []
        for it in items:
            title = it.get("title")
            link = it.get("link")
            date = it.get("date") or it.get("published") or ""
            media = it.get("media") or ""
            desc = it.get("desc") or ""
            if title:
                articles.append(standardize_article(title, link, date, media, desc, "GoogleNews"))
        return articles
    except Exception:
        return []

def fetch_google_rss(company):
    q = f"{company} stock India"
    rss_url = f"https://news.google.com/rss/search?q={requests.utils.quote(q)}&hl=en-IN&gl=IN&ceid=IN:en"
    try:
        feed = feedparser.parse(rss_url)
    except Exception:
        return []
    articles = []
    for entry in feed.entries:
        title = entry.get("title")
        link = entry.get("link")
        published = entry.get("published", entry.get("pubDate", ""))
        source = entry.get("source", {}).get("title") if entry.get("source") else (urlparse(link).netloc if link else "")
        desc = entry.get("summary", "")
        articles.append(standardize_article(title, link, published, source, desc, "GoogleNewsRSS"))
    return articles

# -------------------------------
# MAIN FETCHER PER COMPANY
# -------------------------------
def fetch_company_news(company):
    print(f"\n📰 Fetching news for {company} ...")
    file_path = os.path.join(NEWS_FOLDER, f"{company}.json")
    existing = load_existing(file_path)
    existing_titles = {item.get("title") for item in existing}
    existing_links = {item.get("link") for item in existing if item.get("link")}

    all_articles = []

    print("  🔍 GoogleNews search ...", end=" ", flush=True)
    gn_articles = fetch_googlenews(company)
    print(f"{len(gn_articles)} results")
    all_articles.extend(gn_articles)

    print("  🔍 Google RSS ...", end=" ", flush=True)
    rss_articles = fetch_google_rss(company)
    print(f"{len(rss_articles)} results")
    all_articles.extend(rss_articles)

    if not all_articles:
        print(f"⚠️  No articles found for {company}")
        return

    # Deduplicate
    seen = set()
    unique = []
    for a in all_articles:
        key = (a.get("title","").lower(), a.get("link",""))
        if key not in seen and (a["title"] or a["link"]):
            seen.add(key)
            unique.append(a)

    print(f"  🧩 Found {len(unique)} unique articles.")
    new_articles = []
    for idx, art in enumerate(unique[:20]):
        title = art["title"]
        link = art["link"]
        sys.stdout.write(f"\r  📰 Scraping {idx+1}/{min(20, len(unique))} ...")
        sys.stdout.flush()

        if title in existing_titles or link in existing_links:
            continue

        full_text = scrape_article(link) if link else None
        if full_text:
            art["desc"] += f"\n\nFull article text:\n{full_text}"

        new_articles.append(art)
        time.sleep(0.5)
    print()

    if new_articles:
        updated = new_articles + existing
        save_json(file_path, updated)
        print(f"✅ Added {len(new_articles)} new → {file_path}")
    else:
        print(f"⚠️  No new articles added for {company}")

# -------------------------------
# RUN ALL
# -------------------------------
if __name__ == "__main__":
    print(f"📆 Fetching news for NIFTY50 from {START_DATE} → {END_DATE}")
    for company in NIFTY50:
        try:
            fetch_company_news(company)
        except KeyboardInterrupt:
            print("\n⏹️ Stopped by user.")
            break
        except Exception as e:
            print(f"❌ Error for {company}: {e}")
    print("\n🎉 Done! JSONs saved in:", NEWS_FOLDER)
