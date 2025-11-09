import yfinance as yf
import os
import pandas as pd
from datetime import datetime, timedelta

# -------------------------------
# CONFIGURATION
# -------------------------------
start_date = "2025-01-01"
end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

output_folder = "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/services/data_ingestion/outputs/stock_data"
os.makedirs(output_folder, exist_ok=True)

# -------------------------------
# NIFTY 50 SYMBOLS
# -------------------------------
nifty_50_symbols = {
    "ADANIENT": "ADANIENT.NS", "ADANIPORTS": "ADANIPORTS.NS", "APOLLOHOSP": "APOLLOHOSP.NS",
    "ASIANPAINT": "ASIANPAINT.NS", "AXISBANK": "AXISBANK.NS", "BAJAJ-AUTO": "BAJAJ-AUTO.NS",
    "BAJFINANCE": "BAJFINANCE.NS", "BAJAJFINSV": "BAJAJFINSV.NS", "BPCL": "BPCL.NS",
    "BHARTIARTL": "BHARTIARTL.NS", "BRITANNIA": "BRITANNIA.NS", "CIPLA": "CIPLA.NS",
    "COALINDIA": "COALINDIA.NS", "DIVISLAB": "DIVISLAB.NS", "DRREDDY": "DRREDDY.NS",
    "EICHERMOT": "EICHERMOT.NS", "GRASIM": "GRASIM.NS", "HCLTECH": "HCLTECH.NS",
    "HDFCBANK": "HDFCBANK.NS", "HEROMOTOCO": "HEROMOTOCO.NS", "HINDALCO": "HINDALCO.NS",
    "HINDUNILVR": "HINDUNILVR.NS", "ICICIBANK": "ICICIBANK.NS", "INDUSINDBK": "INDUSINDBK.NS",
    "INFY": "INFY.NS", "ITC": "ITC.NS", "JSWSTEEL": "JSWSTEEL.NS", "KOTAKBANK": "KOTAKBANK.NS",
    "LT": "LT.NS", "M&M": "M&M.NS", "MARUTI": "MARUTI.NS", "NESTLEIND": "NESTLEIND.NS",
    "NTPC": "NTPC.NS", "ONGC": "ONGC.NS", "POWERGRID": "POWERGRID.NS", "RELIANCE": "RELIANCE.NS",
    "SBILIFE": "SBILIFE.NS", "SBIN": "SBIN.NS", "SUNPHARMA": "SUNPHARMA.NS",
    "TATACONSUM": "TATACONSUM.NS", "TATAMOTORS": "TATAMOTORS.NS", "TATASTEEL": "TATASTEEL.NS",
    "TCS": "TCS.NS", "TECHM": "TECHM.NS", "TITAN": "TITAN.NS", "ULTRACEMCO": "ULTRACEMCO.NS",
    "UPL": "UPL.NS", "WIPRO": "WIPRO.NS"
}

# -------------------------------
# FETCH & APPEND LOGIC
# -------------------------------
for company, symbol in nifty_50_symbols.items():
    file_path = os.path.join(output_folder, f"{company}.csv")

    # Load existing data if available
    if os.path.exists(file_path):
        try:
            existing_df = pd.read_csv(file_path, parse_dates=["Date"])
            if "Date" not in existing_df.columns or existing_df.empty:
                raise ValueError("Invalid CSV")
            last_date = existing_df["Date"].max()
            fetch_start = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
        except Exception:
            print(f"⚠️ Resetting {company} (bad or empty CSV).")
            existing_df = pd.DataFrame()
            fetch_start = start_date
    else:
        existing_df = pd.DataFrame()
        fetch_start = start_date

    # Skip if already up to date
    if fetch_start > end_date:
        print(f"✅ {company} already up-to-date.")
        continue

    print(f"📈 Fetching {company} ({symbol}) from {fetch_start} → {end_date} ...")

    try:
        new_data = yf.download(
            tickers=symbol,
            start=fetch_start,
            end=end_date,
            interval="1d",
            auto_adjust=True,
            progress=False,
            threads=True
        )

        # Handle MultiIndex columns (new yfinance format)
        if isinstance(new_data.columns, pd.MultiIndex):
            new_data.columns = [c[0] if isinstance(c, tuple) else c for c in new_data.columns]

        # Ensure required columns exist
        if new_data.empty or "Close" not in new_data.columns:
            print(f"⚠️ No valid data found for {company}")
            continue

        new_data.reset_index(inplace=True)
        new_data["Date"] = pd.to_datetime(new_data["Date"])

        # Combine and deduplicate
        combined = pd.concat([existing_df, new_data], ignore_index=True)
        combined.drop_duplicates(subset=["Date"], inplace=True)
        combined.sort_values("Date", inplace=True)

        combined.to_csv(file_path, index=False)
        print(f"✅ {company}: {len(new_data)} new rows added.\n")

    except Exception as e:
        print(f"❌ Error fetching {company}: {e}")

print("\n🚀 All NIFTY 50 data updated successfully!")
