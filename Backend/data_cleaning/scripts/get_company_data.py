import yfinance as yf
import pandas as pd
import os
from datetime import datetime

# -------------------------------
# CONFIGURATION
# -------------------------------
output_folder = "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/data_cleaning/outputs/company_data"
os.makedirs(output_folder, exist_ok=True)

# -------------------------------
# NIFTY 50 SYMBOLS (NSE)
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
# SAFE SAVE FUNCTION (append only)
# -------------------------------
def save_or_append_csv(new_df: pd.DataFrame, file_path: str):
    """
    Appends new data if it's not already in the existing file.
    Assumes index (like period or date) differentiates records.
    """
    if os.path.exists(file_path):
        try:
            existing_df = pd.read_csv(file_path, index_col=0)
        except Exception:
            existing_df = pd.DataFrame()

        # Merge without duplicates
        combined = pd.concat([existing_df, new_df])
        combined = combined[~combined.index.duplicated(keep="last")]

        if not combined.equals(existing_df):
            combined.to_csv(file_path)
            print(f"  ↪️ Updated {os.path.basename(file_path)} ({len(combined) - len(existing_df)} new rows)")
        else:
            print(f"  ✅ No new data for {os.path.basename(file_path)}")
    else:
        new_df.to_csv(file_path)
        print(f"  🆕 Created {os.path.basename(file_path)}")


# -------------------------------
# FETCH COMPANY FUNDAMENTALS
# -------------------------------
for company, symbol in nifty_50_symbols.items():
    print(f"\n🏢 Fetching fundamentals for {company} ({symbol})...")
    company_folder = os.path.join(output_folder, company)
    os.makedirs(company_folder, exist_ok=True)

    try:
        ticker = yf.Ticker(symbol)

        # ---- Company Info ----
        info_path = os.path.join(company_folder, "info.csv")
        info = pd.DataFrame([ticker.info])
        save_or_append_csv(info, info_path)

        # ---- Financials ----
        financials = ticker.financials
        if not financials.empty:
            fin_path = os.path.join(company_folder, "income_statement.csv")
            save_or_append_csv(financials.T, fin_path)

        # ---- Balance Sheet ----
        balance_sheet = ticker.balance_sheet
        if not balance_sheet.empty:
            bs_path = os.path.join(company_folder, "balance_sheet.csv")
            save_or_append_csv(balance_sheet.T, bs_path)

        # ---- Cash Flow ----
        cashflow = ticker.cashflow
        if not cashflow.empty:
            cf_path = os.path.join(company_folder, "cashflow.csv")
            save_or_append_csv(cashflow.T, cf_path)

        print(f"✅ Done for {company}")

    except Exception as e:
        print(f"❌ Error fetching {company}: {e}")

print("\n🚀 Done! All company data updated in 'company_data' folder.")
