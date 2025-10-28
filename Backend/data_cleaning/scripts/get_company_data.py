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
# FETCH COMPANY FUNDAMENTALS
# -------------------------------
for company, symbol in nifty_50_symbols.items():
    print(f"🏢 Fetching fundamentals for {company} ({symbol})...")

    company_folder = os.path.join(output_folder, company)
    os.makedirs(company_folder, exist_ok=True)

    try:
        ticker = yf.Ticker(symbol)

        # ---- Company Info ----
        info = pd.DataFrame([ticker.info])
        info.to_csv(os.path.join(company_folder, "info.csv"), index=False)

        # ---- Financials ----
        financials = ticker.financials
        if not financials.empty:
            financials.T.to_csv(os.path.join(company_folder, "income_statement.csv"))

        # ---- Balance Sheet ----
        balance_sheet = ticker.balance_sheet
        if not balance_sheet.empty:
            balance_sheet.T.to_csv(os.path.join(company_folder, "balance_sheet.csv"))

        # ---- Cash Flow ----
        cashflow = ticker.cashflow
        if not cashflow.empty:
            cashflow.T.to_csv(os.path.join(company_folder, "cashflow.csv"))

        print(f"✅ Saved data for {company}\n")

    except Exception as e:
        print(f"❌ Error fetching {company}: {e}\n")

print("\n🚀 Done! All company data saved in 'company_data' folder.")
