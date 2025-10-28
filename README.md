# Investment-Intelligence-Platform

Folder structue 

Backend/
│
├── data_cleaning/
│   ├── scripts/
│   │   ├── get_stock_data.py      # Fetches historical stock price data for NIFTY50 companies.
│   │   ├── get_company_data.py    # Scrapes or fetches company financials (balance sheet, cash flow, etc.)
│   │   ├── get_news_data.py       # Collects recent financial news from multiple sources and standardizes format.
│   │
│   ├── outputs/                   # Stores cleaned or raw output files from data scripts.
│   │   ├── stock_output/          # CSV files for each company’s stock prices.
│   │   ├── company_output/        # CSV/JSON financial statements.
│   │   ├── news_output/           # Standardized JSONs containing aggregated news data.
│   │
│   └── README.md                  # (You are here) Explains structure, usage, and setup.
│
├── main.py                        # Entry point for the backend API or data service.
├── services/                      # Core service modules (API, embeddings, ML pipeline, etc.)
├── routers/                       # FastAPI route definitions.
├── utils/                         # Shared helper functions.
└── requirements.txt               # Python dependencies.
