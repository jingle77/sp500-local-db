# sp500-local-db

![alt text](https://github.com/jingle77/sp500-local-db/blob/main/growtika-ZfVyuV8l7WU-unsplash.jpg)

A lightweight local data warehouse for pricing, financial, and fundamental data for companies in the S&P 500 with a data feed from Financial Modeling Prep (FMP) stable endpoints. It supports incremental, idempotent updates, atomic writes, parallel downloads with rate limiting, and DuckDB views over Parquet for fast analytics without the need for a database server.

---
## Features
- **Local Lakehouse:** Columnar Parquet files on disk, queryable via DuckDB
- **Incremental Upserts:** Per Symbol and no full redownloads
- **Idempotent writes:** atomic temp-file + rename to prevent corruption
- **Parallel Downloads:** Parralel Programing with ThreadPoolExecutor with a minute rate limiter
- **Progress Bars:** via tqdm module
- **Sample queries:** via sample_queries.py file
---

## Data Sources
All endpoints use FMP stable base:

- **S&P 500 Constituents:** https://financialmodelingprep.com/stable/sp500-constituent?apikey=<API_KEY>
- **Dividend-Adjusted Prices:** https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted?symbol=SYMBOL&apikey=<API_KEY>
- **Balance Sheet:** https://financialmodelingprep.com/stable/balance-sheet-statement?symbol=SYMBOL&apikey=<API_KEY>
- **Income Statement:** https://financialmodelingprep.com/stable/income-statement?symbol=SYMBOL&apikey=<API_KEY>
- **Key Metrics:** https://financialmodelingprep.com/stable/key-metrics?symbol=SYMBOL&apikey=<API_KEY>
- **Ratios:** https://financialmodelingprep.com/stable/ratios?symbol=SYMBOL&apikey=<API_KEY>
- **Revenue Product Segmentation:** https://financialmodelingprep.com/stable/revenue-product-segmentation?symbol=SYMBOL&apikey=<API_KEY>

## Installation

Prerequisites
- Python 3.11+
- An FMP API key. (Note, adjust the value for REQUESTS_PER_MINUTE accordingly based on your API plan)

1. Clone and Create repository 
```bash
git clone https://github.com/jingle77/sp500-local-db
cd sp500-local-db

# Create Virtual Environment
python -m venv .venv

# macOS/Linux
source .venv/bin/activate

# Windows PowerShell
.\.venv\Scripts\activate
```

2. Install Dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

3. Configure Environment
```bash
cp .env.example .env
# edit .env and set API_KEY
# you can also edit REQUESTS_PER_MINUTE here to be within your API plan's threshold
# API_KEY = YOUR_FMP_API_KEY
# REQUESTS_PER_MINUTE = 700
```

4. Usage (Initial and Subsequent Runs are recommended to be completed in this order)
```bash
# 1) Bootstrap constituents + views
python main.py

# 2) Incremental prices (per symbol)
python prices.py

# 3) Incremental fundamentals (per dataset, per symbol)
python fundamentals.py

# 4) Quick analytics & health snapshot
python sample_queries.py

# 5) Tests
pytest -v
```
---
