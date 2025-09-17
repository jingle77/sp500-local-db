# sp500-local-db

Local Parquet + DuckDB warehouse for S&P 500:
- Constituents snapshot
- Dividend-adjusted historical prices (incremental)
- Balance sheet, income statement, key metrics, ratios
- Product revenue segmentation (normalized to long format)

## Setup
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate

pip install -r requirements.txt
cp .env.example .env
# put your Financial Modeling Prep API key in .env

## Run
python main.py          # snapshot constituents + (re)create DuckDB views
python prices.py        # incremental prices
python fundamentals.py  # fundamentals + revenue segmentation
python sample_queries.py

## Notes
- Parallel + rate-limited (headroom under 750/min. End user will need to adjust given current FMP API access).
- Incremental upserts per-symbol Parquet with atomic write.
- DuckDB views read folder wildcards, so files append seamlessly.
