# sp500-local-db

A lightweight local data warehouse for pricing, financial, and fundamental data for companies in the S&P 500 with a data feed from Financial Modeling Prep (FMP) stable endpoints. It supports incremental, idempotent updates, atomic writes, parallel downloads with rate limiting, and DuckDB views over Parquet for fast analytics without the need for a database server.
