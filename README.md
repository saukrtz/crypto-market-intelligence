# Crypto Market Intelligence Pipeline

> **Authors:** Saurav Kumar
> **Last Updated:** April 2026
> **Repository:** crypto-market-intelligence

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Step-by-Step Implementation](#3-step-by-step-implementation)
   - 3.1 Data Ingestion (CoinMarketCap)
   - 3.2 Data Ingestion (CoinGecko)
   - 3.3 Containerization
   - 3.4 Bronze Validation
   - 3.5 Silver Transformations
   - 3.6 Gold Transformations
   - 3.7 Airflow Orchestration
   - 3.8 Kubernetes Infrastructure
   - 3.9 FastAPI Service
4. [Challenges Faced & Solutions](#4-challenges-faced--solutions)
5. [How the Code Satisfies Requirements](#5-how-the-code-satisfies-requirements)
6. [Technology Comparison](#6-technology-comparison--why-our-choices-are-better)
7. [Future Improvements](#7-future-improvements)

---

## 1. Project Overview

This project is an **end-to-end data engineering pipeline** for cryptocurrency market intelligence. It:

- **Ingests** real-time and historical crypto data from two APIs (CoinMarketCap and CoinGecko).
- **Stores** raw data in Azure Data Lake Storage Gen2 (ADLS) in date-partitioned Bronze zone.
- **Transforms** data through a Medallion Architecture (Bronze → Silver → Gold) using Databricks Spark notebooks.
- **Orchestrates** the entire workflow with Apache Airflow running on Kubernetes (Minikube).
- **Serves** analytics-ready data through a FastAPI REST API.

---

## 2. Architecture

```
┌──────────────────┐    ┌──────────────────┐
│  CoinMarketCap   │    │    CoinGecko     │
│      API         │    │      API         │
└────────┬─────────┘    └────────┬─────────┘
         │                       │
         ▼                       ▼
┌─────────────────────────────────────────┐
│   Python Ingestion Scripts (Docker)     │
│   cmc_ingestor.py / coingecko_ingestor  │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│     Azure Data Lake Storage Gen2        │
│              BRONZE ZONE                │
│  bronze/source/ingestion_date=.../      │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│      Databricks Spark Notebooks         │
│  01_bronze_validation.ipynb             │
│  02_silver_transformations.ipynb        │
│  03_gold_transformations.ipynb          │
└──────┬──────────────────┬───────────────┘
       │                  │
       ▼                  ▼
┌──────────────┐  ┌───────────────┐
│ SILVER ZONE  │  │  GOLD ZONE    │
│ Delta Tables │  │ Delta Tables  │
│ (cleaned)    │  │ (aggregated)  │
└──────────────┘  └───────┬───────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │  FastAPI REST   │
                 │    Service      │
                 └─────────────────┘

ORCHESTRATION: Apache Airflow on Minikube (Kubernetes)
```

### Repository Structure

```
.
├── airflow_dags/
│   └── crypto_pipeline_dag.py        # 3 Airflow DAG definitions
├── api_service/
│   ├── main.py                       # FastAPI endpoints
│   ├── Dockerfile                    # API container config
│   └── requirements.txt
├── databricks_notebooks/
│   ├── 01_bronze_validation.ipynb    # Data quality gate
│   ├── 02_silver_transformations.ipynb  # Schema enforcement + Delta
│   └── 03_gold_transformations.ipynb    # Feature engineering
├── infra/
│   ├── airflow-manifests.yaml        # K8s Deployment for Airflow
│   ├── ingestion-job.yaml            # K8s Jobs for ingestion
│   └── api-deployment.yaml           # (placeholder)
├── ingestion/
│   ├── cmc_ingestor.py               # CoinMarketCap API ingestion
│   ├── coingecko_ingestor.py         # CoinGecko API ingestion
│   ├── Dockerfile                    # Ingestion container config
│   └── requirements.txt
├── ml_models/                        # (placeholder for future ML)
├── docker-compose.yaml               # Local dev stack
├── requirements.txt                  # Root-level Python deps
└── .gitignore
```

---

## 3. Step-by-Step Implementation

### 3.1 CoinMarketCap Ingestion (`ingestion/cmc_ingestor.py`)

**Purpose:** Fetch real-time cryptocurrency data from CoinMarketCap's Pro API and upload raw JSON to ADLS Bronze zone.

**How it works:**

1. Loads `CMC_API_KEY` and `AZURE_STORAGE_CONNECTION_STRING` from environment variables via `python-dotenv`.
2. Defines three CMC endpoints:
   - **listings** — Top 100 coins with price, volume, and market cap (USD).
   - **global** — Total crypto market cap, total volume, BTC dominance.
   - **quotes** — Specific BTC and ETH quote snapshots.
3. `fetch_cmc_data()` makes an authenticated GET request with the API key in the `X-CMC_PRO_API_KEY` header. It wraps the response in a metadata envelope:
   ```json
   {
     "endpoint_type": "listings",
     "source": "coinmarketcap",
     "ingested_at": "2026-04-20T08:00:00+00:00",
     "payload": { ... raw API data ... }
   }
   ```
4. `upload_to_azure()` pushes each JSON blob to ADLS at path:
   ```
   bronze/coinmarketcap/ingestion_date=2026-04-20/listings_080000.json
   ```
5. `run_pipeline()` calls all three endpoints sequentially.

**Key design decisions:**
- Date-partitioned paths (`ingestion_date=YYYY-MM-DD`) prevent data loss from overwrites.
- Metadata envelope preserves source lineage and ingestion timestamps.
- `overwrite=True` on blob upload prevents duplicate blob errors within the same second.

---

### 3.2 CoinGecko Ingestion (`ingestion/coingecko_ingestor.py`)

**Purpose:** Fetch market snapshots and 30-day historical price data from CoinGecko's free API.

**How it works:**

1. Fetches top-50 coins market snapshot from `/coins/markets`.
2. Loops through Bitcoin and Ethereum to fetch 30-day historical charts from `/coins/{id}/market_chart`.
3. Adds a `coin_id` field to historical records (e.g., `"bitcoin"`, `"ethereum"`) to distinguish files.
4. Uploads to ADLS at:
   ```
   bronze/coingecko/ingestion_date=2026-04-20/historical_backfill_bitcoin_080002.json
   ```
5. Uses `time.sleep(2)` between historical API calls to respect CoinGecko's rate limits.

**Key design decisions:**
- The `coin_suffix` in filenames prevents Bitcoin and Ethereum files from overwriting each other.
- No API key required (free tier), making it zero-cost for historical data.

---

### 3.3 Containerization (`ingestion/Dockerfile`)

**Purpose:** Package both ingestor scripts into a single portable Docker image.

```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY cmc_ingestor.py .
COPY coingecko_ingestor.py .
CMD ["python", "cmc_ingestor.py"]
```

- Base image: `python:3.9-slim` (minimal footprint).
- Default CMD runs CMC, but K8s Jobs override this to select which script runs.
- Dependencies: `requests`, `azure-storage-blob`, `python-dotenv`.

---

### 3.4 Bronze Validation (`databricks_notebooks/01_bronze_validation.ipynb`)

**Purpose:** Quality gate that inspects raw Bronze data before transformation.

**Steps performed:**

1. **Connect to ADLS:** Sets the Spark config with the Azure storage account key.
2. **Load all Bronze JSON:** Uses `recursiveFileLookup` to read all files across date-partitioned subfolders.
3. **Add traceability:** Adds `source_file` column via `input_file_name()`.
4. **Inspect structure:** Prints total record count, schema (`printSchema()`), and groups by `source` + `endpoint_type`.
5. **Null payload check:** Filters for records with null payloads. If found → warning; otherwise → success confirmation.
6. **Sample display:** Shows sample payload content for manual inspection.

**Why this matters:** Without validation, corrupted or empty payloads would silently flow into Silver and produce wrong analytics.

---

### 3.5 Silver Transformations (`databricks_notebooks/02_silver_transformations.ipynb`)

**Purpose:** Transform raw JSON into clean, schema-enforced Delta tables.

**Three output tables are produced:**

#### A. `silver/crypto_quotes` (from CMC Listings)

- Filters Bronze for `endpoint_type == "listings"`.
- Defines explicit PySpark schema for nested JSON:
  ```
  ArrayType(StructType([id, name, symbol, quote.USD.price/volume/market_cap]))
  ```
- Parses with `from_json`, then `explode`s the array into individual coin rows.
- Extracts: `coin_id`, `symbol`, `name`, `price_usd`, `volume_24h`, `market_cap`, `event_timestamp`, `silver_processing_timestamp`.

#### B. `silver/global_metrics` (from CMC Global)

- Filters for `endpoint_type == "global"`.
- Uses `get_json_object` to extract scalar values: `total_market_cap`, `total_volume_24h`, `btc_dominance`.

#### C. `silver/historical_prices` (from CoinGecko Historical)

- Filters for `endpoint_type == "historical_backfill"`.
- Defines schema for CoinGecko's `[[timestamp, value], ...]` nested array format.
- Explodes prices array and converts Unix millisecond timestamps to Spark timestamps.
- Extracts: `coin_id`, `price_timestamp`, `price_usd`, `source`.

**All tables saved as Delta** with `mode("overwrite")` and `mergeSchema: true`.

---

### 3.6 Gold Transformations (`databricks_notebooks/03_gold_transformations.ipynb`)

**Purpose:** Create analytics-ready feature tables with computed KPIs.

#### A. `gold/daily_price_summary`

Uses PySpark Window functions partitioned by `symbol` and ordered by `event_timestamp`:

| Feature | Calculation | Purpose |
|---------|------------|---------|
| `prev_day_price` | `lag(price_usd, 1)` | Previous price for comparison |
| `daily_return` | `(current - prev) / prev` | Day-over-day price change % |
| `7d_moving_avg` | `avg(price).rowsBetween(-7, 0)` | Short-term trend smoothing |
| `14d_moving_avg` | `avg(price).rowsBetween(-14, 0)` | Medium-term trend smoothing |
| `7d_avg_volume` | `avg(volume_24h).rowsBetween(-7, 0)` | Baseline volume |
| `volume_spike` | `1 if volume > 2 × 7d_avg_volume` | Unusual activity detection |
| `market_dominance_pct` | `(market_cap / total_market_cap) × 100` | Coin's share of total market |

#### B. `gold/volatility_metrics`

| Feature | Calculation | Purpose |
|---------|------------|---------|
| `rolling_std` | `stddev(price).rowsBetween(-14, 0)` | 14-period price risk measure |

**Validation cells** display: top-10 records, market cap trends, top-5 gainers, BTC price vs. 7-day MA.

---

### 3.7 Airflow Orchestration (`airflow_dags/crypto_pipeline_dag.py`)

**Three independent DAGs:**

| DAG | Schedule | Operator | Action |
|-----|----------|----------|--------|
| `cmc_real_time_ingestion` | `*/15 * * * *` (every 15 min) | `KubernetesPodOperator` | Runs CMC ingestion in a K8s pod |
| `coingecko_on_demand_backfill` | `None` (manual trigger) | `KubernetesPodOperator` | Runs CoinGecko ingestion on-demand |
| `daily_databricks_processing` | `0 8 * * *` (daily 8 AM) | `DatabricksRunNowOperator` | Triggers Databricks Bronze→Silver→Gold |

**Common settings:** `catchup=False`, 1 retry with 5-minute delay, `in_cluster=True` for K8s pods.

---

### 3.8 Kubernetes Infrastructure (`infra/`)

**`airflow-manifests.yaml`:**
- K8s Deployment for Airflow webserver (1 replica, `apache/airflow:2.8.1`).
- Installs K8s and Databricks providers on boot.
- Mounts local DAGs via `hostPath` volume.
- Reads Azure connection string from K8s Secret `ingestion-secrets`.

**`ingestion-job.yaml`:**
- Two K8s Jobs (CMC + CoinGecko) using `crypto-ingestion:v1`.
- `imagePullPolicy: Never` — uses locally-built image in Minikube.
- Secrets injected via `envFrom` from `ingestion-secrets`.

**`docker-compose.yaml` (alternative local dev):**
- PostgreSQL + Airflow webserver + scheduler + init container.
- `LocalExecutor` with all env vars passed through.
- Mounts DAGs, ingestion code, logs, and plugins.

---

### 3.9 FastAPI Service (`api_service/main.py`)

**Purpose:** REST API serving Gold-layer analytics without requiring Spark.

| Endpoint | Description |
|----------|-------------|
| `GET /` | Health check, link to docs |
| `GET /health` | UTC timestamp + status |
| `GET /coins` | List all symbols + schema columns |
| `GET /coins/{symbol}/latest` | Latest Gold record for a coin |
| `GET /market/rankings?top=5` | Top gainers and losers by daily return |
| `GET /coins/{symbol}/volatility` | Latest rolling std and market cap |

**Key technical details:**
- Uses `deltalake` Python library to read Delta tables directly from ADLS (no Spark needed).
- `get_df()` helper converts datetime columns to strings and NaN to None for JSON compatibility.
- Dynamically finds the best timestamp column for sorting.
- Dockerfile exposes port 8000 via `uvicorn`.

---

## 4. Challenges Faced & Solutions

### Challenge 1: Nested JSON Parsing
**Problem:** CMC returns deeply nested structures like `quote.USD.price`.
**Solution:** Defined explicit PySpark schemas (`StructType`/`ArrayType`) and used `from_json` + `explode` to flatten reliably.

### Challenge 2: CoinGecko Rate Limiting
**Problem:** Free tier aggressively limits API calls (~10-30 per minute).
**Solution:** Added `time.sleep(2)` between calls and made the DAG manual-trigger only.

### Challenge 3: Filename Collisions
**Problem:** During the historical loop, Bitcoin and Ethereum files overwrite each other (same `endpoint_type` + timestamp).
**Solution:** Added `coin_id` suffix to blob names in `coingecko_ingestor.py`.

### Challenge 4: Kubernetes Image Pull Failures
**Problem:** Minikube can't access the local Docker daemon's images by default.
**Solution:** Run `eval $(minikube docker-env)` before building + set `imagePullPolicy: Never`.

### Challenge 5: Delta Table JSON Serialization
**Problem:** Pandas datetimes and NaN values break FastAPI's JSON encoder.
**Solution:** `get_df()` converts all datetime columns to strings and replaces NaN with None.

### Challenge 6: Schema Evolution
**Problem:** Different ingestion runs may produce slightly different JSON schemas.
**Solution:** Delta Lake's `mergeSchema: true` handles schema changes gracefully.

### Challenge 7: Credential Management
**Problem:** API keys and connection strings must not be hardcoded.
**Solution:** Used K8s Secrets, `.env` files, and `python-dotenv` across all components.

---

## 5. How the Code Satisfies Requirements

| Requirement | Implementation |
|-------------|---------------|
| Real-time data ingestion | CMC DAG runs every 15 minutes pulling live market data |
| Historical backfill capability | CoinGecko fetches 30-day price history for BTC/ETH |
| Scalable cloud storage | ADLS Gen2 with hierarchical namespace and date-partitioned paths |
| Data quality assurance | Bronze validation checks for null payloads; Silver enforces schemas |
| Medallion Architecture | Clear Bronze (raw) → Silver (cleaned) → Gold (aggregated) separation |
| ACID compliance | Delta Lake format on all Silver and Gold tables |
| Automated orchestration | Airflow DAGs with scheduled (15-min, daily) and manual triggers |
| Container-native deployment | Ingestion scripts Dockerized; Airflow runs on Kubernetes |
| Analytics-ready output | Gold tables have pre-computed moving averages, returns, volatility, dominance |
| API access to data | FastAPI exposes all Gold metrics as REST endpoints |
| Infrastructure as Code | K8s manifests and Docker Compose define the stack declaratively |
| Multiple data sources | Two independent APIs provide redundancy and coverage |

---

## 6. Technology Comparison — Why Our Choices Are Better

### Databricks + Spark vs. Python/Pandas
Spark handles arbitrarily large datasets via distributed processing. As data grows (100+ coins × every 15 min), Pandas would hit memory limits on a single machine. Spark scales horizontally by adding workers.

### Delta Lake vs. Raw Parquet/CSV
Delta provides ACID transactions, schema enforcement, time-travel, and merge capabilities. A failed Parquet write could corrupt the entire dataset silently. Delta's transaction log prevents this.

### ADLS Gen2 vs. AWS S3 or Local Storage
ADLS Gen2 with HNS (Hierarchical Namespace) is optimized for big data workloads and integrates natively with Databricks on Azure. Local storage can't scale; S3 would add cross-cloud latency.

### Airflow vs. Azure Data Factory or Cron Jobs
Airflow provides dependency graphs, retries, a visual UI, extensive logging, and integrates with both Kubernetes and Databricks via providers. Cron jobs lack monitoring, retries, and dependency management. ADF works but locks into Azure-only orchestration.

### KubernetesPodOperator vs. BashOperator
Running ingestion in isolated K8s pods ensures resource isolation, reproducible environments, and prevents dependency conflicts with Airflow's own Python environment.

### FastAPI vs. Flask or Django
FastAPI provides automatic OpenAPI documentation, async support, Pydantic type validation, and benchmarks significantly faster than Flask. Django's ORM and admin panel are unnecessary overhead for a pure API layer.

### Two APIs vs. Single Source
CoinMarketCap provides professional-grade real-time data with detailed quote information. CoinGecko adds free historical backfill capability. Using both provides data redundancy — if one API goes down, the pipeline still partially functions.

---
