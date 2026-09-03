# Data Engineering Laboratory (22MDCEL10) — Complete Lab Submission

**Programme:** M.Sc (Decision and Computing Sciences)
**Dataset:** [Fashion Product Images (Small)](https://www.kaggle.com/datasets/paramaggarwal/fashion-product-images-small) — Kaggle
**Deliverable:** A single, continuous, real, runnable data-engineering pipeline covering all 6
weeks of the course plan, orchestrated with **Apache Airflow**, streamed with **Kafka**, validated with **CI/CD & Pytest**, and
showcased in a **Power BI** dashboard (plus a working Streamlit dashboard as a live bonus demo).

## What's new in this version

| Ask | Where |
|---|---|
| CI/CD & Version Control (Week 6) | `.github/workflows/ci.yml`, `BRANCHING_STRATEGY.md`, `tests/test_transformations.py` — 28 pure unit tests with zero external dependencies, GitFlow-Lite strategy, flake8 linting, and automated GitHub Actions CI workflow |
| Power BI dashboard, with buttons | `powerbi/` — real star-schema data exports + Power Query M scripts + DAX + a step-by-step build guide (including button/bookmark navigation). I can't run Power BI Desktop myself (Windows/Mac-only proprietary software) — see `powerbi/BUILD_GUIDE.md` for why and how to assemble it in ~15 min |
| New data → automatic EDA/ETL | `orchestration/airflow/dags/new_data_watcher_dag.py` — a **real, tested** Airflow DAG that polls `data/incoming/`, and the instant a file lands, ingests it and triggers the full pipeline. Verified end-to-end: file dropped → detected → ingested → pipeline triggered, `state=success` |
| Airflow (Week 5) | `orchestration/airflow/dags/data_engineering_pipeline_dag.py` — the whole Week 1-5 pipeline as a real Airflow task DAG, **installed and actually executed** in this build (not just written) — see `orchestration/AIRFLOW_SETUP.md` |
| Kafka (Week 5) | `orchestration/kafka/` — real producer/consumer Python code + `docker-compose.yml` for a local broker. I can't run a Kafka broker in this sandbox (no Docker/JVM network access here), so this is genuine, runnable code + architecture docs for you to run against a broker you stand up — see `orchestration/kafka/README.md` |
| Star schema tables with visible data (Week 3) | `powerbi/data/*.csv` — the actual `fact_sales`/`dim_product`/etc. tables with real rows, ready to drop straight onto a Power BI Table visual |

Everything in this project runs against the **real dataset** you provided (44,424-row
`styles.csv` + real sample product images) — there is no synthetic/mock data standing in for
the catalog itself. A small number of clearly-labelled **synthetic operational fields**
(stock quantities, order transactions, prices, ratings) were generated deterministically from
real product IDs, because the source dataset is a product *catalog*, not a *sales* dataset —
these are needed to demonstrate ETL/schema/resilience concepts that require transactional
data, and are called out explicitly wherever they appear (dashboard + code comments).

---

## Quick start

```bash
# 1. Create a virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the full pipeline (Weeks 1 -> 5, ~10-15 seconds)
python3 run_pipeline.py

# 4. Launch the interactive dashboard
streamlit run dashboard/app.py
```

Then open the URL Streamlit prints (usually `http://localhost:8501`) and use the sidebar to
navigate between Week 1 through Week 5.

---

## Project structure

```
delab/
├── data/
│   ├── raw/                    # styles.csv + sample images (your uploaded data)
│   ├── staging/                # isolated staging area (Week 5 validation, NoSQL store)
│   └── warehouse/               # SQLite/DuckDB databases produced by the pipeline
├── week1_eda/
│   └── pipeline_week1.py        # collection, cleaning, feature engineering, EDA
├── week2_etl/
│   └── pipeline_week2.py        # extract (4 sources) -> transform -> load -> CDC
├── week3_schema/
│   └── pipeline_week3.py        # OLTP schema, star/snowflake schema, data cube
├── week4_pipeline/
│   ├── api_server.py            # real FastAPI mock REST service
│   └── pipeline_week4.py        # extract from API -> transform -> load -> verify
├── week5_resilience/
│   └── pipeline_week5.py        # staging/validation, idempotency, atomicity, backfill
├── dashboard/                     # bonus: working Streamlit dashboard (optional)
│   ├── app.py
│   └── pages/
├── powerbi/                       # PRIMARY dashboard deliverable
│   ├── data/                      # star-schema CSVs (real data) + data/incoming/ for new drops
│   ├── export_for_powerbi.py      # regenerates the CSVs from the warehouse
│   ├── POWER_QUERY_M_SCRIPTS.md   # M scripts incl. auto-ETL-on-new-data folder query
│   ├── DAX_MEASURES.md
│   └── BUILD_GUIDE.md             # step-by-step .pbix assembly + button/bookmark nav
├── orchestration/
│   ├── AIRFLOW_SETUP.md
│   ├── airflow/dags/
│   │   ├── data_engineering_pipeline_dag.py   # Week 1-5 as a real task DAG
│   │   └── new_data_watcher_dag.py            # auto-triggers on new data
│   └── kafka/
│       ├── docker-compose.yml
│       ├── producer.py            # streams simulated CDC events
│       ├── consumer.py            # idempotent consumer, upserts into warehouse
│       └── README.md              # architecture: topics, keys, replay/backfill
├── run_pipeline.py                # orchestrator - runs all 5 weeks in order (used by Airflow)
├── requirements.txt
└── requirements-airflow.txt        # (see orchestration/AIRFLOW_SETUP.md - separate venv)
```

Each `weekN_*/outputs/` folder is populated fresh every time `run_pipeline.py` runs — these
are the actual artifacts (JSON reports, CSVs, PNG charts) that the dashboard reads and
displays. Nothing on the dashboard is hard-coded.

---

## What each week demonstrates

### Week 1 — Data Collection, Preprocessing, Feature Engineering, EDA
- Two real data source types: structured/tabular (CSV) + unstructured (images), with a
  documented extension path for video/audio/medical sources
- Missing-value handling (categorical → `Unknown` bucket, numeric → median imputation),
  duplicate removal, noisy-value (out-of-range year) filtering
- Feature engineering: derived numeric features, ordinal/one-hot encoding, and **frequency
  encoding** used specifically to avoid one-hot dimensionality explosion on the 140+-level
  `articleType` column
- 6 EDA visualizations + written interpretation

### Week 2 — Core ETL Pipeline
- **4 distinct extraction sources**: flat file (CSV), RDBMS (SQLite), NoSQL (TinyDB), REST
  API (the Week 4 FastAPI service — with graceful fallback if it isn't running)
- Transformation: cleansing, standardization, multi-source joins, category-level aggregation
- **Full load** vs **incremental load** (persisted high-watermark), both implemented and timed
- **CDC** via row-hash snapshot diffing (detects inserts/updates/deletes without DB
  transaction logs)

### Week 3 — Data Architecture & Schema Design
- OLTP vs OLAP components documented for this domain
- **3NF relational schema** (`products`, `orders`, `order_items`, `customers`, `stores`,
  category/subcategory/articleType lookup tables) built in SQLite
- **Star schema** (`fact_sales` + `dim_product`/`dim_time`/`dim_customer`/`dim_store`) *and* a
  **snowflake variant** built in DuckDB
- **Data cube**: 5 targeted analytical queries + one full `GROUP BY CUBE` multidimensional
  query, with interpretation

### Week 4 — Python Batch Pipeline (API-driven)
- A **real FastAPI service** (`api_server.py`) serves the catalog over HTTP
- The batch pipeline performs genuine paginated HTTP extraction against it, transforms the
  data, loads it into the SQLite warehouse, and **verifies** row counts post-load
- Error handling: partial-failure-safe pagination loop, empty-result guard

### Week 5 — Resilient, Production-Ready Pipelines
- **Staging & validation**: isolated Parquet staging area + schema/null/outlier/duplicate
  checks — the warehouse is never touched unless every check passes
- **Idempotency**: proven by loading the *same* batch twice and showing 0 net-new rows on the
  rerun (`INSERT ... ON CONFLICT DO UPDATE`)
- **Atomicity**: proven by deliberately injecting a bad row mid-batch inside a single
  transaction and showing the entire batch rolls back (0 rows persisted, not 149/200)
- **Error handling / backfill**: simulates a historical data-quality bug, detects the exact
  blast radius, and backfills only the affected rows from source-of-truth — not a full reload

### Week 6 — CI/CD and Version Control
- **Git Branching Strategy**: `BRANCHING_STRATEGY.md` defines a GitFlow-Lite model (`main`, `develop`, `feature/*`, `fix/*`, `data/*`, `hotfix/*`, `release/*`) with Conventional Commits.
- **Unit Tests for Transformations**: `tests/test_transformations.py` with 28 pure unit tests covering `transform()`, row hashing, CDC, incremental watermark loading, and edge cases.
- **GitHub Actions CI/CD Pipeline**: `.github/workflows/ci.yml` running 5 automated jobs: `compile-check`, `lint` (flake8), `unit-tests` (pytest + coverage), `integration-tests`, and `ci-summary`.

---

## Notes on environment choices

This was built to be **runnable anywhere with zero external services** (no Docker, no cloud
accounts, no API keys) so it can be graded/demoed on any machine:

| Course plan asks for | Used here | Why |
|---|---|---|
| RDBMS | SQLite | Zero-setup, full SQL, matches course plan's OLTP examples |
| NoSQL | TinyDB | Pure-Python document store, same query concepts as MongoDB |
| OLAP / data cubes | DuckDB | Native `GROUP BY CUBE`, columnar, embedded |
| REST API | FastAPI (local) | A genuine HTTP service — the pipeline makes real network
  calls, just against localhost instead of a third-party API |

If you'd prefer to swap any of these for "real" Postgres/MongoDB/a public API for your
viva, the extraction/loading functions are isolated enough (`extract_rdbms()`,
`extract_nosql()`, `extract_rest_api()` in `week2_etl/pipeline_week2.py`) that only the
connection logic needs to change — the rest of the pipeline is source-agnostic.
