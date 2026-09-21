# Run Evidence

## Week 4
- Python version: 3.10
- Git status/log evidence:
  - 12+ commits with meaningful messages
  - `.env` not tracked by Git (in `.gitignore`)
  - `.env.example` committed with placeholder values
  - Branches: main, goal1-reproducible-environment, goal2-etl-pipeline, goal3-storage-benchmarks, goal4-airflow-orchestration
- Docker image/container evidence:
  - `docker compose build pipeline` — built successfully
  - `docker compose up -d postgres` — container healthy
  - `docker compose run --rm pipeline python -m src.cli validate-env` — ran successfully inside container
  - Schemas verified: staging, curated, audit, public
  - Table verified: curated.sales_order_lines
- External configuration evidence:
  - `config/settings.yml` — non-secret defaults (directories, allowed statuses, quantity ranges)
  - `.env` — environment-specific secrets (POSTGRES_PASSWORD, POSTGRES_HOST). Never committed.
  - `src/config.py` — the single module that reads both files and exposes settings to all other modules
  - No password appears in any committed Python, YAML, SQL, or DAG file

## Week 5
- Raw row counts: 3,003 customers.csv, 601 products.json, 50,005 orders.csv (copied unchanged)
- Staging row counts: 3,000 customers, 599 products, 49,998 orders
- Curated row counts: 49,897 sales_order_lines
- Quarantine row counts: 3 staging (1 invalid price, 2 invalid orders) + 101 orphan references = 104 total
- First load affected rows: 49,897 rows upserted
- Second rerun affected rows / evidence of idempotency:
  - Ran `python -m src.cli load` three times
  - PostgreSQL query: `SELECT COUNT(*) total, COUNT(DISTINCT order_id) distinct_orders FROM curated.sales_order_lines;`
  - Result: total = 49,897, distinct_orders = 49,897 — zero duplicates

## Week 6
- Benchmark table attached: yes (data/benchmarks/benchmark_results.csv)
  - CSV: 14.9 MB, full read 0.375s, filtered 0.364s
  - JSON Lines: 30.2 MB, full read 0.902s, filtered 1.117s
  - Parquet: 5.4 MB, full read 0.071s, filtered 0.033s
  - PostgreSQL: 16.5 MB, full read 0.492s, filtered 0.084s
- Partition selected: order_year=2026, order_month=1
- Partition row count: 2,506 rows
- PostgreSQL verification query:
  - `SELECT * FROM audit.partition_loads;` shows partition_key=2026-01, row_count=2506

## Week 7
- DAG ID: dss150p_sales_pipeline
- Schedule: `0 2 * * *` (daily at 2 AM — low system load, after business day ends)
- Parameters used:
  - Full mode: run_mode=full (default)
  - Partition mode: run_mode=partition, year=2026, month=1
- Successful run ID: visible in Airflow Grid view (all 4 tasks green)
- Deliberate failure run ID: triggered after renaming orders.csv
  - Extract task failed with FileNotFoundError
  - Retried 2 times (as configured), then marked failed
  - Failure callback printed: task name, DAG ID, run ID, error message
- Retry/failure-handling evidence:
  - Task log shows 3 attempts (1 original + 2 retries)
  - Failure callback output visible in logs
  - No data corruption occurred during failed run
- Final recovery run ID: triggered after restoring orders.csv
  - All 4 tasks completed successfully (green)
  - No manual database cleanup needed — pipeline is idempotent
