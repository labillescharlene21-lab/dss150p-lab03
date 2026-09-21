# DSS150P Laboratory 3 — ETL Pipeline

## Overview
A modular ETL pipeline that extracts source data (CSV/JSON), transforms it through raw → staging → curated layers, loads it into PostgreSQL, and validates the output. Includes storage format benchmarks and Apache Airflow orchestration.

## Quick Start

### Prerequisites
- Python 3.10+
- Docker Desktop
- Git

### Setup
```bash
git clone <repo-url>
cd dss150p-lab03
python -m venv .venv
# Windows: .venv\Scripts\activate
# Mac/Linux: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then edit .env with your database password
```

### Run the Pipeline

```bash
# Goal 1: Validate environment
python -m src.cli validate-env
docker compose build pipeline
docker compose up -d postgres
docker compose run --rm pipeline python -m src.cli validate-env

# Goal 2: Full ETL pipeline
python -m src.cli run-all
python -m src.cli load
python -m src.cli validate

# Goal 3: Benchmark and partitioning
python -m src.cli benchmark --repeats 5
python -m src.cli load-partition --year 2026 --month 1

# Goal 4: Airflow
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
# Open http://localhost:8080 (admin/admin)
```

## Project Structure

| Directory | Purpose |
|-----------|---------|
| `src/extract/` | Copies source files into run-specific raw snapshots |
| `src/transform/` | Staging cleanup, curated joins and business calculations |
| `src/load/` | PostgreSQL UPSERT with rerun-safe record_hash |
| `src/validate/` | Data quality assertions on curated output |
| `src/benchmark/` | Storage format benchmarks and partitioned Parquet |
| `src/common/` | Shared utilities (audit columns, run IDs, hashing) |
| `src/cli.py` | Thin CLI entry point wiring all modules |
| `dags/` | Airflow DAG delegating to src.cli commands |
| `config/` | Non-secret settings (settings.yml) |
| `sql/init/` | PostgreSQL schema initialization |

## Configuration Separation
- **`config/settings.yml`**: Non-secret pipeline defaults (directories, allowed statuses, quantity ranges)
- **`.env`**: Environment-specific secrets (database password, host). Never committed to Git.
- **`src/config.py`**: Single module that reads both and exposes them to the rest of the code.
- No password appears in any committed Python, YAML, SQL, or DAG file.

## Goal 2 — ETL Pipeline Summary

### Data Flow
| Layer | Records | Description |
|-------|---------|-------------|
| Source | 3,003 customers, 601 products, 50,005 orders | Raw files unchanged |
| Staging | 3,000 customers, 599 products, 49,998 orders | Deduped, cleaned, typed |
| Quarantine (staging) | 3 records | 1 invalid price, 2 invalid orders |
| Curated | 49,897 rows | Joined, amounts calculated |
| Quarantine (orphans) | 101 records | Orphan customer/product references |

### Rerun Safety
Running `python -m src.cli load` multiple times produces no duplicates:

total | distinct_orders
49897 | 49897

The UPSERT uses `order_id` as the conflict key and `record_hash` to skip updates when business content is unchanged.

## Goal 3 — Storage Benchmark Results

| Format | Size | Full Read (median) | Filtered Read (median) |
|--------|------|--------------------|----------------------|
| CSV | 14.9 MB | 0.375s | 0.364s |
| JSON Lines | 30.2 MB | 0.902s | 1.117s |
| Parquet | 5.4 MB | 0.071s | 0.033s |
| PostgreSQL | 16.5 MB | 0.492s | 0.084s |

### Goal 3 Analysis Questions

**1. Which file format was smallest, and why?**
Parquet is the smallest format (5.4 MB). It employs column-wise data storage format with Snappy compression, encoding columns according to their types (dictionary encoding for strings such as status and city, run-length encoding for repetitive elements). The CSV format holds data in plain text form, while JSON Lines additionally contains field names on every line.

**2. Which was fastest for a full read? Does that imply best for every workload?**
Parquet proved to be faster for complete scans. This, however, does not imply that Parquet will be ideal for all kinds of workloads. PostgreSQL allows parallel accesses and transaction management which is not possible using flat files. CSV, on the other hand, can be read using any tool.

**3. How did filtered retrieval differ between Parquet and PostgreSQL?**
The Parquet filtered reads operation was quicker (0.033s compared to 0.084s) as it uses per-column minimum and maximum values and skips row groups that do not have the desired value. The Postgres query operation performed a sequential scan. It is possible that adding an index (using `CREATE INDEX ON curated.sales_order_lines(status)`) might improve the performance of Postgres.

**4. Why is JSON Lines more pipeline-friendly than a giant JSON array?**
JSON Lines can be added to without having to read the whole file, can be parsed line by line with constant memory use, and can be distributed among workers. A large JSON array will need to be loaded completely into memory before it is possible to parse it.

**5. What happens if a partition key has extremely high cardinality?**
If too many partitions (partitioning by order_id) are created, there will be a huge number of very small files/directories, which would increase the overhead on metadata and file handle. Queries scanning several partitions would slow down, and storage would be wasted on metadata. Partition keys need to have moderate cardinality.

## Goal 4 — Airflow Orchestration

### DAG Configuration
| Setting | Value | Reasoning |
|---------|-------|-----------|
| Schedule | `0 2 * * *` | Runs daily at 2 AM when system load is low |
| Catchup | `False` | Prevents backfilling all historical dates on first deploy |
| Retries | 2 | Handles transient failures without manual intervention |
| Retry delay | 1 minute | Gives temporary issues time to resolve |
| Execution timeout | 30 minutes | Prevents tasks from hanging indefinitely |

### Task Dependencies
`extract → transform → load → validate`

Each task passes the same `PIPELINE_RUN_ID` (Airflow's `run_id`) so all stages share one consistent identifier.

### Failure/Recovery
- Deliberately renamed `orders.csv` to trigger a FileNotFoundError
- Extract task failed, retried 2 times, then marked as failed
- Failure callback logged task name, DAG ID, run ID, and error message
- Restored the file and retriggered — pipeline completed successfully
- No manual database cleanup needed because the pipeline is idempotent

## Technical Reflection

### Modularity
Each module has one responsibility: extract copies files, transform cleans data, load writes to PostgreSQL, validate checks quality. The CLI is a thin orchestrator that wires them together. This means any module can be tested, replaced, or reused independently.

### Idempotency
The pipeline can be run multiple times without creating duplicate data. The UPSERT uses `order_id` as the conflict key and `record_hash` to avoid unnecessary updates. Partition loads use the same pattern. This is critical for retry safety — if a task fails and Airflow retries it, no damage occurs.

### Storage Trade-offs
No single format is universally best. Parquet excels at analytical reads with its columnar layout and compression. PostgreSQL provides transactions, indexing, and concurrent access. CSV offers universal compatibility. JSON Lines supports streaming and schema flexibility. The choice depends on the workload.

### Orchestration vs Business Logic
The Airflow DAG defines execution order, retries, scheduling, and failure handling — but contains zero business logic. All transformation rules live in `src/`. This makes the pipeline testable without Airflow, and the DAG maintainable without understanding business rules.

## Technical Questions

**1. Why is record_hash useful, and which columns should not be included?**
record_hash generates a hash value for the business records. In the event that the rerun generates the same hash, the UPSERT process will not update. This is because the columns `processed_at_utc` and `pipeline_run_id` should not be included in the hash since their values keep changing with each run.

**2. Why preserve raw data even when curated outputs exist?**
Raw data supports the possibility of replication — in case there is an error in the transformation, one can start from the very snapshot of the data. Moreover, raw data gives a history of the source data to check whether everything was done properly.

**3. Difference between data-quality rejection and system exception?**
Data quality rejection (quarantine) involves that the data is bad in itself (like negative prices or orphaned foreign key) – the pipeline continues and the erroneous data is stored to be reviewed later. On the other hand, system exception happens when there is an error in the pipeline architecture itself (for example, lost database connection).

**4. Why might Parquet outperform CSV for analytical workloads?**
The Parquet format uses column storage for data. Reading of the required columns results in skipping the unnecessary data. Efficient compression is done according to the column types. The minimum/maximum statistics are stored for predicate pushdown. The CSV format requires reading of all bytes of all rows regardless of the number of columns needed.

**5. Why is a DAG with all transformation logic harder to maintain?**
Embedding the business rules in the DAG results in all changes to the data necessitating the modification of the orchestration file that also handles scheduling, retry attempts, and dependencies. Testing of the business logic requires the presence of an Airflow environment. There are two issues that should be separated because they have different considerations.

**6. How do retries interact with idempotency?**
A retry repeats a failed task. If there is no idempotency, then a retry can cause duplication of data (like when the failed task inserts 500 rows but fails to complete – a retry inserts another 500 rows). Idempotent UPSERT avoids data duplication.

**7. Trade-off of partitioning too aggressively?**
When the file system is over-partitioned, then a large number of files are created, and there will be some amount of filesystem overhead in every single file. If there are queries involving more than one partition, then there will be a need to open a lot of files.

**8. How to adapt if the source became an API or database?**
Reimplement the existing `src/extract/files.py` module to use an API call or database query to store data into the `data/raw/run_id=.../` format. Downstream processing (staging, curated, loading, and validation) will be exactly the same as they read data from the raw stage, not the source itself.