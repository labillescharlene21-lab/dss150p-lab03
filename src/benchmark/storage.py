import pandas as pd
import json
import time
import os
import csv
import logging
import platform
import statistics
from pathlib import Path
from src.config import path_for, SETTINGS, DB

logger = logging.getLogger(__name__)


def _time_it(func, *args, **kwargs):
    """Time a function call and return (result, elapsed_seconds)."""
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return result, elapsed


def _median(values):
    """Return median of a list of numbers."""
    return statistics.median(values)


def run_benchmark(curated_path, output_dir, repeats: int = 5):
    """Compare the same logical dataset in CSV, JSON Lines, Parquet, and PostgreSQL."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(curated_path)
    filter_status = SETTINGS['storage_benchmark']['filter_status']
    row_count = len(df)

    logger.info("Benchmarking %d rows, %d repeats, filter=%s", row_count, repeats, filter_status)

    results = []

    # ---- Hardware/OS context ----
    hw_context = f"{platform.system()} {platform.release()} | {platform.processor()} | Python {platform.python_version()}"

    # ========== CSV ==========
    csv_path = output_dir / 'benchmark.csv'
    _, write_time = _time_it(lambda: df.to_csv(csv_path, index=False))
    csv_size = os.path.getsize(csv_path)

    read_times = []
    for _ in range(repeats):
        _, t = _time_it(lambda: pd.read_csv(csv_path))
        read_times.append(t)

    filter_times = []
    for _ in range(repeats):
        _, t = _time_it(lambda: pd.read_csv(csv_path).query(f"status == '{filter_status}'"))
        filter_times.append(t)

    filtered_count = len(pd.read_csv(csv_path).query(f"status == '{filter_status}'"))

    results.append({
        'format': 'CSV',
        'size_bytes': csv_size,
        'write_time_s': round(write_time, 6),
        'full_read_median_s': round(_median(read_times), 6),
        'filtered_read_median_s': round(_median(filter_times), 6),
        'row_count': row_count,
        'filtered_row_count': filtered_count,
        'hardware_context': hw_context,
    })

    # ========== JSON Lines ==========
    jsonl_path = output_dir / 'benchmark.jsonl'
    _, write_time = _time_it(lambda: df.to_json(jsonl_path, orient='records', lines=True, date_format='iso'))
    jsonl_size = os.path.getsize(jsonl_path)

    read_times = []
    for _ in range(repeats):
        _, t = _time_it(lambda: pd.read_json(jsonl_path, lines=True))
        read_times.append(t)

    filter_times = []
    for _ in range(repeats):
        _, t = _time_it(lambda: pd.read_json(jsonl_path, lines=True).query(f"status == '{filter_status}'"))
        filter_times.append(t)

    results.append({
        'format': 'JSON Lines',
        'size_bytes': jsonl_size,
        'write_time_s': round(write_time, 6),
        'full_read_median_s': round(_median(read_times), 6),
        'filtered_read_median_s': round(_median(filter_times), 6),
        'row_count': row_count,
        'filtered_row_count': filtered_count,
        'hardware_context': hw_context,
    })

    # ========== Parquet ==========
    parquet_path = output_dir / 'benchmark.parquet'
    _, write_time = _time_it(lambda: df.to_parquet(parquet_path, index=False, compression='snappy'))
    parquet_size = os.path.getsize(parquet_path)

    read_times = []
    for _ in range(repeats):
        _, t = _time_it(lambda: pd.read_parquet(parquet_path))
        read_times.append(t)

    filter_times = []
    for _ in range(repeats):
        _, t = _time_it(lambda: pd.read_parquet(parquet_path, filters=[('status', '==', filter_status)]))
        filter_times.append(t)

    results.append({
        'format': 'Parquet',
        'size_bytes': parquet_size,
        'write_time_s': round(write_time, 6),
        'full_read_median_s': round(_median(read_times), 6),
        'filtered_read_median_s': round(_median(filter_times), 6),
        'row_count': row_count,
        'filtered_row_count': filtered_count,
        'hardware_context': hw_context,
    })

    # ========== PostgreSQL ==========
    try:
        import psycopg
        conn = psycopg.connect(
            host=DB['host'], port=DB['port'],
            dbname=DB['dbname'], user=DB['user'], password=DB['password'],
        )

        # Write time (already loaded, so measure a count query as baseline)
        _, write_time = _time_it(
            lambda: conn.execute("SELECT COUNT(*) FROM curated.sales_order_lines").fetchone()
        )

        # Full read
        read_times = []
        for _ in range(repeats):
            _, t = _time_it(
                lambda: conn.execute("SELECT * FROM curated.sales_order_lines").fetchall()
            )
            read_times.append(t)

        # Filtered read
        filter_times = []
        for _ in range(repeats):
            _, t = _time_it(
                lambda: conn.execute(
                    "SELECT * FROM curated.sales_order_lines WHERE status = %s", (filter_status,)
                ).fetchall()
            )
            filter_times.append(t)

        # Table size
        size_row = conn.execute(
            "SELECT pg_total_relation_size('curated.sales_order_lines')"
        ).fetchone()
        pg_size = size_row[0] if size_row else 'N/A'

        conn.close()

        results.append({
            'format': 'PostgreSQL',
            'size_bytes': pg_size,
            'write_time_s': round(write_time, 6),
            'full_read_median_s': round(_median(read_times), 6),
            'filtered_read_median_s': round(_median(filter_times), 6),
            'row_count': row_count,
            'filtered_row_count': filtered_count,
            'hardware_context': hw_context,
        })
    except Exception as e:
        logger.warning("PostgreSQL benchmark skipped: %s", e)
        results.append({
            'format': 'PostgreSQL',
            'size_bytes': 'N/A',
            'write_time_s': 'N/A',
            'full_read_median_s': 'N/A',
            'filtered_read_median_s': 'N/A',
            'row_count': row_count,
            'filtered_row_count': filtered_count,
            'hardware_context': hw_context,
        })

    # ========== Save results ==========
    results_path = output_dir / 'benchmark_results.csv'
    pd.DataFrame(results).to_csv(results_path, index=False)
    logger.info("Benchmark results saved to %s", results_path)

    for r in results:
        logger.info(
            "  %s: size=%s, write=%.4fs, read=%.4fs, filter=%.4fs",
            r['format'], r['size_bytes'],
            r['write_time_s'] if isinstance(r['write_time_s'], float) else 0,
            r['full_read_median_s'] if isinstance(r['full_read_median_s'], float) else 0,
            r['filtered_read_median_s'] if isinstance(r['filtered_read_median_s'], float) else 0,
        )

    return results


def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month."""
    output_dir = Path(output_dir)
    df = df.copy()

    # Ensure order_timestamp is datetime
    df['order_timestamp'] = pd.to_datetime(df['order_timestamp'], utc=True)

    # Create partition columns
    df['order_year'] = df['order_timestamp'].dt.year
    df['order_month'] = df['order_timestamp'].dt.month

    # Write partitioned parquet
    df.to_parquet(
        output_dir,
        engine='pyarrow',
        partition_cols=['order_year', 'order_month'],
        index=False,
    )

    logger.info("Partitioned Parquet written to %s", output_dir)
    return output_dir