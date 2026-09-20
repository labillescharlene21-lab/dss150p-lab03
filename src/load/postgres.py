import psycopg
import logging
from src.config import DB
from src.common.audit import utc_now_iso

logger = logging.getLogger(__name__)


def get_connection():
    """Create a PostgreSQL connection."""
    return psycopg.connect(
        host=DB['host'],
        port=DB['port'],
        dbname=DB['dbname'],
        user=DB['user'],
        password=DB['password'],
    )


def upsert_curated(df, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics."""
    if df.empty:
        logger.warning("No curated rows to load.")
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    upserted = 0

    try:
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO curated.sales_order_lines (
                    order_id, customer_id, product_id, order_timestamp,
                    customer_city, customer_tier, product_name, category, brand,
                    quantity, unit_price, discount_pct,
                    gross_amount, discount_amount, net_amount, status,
                    source_updated_at, pipeline_run_id, processed_at_utc, record_hash
                ) VALUES (
                    %(order_id)s, %(customer_id)s, %(product_id)s, %(order_timestamp)s,
                    %(customer_city)s, %(customer_tier)s, %(product_name)s, %(category)s, %(brand)s,
                    %(quantity)s, %(unit_price)s, %(discount_pct)s,
                    %(gross_amount)s, %(discount_amount)s, %(net_amount)s, %(status)s,
                    %(source_updated_at)s, %(pipeline_run_id)s, %(processed_at_utc)s, %(record_hash)s
                )
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    product_id = EXCLUDED.product_id,
                    order_timestamp = EXCLUDED.order_timestamp,
                    customer_city = EXCLUDED.customer_city,
                    customer_tier = EXCLUDED.customer_tier,
                    product_name = EXCLUDED.product_name,
                    category = EXCLUDED.category,
                    brand = EXCLUDED.brand,
                    quantity = EXCLUDED.quantity,
                    unit_price = EXCLUDED.unit_price,
                    discount_pct = EXCLUDED.discount_pct,
                    gross_amount = EXCLUDED.gross_amount,
                    discount_amount = EXCLUDED.discount_amount,
                    net_amount = EXCLUDED.net_amount,
                    status = EXCLUDED.status,
                    source_updated_at = EXCLUDED.source_updated_at,
                    pipeline_run_id = EXCLUDED.pipeline_run_id,
                    processed_at_utc = EXCLUDED.processed_at_utc,
                    record_hash = EXCLUDED.record_hash
                WHERE curated.sales_order_lines.record_hash != EXCLUDED.record_hash
            """, row.to_dict())
            upserted += 1

        conn.commit()
        logger.info("Upserted %d rows into curated.sales_order_lines", upserted)

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"[load] PostgreSQL upsert failed: {e}") from e
    finally:
        cursor.close()
        conn.close()

    return upserted


def record_partition_load(year: int, month: int, row_count: int, run_id: str):
    """Record a partition load in audit.partition_loads."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        partition_key = f"{year}-{month:02d}"
        cursor.execute("""
            INSERT INTO audit.partition_loads (partition_key, loaded_at_utc, row_count, pipeline_run_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (partition_key) DO UPDATE SET
                loaded_at_utc = EXCLUDED.loaded_at_utc,
                row_count = EXCLUDED.row_count,
                pipeline_run_id = EXCLUDED.pipeline_run_id
        """, (partition_key, utc_now_iso(), row_count, run_id))
        conn.commit()
        logger.info("Recorded partition load: %s (%d rows)", partition_key, row_count)
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"[load] Partition audit failed: {e}") from e
    finally:
        cursor.close()
        conn.close()


def load_partition(df, year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads."""
    # Filter to the selected partition
    df = df.copy()
    df['order_timestamp'] = df['order_timestamp'] if hasattr(df['order_timestamp'], 'dt') else df['order_timestamp']
    partition = df[
        (df['order_timestamp'].dt.year == year) &
        (df['order_timestamp'].dt.month == month)
    ]

    if partition.empty:
        logger.warning("No rows for partition %d-%02d", year, month)
        return 0

    count = upsert_curated(partition, run_id)
    record_partition_load(year, month, len(partition), run_id)
    return count