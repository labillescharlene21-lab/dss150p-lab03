import pandas as pd
import logging
from src.config import path_for
from src.common.audit import utc_now_iso, record_hash

logger = logging.getLogger(__name__)

HASH_COLUMNS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp',
    'customer_city', 'customer_tier', 'product_name', 'category', 'brand',
    'quantity', 'unit_price', 'discount_pct',
    'gross_amount', 'discount_amount', 'net_amount', 'status',
    'source_updated_at',
]


def build_curated(staging: dict, run_id: str):
    """Join staging datasets and create analysis-ready sales rows."""
    orders = staging['orders'].copy()
    customers = staging['customers'].copy()
    products = staging['products'].copy()

    quarantine_dir = path_for('quarantine_dir')
    curated_dir = path_for('curated_dir')
    curated_dir.mkdir(parents=True, exist_ok=True)

    # ----- Join orders to customers -----
    merged = orders.merge(
        customers[['customer_id', 'city', 'customer_tier']],
        on='customer_id',
        how='left',
        indicator=True,
    )

    # Quarantine orphan customer references
    orphan_cust = merged[merged['_merge'] == 'left_only'].copy()
    merged = merged[merged['_merge'] == 'both'].drop(columns=['_merge'])

    # ----- Join to products -----
    products_slim = products[['product_id', 'name', 'category_name', 'brand']].rename(
        columns={'name': 'product_name', 'category_name': 'category'}
    )
    merged = merged.merge(
        products_slim,
        on='product_id',
        how='left',
        indicator=True,
    )

    # Quarantine orphan product references
    orphan_prod = merged[merged['_merge'] == 'left_only'].copy()
    merged = merged[merged['_merge'] == 'both'].drop(columns=['_merge'])

    # Save orphan quarantine
    orphan_parts = []
    if len(orphan_cust):
        orphan_cust = orphan_cust.drop(columns=['_merge'], errors='ignore')
        orphan_cust['quarantine_reason'] = 'orphan_customer_reference'
        orphan_cust['pipeline_run_id'] = run_id
        orphan_cust['quarantined_at_utc'] = pd.Timestamp.now('UTC')
        orphan_parts.append(orphan_cust)
    if len(orphan_prod):
        orphan_prod = orphan_prod.drop(columns=['_merge'], errors='ignore')
        orphan_prod['quarantine_reason'] = 'orphan_product_reference'
        orphan_prod['pipeline_run_id'] = run_id
        orphan_prod['quarantined_at_utc'] = pd.Timestamp.now('UTC')
        orphan_parts.append(orphan_prod)

    orphan_quarantine = pd.concat(orphan_parts, ignore_index=True) if orphan_parts else pd.DataFrame()
    if not orphan_quarantine.empty:
        orphan_quarantine.to_parquet(quarantine_dir / 'orphan_quarantine.parquet', index=False)

    # ----- Rename columns to match DB schema -----
    merged = merged.rename(columns={'city': 'customer_city'})

    # ----- Calculate monetary amounts -----
    merged['gross_amount'] = merged['quantity'] * merged['unit_price']
    merged['discount_amount'] = merged['gross_amount'] * merged['discount_pct']
    merged['net_amount'] = merged['gross_amount'] - merged['discount_amount']

    # Round to 2 decimal places
    merged['gross_amount'] = merged['gross_amount'].round(2)
    merged['discount_amount'] = merged['discount_amount'].round(2)
    merged['net_amount'] = merged['net_amount'].round(2)

    # ----- Audit columns -----
    merged['source_updated_at'] = merged['updated_at']
    merged['pipeline_run_id'] = run_id
    merged['processed_at_utc'] = pd.Timestamp.now('UTC')

    # record_hash: deterministic on business content only
    merged['record_hash'] = merged.apply(
        lambda row: record_hash(row.to_dict(), HASH_COLUMNS), axis=1
    )

    # ----- Select final columns to match DB schema -----
    final_columns = [
        'order_id', 'customer_id', 'product_id', 'order_timestamp',
        'customer_city', 'customer_tier', 'product_name', 'category', 'brand',
        'quantity', 'unit_price', 'discount_pct',
        'gross_amount', 'discount_amount', 'net_amount', 'status',
        'source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash',
    ]
    curated = merged[final_columns].copy()

    # Save curated as Parquet
    curated.to_parquet(curated_dir / 'sales_order_lines.parquet', index=False)

    logger.info(
        "Curated complete: %d rows, %d orphans quarantined",
        len(curated), len(orphan_quarantine),
    )
    return curated, orphan_quarantine