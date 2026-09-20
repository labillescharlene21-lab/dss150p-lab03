import pandas as pd
import json
import logging
from pathlib import Path
from src.config import path_for, SETTINGS
from src.common.audit import utc_now_iso

logger = logging.getLogger(__name__)

ALLOWED_STATUSES = SETTINGS['quality']['allowed_order_statuses']
MIN_QTY = SETTINGS['quality']['min_quantity']
MAX_QTY = SETTINGS['quality']['max_quantity']


def _add_audit(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    """Add standard audit columns to a staging DataFrame."""
    df = df.copy()
    df['pipeline_run_id'] = run_id
    df['staged_at_utc'] = pd.Timestamp.now('UTC')
    return df


def _quarantine(df: pd.DataFrame, reason: str, run_id: str) -> pd.DataFrame:
    """Mark rows for quarantine with a reason."""
    q = df.copy()
    q['quarantine_reason'] = reason
    q['pipeline_run_id'] = run_id
    q['quarantined_at_utc'] = pd.Timestamp.now('UTC')
    return q


def _dedup(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Keep the most recent version of duplicate business keys."""
    df = df.sort_values('updated_at', ascending=False)
    return df.drop_duplicates(subset=[key], keep='first')


def stage_customers(raw_dir: Path, run_id: str):
    """Clean and type the customers dataset."""
    df = pd.read_csv(raw_dir / 'customers.csv')

    # Parse timestamps as UTC
    df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True)

    # Deduplicate by business key
    df = _dedup(df, 'customer_id')

    # Normalize: lowercase/trim email, trim/title-case city
    df['email'] = df['email'].astype(str).str.strip().str.lower()
    df.loc[df['email'] == 'nan', 'email'] = None  # preserve missing email
    df['city'] = df['city'].astype(str).str.strip().str.title()
    df.loc[df['city'] == 'Nan', 'city'] = None

    df = _add_audit(df, run_id)
    logger.info("Staged %d customers", len(df))
    return df, pd.DataFrame()  # no quarantine for customers


def stage_products(raw_dir: Path, run_id: str):
    """Clean, flatten, and type the products dataset."""
    with open(raw_dir / 'products.json', encoding='utf-8') as f:
        data = json.load(f)
    df = pd.json_normalize(data)

    # Rename nested category columns
    df = df.rename(columns={
        'category.name': 'category_name',
        'category.department': 'category_department',
    })

    # Parse price as numeric
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')

    # Parse timestamp
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True)

    # Quarantine negative or invalid prices
    bad_price = df['unit_price'].isna() | (df['unit_price'] < 0)
    quarantine = _quarantine(df[bad_price], 'invalid_or_negative_price', run_id)
    df = df[~bad_price].copy()

    # Deduplicate by business key
    df = _dedup(df, 'product_id')

    df = _add_audit(df, run_id)
    logger.info("Staged %d products, quarantined %d", len(df), len(quarantine))
    return df, quarantine


def stage_orders(raw_dir: Path, run_id: str):
    """Clean and type the orders dataset."""
    df = pd.read_csv(raw_dir / 'orders.csv')

    # Parse timestamps as UTC
    df['order_timestamp'] = pd.to_datetime(df['order_timestamp'], utc=True)
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True)

    # Parse quantity as numeric
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

    # Parse discount_pct and unit_price as numeric
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['discount_pct'] = pd.to_numeric(df['discount_pct'], errors='coerce').fillna(0)

    # Identify invalid records
    bad_qty = df['quantity'].isna() | (df['quantity'] < MIN_QTY) | (df['quantity'] > MAX_QTY)
    bad_status = ~df['status'].isin(ALLOWED_STATUSES)
    bad_mask = bad_qty | bad_status

    # Build quarantine with specific reasons
    quarantine_parts = []
    if bad_qty.any():
        quarantine_parts.append(
            _quarantine(df[bad_qty], f'quantity_out_of_range_{MIN_QTY}_{MAX_QTY}', run_id)
        )
    if bad_status.any():
        quarantine_parts.append(
            _quarantine(df[bad_status & ~bad_qty], 'invalid_order_status', run_id)
        )

    quarantine = pd.concat(quarantine_parts, ignore_index=True) if quarantine_parts else pd.DataFrame()
    df = df[~bad_mask].copy()

    # Deduplicate by business key
    df = _dedup(df, 'order_id')

    # Ensure quantity is integer after cleaning
    df['quantity'] = df['quantity'].astype(int)

    df = _add_audit(df, run_id)
    logger.info("Staged %d orders, quarantined %d", len(df), len(quarantine))
    return df, quarantine


def build_staging(raw_dir, run_id: str):
    """Create cleaned, typed staging datasets and save as Parquet."""
    raw_dir = Path(raw_dir)
    staging_dir = path_for('staging_dir')
    quarantine_dir = path_for('quarantine_dir')
    staging_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    # Stage each dataset
    customers, q_cust = stage_customers(raw_dir, run_id)
    products, q_prod = stage_products(raw_dir, run_id)
    orders, q_ord = stage_orders(raw_dir, run_id)

    # Save staging as Parquet
    customers.to_parquet(staging_dir / 'customers.parquet', index=False)
    products.to_parquet(staging_dir / 'products.parquet', index=False)
    orders.to_parquet(staging_dir / 'orders.parquet', index=False)

    # Combine and save quarantine
    all_quarantine = pd.concat([q_cust, q_prod, q_ord], ignore_index=True)
    if not all_quarantine.empty:
        all_quarantine.to_parquet(quarantine_dir / 'quarantine.parquet', index=False)

    staging = {
        'customers': customers,
        'products': products,
        'orders': orders,
    }

    logger.info(
        "Staging complete: %d customers, %d products, %d orders, %d quarantined",
        len(customers), len(products), len(orders), len(all_quarantine),
    )
    return staging, all_quarantine