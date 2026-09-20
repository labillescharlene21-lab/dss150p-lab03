import logging
from src.config import SETTINGS

logger = logging.getLogger(__name__)

ALLOWED_STATUSES = SETTINGS['quality']['allowed_order_statuses']
MIN_QTY = SETTINGS['quality']['min_quantity']
MAX_QTY = SETTINGS['quality']['max_quantity']


def validate_curated(df) -> list[str]:
    """Return a list of human-readable validation errors."""
    errors = []

    # Check order_id is non-null
    null_ids = df['order_id'].isna().sum()
    if null_ids > 0:
        errors.append(f"Found {null_ids} null order_id values")

    # Check order_id uniqueness
    dup_ids = df['order_id'].duplicated().sum()
    if dup_ids > 0:
        errors.append(f"Found {dup_ids} duplicate order_id values")

    # Check quantity range
    bad_qty = ((df['quantity'] < MIN_QTY) | (df['quantity'] > MAX_QTY)).sum()
    if bad_qty > 0:
        errors.append(f"Found {bad_qty} rows with quantity outside {MIN_QTY}-{MAX_QTY}")

    # Check non-negative amounts
    for col in ['gross_amount', 'discount_amount', 'net_amount']:
        neg = (df[col] < 0).sum()
        if neg > 0:
            errors.append(f"Found {neg} negative {col} values")

    # Check allowed statuses
    bad_status = (~df['status'].isin(ALLOWED_STATUSES)).sum()
    if bad_status > 0:
        errors.append(f"Found {bad_status} rows with invalid status")

    # Check required audit fields
    for col in ['pipeline_run_id', 'processed_at_utc', 'record_hash', 'source_updated_at']:
        nulls = df[col].isna().sum()
        if nulls > 0:
            errors.append(f"Found {nulls} null values in required field: {col}")

    if errors:
        for e in errors:
            logger.warning("[validate] %s", e)
    else:
        logger.info("[validate] All checks passed (%d rows)", len(df))

    return errors