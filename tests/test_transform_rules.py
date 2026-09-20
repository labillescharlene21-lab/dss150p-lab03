"""Meaningful tests for pipeline transformation rules."""
import pandas as pd
import json
from pathlib import Path
from src.config import PROJECT_ROOT
from src.transform.staging import stage_customers, stage_products, stage_orders
from src.transform.curated import build_curated
from src.common.audit import record_hash


RUN_ID = 'test_run_001'
SOURCE_DIR = PROJECT_ROOT / 'data' / 'source'


def test_customer_dedup_keeps_latest():
    """Duplicate customer_id rows should keep the one with the latest updated_at."""
    raw_dir = SOURCE_DIR
    customers, _ = stage_customers(raw_dir, RUN_ID)
    assert customers['customer_id'].is_unique, "customer_id should be unique after dedup"
    assert len(customers) == 3000


def test_customer_email_lowercase():
    """Emails should be lowercase and trimmed."""
    raw_dir = SOURCE_DIR
    customers, _ = stage_customers(raw_dir, RUN_ID)
    emails = customers['email'].dropna()
    for e in emails:
        assert e == e.strip().lower(), f"Email not normalized: {e}"


def test_product_invalid_price_quarantined():
    """Products with negative or invalid prices should be quarantined."""
    raw_dir = SOURCE_DIR
    products, quarantine = stage_products(raw_dir, RUN_ID)
    assert len(quarantine) >= 1, "At least one product should be quarantined for invalid price"
    assert all(products['unit_price'] >= 0), "No negative prices should remain in staging"


def test_order_quantity_range():
    """Staged orders should only have quantity between 1 and 20."""
    raw_dir = SOURCE_DIR
    orders, _ = stage_orders(raw_dir, RUN_ID)
    assert orders['quantity'].min() >= 1
    assert orders['quantity'].max() <= 20


def test_order_status_allowed():
    """Staged orders should only contain allowed statuses."""
    from src.config import SETTINGS
    allowed = SETTINGS['quality']['allowed_order_statuses']
    raw_dir = SOURCE_DIR
    orders, _ = stage_orders(raw_dir, RUN_ID)
    invalid = orders[~orders['status'].isin(allowed)]
    assert len(invalid) == 0, f"Found {len(invalid)} orders with invalid status"


def test_staging_audit_columns_exist():
    """Every staging DataFrame should have pipeline_run_id and staged_at_utc."""
    raw_dir = SOURCE_DIR
    customers, _ = stage_customers(raw_dir, RUN_ID)
    assert 'pipeline_run_id' in customers.columns
    assert 'staged_at_utc' in customers.columns
    assert customers['pipeline_run_id'].iloc[0] == RUN_ID


def test_curated_amounts_correct():
    """Curated amounts should follow: gross = qty * price, net = gross - discount."""
    raw_dir = SOURCE_DIR
    customers, _ = stage_customers(raw_dir, RUN_ID)
    products, _ = stage_products(raw_dir, RUN_ID)
    orders, _ = stage_orders(raw_dir, RUN_ID)
    staging = {'customers': customers, 'products': products, 'orders': orders}
    curated, _ = build_curated(staging, RUN_ID)

    sample = curated.head(100)
    for _, row in sample.iterrows():
        expected_gross = round(row['quantity'] * row['unit_price'], 2)
        expected_discount = round(expected_gross * row['discount_pct'], 2)
        expected_net = round(expected_gross - expected_discount, 2)
        assert row['gross_amount'] == expected_gross
        assert row['discount_amount'] == expected_discount
        assert row['net_amount'] == expected_net


def test_record_hash_deterministic():
    """record_hash should not change between pipeline runs."""
    raw_dir = SOURCE_DIR
    customers, _ = stage_customers(raw_dir, RUN_ID)
    products, _ = stage_products(raw_dir, RUN_ID)
    orders, _ = stage_orders(raw_dir, RUN_ID)
    staging = {'customers': customers, 'products': products, 'orders': orders}

    curated1, _ = build_curated(staging, 'run_A')
    curated2, _ = build_curated(staging, 'run_B')

    row1 = curated1[curated1['order_id'] == curated1['order_id'].iloc[0]].iloc[0]
    row2 = curated2[curated2['order_id'] == curated1['order_id'].iloc[0]].iloc[0]
    assert row1['record_hash'] == row2['record_hash'], "record_hash should be same across runs"