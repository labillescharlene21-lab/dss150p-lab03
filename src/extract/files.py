from pathlib import Path
import shutil
import logging
from src.config import path_for

logger = logging.getLogger(__name__)


def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into a run-specific raw directory."""
    source_dir = path_for('source_dir')
    raw_dir = path_for('raw_dir') / f'run_id={run_id}'

    if not source_dir.exists():
        raise FileNotFoundError(f"[extract] Source directory not found: {source_dir}")

    raw_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Extracting sources into %s", raw_dir)

    expected_files = ['customers.csv', 'products.json', 'orders.csv']

    for filename in expected_files:
        src = source_dir / filename
        if not src.exists():
            raise FileNotFoundError(f"[extract] Required source file missing: {src}")
        shutil.copy2(src, raw_dir / filename)
        logger.info("Copied %s", filename)

    return raw_dir