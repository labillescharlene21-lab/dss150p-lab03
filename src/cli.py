import argparse
import logging
import pandas as pd
from src.config import PROJECT_ROOT, DB, SETTINGS, path_for
from src.common.audit import new_run_id

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
    b = sub.add_parser('benchmark'); b.add_argument('--repeats', type=int, default=5)
    p = sub.add_parser('load-partition'); p.add_argument('--year', type=int, required=True); p.add_argument('--month', type=int, required=True)
    sub.add_parser('run-all')
    args = parser.parse_args()

    if args.command == 'validate-env':
        print('PROJECT_ROOT=', PROJECT_ROOT)
        print('DB host/database=', DB['host'], DB['dbname'])
        print('Configured source=', SETTINGS['pipeline']['source_dir'])
        return

    run_id = new_run_id()
    logger.info("Pipeline run: %s (command: %s)", run_id, args.command)

    if args.command == 'extract':
        from src.extract.files import extract_sources
        raw_dir = extract_sources(run_id)
        logger.info("Extraction complete: %s", raw_dir)

    elif args.command == 'transform':
        from src.extract.files import extract_sources
        from src.transform.staging import build_staging
        from src.transform.curated import build_curated
        raw_dir = extract_sources(run_id)
        staging, quarantine = build_staging(raw_dir, run_id)
        curated, orphan_q = build_curated(staging, run_id)
        logger.info("Transform complete: %d curated rows", len(curated))

    elif args.command == 'load':
        from src.load.postgres import upsert_curated
        curated_path = path_for('curated_dir') / 'sales_order_lines.parquet'
        if not curated_path.exists():
            raise FileNotFoundError(f"[load] Curated file not found: {curated_path}. Run 'transform' first.")
        curated = pd.read_parquet(curated_path)
        count = upsert_curated(curated, run_id)
        logger.info("Loaded %d rows", count)

    elif args.command == 'validate':
        from src.validate.quality import validate_curated
        curated_path = path_for('curated_dir') / 'sales_order_lines.parquet'
        if not curated_path.exists():
            raise FileNotFoundError(f"[validate] Curated file not found: {curated_path}. Run 'transform' first.")
        curated = pd.read_parquet(curated_path)
        errors = validate_curated(curated)
        if errors:
            print(f"VALIDATION FAILED with {len(errors)} issue(s):")
            for e in errors:
                print(f"  - {e}")
        else:
            print(f"VALIDATION PASSED: {len(curated)} rows OK")

    elif args.command == 'benchmark':
        from src.benchmark.storage import run_benchmark
        curated_path = path_for('curated_dir') / 'sales_order_lines.parquet'
        if not curated_path.exists():
            raise FileNotFoundError(f"[benchmark] Curated file not found. Run 'run-all' first.")
        output_dir = path_for('benchmark_dir')
        run_benchmark(curated_path, output_dir, repeats=args.repeats)

    elif args.command == 'load-partition':
        from src.load.postgres import load_partition
        from src.benchmark.storage import write_partitioned_parquet
        curated_path = path_for('curated_dir') / 'sales_order_lines.parquet'
        if not curated_path.exists():
            raise FileNotFoundError(f"[load-partition] Curated file not found. Run 'run-all' first.")
        curated = pd.read_parquet(curated_path)
        # Ensure partitioned parquet exists
        partition_dir = path_for('partition_dir')
        if not partition_dir.exists():
            write_partitioned_parquet(curated, partition_dir)
        count = load_partition(curated, args.year, args.month, run_id)
        logger.info("Partition load complete: %d rows for %d-%02d", count, args.year, args.month)

    elif args.command == 'run-all':
        from src.extract.files import extract_sources
        from src.transform.staging import build_staging
        from src.transform.curated import build_curated
        from src.load.postgres import upsert_curated

        raw_dir = extract_sources(run_id)
        staging, quarantine = build_staging(raw_dir, run_id)
        curated, orphan_q = build_curated(staging, run_id)
        count = upsert_curated(curated, run_id)

        logger.info(
            "run-all complete: %d curated, %d quarantined, %d loaded",
            len(curated), len(quarantine) + len(orphan_q), count,
        )

    else:
        raise NotImplementedError(f'Unknown command: {args.command}')


if __name__ == '__main__':
    main()