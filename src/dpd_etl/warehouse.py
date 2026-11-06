"""Publish accepted Spark outputs into typed warehouse sources, one date at a time."""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import date
from pathlib import Path

import duckdb

# Keep a small, explicit contract rather than leaking every detector work column.
TABLES = {
    'products': ('drug_products', {
        'drug_code': 'INTEGER', 'din': 'VARCHAR(8)', 'brand_name': 'VARCHAR(1000)',
        'drug_class': 'VARCHAR(100)', 'company_code': 'VARCHAR(100)',
        'company_name': 'VARCHAR(1000)', 'atc_group': 'VARCHAR(20)',
        'current_status': 'VARCHAR(100)', 'snapshot_date': 'DATE',
    }),
    'ingredients': ('drug_ingredients', {
        'drug_code': 'INTEGER', 'ingredient': 'VARCHAR(1000)', 'strength': 'VARCHAR(1000)',
        'strength_unit': 'VARCHAR(100)', 'snapshot_date': 'DATE',
    }),
    'pairs': ('product_links', {
        'left_id': 'INTEGER', 'right_id': 'INTEGER', 'score': 'DOUBLE',
        'is_match': 'BOOLEAN', 'reason': 'VARCHAR(100)', 'snapshot_date': 'DATE',
    }),
    'strengths': ('strength_anomalies', {
        'drug_code': 'INTEGER', 'ingredient': 'VARCHAR(1000)', 'strength': 'VARCHAR(1000)',
        'strength_unit': 'VARCHAR(100)', 'is_anomaly': 'BOOLEAN', 'snapshot_date': 'DATE',
    }),
    'manifest': (None, {
        'table_name': 'VARCHAR(100)', 'row_count': 'INTEGER', 'snapshot_date': 'DATE',
    }),
}


def valid_day(value):
    if date.fromisoformat(value).isoformat() != value:
        raise ValueError('snapshot date must be YYYY-MM-DD')
    return value


def identifier(value):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', value):
        raise ValueError('use an unquoted SQL identifier')
    return value


def read_snapshot(parquet_root: Path, day: str) -> dict:
    """Read the exact files Spark wrote, including typed empty candidate tables."""
    valid_day(day)
    result = {}
    with duckdb.connect() as con:
        for name, (source, columns) in TABLES.items():
            if source is None:
                continue
            paths = sorted((parquet_root / source / f'snapshot_date={day}').glob('*.parquet'))
            if not paths:
                raise FileNotFoundError(f'missing Spark output: {source} for {day}')
            select = ', '.join(
                f'CAST({col} AS {kind}) AS {col}' for col, kind in columns.items()
                if col != 'snapshot_date'
            )
            result[name] = con.execute(
                f'SELECT {select}, CAST(? AS DATE) snapshot_date '
                'FROM read_parquet(?, hive_partitioning=false)',
                [day, [str(p) for p in paths]],
            ).fetchall()
    if not result['products']:
        raise ValueError('cannot publish an empty catalogue')
    result['manifest'] = [(name, len(rows), date.fromisoformat(day))
                          for name, rows in result.items()]
    return result


def publish(connection, rows: dict, day: str, *, schema='RAW', snowflake=True):
    """DDL is outside the DML transaction (Snowflake DDL commits implicitly).

    All four sources and their reconciliation manifest replace together. Serialize
    publishers for the same date. Never store credentials in this function's logs.
    """
    valid_day(day)
    schema = identifier(schema)
    cursor = connection.cursor()
    marker = '%s' if snowflake else '?'
    try:
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
        for name, (_, columns) in TABLES.items():
            ddl = ', '.join(f'{col} {kind}' for col, kind in columns.items())
            cursor.execute(f'CREATE TABLE IF NOT EXISTS {schema}.{name} ({ddl})')
        cursor.execute('BEGIN TRANSACTION')
        try:
            for name, (_, columns) in TABLES.items():
                cursor.execute(f'DELETE FROM {schema}.{name} WHERE snapshot_date = {marker}', [day])
                values = rows[name]
                if values:
                    placeholders = ', '.join([marker] * len(columns))
                    cursor.executemany(f'INSERT INTO {schema}.{name} VALUES ({placeholders})', values)
                actual = cursor.execute(
                    f'SELECT COUNT(*) FROM {schema}.{name} WHERE snapshot_date = {marker}', [day]
                ).fetchone()[0]
                if actual != len(values):
                    raise ValueError(f'{name}: warehouse {actual} != Spark output {len(values)}')
            cursor.execute('COMMIT')
        except Exception:
            cursor.execute('ROLLBACK')
            raise
    finally:
        cursor.close()
    return {name: len(values) for name, values in rows.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--parquet-root', type=Path, required=True)
    parser.add_argument('--snapshot-date', required=True)
    parser.add_argument('--local-db', type=Path, help='DuckDB sandbox, no cloud account required')
    args = parser.parse_args()
    rows = read_snapshot(args.parquet_root, args.snapshot_date)
    if args.local_db:
        args.local_db.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(args.local_db))
    else:
        import snowflake.connector
        con = snowflake.connector.connect(
            account=os.environ['SNOWFLAKE_ACCOUNT'], user=os.environ['SNOWFLAKE_USER'],
            password=os.environ['SNOWFLAKE_PASSWORD'],
            warehouse=os.environ['SNOWFLAKE_WAREHOUSE'], database=os.environ['SNOWFLAKE_DATABASE'],
            role=os.environ['SNOWFLAKE_ROLE'], autocommit=False,
        )
    try:
        print(json.dumps(publish(con, rows, args.snapshot_date,
                                 schema=os.environ.get('DPD_RAW_SCHEMA', 'RAW'),
                                 snowflake=not bool(args.local_db)), indent=2))
    finally:
        con.close()


if __name__ == '__main__':
    main()
