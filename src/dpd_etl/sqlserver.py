"""Transactional SQL Server mirror of the checked dbt star schema."""
import argparse
import json
import os
from pathlib import Path

import duckdb

# Ordered parents before children. Tables enforce the same keys as dbt's tests.
DDL = {
    'dim_company': '''company_key varchar(150) NOT NULL PRIMARY KEY, snapshot_date date NOT NULL,
                     company_code varchar(100), company_name nvarchar(1000)''',
    'dim_ingredient': '''ingredient_key varchar(32) NOT NULL PRIMARY KEY, ingredient nvarchar(1000) NOT NULL''',
    'dim_product': '''product_key varchar(100) NOT NULL PRIMARY KEY, snapshot_date date NOT NULL,
        drug_code int NOT NULL, din char(8) NOT NULL CHECK (din NOT LIKE '%[^0-9]%'),
        brand_name nvarchar(1000), drug_class varchar(100), atc_group varchar(20),
        current_status varchar(100), company_key varchar(150) NOT NULL REFERENCES dbo.dim_company(company_key)''',
    'fct_product': '''product_key varchar(100) NOT NULL PRIMARY KEY REFERENCES dbo.dim_product(product_key),
        company_key varchar(150) NOT NULL REFERENCES dbo.dim_company(company_key), snapshot_date date NOT NULL,
        drug_class varchar(100), ingredient_records bigint NOT NULL CHECK (ingredient_records >= 0),
        flagged_strengths bigint NOT NULL CHECK (flagged_strengths >= 0)''',
    'fct_product_ingredient': '''product_ingredient_key varchar(150) NOT NULL PRIMARY KEY,
        product_key varchar(100) NOT NULL REFERENCES dbo.dim_product(product_key),
        ingredient_key varchar(32) NOT NULL REFERENCES dbo.dim_ingredient(ingredient_key), snapshot_date date NOT NULL,
        strength nvarchar(1000), strength_unit varchar(100)''',
    'fct_duplicate_pair': '''pair_key varchar(150) NOT NULL PRIMARY KEY,
        left_product_key varchar(100) REFERENCES dbo.dim_product(product_key),
        right_product_key varchar(100) REFERENCES dbo.dim_product(product_key), snapshot_date date NOT NULL,
        score float, is_match bit, reason varchar(100)''',
}
VIEWS = {
    'rpt_catalogue': '''SELECT snapshot_date, drug_class, count_big(*) products,
        sum(ingredient_records) ingredient_records, sum(flagged_strengths) flagged_strengths
        FROM dbo.fct_product GROUP BY snapshot_date, drug_class''',
    'rpt_reconciliation': '''SELECT f.snapshot_date, count_big(*) products,
        sum(f.ingredient_records) expected_ingredients,
        (SELECT count_big(*) FROM dbo.fct_product_ingredient i WHERE i.snapshot_date=f.snapshot_date) actual_ingredients
        FROM dbo.fct_product f GROUP BY f.snapshot_date''',
    'rpt_duplicate_review': '''SELECT p.snapshot_date, a.din left_din, b.din right_din,
        a.brand_name left_brand, b.brand_name right_brand, p.score, p.reason
        FROM dbo.fct_duplicate_pair p JOIN dbo.dim_product a ON p.left_product_key=a.product_key
        JOIN dbo.dim_product b ON p.right_product_key=b.product_key WHERE p.is_match=1''',
}
INDEX_SQL = '''IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='ix_product_date_class'
               AND object_id=OBJECT_ID('dbo.fct_product'))
               CREATE INDEX ix_product_date_class ON dbo.fct_product(snapshot_date, drug_class)
               INCLUDE(ingredient_records, flagged_strengths)'''


def connect():
    import pymssql
    return pymssql.connect(server=os.environ.get('DPD_SQLSERVER_HOST', '127.0.0.1'),
                           port=os.environ.get('DPD_SQLSERVER_PORT', '1433'),
                           user=os.environ.get('DPD_SQLSERVER_USER', 'sa'),
                           password=os.environ['DPD_SQLSERVER_PASSWORD'],
                           database=os.environ.get('DPD_SQLSERVER_DATABASE', 'tempdb'),
                           autocommit=False)


def publish(con, warehouse_db):
    """The dedicated target is a full mirror, so deleted source dates disappear too."""
    cur = con.cursor()
    try:
        for name, columns in DDL.items():
            cur.execute(f"IF OBJECT_ID('dbo.{name}', 'U') IS NULL CREATE TABLE dbo.{name} ({columns})")
        with duckdb.connect(str(warehouse_db), read_only=True) as source:
            for name in reversed(DDL):
                cur.execute(f'DELETE FROM dbo.{name}')
            totals = {}
            for name in DDL:
                data = source.execute(f'SELECT * FROM analytics.{name}')
                columns = ', '.join(d[0] for d in data.description)
                values = data.fetchall()
                if values:
                    markers = ', '.join(['%s'] * len(values[0]))
                    cur.executemany(f'INSERT INTO dbo.{name} ({columns}) VALUES ({markers})', values)
                cur.execute(f'SELECT count_big(*) FROM dbo.{name}')
                actual = cur.fetchone()[0]
                if actual != len(values):
                    raise ValueError(f'SQL Server {name}: {actual} != dbt {len(values)}')
                totals[name] = actual
            for name, query in VIEWS.items():
                cur.execute(f'CREATE OR ALTER VIEW dbo.{name} AS {query}')
            cur.execute('SELECT * FROM dbo.rpt_reconciliation WHERE expected_ingredients != actual_ingredients')
            if cur.fetchall():
                raise ValueError('SQL Server ingredient reconciliation failed')
        con.commit()
        return totals
    except Exception:
        con.rollback()
        raise
    finally:
        cur.close()


def explain(con, output: Path):
    """Save actual optimizer plans before/after the covering index; no speed claim."""
    query = "SELECT * FROM dbo.rpt_catalogue WHERE snapshot_date='2026-11-01' AND drug_class='HUMAN'"
    con.commit()
    con.autocommit(True)
    cur = con.cursor()
    try:
        output.mkdir(parents=True, exist_ok=True)
        cur.execute("IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='ix_product_date_class' AND object_id=OBJECT_ID('dbo.fct_product')) DROP INDEX ix_product_date_class ON dbo.fct_product")
        for label in ['before', 'after']:
            cur.execute('SET SHOWPLAN_XML ON')
            try:
                cur.execute(query)
                (output / f'{label}.sqlplan').write_text(cur.fetchone()[0])
            finally:
                cur.execute('SET SHOWPLAN_XML OFF')
            if label == 'before':
                cur.execute(INDEX_SQL)
    finally:
        cur.close()
        con.autocommit(False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--warehouse-db', type=Path, required=True)
    parser.add_argument('--plans', type=Path)
    args = parser.parse_args()
    con = connect()
    try:
        print(json.dumps(publish(con, args.warehouse_db), indent=2))
        if args.plans:
            explain(con, args.plans)
    finally:
        con.close()


if __name__ == '__main__':
    main()
