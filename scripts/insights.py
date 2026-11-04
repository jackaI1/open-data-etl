"""Print reproducible snapshot summaries from the query warehouse."""

import json
import sys

import duckdb

QUERIES = {
    "products_by_class": """
        SELECT drug_class, count(*) AS products FROM drug_products
        WHERE snapshot_date = (SELECT max(snapshot_date) FROM drug_products)
        GROUP BY drug_class ORDER BY products DESC, drug_class
    """,
    "combination_products": """
        SELECT count(*) AS products, count(*) FILTER (WHERE number_of_ais > 1) AS combinations
        FROM drug_products WHERE snapshot_date = (SELECT max(snapshot_date) FROM drug_products)
    """,
    "status_history": """
        SELECT count(*) AS events, count(DISTINCT drug_code) AS products
        FROM drug_status_history
        WHERE snapshot_date = (SELECT max(snapshot_date) FROM drug_status_history)
    """,
}

if __name__ == "__main__":
    with duckdb.connect(sys.argv[1], read_only=True) as con:
        for name, query in QUERIES.items():
            result = con.execute(query)
            columns = [col[0] for col in result.description]
            print(json.dumps({name: [dict(zip(columns, row)) for row in result.fetchall()]}))
