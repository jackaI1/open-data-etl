"""Publish accepted DuckDB catalogue snapshots as nested MongoDB documents."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import duckdb
from pymongo import ASCENDING, MongoClient, ReplaceOne


def _date(value):
    if date.fromisoformat(value).isoformat() != value:
        raise ValueError("snapshot date must be YYYY-MM-DD")
    return value


def documents(db: Path, snapshot: str):
    _date(snapshot)
    with duckdb.connect(str(db), read_only=True) as con:
        products = con.execute(
            "SELECT * FROM drug_products WHERE snapshot_date = ? ORDER BY drug_code",
            [snapshot],
        ).fetchall()
        product_columns = [column[0] for column in con.description]
        ingredients = con.execute(
            "SELECT drug_code, ingredient, strength, strength_unit FROM drug_ingredients WHERE snapshot_date = ? ORDER BY ALL",
            [snapshot],
        ).fetchall()
        flags = con.execute(
            "SELECT drug_code, ingredient, strength, strength_unit, reason FROM strength_anomalies WHERE snapshot_date = ? AND is_anomaly ORDER BY ALL",
            [snapshot],
        ).fetchall()
    by_product = {}
    for code, ingredient, strength, unit in ingredients:
        by_product.setdefault(code, []).append(
            {"ingredient": ingredient, "strength": strength, "strength_unit": unit}
        )
    by_flag = {}
    for code, ingredient, strength, unit, reason in flags:
        by_flag.setdefault(code, []).append(
            {
                "ingredient": ingredient,
                "strength": strength,
                "strength_unit": unit,
                "reason": reason,
            }
        )
    for row in products:
        product = dict(zip(product_columns, row))
        code = product["drug_code"]
        for key, value in product.items():
            if isinstance(value, date):
                product[key] = value.isoformat()
        product["ingredients"] = by_product.get(code, [])
        product["review_flags"] = by_flag.get(code, [])
        yield product


def sql_summary(db: Path, snapshot: str):
    _date(snapshot)
    with duckdb.connect(str(db), read_only=True) as con:
        rows = con.execute(
            "SELECT ingredient, count(*) FROM strength_anomalies WHERE snapshot_date = ? AND is_anomaly GROUP BY ingredient ORDER BY ingredient",
            [snapshot],
        ).fetchall()
    return dict(rows)


SCHEMA = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["drug_code", "snapshot_date", "brand_name", "ingredients", "review_flags"],
        "properties": {
            "drug_code": {"bsonType": ["int", "long"]},
            "snapshot_date": {"bsonType": "string", "pattern": r"^\d{4}-\d{2}-\d{2}$"},
            "brand_name": {"bsonType": ["string", "null"]},
            "ingredients": {
                "bsonType": "array",
                "items": {
                    "bsonType": "object",
                    "required": ["ingredient", "strength", "strength_unit"],
                    "properties": {
                        "ingredient": {"bsonType": "string"},
                        "strength": {"bsonType": ["string", "null"]},
                        "strength_unit": {"bsonType": ["string", "null"]},
                    },
                },
            },
            "review_flags": {
                "bsonType": "array",
                "items": {
                    "bsonType": "object",
                    "required": ["ingredient", "strength", "strength_unit", "reason"],
                    "properties": {
                        "ingredient": {"bsonType": "string"},
                        "strength": {"bsonType": ["string", "null"]},
                        "strength_unit": {"bsonType": ["string", "null"]},
                        "reason": {"bsonType": "string"},
                    },
                },
            },
        },
    }
}


def publish(db: Path, snapshot: str, database):
    _date(snapshot)
    rows = list(documents(db, snapshot))
    if not rows:
        raise ValueError("snapshot has no products")
    if "products" not in database.list_collection_names():
        database.create_collection("products", validator=SCHEMA)
    else:
        database.command("collMod", "products", validator=SCHEMA, validationLevel="strict")
    collection = database.products
    collection.create_index([("snapshot_date", ASCENDING), ("drug_code", ASCENDING)], unique=True)
    collection.create_index([("snapshot_date", ASCENDING), ("ingredients.ingredient", ASCENDING)])
    collection.create_index([("snapshot_date", ASCENDING), ("review_flags.ingredient", ASCENDING)])
    for start in range(0, len(rows), 500):
        collection.bulk_write(
            [
                ReplaceOne(
                    {"snapshot_date": snapshot, "drug_code": row["drug_code"]}, row, upsert=True
                )
                for row in rows[start : start + 500]
            ]
        )
    collection.delete_many(
        {"snapshot_date": snapshot, "drug_code": {"$nin": [r["drug_code"] for r in rows]}}
    )
    pipeline = [
        {"$match": {"snapshot_date": snapshot}},
        {"$unwind": "$review_flags"},
        {"$group": {"_id": "$review_flags.ingredient", "count": {"$sum": 1}}},
    ]
    actual = {r["_id"]: r["count"] for r in collection.aggregate(pipeline)}
    expected = sql_summary(db, snapshot)
    if actual != expected:
        raise ValueError(f"MongoDB and SQL totals differ: {actual!r} != {expected!r}")
    return actual


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--uri", default="mongodb://localhost:27017")
    args = parser.parse_args()
    with MongoClient(args.uri, serverSelectionTimeoutMS=5000) as client:
        print(json.dumps(publish(args.db, args.snapshot_date, client.dpd), sort_keys=True))


if __name__ == "__main__":
    main()
