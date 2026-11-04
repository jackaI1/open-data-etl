"""Run the production detection functions against independently labelled examples."""

import argparse
import json
from itertools import combinations
from pathlib import Path

from .detection import detect_strength_anomalies, link_products, product_context
from .transform import get_spark


def fixture_frames(spark, fixture):
    records = fixture["products"]
    products = spark.createDataFrame(
        [
            {k: r[k] for k in ["drug_code", "brand_name", "din", "company_name", "drug_class"]}
            for r in records
        ],
        "drug_code int, brand_name string, din string, company_name string, drug_class string",
    )
    ingredients = spark.createDataFrame(
        [(r["drug_code"], "EXAMPLE INGREDIENT", r["strength"], "MG") for r in records],
        "drug_code int, ingredient string, strength string, strength_unit string",
    )
    forms = spark.createDataFrame(
        [(r["drug_code"], r["form"]) for r in records], "drug_code int, pharm_form_code string"
    )
    routes = spark.createDataFrame(
        [(r["drug_code"], r["route"]) for r in records], "drug_code int, route_code string"
    )
    strengths = spark.createDataFrame(
        [
            {k: r[k] for k in ["drug_code", "ingredient", "strength", "strength_unit"]}
            for r in fixture["strengths"]
        ],
        ingredients.schema,
    )
    return products, ingredients, forms, routes, strengths


def metrics(truth, predicted, universe):
    tp = len(truth & predicted)
    fp = len(predicted - truth)
    fn = len(truth - predicted)
    return {
        "true_positive": tp,
        "false_positive": fp,
        "false_negative": fn,
        "true_negative": len(universe - truth - predicted),
        "precision": tp / (tp + fp) if tp + fp else None,
        "recall": tp / (tp + fn) if tp + fn else None,
    }


def evaluate(spark, fixture):
    products, ingredients, forms, routes, strengths = fixture_frames(spark, fixture)
    links, _ = link_products(products, product_context(ingredients, forms, routes))
    rows = links.collect()
    all_pairs = list(combinations(sorted(fixture["products"], key=lambda r: r["drug_code"]), 2))
    universe = {(a["drug_code"], b["drug_code"]) for a, b in all_pairs}
    truth = {(a["drug_code"], b["drug_code"]) for a, b in all_pairs if a["entity"] == b["entity"]}
    candidates = {(r.left_id, r.right_id) for r in rows}
    predicted = {(r.left_id, r.right_id) for r in rows if r.is_match}
    flags = detect_strength_anomalies(strengths).collect()
    anomaly_truth = {r["drug_code"] for r in fixture["strengths"] if r["is_anomaly"]}
    anomaly_predicted = {r.drug_code for r in flags if r.is_anomaly}
    linkage = metrics(truth, predicted, universe)
    linkage.update(
        candidate_pairs=len(candidates),
        all_pairs=len(universe),
        blocking_recall=len(truth & candidates) / len(truth) if truth else None,
    )
    anomaly = metrics(anomaly_truth, anomaly_predicted, {r.drug_code for r in flags})
    anomaly.update(labelled_rows=len(flags), evaluated_rows=sum(r.is_evaluated for r in flags))
    return {
        "scope": "Synthetic regression challenge, not an estimate of catalogue accuracy",
        "linkage": linkage,
        "anomalies": anomaly,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, default=Path("tests/fixtures/detection.json"))
    parser.add_argument(
        "--output", type=Path, default=Path("data/reports/detection_evaluation.json")
    )
    args = parser.parse_args()
    spark = get_spark("dpd-detection-evaluation")
    try:
        report = evaluate(spark, json.loads(args.fixture.read_text()))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
        print(json.dumps(report, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
