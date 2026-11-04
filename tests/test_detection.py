import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from dpd_etl.detection import detect_strength_anomalies, link_products, product_context
from dpd_etl.evaluate import evaluate, fixture_frames, metrics

FIXTURE = Path(__file__).parent / "fixtures" / "detection.json"


def test_labelled_evaluation_counts_blocking_misses_and_abstentions(spark):
    report = evaluate(spark, json.loads(FIXTURE.read_text()))
    linkage = report["linkage"]
    assert linkage["all_pairs"] == 105
    assert linkage["true_positive"] == 5
    assert linkage["false_positive"] == 1  # missing DIN cannot distinguish two lookalikes
    assert linkage["false_negative"] == 2  # first-three-letter typo misses both Birch links
    assert linkage["precision"] == pytest.approx(5 / 6)
    assert linkage["recall"] == pytest.approx(5 / 7)
    assert linkage["blocking_recall"] == pytest.approx(5 / 7)
    assert linkage["candidate_pairs"] < linkage["all_pairs"]
    anomaly = report["anomalies"]
    assert anomaly["true_positive"] == 2
    assert anomaly["false_positive"] == 0
    assert anomaly["false_negative"] == 1  # zero MAD abstention counts against recall
    assert anomaly["recall"] == pytest.approx(2 / 3)
    assert anomaly["evaluated_rows"] == 13


def test_scored_links_respect_identity_and_complete_context(spark):
    products, ingredients, forms, routes, _ = fixture_frames(spark, json.loads(FIXTURE.read_text()))
    context = product_context(ingredients, forms, routes)
    links, _ = link_products(products, context)
    rows = {(r.left_id, r.right_id): r for r in links.collect()}
    assert rows[1, 2].reason == "exact_din"
    assert rows[1, 3].is_match and rows[1, 3].reason == "fuzzy_match"
    assert 0.90 <= rows[1, 3].score < 1
    assert rows[1, 4].reason == "conflicting_din" and not rows[1, 4].is_match
    assert (1, 5) not in rows  # different strength
    assert (1, 6) not in rows  # different route
    assert (1, 7) not in rows  # different dosage form
    assert not rows[1, 8].is_match  # different company
    assert not rows[1, 10].is_match  # missing company
    assert rows[14, 15].is_match  # fuzzy match with both DINs unavailable
    missing = product_context(ingredients, forms.where("drug_code != 3"), routes)
    assert missing.where("drug_code = 3").first().context is None
    incomplete = product_context(
        ingredients.withColumn("strength", F.when(F.col("drug_code") != 3, F.col("strength"))),
        forms,
        routes,
    )
    assert incomplete.where("drug_code = 3").first().context is None
    # A null route alongside a valid one must not disappear during collect_set.
    partial_routes = routes.union(spark.createDataFrame([(3, None)], routes.schema))
    partial = product_context(ingredients, forms, partial_routes)
    assert partial.where("drug_code = 3").first().context is None


def test_oversized_blocks_are_reported_instead_of_cross_joined(spark):
    products, ingredients, forms, routes, _ = fixture_frames(spark, json.loads(FIXTURE.read_text()))
    links, blocks = link_products(products, product_context(ingredients, forms, routes), 2)
    assert blocks.where("skipped").count() == 1
    assert {(r.left_id, r.right_id) for r in links.collect()} == {(1, 2), (11, 13), (14, 15)}
    with pytest.raises(ValueError):
        link_products(products, product_context(ingredients, forms, routes), 1)


def test_strength_scores_are_robust_and_unscored_values_stay_visible(spark):
    *_, strengths = fixture_frames(spark, json.loads(FIXTURE.read_text()))
    rows = {r.drug_code: r for r in detect_strength_anomalies(strengths).collect()}
    assert rows[112].is_anomaly and rows[112].robust_z > 3.5
    assert rows[113].is_anomaly and rows[113].robust_z < -3.5
    assert rows[107].peer_median == 105  # extreme values do not drag the centre
    assert rows[107].peer_mad == 15
    assert rows[209].reason == "zero_mad" and rows[209].robust_z is None
    assert rows[401].reason == "too_few_peers"  # MCG stays separate from MG
    for code in range(301, 307):
        assert rows[code].reason == "unsupported_strength"
        assert rows[code].robust_z is None
        assert not rows[code].is_evaluated and not rows[code].is_anomaly
    assert detect_strength_anomalies(strengths.limit(0)).count() == 0


def test_metrics_include_false_positives_and_undefined_precision():
    assert metrics({1, 2}, {2, 3}, {1, 2, 3, 4}) == {
        "true_positive": 1,
        "false_positive": 1,
        "false_negative": 1,
        "true_negative": 1,
        "precision": 0.5,
        "recall": 0.5,
    }
    assert metrics({1}, set(), {1})["precision"] is None
