import importlib.util
import json
from pathlib import Path

import pytest

from dpd_etl.graph_match import compare

ROOT = Path(__file__).resolve().parents[2]
CHALLENGE = ROOT / "tests" / "fixtures" / "graph_challenge.json"
WAREHOUSE = ROOT / "data" / "fixture" / "warehouse" / "dpd.duckdb"


def test_committed_challenge_matches_the_builder():
    if not WAREHOUSE.exists():
        pytest.skip("run `make fixture` first to build the warehouse the challenge comes from")
    spec = importlib.util.spec_from_file_location(
        "build_graph_challenge", ROOT / "scripts" / "build_graph_challenge.py"
    )
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)
    rebuilt = builder.build(WAREHOUSE, ROOT / "tests" / "fixtures" / "raw")
    assert rebuilt == json.loads(CHALLENGE.read_text())


def test_held_out_comparison_reports_both_matchers(spark):
    report = compare(spark, json.loads(CHALLENGE.read_text()))
    held = report["held_out"]
    assert held["products"] == 60
    assert held["true_pairs"] == 20
    assert held["all_pairs"] == 1770
    assert held["blocking_recall"] == 1.0
    assert held["candidate_pairs"] == 57

    rule = report["rule"]
    assert (rule["true_positive"], rule["false_positive"], rule["false_negative"]) == (20, 10, 0)
    assert rule["precision"] == pytest.approx(20 / 30)
    assert rule["recall"] == 1.0
    assert all("rule" in miss["reason"] or "blocking" in miss["reason"] for miss in rule["misses"])

    graph = report["graph"]
    assert (graph["true_positive"], graph["false_positive"], graph["false_negative"]) == (16, 5, 4)
    assert graph["precision"] == pytest.approx(16 / 21)
    assert graph["recall"] == pytest.approx(0.8)
    assert {m["change"] for m in graph["misses"]} == {"missing_din", "typo"}

    isolated = report["graph_without_links"]
    assert (isolated["true_positive"], isolated["false_positive"]) == (19, 5)
    assert isolated["recall"] == pytest.approx(0.95)
    assert report["training"]["final_loss"] < report["training"]["first_loss"]
