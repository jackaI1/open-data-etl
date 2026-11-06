"""Export contract: real DuckDB input; integration tests use the pinned server."""

import json

import duckdb
import pytest

from dpd_etl.elastic import mapping, snapshot_documents, validate_date

DAY = "2026-11-01"


def test_documents_include_searchable_products_flags_checks_and_zero_safe_summary(
    elastic_snapshot,
):
    docs = list(snapshot_documents(*elastic_snapshot, DAY))
    kinds = {doc["kind"] for doc in docs}
    assert kinds == {"product", "strength", "duplicate_pair", "quality_check", "summary"}
    assert all(doc["snapshot_date"] == DAY for doc in docs)
    summary = next(d for d in docs if d["kind"] == "summary")
    assert (
        summary["product_count"],
        summary["flagged_strength_count"],
        summary["duplicate_pair_count"],
    ) == (1, 1, 1)
    assert next(d for d in docs if d["kind"] == "product")["din"] == "00000001"
    assert next(d for d in docs if d["kind"] == "strength")["numeric_strength"] == 999
    assert list(snapshot_documents(*elastic_snapshot, DAY)) == docs
    fields = mapping()["mappings"]["properties"]
    assert mapping()["mappings"]["dynamic"] == "strict"
    assert fields["din"]["type"] == "keyword"
    assert fields["snapshot_date"]["type"] == "date"
    assert fields["numeric_strength"]["type"] == "double"
    assert all(set(d) <= fields.keys() for d in docs)


def test_rejected_or_missing_snapshot_cannot_be_published(elastic_snapshot):
    db, report = elastic_snapshot
    with pytest.raises(ValueError, match="products"):
        list(snapshot_documents(db, report, "2026-11-02"))
    report.write_text('[{"name":"bad","passed":false}]')
    with pytest.raises(ValueError, match="quality"):
        list(snapshot_documents(db, report, DAY))


@pytest.mark.parametrize("value", ["2026-1-1", "2026-02-30", "../bad", "2026-11-01T00:00:00"])
def test_dates_are_validated(value):
    with pytest.raises(ValueError):
        validate_date(value)


def test_nonfinite_strengths_remain_reviewable_as_null(elastic_snapshot):
    db, report = elastic_snapshot
    with duckdb.connect(str(db)) as con:
        con.execute("UPDATE strength_anomalies SET numeric_strength = 'Infinity'::DOUBLE")
    doc = next(d for d in snapshot_documents(db, report, DAY) if d["kind"] == "strength")
    assert doc["numeric_strength"] is None
    json.dumps(doc, allow_nan=False)
