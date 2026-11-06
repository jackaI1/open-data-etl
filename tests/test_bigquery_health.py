from unittest.mock import Mock

import pytest

from dpd_etl.bigquery_health import health_checks


def test_query_uses_parameters_and_billing_cap():
    client = Mock()
    client.query.return_value.result.return_value = [{"rows_processed": 60, "flagged_strengths": 2,
        "recomputed_strength_flags": 2, "duplicate_pairs": 1, "recomputed_duplicate_pairs": 1}]
    result = health_checks(client, "dpd-test", "review", "2026-11-01", expected={"rows_processed": 60})
    assert result["rows_processed"] == 60
    sql = client.query.call_args.args[0]
    config = client.query.call_args.kwargs["job_config"]
    assert "@snapshot_date" in sql and "@z_threshold" in sql
    assert "2026-11-01" not in sql
    assert config.maximum_bytes_billed == 1024**3
    assert [(p.name, p.value) for p in config.query_parameters] == [
        ("snapshot_date", "2026-11-01"), ("z_threshold", 3.5)]
    with pytest.raises(ValueError, match="Spark output mismatch"):
        health_checks(client, "dpd-test", "review", "2026-11-01", expected={"rows_processed": 61})
    client.query.return_value.result.return_value[0]["recomputed_strength_flags"] = 0
    with pytest.raises(ValueError, match="SQL detection mismatch"):
        health_checks(client, "dpd-test", "review", "2026-11-01")
