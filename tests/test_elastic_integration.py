"""Requires the real Compose stack; CI always enables these tests."""

import json
import os
from pathlib import Path
from urllib.request import Request, urlopen
from uuid import uuid4

import duckdb
import pytest
from test_elastic import DAY

from dpd_etl.elastic import Elastic, export_snapshot


@pytest.fixture
def server():
    url = os.environ.get("ELASTICSEARCH_URL")
    if not url:
        pytest.skip("set ELASTICSEARCH_URL; CI runs against pinned Elasticsearch")
    client = Elastic(url)
    prefix = "dpd-test-" + uuid4().hex[:12]
    yield client, url, prefix
    # Strictly scoped to this test's random prefix, including failed staging indexes.
    indexes = client.request("GET", f"/{prefix}-*/_settings", missing_ok=True)
    for name in indexes:
        client.request("DELETE", f"/{name}", missing_ok=True)


def publish(elastic_snapshot, server, day=DAY, **kwargs):
    _, url, prefix = server
    return export_snapshot(*elastic_snapshot, day, url=url, prefix=prefix, batch_size=2, **kwargs)


def search(server, body):
    client, _, prefix = server
    return client.request("POST", f"/{prefix}-snapshots/_search", body)


def test_real_bulk_mapping_history_and_cleared_flags(elastic_snapshot, server):
    client, _, prefix = server
    assert publish(elastic_snapshot, server)["documents"] == 5
    ids = {h["_id"] for h in search(server, {"size": 100})["hits"]["hits"]}
    assert publish(elastic_snapshot, server)["documents"] == 5
    assert {h["_id"] for h in search(server, {"size": 100})["hits"]["hits"]} == ids
    with duckdb.connect(str(elastic_snapshot[0])) as con:
        for table in ("drug_products", "strength_anomalies", "product_links"):
            con.execute(
                f"INSERT INTO {table} SELECT * REPLACE (DATE '2026-11-02' "
                f"AS snapshot_date) FROM {table}"
            )
    publish(elastic_snapshot, server, "2026-11-02")
    with duckdb.connect(str(elastic_snapshot[0])) as con:
        con.execute("DELETE FROM product_links WHERE snapshot_date = ?", [DAY])
        con.execute(
            "UPDATE strength_anomalies SET is_anomaly = false WHERE snapshot_date = ?", [DAY]
        )
    assert publish(elastic_snapshot, server)["documents"] == 4
    result = search(
        server,
        {
            "size": 0,
            "query": {"term": {"kind": "summary"}},
            "aggs": {
                "dates": {
                    "date_histogram": {"field": "snapshot_date", "calendar_interval": "day"},
                    "aggs": {
                        "strengths": {"sum": {"field": "flagged_strength_count"}},
                        "pairs": {"sum": {"field": "duplicate_pair_count"}},
                    },
                }
            },
        },
    )
    buckets = result["aggregations"]["dates"]["buckets"]
    assert [(b["strengths"]["value"], b["pairs"]["value"]) for b in buckets] == [(0, 0), (1, 1)]
    mapping = client.request("GET", f"/{prefix}-snapshots/_mapping")
    assert len(mapping) == 2  # Exactly one live generation per date, no orphan stages.
    assert all(m["mappings"]["properties"]["din"]["type"] == "keyword" for m in mapping.values())
    found = search(server, {"query": {"match": {"brand_name": "sample"}}})
    assert found["hits"]["total"]["value"] == 2


def test_http_200_bulk_item_failure_keeps_previous_snapshot(elastic_snapshot, server, monkeypatch):
    client, _, prefix = server
    publish(elastic_snapshot, server)
    old = client.request("GET", f"/_alias/{prefix}-snapshots")
    request = Elastic.request

    def invalid_document(self, method, path, body=None, **kwargs):
        if path.endswith("/_bulk"):
            lines = body.decode().splitlines()
            doc = json.loads(lines[1])
            doc["unmapped_field"] = "must fail strict mapping"
            lines[1] = json.dumps(doc)
            body = ("\n".join(lines) + "\n").encode()
        return request(self, method, path, body, **kwargs)

    monkeypatch.setattr(Elastic, "request", invalid_document)
    with pytest.raises(RuntimeError, match="bulk item failed"):
        publish(elastic_snapshot, server)
    assert client.request("GET", f"/_alias/{prefix}-snapshots") == old
    assert client.request("GET", f"/{prefix}-*/_count")["count"] == 5


def test_rejected_export_does_not_contact_server(elastic_snapshot, server):
    elastic_snapshot[1].write_text('[{"passed": false}]')
    with pytest.raises(ValueError, match="quality"):
        publish(elastic_snapshot, server)
    assert server[0].request("GET", f"/_alias/{server[2]}-snapshots", missing_ok=True) == {}


def test_kibana_dashboard_roundtrip():
    url = os.environ.get("KIBANA_URL")
    if not url:
        pytest.skip("set KIBANA_URL; CI imports and exports the actual saved objects")
    # Check the committed export via the supported import API, not direct system-index writes.
    import subprocess

    result = subprocess.run(
        [
            "curl",
            "--fail",
            "--silent",
            "--show-error",
            "--max-time",
            "60",
            "-X",
            "POST",
            f"{url}/api/saved_objects/_import?overwrite=true",
            "-H",
            "kbn-xsrf: true",
            "--form",
            "file=@elastic/dashboard.ndjson",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    imported = json.loads(result.stdout)
    assert imported["success"] is True
    assert imported["successCount"] == 7
    body = json.dumps(
        {"objects": [{"type": "dashboard", "id": "dpd-quality"}], "includeReferencesDeep": True}
    ).encode()
    request = Request(
        f"{url}/api/saved_objects/_export",
        data=body,
        method="POST",
        headers={"kbn-xsrf": "true", "Content-Type": "application/json"},
    )
    with urlopen(request, timeout=60) as response:
        objects = [json.loads(line) for line in response.read().decode().splitlines()]
    objects = [o for o in objects if "type" in o]
    assert {o["type"] for o in objects} == {"dashboard", "search", "visualization", "index-pattern"}
    assert len(objects) == 7
    ids = {o["id"] for o in objects}
    assert all(ref["id"] in ids for o in objects for ref in o.get("references", []))
    dashboard = next(o for o in objects if o["type"] == "dashboard")
    assert len(json.loads(dashboard["attributes"]["panelsJSON"])) == 5
    # Optional: capture a fresh export after a successful import, for release artifacts.
    if destination := os.environ.get("KIBANA_EXPORT_PATH"):
        Path(destination).write_text("".join(json.dumps(o) + "\n" for o in objects))


def test_export_real_pipeline_sample(server):
    root = os.environ.get("ELASTIC_SAMPLE_DIR")
    if not root:
        pytest.skip("set ELASTIC_SAMPLE_DIR to the output of make fixture")
    root = Path(root)
    sample = (root / "warehouse/dpd.duckdb", root / f"reports/dq_{DAY}.json")
    publish(sample, server)
    publish(sample, server)
    assert search(server, {"query": {"term": {"kind": "product"}}})["hits"]["total"]["value"] == 60
    checks = search(server, {"size": 100, "query": {"term": {"kind": "quality_check"}}})
    assert checks["hits"]["total"]["value"] >= 10
    assert all(h["_source"]["passed"] for h in checks["hits"]["hits"])
