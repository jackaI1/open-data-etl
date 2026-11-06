"""Exercise imported chart queries through the real Superset HTTP API."""

import json
import os
from pathlib import Path
from zipfile import ZipFile

import requests
import yaml

base = "http://127.0.0.1:18088"
with requests.Session() as session:
    def request(method, path, **kw):
        response = session.request(method, base + path, timeout=120, **kw)
        if not response.ok:
            raise AssertionError(f"{method} {path}: {response.status_code}: {response.text[:1500]}")
        return response.json()

    login = request("POST", "/api/v1/security/login", json={
        "username": "admin", "password": os.environ["SUPERSET_ADMIN_PASSWORD"], "provider": "db", "refresh": True})
    session.headers["Authorization"] = "Bearer " + login["access_token"]
    session.headers["X-CSRFToken"] = request("GET", "/api/v1/security/csrf_token/")["result"]
    dashboards = request("GET", "/api/v1/dashboard/")["result"]
    assert len(dashboards) == 1 and dashboards[0]["slug"] == "catalogue-quality"
    charts = request("GET", "/api/v1/chart/?q=(page_size:100)")["result"]
    assert len(charts) == 11
    for chart in charts:
        detail = request("GET", f'/api/v1/chart/{chart["id"]}')["result"]
        params = json.loads(detail["params"])
        dataset_id = int(params["datasource"].split("__", 1)[0])
        ds = request("GET", f"/api/v1/dataset/{dataset_id}")["result"]
        query = {"columns": params["groupby"], "metrics": params["metrics"],
                     "filters": [], "extras": {"having": "", "where": ""},
                     "time_range": "No filter", "row_limit": 10000,
                     "granularity": "snapshot_date", "is_timeseries": False,
                     "order_desc": True}
        if params["viz_type"].startswith("echarts") and not params["groupby"]:
            query.update(columns=[{"timeGrain": "P1D", "columnType": "BASE_AXIS",
                "sqlExpression": "snapshot_date", "label": "snapshot_date",
                "expressionType": "SQL"}])
        def run(q, dataset_id=dataset_id, params=params):
            payload = {"datasource": {"id": dataset_id, "type": "table"}, "queries": [q],
                           "form_data": params, "result_format": "json", "result_type": "full", "force": True}
            result = request("POST", "/api/v1/chart/data", json=payload)["result"][0]
            assert result["status"] == "success", result
            return result["data"]
        rows = run(query)
        assert rows, chart["slice_name"]
        if chart["slice_name"] == "Products processed":
            assert rows[0]["products"] == 9
            query["time_range"] = "2026-11-01 : 2026-11-02"
            assert run(query)[0]["products"] == 3
        if ds["table_name"] == "products":
            query["filters"] = [{"col": "drug_class", "op": "IN", "val": ["Human"]}]
            filtered = run(query)
            assert all(row.get("drug_class") == "Human" for row in filtered)
        print("PASS", chart["slice_name"])
    print("11 imported chart queries, date and drug-class filters passed")

# A round trip must contain all charts, datasets and the native filters.
with ZipFile("data/superset/roundtrip.zip") as archive:
    names = archive.namelist()
    assert len([n for n in names if "/charts/" in n]) == 11
    assert len([n for n in names if "/datasets/" in n]) == 3
    path = next(n for n in names if "/dashboards/" in n)
    dashboard = yaml.safe_load(archive.read(path))
    assert len(dashboard["metadata"]["native_filter_configuration"]) == 3
Path("data/superset/validated.txt").write_text("11 charts, 3 datasets, 3 filters validated in Superset 4.1.2\n")
