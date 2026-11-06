"""Read-only Cloud Run report viewer. Cloud Run handles caller authentication."""

import os

from flask import Flask, jsonify, render_template_string
from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.cloud import storage

from .gcp_batch import valid_date, valid_run

PAGE = """<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Drug catalogue check report</title>
<style>body{font:18px system-ui;max-width:1000px;margin:3rem auto;padding:0 1rem}
td,th{padding:.6rem;text-align:left;border-bottom:1px solid #ccc}table{width:100%}</style>
<h1>Drug catalogue check report</h1>
<p>Snapshot {{ report.snapshot_date }} · Attempt {{ report.run_id }}</p>
<p>Pipeline: <strong>{{ report.status }}</strong> ·
{% if report.fixture %}Sample data{% else %}Catalogue data{% endif %}</p>
<p>Passing checks describe the data. Only a completion record confirms all outputs uploaded.</p>
<table><caption>Data quality checks</caption><tr><th>Check</th><th>Result</th>
<th>Observed</th><th>Limit</th></tr>
{% for check in report.checks %}<tr><td>{{ check.name }}</td>
<td>{{ 'PASS' if check.passed else 'FAIL' }}</td><td>{{ check.observed }}</td>
<td>{{ check.limit }}</td></tr>{% endfor %}</table>
<p><a href="{{ json_url }}">Download the report as JSON</a></p></html>"""


def create_app(client=None, bucket_name=None):
    app = Flask(__name__)

    @app.get("/healthz")
    def health():
        return {"status": "ok"}

    @app.get("/")
    def index():
        return "Drug catalogue reports: /reports/YYYY-MM-DD/RUN_ID (or append .json)\n"

    @app.get("/reports/<snapshot>/<run_id>")
    def report(snapshot, run_id):
        import json

        wants_json = run_id.endswith(".json")
        run_id = run_id.removesuffix(".json")
        try:
            valid_date(snapshot)
            valid_run(run_id)
        except ValueError:
            return {"error": "invalid snapshot date or run ID"}, 400
        try:
            bucket = (client or storage.Client()).bucket(bucket_name or os.environ["DPD_BUCKET"])
            payload = json.loads(bucket.blob(
                f"runs/{snapshot}/{run_id}/report.json"
            ).download_as_bytes(timeout=30))
        except NotFound:
            return {"error": "report not found"}, 404
        except GoogleAPICallError:
            return {"error": "report storage unavailable"}, 503
        response = jsonify(payload) if wants_json else app.make_response(render_template_string(
            PAGE, report=payload, json_url=f"/reports/{snapshot}/{run_id}.json"
        ))
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    return app
