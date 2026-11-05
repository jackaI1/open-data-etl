"""Run good and rejected snapshots through the real Datadog client locally."""

import csv
import json
import os
import shutil
import socket
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from dpd_etl.pipeline import main

ROOT = Path(__file__).resolve().parents[1]


def demo():
    saved = dict(os.environ)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver, TemporaryDirectory(
            prefix="dpd-monitor-demo-"
        ) as temp:
            receiver.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 262144)
            receiver.bind(("127.0.0.1", 0))
            receiver.settimeout(0.1)
            port = receiver.getsockname()[1]
            print(f"[owned receiver] pid={os.getpid()} port={port}/udp "
                  f"created={datetime.now(timezone.utc).isoformat()} "
                  "cleanup=socket.close() on context exit")
            os.environ.update({
                "DPD_DATADOG_ENABLED": "true", "DD_AGENT_HOST": "127.0.0.1",
                "DD_DOGSTATSD_PORT": str(port), "DD_ENV": "local", "DD_VERSION": "demo",
            })
            os.environ.pop("DD_DOGSTATSD_DISABLE", None)
            raw = Path(temp) / "raw"
            shutil.copytree(ROOT / "tests/fixtures/raw", raw)
            scenarios = []
            for name, expected in [("success", 0), ("quality_failed", 2)]:
                if expected == 2:
                    drug = raw / "drug.txt"
                    with drug.open(newline="") as fh:
                        rows = list(csv.reader(fh))
                    for row in rows:
                        row[4] = ""
                    with drug.open("w", newline="") as fh:
                        csv.writer(fh, quoting=csv.QUOTE_ALL).writerows(rows)
                output = Path(temp) / name
                result = main([
                    "transform", "--fixture", "--raw-dir", str(raw),
                    "--data-dir", str(output), "--snapshot-date", "2026-11-01",
                ])
                assert result == expected, f"{name}: exit {result}, expected {expected}"
                if expected == 0:
                    with duckdb.connect(str(output / "warehouse/dpd.duckdb")) as db:
                        assert db.execute("select count(*) from drug_products").fetchone()[0] == 60
                else:
                    assert not (output / "parquet").exists()
                    assert not (output / "warehouse").exists()
                packets = []
                while True:
                    try:
                        packets.append(receiver.recv(8192).decode())
                    except TimeoutError:
                        break
                status = 0 if expected == 0 else 2
                assert any(p.startswith(f"_sc|dpd_etl.run|{status}|") for p in packets)
                assert any(p.startswith(f"_sc|dpd_etl.quality_gate|{status}|") for p in packets)
                assert any(f"outcome:{name}" in p for p in packets)
                assert any("check:null_ratio_brand_name" in p and
                           p.startswith(f"dpd_etl.quality.failed:{int(expected != 0)}|g|")
                           for p in packets)
                scenarios.append({"scenario": name, "exit_code": result, "packets": packets})
        assert receiver.fileno() == -1
        report = ROOT / "data/reports/monitoring-demo.json"
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(scenarios, indent=2) + "\n")
        counts = [len(s["packets"]) for s in scenarios]
        print(f"[monitor demo] verified success and rejection; packets={counts}; receiver closed")
        print(f"[monitor demo] report: {report}")
    finally:
        os.environ.clear()
        os.environ.update(saved)


if __name__ == "__main__":
    demo()
