"""Compare catalogue refreshes using baseline-derived strength bins."""

from __future__ import annotations

import bisect
import math
from collections import Counter


def _psi(old, new):
    old = sorted(x for x in old if x is not None and math.isfinite(x))
    new = [x for x in new if x is not None and math.isfinite(x)]
    if not old or not new:
        return None
    edges = sorted({old[min(len(old) - 1, len(old) * i // 10)] for i in range(1, 10)})
    a = Counter(bisect.bisect_right(edges, x) for x in old)
    b = Counter(bisect.bisect_right(edges, x) for x in new)
    # Additive smoothing avoids infinite values for empty bins.
    bins = len(edges) + 1
    return sum(
        (q - p) * math.log(q / p)
        for p, q in (
            ((a[i] + 0.5) / (len(old) + 0.5 * bins), (b[i] + 0.5) / (len(new) + 0.5 * bins))
            for i in range(bins)
        )
    )


def compare(before, after, null_columns, category, strength):
    if not before or not after:
        raise ValueError("both snapshots must contain rows")
    row_change = len(after) / len(before) - 1
    nulls = {
        col: {
            "previous": sum(row.get(col) is None for row in before) / len(before),
            "current": sum(row.get(col) is None for row in after) / len(after),
        }
        for col in null_columns
    }
    old_categories = {r.get(category) for r in before} - {None}
    new_categories = {r.get(category) for r in after} - {None}
    psi = _psi([r.get(strength) for r in before], [r.get(strength) for r in after])
    changes = [abs(row_change)] + [max(0, v["current"] - v["previous"]) for v in nulls.values()]
    severity = (
        "fail"
        if max(changes) >= 0.10 or (psi is not None and psi >= 0.25)
        else ("warn" if max(changes) >= 0.05 or (psi is not None and psi >= 0.10) else "ok")
    )
    if old_categories != new_categories and severity == "ok":
        severity = "warn"
    return {
        "row_change": row_change,
        "null_rates": nulls,
        "new_categories": sorted(new_categories - old_categories),
        "removed_categories": sorted(old_categories - new_categories),
        "psi": psi,
        "severity": severity,
    }


def monitor_refresh(db, snapshot, report_path, monitor=None):
    """Compare accepted snapshots and persist one replaceable record per date."""
    import json

    import duckdb

    with duckdb.connect(str(db)) as con:
        previous = con.execute(
            "select max(snapshot_date) from drug_products where snapshot_date < ?", [snapshot]
        ).fetchone()[0]
        if previous is None:
            result = {"previous": None, "snapshot": str(snapshot), "severity": "baseline"}
            con.execute(
                "create table if not exists refresh_monitor_history (snapshot_date date primary key, metrics json)"
            )
            con.execute(
                "insert or replace into refresh_monitor_history values (?, ?)",
                [snapshot, json.dumps(result)],
            )
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                f"Refresh {snapshot}: baseline; no earlier snapshot is available.\n"
            )
            return result
        columns = [
            r[0]
            for r in con.execute("describe drug_products").fetchall()
            if r[0] not in {"snapshot_date", "drug_code"}
        ]

        def rows_for(day):
            cur = con.execute("select * from drug_products where snapshot_date = ?", [day])
            names = [d[0] for d in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()]

        old, new = rows_for(previous), rows_for(snapshot)
        result = compare(old, new, columns, "current_status", "_unused_strength")

        def strengths(day):
            return [
                r[0]
                for r in con.execute(
                    "select try_cast(strength as double) from drug_ingredients where snapshot_date = ?",
                    [day],
                ).fetchall()
            ]

        result["psi"] = _psi(strengths(previous), strengths(snapshot))
        if result["psi"] is not None:
            if result["psi"] >= 0.25:
                result["severity"] = "fail"
            elif result["psi"] >= 0.10 and result["severity"] == "ok":
                result["severity"] = "warn"
        result.update(previous=str(previous), snapshot=str(snapshot))
        con.execute(
            "create table if not exists refresh_monitor_history (snapshot_date date primary key, metrics json)"
        )
        con.execute(
            "insert or replace into refresh_monitor_history values (?, ?)",
            [snapshot, json.dumps(result)],
        )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        f"Refresh {snapshot} compared with {previous}: {result['severity']}.\n"
        + json.dumps(result, indent=2)
        + "\n"
    )
    if monitor is not None:
        monitor.refresh(result["severity"])
    return result
