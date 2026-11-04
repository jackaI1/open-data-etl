import json

import duckdb
import pytest

from dpd_etl.pipeline import main

DATE = "2026-11-01"


def run(raw_dir, data_dir, date=DATE):
    return main(
        [
            "transform",
            "--raw-dir",
            str(raw_dir),
            "--data-dir",
            str(data_dir),
            "--snapshot-date",
            date,
            "--fixture",
        ]
    )


def test_end_to_end_writes_partitioned_parquet_and_duckdb(raw_dir, tmp_path):
    data = tmp_path / "data"
    assert run(raw_dir, data) == 0
    assert (data / "parquet" / "drug_products" / f"snapshot_date={DATE}").is_dir()
    assert (data / "parquet" / "drug_status_history" / f"snapshot_date={DATE}").is_dir()
    con = duckdb.connect(str(data / "warehouse" / "dpd.duckdb"), read_only=True)
    assert con.execute("select count(*) from drug_products").fetchone()[0] == 60
    assert con.execute("select count(*) from product_links").fetchone()[0] == 0
    assert con.execute("select count(*) from linkage_blocks").fetchone()[0] > 0
    assert con.execute("select count(*) from strength_anomalies").fetchone()[0] > 0
    review = json.loads((data / "reports" / f"review_{DATE}.json").read_text())
    assert review["product_links"] == {"rows": 0, "flagged": 0}
    assert (
        con.execute("select count(*) from drug_status_history where is_current").fetchone()[0] == 60
    )
    report = json.loads((data / "reports" / f"dq_{DATE}.json").read_text())
    assert all(c["passed"] for c in report)
    con.close()


def test_rerun_same_snapshot_is_idempotent_and_new_snapshot_appends(raw_dir, tmp_path):
    data = tmp_path / "data"
    assert run(raw_dir, data) == 0
    assert run(raw_dir, data) == 0
    assert run(raw_dir, data, "2026-11-08") == 0
    assert run(raw_dir, data) == 0  # Replace an older partition while preserving the newer one.
    con = duckdb.connect(str(data / "warehouse" / "dpd.duckdb"), read_only=True)
    counts = dict(
        con.execute(
            "select snapshot_date::varchar, count(*) from drug_products group by 1"
        ).fetchall()
    )
    assert counts == {DATE: 60, "2026-11-08": 60}
    parquet_counts = dict(
        con.execute(
            "select snapshot_date::varchar, count(*) from read_parquet(?) group by 1",
            [str(data / "parquet" / "drug_products" / "snapshot_date=*" / "*.parquet")],
        ).fetchall()
    )
    assert parquet_counts == {DATE: 60, "2026-11-08": 60}
    for table in ["product_links", "linkage_blocks", "strength_anomalies"]:
        assert con.execute(f"select count(distinct snapshot_date) from {table}").fetchone()[0] == (
            0 if table == "product_links" else 2
        )
    con.close()


def test_null_brand_names_fail_the_run_and_write_nothing(raw_dir, tmp_path):
    drug = raw_dir / "drug.txt"
    lines = drug.read_text().splitlines()
    # Blank the brand name (5th field) on every line.
    blanked = [l.replace(l.split('","')[4], "", 1) if len(l.split('","')) > 4 else l for l in lines]
    drug.write_text("\n".join(blanked) + "\n")
    data = tmp_path / "data"
    assert run(raw_dir, data) == 2
    assert not (data / "parquet").exists()
    assert not (data / "warehouse").exists()
    report = json.loads((data / "reports" / f"dq_{DATE}.json").read_text())
    assert any(c["name"] == "null_ratio_brand_name" and not c["passed"] for c in report)


def test_row_count_below_minimum_fails_the_run(raw_dir, tmp_path):
    drug = raw_dir / "drug.txt"
    drug.write_text("".join(drug.read_text().splitlines(True)[:5]))
    data = tmp_path / "data"
    assert run(raw_dir, data) == 2
    report = json.loads((data / "reports" / f"dq_{DATE}.json").read_text())
    assert any(c["name"] == "row_count_min" and not c["passed"] for c in report)


def test_large_drop_vs_previous_snapshot_fails(raw_dir, tmp_path):
    data = tmp_path / "data"
    assert run(raw_dir, data, "2026-11-01") == 0
    drug = raw_dir / "drug.txt"
    drug.write_text("".join(drug.read_text().splitlines(True)[:30]))  # 60 -> ~30 products
    assert run(raw_dir, data, "2026-11-08") == 2
    report = json.loads((data / "reports" / "dq_2026-11-08.json").read_text())
    assert any(c["name"] == "row_drop_vs_previous_snapshot" and not c["passed"] for c in report)


def test_invalid_snapshot_date_is_rejected_before_writing(raw_dir, tmp_path):
    with pytest.raises(SystemExit) as exc:
        run(raw_dir, tmp_path / "data", "2026-02-30")
    assert exc.value.code == 2
    assert not (tmp_path / "data").exists()


@pytest.mark.parametrize(
    ("filename", "field", "check_name"),
    [
        ("drug.txt", 9, "null_ratio_last_update_date"),
        ("status.txt", 3, "null_ratio_status_valid_from"),
    ],
)
def test_invalid_source_date_fails_quality_gate(raw_dir, tmp_path, filename, field, check_name):
    import csv

    drug = raw_dir / filename
    with drug.open(newline="") as fh:
        rows = list(csv.reader(fh))
    for row in rows:
        row[field] = "not-a-date"
    with drug.open("w", newline="") as fh:
        csv.writer(fh, quoting=csv.QUOTE_ALL).writerows(rows)
    data = tmp_path / "data"
    assert run(raw_dir, data) == 2
    report = json.loads((data / "reports" / f"dq_{DATE}.json").read_text())
    assert any(c["name"] == check_name and not c["passed"] for c in report)
    assert not (data / "parquet").exists()


def test_duplicate_candidates_are_loaded_without_removing_products(raw_dir, tmp_path):
    import csv

    drug = raw_dir / "drug.txt"
    with drug.open(newline="") as fh:
        rows = list(csv.reader(fh))
    rows[1][3] = rows[0][3]  # same valid DIN, distinct drug_code
    with drug.open("w", newline="") as fh:
        csv.writer(fh, quoting=csv.QUOTE_ALL).writerows(rows)
    data = tmp_path / "data"
    assert run(raw_dir, data) == 0
    with duckdb.connect(str(data / "warehouse" / "dpd.duckdb"), read_only=True) as con:
        assert con.execute("select count(*) from drug_products").fetchone()[0] == 60
        assert con.execute("select count(*) from product_links where is_match").fetchone()[0] == 1
    # Replacing the snapshot with no candidates must remove its previous links.
    rows[1][3] = "99999999"
    with drug.open("w", newline="") as fh:
        csv.writer(fh, quoting=csv.QUOTE_ALL).writerows(rows)
    assert run(raw_dir, data) == 0
    with duckdb.connect(str(data / "warehouse" / "dpd.duckdb"), read_only=True) as con:
        assert con.execute("select count(*) from product_links").fetchone()[0] == 0
