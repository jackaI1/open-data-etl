from dpd_etl.refresh_drift import compare


def test_stable_snapshot_does_not_alert():
    rows = [{"brand": "A", "category": "X", "strength": 10.0}] * 100
    result = compare(rows, rows, ["brand"], "category", "strength")
    assert result["severity"] == "ok"
    assert result["psi"] == 0


def test_detects_drop_null_spike_categories_and_distribution_shift():
    before = [{"brand": "A", "category": "X", "strength": float(i % 10)} for i in range(100)]
    after = [{"brand": None, "category": "Y", "strength": 100.0} for _ in range(50)]
    result = compare(before, after, ["brand"], "category", "strength")
    assert result["severity"] == "fail"
    assert result["row_change"] == -0.5
    assert result["null_rates"]["brand"]["current"] == 1
    assert result["new_categories"] == ["Y"] and result["removed_categories"] == ["X"]
    assert result["psi"] > 0.25


def test_history_compares_latest_earlier_snapshot_and_replaces_rerun(tmp_path):
    import duckdb

    from dpd_etl.refresh_drift import monitor_refresh

    db = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(db)) as con:
        con.execute(
            "create table drug_products (drug_code int, brand_name varchar, current_status varchar, snapshot_date date)"
        )
        con.execute(
            "insert into drug_products values (1,'A','ACTIVE','2026-01-01'), (1,NULL,'ACTIVE','2026-02-01')"
        )
        con.execute(
            "create table drug_ingredients (drug_code int, strength varchar, snapshot_date date)"
        )
        con.execute(
            "insert into drug_ingredients values (1,'10','2026-01-01'), (1,'10','2026-02-01')"
        )
    report = monitor_refresh(db, "2026-02-01", tmp_path / "report.txt")
    assert report["severity"] == "fail"
    assert "2026-01-01" in (tmp_path / "report.txt").read_text()
    monitor_refresh(db, "2026-02-01", tmp_path / "report.txt")
    with duckdb.connect(str(db)) as con:
        assert con.execute("select count(*) from refresh_monitor_history").fetchone()[0] == 1


def test_first_snapshot_records_baseline(tmp_path):
    import duckdb

    from dpd_etl.refresh_drift import monitor_refresh

    db = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(db)) as con:
        con.execute(
            "create table drug_products as select 1 drug_code, 'A' brand_name, 'ACTIVE' current_status, date '2026-01-01' snapshot_date"
        )
    result = monitor_refresh(db, "2026-01-01", tmp_path / "first.txt")
    assert result["severity"] == "baseline"
    with duckdb.connect(str(db)) as con:
        assert con.execute("select count(*) from refresh_monitor_history").fetchone()[0] == 1


def test_warn_and_fail_thresholds_and_missing_strengths():
    before = [{"brand": "A", "category": "X", "strength": None} for _ in range(100)]
    warning = before[:94]
    failure = before[:89]
    assert compare(before, warning, ["brand"], "category", "strength")["severity"] == "warn"
    result = compare(before, failure, ["brand"], "category", "strength")
    assert result["severity"] == "fail"
    assert result["psi"] is None


def test_category_change_warns_when_volume_and_nulls_stable():
    before = [{"brand": "A", "category": "X", "strength": 1.0}] * 100
    after = [{"brand": "A", "category": "Y", "strength": 1.0}] * 100
    assert compare(before, after, ["brand"], "category", "strength")["severity"] == "warn"
