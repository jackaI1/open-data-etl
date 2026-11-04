import datetime as dt

import pytest
from py4j.protocol import Py4JJavaError
from pyspark.sql import functions as F

from dpd_etl.transform import (
    build_status_scd2,
    clean_drugs,
    clean_status,
    read_raw,
)


def test_dedup_keeps_latest_record_and_cleans_text(spark, raw_dir):
    raw = read_raw(spark, raw_dir, "drug")
    assert raw.count() == 61  # 60 products + 1 stale duplicate in the fixture
    drugs = clean_drugs(raw)
    assert drugs.count() == 60
    assert drugs.select("drug_code").distinct().count() == 60
    # Duplicate was lowercase/padded with an old date; the cleaned survivor is the upper-case one.
    assert drugs.where(F.col("brand_name") != F.upper(F.trim("brand_name"))).count() == 0
    assert drugs.where(F.col("last_update_date") < F.lit("2005-01-01").cast("date")).count() == 0


def test_dates_are_parsed(spark, raw_dir):
    drugs = clean_drugs(read_raw(spark, raw_dir, "drug"))
    assert drugs.where(F.col("last_update_date").isNull()).count() == 0
    assert drugs.agg(F.max("last_update_date")).first()[0] <= dt.date.today()


def test_scd2_has_one_current_version_and_contiguous_intervals(spark, raw_dir):
    scd = build_status_scd2(clean_status(read_raw(spark, raw_dir, "status")))
    assert scd.where("is_current").groupBy("drug_code").count().where("count > 1").count() == 0
    assert scd.where("is_current").count() == scd.select("drug_code").distinct().count()
    assert scd.where("is_current").where(F.col("valid_to").isNotNull()).count() == 0
    assert scd.where(F.col("valid_to") < F.col("valid_from")).count() == 0


def test_scd2_small_example(spark):
    df = spark.createDataFrame(
        [
            (1, "APPROVED", dt.date(2000, 1, 1), True),
            (1, "MARKETED", dt.date(2001, 6, 1), True),
            (1, "CANCELLED POST MARKET", dt.date(2010, 3, 1), True),
        ],
        "drug_code int, status string, history_date date, source_is_current boolean",
    )
    rows = {r["status"]: r for r in build_status_scd2(df).collect()}
    assert rows["APPROVED"]["valid_to"] == dt.date(2001, 6, 1)
    assert not rows["APPROVED"]["is_current"]
    assert rows["CANCELLED POST MARKET"]["is_current"]
    assert rows["CANCELLED POST MARKET"]["valid_to"] is None


def test_schema_enforcement_fails_on_malformed_row(spark, raw_dir):
    bad = raw_dir / "drug.txt"
    bad.write_text(bad.read_text() + '"only","three","fields"\n')
    with pytest.raises(Py4JJavaError, match="MALFORMED"):
        read_raw(spark, raw_dir, "drug").collect()


def test_status_dedup_preserves_current_flag_and_orders_same_day_events(spark):
    raw = spark.createDataFrame(
        [
            ("1", "N", "APPROVED", "01-JAN-2000"),
            ("1", "N", "DORMANT", "01-JAN-2001"),
            ("1", "Y", "APPROVED", "01-JAN-2001"),
            ("1", "N", "APPROVED", "01-JAN-2001"),
        ],
        "drug_code string, current_status_flag string, status string, history_date string",
    )
    rows = build_status_scd2(clean_status(raw)).where("is_current").collect()
    assert len(rows) == 1
    assert rows[0].status == "APPROVED"
    assert rows[0].source_is_current is True


def test_ingredient_text_strength_is_preserved(spark):
    from dpd_etl.transform import clean_ingredients

    raw = spark.createDataFrame(
        [("1", "Example", "1:1000", "MG/ML")],
        "drug_code string, ingredient string, strength string, strength_unit string",
    )
    assert clean_ingredients(raw).first().strength == "1:1000"


def test_company_and_atc_selection_is_deterministic(spark):
    from dpd_etl.transform import clean_atc, clean_companies

    companies = spark.createDataFrame(
        [
            ("1", "2", "Z MAIL AGENT", "ONTARIO", "MAILING"),
            ("1", "3", "B OWNER", "ALBERTA", "DIN_OWNER"),
            ("1", "1", "A OWNER", "ONTARIO", "DIN_OWNER"),
        ],
        "drug_code string, company_code string, company_name string, province string, "
        "company_type string",
    )
    assert clean_companies(companies).first().company_name == "A OWNER"
    atc = spark.createDataFrame(
        [("1", "N01", "Z"), ("1", "A01", "A")],
        "drug_code string, atc_number string, atc_name string",
    )
    assert clean_atc(atc).first().atc_code == "A01"


def test_product_dedup_breaks_equal_date_and_din_ties_stably(spark):
    raw = spark.createDataFrame(
        [
            ("1", "", "Human", "00123456", brand, "", "N", "1", "01-JAN-2020")
            for brand in ["Z BRAND", "A BRAND"]
        ],
        "drug_code string, product_categorization string, class string, din string, "
        "brand_name string, descriptor string, pediatric_flag string, number_of_ais string, "
        "last_update_date string",
    )
    assert clean_drugs(raw).first().brand_name == "A BRAND"
