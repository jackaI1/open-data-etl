"""Explicit schemas for the headerless DPD extract files (enforced on read, FAILFAST)."""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType


def _all_strings(*names: str) -> StructType:
    return StructType([StructField(n, StringType(), True) for n in names])


SCHEMAS: dict[str, StructType] = {
    "drug": _all_strings(
        "drug_code",
        "product_categorization",
        "class",
        "din",
        "brand_name",
        "descriptor",
        "pediatric_flag",
        "accession_number",
        "number_of_ais",
        "last_update_date",
        "ai_group_no",
        "class_f",
        "brand_name_f",
        "descriptor_f",
    ),
    "status": _all_strings(
        "drug_code",
        "current_status_flag",
        "status",
        "history_date",
        "status_f",
        "lot_number",
        "expiration_date",
    ),
    "comp": _all_strings(
        "drug_code",
        "mfr_code",
        "company_code",
        "company_name",
        "company_type",
        "address_mailing_flag",
        "address_billing_flag",
        "address_notification_flag",
        "address_other",
        "suite_number",
        "street_name",
        "city_name",
        "province",
        "country",
        "postal_code",
        "post_office_box",
        "province_f",
        "country_f",
    ),
    "ingred": _all_strings(
        "drug_code",
        "active_ingredient_code",
        "ingredient",
        "ingredient_supplied_ind",
        "strength",
        "strength_unit",
        "strength_type",
        "dosage_value",
        "base",
        "dosage_unit",
        "notes",
        "ingredient_f",
        "strength_unit_f",
        "strength_type_f",
        "dosage_unit_f",
    ),
    "ther": _all_strings("drug_code", "atc_number", "atc_name", "atc_name_f"),
    "form": _all_strings("drug_code", "pharm_form_code", "pharm_form", "pharm_form_f"),
    "route": _all_strings("drug_code", "route_code", "route", "route_f"),
}
