"""Rebuild the readable Superset 4.1 export with stable object identifiers."""

from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import yaml

ROOT = Path(__file__).resolve().parents[1] / "superset" / "assets"


def uid(name):
    return str(uuid5(NAMESPACE_URL, "https://github.com/jackaI1/open-data-etl/" + name))


def write(path, data):
    (ROOT / path).write_text(yaml.safe_dump(data, sort_keys=False))


def main():
    write("metadata.yaml", {"version": "1.0.0", "type": "Dashboard", "timestamp": "2026-11-06T00:00:00+00:00"})
    write("databases/catalogue.yaml", {"database_name": "Accepted catalogue snapshots",
          "sqlalchemy_uri": "sqlite:////app/catalogue/catalogue.db", "expose_in_sqllab": True,
          "allow_ctas": False, "allow_cvas": False, "allow_dml": False, "allow_csv_upload": False, "allow_run_async": False,
          "extra": {}, "uuid": uid("database"), "version": "1.0.0"})
    datasets = {
        "runs": {
            "columns": {"snapshot_date": "DATE", "rows_processed": "BIGINT",
                        "flagged_records": "BIGINT", "flagged_strengths": "BIGINT",
                        "duplicate_pairs": "BIGINT", "duration_seconds": "FLOAT",
                        "source_kind": "TEXT"},
            "metrics": {"products": "SUM(rows_processed)", "flagged_products": "SUM(flagged_records)",
                        "flags": "SUM(flagged_strengths)", "matches": "SUM(duplicate_pairs)",
                        "duration": "AVG(duration_seconds)"}},
        "products": {
            "columns": {"snapshot_date": "DATE", "drug_code": "BIGINT", "din": "TEXT",
                        "brand_name": "TEXT", "drug_class": "TEXT", "atc_group": "TEXT",
                        "ingredient_rows": "BIGINT", "flagged_strengths": "BIGINT",
                        "flagged_record": "BIGINT"},
            "metrics": {"product_count": "COUNT(*)", "strength_flags": "SUM(flagged_strengths)",
                        "flagged_share": "1.0 * SUM(flagged_record) / NULLIF(COUNT(*), 0)"}},
        "pairs": {
            "columns": {"snapshot_date": "DATE", "left_id": "BIGINT", "right_id": "BIGINT",
                        "score": "FLOAT", "is_match": "BIGINT", "reason": "TEXT",
                        "review_status": "TEXT"},
            "metrics": {"pair_count": "COUNT(*)", "suggested_matches": "SUM(is_match)"}},
    }
    for table, data in datasets.items():
        write(f"datasets/catalogue/{table}.yaml", {
            "table_name": table, "main_dttm_col": "snapshot_date", "description": (
                "One row per accepted date. Totals across dates count product snapshots. "
                "source_kind=example is invented demo history." if table == "runs" else
                "Dated review data. Pending means no human decision was supplied."),
            "default_endpoint": None, "offset": 0, "cache_timeout": 0, "schema": None,
            "sql": None, "params": {}, "template_params": None, "filter_select_enabled": True,
            "fetch_values_predicate": None, "extra": {}, "uuid": uid(table), "version": "1.0.0",
            "database_uuid": uid("database"),
            "columns": [{"column_name": c, "is_dttm": c == "snapshot_date", "type": t,
                          "groupby": True, "filterable": True, "expression": None,
                          "verbose_name": None, "description": None, "python_date_format": None,
                          "extra": {}} for c, t in data["columns"].items()],
            "metrics": [{"metric_name": m, "verbose_name": m.replace("_", " ").title(),
                          "metric_type": None, "expression": e, "description": None,
                          "d3format": ".1%" if m == "flagged_share" else None,
                          "extra": {}, "warning_text": None} for m, e in data["metrics"].items()]})
    specs = [
        ("Products processed", "runs", "products", "big_number_total", []),
        ("Products with flags", "runs", "flagged_products", "big_number_total", []),
        ("Suggested duplicate pairs", "runs", "matches", "big_number_total", []),
        ("Mean run seconds", "runs", "duration", "big_number_total", []),
        ("Products over time", "runs", "products", "echarts_timeseries_line", []),
        ("Flagged products over time", "runs", "flagged_products", "echarts_timeseries_line", []),
        ("Duplicate pairs over time", "runs", "matches", "echarts_timeseries_line", []),
        ("Run seconds over time", "runs", "duration", "echarts_timeseries_line", []),
        ("Flagged product share by class", "products", "flagged_share", "echarts_timeseries_bar", ["drug_class"]),
        ("Ingredient flags by class", "products", "strength_flags", "echarts_timeseries_bar", ["drug_class"]),
        ("Pair review status", "pairs", "pair_count", "pie", ["review_status"]),
    ]
    position = {"DASHBOARD_VERSION_KEY": "v2", "ROOT_ID": {
        "id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID", "children": ["ROW-NOTE"], "parents": ["ROOT_ID"]}}
    position["ROW-NOTE"] = {"id": "ROW-NOTE", "type": "ROW", "children": ["NOTE"],
        "parents": ["ROOT_ID", "GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
    position["NOTE"] = {"id": "NOTE", "type": "MARKDOWN", "children": [],
        "parents": ["ROOT_ID", "GRID_ID", "ROW-NOTE"],
        "meta": {"width": 12, "height": 12, "code": "The starter history is invented example data. Publish accepted pipeline dates to replace it. Totals count product snapshots across the selected dates; flags are suggestions to review records."}}
    for i, (name, table, metric, viz, groups) in enumerate(specs, 1):
        params = {"viz_type": viz, "datasource": "1__table", "metrics": [metric], "metric": metric,
                      "groupby": groups, "time_range": "No filter", "granularity_sqla": "snapshot_date",
                      "time_grain_sqla": "P1D", "row_limit": 10000, "adhoc_filters": [],
                      "show_legend": True, "rich_tooltip": True,
                      "color_scheme": "supersetColors", "y_axis_format": "SMART_NUMBER"}
        if viz == "big_number_total":
            params.update(subheader="Selected dates", y_axis_format="SMART_NUMBER")
        elif viz.startswith("echarts"):
            params.update(x_axis="snapshot_date" if not groups else groups[0],
                          x_axis_sort_asc=True, truncate_metric=True,
                          show_value=False)
        if metric == "flagged_share":
            params["y_axis_format"] = ".0%"
        chart_id = uid(name)
        write(f"charts/{i:02d}.yaml", {"slice_name": name, "description": None, "certified_by": None,
              "certification_details": None, "viz_type": viz, "params": params, "cache_timeout": 0,
              "uuid": chart_id, "version": "1.0.0", "dataset_uuid": uid(table)})
        row = (i - 1) // 4 if i <= 4 else 1 + (i - 5) // 2
        row_id = f"ROW-{row}"
        if row_id not in position:
            position["GRID_ID"]["children"].append(row_id)
            position[row_id] = {"id": row_id, "type": "ROW", "children": [],
                                    "parents": ["ROOT_ID", "GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        chart_key = f"CHART-{i}"
        position[row_id]["children"].append(chart_key)
        position[chart_key] = {"id": chart_key, "type": "CHART", "children": [],
             "parents": ["ROOT_ID", "GRID_ID", row_id],
             "meta": {"chartId": i, "uuid": chart_id, "width": 3 if i <= 4 else 6,
                       "height": 20 if i <= 4 else 45, "sliceName": name}}
    filters = [{"id": "NATIVE_FILTER-date", "name": "Snapshot date", "filterType": "filter_time",
                   "targets": [{}], "defaultDataMask": {"extraFormData": {}, "filterState": {}},
                   "cascadeParentIds": [], "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                   "controlValues": {}, "type": "NATIVE_FILTER", "description": "Select saved dates; totals count product snapshots."}]
    for column, table, excluded in [("drug_class", "products", list(range(1,9)) + [11]),
                                     ("review_status", "pairs", list(range(1,11)))]:
        filters.append({"id": "NATIVE_FILTER-" + column, "name": column.replace("_", " ").title(),
            "filterType": "filter_select", "targets": [{"datasetUuid": uid(table), "column": {"name": column}}],
            "defaultDataMask": {"extraFormData": {}, "filterState": {}}, "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": excluded},
            "controlValues": {"enableEmptyFilter": False, "multiSelect": True, "searchAllOptions": False,
                           "inverseSelection": False}, "type": "NATIVE_FILTER"})
    write("dashboards/catalogue.yaml", {"dashboard_title": "Catalogue quality review",
          "description": "Accepted snapshot counts and review queues. Example history is invented. Flags are review suggestions, not medical findings.",
          "css": '', "slug": "catalogue-quality", "certified_by": None, "certification_details": None,
          "published": True, "uuid": uid("dashboard"), "version": "1.0.0", "position": position,
          "metadata": {"native_filter_configuration": filters, "color_scheme": "supersetColors",
                    "refresh_frequency": 0, "expanded_slices": {}, "label_colors": {},
                    "timed_refresh_immune_slices": []}})


if __name__ == "__main__":
    main()
