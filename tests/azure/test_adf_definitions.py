"""Check the Data Factory JSON the way the service would read it, without an Azure account."""

import json
import re
from pathlib import Path

from dpd_etl import sqlserver

ROOT = Path(__file__).resolve().parents[2]
ADF = ROOT / "adf"
BICEP = ROOT / "infra" / "azure" / "data-factory.bicep"
LINKED_SERVICES = {"ExtractBlobStorage", "DpdSqlDatabase"}


def definitions(kind: str) -> dict:
    return {p.stem: json.loads(p.read_text())["properties"] for p in (ADF / kind).glob("*.json")}


def reference_names(node):
    if isinstance(node, dict):
        if "referenceName" in node:
            yield node["referenceName"]
        for value in node.values():
            yield from reference_names(value)
    elif isinstance(node, list):
        for value in node:
            yield from reference_names(value)


def activities(node):
    if isinstance(node, dict):
        if "activities" in node and isinstance(node["activities"], list):
            for activity in node["activities"]:
                yield activity
                yield from activities(activity)
        for key, value in node.items():
            if key != "activities":
                yield from activities(value)
    elif isinstance(node, list):
        for value in node:
            yield from activities(value)


def test_every_json_file_is_loaded_by_the_bicep_template():
    text = BICEP.read_text()
    for path in ADF.glob("*/*.json"):
        assert f"loadJsonContent('../../adf/{path.parent.name}/{path.name}')" in text, path.name
    for name in LINKED_SERVICES:
        assert f"name: '{name}'" in text


def test_references_resolve_to_defined_resources():
    datasets = definitions("datasets")
    pipelines = definitions("pipelines")
    known = LINKED_SERVICES | set(datasets) | set(pipelines)
    for name, props in datasets.items():
        assert props["linkedServiceName"]["referenceName"] in LINKED_SERVICES, name
    for name, props in {**pipelines, **definitions("triggers")}.items():
        for ref in reference_names(props):
            assert ref in known, (name, ref)


def test_copy_datasets_receive_exactly_their_declared_parameters():
    datasets = definitions("datasets")
    for activity in activities(definitions("pipelines")["MonthlyExtractToSql"]):
        if activity.get("type") != "Copy":
            continue
        for side in ("inputs", "outputs"):
            for ref in activity[side]:
                declared = set(datasets[ref["referenceName"]]["parameters"])
                assert set(ref["parameters"]) == declared, ref["referenceName"]


def test_every_activity_retries_and_has_a_timeout():
    checked = 0
    for activity in activities(definitions("pipelines")["MonthlyExtractToSql"]):
        if activity.get("type") in {"Copy", "SqlServerStoredProcedure"}:
            checked += 1
            assert activity["policy"]["retry"] >= 2, activity["name"]
            assert activity["policy"]["timeout"], activity["name"]
    assert checked == 3


def test_table_loop_follows_the_parent_first_order_the_python_loader_uses():
    pipeline = definitions("pipelines")["MonthlyExtractToSql"]
    assert pipeline["parameters"]["tables"]["defaultValue"] == list(sqlserver.DDL)


def test_stored_procedures_exist_in_the_python_module():
    names = set()
    for activity in activities(definitions("pipelines")["MonthlyExtractToSql"]):
        if activity.get("type") == "SqlServerStoredProcedure":
            names.add(re.fullmatch(r"\[dbo\]\.\[(\w+)\]", activity["typeProperties"]["storedProcedureName"])[1])
    assert names == {"usp_clear_mirror", "usp_refresh_flagged_strength_summary"}
    assert names <= set(sqlserver.PROCEDURES)


def test_monthly_trigger_passes_the_first_day_as_snapshot_date():
    trigger = definitions("triggers")["MonthlySchedule"]
    recurrence = trigger["typeProperties"]["recurrence"]
    assert (recurrence["frequency"], recurrence["schedule"]["monthDays"]) == ("Month", [1])
    value = trigger["pipelines"][0]["parameters"]["snapshotDate"]["value"]
    assert value == "@formatDateTime(trigger().scheduledTime, 'yyyy-MM-01')"


def test_definitions_contain_no_connection_secret():
    for path in ADF.glob("*/*.json"):
        assert "Password=" not in path.read_text(), path.name
