"""Write the SQL mirror tables as CSV files, then upload them for Azure Data Factory.

Data Factory copies these files into Azure SQL Database with a parameterised Copy activity.
The column names in each header match the SQL table, so the copy maps columns by name, and
booleans are written as 1 or 0 because SQL Server's bit type reads those directly.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path

import duckdb

from . import sqlserver
from .elastic import validate_date

DEFAULT_CONTAINER = "sql-extracts"
MANIFEST = "manifest.json"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _select_list(columns: list[tuple[str, str]]) -> str:
    parts = []
    for name, kind in columns:
        quoted = '"' + name.replace('"', '""') + '"'
        parts.append(f"CAST({quoted} AS INTEGER) AS {quoted}" if kind == "BOOLEAN" else quoted)
    return ", ".join(parts)


def export(warehouse_db: Path, snapshot: str, output_dir: Path) -> dict:
    """Write one CSV per mirror table into output_dir/<snapshot>/ and return row counts."""
    validate_date(snapshot)
    target = output_dir / snapshot
    target.mkdir(parents=True, exist_ok=True)
    tables = {}
    with duckdb.connect(str(warehouse_db), read_only=True) as source:
        for name in sqlserver.DDL:
            columns = source.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'analytics' AND table_name = ? ORDER BY ordinal_position",
                [name],
            ).fetchall()
            if not columns:
                raise ValueError(f"analytics.{name} is missing from the warehouse")
            path = target / f"{name}.csv"
            literal = str(path).replace("'", "''")
            source.execute(
                f'COPY (SELECT {_select_list(columns)} FROM analytics."{name}") '
                f"TO '{literal}' (HEADER, DELIMITER ',')"
            )
            rows = source.execute(f'SELECT count(*) FROM analytics."{name}"').fetchone()[0]
            tables[name] = {"rows": rows, "sha256": _sha256(path)}
    manifest = {"snapshot_date": snapshot, "tables": tables}
    (target / MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def upload(connection_string: str, container: str, source_dir: Path, snapshot: str) -> int:
    """Upload the CSV files and manifest to <container>/<snapshot>/, replacing the same names."""
    from azure.storage.blob import BlobServiceClient, ContentSettings

    validate_date(snapshot)
    folder = source_dir / snapshot
    files = sorted(folder.glob("*.csv")) + [folder / MANIFEST]
    if sum(1 for f in files if f.suffix == ".csv") != len(sqlserver.DDL):
        raise ValueError(f"{folder} does not hold one CSV per mirror table; run export first")
    with BlobServiceClient.from_connection_string(connection_string) as service:
        client = service.get_container_client(container)
        if not client.exists():
            client.create_container()
        for path in files:
            with path.open("rb") as handle:
                client.upload_blob(
                    f"{snapshot}/{path.name}", handle, overwrite=True,
                    content_settings=ContentSettings(content_type="text/csv"
                                                     if path.suffix == ".csv" else "application/json"),
                )
    return len(files)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    write = commands.add_parser("export", help="Write CSV files from the dbt warehouse")
    write.add_argument("--warehouse-db", type=Path, required=True)
    write.add_argument("--snapshot-date", required=True)
    write.add_argument("--output-dir", type=Path, default=Path("data/adf"))
    send = commands.add_parser("upload", help="Upload written CSV files to Blob Storage")
    send.add_argument("--snapshot-date", required=True)
    send.add_argument("--output-dir", type=Path, default=Path("data/adf"))
    send.add_argument("--container", default=os.environ.get("DPD_ADF_CONTAINER", DEFAULT_CONTAINER))
    args = parser.parse_args(argv)
    if args.command == "export":
        manifest = export(args.warehouse_db, args.snapshot_date, args.output_dir)
        print(json.dumps(manifest["tables"], indent=2, sort_keys=True))
    else:
        connection = os.environ["DPD_ADF_CONNECTION_STRING"]
        print(f"uploaded {upload(connection, args.container, args.output_dir, args.snapshot_date)} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
