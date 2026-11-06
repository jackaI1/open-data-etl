"""Package readable version-controlled exports and import into a fresh instance."""

import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile


def bundle(assets, destination):
    with ZipFile(destination, "w") as archive:
        for path in sorted(assets.rglob("*.yaml")):
            archive.write(path, "catalogue/" + path.relative_to(assets).as_posix())


def main():
    with TemporaryDirectory(prefix="dpd-superset-import-") as work:
        path = Path(work) / "dashboard.zip"
        bundle(Path(__file__).parent / "assets", path)
        subprocess.run(["superset", "import-dashboards", "-p", str(path), "-u", "admin"],
                       check=True)


if __name__ == "__main__":
    main()
