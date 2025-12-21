import subprocess
from pathlib import Path

import pytest


def _run_and_load_yaml(tmp_path: Path, *, script: Path, args: list[str]) -> dict:
    yaml = pytest.importorskip("yaml")
    subprocess.check_call(["python3", str(script), *args])
    out_path = tmp_path / "out.yml"
    return yaml.safe_load(out_path.read_text())


def test_e2e_zendesk_discover_to_yaml(tmp_path: Path):
    """End-to-end: discovery adapter -> CSV -> generator -> spec validation."""
    repo = Path(__file__).resolve().parents[1]

    # Ensure repo root is importable for libs.spec_parser
    import sys

    sys.path.insert(0, str(repo))
    from libs.spec_parser import SpecParser

    discover = repo / "tools/ingestion_dab_generator/examples/zendesk/discover_zendesk_tables.py"
    gen = repo / "tools/ingestion_dab_generator/generate_ingestion_dab_yaml.py"

    csv_path = tmp_path / "zendesk.csv"
    out_path = tmp_path / "out.yml"

    subprocess.check_call(
        [
            "python3",
            str(discover),
            "--output-csv",
            str(csv_path),
            "--dest-catalog",
            "main",
            "--dest-schema",
            "bronze",
            "--schedule",
            "*/30 * * * *",
        ]
    )

    subprocess.check_call(
        [
            "python3",
            str(gen),
            "--input-csv",
            str(csv_path),
            "--output-yaml",
            str(out_path),
            "--connector-name",
            "zendesk",
            "--connection-name",
            "my_zendesk_connection",
            "--dest-catalog",
            "main",
            "--dest-schema",
            "bronze",
            "--num-pipelines",
            "2",
            "--emit-jobs",
        ]
    )

    yaml = pytest.importorskip("yaml")
    data = yaml.safe_load(out_path.read_text())

    assert "resources" in data
    assert "pipelines" in data["resources"]
    assert len(data["resources"]["pipelines"]) == 2

    # At least one job per pipeline group when schedule is provided.
    assert "jobs" in data["resources"]
    assert len(data["resources"]["jobs"]) == 2

    for pdef in data["resources"]["pipelines"].values():
        SpecParser(pdef["ingestion_definition"])


def test_e2e_hubspot_discover_to_yaml(tmp_path: Path):
    """End-to-end (offline): HubSpot adapter without custom discovery."""
    repo = Path(__file__).resolve().parents[1]

    import sys

    sys.path.insert(0, str(repo))
    from libs.spec_parser import SpecParser

    discover = repo / "tools/ingestion_dab_generator/examples/hubspot/discover_hubspot_tables.py"
    gen = repo / "tools/ingestion_dab_generator/generate_ingestion_dab_yaml.py"

    csv_path = tmp_path / "hubspot.csv"
    out_path = tmp_path / "out.yml"

    subprocess.check_call(
        [
            "python3",
            str(discover),
            "--output-csv",
            str(csv_path),
            "--dest-catalog",
            "main",
            "--dest-schema",
            "bronze",
            "--schedule",
            "0 */2 * * *",
        ]
    )

    subprocess.check_call(
        [
            "python3",
            str(gen),
            "--input-csv",
            str(csv_path),
            "--output-yaml",
            str(out_path),
            "--connector-name",
            "hubspot",
            "--connection-name",
            "my_hubspot_connection",
            "--dest-catalog",
            "main",
            "--dest-schema",
            "bronze",
            "--num-pipelines",
            "2",
            "--emit-jobs",
        ]
    )

    yaml = pytest.importorskip("yaml")
    data = yaml.safe_load(out_path.read_text())

    assert len(data["resources"]["pipelines"]) == 2
    assert len(data["resources"]["jobs"]) == 2

    for pdef in data["resources"]["pipelines"].values():
        SpecParser(pdef["ingestion_definition"])
