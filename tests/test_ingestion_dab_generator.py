import subprocess
import textwrap
from pathlib import Path

import pytest



def test_ingestion_dab_generator_balancing(tmp_path: Path):
    yaml = pytest.importorskip("yaml")

    repo = Path(__file__).resolve().parents[1]
    import sys
    sys.path.insert(0, str(repo))
    from libs.spec_parser import SpecParser
    gen = repo / "tools/ingestion_dab_generator/generate_ingestion_dab_yaml.py"

    csv_path = tmp_path / "in.csv"
    out_path = tmp_path / "out.yml"

    # JSON-in-CSV must be properly quoted.
    csv_path.write_text(
        textwrap.dedent(
            """\
            connection_name,source_table,destination_catalog,destination_schema,destination_table,pipeline_group,schedule,table_options_json,weight
            my_conn,pi_timeseries,main,bronze,pi_timeseries_a,,*/15 * * * *,"{""nameFilter"":""A_*""}",100
            my_conn,pi_timeseries,main,bronze,pi_timeseries_b,,*/15 * * * *,"{""nameFilter"":""B_*""}",10
            my_conn,pi_timeseries,main,bronze,pi_timeseries_c,,*/15 * * * *,"{""nameFilter"":""C_*""}",90
            """
        )
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
            "osipi",
            "--connection-name",
            "my_conn",
            "--dest-catalog",
            "main",
            "--dest-schema",
            "bronze",
            "--num-pipelines",
            "2",
            "--emit-jobs",
        ]
    )

    data = yaml.safe_load(out_path.read_text())
    # Validate that each ingestion_definition conforms to the PipelineSpec model
    for pdef in data["resources"]["pipelines"].values():
        SpecParser(pdef["ingestion_definition"])
    assert "resources" in data
    assert "pipelines" in data["resources"]
    assert len(data["resources"]["pipelines"]) == 2
    assert "jobs" in data["resources"]
    assert len(data["resources"]["jobs"]) == 2



def test_ingestion_dab_generator_prefix_priority(tmp_path: Path):
    yaml = pytest.importorskip("yaml")

    repo = Path(__file__).resolve().parents[1]
    import sys
    sys.path.insert(0, str(repo))
    from libs.spec_parser import SpecParser

    gen = repo / "tools/ingestion_dab_generator/generate_ingestion_dab_yaml.py"

    csv_path = tmp_path / "in_prefix_priority.csv"
    out_path = tmp_path / "out_prefix_priority.yml"

    # pipeline_group is empty; prefix+priority should derive grouping.
    csv_path.write_text(
        textwrap.dedent(
            """            connection_name,source_table,destination_catalog,destination_schema,destination_table,pipeline_group,prefix,priority,schedule,table_options_json,weight
            my_conn,issues,main,bronze,issues_core,,core,01,*/15 * * * *,"{""owner"":""o"",""repo"":""r1""}",100
            my_conn,issues,main,bronze,issues_support,,support,02,*/30 * * * *,"{""owner"":""o"",""repo"":""r2""}",10
            """
        )
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
            "github",
            "--connection-name",
            "my_conn",
            "--dest-catalog",
            "main",
            "--dest-schema",
            "bronze",
            "--emit-jobs",
        ]
    )

    data = yaml.safe_load(out_path.read_text())
    assert len(data["resources"]["pipelines"]) == 2
    assert len(data["resources"]["jobs"]) == 2

    for pdef in data["resources"]["pipelines"].values():
        SpecParser(pdef["ingestion_definition"])
