#!/usr/bin/env python3
"""Create/update an OSIPI Unity Catalog connection and run a minimal end-to-end read test.

This is OSIPI-specific (lives under sources/osipi/examples).

What it does
------------
1) Creates (or replaces) a UC Connection with:
   - pi_base_url
   - access_token via Databricks Secrets reference
2) Uploads the merged connector file to your workspace user home
3) Uploads a tiny test notebook
4) Runs the notebook on an existing cluster (auto-picks a running cluster if not provided)

Prereqs
-------
- Databricks CLI/SDK auth configured (we use ~/.databrickscfg profile)
- A secret exists: scope/key provided (token is never printed)
- You have at least one usable cluster (or pass --cluster-id)

Example
-------
python3 sources/osipi/examples/uc_connection/setup_and_test_uc_connection_via_sdk.py \
  --profile DEFAULT \
  --connection-name osipi_mock_connection \
  --pi-base-url https://mock-piwebapi-912141448724.us-central1.run.app \
  --secret-scope sp-osipi \
  --secret-key mock-bearer-token \
  --verify-ssl true
"""

from __future__ import annotations

import argparse
import base64
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType
from databricks.sdk.service import workspace as ws


def _b(v: bool) -> str:
    return "true" if v else "false"


def _import_workspace_file(w: WorkspaceClient, *, workspace_path: str, content: str) -> None:
    w.workspace.mkdirs(str(Path(workspace_path).parent))
    try:
        w.workspace.delete(workspace_path)
    except Exception:
        pass
    w.workspace.import_(
        path=workspace_path,
        format=ws.ImportFormat.SOURCE,
        language=ws.Language.PYTHON,
        content=base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        overwrite=True,
    )


def _pick_cluster_id(w: WorkspaceClient) -> str:
    # Prefer a RUNNING cluster; if none, pick the first non-terminated.
    running = []
    other = []
    for c in w.clusters.list():
        state = str(c.state or "").upper()
        if state == "RUNNING":
            running.append(c)
        elif state not in ("TERMINATED", "TERMINATING"):
            other.append(c)

    chosen = (running or other)
    if not chosen:
        raise RuntimeError("No usable clusters found. Start a cluster or pass --cluster-id.")

    # Deterministic-ish: pick smallest cluster name
    chosen.sort(key=lambda x: (x.cluster_name or "", x.cluster_id or ""))
    if not chosen[0].cluster_id:
        raise RuntimeError("Selected cluster has no cluster_id")
    return chosen[0].cluster_id



def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--profile", default="DEFAULT")
    p.add_argument("--connection-name", default="osipi_connection")
    p.add_argument("--pi-base-url", required=True)
    p.add_argument("--secret-scope", required=True)
    p.add_argument("--secret-key", required=True)
    p.add_argument("--verify-ssl", default="true", choices=["true", "false"])
    p.add_argument("--cluster-id", default=None)

    args = p.parse_args()

    w = WorkspaceClient(profile=args.profile)
    me = w.current_user.me()
    username = me.user_name
    if not username:
        raise RuntimeError("Unable to resolve current user username")

    secret_ref = f"{{{{secrets/{args.secret_scope}/{args.secret_key}}}}}"

    # 1) Create/replace UC Connection
    try:
        w.connections.delete(args.connection_name)
    except Exception:
        pass

    # UC ConnectionType.HTTP (bearer) supports exactly: host, bearer_token, base_path, port
    # We store PI base URL in `host` (must include scheme), and bearer token as secret ref.
    w.connections.create(
        name=args.connection_name,
        connection_type=ConnectionType.HTTP,
        options={
            "host": args.pi_base_url,
            "bearer_token": secret_ref,
        },
        comment="OSI PI connector test connection (created by setup_and_test_uc_connection_via_sdk.py)",
    )



    # 2) Upload merged connector source to workspace user home
    repo_root = Path(__file__).resolve().parents[4]
    gen_path = repo_root / "sources" / "osipi" / "_generated_osipi_python_source.py"
    gen_content = gen_path.read_text(encoding="utf-8")

    ws_dir = f"/Users/{username}/osipi"
    ws_gen = f"{ws_dir}/_generated_osipi_python_source.py"
    _import_workspace_file(w, workspace_path=ws_gen, content=gen_content)

    # 3) Upload a tiny notebook that registers + reads a table
    nb_path = f"{ws_dir}/test_osipi_uc_connection"
    nb = f"""# Databricks notebook source\n\n# DBTITLE 1,Register Connector\nexec(open(\"{ws_gen}\").read())\nregister_lakeflow_source(spark)\nprint(\"registered\")\n\n# DBTITLE 1,Read via UC Connection\ndf = (\n  spark.read.format(\"lakeflow_connect\")\n    .option(\"databricks.connection\", \"{args.connection_name}\")\n    .option(\"tableName\", \"pi_dataservers\")\n    .load()\n)\n\ncount = df.count()\nprint(\"pi_dataservers count=\", count)\nassert count >= 0\n"""

    w.workspace.mkdirs(ws_dir)
    w.workspace.import_(
        path=nb_path,
        format=ws.ImportFormat.SOURCE,
        language=ws.Language.PYTHON,
        content=base64.b64encode(nb.encode("utf-8")).decode("utf-8"),
        overwrite=True,
    )

    # 4) Run notebook on a cluster
    cluster_id = args.cluster_id or _pick_cluster_id(w)

    run = w.jobs.submit(
        run_name="osipi_uc_connection_smoke_test",
        tasks=[
            {
                "task_key": "osipi_uc",
                "existing_cluster_id": cluster_id,
                "notebook_task": {"notebook_path": nb_path},
            }
        ],
    )

    run_id = run.run_id
    if run_id is None:
        raise RuntimeError("Job submit returned no run_id")

    print(f"Submitted run_id={run_id} on cluster_id={cluster_id} notebook={nb_path}")
    print("Open in UI via Jobs Runs if needed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
