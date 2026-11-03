# Lakeflow KX KDB+ HDB connector

Ingest immutable, splayed KDB+ HDB tables from a Unity Catalog Volume into
Delta streaming tables. The connector reads HDB files locally through PyKX;
it does not require a running q process.

![KX KDB HDB connector](kx_kdb.png)

## Capabilities

| Capability | Support |
| --- | --- |
| Source | Splayed HDB under `/Volumes/...` |
| Table discovery | Any table directory found below a date partition |
| Ingestion | Append-only |
| Streaming offset | Latest complete HDB date partition |
| Micro-batch size | Up to `max_dates_per_batch` dates and `max_tasks_per_batch` tasks |
| Parallelism | One Spark task per row range of a date, up to `max_rows_per_task` rows |
| Schema | Inferred from the first date partition's splayed metadata through PyKX |
| Serverless Lakeflow | Validated |

Snapshot ingestion and paths that are not visible to every executor are not
supported.

## Prerequisites

- A Unity Catalog Volume containing the HDB.
- `READ VOLUME` for the Lakeflow pipeline identity.
- PyKX (`pykx==4.0.0b5`). The connector package declares it; a merged-source
  deployment adds it to the pipeline environment dependencies. Managed
  ingestion analysis workers do not receive that environment, so with an
  offline bundle a process that cannot import PyKX installs exactly
  `pykx==4.0.0b5` from the configured package index, once per node.
- A customer-provided commercial KX license that permits third-party-cloud use.
- KDB-X either already in the pipeline environment, or staged as a KDB-X
  offline bundle in the same Unity Catalog Volume as the license. The
  connector never downloads KDB-X.

PyKX uses a dual-license model, including commercial terms for its `q.so`
components. Review the [KDB-X Python license terms][kx-license] and
[installation requirements][kx-install] before using this connector. PyKX,
`q.so`, KDB-X, the KX installer, offline bundles, and KX license files are not
bundled or redistributed with the connector. Each user must obtain and accept
the applicable KX terms and provide their own licensed runtime.

For this connector on Databricks, Personal and Community licenses are not
supported. Customers are responsible for obtaining a commercial KX license
whose terms expressly permit deployment on a third-party cloud platform.

See [NOTICE.md](NOTICE.md) for the connector dependency and attribution list.
No GPL, LGPL, or AGPL dependency is declared by the connector or identified
among its mandatory runtime dependencies.

[kx-license]: https://code.kx.com/pykx/4.0/license.html
[kx-install]: https://code.kx.com/pykx/4.0/getting-started/installing.html

## HDB layout

```text
/Volumes/<catalog>/<schema>/<volume>/hdb/
├── sym
├── 2024.01.01/
│   ├── TRADES/
│   └── QUOTES/
└── 2024.01.02/
    ├── TRADES/
    └── QUOTES/
```

The root `sym` file provides symbol enumeration indices. Date names must use
`YYYY.MM.DD`. Table discovery is case-insensitive and exposes logical
lower-case names such as `trades` and `quotes`.

## Create the connection

Create a Generic Lakeflow Connect connection with `sourceName: kx_kdb` and
the parameters from `connector_spec.yaml`. Stage the license and, unless
KDB-X is preinstalled, the KDB-X offline bundle in one Volume:

```text
/Volumes/<catalog>/<schema>/<volume>/
├── keys/
│   └── kc.lic               (or k4.lic)
└── kdbx/
    └── l64arm-bundle.zip    (l64-bundle.zip on x86_64)
```

```sql
CREATE CONNECTION kx_hdb TYPE GENERIC_LAKEFLOW_CONNECT
OPTIONS (
  sourceName 'kx_kdb',
  hdb_root_path '/Volumes/<catalog>/<schema>/<volume>/hdb',
  license_volume_path '/Volumes/<catalog>/<schema>/<volume>/keys',
  kdbx_offline_bundle_sha256 '<sha256 of l64arm-bundle.zip>'
);
```

The bundle is found in `license_volume_path` or its sibling `kdbx` directory;
set `kdbx_offline_bundle_path` to use another file in the Volume. Compute the
digest with `sha256sum l64arm-bundle.zip`. Without a bundle, `QLIC` points at
`license_volume_path` and PyKX and KDB-X must already be installed.

## KDB-X installation

When a bundle is configured, the first Python worker on each node:

1. copies the bundle from the Volume to an owner-only directory on local disk,
2. checks that the bundle still has the size and modification time the driver
   saw when the pipeline update started, and that the copy matches the
   configured SHA-256 if set,
3. runs `install_kdb.sh --offline -y` from the bundle with the license.

Other Python workers on the node wait for the install and reuse it. A bundle
that is replaced while an update runs fails that update, so all workers use
the same KDB-X build. A worker that cannot import PyKX installs the pinned
`pykx==4.0.0b5` into the same directory under the same lock, also once per
node.

## Security boundaries

- On managed ingestion, pipeline table configuration can supply options that
  the connection leaves unset; the connector cannot tell where a key came
  from. Set every bootstrap option you use on the connection. A pipeline can
  only point bootstrap options at files in the Volume of `license_volume_path`,
  which the connection owner controls. No option downloads software, chooses
  a package to install, or reads a Databricks secret; the removed options (`kdbx_install_mode`, `kdbx_install_bearer_token`,
  `kdbx_license_b64`, `kdbx_secret_scope`, `kdbx_install_bearer_secret_key`,
  `kdbx_license_b64_secret_key`, `pykx_install_spec`) are rejected.
- Table, column, and date names from the HDB are passed to q as arguments,
  never as q source. The connector opens individual splayed partitions and
  never loads the HDB root as a q database, so q scripts stored in the HDB are
  not executed.
- Symbolic links inside the HDB root are rejected: date partitions, table
  directories, every file in a table partition (including `.d` and
  nested-column companion files), and `sym`. Column names must be single path
  components.
- A license that must be materialized locally is written to an owner-only
  (`0600`) file in an owner-only (`0700`) directory; the license is never
  exported as an environment variable. The installer receives only locale,
  proxy, and certificate environment variables, and its errors are redacted.
- `install_kdb.sh` accepts the license only as a command-line argument
  (`--b64lic`/`--k4b64lic`), so the base64 license is visible to other
  processes in the same container while the installer runs. Use a
  preinstalled runtime to avoid this exposure.
- Offline bundle members with absolute paths, `..` components, or symbolic
  links are rejected before extraction.
- Setup changes only `QLIC`, `QHOME`, `PYKX_LICENSED`, and, once, `PATH` in
  the Python process. When the PyKX fallback is used, its directory is also
  appended once to `sys.path` and `PYTHONPATH` (PyKX imports itself in a child
  process while it starts). With an offline bundle, `HOME` points at the
  install only while PyKX is imported, because embedded q reads
  `$HOME/.kx/config` at startup, and is then restored. The working directory
  is not changed.
- The PyKX fallback is pinned by version, not by wheel hash.

## Connection parameters

| Parameter | Required | Description |
| --- | --- | --- |
| `hdb_root_path` | Yes | HDB root containing `sym` and date directories |
| `license_volume_path` | Yes | Directory holding `kc.lic` or `k4.lic`; every bootstrap file must be in its Volume |
| `kdbx_offline_bundle_path` | No | KDB-X offline bundle in the license Volume; use `l64arm-bundle.zip` on current serverless ARM64 and `l64-bundle.zip` on x86_64. Defaults to a bundle in `license_volume_path` or its sibling `kdbx` directory |
| `kdbx_offline_bundle_sha256` | No | Recommended SHA-256 of the offline bundle, checked on every node before installing |
| `kdbx_license_file_path` | No | License file or directory in the license Volume; defaults to `license_volume_path`. `k4.lic` selects a commercial k4 license |
| `kdbx_license_kind` | No | `k4` (commercial `k4.lic`) or `kc` (`kc.lic`); defaults to the license file name, otherwise `kc` |
| `discovery_sample_dates` | No | Dates sampled during discovery; `0` scans all |

## Table options

| Option | Default | Description |
| --- | --- | --- |
| `start_date` | First date | Inclusive partition filter |
| `end_date` | Last date | Inclusive partition filter |
| `ingestion_mode` | `append` | Only `append` is supported |
| `max_dates_per_batch` | `250` | Most dates admitted to one micro-batch |
| `max_tasks_per_batch` | `2000` | Most Spark tasks in one micro-batch; a single larger date is still admitted alone |
| `max_rows_per_task` | `2000000` | Most rows read by one Spark task |
| `min_partition_age_minutes` | `0` | Minimum time since a date's last file change before it is admitted |
| `missing_columns` | `fail` | `fail` stops at an older date whose `.d` lists a column without a file; `null` reads such columns as nulls and logs a warning. The newest date always needs every column |

Example table selection:

```json
{
  "connection_name": "kx_hdb",
  "objects": [
    {
      "table": {
        "source_table": "trades",
        "destination_table": "trades_raw",
        "table_configuration": {
          "start_date": "2024.01.01",
          "end_date": "2024.12.31",
          "max_dates_per_batch": "60"
        }
      }
    },
    {
      "table": {
        "source_table": "quotes",
        "destination_table": "quotes_raw",
        "table_configuration": {
          "max_rows_per_task": "5000000",
          "min_partition_age_minutes": "30"
        }
      }
    }
  ]
}
```

## Read model

1. The driver lists date directories by name.
2. `latest_offset()` admits dates after the committed offset, in order. A date
   is admitted only when its `.d` file and every column it lists exist. The
   table's newest date must also have columns of equal length. When
   `min_partition_age_minutes` is set, no file of the date may have changed
   within that time. The batch stops before the newest date, or a recently
   changed date, until it passes. Any other incomplete date ends the batch
   just before it, so earlier dates are committed, and then fails the next
   batch instead of being skipped. Admission stops at `max_dates_per_batch`
   dates or `max_tasks_per_batch` tasks.
3. `get_partitions()` reads each admitted date's row count from the first
   column in `.d` and emits JSON-serializable
   `{date_partition, row_offset, row_count, total_rows}` descriptors of at
   most `max_rows_per_task` rows.
4. Each executor loads the root `sym` domain, checks that every schema column
   exists and has `total_rows` rows, and reads only its rows, in slices of at
   most 10,000 rows. Records are emitted as Python dictionaries. Null symbols,
   and symbols whose index is beyond the root `sym` file, are ingested as
   empty strings.
5. The committed date offset prevents completed HDB dates from being read
   again.

## Limitations

- The schema comes from the first selected date partition. Columns that exist
  only in later partitions are not ingested, and a later partition without a
  schema column fails the read.
- Dates written earlier than the committed offset (backfills) are not read.
- Completeness relies on the writer creating `.d` after the column files, as
  `.Q.dpft` does. A writer that creates `.d` first or rewrites columns in
  place is detected only with `min_partition_age_minutes`, or when a column
  length differs from the planned row count.
- Copy or sync the root `sym` file before the date directories that use it;
  rows enumerated past the end of `sym` are ingested with an empty symbol.
- An older date whose `.d` lists a column without a file (q cannot select
  that partition either) stops ingestion until the partition is repaired, for
  example with dbmaint's `addcol`, or `missing_columns` is set to `null`.
- Directory listings are reused for up to five minutes per Python worker.
- Segmented HDBs (`par.txt`) are not supported.

See `kx_kdb_api_doc.md` for the complete source, schema, offset, and runtime
contract.
