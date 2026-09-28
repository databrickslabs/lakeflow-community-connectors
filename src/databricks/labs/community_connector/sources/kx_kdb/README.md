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
| Streaming offset | Latest completed HDB date partition |
| Parallelism | One Spark partition per `(date, symbol)` |
| Schema | Inferred from the first date partition's splayed metadata through PyKX |
| Serverless Lakeflow | Validated |

Snapshot ingestion, date-only task partitioning, and paths that are not
visible to every executor are not supported.

## Prerequisites

- A Unity Catalog Volume containing the HDB.
- `READ VOLUME` for the Lakeflow pipeline identity.
- PyKX in the pipeline environment, or a configured PyKX wheel/package spec.
- A customer-provided commercial KX license that permits third-party-cloud use.
- KDB-X already available, staged as an offline bundle, or installable with a
  secret-backed KX installer token.

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
the parameters from `connector_spec.yaml`.

The recommended deployment is to provide a licensed PyKX/KDB-X runtime and
license through customer-controlled paths:

```yaml
hdb_root_path: /Volumes/<catalog>/<schema>/<volume>/hdb
license_volume_path: /Volumes/<catalog>/<schema>/<volume>/keys
```

The optional online bootstrap uses only a KX bearer token and license supplied
by the customer. Store both values in customer-controlled Databricks secrets
and expose them through secret-backed connection options:

```sql
CREATE CONNECTION kx_hdb TYPE GENERIC_LAKEFLOW_CONNECT
OPTIONS (
  sourceName 'kx_kdb',
  hdb_root_path '/Volumes/<catalog>/<schema>/<volume>/hdb',
  license_volume_path '/Volumes/<catalog>/<schema>/<volume>/keys',
  kdbx_install_mode 'online',
  kdbx_install_bearer_token secret('<scope>', '<installer-token-key>'),
  kdbx_license_b64 secret('<scope>', '<license-key>')
);
```

Do not place bearer tokens or license contents directly in source control or
pipeline specifications.

For a commercial `k4.lic` license supplied through `kdbx_license_b64` or
secrets, also set `kdbx_license_kind 'k4'`. A license read through
`kdbx_license_file_path` uses its file name (`k4.lic` or `kc.lic`).

The online bootstrap downloads the installer directly from KX into a
process-local temporary directory, executes it there, and removes the
temporary directory afterward. The connector does not persist, cache, upload,
or re-serve the installer to another customer or workspace. It has no
Databricks-held KX credential or license and never falls back to one. Missing
or incomplete customer bootstrap credentials produce an error or use the
customer-provided preinstalled runtime path.

## Security boundaries

- HDB location, license, and KDB-X/PyKX bootstrap settings are read only from
  the Unity Catalog connection. Pipeline table configuration can set only the
  table options listed below.
- Table, column, and date names from the HDB are passed to q as arguments,
  never as q source. The connector opens individual splayed partitions and
  never loads the HDB root as a q database, so q scripts stored in the HDB are
  not executed.
- Symbolic links inside the HDB root (date partitions, table directories,
  column files, `sym`) are rejected. Column names must be single path
  components.
- The installer bearer token is passed to `curl` on standard input, not on the
  command line. A base64 license that must be materialized locally is written
  to an owner-only (`0600`) file in an owner-only (`0700`) directory; the
  license is never exported as an environment variable. Subprocesses do not
  inherit `KDB_LICENSE_B64`/`KDB_K4LICENSE_B64` or any environment value equal
  to a connector secret, and subprocess errors are redacted.
- `install_kdb.sh` accepts the license only as a command-line argument
  (`--b64lic`/`--k4b64lic`), so the base64 license is visible to processes
  running as the same user in the same container while the installer runs.
  Use a preinstalled runtime to avoid this exposure.
- The online installer URL tracks KX's latest release and the default PyKX
  package spec resolves from the configured package index. For reproducible,
  reviewed runtimes, stage an offline bundle and set `pykx_install_spec` to a
  vetted wheel. Offline bundle members with absolute paths, `..` components,
  or symbolic links are rejected before extraction.

## Connection parameters

| Parameter | Required | Description |
| --- | --- | --- |
| `hdb_root_path` | Yes | HDB root containing `sym` and date directories |
| `license_volume_path` | Yes | KX license/runtime working directory |
| `kdbx_install_mode` | No | `auto` (default), `offline`, or `online` |
| `kdbx_offline_bundle_path` | No | Explicit KDB-X air-gapped bundle; use `l64arm-bundle.zip` on current serverless ARM64 and `l64-bundle.zip` on x86_64 |
| `kdbx_license_file_path` | No | Existing license file or directory; `k4.lic` selects a commercial k4 license |
| `kdbx_license_kind` | No | `k4` (commercial `k4.lic`) or `kc` (`kc.lic`); defaults to the license file name, otherwise `kc` |
| `kdbx_install_bearer_token` | No | Secret-backed installer token |
| `kdbx_license_b64` | No | Secret-backed base64 license |
| `kdbx_secret_scope` | No | Legacy secret-scope lookup |
| `kdbx_install_bearer_secret_key` | No | Legacy token key |
| `kdbx_license_b64_secret_key` | No | Legacy license key |
| `pykx_install_spec` | No | PyKX wheel path or package spec |
| `discovery_sample_dates` | No | Dates sampled during discovery; `0` scans all |

## Table options

| Option | Default | Description |
| --- | --- | --- |
| `start_date` | First date | Inclusive partition filter |
| `end_date` | Last date | Inclusive partition filter |
| `ingestion_mode` | `append` | Only `append` is supported |
| `partition_strategy` | `date_sym` | Only `date_sym` is supported |
| `sym_column` | `sym` | Physical enumerated symbol column |

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
          "partition_strategy": "date_sym"
        }
      }
    },
    {
      "table": {
        "source_table": "quotes",
        "destination_table": "quotes_raw",
        "table_configuration": {
          "sym_column": "optionId"
        }
      }
    }
  ]
}
```

## Read model

1. The driver lists date directories by name and loads the root symbol
   enumeration.
2. `get_partitions()` emits JSON-serializable
   `{date_partition, sym, sym_index}` descriptors. `sym_index` is the
   symbol's position in the root `sym` file, including the empty symbol.
3. Each executor loads the root `sym` domain and filters the configured symbol
   column by integer enumeration index.
4. Matching rows are read in slices of at most 10,000 rows; the last slice
   reads only the remaining rows. Records are emitted as Python dictionaries.
5. A committed date offset prevents completed HDB dates from being read again.

## Limitations

- The schema comes from the first selected date partition. Columns that exist
  only in later partitions are not ingested.
- Every selected date is paired with every symbol in the root `sym` file, so
  very large symbol files create many empty tasks.
- Directory listings are reused for up to five minutes per Python worker.
- Segmented HDBs (`par.txt`) are not supported.

See `kx_kdb_api_doc.md` for the complete source, schema, offset, and runtime
contract.
