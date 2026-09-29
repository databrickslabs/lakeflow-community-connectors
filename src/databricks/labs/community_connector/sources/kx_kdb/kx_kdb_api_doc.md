# KX KDB+ HDB source contract

## Source

The connector reads immutable, splayed KDB+ HDB tables from a filesystem path
that is visible to the Lakeflow driver and executors. On Databricks, the
supported production layout is a Unity Catalog Volume mounted under
`/Volumes/<catalog>/<schema>/<volume>/...`.

The connector does not connect to a running q process. It opens the HDB files
locally through PyKX.

## HDB layout

```text
<hdb_root_path>/
├── sym
├── YYYY.MM.DD/
│   ├── TRADES/
│   │   ├── .d
│   │   ├── sym
│   │   ├── time
│   │   └── price
│   └── QUOTES/
│       ├── .d
│       ├── sym
│       ├── time
│       ├── bid
│       └── ask
```

`sym` is the root KDB symbol enumeration. Date directories must use the
`YYYY.MM.DD` form. Each child directory under a date is exposed as a
lower-case Lakeflow table name, so `TRADES` is returned as `trades`.

The connector is table-agnostic: any splayed HDB table that follows this
layout can be discovered and ingested.

## Discovery and schemas

`list_tables()` scans date directories and returns the union of their table
children. `discovery_sample_dates` can limit the number of dates inspected
during discovery.

`get_table_schema()` reads `meta` of the first selected splayed partition
with PyKX. The HDB root is never loaded as a q database. KDB types are mapped
to Spark scalar types. A string `date` column is added when the physical table
does not already contain one. Duplicate column names are removed
case-insensitively. Column names must be single path components.

Common mappings include:

| KDB type | Spark type |
| --- | --- |
| boolean | `BooleanType` |
| byte, short, int | `IntegerType` |
| long | `LongType` |
| real | `FloatType` |
| float | `DoubleType` |
| char, string, symbol | `StringType` |
| timestamp/datetime | `TimestampType` (microsecond precision) |
| timespan | `LongType` (nanoseconds) |
| date/month/minute/second/time | `StringType` in q format, such as `2024.01.31` or `09:30:00.123` |

Null integer, float, timestamp, and temporal values become Spark nulls. q
integer infinities (`0W`) keep their integer value. The null symbol is
ingested as an empty string; null chars and null GUIDs keep their q values
(a space and the all-zero GUID).

## Ingestion contract

All discovered tables use:

- `ingestion_type`: `append`
- `primary_keys`: `null`
- `cursor_field`: `null`
- partitioned streaming through `SupportsPartitionedStream`

The streaming offset is:

```json
{"date_partition": "2024.01.02"}
```

Offsets are exclusive at the start and inclusive at the end. Given start
`2024.01.01` and end `2024.01.03`, the connector returns work for
`2024.01.02` and `2024.01.03`. Equal start and end offsets return no
partitions.

## Partition model

The only supported partition strategy is `date_sym`. The driver reads the
root `sym` enumeration once and emits one descriptor for every selected
date and symbol:

```json
{
  "date_partition": "2024.01.02",
  "sym": "AAPL",
  "sym_index": 0
}
```

`sym_index` is the symbol's position in the root `sym` file. The empty
symbol and symbols containing `/` keep their positions. Each date also gets
one descriptor with `sym_index: null` and `sym_count` (the number of symbols
in the root `sym` file). It reads the rows whose enumeration index is null or
not below `sym_count`; those rows are ingested with an empty `sym`.

`sym_column` must name an enumerated symbol column of the table.

Executors load the root `sym` domain, then filter the table's symbol column by
integer enumeration index. The physical symbol column defaults to `sym` and
can be changed per table with `sym_column`. The root enumeration file remains
`sym`.

Matching rows are read in chunks of at most 10,000 rows so a single active
symbol does not require the whole date partition to fit in Python memory. The
last chunk reads exactly the remaining rows. `read_partition()` yields one
Python `dict` per row, keyed by the schema column names.

Symbolic links inside the HDB root are rejected for date partitions, table
directories, every file in a table partition (including `.d` and
nested-column companion files), and `sym`.

## Connection parameters

| Parameter | Required | Purpose |
| --- | --- | --- |
| `hdb_root_path` | Yes | Absolute FUSE path containing `sym` and date directories |
| `license_volume_path` | Yes | Directory used for `QLIC` and KDB-X runtime files |
| `kdbx_install_mode` | No | `auto`, `offline`, or `online` |
| `kdbx_offline_bundle_path` | No | Explicit KDB-X bundle: `l64arm-bundle.zip` for current serverless ARM64, `l64-bundle.zip` for x86_64 |
| `kdbx_license_file_path` | No | Existing KX license file or directory; `k4.lic` selects a commercial k4 license |
| `kdbx_license_kind` | No | `k4` or `kc`; selects `--k4b64lic`/`k4.lic` or `--b64lic`/`kc.lic`; defaults to the license file name, otherwise `kc` |
| `kdbx_install_bearer_token` | No | Secret-valued online installer token |
| `kdbx_license_b64` | No | Secret-valued base64 KX license |
| `kdbx_secret_scope` | No | Legacy Databricks secret-scope lookup |
| `kdbx_install_bearer_secret_key` | No | Legacy installer-token secret key |
| `kdbx_license_b64_secret_key` | No | Legacy license secret key |
| `pykx_install_spec` | No | PyKX wheel path or package spec when not preinstalled |
| `discovery_sample_dates` | No | Date count used for table discovery; `0` scans all |

Direct bearer-token and base64-license values must be supplied through
secret-backed Unity Catalog connection options. They must never appear in a
pipeline specification or logs.

Connection parameters are read only from connection-level options. The
`tableConfigs` payload passed to the metadata reader never overrides them, and
per-table options are limited to `external_options_allowlist`.

## Bootstrap isolation

The recommended deployment uses a customer-provided licensed PyKX/KDB-X
runtime. The optional online mode uses only the customer's KX bearer token and
license. It downloads `install_kdb.sh` directly from KX into a process-local
temporary directory, executes it there, and removes that directory afterward.

The connector does not store the installer in a shared cache or Volume and
does not upload or re-serve it to another customer or workspace. It contains
no shared or Databricks-held KX credential or license, and there is no fallback
to one.

Secret handling:

- `curl -q --config -` receives the bearer token on standard input.
- `install_kdb.sh` accepts the license only as an argument, so the base64
  license is visible to other processes in the container while the installer
  runs. A preinstalled runtime avoids this exposure. The installer receives
  only locale, proxy, and certificate environment variables.
- A materialized license file is `0600` inside a `0700` directory. The license
  is not exported as `KDB_LICENSE_B64`.
- Child processes do not inherit KDB license variables or values equal to a
  connector secret. Subprocess error output is redacted.
- Offline bundles are copied to a process-local cache keyed by source path,
  size, and modification time. Members with absolute paths, `..` components,
  or symbolic links are rejected.
- The default installer URL tracks KX's latest release. Stage an offline
  bundle and set `pykx_install_spec` to a vetted wheel for reproducible
  runtimes.

## Table options

| Option | Default | Purpose |
| --- | --- | --- |
| `start_date` | First available date | Inclusive lower discovery filter |
| `end_date` | Last available date | Inclusive upper discovery filter |
| `ingestion_mode` | `append` | Must remain `append` |
| `partition_strategy` | `date_sym` | Must remain `date_sym` |
| `sym_column` | `sym` | Physical enumerated column used for symbol filtering |

## Runtime requirements

- A Lakeflow runtime with Unity Catalog Volume FUSE access.
- PyKX available in the pipeline environment or installable from the
  configured package spec.
- A customer-provided commercial KX license that permits third-party-cloud use.
- `READ VOLUME` on the HDB, license, wheel, and optional offline-bundle
  locations.

PyKX uses a dual-license model, including commercial terms for its `q.so`
components. PyKX, `q.so`, KDB-X, the KX installer, offline bundles, and license
files are not bundled or redistributed with this connector package. For this
connector on Databricks, Personal and Community licenses are not supported.
Customers must obtain a commercial KX license whose terms expressly permit
deployment on a third-party cloud platform.

See [NOTICE.md](NOTICE.md) for dependency attributions and the copyleft review.

[kx-license]: https://code.kx.com/pykx/4.0/license.html
