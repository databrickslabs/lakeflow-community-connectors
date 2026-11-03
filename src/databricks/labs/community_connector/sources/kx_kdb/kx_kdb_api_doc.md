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

## Date admission

`latest_offset()` walks the dates after the committed offset in order and
admits a date only when its table partition is complete:

- `.d` exists and every column it lists has a file;
- for the table's newest date, every column has the same length;
- when `min_partition_age_minutes` is set, no file of a date not yet admitted
  changed within that time.

If the newest date, or a date that changed within the minimum age, is not
complete, the batch stops before it and the offset does not move past it. Any
other incomplete date fails the batch with the partition path and reason; it
is never skipped. A date without the table's directory (a sparse table) is
admitted as empty and checked again in later batches.

These checks rely on the writer creating `.d` after the column files, as
`.Q.dpft` does. For a writer that creates `.d` first or rewrites columns in
place, set `min_partition_age_minutes`; executors also fail a task when any
column's length differs from the planned row count.

Admission stops at `max_dates_per_batch` dates or when the next date would
exceed `max_tasks_per_batch` tasks. The first date is always admitted, so a
single date with more tasks than the cap is read in a batch of its own and a
warning is logged. Batch reads (no offsets) apply the same completeness rules
without the caps.

## Partition model

`get_partitions()` takes each admitted date's row count on the driver (q
`count` of the first column in `.d`, of every column for the newest date) and
splits the date into contiguous row ranges of at most `max_rows_per_task`
rows:

```json
{
  "date_partition": "2024.01.02",
  "row_offset": 2000000,
  "row_count": 2000000,
  "total_rows": 5123456
}
```

Dates with no rows produce no descriptors. Row counts of older complete dates
are cached on the driver; the newest date is checked again each batch.

`read_partition()` loads the root `sym` domain, then fails if any schema
column is missing or does not have `total_rows` rows. It reads rows
`row_offset + til row_count` of every column in chunks of at most 10,000 rows,
so a task touches only its own rows and does not need the whole date in
Python memory. Rows keep their on-disk order. Every row of the date is read
exactly once, including rows whose symbol index is null or beyond the root
`sym` file; those symbols are ingested as an empty string. Copy or sync the
root `sym` file before the date directories that use it.
`read_partition()` yields one Python `dict` per row, keyed by the schema
column names.

Symbolic links inside the HDB root are rejected for date partitions, table
directories, every file in a table partition (including `.d` and
nested-column companion files), and `sym`.

## Connection parameters

| Parameter | Required | Purpose |
| --- | --- | --- |
| `hdb_root_path` | Yes | Absolute FUSE path containing `sym` and date directories |
| `license_volume_path` | Yes | Directory holding `kc.lic` or `k4.lic`; bounds every bootstrap file to its Unity Catalog Volume |
| `kdbx_offline_bundle_path` | No | KDB-X offline bundle in the license Volume: `l64arm-bundle.zip` for current serverless ARM64, `l64-bundle.zip` for x86_64; defaults to a bundle in `license_volume_path` or its sibling `kdbx` directory |
| `kdbx_offline_bundle_sha256` | No | Recommended SHA-256 of the offline bundle |
| `kdbx_license_file_path` | No | KX license file or directory in the license Volume; defaults to `license_volume_path`; `k4.lic` selects a commercial k4 license |
| `kdbx_license_kind` | No | `k4` or `kc`; selects `--k4b64lic`/`k4.lic` or `--b64lic`/`kc.lic`; defaults to the license file name, otherwise `kc` |
| `discovery_sample_dates` | No | Date count used for table discovery; `0` scans all |

On managed ingestion, pipeline table configuration can supply connection keys
that the connection leaves unset, and the connector cannot tell where a key
came from. The bootstrap options above can therefore only select files inside
the Volume of `license_volume_path`, which is always set on the connection.
These options of earlier builds are rejected: `kdbx_install_mode`,
`kdbx_install_bearer_token`, `kdbx_license_b64`, `kdbx_secret_scope`,
`kdbx_install_bearer_secret_key`, `kdbx_license_b64_secret_key`, and
`pykx_install_spec`. The `tableConfigs` payload passed to the metadata reader
is not parsed into connection options.

## Runtime setup

The connector never downloads KDB-X. Either:

- PyKX and KDB-X are already in the pipeline environment, and `QLIC` points
  at `license_volume_path`; or
- a KDB-X offline bundle is staged in the license Volume. The driver records
  the bundle's size and modification time. On each node, the first Python
  process takes a file lock in an owner-only directory under the temporary
  directory, copies the bundle to local disk, checks it against the recorded
  size and modification time and the optional SHA-256, and runs
  `install_kdb.sh --offline -y`. Later processes on the node reuse the
  install. A directory that is not owner-only, or is a symbolic link, is not
  reused; a private directory is used instead.

Managed-ingestion analysis workers do not receive the pipeline environment.
With an offline bundle, a process that cannot import PyKX therefore runs
`python -I -m pip install --target <dir> --pre pykx==4.0.0b5` into the same
owner-only directory, under the same lock, once per node. The requirement is
fixed in the connector; no option changes it. Pip does not inherit
`PYTHONPATH`, `PYTHONHOME`, KDB license variables, or the license value.

PyKX is imported after setup. Setup sets only `QLIC`, `QHOME` (the install's
`.kx` directory), `PYKX_LICENSED`, and adds the install's `bin` directory to
`PATH` once. The PyKX fallback directory is appended once to `sys.path`, so
the runtime's own packages keep precedence, and to `PYTHONPATH`, because PyKX
imports itself in a child process while it initializes embedded q. With an
offline bundle, `HOME` points at the install only while PyKX is imported,
because embedded q reads `$HOME/.kx/config` at startup and the process `HOME`
may not be readable (`/root` on serverless workers); it is restored
afterwards. The working directory is not changed. Processes wait at most 25 minutes for another process's
install on the same node.

Secret handling:

- `install_kdb.sh` accepts the license only as an argument, so the base64
  license is visible to other processes in the container while the installer
  runs. A preinstalled runtime avoids this exposure. The installer receives
  only locale, proxy, and certificate environment variables, and its standard
  input is closed.
- A materialized license file is `0600` inside a `0700` directory. The license
  is not exported as `KDB_LICENSE_B64`.
- Installer error output is redacted.
- Offline bundle members with absolute paths, `..` components, or symbolic
  links are rejected.

## Table options

| Option | Default | Purpose |
| --- | --- | --- |
| `start_date` | First available date | Inclusive lower discovery filter |
| `end_date` | Last available date | Inclusive upper discovery filter |
| `ingestion_mode` | `append` | Must remain `append` |
| `max_dates_per_batch` | `250` | Most dates admitted to one micro-batch |
| `max_tasks_per_batch` | `2000` | Most tasks in one micro-batch, except a single larger date |
| `max_rows_per_task` | `2000000` | Most rows in one task |
| `min_partition_age_minutes` | `0` | Minimum time since a date's last file change before it is admitted |

`partition_strategy` and `sym_column` from earlier builds are rejected.

## Runtime requirements

- A Lakeflow runtime with Unity Catalog Volume FUSE access.
- `pykx==4.0.0b5` in the pipeline environment, or package-index access for
  the once-per-node fallback when an offline bundle is used.
- A customer-provided commercial KX license that permits third-party-cloud use.
- `READ VOLUME` on the HDB, license, and optional offline-bundle locations.

PyKX uses a dual-license model, including commercial terms for its `q.so`
components. PyKX, `q.so`, KDB-X, the KX installer, offline bundles, and license
files are not bundled or redistributed with this connector package. For this
connector on Databricks, Personal and Community licenses are not supported.
Customers must obtain a commercial KX license whose terms expressly permit
deployment on a third-party cloud platform.

See [NOTICE.md](NOTICE.md) for dependency attributions and the copyleft review.

[kx-license]: https://code.kx.com/pykx/4.0/license.html
