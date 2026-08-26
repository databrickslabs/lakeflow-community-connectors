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

`get_table_schema()` inspects the first selected partition with PyKX. KDB
types are mapped to Spark scalar types. A string `date` column is added when
the physical table does not already contain one. Duplicate column names are
removed case-insensitively.

Common mappings include:

| KDB type | Spark type |
| --- | --- |
| boolean | `BooleanType` |
| short | `ShortType` |
| int | `IntegerType` |
| long | `LongType` |
| real | `FloatType` |
| float | `DoubleType` |
| char/symbol | `StringType` |
| timestamp/datetime | `TimestampType` |
| date/time/month/minute/second | `StringType` |

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

Executors filter the table's symbol column by integer enumeration index. The
physical symbol column defaults to `sym` and can be changed per table with
`sym_column`. The root enumeration file remains `sym`.

Splayed columns are read in 10,000-row chunks so a single active symbol does
not require the whole date partition to fit in Python memory.

## Connection parameters

| Parameter | Required | Purpose |
| --- | --- | --- |
| `hdb_root_path` | Yes | Absolute FUSE path containing `sym` and date directories |
| `license_volume_path` | Yes | Directory used for `QLIC` and KDB-X runtime files |
| `kdbx_install_mode` | No | `auto`, `offline`, or `online` |
| `kdbx_offline_bundle_path` | No | Explicit KDB-X bundle: `l64arm-bundle.zip` for current serverless ARM64, `l64-bundle.zip` for x86_64 |
| `kdbx_license_file_path` | No | Existing KX license file or directory |
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

## Table options

| Option | Default | Purpose |
| --- | --- | --- |
| `start_date` | First available date | Inclusive lower discovery filter |
| `end_date` | Last available date | Inclusive upper discovery filter |
| `ingestion_mode` | `append` | Must remain `append` |
| `partition_strategy` | `date_sym` | Must remain `date_sym` |
| `partition_conversion_mode` | `pandas` | `pandas`, `arrow_pandas`, or `arrow_direct` |
| `sym_column` | `sym` | Physical enumerated column used for symbol filtering |

## Runtime requirements

- A Lakeflow runtime with Unity Catalog Volume FUSE access.
- PyKX available in the pipeline environment or installable from the
  configured package spec.
- A valid KX license.
- `READ VOLUME` on the HDB, license, wheel, and optional offline-bundle
  locations.

PyKX uses a dual-license model, including commercial terms for its `q.so`
components. PyKX, `q.so`, KDB-X, the KX installer, offline bundles, and license
files are not bundled or redistributed with this Apache-2.0 connector package.
Users must review the [KDB-X Python license terms][kx-license], accept the
applicable KX terms, and provide their own licensed runtime.

[kx-license]: https://code.kx.com/pykx/4.0/license.html

## CI and live validation

Offline CI follows the repository's HL7 v2 Volume precedent: tests create a
temporary HDB-shaped directory and replace only the PyKX schema, enumeration,
and column-read boundaries with deterministic corpus data. Filesystem
discovery, offsets, partition planning, schema conversion, and row contracts
run through production code.

Live validation must additionally run against a licensed HDB and cover at
least two tables, multiple dates, multiple symbols, restart/resume behavior,
and a large symbol partition.
