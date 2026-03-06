# pylint: disable=undefined-variable
"""Direct Python Data Source API implementation for DICOMweb.

Follows the "Direct Implementation of Python Data Source API" strategy
described in src/databricks/labs/community_connector/interface/README.md.

This module customises the data-partitioning logic so that WADO-RS file
downloads and per-instance metadata fetches run on Spark **executors** via
``DataSourceStreamReader``, rather than on the driver.

Architecture
------------
The merge script (``merge_python_source.py``) always appends
``lakeflow_datasource.py`` last, which defines ``LakeflowSource(DataSource)``
with a driver-only ``simpleStreamReader``.  We cannot remove or reorder that
section, so we use ``DataSource.__init_subclass__`` to inject a
``streamReader`` method onto ``LakeflowSource`` when it is defined.

PySpark prefers ``streamReader()`` over ``simpleStreamReader()`` when both
are present, so the injected method takes effect automatically.

Serialisation note
------------------
``LakeflowSource`` inherits from the **original** pyspark ``DataSource``
(importable on executors).  The injected ``streamReader`` is a plain closure
— cloudpickle serialises it by value, avoiding any reference to the
``databricks.labs`` module path that is only available on the driver.
"""

from dataclasses import dataclass
from datetime import date
import json

from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition


# ---------------------------------------------------------------------------
# Partition types
# ---------------------------------------------------------------------------


@dataclass
class SimplePartition(InputPartition):
    """Single partition carrying the serialised start-offset."""

    start_json: str


@dataclass
class DicomBatchPartition(InputPartition):
    """Batch of instance records to process on an executor."""

    instances_json: str
    options_json: str


# ---------------------------------------------------------------------------
# DataSourceStreamReader — runs read() on executors
# ---------------------------------------------------------------------------


class DicomStreamReader(DataSourceStreamReader):
    """Executor-distributed streaming reader for DICOMweb.

    * Non-file tables and instances without ``fetch_dicom_files`` /
      ``fetch_metadata``: a single ``SimplePartition`` is returned and
      ``read()`` queries the source normally on one executor.

    * Instances with file/metadata fetch enabled: ``partitions()`` collects
      bare instance records on the driver, then batches them into
      ``DicomBatchPartition`` objects.  Each ``read(DicomBatchPartition)``
      runs on a Spark executor with UC Volume FUSE access and performs the
      WADO-RS downloads there.
    """

    def __init__(self, options, schema, lakeflow_connect):
        self.options = options
        self.schema = schema
        self._lakeflow_connect = lakeflow_connect

    # -- offsets -------------------------------------------------------------

    def initialOffset(self):
        return {}

    def latestOffset(self):
        return {"study_date": date.today().strftime("%Y%m%d"), "page_offset": 0}

    # -- partitioning --------------------------------------------------------

    def partitions(self, start, end):
        is_delete_flow = self.options.get(IS_DELETE_FLOW) == "true"
        table_options = {k: v for k, v in self.options.items() if k != IS_DELETE_FLOW}
        table_name = self.options.get(TABLE_NAME)
        fetch_files = table_options.get("fetch_dicom_files", "false").lower() == "true"
        fetch_metadata = table_options.get("fetch_metadata", "false").lower() == "true"
        volume_path = table_options.get("dicom_volume_path", "")

        if not is_delete_flow and table_name == "instances" and (fetch_files or fetch_metadata):
            if fetch_files and not volume_path:
                raise ValueError("fetch_dicom_files=true requires dicom_volume_path")
            return self._collect_instance_partitions(table_name, table_options, start)
        return [SimplePartition(start_json=json.dumps(start if start else {}))]

    def _collect_instance_partitions(self, table_name, table_options, start):
        """Driver-side: page through bare instance records, batch into partitions."""
        meta_options = dict(table_options)
        meta_options["fetch_dicom_files"] = "false"
        meta_options["fetch_metadata"] = "false"

        all_records = []
        current_start = dict(start) if start else {}
        while True:
            records, next_offset = self._lakeflow_connect.read_table(
                table_name,
                current_start,
                meta_options,
            )
            batch = list(records)
            all_records.extend(batch)
            if not batch or next_offset.get("page_offset", 0) == 0:
                break
            if next_offset == current_start:
                break
            current_start = next_offset

        batch_size = int(self.options.get("dicom_batch_size", "50"))
        options_json = json.dumps(dict(self.options))
        return [
            DicomBatchPartition(
                instances_json=json.dumps(all_records[i : i + batch_size], default=str),
                options_json=options_json,
            )
            for i in range(0, max(1, len(all_records)), batch_size)
        ]

    # -- read (executor-side) ------------------------------------------------

    def read(self, partition):
        if isinstance(partition, DicomBatchPartition):
            return self._read_dicom_batch(partition)
        return self._read_simple(partition)

    def _read_dicom_batch(self, partition):
        """Executor: download DICOM files and/or fetch metadata."""
        instances = json.loads(partition.instances_json)
        options = json.loads(partition.options_json)
        table_options = {k: v for k, v in options.items() if k != IS_DELETE_FLOW}
        volume_path = table_options.get("dicom_volume_path", "")
        wado_mode = table_options.get("wado_mode", "auto")
        fetch_metadata = table_options.get("fetch_metadata", "false").lower() == "true"
        fetch_files = table_options.get("fetch_dicom_files", "false").lower() == "true"
        connector = LakeflowConnectImpl(options)

        if fetch_metadata:
            sop_to_meta: dict = {}
            seen_series: set = set()
            for rec in instances:
                key = (rec.get("study_instance_uid"), rec.get("series_instance_uid"))
                if key not in seen_series and all(key):
                    seen_series.add(key)
                    sop_to_meta.update(connector._build_metadata_map(key[0], key[1]))
            for rec in instances:
                sop_uid = rec.get("sop_instance_uid")
                rec["metadata"] = sop_to_meta.get(sop_uid) if sop_uid else None

        results = []
        for rec in instances:
            if fetch_files:
                rec = connector._attach_dicom_file(rec, volume_path, wado_mode)
            results.append(parse_value(rec, self.schema))
        return iter(results)

    def _read_simple(self, partition):
        """Executor: query source and return records (no file downloads)."""
        start = json.loads(partition.start_json)
        table_name = self.options.get(TABLE_NAME)
        is_delete_flow = self.options.get(IS_DELETE_FLOW) == "true"
        table_options = {k: v for k, v in self.options.items() if k != IS_DELETE_FLOW}
        connector = LakeflowConnectImpl(self.options)
        if is_delete_flow:
            records, _ = connector.read_table_deletes(table_name, start, table_options)
        else:
            records, _ = connector.read_table(table_name, start, table_options)
        return iter(map(lambda x: parse_value(x, self.schema), records))

    def commit(self, end):
        pass


# ---------------------------------------------------------------------------
# __init_subclass__ hook — inject streamReader into LakeflowSource
# ---------------------------------------------------------------------------
# When lakeflow_datasource.py (merged after this section) defines
# ``class LakeflowSource(DataSource)``, Python calls
# ``DataSource.__init_subclass__(LakeflowSource)``.  We use this hook to
# attach ``streamReader`` directly onto ``LakeflowSource.__dict__``.
#
# Because the base class remains the original pyspark DataSource (importable
# on executors) and ``_stream_reader`` is a plain closure, cloudpickle
# serialises everything by value — no ``databricks.labs`` module reference
# ends up in the pickle stream.
# ---------------------------------------------------------------------------


@classmethod
def _dicom_init_subclass(cls, **kwargs):
    def _stream_reader(self, schema):
        return DicomStreamReader(self.options, schema, self.lakeflow_connect)

    cls.streamReader = _stream_reader


DataSource.__init_subclass__ = _dicom_init_subclass
