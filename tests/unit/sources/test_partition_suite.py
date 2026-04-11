"""Test mixins for SupportsPartition and SupportsPartitionedStream connectors.

Usage — add the appropriate mixin alongside ``LakeflowConnectTests``::

    from tests.unit.sources.test_suite import LakeflowConnectTests
    from tests.unit.sources.test_suite_partition import SupportsPartitionedStreamTests

    class TestMyConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
        connector_class = MyLakeflowConnect

``SupportsPartitionedStreamTests`` includes all ``SupportsPartitionTests``.
"""

import json
import traceback
from typing import List, Optional

import pytest
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface.supports_partition import (
    SupportsPartition,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.libs.utils import parse_value


class SupportsPartitionTests:
    """Test mixin for connectors implementing SupportsPartition.

    Requires the host class to provide ``connector``, ``_tables()``,
    ``_partitioned_tables()``, ``_opts()``, and ``sample_records``
    (all supplied by ``LakeflowConnectTests``).

    """

    sample_records: int = 50

    # ------------------------------------------------------------------
    # test_get_partitions
    # ------------------------------------------------------------------

    def test_get_partitions(self):
        """get_partitions returns a non-empty sequence of dicts for partitioned tables."""
        tables = self._partitioned_tables()
        if not tables:
            pytest.skip("No partitioned tables")

        assert isinstance(self.connector, SupportsPartition), (
            f"{type(self.connector).__name__} does not implement SupportsPartition"
        )

        errors = []
        for table in tables:
            try:
                partitions = self.connector.get_partitions(table, self._opts(table))
                if not hasattr(partitions, "__len__"):
                    errors.append(
                        f"[{table}] get_partitions returned {type(partitions).__name__}, "
                        f"expected Sequence.\n"
                        "  Fix: get_partitions() must return a list or tuple of dicts."
                    )
                    continue
                if len(partitions) == 0:
                    errors.append(
                        f"[{table}] get_partitions returned empty sequence.\n"
                        "  Fix: Expected at least one partition for batch reads."
                    )
                    continue
                for i, p in enumerate(partitions):
                    if not isinstance(p, dict):
                        errors.append(
                            f"[{table}] Partition {i} is {type(p).__name__}, expected dict."
                        )
                        break
            except Exception as e:
                errors.append(f"[{table}] get_partitions raised: {e}\n{traceback.format_exc()}")
        if errors:
            pytest.fail("\n\n".join(errors))

    # ------------------------------------------------------------------
    # test_partition_descriptors_serializable
    # ------------------------------------------------------------------

    def test_partition_descriptors_serializable(self):
        """All partition descriptors are JSON-serializable."""
        tables = self._partitioned_tables()
        if not tables:
            pytest.skip("No partitioned tables")

        errors = []
        for table in tables:
            try:
                partitions = self.connector.get_partitions(table, self._opts(table))
                for i, p in enumerate(partitions):
                    try:
                        json.dumps(p)
                    except (TypeError, ValueError) as e:
                        errors.append(
                            f"[{table}] Partition {i} not JSON-serializable: {e}\n"
                            "  Fix: Partition dicts must contain only primitive types."
                        )
                        break
            except Exception as e:
                errors.append(f"[{table}] get_partitions raised: {e}")
        if errors:
            pytest.fail("\n\n".join(errors))

    # ------------------------------------------------------------------
    # test_read_partition
    # ------------------------------------------------------------------

    def test_read_partition(self):
        """read_partition returns valid records for each partition."""
        tables = self._partitioned_tables()
        if not tables:
            pytest.skip("No partitioned tables")

        errors = []
        for table in tables:
            err = self._validate_partitioned_read(table)
            if err:
                errors.append(err)
        if errors:
            pytest.fail("\n\n".join(errors))

    def _validate_partitioned_read(self, table: str) -> Optional[str]:
        """Validate get_partitions + read_partition for one table."""
        try:
            partitions = self.connector.get_partitions(table, self._opts(table))
        except Exception as e:
            return f"[{table}] get_partitions raised: {e}\n{traceback.format_exc()}"

        if not partitions:
            return f"[{table}] get_partitions returned empty — cannot test read_partition."

        try:
            schema = self.connector.get_table_schema(table, self._opts(table))
        except Exception as e:
            return f"[{table}] get_table_schema raised: {e}"

        total_records = 0
        budget = self.sample_records
        for i, partition in enumerate(partitions):
            if budget <= 0:
                break
            try:
                iterator = self.connector.read_partition(
                    table, partition, self._opts(table)
                )
                if not hasattr(iterator, "__iter__"):
                    return (
                        f"[{table}] read_partition (partition {i}) returned "
                        f"{type(iterator).__name__}, expected iterator.\n"
                        "  Fix: read_partition() must return an Iterator[dict]."
                    )
                records = []
                for rec in iterator:
                    if not isinstance(rec, dict):
                        return (
                            f"[{table}] Record from partition {i} is "
                            f"{type(rec).__name__}, expected dict.\n"
                            "  Fix: read_partition() must yield dicts."
                        )
                    records.append(rec)
                    if len(records) >= budget:
                        break

                err = self._validate_partition_records(table, i, records, schema)
                if err:
                    return err
                total_records += len(records)
                budget -= len(records)

            except Exception as e:
                return (
                    f"[{table}] read_partition (partition {i}) raised: {e}\n"
                    f"{traceback.format_exc()}"
                )

        if total_records == 0:
            return (
                f"[{table}] All partitions produced zero records.\n"
                "  Fix: At least one partition should contain data."
            )

        return None

    def _validate_partition_records(
        self, table: str, partition_idx: int, records: list, schema: StructType
    ) -> Optional[str]:
        """Validate records from a single partition against the schema."""
        for j, rec in enumerate(records):
            try:
                parse_value(rec, schema)
            except Exception as e:
                return (
                    f"[{table}] Partition {partition_idx}, record {j} failed "
                    f"schema parsing: {e}\n"
                    "  Fix: Ensure read_partition() yields records compatible "
                    "with get_table_schema()."
                )
        return None


class SupportsPartitionedStreamTests(SupportsPartitionTests):
    """Test mixin for connectors implementing SupportsPartitionedStream.

    Includes all ``SupportsPartitionTests`` plus streaming-specific tests
    for ``latest_offset``, ``is_partitioned``, and offset-aware
    ``get_partitions``.
    """

    # ------------------------------------------------------------------
    # test_is_partitioned
    # ------------------------------------------------------------------

    def test_is_partitioned(self):
        """is_partitioned returns a bool for every table."""
        assert isinstance(self.connector, SupportsPartitionedStream), (
            f"{type(self.connector).__name__} does not implement SupportsPartitionedStream"
        )

        errors = []
        for table in self._tables():
            result = self.connector.is_partitioned(table)
            if not isinstance(result, bool):
                errors.append(
                    f"[{table}] is_partitioned returned {type(result).__name__}, expected bool."
                )
        if errors:
            pytest.fail("\n\n".join(errors))

    # ------------------------------------------------------------------
    # test_latest_offset
    # ------------------------------------------------------------------

    def test_latest_offset(self):
        """latest_offset returns a JSON-serializable dict for partitioned tables."""
        tables = self._partitioned_tables()
        if not tables:
            pytest.skip("No partitioned tables")

        errors = []
        for table in tables:
            try:
                offset = self.connector.latest_offset(table, self._opts(table))
                if not isinstance(offset, dict):
                    errors.append(
                        f"[{table}] latest_offset returned {type(offset).__name__}, "
                        f"expected dict.\n"
                        "  Fix: latest_offset() must return a dict."
                    )
                    continue
                try:
                    json.dumps(offset)
                except (TypeError, ValueError) as e:
                    errors.append(
                        f"[{table}] Offset not JSON-serializable: {e}\n"
                        "  Fix: Use only primitive types in the offset dict."
                    )
                    continue
                if not offset:
                    errors.append(
                        f"[{table}] latest_offset returned empty dict.\n"
                        "  Fix: Offset must contain at least one key."
                    )
            except Exception as e:
                errors.append(f"[{table}] latest_offset raised: {e}\n{traceback.format_exc()}")
        if errors:
            pytest.fail("\n\n".join(errors))

    # ------------------------------------------------------------------
    # test_latest_offset_with_start
    # ------------------------------------------------------------------

    def test_latest_offset_with_start(self):
        """latest_offset accepts a start_offset and returns a valid offset."""
        tables = self._partitioned_tables()
        if not tables:
            pytest.skip("No partitioned tables")

        errors = []
        for table in tables:
            try:
                # Get initial offset
                offset1 = self.connector.latest_offset(table, self._opts(table))
                if not isinstance(offset1, dict):
                    continue
                # Pass it back as start_offset
                offset2 = self.connector.latest_offset(
                    table, self._opts(table), start_offset=offset1
                )
                if not isinstance(offset2, dict):
                    errors.append(
                        f"[{table}] latest_offset(start_offset=...) returned "
                        f"{type(offset2).__name__}, expected dict."
                    )
            except Exception as e:
                errors.append(
                    f"[{table}] latest_offset with start_offset raised: {e}\n"
                    f"{traceback.format_exc()}"
                )
        if errors:
            pytest.fail("\n\n".join(errors))

    # ------------------------------------------------------------------
    # test_microbatch_offset_convergence
    # ------------------------------------------------------------------

    max_microbatch_iterations: int = 20

    def test_microbatch_offset_convergence(self):
        """Repeatedly feeding latest_offset output as start_offset converges.

        Simulates the Spark micro-batch loop: each iteration calls
        latest_offset(start_offset=previous), then get_partitions with the
        resulting range. The offsets must eventually stabilise (returned
        offset == start_offset), meaning there is no more data to process.
        """
        tables = self._partitioned_tables()
        if not tables:
            pytest.skip("No partitioned tables")

        errors = []
        for table in tables:
            err = self._validate_offset_convergence(table)
            if err:
                errors.append(err)
        if errors:
            pytest.fail("\n\n".join(errors))

    def _validate_offset_convergence(self, table: str) -> Optional[str]:
        """Run micro-batch iterations until offset converges or limit hit."""
        opts = self._opts(table)
        try:
            end_offset = self.connector.latest_offset(table, opts)
        except Exception as e:
            return f"[{table}] latest_offset raised: {e}"

        if not isinstance(end_offset, dict):
            return (
                f"[{table}] latest_offset returned {type(end_offset).__name__}, "
                f"expected dict."
            )

        start_offset = None
        for iteration in range(self.max_microbatch_iterations):
            # Simulate Spark calling get_partitions for this micro-batch
            try:
                partitions = self.connector.get_partitions(
                    table, opts,
                    start_offset=start_offset, end_offset=end_offset,
                )
            except Exception as e:
                return (
                    f"[{table}] get_partitions raised on iteration {iteration}: {e}"
                )

            # Advance: previous end becomes next start
            start_offset = end_offset

            # Ask for next end offset
            try:
                end_offset = self.connector.latest_offset(
                    table, opts, start_offset=start_offset
                )
            except Exception as e:
                return (
                    f"[{table}] latest_offset raised on iteration {iteration}: {e}"
                )

            if not isinstance(end_offset, dict):
                return (
                    f"[{table}] latest_offset returned "
                    f"{type(end_offset).__name__} on iteration {iteration}."
                )

            # Converged: no new data
            if end_offset == start_offset:
                return None

        return (
            f"[{table}] Offset did not converge after "
            f"{self.max_microbatch_iterations} iterations.\n"
            f"  Last start_offset: {start_offset}\n"
            f"  Last end_offset:   {end_offset}\n"
            "  Fix: latest_offset(start_offset=X) must eventually return X "
            "when there is no new data beyond X."
        )

    # ------------------------------------------------------------------
    # test_get_partitions_with_offsets
    # ------------------------------------------------------------------

    def test_get_partitions_with_offsets(self):
        """get_partitions works with start/end offsets from latest_offset."""
        tables = self._partitioned_tables()
        if not tables:
            pytest.skip("No partitioned tables")

        errors = []
        for table in tables:
            try:
                end_offset = self.connector.latest_offset(table, self._opts(table))
                if not isinstance(end_offset, dict):
                    continue

                # With start_offset=None (first micro-batch)
                partitions = self.connector.get_partitions(
                    table, self._opts(table),
                    start_offset=None, end_offset=end_offset,
                )
                if not hasattr(partitions, "__len__"):
                    errors.append(
                        f"[{table}] get_partitions(offsets) returned "
                        f"{type(partitions).__name__}, expected Sequence."
                    )
                    continue
                for i, p in enumerate(partitions):
                    if not isinstance(p, dict):
                        errors.append(
                            f"[{table}] Partition {i} is {type(p).__name__}, expected dict."
                        )
                        break
            except Exception as e:
                errors.append(
                    f"[{table}] get_partitions with offsets raised: {e}\n"
                    f"{traceback.format_exc()}"
                )
        if errors:
            pytest.fail("\n\n".join(errors))

    # ------------------------------------------------------------------
    # test_equal_offsets_empty_partitions
    # ------------------------------------------------------------------

    def test_equal_offsets_empty_partitions(self):
        """get_partitions returns empty when start_offset == end_offset."""
        tables = self._partitioned_tables()
        if not tables:
            pytest.skip("No partitioned tables")

        errors = []
        for table in tables:
            try:
                offset = self.connector.latest_offset(table, self._opts(table))
                if not isinstance(offset, dict):
                    continue
                partitions = self.connector.get_partitions(
                    table, self._opts(table),
                    start_offset=offset, end_offset=offset,
                )
                if len(partitions) != 0:
                    errors.append(
                        f"[{table}] Expected 0 partitions when start==end offset, "
                        f"got {len(partitions)}.\n"
                        "  Fix: get_partitions() should return empty when "
                        "start_offset == end_offset."
                    )
            except Exception as e:
                errors.append(
                    f"[{table}] get_partitions(equal offsets) raised: {e}\n"
                    f"{traceback.format_exc()}"
                )
        if errors:
            pytest.fail("\n\n".join(errors))

    # ------------------------------------------------------------------
    # test_partitioned_stream_round_trip
    # ------------------------------------------------------------------

    def test_partitioned_stream_round_trip(self):
        """Full round-trip: latest_offset -> get_partitions -> read_partition."""
        tables = self._partitioned_tables()
        if not tables:
            pytest.skip("No partitioned tables")

        errors = []
        for table in tables:
            err = self._validate_stream_round_trip(table)
            if err:
                errors.append(err)
        if errors:
            pytest.fail("\n\n".join(errors))

    def _validate_stream_round_trip(self, table: str) -> Optional[str]:
        """Validate the full streaming round-trip for one table."""
        try:
            end_offset = self.connector.latest_offset(table, self._opts(table))
        except Exception as e:
            return f"[{table}] latest_offset raised: {e}"

        if not isinstance(end_offset, dict):
            return f"[{table}] latest_offset returned {type(end_offset).__name__}, expected dict."

        try:
            partitions = self.connector.get_partitions(
                table, self._opts(table),
                start_offset=None, end_offset=end_offset,
            )
        except Exception as e:
            return f"[{table}] get_partitions(offsets) raised: {e}"

        if not partitions:
            return None  # No data in range — acceptable

        try:
            schema = self.connector.get_table_schema(table, self._opts(table))
        except Exception as e:
            return f"[{table}] get_table_schema raised: {e}"

        total_records = 0
        budget = self.sample_records
        for i, partition in enumerate(partitions):
            if budget <= 0:
                break
            try:
                iterator = self.connector.read_partition(
                    table, partition, self._opts(table)
                )
                records: List[dict] = []
                for rec in iterator:
                    if not isinstance(rec, dict):
                        return (
                            f"[{table}] Stream round-trip: partition {i} yielded "
                            f"{type(rec).__name__}, expected dict."
                        )
                    records.append(rec)
                    if len(records) >= budget:
                        break

                err = self._validate_partition_records(table, i, records, schema)
                if err:
                    return err
                total_records += len(records)
                budget -= len(records)
            except Exception as e:
                return (
                    f"[{table}] Stream round-trip: read_partition({i}) raised: {e}\n"
                    f"{traceback.format_exc()}"
                )

        return None
