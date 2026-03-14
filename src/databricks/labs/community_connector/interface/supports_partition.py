from abc import ABC, abstractmethod
from typing import Iterator, Sequence

from databricks.labs.community_connector.interface.lakeflow_connect import (
    LakeflowConnect,
)


class SupportsPartition(ABC):
    """Mixin for connectors that support partitioned reads across Spark executors.

    Must be used together with LakeflowConnect. Implement this when your
    connector can split work into partitions that Spark distributes to workers,
    instead of reading all data in one partition batch.

    Usage::

        class MyConnector(LakeflowConnect, SupportsPartition):
            ...
    """

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Skip abstract intermediate classes (e.g. SupportsPartitionedStream).
        # ABCMeta hasn't set __abstractmethods__ yet, so check the class's own dict.
        if any(getattr(v, "__isabstractmethod__", False) for v in vars(cls).values()):
            return
        if not issubclass(cls, LakeflowConnect):
            raise TypeError(
                f"{cls.__name__} uses SupportsPartition but does not extend "
                f"LakeflowConnect. Add LakeflowConnect to the base classes."
            )

    @abstractmethod
    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
    ) -> Sequence[dict]:
        """Return partition descriptors for reading data.

        Each returned dict is passed to :meth:`read_partition` on an executor.

        Args:
            table_name: The name of the table.
            table_options: A dictionary of options for accessing the table.
        Returns:
            A sequence of partition descriptor dicts. Each dict must be
            JSON-serialisable (primitive types only).
        """

    @abstractmethod
    def read_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Read records for a single partition.

        This method runs on Spark executors and must be self-contained.

        Args:
            table_name: The name of the table.
            partition: One of the partition dicts returned by
                :meth:`get_partitions`.
            table_options: A dictionary of options for accessing the table.
        Returns:
            An iterator of records as JSON-compatible dicts.
        """


class SupportsPartitionedStream(SupportsPartition):
    """Mixin for connectors that support partitioned streaming reads.

    Extends :class:`SupportsPartition` with offset awareness for streaming
    micro-batches. A connector implementing this mixin automatically supports
    both partitioned batch reads (via the inherited interface) and partitioned
    streaming reads.

    Usage::

        class MyConnector(LakeflowConnect, SupportsPartitionedStream):
            ...
    """

    def is_partitioned(self, table_name: str) -> bool:
        """Return whether the given table supports partitioned streaming.

        The default returns True. Override to return False for tables that
        should fall back to simpleStreamReader.

        Args:
            table_name: The name of the table.
            table_options: A dictionary of options for accessing the table.
        """
        return True

    @abstractmethod
    def latest_offset(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """Return the most recent offset available for the table.

        Called by Spark on every micro-batch to discover new data.

        Args:
            table_name: The name of the table.
            table_options: A dictionary of options for accessing the table.
        Returns:
            A dict whose keys and values are primitive types (str, int, bool).
        """

    @abstractmethod
    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        """Return partition descriptors for reading data.

        Each returned dict is passed to :meth:`read_partition` on an executor.

        For batch reads, ``start_offset`` and ``end_offset`` are both None —
        return partitions covering the entire table.

        For streaming micro-batches, they delimit the offset range — return
        partitions covering that range, or an empty sequence when
        ``start_offset == end_offset``.

        Args:
            table_name: The name of the table.
            table_options: A dictionary of options for accessing the table.
            start_offset: The start offset (exclusive), or None for batch.
            end_offset: The end offset (inclusive), or None for batch.
        Returns:
            A sequence of partition descriptor dicts. Each dict must be
            JSON-serialisable (primitive types only).
        """
