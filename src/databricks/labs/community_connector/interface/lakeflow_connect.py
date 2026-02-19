from abc import ABC, abstractmethod
from typing import Iterator

from pyspark.sql.types import StructType


class LakeflowConnect(ABC):
    """Base interface that each source connector must implement.

    Subclass this and implement all abstract methods to create a connector that
    integrates with the community connector library and ingestion pipeline.
    """

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the source connector with parameters needed to connect to the source.
        Args:
            options: A dictionary of parameters like authentication tokens, table names,
                and other configurations.
        """
        self.options = options

    @abstractmethod
    def list_tables(self) -> list[str]:
        """
        List names of all the tables supported by the source connector.
        The list could either be a static list or retrieved from the source via API.
        Returns:
            A list of table names.
        """

    @abstractmethod
    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.
        Args:
            table_name: The name of the table to fetch the schema for.
            table_options: A dictionary of options for accessing the table. For example,
                the source API may require extra parameters needed to fetch the schema.
                If there are no additional options required, you can ignore this
                parameter, and no options will be provided during execution.
                Only add parameters to table_options if they are essential for accessing
                or retrieving the data (such as specifying table namespaces).
        Returns:
            A StructType object representing the schema of the table.
        """

    @abstractmethod
    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch the metadata of a table.
        Args:
            table_name: The name of the table to fetch the metadata for.
            table_options: A dictionary of options for accessing the table. For example,
                the source API may require extra parameters needed to fetch the metadata.
                If there are no additional options required, you can ignore this
                parameter, and no options will be provided during execution.
                Only add parameters to table_options if they are essential for accessing
                or retrieving the data (such as specifying table namespaces).
        Returns:
            A dictionary containing the metadata of the table. It should include the
            following keys:
                - primary_keys: List of string names of the primary key columns of
                    the table.
                - cursor_field: The name of the field to use as a cursor for
                    incremental loading.
                - ingestion_type: The type of ingestion to use for the table. It
                    should be one of the following values:
                    - "snapshot": For snapshot loading.
                    - "cdc": Capture incremental changes (no delete support).
                    - "cdc_with_deletes": Capture incremental changes with delete
                        support. Requires implementing read_table_deletes().
                    - "append": Incremental append.
        """

    @abstractmethod
    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """
        Read the records of a table and return an iterator of records and an offset.

        The framework calls this method repeatedly to paginate through data.
        start_offset is None only on the very first call of the very first run
        of a connector. On subsequent runs the framework resumes from the last
        checkpointed offset, so start_offset will already be populated. Each
        call returns (records, end_offset). The framework passes end_offset as
        start_offset to the next call. Pagination stops when the returned
        offset equals start_offset (i.e., no more data).

        For tables that cannot be incrementally read, return None as the offset to
        read the entire table in one batch. Non-checkpointable synthetic offsets can
        be used to split the data into multiple batches.

        Args:
            table_name: The name of the table to read.
            start_offset: The offset to start reading from. None only on the
                first call of the first run; on subsequent runs it carries the
                checkpointed offset from the previous run.
            table_options: A dictionary of options for accessing the table. For example,
                the source API may require extra parameters needed to read the table.
                If there are no additional options required, you can ignore this
                parameter, and no options will be provided during execution.
                Only add parameters to table_options if they are essential for accessing
                or retrieving the data (such as specifying table namespaces).
        Returns:
            A two-element tuple of (records, offset).
            records: An iterator of records as JSON-compatible dicts. Do NOT convert
                values according to get_table_schema(); the framework handles that.
            offset: A dict representing the position after this batch.
        """

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """
        Read deleted records from a table for CDC delete synchronization.
        This method is called when ingestion_type is "cdc_with_deletes" to fetch
        records that have been deleted from the source system.

        This method follows the same pagination and offset protocol as read_table:
        the framework calls it repeatedly, passing the previous end_offset as
        start_offset, until the returned offset equals start_offset.

        Override this method if any of your tables use ingestion_type
        "cdc_with_deletes". The default implementation raises NotImplementedError.

        The returned records should have at minimum the primary key fields and
        cursor field populated. Other fields can be null.

        Args:
            table_name: The name of the table to read deleted records from.
            start_offset: The offset to start reading from (same format as read_table).
            table_options: A dictionary of options for accessing the table.
        Returns:
            A two-element tuple of (records, offset).
            records: An iterator of deleted records (must include primary keys and cursor).
            offset: A dict (same format as read_table).
        """
        raise NotImplementedError(
            "read_table_deletes() must be implemented when ingestion_type is 'cdc_with_deletes'"
        )
