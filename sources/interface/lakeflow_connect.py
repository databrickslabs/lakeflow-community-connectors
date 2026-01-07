from typing import Iterator, Any
from pyspark.sql.types import *


# This is the class each source connector needs to implement.
# !! DO NOT CHANGE THE CLASS NAME OR CREATE AN SUBCLASS OF THIS CLASS !!
# Please directly implement the methods below.
class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the source connector with parameters needed to connect to the source.
        Args:
            options: A dictionary of parameters like authentication tokens, table names,
                and other configurations.
        """

    def list_tables(self) -> list[str]:
        """
        List names of all the tables supported by the source connector.
        The list could either be a static list or retrieved from the source via API.
        Returns:
            A list of table names.
        """

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

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the records of a table and return an iterator of records and an offset.
        The read starts from the provided start_offset.
        Records returned in the iterator will be one batch of records marked by the
        offset as its end_offset.
        The read_table function could be called multiple times to read the entire table
        in multiple batches and it stops when the same offset is returned again.
        If the table cannot be incrementally read, the offset can be None if we want to
        read the entire table in one batch.
        We could still return some fake offsets (cannot checkpointing) to split the
        table into multiple batches.
        Args:
            table_name: The name of the table to read.
            start_offset: The offset to start reading from.
            table_options: A dictionary of options for accessing the table. For example,
                the source API may require extra parameters needed to read the table.
                If there are no additional options required, you can ignore this
                parameter, and no options will be provided during execution.
                Only add parameters to table_options if they are essential for accessing
                or retrieving the data (such as specifying table namespaces).
        Returns:
            An iterator of records in JSON format and an offset.
            DO NOT convert the JSON based on the schema in `get_table_schema` in
            `read_table`.
            records: An iterator of records in JSON format.
            offset: An offset in dict.
        """

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read deleted records from a table for CDC delete synchronization.
        This method is called when ingestion_type is "cdc_with_deletes" to fetch
        records that have been deleted from the source system.

        The returned records should have at minimum the primary key fields and
        cursor field populated. Other fields can be null.

        Args:
            table_name: The name of the table to read deleted records from.
            start_offset: The offset to start reading from (same format as read_table).
            table_options: A dictionary of options for accessing the table.
        Returns:
            An iterator of deleted records in JSON format and an offset.
            records: An iterator of deleted records (must include primary keys and cursor).
            offset: An offset in dict (same format as read_table).
        """
