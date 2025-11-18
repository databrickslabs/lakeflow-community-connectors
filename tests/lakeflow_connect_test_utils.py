from typing import Dict, Tuple, List


class LakeflowConnectTestUtils:
    """
    Base class for connector-specific test utilities.
    Each connector should extend this class to provide connector-specific implementations
    for testing operations.
    
    Provides default implementations for WriteToSource functionality that return
    empty/default values. Connectors can override these methods to provide actual
    write functionality.
    """

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize test utilities with connection options.

        Args:
            options: A dictionary of connection parameters (tokens, credentials, etc.)
                    Same options format as used by LakeflowConnect
        """
        self.options = options

    def get_source_name(self) -> str:
        """
        Return the source connector name.
        
        Returns:
            String name of the connector (default: "unknown")
        """
        return "unknown"

    def list_insertable_tables(self) -> List[str]:
        """
        List all tables that support insert/write-back functionality.
        
        Default implementation returns an empty list, indicating no tables
        support write functionality. Override this method in connector-specific
        test utils to provide actual insertable tables.
        
        Returns:
            List of table names that support inserting new data (default: empty list)
        """
        return []

    def generate_rows_and_write(
        self, table_name: str, number_of_rows: int
    ) -> Tuple[bool, List[Dict], Dict[str, str]]:
        """
        Generate specified number of rows and write them to the given table.
        
        Default implementation returns failure with empty data. Override this
        method in connector-specific test utils to provide actual write functionality.
        
        DISCLAIMER: Currently the function only supports testing of inserts of rows,
        not deletes or updates.

        Args:
            table_name: Name of the table to write to
            number_of_rows: Number of rows to generate and write

        Returns:
            Tuple containing:
            - Boolean indicating success of the operation (default: False)
            - List of rows as dictionaries (default: empty list)
            - Dictionary mapping written column names to returned column names (default: empty dict)
        """
        return False, [], {}
