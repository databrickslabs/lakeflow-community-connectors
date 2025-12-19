from abc import ABC
from typing import List, Dict, Any, Iterator, Tuple
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
import hashlib
import json


METADATA_TABLE = "_lakeflow_metadata"


class Enrichment(ABC):
    """
    Base class for all enrichments.
    
    Subclasses can override any of the methods to apply transformations
    to schemas, records, or metadata.
    """

    def __init__(self):
        self.connect = None  # Set by LakeflowConnectEnricher.add()

    def should_apply(self, table_name: str, metadata: dict) -> bool:
        """
        Determine if this enrichment should be applied to the given table.
        
        Args:
            table_name: Name of the table being processed
            metadata: Table metadata from read_table_metadata()
            
        Returns:
            True if enrichment should be applied, False otherwise
        """
        return True

    def enrich_schema(
        self, schema: StructType, table_name: str, metadata: dict
    ) -> StructType:
        """
        Enrich the table schema.
        
        Args:
            schema: Original schema from get_table_schema()
            table_name: Name of the table
            metadata: Table metadata
            
        Returns:
            Enriched schema
        """
        return schema

    def enrich_records(
        self, records: List[Dict], table_name: str, metadata: dict
    ) -> List[Dict]:
        """
        Enrich the records.
        
        Args:
            records: List of records from read_table()
            table_name: Name of the table
            metadata: Table metadata
            
        Returns:
            Enriched records
        """
        return records

    def enrich_metadata(self, metadata: dict, table_name: str) -> dict:
        """
        Enrich the table metadata.
        
        Args:
            metadata: Original metadata from read_table_metadata()
            table_name: Name of the table
            
        Returns:
            Enriched metadata
        """
        return metadata


class HashedPrimaryKeyEnrichment(Enrichment):
    """
    Adds a hashed primary key (_pk) when table has no primary key defined.
    
    The hash is computed from the JSON serialization of the entire record,
    providing a deterministic unique identifier.
    """

    PK_FIELD_NAME = "_pk"

    def should_apply(self, table_name: str, metadata: dict) -> bool:
        """Only apply if table is not metadata table and has no primary key."""
        if table_name == METADATA_TABLE:
            return False
        
        pk = metadata.get("primary_keys")
        # Apply if primary_keys is None, empty string, or empty list
        if pk is None:
            return True
        if isinstance(pk, str) and pk == "":
            return True
        if isinstance(pk, list) and len(pk) == 0:
            return True
        return False

    def enrich_schema(
        self, schema: StructType, table_name: str, metadata: dict
    ) -> StructType:
        """Add _pk field at the beginning of the schema."""
        pk_field = StructField(self.PK_FIELD_NAME, StringType(), False)
        return StructType([pk_field] + list[StructField](schema.fields))

    def enrich_records(
        self, records: List[Dict], table_name: str, metadata: dict
    ) -> List[Dict]:
        """Add _pk field to each record."""
        return [self._add_pk(record) for record in records]

    def enrich_metadata(self, metadata: dict, table_name: str) -> dict:
        """Set primary_keys to _pk in metadata."""
        return {**metadata, "primary_keys": self.PK_FIELD_NAME}

    def _add_pk(self, record: Dict) -> Dict:
        """Add hashed primary key to a record."""
        pk_value = self._compute_hash(record)
        return {self.PK_FIELD_NAME: pk_value, **record}

    def _compute_hash(self, record: Dict) -> str:
        """
        Compute SHA-256 hash of the record.
        
        Uses JSON serialization with sorted keys for deterministic output.
        """
        # Sort keys for deterministic serialization
        record_str = json.dumps(record, sort_keys=True, default=str)
        return hashlib.sha256(record_str.encode()).hexdigest()


class LakeflowConnectEnricher:
    """
    Wraps a LakeflowConnect instance and applies enrichments.
    
    This class delegates all calls to the underlying connect instance,
    applying registered enrichments to the results.
    
    Usage:
        raw_connect = LakeflowConnect(options)
        enricher = LakeflowConnectEnricher(raw_connect)
        enricher.add(HashedPrimaryKeyEnrichment())
        
        # Use enricher in place of raw_connect
        schema = enricher.get_table_schema(table_name, options)
        records, offset = enricher.read_table(table_name, offset, options)
    """

    def __init__(self, connect):
        """
        Initialize the enricher with an underlying LakeflowConnect instance.
        
        Args:
            connect: A LakeflowConnect instance to wrap
        """
        self.connect = connect
        self.enrichments: List[Enrichment] = []

    def add(self, enrichment: Enrichment) -> "LakeflowConnectEnricher":
        """
        Add an enrichment to the pipeline.
        
        Args:
            enrichment: An Enrichment instance to add
            
        Returns:
            self for method chaining
        """
        enrichment.connect = self.connect  # Give enrichment access to the source
        self.enrichments.append(enrichment)
        return self

    def list_tables(self) -> List[str]:
        """Delegate to underlying connect."""
        return self.connect.list_tables()

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Get table schema with enrichments applied.
        
        Args:
            table_name: Name of the table
            table_options: Options for the table
            
        Returns:
            Enriched schema
        """
        schema = self.connect.get_table_schema(table_name, table_options)
        metadata = self.connect.read_table_metadata(table_name, table_options)

        for enrichment in self.enrichments:
            if enrichment.should_apply(table_name, metadata):
                schema = enrichment.enrich_schema(schema, table_name, metadata)

        return schema

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Get table metadata with enrichments applied.
        
        Args:
            table_name: Name of the table
            table_options: Options for the table
            
        Returns:
            Enriched metadata
        """
        metadata = self.connect.read_table_metadata(table_name, table_options)

        for enrichment in self.enrichments:
            if enrichment.should_apply(table_name, metadata):
                metadata = enrichment.enrich_metadata(metadata, table_name)

        return metadata

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """
        Read table records with enrichments applied.
        
        Args:
            table_name: Name of the table
            start_offset: Offset to start reading from
            table_options: Options for the table
            
        Returns:
            Tuple of (enriched_records, new_offset)
        """
        records, new_offset = self.connect.read_table(
            table_name, start_offset, table_options
        )
        metadata = self.connect.read_table_metadata(table_name, table_options)

        for enrichment in self.enrichments:
            if enrichment.should_apply(table_name, metadata):
                records = enrichment.enrich_records(records, table_name, metadata)

        return records, new_offset

    def test_connection(self) -> dict:
        """Delegate to underlying connect if available."""
        if hasattr(self.connect, "test_connection"):
            return self.connect.test_connection()
        return {"status": "success", "message": "Connection test not implemented"}

