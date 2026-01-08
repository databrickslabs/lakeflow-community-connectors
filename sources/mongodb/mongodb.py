from typing import Iterator, Dict, List, Any
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    ArrayType,
    BinaryType,
    TimestampType,
)
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from bson import ObjectId, Decimal128, Binary, Timestamp as BSONTimestamp
from datetime import datetime
import time


class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize MongoDB connector with connection parameters.
        
        Args:
            options: Dictionary containing:
                - connection_uri: MongoDB connection string (required)
                  Formats supported:
                    - Standard: mongodb://[username:password@]host[:port][/[database][?options]]
                    - Atlas SRV: mongodb+srv://username:password@cluster.mongodb.net/database
                  Examples:
                    - mongodb://user:pass@localhost:27017/mydb
                    - mongodb+srv://user:pass@cluster0.mongodb.net/mydb
                  Note: SRV URIs require dnspython package to be installed
        """
        connection_uri = options.get("connection_uri")
        if not connection_uri:
            raise ValueError("MongoDB connector requires 'connection_uri' in options")
        
        self.connection_uri = connection_uri
        self.client = None
        self._initialize_client()
        
        # Cache for schemas and metadata
        self._schema_cache = {}
        self._metadata_cache = {}
        
        # Configuration
        self.sample_size = int(options.get("schema_sample_size", "100"))
        self.batch_size = int(options.get("batch_size", "1000"))
        
    def _initialize_client(self):
        """Initialize MongoDB client and test connection."""
        try:
            self.client = MongoClient(
                self.connection_uri,
                serverSelectionTimeoutMS=30000,
                connectTimeoutMS=30000
            )
            # Test connection
            self.client.admin.command('ping')
        except Exception as e:
            raise ConnectionFailure(f"Failed to connect to MongoDB: {str(e)}")
    
    def _is_replica_set(self) -> bool:
        """Check if MongoDB deployment is a replica set or sharded cluster."""
        try:
            topology_type = self.client.topology_description.topology_type_name
            return topology_type in ['ReplicaSetWithPrimary', 'Sharded']
        except Exception:
            return False
    
    def _get_database_name(self, table_options: Dict[str, str]) -> str:
        """
        Extract database name from table_options or connection URI.
        
        Args:
            table_options: Dictionary that should contain 'database' parameter
            
        Returns:
            Database name string
        """
        database = table_options.get("database")
        if not database:
            # Try to extract from connection URI
            if "/" in self.connection_uri:
                parts = self.connection_uri.split("/")
                if len(parts) >= 4:
                    db_with_params = parts[3]
                    database = db_with_params.split("?")[0] if "?" in db_with_params else db_with_params
        
        if not database:
            raise ValueError(
                "Database name is required. Provide 'database' in table_options or in connection URI."
            )
        
        return database
    
    def _parse_collection_name(self, table_name: str) -> tuple[str, str]:
        """
        Parse table_name into database and collection.
        Format: database.collection or just collection (use default db)
        
        Returns:
            Tuple of (database_name, collection_name)
        """
        if "." in table_name:
            parts = table_name.split(".", 1)
            return parts[0], parts[1]
        return "", table_name
    
    def list_tables(self) -> List[str]:
        """
        List all collections across all non-system databases.
        
        Returns:
            List of table names in format: database.collection
        """
        tables = []
        
        try:
            # List all databases (excluding system databases)
            database_names = self.client.list_database_names()
            system_dbs = {"admin", "config", "local"}
            
            for db_name in database_names:
                if db_name in system_dbs:
                    continue
                
                db = self.client[db_name]
                collection_names = db.list_collection_names()
                
                # Filter out system collections
                for coll_name in collection_names:
                    if not coll_name.startswith("system."):
                        tables.append(f"{db_name}.{coll_name}")
        
        except Exception as e:
            raise OperationFailure(f"Failed to list MongoDB tables: {str(e)}")
        
        return tables
    
    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a MongoDB collection by sampling documents.
        
        Args:
            table_name: Name of the collection (or database.collection)
            table_options: Options including 'database' (required if not in table_name)
            
        Returns:
            StructType representing the inferred schema
        """
        # Validate table exists
        supported_tables = self.list_tables()
        
        # Parse database and collection
        db_from_name, collection_name = self._parse_collection_name(table_name)
        database_name = db_from_name if db_from_name else self._get_database_name(table_options)
        
        full_table_name = f"{database_name}.{collection_name}"
        if full_table_name not in supported_tables:
            raise ValueError(
                f"Table '{table_name}' is not supported. Available tables: {supported_tables}"
            )
        
        # Check cache
        cache_key = full_table_name
        if cache_key in self._schema_cache:
            return self._schema_cache[cache_key]
        
        # Infer schema from sample
        schema = self._infer_schema(database_name, collection_name)
        
        # Cache result
        self._schema_cache[cache_key] = schema
        
        return schema
    
    def _infer_schema(self, database_name: str, collection_name: str) -> StructType:
        """
        Infer schema by sampling documents from the collection.
        
        Args:
            database_name: Database name
            collection_name: Collection name
            
        Returns:
            StructType with inferred fields
        """
        db = self.client[database_name]
        collection = db[collection_name]
        
        # Sample documents
        sample_docs = list(collection.find().limit(self.sample_size))
        
        if not sample_docs:
            # Empty collection - return minimal schema with just _id
            return StructType([
                StructField("_id", StringType(), False)
            ])
        
        # Collect all field names and types
        field_types = {}
        
        for doc in sample_docs:
            self._collect_field_types(doc, field_types)
        
        # Build schema fields
        schema_fields = []
        
        # Always include _id first as primary key (non-nullable)
        if "_id" in field_types:
            schema_fields.append(StructField("_id", StringType(), False))
            del field_types["_id"]
        
        # Add other fields (all nullable since MongoDB is schema-less)
        for field_name in sorted(field_types.keys()):
            types = field_types[field_name]
            spark_type = self._resolve_spark_type(types)
            schema_fields.append(StructField(field_name, spark_type, True))
        
        return StructType(schema_fields)
    
    def _collect_field_types(self, obj: Any, field_types: Dict[str, set], prefix: str = ""):
        """
        Recursively collect field types from a document.
        
        Args:
            obj: Object to analyze (dict, list, or primitive)
            field_types: Dictionary to accumulate field name -> set of types
            prefix: Current field path prefix
        """
        if isinstance(obj, dict):
            for key, value in obj.items():
                field_path = f"{prefix}.{key}" if prefix else key
                value_type = self._get_bson_type(value)
                
                if field_path not in field_types:
                    field_types[field_path] = set()
                field_types[field_path].add(value_type)
                
                # For nested objects and arrays, we store the type but don't recurse deeply
                # to avoid overly complex schemas
        elif isinstance(obj, list):
            # For arrays, track the element types
            for item in obj:
                item_type = self._get_bson_type(item)
                if prefix not in field_types:
                    field_types[prefix] = set()
                field_types[prefix].add(f"array<{item_type}>")
    
    def _get_bson_type(self, value: Any) -> str:
        """
        Determine BSON type of a value.
        
        Returns:
            String representation of the type
        """
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "long"
        elif isinstance(value, float):
            return "double"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, ObjectId):
            return "objectid"
        elif isinstance(value, datetime):
            return "datetime"
        elif isinstance(value, Decimal128):
            return "decimal"
        elif isinstance(value, Binary):
            return "binary"
        elif isinstance(value, BSONTimestamp):
            return "timestamp"
        elif isinstance(value, dict):
            return "object"
        elif isinstance(value, list):
            return "array"
        else:
            return "string"  # Default fallback
    
    def _resolve_spark_type(self, types: set) -> Any:
        """
        Resolve a Spark DataType from a set of observed BSON types.
        
        Args:
            types: Set of BSON type strings
            
        Returns:
            Spark DataType
        """
        # Remove null from consideration
        non_null_types = {t for t in types if t != "null"}
        
        if not non_null_types:
            return StringType()
        
        # If multiple types, default to StringType for flexibility
        if len(non_null_types) > 1:
            # Check if all are numeric types
            numeric_types = {"long", "double", "decimal"}
            if non_null_types.issubset(numeric_types):
                if "double" in non_null_types or "decimal" in non_null_types:
                    return DoubleType()
                return LongType()
            return StringType()
        
        # Single type
        single_type = next(iter(non_null_types))
        
        # Handle array types
        if single_type.startswith("array"):
            return ArrayType(StringType(), True)
        
        # Map to Spark type
        type_mapping = {
            "string": StringType(),
            "objectid": StringType(),
            "long": LongType(),
            "double": DoubleType(),
            "decimal": DoubleType(),
            "boolean": BooleanType(),
            "datetime": StringType(),  # Store as ISO string
            "timestamp": LongType(),
            "binary": BinaryType(),
            "object": StringType(),  # Store as JSON string
            "array": ArrayType(StringType(), True),
        }
        
        return type_mapping.get(single_type, StringType())
    
    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Fetch metadata for a MongoDB collection.
        
        Args:
            table_name: Name of the collection
            table_options: Options including 'database'
            
        Returns:
            Dictionary with primary_keys, cursor_field, and ingestion_type
        """
        # Validate table exists
        supported_tables = self.list_tables()
        
        # Parse database and collection
        db_from_name, collection_name = self._parse_collection_name(table_name)
        database_name = db_from_name if db_from_name else self._get_database_name(table_options)
        
        full_table_name = f"{database_name}.{collection_name}"
        if full_table_name not in supported_tables:
            raise ValueError(
                f"Table '{table_name}' is not supported. Available tables: {supported_tables}"
            )
        
        # Check cache
        cache_key = full_table_name
        if cache_key in self._metadata_cache:
            return self._metadata_cache[cache_key]
        
        # Determine ingestion type based on deployment topology
        is_replica_set = self._is_replica_set()
        
        # MongoDB collections always have _id as primary key
        metadata = {
            "primary_keys": ["_id"],
            "cursor_field": "_id",  # Use _id for cursor-based pagination
        }
        
        # Set ingestion type based on capabilities
        if is_replica_set:
            # Change Streams available - support CDC with deletes
            metadata["ingestion_type"] = "cdc_with_deletes"
        else:
            # Standalone deployment - use snapshot or append
            # Default to append (incremental using _id cursor)
            metadata["ingestion_type"] = "append"
        
        # Cache result
        self._metadata_cache[cache_key] = metadata
        
        return metadata
    
    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """
        Read records from a MongoDB collection.
        
        Args:
            table_name: Name of the collection
            start_offset: Dictionary containing cursor information
            table_options: Options including 'database'
            
        Returns:
            Tuple of (record iterator, next_offset)
        """
        # Validate table exists
        supported_tables = self.list_tables()
        
        # Parse database and collection
        db_from_name, collection_name = self._parse_collection_name(table_name)
        database_name = db_from_name if db_from_name else self._get_database_name(table_options)
        
        full_table_name = f"{database_name}.{collection_name}"
        if full_table_name not in supported_tables:
            raise ValueError(
                f"Table '{table_name}' is not supported. Available tables: {supported_tables}"
            )
        
        # Get metadata to determine ingestion type
        metadata = self.read_table_metadata(table_name, table_options)
        ingestion_type = metadata.get("ingestion_type")
        
        # Route to appropriate read method
        if ingestion_type == "cdc_with_deletes":
            return self._read_change_stream(database_name, collection_name, start_offset)
        else:
            return self._read_incremental(database_name, collection_name, start_offset)
    
    def _read_incremental(
        self, database_name: str, collection_name: str, start_offset: dict
    ) -> tuple[List[dict], dict]:
        """
        Read documents incrementally using _id cursor.
        
        Args:
            database_name: Database name
            collection_name: Collection name
            start_offset: Dictionary with 'cursor' key containing last _id
            
        Returns:
            Tuple of (records list, next_offset)
        """
        db = self.client[database_name]
        collection = db[collection_name]
        
        # Build query filter based on cursor
        query_filter = {}
        cursor = start_offset.get("cursor") if start_offset else None
        
        if cursor:
            # Resume from last cursor
            try:
                cursor_id = ObjectId(cursor) if isinstance(cursor, str) else cursor
                query_filter = {"_id": {"$gt": cursor_id}}
            except Exception:
                # If cursor is not a valid ObjectId, treat as string
                query_filter = {"_id": {"$gt": cursor}}
        
        # Fetch batch of documents
        cursor_obj = collection.find(query_filter).sort("_id", 1).limit(self.batch_size)
        
        records = []
        last_id = cursor
        
        for doc in cursor_obj:
            # Convert document to JSON-serializable format
            record = self._convert_document(doc)
            records.append(record)
            last_id = record["_id"]
        
        # Build next offset
        next_offset = {"cursor": last_id} if last_id else {}
        
        return records, next_offset
    
    def _read_change_stream(
        self, database_name: str, collection_name: str, start_offset: dict
    ) -> tuple[List[dict], dict]:
        """
        Read documents using Change Streams (CDC) for replica sets.
        
        Args:
            database_name: Database name
            collection_name: Collection name
            start_offset: Dictionary with 'resume_token' or 'start_time'
            
        Returns:
            Tuple of (records list, next_offset)
        """
        db = self.client[database_name]
        collection = db[collection_name]
        
        records = []
        resume_token = start_offset.get("resume_token") if start_offset else None
        
        # For initial sync, perform a full scan first
        if not resume_token:
            # Initial full scan
            cursor_obj = collection.find().sort("_id", 1).limit(self.batch_size)
            
            for doc in cursor_obj:
                record = self._convert_document(doc)
                records.append(record)
            
            # After initial scan, set up for change stream on next call
            # Use current cluster time as starting point
            next_offset = {"resume_token": None, "initial_sync": True}
            return records, next_offset
        
        # For subsequent reads, use Change Streams
        # Note: In production, this would watch for a limited time or count
        # For this implementation, we return empty to indicate CDC setup is ready
        # Real CDC implementation would require a long-running process
        
        # Build watch options
        watch_options = {"full_document": "updateLookup"}
        if resume_token and resume_token != "None":
            watch_options["resume_after"] = resume_token
        
        try:
            # Watch for changes (with timeout to avoid blocking indefinitely)
            with collection.watch(**watch_options) as stream:
                # Process a limited number of changes in this batch
                change_count = 0
                max_changes = self.batch_size
                last_token = resume_token
                
                for change in stream:
                    if change_count >= max_changes:
                        break
                    
                    # Process change event
                    record = self._process_change_event(change)
                    if record:
                        records.append(record)
                    
                    last_token = stream.resume_token
                    change_count += 1
                
                next_offset = {"resume_token": last_token}
        
        except Exception as e:
            # If change stream fails, fall back to incremental read
            return self._read_incremental(database_name, collection_name, start_offset)
        
        return records, next_offset
    
    def _process_change_event(self, change: dict) -> dict:
        """
        Process a change stream event into a record.
        
        Args:
            change: Change event from MongoDB change stream
            
        Returns:
            Processed record dictionary or None for deletes
        """
        operation_type = change.get("operationType")
        
        if operation_type in ["insert", "replace", "update"]:
            # Return the full document
            full_doc = change.get("fullDocument")
            if full_doc:
                return self._convert_document(full_doc)
        
        # Deletes are handled separately in read_table_deletes
        return None
    
    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> tuple[List[dict], dict]:
        """
        Read deleted records using Change Streams.
        
        Args:
            table_name: Name of the collection
            start_offset: Dictionary with resume token
            table_options: Options including 'database'
            
        Returns:
            Tuple of (deleted records, next_offset)
        """
        # Validate table exists
        supported_tables = self.list_tables()
        
        # Parse database and collection
        db_from_name, collection_name = self._parse_collection_name(table_name)
        database_name = db_from_name if db_from_name else self._get_database_name(table_options)
        
        full_table_name = f"{database_name}.{collection_name}"
        if full_table_name not in supported_tables:
            raise ValueError(
                f"Table '{table_name}' is not supported. Available tables: {supported_tables}"
            )
        
        db = self.client[database_name]
        collection = db[collection_name]
        
        records = []
        resume_token = start_offset.get("resume_token") if start_offset else None
        
        # Build watch options for delete operations only
        pipeline = [{"$match": {"operationType": "delete"}}]
        watch_options = {"pipeline": pipeline}
        
        if resume_token and resume_token != "None":
            watch_options["resume_after"] = resume_token
        
        try:
            with collection.watch(**watch_options) as stream:
                # Process a limited number of deletes in this batch
                delete_count = 0
                max_deletes = self.batch_size
                last_token = resume_token
                
                for change in stream:
                    if delete_count >= max_deletes:
                        break
                    
                    # For deletes, we only have documentKey (which contains _id)
                    document_key = change.get("documentKey", {})
                    doc_id = document_key.get("_id")
                    
                    if doc_id:
                        # Return minimal record with just _id
                        delete_record = {
                            "_id": str(doc_id) if isinstance(doc_id, ObjectId) else doc_id
                        }
                        records.append(delete_record)
                    
                    last_token = stream.resume_token
                    delete_count += 1
                
                next_offset = {"resume_token": last_token}
        
        except Exception as e:
            # If change stream not available, return empty
            # (delete tracking requires replica set)
            return [], start_offset if start_offset else {}
        
        return records, next_offset
    
    def _convert_document(self, doc: dict) -> dict:
        """
        Convert MongoDB document to JSON-serializable format.
        
        Args:
            doc: MongoDB document
            
        Returns:
            JSON-serializable dictionary
        """
        result = {}
        
        for key, value in doc.items():
            result[key] = self._convert_value(value)
        
        return result
    
    def _convert_value(self, value: Any) -> Any:
        """
        Convert a BSON value to JSON-serializable format.
        
        Args:
            value: BSON value
            
        Returns:
            JSON-serializable value
        """
        if value is None:
            return None
        elif isinstance(value, ObjectId):
            return str(value)
        elif isinstance(value, datetime):
            return value.isoformat() + "Z"
        elif isinstance(value, Decimal128):
            return float(value.to_decimal())
        elif isinstance(value, Binary):
            return value.hex()
        elif isinstance(value, BSONTimestamp):
            return value.time
        elif isinstance(value, dict):
            return {k: self._convert_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._convert_value(item) for item in value]
        elif isinstance(value, (int, float, str, bool)):
            return value
        else:
            return str(value)

