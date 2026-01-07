import requests
import time
import random
from datetime import datetime
from typing import Dict, List, Tuple


# TODO: Refactor this utils to improve the interface in the future and do not add more tests using this utils.
class LakeflowConnectTestUtils:
    """
    Test utilities for HubSpot connector.
    Provides write-back functionality for testing HubSpot data ingestion.
    """

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize HubSpot test utilities with connection options.

        Args:
            options: Dictionary containing 'access_token' for HubSpot API authentication
        """
        self.options = options
        self.access_token = options["access_token"]
        self.base_url = "https://api.hubapi.com"
        self.auth_header = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def get_source_name(self) -> str:
        """Return the source connector name."""
        return "hubspot"

    def list_insertable_tables(self) -> List[str]:
        """
        List all tables that support insert/write-back functionality in HubSpot.

        Returns only the tables for which generate_rows_and_write has specific implementations.
        """
        return ["contacts", "companies"]

    def generate_rows_and_write(self, table_name: str, number_of_rows: int) -> Tuple[bool, List[Dict], Dict[str, str]]:
        """
        Generate specified number of rows and write them to the given HubSpot table.

        Args:
            table_name: Name of the HubSpot table to write to (e.g., 'contacts', 'companies')
            number_of_rows: Number of rows to generate and write

        Returns:
            Tuple containing:
            - Boolean indicating success of the operation
            - List of rows as dictionaries that were written
            - Dictionary mapping written column names to returned column names
        """
        try:
            if number_of_rows <= 0:
                return False, [], {}

            if table_name not in self.list_insertable_tables():
                return False, [], {}

            # Generate rows based on table type
            generated_rows = self._generate_sample_data(table_name, number_of_rows)
            if not generated_rows:
                return False, [], {}

            # Write rows to HubSpot using batch API if available, otherwise single writes
            success = self._write_rows_to_hubspot(table_name, generated_rows)

            if success:
                # Create mapping from written column names to returned column names
                column_mapping = self._get_column_mapping(table_name, generated_rows)
                # To ensure that the rows are committed.
                time.sleep(60)
                return True, generated_rows, column_mapping
            else:
                return False, [], {}

        except Exception as e:
            print(f"Error in generate_rows_and_write for {table_name}: {e}")
            return False, [], {}

    def _generate_sample_data(self, table_name: str, count: int) -> List[Dict]:
        """
        Generate sample data based on the table type and HubSpot schema requirements.
        """
        rows = []

        for i in range(count):
            if table_name == "contacts":
                row = {
                    "email": f"generated_contact_{i}_{random.randint(1000, 9999)}@example.com",
                    "firstname": f"FirstName_{i}",
                    "lastname": f"LastName_{i}",
                    "phone": f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
                    "jobtitle": f"Job Title {i}",
                    "company": f"Company {i}"
                }
            elif table_name == "companies":
                row = {
                    "name": f"Generated Company {i}",
                    "domain": f"company{i}-{random.randint(100, 999)}.com",
                    "city": f"City {i}",
                    "state": "California",
                    "country": "United States",
                    "industry": "ACCOUNTING",
                    "phone": f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
                }
            else:
                # Fallback for unsupported tables
                row = {
                    "hs_timestamp": str(int(datetime.now().timestamp() * 1000)),
                    "subject": f"Generated {table_name} {i}",
                    "body": f"This is generated content for {table_name} record {i}"
                }

            rows.append(row)

        return rows

    def _write_rows_to_hubspot(self, table_name: str, rows: List[Dict]) -> bool:
        """
        Write rows to HubSpot using the appropriate API endpoint.
        Uses batch API for efficiency when writing multiple records.
        """
        try:
            if len(rows) == 1:
                # Single record creation
                return self._create_single_record(table_name, rows[0])
            else:
                # Batch creation for multiple records
                return self._create_batch_records(table_name, rows)

        except Exception as e:
            print(f"Error writing to HubSpot {table_name}: {e}")
            return False

    def _create_single_record(self, table_name: str, record: Dict) -> bool:
        """Create a single record in HubSpot."""
        url = f"{self.base_url}/crm/v3/objects/{table_name}"

        payload = {
            "properties": record
        }

        try:
            response = requests.post(url, headers=self.auth_header, json=payload)

            if response.status_code in [200, 201]:
                print(f"Successfully created {table_name} record")
                return True
            else:
                print(f"Failed to create {table_name} record: {response.status_code} {response.text}")
                return False

        except Exception as e:
            print(f"Error creating single {table_name} record: {e}")
            return False

    def _create_batch_records(self, table_name: str, records: List[Dict]) -> bool:
        """Create multiple records in HubSpot using batch API."""
        url = f"{self.base_url}/crm/v3/objects/{table_name}/batch/create"

        # Format records for batch API
        batch_inputs = [{"properties": record} for record in records]
        payload = {"inputs": batch_inputs}

        try:
            response = requests.post(url, headers=self.auth_header, json=payload)

            if response.status_code in [200, 201]:
                result_data = response.json()
                results = result_data.get("results", [])
                print(f"Successfully created {len(results)} {table_name} records via batch API")
                return True
            else:
                print(f"Failed to create batch {table_name} records: {response.status_code} {response.text}")
                # Fallback to individual creation if batch fails
                return self._fallback_individual_creation(table_name, records)

        except Exception as e:
            print(f"Error in batch creation for {table_name}: {e}")
            # Fallback to individual creation
            return self._fallback_individual_creation(table_name, records)

    def _fallback_individual_creation(self, table_name: str, records: List[Dict]) -> bool:
        """Fallback method to create records individually if batch fails."""
        success_count = 0

        for i, record in enumerate(records):
            if self._create_single_record(table_name, record):
                success_count += 1
            # Add small delay to avoid rate limits
            time.sleep(0.1)

        print(f"Created {success_count} out of {len(records)} {table_name} records individually")
        return success_count > 0

    def _get_column_mapping(self, table_name: str, generated_rows: List[Dict]) -> Dict[str, str]:
        """
        Create mapping from written column names to returned column names.
        For HubSpot, written properties are nested under the 'properties' field.
        Uses dot notation: 'email' -> 'properties.email'
        """
        if not generated_rows:
            return {}

        column_mapping = {}
        for column in generated_rows[0].keys():
            # In HubSpot, written properties are nested under 'properties' struct
            column_mapping[column] = f"properties.{column}"

        return column_mapping
