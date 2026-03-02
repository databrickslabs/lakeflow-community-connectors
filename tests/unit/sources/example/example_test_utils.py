"""Write-back and delete test utilities for the Example (simulated source) connector."""

import uuid
from typing import Dict, List, Tuple

from databricks.labs.community_connector.libs.simulated_source.api import get_api


MAX_RETRIES = 5


class LakeflowConnectTestUtils:
    def __init__(self, options: Dict[str, str]) -> None:
        username = options.get("username", "default_user")
        password = options.get("password", "default_pass")
        self._api = get_api(username, password)

    def get_source_name(self) -> str:
        return "example"

    def list_insertable_tables(self) -> List[str]:
        return ["users", "orders", "events", "products"]

    def list_deletable_tables(self) -> List[str]:
        return ["orders"]

    def generate_rows_and_write(
        self, table_name: str, number_of_rows: int
    ) -> Tuple[bool, List[Dict], Dict[str, str]]:
        if table_name not in self.list_insertable_tables():
            return False, [], {}

        generators = {
            "users": self._gen_user,
            "orders": self._gen_order,
            "events": self._gen_event,
            "products": self._gen_product,
        }
        gen_fn = generators[table_name]

        written_rows: List[Dict] = []
        for _ in range(number_of_rows):
            row = gen_fn()
            resp = self._post_with_retry(f"/tables/{table_name}/records", row)
            if resp.status_code not in (200, 201):
                return False, [], {}
            written_rows.append(row)

        pk_fields = {
            "users": "user_id",
            "orders": "order_id",
            "events": "event_id",
            "products": "product_id",
        }
        pk = pk_fields[table_name]
        column_mapping = {pk: pk}
        return True, written_rows, column_mapping

    def delete_rows(
        self, table_name: str, number_of_rows: int
    ) -> Tuple[bool, List[Dict], Dict[str, str]]:
        if table_name not in self.list_deletable_tables():
            return False, [], {}

        success, written, _ = self.generate_rows_and_write(table_name, number_of_rows)
        if not success:
            return False, [], {}

        deleted_rows: List[Dict] = []
        for row in written:
            pk_value = row["order_id"]
            resp = self._delete_with_retry(f"/tables/orders/records/{pk_value}")
            if resp.status_code != 200:
                return False, [], {}
            deleted_rows.append({"order_id": pk_value})

        column_mapping = {"order_id": "order_id"}
        return True, deleted_rows, column_mapping

    def _post_with_retry(self, path: str, body: dict):
        for _ in range(MAX_RETRIES):
            resp = self._api.post(path, json=body)
            if resp.status_code not in (429, 500, 503):
                return resp
        return resp

    def _delete_with_retry(self, path: str):
        for _ in range(MAX_RETRIES):
            resp = self._api.delete(path)
            if resp.status_code not in (429, 500, 503):
                return resp
        return resp

    @staticmethod
    def _gen_user() -> dict:
        uid = uuid.uuid4().hex[:8]
        return {
            "user_id": f"test_user_{uid}",
            "email": f"test_{uid}@example.com",
            "display_name": f"Test User {uid}",
            "status": "active",
        }

    @staticmethod
    def _gen_order() -> dict:
        uid = uuid.uuid4().hex[:8]
        return {
            "order_id": f"test_order_{uid}",
            "user_id": f"test_user_{uid}",
            "amount": 99.99,
            "status": "pending",
        }

    @staticmethod
    def _gen_event() -> dict:
        uid = uuid.uuid4().hex[:8]
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "test_event",
            "user_id": f"test_user_{uid}",
            "payload": f"test_payload_{uid}",
        }

    @staticmethod
    def _gen_product() -> dict:
        uid = uuid.uuid4().hex[:8]
        return {
            "product_id": f"test_prod_{uid}",
            "name": f"Test Product {uid}",
            "price": 42.0,
            "category": "electronics",
        }
