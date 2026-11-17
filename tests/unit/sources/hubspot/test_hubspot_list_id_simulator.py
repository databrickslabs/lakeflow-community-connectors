from requests import Request

from databricks.labs.community_connector.source_simulator.specs.hubspot.handlers.list_ids import (
    serve_legacy_lists,
    serve_list_id_mapping,
)


def test_simulator_maps_distinct_legacy_and_current_list_ids():
    corpus = {"contact_lists": [{"listId": "current-a"}, {"listId": "current-b"}]}
    catalog_request = Request(
        "GET", "https://api.hubapi.com/contacts/v1/lists?count=1"
    ).prepare()
    catalog = serve_legacy_lists(catalog_request, None, corpus).json()
    assert catalog == {
        "lists": [{"listId": "1000001"}],
        "has-more": True,
        "offset": 1,
    }

    mapping_request = Request(
        "POST",
        "https://api.hubapi.com/crm/lists/2026-09/idmapping",
        json=["1000001", "missing"],
    ).prepare()
    mapping = serve_list_id_mapping(mapping_request, None, corpus).json()
    assert mapping == {
        "legacyListIdsToIdsMapping": [
            {"listId": "current-a", "legacyListId": "1000001"},
        ],
        "missingLegacyListIds": ["missing"],
    }
