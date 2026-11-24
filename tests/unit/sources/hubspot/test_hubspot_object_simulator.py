"""Object simulator keeps archive filtering, pagination and projection independent."""

from requests import Request

from databricks.labs.community_connector.source_simulator.specs.hubspot.handlers.search import (
    serve_objects,
)


def test_object_listing_archive_pages_and_property_projection():
    corpus = {"calls": [
        {"id": "1", "archived": False, "properties": None},
        {"id": "2", "archived": True, "properties": {"keep": "yes", "drop": "no"}},
        {"id": "3", "archived": True, "properties": None},
    ]}
    url = "https://api.hubapi.com/crm/objects/2026-09/calls"

    def read(**params):
        prep = Request("GET", url, params=params).prepare()
        return serve_objects(prep, None, corpus).json()

    active = read(properties="keep")
    assert active["results"] == [{"id": "1", "archived": False, "properties": None}]
    first = read(archived="true", limit=1, properties="keep")
    assert first["results"] == [{"id": "2", "archived": True, "properties": {"keep": "yes"}}]
    second = read(archived="true", limit=1, after=first["paging"]["next"]["after"], properties="keep")
    assert second["results"] == [{"id": "3", "archived": True, "properties": None}]
    assert second["paging"] == {}
    assert corpus["calls"][1]["properties"]["drop"] == "no"



def test_search_does_not_return_archived_parents():
    from databricks.labs.community_connector.source_simulator.specs.hubspot.handlers.search import (
        serve_search,
    )
    corpus = {"calls": [{"id": "1", "archived": True}, {"id": "2", "archived": False}]}
    prep = Request("POST", "https://api.hubapi.com/crm/objects/2026-09/calls/search",
                   json={"limit": 100}).prepare()
    data = serve_search(prep, None, corpus).json()
    assert [record["id"] for record in data["results"]] == ["2"]
    assert data["total"] == 1
