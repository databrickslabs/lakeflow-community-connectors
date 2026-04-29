from databricks.labs.community_connector.sources.github.github import GithubLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestGithubConnector(LakeflowConnectTests):
    connector_class = GithubLakeflowConnect
    # ``since``/``until`` vary per run (now()-based windows). ``page``/``per_page``
    # drive pagination — we want those collapsed so one cassette entry serves
    # every page-call of the same endpoint.
    record_replay_ignore_query_params = frozenset(
        {"since", "until", "page", "per_page"}
    )
    # Expand each replayed response's records to this many via type-aware
    # variation. Gives the read tests more varied sample data than the 5
    # rows stored in the cassette.
    record_replay_synthesize_count = 30
