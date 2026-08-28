"""Serve cassette responses, optionally expanding records via the synthesizer."""

from __future__ import annotations

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    Cassette,
    RequestRecord,
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    request_record_from_prepared,
    response_from_record,
)
from databricks.labs.community_connector.source_simulator.synthesizer import (
    stable_seed,
    synthesize_body,
)


class Replayer:
    """Match incoming requests against a cassette and return recorded responses.

    With ``synthesize_count > 0``, expands the recorded record array to that
    count via type-aware variation. Synthesis is deterministic per request key.
    """

    def __init__(self, cassette: Cassette, synthesize_count: int = 0) -> None:
        self.cassette = cassette
        self.synthesize_count = synthesize_count

    def replay(self, prep: PreparedRequest) -> Response:
        req_rec = request_record_from_prepared(prep)
        resp_rec = self.cassette.match(req_rec)
        if self.synthesize_count > 0:
            resp_rec = self._maybe_synthesize(req_rec, resp_rec)
        return response_from_record(resp_rec, prep)

    def _maybe_synthesize(
        self, req_rec: RequestRecord, resp_rec: ResponseRecord
    ) -> ResponseRecord:
        seed = stable_seed(
            req_rec.method, req_rec.url, repr(sorted(req_rec.query.items()))
        )
        expanded = synthesize_body(
            resp_rec.body_text,
            target_count=self.synthesize_count,
            seed=seed,
        )
        if expanded is resp_rec.body_text:
            return resp_rec
        # Return a copy so the cached cassette record isn't mutated across
        # repeated replays of the same key.
        return ResponseRecord(
            status_code=resp_rec.status_code,
            headers=dict(resp_rec.headers),
            body_text=expanded,
            body_b64=resp_rec.body_b64,
            encoding=resp_rec.encoding,
            url=resp_rec.url,
        )
