"""Persist live HTTP traffic to a cassette.

Trims response bodies to ``sample_size`` records, strips pagination pointers,
scrubs sensitive response headers and email-shape strings. Dedups by request
match-key so multi-page walks collapse to one cassette entry.
"""

from __future__ import annotations

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    Cassette,
    Interaction,
    scrub_emails,
    scrub_headers,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    request_record_from_prepared,
    response_record_from_response,
)
from databricks.labs.community_connector.source_simulator.sampler import (
    sample_body,
    strip_link_header_next,
)


class Recorder:
    """Append (request, response) pairs to a cassette during live runs."""

    def __init__(self, cassette: Cassette, sample_size: int = 5) -> None:
        self.cassette = cassette
        self.sample_size = sample_size

    def record(self, prep: PreparedRequest, resp: Response) -> None:
        req_rec = request_record_from_prepared(prep)

        # Dedup: if this match-key is already on tape, skip. The connector
        # already received the live response; we just don't append a duplicate.
        if self.cassette.has_key(req_rec):
            return

        resp_rec = response_record_from_response(resp)
        resp_rec.body_text = sample_body(
            resp_rec.body_text, max_records=self.sample_size
        )
        resp_rec.body_text = scrub_emails(resp_rec.body_text)
        resp_rec.headers = scrub_headers(strip_link_header_next(resp_rec.headers))

        self.cassette.append(Interaction(request=req_rec, response=resp_rec))
