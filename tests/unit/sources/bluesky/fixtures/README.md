# Test data

`events.json` contains synthetic create/update/delete, identity, account, sync, and
resync events. The Python HTTP simulator encodes them as jss0/v1 archive bytes.

`native.jss` was independently generated with the **official Go segment writer** at
Jetstream revision `3fa54fdbb0f47ad3aa43de78a6fbbd8dc362f81d`, not the Python simulator.
It contains three events (seq 1–3: create, update, delete), witnessed timestamp
1700000000000000, DID `did:plc:fixture`, collection `app.bsky.feed.post`, rkey `one`,
rev `rev`, and CBOR payload `a164746578746568656c6c6f` (`{"text":"hello"}`) for the
create/update. Delete payload is empty. `segment.Config.MaxEventsPerBlock=2`;
`Append`, `Flush` when full, then `Seal`. Tests read this fixture through the full
HTTP/header/footer/checksum/block-decoder path. Go is not needed to run tests or build.

Regenerate only when intentionally adopting a new archive format. Record the new
upstream revision and review decoded output. See the root NOTICE for attribution.
