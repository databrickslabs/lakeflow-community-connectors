"""Checkpoint (offset) logic for ``cube_data``.

Pure functions with no I/O, so every rule can be unit-tested directly.

A *slice* is one ``(YearId, ScenarioId, CubeId)`` triple. Each slice has its
own cursor, so adding years, scenarios or cubes to the table options backfills
only the new slices, while existing slices keep their position. Slices that are
not in the current scope are left untouched.

Offset format (values are primitives so Spark can checkpoint them)::

    "fmt": 2
    "e": "*" | "id,id,..."        entity scope at commit ("*" = all entities)
    "c|<year>|<scenario>|<cube>": ticks   cursor: highest data-unit stamp read
    "w|<year>|<scenario>|<cube>": ticks   verified: every stamp <= w was read at
                                          least ``lookback`` after it was written,
                                          so no late commit can be pending below w
    "h|<year>|<scenario>|<cube>": ticks   hold-back start: the run that first saw
                                          an in-flight data unit in the slice
    "x": "id,id,..."                     entities being backfilled after joining
    "xp|<year>|<scenario>|<cube>": ticks  catch-up progress for the ``x`` entities
    "xm|<year>|<scenario>|<cube>": ticks  catch-up ceiling (slice cursor at join)

Entity catch-up covers every slice that has a cursor, including slices outside
the current scope (they catch up once they are re-added). A catch-up that
completes stays in that batch's end offset with ``xp == xm`` so the batch's
reads can still be derived from its two offsets; the next advance clears it.

Confirmation re-read: a data unit stamped within ``lookback`` of a run is read
by that run and then re-read exactly once by the first later run that starts
more than ``lookback`` after its stamp. That catches commits that became
visible after a later stamp was read (commit-order races, app-server clock
skew) without re-reading quiet slices on every run.
"""

from __future__ import annotations

OFFSET_FORMAT = 2
_SLICE_FIELDS = ("c", "w", "h", "xp", "xm")
ALL_ENTITIES = "*"


def cube_entity_csv(ids) -> str:
    return ",".join(str(i) for i in sorted({int(i) for i in ids}))


def cube_entity_set(csv: str | None) -> set[int]:
    if not csv or csv == ALL_ENTITIES:
        return set()
    return {int(v) for v in csv.split(",") if v}


def cube_offset_decode(offset: dict | None) -> dict:
    """Offset dict -> working state. ``{}``/``None`` is a fresh start."""
    state = {"e": None, "x": None, **{f: {} for f in _SLICE_FIELDS}}
    if not offset:
        return state
    if "fmt" not in offset:
        raise ValueError(
            "This cube_data checkpoint was written by an earlier version of the OneStream "
            "connector. Run a full refresh of the cube_data table."
        )
    if int(offset["fmt"]) != OFFSET_FORMAT:
        raise ValueError(f"Unsupported cube_data checkpoint format: {offset['fmt']}")
    state["e"] = offset.get("e")
    state["x"] = offset.get("x") or None
    for key, value in offset.items():
        parts = key.split("|")
        if len(parts) == 4 and parts[0] in _SLICE_FIELDS:
            state[parts[0]][(int(parts[1]), int(parts[2]), int(parts[3]))] = int(value)
    return state


def cube_offset_encode(state: dict) -> dict:
    out: dict = {"fmt": OFFSET_FORMAT, "e": state["e"] or ALL_ENTITIES}
    if state.get("x"):
        out["x"] = state["x"]
    for field in _SLICE_FIELDS:
        if field in ("xp", "xm") and not state.get("x"):
            continue
        for (year, scenario, cube), value in sorted(state[field].items()):
            out[f"{field}|{year}|{scenario}|{cube}"] = int(value)
    return out


def cube_copy_state(state: dict) -> dict:
    return {k: (dict(v) if isinstance(v, dict) else v) for k, v in state.items()}


def cube_new_entities(prev_e: str | None, cur_e: str, all_entity_ids) -> set[int]:
    """Entities in the current scope that the previous checkpoint did not cover."""
    if prev_e is None or prev_e == cur_e or prev_e == ALL_ENTITIES:
        return set()  # fresh start, unchanged, or narrowing from "all"
    previous = cube_entity_set(prev_e)
    if cur_e == ALL_ENTITIES:
        return {int(i) for i in all_entity_ids()} - previous
    return cube_entity_set(cur_e) - previous


def cube_catchup_done(state: dict) -> bool:
    """True when every catch-up slice has reached its ceiling (or there is none)."""
    return all(state["xp"].get(k, 0) >= m for k, m in state["xm"].items())


def cube_apply_scope(state: dict, cur_e: str, new_entities: set[int]) -> dict:
    """Record the current entity scope and start (or restart) entity catch-up."""
    nxt = cube_copy_state(state)
    nxt["e"] = cur_e
    if new_entities:
        # Entities whose catch-up already finished need no second pass.
        carry = set() if cube_catchup_done(state) else cube_entity_set(state.get("x"))
        nxt["x"] = cube_entity_csv(new_entities | carry)
        # Catch up from tick 0 to each slice's cursor at the moment they joined;
        # anything stamped later is covered by the normal slice cursor. Slices
        # outside the current scope get a ceiling too: their cursor does not
        # cover the new entities, so they catch up when they are re-added.
        nxt["xm"] = dict(state["c"])
        nxt["xp"] = {k: 0 for k in nxt["xm"]}
        if not nxt["xm"]:
            nxt["x"], nxt["xm"], nxt["xp"] = None, {}, {}
    return nxt


def cube_offset_advance(
    state: dict,
    rows: dict,
    catch_rows: dict,
    now: int,
    lookback: int,
    max_units: int,
    hold: bool,
    hold_timeout: int,
    in_scope: set,
) -> dict:
    """Compute the next state.

    Args:
        state: current state, already carrying the target ``e``/``x``/``xp``/``xm``.
        rows: ``{slice: sorted [(ticks, in_flight)]}`` for in-scope data units
            stamped ``<= now`` (at least every stamp above the slice's ``w``).
        catch_rows: ``{slice: sorted [ticks]}`` for the catch-up entities.
        now: run start in ticks (the init-time cap).
        lookback: confirmation re-read window in ticks.
        max_units: approximate data units per micro-batch.
        hold: skip in-flight data units and keep the cursor below them.
        hold_timeout: ticks after which an in-flight flag is treated as stale.
        in_scope: slices in the current table scope. Slices outside it keep their
            state unchanged (so re-adding one later resumes correctly).
    """
    nxt = cube_copy_state(state)
    budget = max(1, int(max_units))
    certify = now - lookback
    exhausted = False

    for key in sorted(set(rows) | (set(state["c"]) & set(in_scope))):
        if exhausted:
            break
        slice_rows = rows.get(key, [])
        c0, w0 = state["c"].get(key, 0), state["w"].get(key, 0)
        hold_at = _cube_hold_point(state, nxt, key, slice_rows, w0, now, hold, hold_timeout)
        c1, w1, consumed, exhausted = _cube_advance_slice(
            slice_rows, c0, w0, hold_at, certify, budget
        )
        budget -= consumed
        if budget <= 0:
            exhausted = True
        nxt["c"][key] = c1
        nxt["w"][key] = w1

    if nxt.get("x"):
        if cube_catchup_done(nxt):
            # Finished in the previous batch, whose end offset kept it so that
            # batch's catch-up ranges could be planned. Nothing is left to read.
            nxt["x"], nxt["xp"], nxt["xm"] = None, {}, {}
        elif not exhausted:
            _cube_advance_catchup(nxt, catch_rows, budget, in_scope)
    return nxt


def _cube_hold_point(state, nxt, key, slice_rows, w0, now, hold, hold_timeout):
    """Lowest in-flight stamp the slice cursor must stay below, or ``None``.

    Records when the hold began (``h``) so a stuck flag is released after
    ``hold_timeout``; clears it once nothing in the slice is in flight.
    """
    if not hold:
        return None
    in_flight = [t for t, flag in slice_rows if flag and t > w0]
    if not in_flight:
        nxt["h"].pop(key, None)
        return None
    since = state["h"].get(key, now)
    nxt["h"][key] = since
    return min(in_flight) if now - since <= hold_timeout else None


def _cube_advance_slice(slice_rows, c0, w0, hold_at, certify, budget):
    """Advance one slice. Returns ``(cursor, verified, units_consumed, budget_exhausted)``."""
    ticks = [t for t, _ in slice_rows]
    new = [t for t in ticks if t > c0]
    c1 = new[-1] if new else c0
    limit = hold_at - 1 if hold_at is not None else None
    if limit is not None:
        c1 = min(c1, limit)
    c1 = max(c0, c1)
    w_target = min(c1, certify) if limit is None else min(c1, certify, limit)
    rereads = sum(1 for t in ticks if w0 < t <= c0) if w_target > w0 else 0
    selected = [t for t in new if t <= c1]

    exhausted = False
    if selected and len(selected) + rereads > budget:
        take = max(1, budget - rereads)
        c1 = selected[min(take, len(selected)) - 1]
        w_target = min(w_target, c1)
        exhausted = True
    consumed = sum(1 for t in selected if t <= c1) + rereads
    return c1, max(w0, w_target), consumed, exhausted


def _cube_advance_catchup(nxt, catch_rows, budget, in_scope):
    """Advance entity catch-up for in-scope slices within the remaining budget.

    Out-of-scope slices keep their progress until they are re-added. A
    catch-up that completes here is not cleared: ``cube_batch_ranges`` needs
    it in this batch's end offset to plan the reads (see the module docstring).
    """
    for key in sorted(nxt["xm"]):
        if key not in in_scope:
            continue
        ceiling, p0 = nxt["xm"][key], nxt["xp"].get(key, 0)
        pending = [t for t in catch_rows.get(key, []) if p0 < t <= ceiling]
        if len(pending) > budget:
            nxt["xp"][key] = pending[max(1, budget) - 1]
            break
        nxt["xp"][key] = ceiling
        budget -= len(pending)


def cube_batch_ranges(start: dict, end: dict) -> tuple[list, list, str | None]:
    """Tick ranges a micro-batch ``start -> end`` must read.

    Returns ``(main, catch, catch_entities)`` where ``main`` and ``catch`` are
    lists of ``(slice, lo, hi)`` meaning stamps in ``(lo, hi]``. ``main`` applies
    to the entity scope ``end["e"]``; ``catch`` applies to ``catch_entities``.
    Derived only from the two offsets so a restarted driver plans identically.
    """
    main = []
    for key, c1 in sorted(end["c"].items()):
        c0, w0 = start["c"].get(key, 0), start["w"].get(key, 0)
        w1 = end["w"].get(key, 0)
        lo = w0 if w1 > w0 else c0  # confirmation re-read starts at the old w
        if (w1 > w0 or c1 > c0) and c1 > lo:
            main.append((key, lo, c1))

    catch, catch_entities = [], None
    if end.get("x"):
        catch_entities = end["x"]
        same = start.get("x") == end["x"]
        for key, p1 in sorted(end["xp"].items()):
            p0 = start["xp"].get(key, 0) if same else 0
            if p1 > p0:
                catch.append((key, p0, p1))
    elif start.get("x"):  # catch-up cleared: only ranges the start had not reached yet
        catch_entities = start["x"]
        for key, ceiling in sorted(start["xm"].items()):
            p0 = start["xp"].get(key, 0)
            if ceiling > p0:
                catch.append((key, p0, ceiling))
    return main, catch, catch_entities
