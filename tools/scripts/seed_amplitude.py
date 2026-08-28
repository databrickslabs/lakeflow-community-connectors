#!/usr/bin/env python3
"""
Seed the Amplitude trial account with realistic test data that mirrors every
corpus file used by the connector simulator.

What gets seeded
----------------
  events          → 8 event types × 5 users × 8 days = rich /api/2/export window
  sessions        → 3–8 events per session → feeds /api/2/sessions/average
  daily users     → 5 unique user_ids active each day → feeds /api/2/users
  event taxonomy  → 8 distinct event_type names → feeds /api/2/events/list

Usage
-----
  python3 tools/scripts/seed_amplitude.py --api-key KEY [--days N]

  The API key must be supplied via --api-key or the AMPLITUDE_API_KEY env var.
"""
import argparse
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

import requests

INGEST_URL = "https://api2.amplitude.com/2/httpapi"
BATCH_SIZE = 100          # Amplitude max per request

# ── realistic event taxonomy (mirrors events_list corpus shape) ───────────────
EVENT_TYPES = [
    "page_view",
    "button_click",
    "purchase",
    "sign_up",
    "login",
    "search",
    "add_to_cart",
    "checkout_complete",
]

# ── realistic user pool ───────────────────────────────────────────────────────
USERS = [
    {"user_id": "user_alice_001",   "platform": "web",     "country": "US"},
    {"user_id": "user_bob_002",     "platform": "ios",     "country": "GB"},
    {"user_id": "user_carol_003",   "platform": "android", "country": "DE"},
    {"user_id": "user_dave_004",    "platform": "web",     "country": "IN"},
    {"user_id": "user_eve_005",     "platform": "ios",     "country": "CA"},
]

# ── event property templates ──────────────────────────────────────────────────
EVENT_PROPS = {
    "page_view": lambda: {
        "page": random.choice(["/home", "/products", "/cart", "/checkout"])
    },
    "button_click": lambda: {
        "button": random.choice(["buy_now", "add_to_cart", "learn_more", "sign_up"])
    },
    "purchase": lambda: {
        "amount": round(random.uniform(9.99, 499.99), 2),
        "currency": "USD",
        "items": random.randint(1, 5),
    },
    "sign_up": lambda: {"method": random.choice(["email", "google", "apple"])},
    "login": lambda: {"method": random.choice(["email", "sso", "magic_link"])},
    "search": lambda: {
        "query": random.choice(["shoes", "laptop", "headphones", "book", "coffee"])
    },
    "add_to_cart": lambda: {
        "product_id": f"prod_{random.randint(1000,9999)}",
        "price": round(random.uniform(4.99, 199.99), 2),
    },
    "checkout_complete": lambda: {
        "order_id": f"order_{random.randint(10000,99999)}",
        "total": round(random.uniform(19.99, 999.99), 2),
    },
}

OS_MAP = {"web": ("Chrome", "130.0"), "ios": ("iOS", "17.2"), "android": ("Android", "14.0")}
DEVICE_MAP = {
    "web":     ("Desktop",  "MacBook Pro"),
    "ios":     ("Mobile",   "iPhone 15"),
    "android": ("Mobile",   "Samsung Galaxy S24"),
}


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def build_session(user: dict, session_start: datetime, events_in_session: int) -> list:
    """Return a list of Amplitude event dicts for one session."""
    session_id = _ms(session_start)
    os_name, os_ver = OS_MAP[user["platform"]]
    dev_family, dev_type = DEVICE_MAP[user["platform"]]

    # pick a coherent event flow for the session
    flow = [EVENT_TYPES[0]]  # always start with page_view
    flow += random.choices(EVENT_TYPES[1:], k=events_in_session - 1)

    batch = []
    for i, etype in enumerate(flow):
        t = session_start + timedelta(seconds=i * random.randint(15, 90))
        batch.append({
            "user_id":            user["user_id"],
            "device_id":          f"device_{user['user_id']}",
            "event_type":         etype,
            "time":               _ms(t),
            "session_id":         session_id,
            "platform":           user["platform"],
            "os_name":            os_name,
            "os_version":         os_ver,
            "device_family":      dev_family,
            "device_type":        dev_type,
            "country":            user["country"],
            "insert_id":          str(uuid.uuid4()),
            "event_properties":   EVENT_PROPS[etype](),
            "user_properties": {
                "premium":   random.choice([True, False]),
                "signup_day": "2024-01-01",
            },
        })
    return batch


def build_day(day: datetime, sessions_per_user: int = 2) -> list:
    """Return all events for every user on a given day."""
    all_events = []
    for user in USERS:
        for _ in range(sessions_per_user):
            # random session start within business hours
            hour = random.randint(8, 22)
            minute = random.randint(0, 59)
            session_start = day.replace(hour=hour, minute=minute, second=0, microsecond=0)
            n_events = random.randint(3, 8)
            all_events.extend(build_session(user, session_start, n_events))
    return all_events


def send_batch(api_key: str, events: list, dry_run: bool = False) -> dict:
    if dry_run:
        print(f"  [dry-run] would send {len(events)} events")
        return {"code": 200, "events_ingested": len(events)}
    resp = requests.post(
        INGEST_URL,
        json={"api_key": api_key, "events": events},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed Amplitude trial account")
    parser.add_argument("--api-key", default=os.environ.get("AMPLITUDE_API_KEY"),
                        help="Amplitude project API key (or set AMPLITUDE_API_KEY)")
    parser.add_argument("--days", type=int, default=8,
                        help="Number of past days to generate data for (default: 8)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print stats without sending to Amplitude")
    args = parser.parse_args()

    if not args.api_key and not args.dry_run:
        parser.error("no API key: pass --api-key or set AMPLITUDE_API_KEY")

    today = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    all_events: list = []
    for d in range(args.days):
        day = today - timedelta(days=d)
        day_events = build_day(day, sessions_per_user=3)
        all_events.extend(day_events)
        print(f"  {day.date()}  → {len(day_events)} events  "
              f"({len(set(e['user_id'] for e in day_events))} users, "
              f"{len(set(e['session_id'] for e in day_events))} sessions)")

    total = len(all_events)
    print(f"\nTotal events to ingest: {total}")
    print(f"Event types: {sorted(set(e['event_type'] for e in all_events))}")
    print(f"Users:       {sorted(set(e['user_id'] for e in all_events))}\n")

    ingested = 0
    for i in range(0, total, BATCH_SIZE):
        chunk = all_events[i : i + BATCH_SIZE]
        result = send_batch(args.api_key, chunk, dry_run=args.dry_run)
        ingested += result.get("events_ingested", 0)
        print(f"  batch {i // BATCH_SIZE + 1}: sent {len(chunk)}, "
              f"server_upload_time={result.get('server_upload_time', 'n/a')}")

    print(f"\n✓ Done — {ingested} events ingested into Amplitude.")
    print("\nNOTE: The /api/2/export endpoint has a ~5 min processing delay.")
    print("Re-run live tests after waiting:\n")
    print("  CONNECTOR_TEST_MODE=live \\")
    print("  CONNECTOR_TEST_CONFIG_JSON='{\"api_key\":\"...\",\"secret_key\":\"...\"}' \\")
    print("  pytest tests/unit/sources/amplitude/ -v")


if __name__ == "__main__":
    main()
