"""
update_activity.py — Scrape PayoutRedemption, PositionSplit, PositionsMerge,
and NegRiskConversion events from Polymarket's activity-subgraph on Goldsky.

These four event streams are needed alongside trades to compute true realized
USD PnL per user (orderFilled trades alone miss splits/merges and resolution
redemptions, biasing buy-and-hold winners and active inventory holders).

Outputs four CSVs in goldsky/, one per entity:
    goldsky/redemptions.csv
    goldsky/splits.csv
    goldsky/merges.csv
    goldsky/negRiskConversions.csv

Cursor state per-entity in goldsky/activity_cursors.json so each stream
resumes independently.
"""

import os
import json
import signal
import subprocess
import time
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests as req
import pandas as pd
from flatten_json import flatten

QUERY_URL = (
    "https://api.goldsky.com/api/public/"
    "project_cl6mb8i9h0003e201j6li0diw/"
    "subgraphs/activity-subgraph/0.0.4/gn"
)

CURSOR_FILE = "goldsky/activity_cursors.json"
PAGE_SIZE = 1000

# -----------------------------------------------------------------------------
# Entity definitions — one config per activity event
# -----------------------------------------------------------------------------
ENTITIES = {
    "redemption": {
        "plural": "redemptions",
        "fields": ["id", "timestamp", "redeemer", "condition", "payout"],
        "output": "goldsky/redemptions.csv",
    },
    "split": {
        "plural": "splits",
        "fields": ["id", "timestamp", "stakeholder", "condition", "amount"],
        "output": "goldsky/splits.csv",
    },
    "merge": {
        "plural": "merges",
        "fields": ["id", "timestamp", "stakeholder", "condition", "amount"],
        "output": "goldsky/merges.csv",
    },
    "conversion": {
        "plural": "negRiskConversions",
        "fields": [
            "id",
            "timestamp",
            "stakeholder",
            "negRiskMarketId",
            "amount",
            "indexSet",
            "questionCount",
        ],
        "output": "goldsky/negRiskConversions.csv",
    },
}


# -----------------------------------------------------------------------------
# Shutdown handling
# -----------------------------------------------------------------------------
shutdown_event = threading.Event()
print_lock = threading.Lock()
cursor_lock = threading.Lock()


def tprint(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs, flush=True)


def handle_shutdown(signum, frame):
    tprint("\n⚠ Shutdown requested — finishing current pages, then exiting.")
    shutdown_event.set()


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# -----------------------------------------------------------------------------
# Cursor — single JSON file, keyed per entity
# -----------------------------------------------------------------------------
def _load_all_cursors():
    if not os.path.isfile(CURSOR_FILE):
        return {}
    try:
        with open(CURSOR_FILE) as f:
            return json.load(f)
    except Exception as e:
        tprint(f"⚠ Cursor file unreadable, starting fresh: {e}")
        return {}


def _save_all_cursors(cursors):
    with cursor_lock:
        os.makedirs(os.path.dirname(CURSOR_FILE), exist_ok=True)
        tmp = CURSOR_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(cursors, f, indent=2)
        os.replace(tmp, CURSOR_FILE)


def get_entity_cursor(entity, all_cursors):
    """Returns (last_timestamp, last_id, sticky_timestamp) for an entity."""
    state = all_cursors.get(entity, {})
    ts = state.get("last_timestamp", 0)
    lid = state.get("last_id")
    sticky = state.get("sticky_timestamp")
    if sticky is not None and lid is None:
        sticky = None

    # Cursor present — use it
    if ts > 0:
        readable = datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M UTC"
        )
        tprint(f"  [{entity}] resume: ts={ts} ({readable}) sticky={sticky}")
        return ts, lid, sticky

    # No cursor — try to recover from CSV tail
    output_file = ENTITIES[entity]["output"]
    if os.path.isfile(output_file):
        try:
            tail = subprocess.run(
                ["tail", "-n", "1", output_file],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            head = (
                subprocess.run(
                    ["head", "-n", "1", output_file],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                .stdout.strip()
                .split(",")
            )
            if tail and "timestamp" in head:
                ts_idx = head.index("timestamp")
                vals = tail.split(",")
                if len(vals) > ts_idx:
                    recovered = int(vals[ts_idx])
                    tprint(f"  [{entity}] resume from CSV tail: ts={recovered}")
                    return recovered - 1, None, None
        except Exception as e:
            tprint(f"  [{entity}] CSV tail recovery failed: {e}")

    tprint(f"  [{entity}] starting from beginning")
    return 0, None, None


def update_entity_cursor(entity, all_cursors, last_ts, last_id, sticky):
    all_cursors[entity] = {
        "last_timestamp": last_ts,
        "last_id": last_id,
        "sticky_timestamp": sticky,
    }
    _save_all_cursors(all_cursors)


# -----------------------------------------------------------------------------
# GraphQL query
# -----------------------------------------------------------------------------
def _build_query(plural, fields, where_clause, at_once):
    field_list = "\n                ".join(fields)
    return (
        """query MyQuery {
            """
        + plural
        + """(orderBy: timestamp, orderDirection: asc
                """
        + ("first: " + str(at_once))
        + """
                where: {"""
        + where_clause
        + """}) {
                """
        + field_list
        + """
            }
        }"""
    )


def _query(session, plural, fields, where_clause, at_once=PAGE_SIZE):
    """Single GraphQL query with retry until shutdown."""
    q = _build_query(plural, fields, where_clause, at_once)
    attempt = 0
    while not shutdown_event.is_set():
        try:
            resp = session.post(QUERY_URL, json={"query": q}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                raise Exception(f"GraphQL errors: {data['errors']}")
            return data["data"][plural]
        except Exception as e:
            attempt += 1
            wait = min(60, 2 * attempt)
            if attempt % 5 == 0:
                tprint(
                    f"  [{plural}] query failing (attempt {attempt}): {e}. "
                    f"Retry in {wait}s"
                )
            time.sleep(wait)
    return []


def _get_latest_remote_timestamp(session, plural):
    q = (
        "{ "
        + plural
        + "(first: 1, orderBy: timestamp, orderDirection: desc) { timestamp } }"
    )
    try:
        resp = session.post(QUERY_URL, json={"query": q}, timeout=15)
        resp.raise_for_status()
        events = resp.json()["data"][plural]
        if events:
            return int(events[0]["timestamp"])
    except Exception as e:
        tprint(f"  [{plural}] could not fetch latest remote ts: {e}")
    return None


# -----------------------------------------------------------------------------
# Scrape one entity
# -----------------------------------------------------------------------------
def scrape_entity(entity, all_cursors_ref):
    """Worker function — scrapes one entity stream end-to-end."""
    cfg = ENTITIES[entity]
    plural = cfg["plural"]
    fields = cfg["fields"]
    output = cfg["output"]

    last_ts, last_id, sticky = get_entity_cursor(entity, all_cursors_ref)

    session = req.Session()
    session.headers.update({"Content-Type": "application/json"})

    remote_latest = _get_latest_remote_timestamp(session, plural)
    if remote_latest is None:
        tprint(f"  [{entity}] using current time as remote latest")
        remote_latest = int(time.time())

    gap = remote_latest - last_ts
    rstart = datetime.fromtimestamp(last_ts, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M UTC"
    )
    rend = datetime.fromtimestamp(remote_latest, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M UTC"
    )
    tprint(f"  [{entity}] range: {rstart} -> {rend} ({gap // 86400}d)")

    if gap <= 0:
        tprint(f"  [{entity}] up to date.")
        session.close()
        return 0

    file_started = os.path.isfile(output)
    total = 0
    page = 0
    t0 = time.time()

    while not shutdown_event.is_set():
        if sticky is not None:
            where = f'timestamp: "{sticky}", id_gt: "{last_id}"'
        else:
            where = f'timestamp_gt: "{last_ts}"'

        events = _query(session, plural, fields, where)
        if shutdown_event.is_set():
            break

        if not events:
            if sticky is not None:
                # Sticky page drained — advance past it
                last_ts = sticky
                sticky = None
                last_id = None
                update_entity_cursor(entity, all_cursors_ref, last_ts, last_id, sticky)
                continue
            break

        df = pd.DataFrame([flatten(e) for e in events]).reset_index(drop=True)
        df = df.sort_values(["timestamp", "id"]).reset_index(drop=True)

        batch_last_ts = int(df.iloc[-1]["timestamp"])
        batch_last_id = df.iloc[-1]["id"]

        full_page = len(df) >= PAGE_SIZE
        if full_page:
            sticky = batch_last_ts
            last_id = batch_last_id
        else:
            if sticky is not None:
                last_ts = sticky
                sticky = None
                last_id = None
            else:
                last_ts = batch_last_ts

        df = df.drop_duplicates(subset=["id"])
        df_save = df[fields].copy()

        # flatten_json returns list-typed cols as 'fieldname_0', 'fieldname_1'…
        # for indexSets specifically. Coalesce any missing array cols.
        for col in fields:
            if col not in df_save.columns:
                df_save[col] = ""
        df_save = df_save[fields]

        if file_started:
            df_save.to_csv(output, index=False, mode="a", header=False)
        else:
            df_save.to_csv(output, index=False)
            file_started = True

        total += len(df_save)
        page += 1

        update_entity_cursor(entity, all_cursors_ref, last_ts, last_id, sticky)

        if page % 25 == 0 or not full_page:
            readable = datetime.fromtimestamp(batch_last_ts, tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M UTC"
            )
            elapsed = time.time() - t0
            rate = total / elapsed if elapsed else 0
            tprint(
                f"  [{entity}] page {page}: ts={readable} | "
                f"total {total:,} | {rate:.0f} rec/s"
            )

        if not full_page and sticky is None:
            break

    session.close()
    tprint(f"  [{entity}] DONE: {total:,} records in {(time.time() - t0) / 60:.1f}min")
    return total


# -----------------------------------------------------------------------------
# Main entry
# -----------------------------------------------------------------------------
def update_activity(only=None):
    """
    Scrape all four activity entities concurrently.

    Args:
        only: optional list of entity names to run a subset, e.g.
              update_activity(only=['redemption', 'merge'])
    """
    print("=" * 60)
    print("📜 Activity events (redemptions / splits / merges / conversions)")
    print("=" * 60)

    os.makedirs("goldsky", exist_ok=True)

    entities_to_run = list(ENTITIES) if only is None else list(only)
    bad = [e for e in entities_to_run if e not in ENTITIES]
    if bad:
        raise ValueError(f"unknown entities: {bad}; valid: {list(ENTITIES)}")

    all_cursors = _load_all_cursors()

    # Run the four streams concurrently — they're independent and the subgraph
    # comfortably handles parallel queries (Polymarket's own UI does this).
    grand_total = 0
    with ThreadPoolExecutor(max_workers=len(entities_to_run)) as ex:
        futures = {
            ex.submit(scrape_entity, entity, all_cursors): entity
            for entity in entities_to_run
        }
        for fut in as_completed(futures):
            entity = futures[fut]
            try:
                grand_total += fut.result()
            except Exception as e:
                tprint(f"  [{entity}] FAILED: {e}")

    if shutdown_event.is_set():
        print(f"\n⚠ Stopped early. New records: {grand_total:,}")
    else:
        print(f"\n✅ Done. New records: {grand_total:,}")
    print(f"   Outputs: {', '.join(ENTITIES[e]['output'] for e in entities_to_run)}")
    print(f"   Cursor:  {CURSOR_FILE}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        update_activity(only=sys.argv[1:])
    else:
        update_activity()
