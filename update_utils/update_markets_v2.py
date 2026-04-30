import requests
import csv
import json
import os
import time
from typing import List, Dict


GAMMA_BASE = "https://gamma-api.polymarket.com"


def count_csv_lines(csv_filename: str) -> int:
    """Count the number of data lines in CSV (excluding header)"""
    if not os.path.exists(csv_filename):
        return 0
    try:
        with open(csv_filename, "r", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)
            return sum(1 for row in reader if row)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return 0


def _cursor_path(csv_filename: str) -> str:
    return csv_filename + ".cursor"


def _bad_log_path(csv_filename: str) -> str:
    return csv_filename + ".bad_rows.log"


def _load_cursor(csv_filename: str):
    p = _cursor_path(csv_filename)
    if not os.path.isfile(p):
        return None
    with open(p, "r", encoding="utf-8") as f:
        return f.read().strip() or None


def _save_cursor(csv_filename: str, cursor):
    p = _cursor_path(csv_filename)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(cursor or "")
    os.replace(tmp, p)


def _clear_cursor(csv_filename: str):
    p = _cursor_path(csv_filename)
    if os.path.isfile(p):
        os.remove(p)


def _is_blank(v) -> bool:
    """null or empty string both count as missing."""
    return v is None or v == ""


def _parse_str_list(v):
    """Polymarket returns outcomes/clobTokenIds as a JSON-encoded string. Return list or None."""
    if v is None or v == "":
        return None
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, list) else None
        except json.JSONDecodeError:
            return None
    return None


def _format_tags(market: dict) -> str:
    """Pipe-joined tag labels from /markets endpoint when include_tag=true."""
    tags = market.get("tags") or []
    labels = []
    for t in tags:
        label = t.get("label") or t.get("slug")
        if label:
            labels.append(label)
    return "|".join(labels)


def _build_row(market: dict) -> List:
    """Build a CSV row from a market dict. Preserves null/empty as empty strings —
    caller is responsible for detecting and re-fetching."""
    outcomes = _parse_str_list(market.get("outcomes"))
    clob_tokens = _parse_str_list(market.get("clobTokenIds"))

    answer1 = outcomes[0] if outcomes and len(outcomes) > 0 else ""
    answer2 = outcomes[1] if outcomes and len(outcomes) > 1 else ""
    token1 = clob_tokens[0] if clob_tokens and len(clob_tokens) > 0 else ""
    token2 = clob_tokens[1] if clob_tokens and len(clob_tokens) > 1 else ""

    neg_risk = market.get("negRiskAugmented", False) or market.get(
        "negRiskOther", False
    )
    question_text = market.get("question", "") or market.get("title", "") or ""

    ticker = ""
    if market.get("events") and len(market["events"]) > 0:
        ticker = market["events"][0].get("ticker", "") or ""

    actual_closed = market.get("closedTime") or ""
    resolved = bool(actual_closed)
    closed_time_value = actual_closed if resolved else (market.get("endDate") or "")

    return [
        market.get("createdAt", "") or "",
        market.get("id", "") or "",
        question_text,
        answer1,
        answer2,
        neg_risk,
        market.get("slug", "") or "",
        token1,
        token2,
        market.get("conditionId", "") or "",
        market.get("volume", "") or "",
        ticker,
        resolved,
        closed_time_value,
        _format_tags(market),
    ]


# Indices into the row list (kept in sync with _build_row / headers)
COL_ID = 1
COL_ANSWER1 = 3
COL_ANSWER2 = 4
COL_TOKEN1 = 7
COL_TOKEN2 = 8
COL_CLOSED_TIME = 13
COL_TAGS = 14


def _row_is_missing_critical(row: List) -> List[str]:
    """Return list of missing-field names. Empty list = row is fine."""
    missing = []
    if _is_blank(row[COL_ANSWER1]) and _is_blank(row[COL_ANSWER2]):
        missing.append("outcomes")
    if _is_blank(row[COL_TOKEN1]) and _is_blank(row[COL_TOKEN2]):
        missing.append("clobTokenIds")
    if _is_blank(row[COL_CLOSED_TIME]):
        missing.append("closedTime/endDate")
    return missing


def _request_with_retry(session, url, params=None, max_attempts=10):
    """GET with retry on 429/500/503/network errors. Returns response or None."""
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                wait = min(60, 5 * attempt)
                print(f"  Rate limited, waiting {wait}s (attempt {attempt})")
                time.sleep(wait)
                continue
            if resp.status_code in (500, 503):
                wait = min(60, 3 * attempt)
                print(f"  {resp.status_code}, retrying in {wait}s (attempt {attempt})")
                time.sleep(wait)
                continue
            return resp
        except requests.exceptions.RequestException as e:
            wait = min(60, 3 * attempt)
            print(f"  Network error: {e}, retrying in {wait}s")
            time.sleep(wait)
    return None


def _fetch_market_by_id(session, market_id):
    """Fetch a single market with tags. Returns market dict or None on 404/total failure."""
    url = f"{GAMMA_BASE}/markets/{market_id}"
    resp = _request_with_retry(session, url, params={"include_tag": "true"})
    if resp is None:
        return None
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        print(
            f"  Unexpected status {resp.status_code} for market {market_id}: {resp.text[:200]}"
        )
        return None
    try:
        return resp.json()
    except Exception as e:
        print(f"  JSON parse failed for market {market_id}: {e}")
        return None


# CSV header (kept in sync with _build_row and COL_* indices)
HEADERS = [
    "createdAt",
    "id",
    "question",
    "answer1",
    "answer2",
    "neg_risk",
    "market_slug",
    "token1",
    "token2",
    "condition_id",
    "volume",
    "ticker",
    "resolved",
    "closedTime",
    "tags",
]


def _phase1_keyset_fetch(csv_filename: str, batch_size: int):
    """Phase 1: keyset-paginate /markets/keyset, write rows, resume via cursor sidecar."""
    base_url = f"{GAMMA_BASE}/markets/keyset"

    existing_lines = count_csv_lines(csv_filename)
    after_cursor = _load_cursor(csv_filename)
    file_exists = os.path.exists(csv_filename) and existing_lines > 0

    if file_exists and after_cursor:
        print(f"Phase 1: resuming from saved cursor ({existing_lines} existing rows)")
        mode = "a"
    elif file_exists and not after_cursor:
        print(
            f"Phase 1: file exists ({existing_lines} rows) but no cursor — "
            f"skipping phase 1 (assume keyset pass complete)"
        )
        return
    else:
        print(f"Phase 1: creating new CSV {csv_filename}")
        mode = "w"

    session = requests.Session()
    total_fetched = 0

    with open(csv_filename, mode, newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        if mode == "w":
            writer.writerow(HEADERS)

        while True:
            params = {"order": "createdAt", "ascending": "true", "limit": batch_size}
            if after_cursor:
                params["after_cursor"] = after_cursor

            print(f"  Fetching batch (cursor={after_cursor!r})...")
            resp = _request_with_retry(session, base_url, params=params)
            if resp is None:
                print("  Giving up after repeated failures.")
                break
            if resp.status_code != 200:
                print(f"  API error {resp.status_code}: {resp.text[:300]}")
                time.sleep(3)
                continue

            try:
                payload = resp.json()
            except Exception as e:
                print(f"  JSON parse error: {e}")
                time.sleep(3)
                continue

            markets = payload.get("markets", [])
            next_cursor = payload.get("next_cursor")

            if not markets:
                print("  No markets returned. Done with phase 1.")
                _clear_cursor(csv_filename)
                break

            for market in markets:
                try:
                    writer.writerow(_build_row(market))
                    total_fetched += 1
                except Exception as e:
                    print(
                        f"  Error building row for market {market.get('id', '?')}: {e}"
                    )

            csvfile.flush()
            if next_cursor:
                _save_cursor(csv_filename, next_cursor)

            print(f"  +{len(markets)} markets (total new: {total_fetched})")

            if not next_cursor:
                print("  No next_cursor — reached end.")
                _clear_cursor(csv_filename)
                break
            after_cursor = next_cursor

    session.close()
    print(f"Phase 1 done. Wrote {total_fetched} new rows.")


def _phase2_repair(csv_filename: str, what: str):
    """
    Phase 2/3: stream the CSV, identify rows needing repair, re-fetch by id, rewrite.

    what = "critical"  -> rows missing outcomes/clobTokenIds/closedTime
    what = "tags"      -> rows with empty tags column
    """
    if not os.path.isfile(csv_filename):
        print(f"  {csv_filename} not found, skipping.")
        return

    # First pass: scan for rows that need repair, build set of row indices + ids
    print(
        f"Phase {'2' if what == 'critical' else '3'}: scanning for rows needing repair ({what})..."
    )
    to_repair = {}  # row_index -> market_id
    with open(csv_filename, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for idx, row in enumerate(reader):
            if not row:
                continue
            if what == "critical":
                if _row_is_missing_critical(row):
                    mid = row[COL_ID]
                    if mid:
                        to_repair[idx] = mid
            elif what == "tags":
                if _is_blank(row[COL_TAGS]):
                    mid = row[COL_ID]
                    if mid:
                        to_repair[idx] = mid

    if not to_repair:
        print(f"  Nothing to repair for {what}. Skipping.")
        return

    print(f"  {len(to_repair):,} rows need repair. Fetching individually...")

    # Fetch all needed market objects
    session = requests.Session()
    fixed_rows = {}  # row_index -> new row
    bad_log = open(_bad_log_path(csv_filename), "a", encoding="utf-8")
    repaired_count = 0
    still_bad_count = 0
    not_found_count = 0

    try:
        for n, (idx, mid) in enumerate(to_repair.items(), start=1):
            market = _fetch_market_by_id(session, mid)
            if market is None:
                not_found_count += 1
                bad_log.write(f"{mid}\tnot_found_or_failed\t{what}\n")
                continue
            new_row = _build_row(market)
            fixed_rows[idx] = new_row

            still_missing = (
                _row_is_missing_critical(new_row) if what == "critical" else []
            )
            if still_missing or (what == "tags" and _is_blank(new_row[COL_TAGS])):
                still_bad_count += 1
                fields = ",".join(still_missing) if still_missing else "tags"
                bad_log.write(f"{mid}\tstill_missing\t{fields}\n")
            else:
                repaired_count += 1

            if n % 100 == 0:
                print(
                    f"    {n:,}/{len(to_repair):,} re-fetched (repaired: {repaired_count}, still bad: {still_bad_count}, 404: {not_found_count})"
                )
    finally:
        session.close()
        bad_log.close()

    print(
        f"  Re-fetch done. Repaired: {repaired_count}, still bad: {still_bad_count}, not found/failed: {not_found_count}"
    )

    # Rewrite CSV streaming, swapping in fixed rows
    print(f"  Rewriting {csv_filename} with repaired rows...")
    tmp_path = csv_filename + ".tmp"
    with open(csv_filename, "r", encoding="utf-8") as fin, open(
        tmp_path, "w", newline="", encoding="utf-8"
    ) as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        header = next(reader, None)
        if header:
            writer.writerow(header)
        for idx, row in enumerate(reader):
            if idx in fixed_rows:
                writer.writerow(fixed_rows[idx])
            else:
                writer.writerow(row)
    os.replace(tmp_path, csv_filename)
    print(f"  Rewrite complete.")


def update_markets(csv_filename: str = "markets_v2.csv", batch_size: int = 500):
    """
    Three-pass market scrape:
      Phase 1 — keyset paginate /markets/keyset, write rows.
      Phase 2 — re-fetch any row missing outcomes/clobTokenIds/closedTime via /markets/{id}.
      Phase 3 — re-fetch any row with empty tags via /markets/{id}?include_tag=true.

    Resume:
      - Phase 1 resumes from <csv>.cursor sidecar.
      - Phases 2 & 3 are idempotent: they scan the CSV and repair what's still bad.

    Bad rows that can't be repaired are appended to <csv>.bad_rows.log.
    """
    print("=" * 60)
    print(f"update_markets -> {csv_filename}")
    print("=" * 60)
    _phase1_keyset_fetch(csv_filename, batch_size)
    _phase2_repair(csv_filename, what="critical")
    _phase2_repair(csv_filename, what="tags")
    print("\nAll phases complete.")
    print(f"  Data:    {csv_filename}")
    bp = _bad_log_path(csv_filename)
    if os.path.isfile(bp):
        print(f"  Bad log: {bp}")


# if __name__ == "__main__":
#     update_markets()
