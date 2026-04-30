import requests
import csv
import json
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict


GAMMA_BASE = "https://gamma-api.polymarket.com"
REPAIR_WORKERS = 32


def count_csv_lines(csv_filename: str) -> int:
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


def _bad_log_path(csv_filename: str) -> str:
    return csv_filename + ".bad_rows.log"


def _is_blank(v) -> bool:
    return v is None or v == ""


def _parse_str_list(v):
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
    tags = market.get("tags") or []
    labels = []
    for t in tags:
        label = t.get("label") or t.get("slug")
        if label:
            labels.append(label)
    return "|".join(labels)


def _build_row(market: dict) -> List:
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


COL_ID = 1
COL_ANSWER1 = 3
COL_ANSWER2 = 4
COL_TOKEN1 = 7
COL_TOKEN2 = 8
COL_CLOSED_TIME = 13
COL_TAGS = 14


def _row_is_missing_critical(row: List) -> List[str]:
    missing = []
    if _is_blank(row[COL_ANSWER1]) and _is_blank(row[COL_ANSWER2]):
        missing.append("outcomes")
    if _is_blank(row[COL_TOKEN1]) and _is_blank(row[COL_TOKEN2]):
        missing.append("clobTokenIds")
    if _is_blank(row[COL_CLOSED_TIME]):
        missing.append("closedTime/endDate")
    return missing


def _request_with_retry(session, url, params=None, max_attempts=10):
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
    url = f"{GAMMA_BASE}/markets/{market_id}"
    resp = _request_with_retry(session, url, params={"include_tag": "true"})
    if resp is None:
        return None
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        return None
    try:
        return resp.json()
    except Exception:
        return None


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


def _phase1_subfetch(csv_filename: str, batch_size: int, closed_flag: bool):
    """One sub-pass of phase 1: fetches either all open or all closed markets."""
    base_url = f"{GAMMA_BASE}/markets/keyset"
    label = "closed" if closed_flag else "open"
    cursor_file = csv_filename + f".cursor.{label}"
    done_marker = csv_filename + f".done.{label}"

    if os.path.isfile(done_marker):
        print(f"Phase 1 [{label}]: already complete, skipping.")
        return

    after_cursor = None
    if os.path.isfile(cursor_file):
        with open(cursor_file) as f:
            after_cursor = f.read().strip() or None
        print(f"Phase 1 [{label}]: resuming from saved cursor.")
    else:
        print(f"Phase 1 [{label}]: starting fresh.")

    session = requests.Session()
    total_fetched = 0

    with open(csv_filename, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)

        while True:
            # NOTE: don't pass `order=createdAt` — server 500s when combined with `closed=true`.
            # Default sort (by id) is fine and gives full historical coverage.
            params = {"limit": batch_size, "closed": "true" if closed_flag else "false"}
            if after_cursor:
                params["after_cursor"] = after_cursor

            print(f"  [{label}] Fetching batch (cursor={after_cursor!r})...")
            resp = _request_with_retry(session, base_url, params=params)
            if resp is None:
                print(f"  [{label}] Giving up after request failures.")
                break
            if resp.status_code != 200:
                print(f"  [{label}] API error {resp.status_code}: {resp.text[:300]}")
                time.sleep(3)
                continue

            try:
                payload = resp.json()
            except Exception as e:
                print(f"  [{label}] JSON parse error: {e}")
                time.sleep(3)
                continue

            markets = payload.get("markets", [])
            next_cursor = payload.get("next_cursor")

            # Write whatever markets came back.
            for market in markets:
                try:
                    writer.writerow(_build_row(market))
                    total_fetched += 1
                except Exception as e:
                    print(
                        f"  [{label}] Error building row for market {market.get('id', '?')}: {e}"
                    )
            csvfile.flush()

            if markets:
                print(f"  [{label}] +{len(markets)} markets (total: {total_fetched})")

            # No next_cursor = last page per spec. Done.
            if not next_cursor:
                print(
                    f"  [{label}] No next_cursor — reached end. Sub-pass total: {total_fetched}"
                )
                if os.path.isfile(cursor_file):
                    os.remove(cursor_file)
                with open(done_marker, "w") as f:
                    f.write("done\n")
                break

            # Save cursor and advance.
            tmp = cursor_file + ".tmp"
            with open(tmp, "w") as f:
                f.write(next_cursor)
            os.replace(tmp, cursor_file)
            after_cursor = next_cursor

    session.close()


def _phase1_keyset_fetch(csv_filename: str, batch_size: int):
    """Phase 1: fetch all markets via two sub-passes (open + closed)."""
    file_exists = os.path.exists(csv_filename) and count_csv_lines(csv_filename) > 0

    if not file_exists:
        print(f"Phase 1: creating new CSV {csv_filename}")
        with open(csv_filename, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADERS)

    for closed_flag in (False, True):
        _phase1_subfetch(csv_filename, batch_size, closed_flag)


def _phase2_repair(csv_filename: str, what: str):
    """
    what = "critical"  -> rows missing outcomes/clobTokenIds/closedTime
    what = "tags"      -> rows with empty tags column
    """
    if not os.path.isfile(csv_filename):
        print(f"  {csv_filename} not found, skipping.")
        return

    phase_label = "2" if what == "critical" else "3"
    print(f"Phase {phase_label}: scanning for rows needing repair ({what})...")

    to_repair = {}
    with open(csv_filename, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
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
        print(f"  Nothing to repair for {what}.")
        return

    print(
        f"  {len(to_repair):,} rows need repair. Fetching with {REPAIR_WORKERS} workers..."
    )

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=REPAIR_WORKERS, pool_maxsize=REPAIR_WORKERS
    )
    session.mount("https://", adapter)

    fixed_rows = {}
    bad_log = open(_bad_log_path(csv_filename), "a", encoding="utf-8")
    counts = {"repaired": 0, "still_bad": 0, "not_found": 0, "done": 0}
    lock = threading.Lock()

    def fetch_one(idx_mid):
        idx, mid = idx_mid
        market = _fetch_market_by_id(session, mid)
        return idx, mid, market

    try:
        with ThreadPoolExecutor(max_workers=REPAIR_WORKERS) as ex:
            futures = [ex.submit(fetch_one, item) for item in to_repair.items()]
            for fut in as_completed(futures):
                idx, mid, market = fut.result()
                with lock:
                    counts["done"] += 1
                    if market is None:
                        counts["not_found"] += 1
                        bad_log.write(f"{mid}\tnot_found_or_failed\t{what}\n")
                    else:
                        new_row = _build_row(market)
                        fixed_rows[idx] = new_row
                        still_missing = (
                            _row_is_missing_critical(new_row)
                            if what == "critical"
                            else []
                        )
                        if still_missing or (
                            what == "tags" and _is_blank(new_row[COL_TAGS])
                        ):
                            counts["still_bad"] += 1
                            fields = (
                                ",".join(still_missing) if still_missing else "tags"
                            )
                            bad_log.write(f"{mid}\tstill_missing\t{fields}\n")
                        else:
                            counts["repaired"] += 1
                    if counts["done"] % 500 == 0:
                        print(
                            f"    {counts['done']:,}/{len(to_repair):,} done "
                            f"(repaired: {counts['repaired']}, still bad: {counts['still_bad']}, 404: {counts['not_found']})"
                        )
    finally:
        session.close()
        bad_log.close()

    print(
        f"  Re-fetch done. Repaired: {counts['repaired']}, "
        f"still bad: {counts['still_bad']}, not found/failed: {counts['not_found']}"
    )

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


def update_markets(csv_filename: str = "new_markets.csv", batch_size: int = 500):
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
