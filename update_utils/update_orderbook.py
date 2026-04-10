import os
import json
import gzip
import signal
import time
import csv
import threading
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

EARLIEST_TS = 1762984233541
EARLIEST_DATE = datetime(2025, 11, 12, 21, 50, 33, tzinfo=timezone.utc)
NOW_TS = int(time.time() * 1000)

CLOB_BASE = "https://clob.polymarket.com"
DATA_DIR = "orderbook_snapshots"
PROGRESS_FILE = os.path.join(DATA_DIR, "progress.json")
PAGE_SIZE = 1000

# Markets with more than this many snapshots get parallel chunk fetching
BIG_MARKET_THRESHOLD = 5000
CHUNKS_PER_BIG_MARKET = 8

print_lock = threading.Lock()
progress_lock = threading.Lock()


def tprint(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs, flush=True)


def handle_shutdown(signum, frame):
    tprint("\n⚠ Stopping. Progress saved, data safe. Re-run to continue.")
    os._exit(0)


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def load_progress():
    if os.path.isfile(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_progress(progress):
    with progress_lock:
        tmp = PROGRESS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(progress, f)
        os.replace(tmp, PROGRESS_FILE)


def load_markets(csv_file="markets.csv"):
    markets = []
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            token1 = row.get("token1", "").strip()
            if not token1:
                continue
            markets.append(
                {
                    "id": row.get("id", "").strip(),
                    "token1": token1,
                    "closedTime": row.get("closedTime", "").strip(),
                }
            )
    return markets


def is_closed(market):
    ct = market.get("closedTime", "")
    return bool(ct and ct.lower() not in ("", "none", "false"))


def closed_before_data(market):
    ct = market.get("closedTime", "")
    if not ct or ct.lower() in ("", "none", "false"):
        return False
    try:
        dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt < EARLIEST_DATE
    except (ValueError, TypeError):
        return False


def fetch_page(session, asset_id, start_ts, market_id, end_ts=None):
    params = f"asset_id={asset_id}&startTs={start_ts}&limit={PAGE_SIZE}"
    if end_ts is not None:
        params += f"&endTs={end_ts}"
    url = f"{CLOB_BASE}/orderbook-history?{params}"

    for attempt in range(1, 51):
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 429:
                wait = min(120, 10 * attempt)
                if attempt % 3 == 0:
                    tprint(
                        f"    Market {market_id}: 429, waiting {wait}s (attempt {attempt})"
                    )
                time.sleep(wait)
                continue
            if resp.status_code >= 500:
                wait = min(60, 5 * attempt)
                if attempt % 3 == 0:
                    tprint(
                        f"    Market {market_id}: {resp.status_code}, retrying in {wait}s (attempt {attempt})"
                    )
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.ConnectionError:
            wait = min(60, 3 * attempt)
            if attempt % 3 == 0:
                tprint(
                    f"    Market {market_id}: conn error, retrying in {wait}s (attempt {attempt})"
                )
            time.sleep(wait)
        except requests.exceptions.Timeout:
            wait = min(60, 5 * attempt)
            if attempt % 3 == 0:
                tprint(
                    f"    Market {market_id}: timeout, retrying in {wait}s (attempt {attempt})"
                )
            time.sleep(wait)
        except Exception as e:
            wait = min(60, 5 * attempt)
            tprint(
                f"    Market {market_id}: {e}, retrying in {wait}s (attempt {attempt})"
            )
            time.sleep(wait)

    tprint(f"    Market {market_id}: GAVE UP after 50 attempts")
    return None


def probe_market(session, asset_id, start_ts, market_id):
    """Quick probe to get total snapshot count."""
    data = fetch_page(session, asset_id, start_ts, market_id)
    if data is None:
        return 0
    return data.get("count", 0)


def fetch_chunk(session, asset_id, start_ts, end_ts, market_id, chunk_file):
    """Fetch a time range chunk, paginating sequentially. Writes to chunk_file."""
    total = 0
    page = 0
    cursor = start_ts
    last_hash = None

    while True:
        data = fetch_page(session, asset_id, cursor, market_id, end_ts=end_ts)
        if data is None:
            break

        snapshots = data.get("data", [])
        if not snapshots:
            break

        # Dedupe
        seen = set()
        unique = []
        for snap in snapshots:
            h = snap["hash"]
            if h not in seen:
                seen.add(h)
                unique.append(snap)

        if unique:
            mode = "a" if os.path.isfile(chunk_file) else "w"
            with open(chunk_file, mode) as f:
                for snap in unique:
                    f.write(json.dumps(snap, separators=(",", ":")) + "\n")

            total += len(unique)
            last_hash = unique[-1]["hash"]

        page += 1
        cursor = int(snapshots[-1]["timestamp"])

        if len(snapshots) < PAGE_SIZE:
            break

    return total, cursor, last_hash


def fetch_market_orderbook(session, market, progress):
    market_id = market["id"]
    asset_id = market["token1"]

    with progress_lock:
        state = progress.get(asset_id, {})

    if state.get("complete"):
        return 0

    start_ts = state.get("last_ts", EARLIEST_TS)
    last_hash = state.get("last_hash")

    out_file = os.path.join(DATA_DIR, "data", f"{market_id}.jsonl")
    gz_file = out_file + ".gz"

    if os.path.isfile(gz_file):
        return 0

    # Probe to get count
    count = probe_market(session, asset_id, start_ts, market_id)

    if count == 0:
        # No data
        closed = is_closed(market)
        with progress_lock:
            progress[asset_id] = {
                "last_ts": start_ts,
                "last_hash": last_hash,
                "complete": closed,
                "count": 0,
                "market_id": market_id,
            }
        save_progress(progress)
        return 0

    total_new = 0

    if count <= BIG_MARKET_THRESHOLD:
        # Small market: sequential fetch (same as before)
        page = 0
        mode = "a" if os.path.isfile(out_file) else "w"
        last_written_hash = last_hash
        cursor = start_ts

        while True:
            data = fetch_page(session, asset_id, cursor, market_id)
            if data is None:
                break

            snapshots = data.get("data", [])
            if not snapshots:
                break

            seen = set()
            unique = []
            skip_past = last_written_hash is not None
            found = False

            for snap in snapshots:
                h = snap["hash"]
                if skip_past:
                    if h == last_written_hash:
                        found = True
                        continue
                    elif not found:
                        continue
                    else:
                        skip_past = False
                if h not in seen:
                    seen.add(h)
                    unique.append(snap)

            if skip_past and not found and last_written_hash is not None:
                cursor = int(snapshots[-1]["timestamp"])
                last_written_hash = snapshots[-1]["hash"]
                continue

            last_written_hash = None

            if not unique:
                cursor = int(snapshots[-1]["timestamp"])
                continue

            with open(out_file, mode) as f:
                for snap in unique:
                    f.write(json.dumps(snap, separators=(",", ":")) + "\n")
            mode = "a"

            total_new += len(unique)
            page += 1
            cursor = int(snapshots[-1]["timestamp"])
            last_written_hash = unique[-1]["hash"]

            if page % 5 == 0:
                readable = datetime.fromtimestamp(
                    cursor / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S UTC")
                tprint(
                    f"    Market {market_id}: page {page}, {total_new:,} snapshots, last: {readable}"
                )

            if len(snapshots) < PAGE_SIZE:
                break

        final_hash = last_written_hash
        final_ts = cursor

    else:
        # Big market: parallel chunk fetching
        end_ts = NOW_TS
        time_range = end_ts - start_ts
        num_chunks = CHUNKS_PER_BIG_MARKET

        chunk_ranges = []
        for i in range(num_chunks):
            c_start = start_ts + (i * time_range // num_chunks)
            c_end = (
                start_ts + ((i + 1) * time_range // num_chunks)
                if i < num_chunks - 1
                else end_ts
            )
            chunk_ranges.append((c_start, c_end))

        chunk_dir = os.path.join(DATA_DIR, "chunks", market_id)
        os.makedirs(chunk_dir, exist_ok=True)

        tprint(
            f"    Market {market_id}: {count:,} snapshots, splitting into {num_chunks} parallel chunks"
        )

        chunk_results = [None] * num_chunks

        with ThreadPoolExecutor(max_workers=num_chunks) as ex:
            futures = {}
            for i, (c_start, c_end) in enumerate(chunk_ranges):
                chunk_file = os.path.join(chunk_dir, f"chunk_{i}.jsonl")
                future = ex.submit(
                    fetch_chunk,
                    session,
                    asset_id,
                    c_start,
                    c_end,
                    market_id,
                    chunk_file,
                )
                futures[future] = i

            for future in as_completed(futures):
                i = futures[future]
                try:
                    chunk_total, chunk_last_ts, chunk_last_hash = future.result()
                    chunk_results[i] = (chunk_total, chunk_last_ts, chunk_last_hash)
                except Exception as e:
                    tprint(f"    Market {market_id}: chunk {i} failed: {e}")
                    chunk_results[i] = (0, chunk_ranges[i][1], None)

        # Merge chunks in order
        mode = "a" if os.path.isfile(out_file) else "w"
        final_ts = start_ts
        final_hash = last_hash

        for i in range(num_chunks):
            chunk_file = os.path.join(chunk_dir, f"chunk_{i}.jsonl")
            if not os.path.isfile(chunk_file):
                continue

            with open(chunk_file, "r") as cf, open(out_file, mode) as of:
                for line in cf:
                    of.write(line)
            mode = "a"

            if chunk_results[i]:
                chunk_total, chunk_last_ts, chunk_last_hash = chunk_results[i]
                total_new += chunk_total
                if chunk_last_ts > final_ts:
                    final_ts = chunk_last_ts
                    final_hash = chunk_last_hash

            os.remove(chunk_file)

        # Clean up chunk dir
        try:
            os.rmdir(chunk_dir)
        except OSError:
            pass

    # Save progress
    closed = is_closed(market)
    with progress_lock:
        progress[asset_id] = {
            "last_ts": final_ts,
            "last_hash": final_hash,
            "complete": closed,
            "count": state.get("count", 0) + total_new,
            "market_id": market_id,
        }
    save_progress(progress)

    # Compress completed markets
    if closed and total_new > 0 and os.path.isfile(out_file):
        gz_file = out_file + ".gz"
        try:
            with open(out_file, "rb") as f_in, gzip.open(gz_file, "wb") as f_out:
                while True:
                    chunk = f_in.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    f_out.write(chunk)
            os.remove(out_file)
        except Exception as e:
            tprint(f"    Market {market_id}: compression failed: {e}")

    return total_new


def orderbook_worker(markets_chunk, worker_id, progress):
    session = requests.Session()
    total = 0
    done = 0
    skipped = 0
    with_data = 0
    start_time = time.time()
    tprint(f"  [Worker {worker_id}] Starting with {len(markets_chunk):,} markets...")

    for market in markets_chunk:
        new = fetch_market_orderbook(session, market, progress)
        total += new
        done += 1

        if new > 0:
            with_data += 1
            status = "CLOSED" if is_closed(market) else "ACTIVE"
            tprint(
                f"  [Worker {worker_id}] ✓ Market {market['id']}: {new:,} snapshots [{status}] | {done}/{len(markets_chunk)} ({with_data} with data, {skipped} skipped)"
            )
        else:
            skipped += 1
            if done == 1 or done % 10 == 0:
                elapsed = time.time() - start_time
                rate = done / elapsed if elapsed > 0 else 0
                eta_mins = (len(markets_chunk) - done) / rate / 60 if rate > 0 else 0
                tprint(
                    f"  [Worker {worker_id}] Progress: {done:,}/{len(markets_chunk):,} | {with_data} with data | {skipped} empty | {rate:.1f}/s | ETA: {eta_mins:.0f}min"
                )

    elapsed = time.time() - start_time
    session.close()
    tprint(
        f"  [Worker {worker_id}] Finished: {total:,} snapshots from {with_data} markets ({skipped} empty) in {elapsed / 60:.1f}min"
    )
    return total


def update_orderbook(csv_file="markets.csv", num_workers=200):
    print("=" * 60, flush=True)
    print("📖 Orderbook History Snapshots", flush=True)
    print("=" * 60, flush=True)

    os.makedirs(os.path.join(DATA_DIR, "data"), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "chunks"), exist_ok=True)

    print(f"Loading markets from {csv_file}...", flush=True)
    all_markets = load_markets(csv_file)
    print(f"  Total markets with token1: {len(all_markets):,}", flush=True)

    pre_filter = len(all_markets)
    all_markets = [m for m in all_markets if not closed_before_data(m)]
    skipped_old = pre_filter - len(all_markets)
    print(f"  Skipped (closed before Nov 12, 2025): {skipped_old:,}", flush=True)
    print(f"  Candidates: {len(all_markets):,}", flush=True)

    progress = load_progress()
    already_done = sum(
        1 for m in all_markets if progress.get(m["token1"], {}).get("complete")
    )
    print(f"  Already completed: {already_done:,}", flush=True)

    remaining = [
        m for m in all_markets if not progress.get(m["token1"], {}).get("complete")
    ]
    print(f"  Remaining: {len(remaining):,}", flush=True)

    if not remaining:
        print("✅ All markets fully scraped!")
        return

    closed = [m for m in remaining if is_closed(m)]
    active = [m for m in remaining if not is_closed(m)]
    ordered = closed + active
    print(f"  Closed: {len(closed):,} | Active: {len(active):,}", flush=True)
    print(
        f"  Big market threshold: {BIG_MARKET_THRESHOLD:,} snapshots -> {CHUNKS_PER_BIG_MARKET} parallel chunks",
        flush=True,
    )
    print(
        f"\nScraping with {num_workers} workers (Ctrl+C to stop safely)...", flush=True
    )

    chunks = [[] for _ in range(num_workers)]
    for i, market in enumerate(ordered):
        chunks[i % num_workers].append(market)

    grand_total = 0
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {}
        for i, chunk in enumerate(chunks):
            if not chunk:
                continue
            future = executor.submit(orderbook_worker, chunk, i, progress)
            futures[future] = i

        for future in as_completed(futures):
            worker_id = futures[future]
            try:
                total = future.result()
                grand_total += total
            except Exception as e:
                print(f"Worker {worker_id} failed: {e}")

    print(f"\n{'=' * 60}", flush=True)
    print(f"✅ Done! Total new snapshots: {grand_total:,}", flush=True)
    print(f"   Data: {DATA_DIR}/data/", flush=True)
    print(f"   Progress: {PROGRESS_FILE}", flush=True)
    print(f"{'=' * 60}", flush=True)


def compress_completed():
    progress = load_progress()
    data_dir = os.path.join(DATA_DIR, "data")
    compressed = 0
    saved_bytes = 0

    for asset_id, state in progress.items():
        if not state.get("complete"):
            continue
        market_id = state.get("market_id", "")
        if not market_id:
            continue
        jsonl = os.path.join(data_dir, f"{market_id}.jsonl")
        gz = jsonl + ".gz"
        if os.path.isfile(jsonl) and not os.path.isfile(gz):
            orig_size = os.path.getsize(jsonl)
            try:
                with open(jsonl, "rb") as f_in, gzip.open(gz, "wb") as f_out:
                    while True:
                        chunk = f_in.read(8 * 1024 * 1024)
                        if not chunk:
                            break
                        f_out.write(chunk)
                gz_size = os.path.getsize(gz)
                os.remove(jsonl)
                saved_bytes += orig_size - gz_size
                compressed += 1
                if compressed % 100 == 0:
                    print(
                        f"  Compressed {compressed} files, saved {saved_bytes / 1024**3:.1f} GB so far...",
                        flush=True,
                    )
            except Exception as e:
                print(f"  Failed to compress {market_id}: {e}", flush=True)

    print(
        f"Compressed {compressed} files, saved {saved_bytes / 1024**3:.1f} GB total",
        flush=True,
    )


if __name__ == "__main__":
    update_orderbook()
