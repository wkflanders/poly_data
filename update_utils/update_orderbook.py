import os
import json
import signal
import time
import csv
import threading
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Earliest available orderbook snapshot data (ms)
EARLIEST_TS = 1762984233541

CLOB_BASE = "https://clob.polymarket.com"
DATA_DIR = "orderbook_snapshots"
PROGRESS_FILE = os.path.join(DATA_DIR, "progress.json")

print_lock = threading.Lock()
progress_lock = threading.Lock()


def tprint(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs, flush=True)


def handle_shutdown(signum, frame):
    tprint("\n⚠ Stopping. Progress is saved, data is safe. Re-run to continue.")
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
    """Load markets from CSV, return list of dicts with id, token1, closedTime."""
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


def fetch_market_orderbook(session, market, progress):
    """Scrape all orderbook snapshots for a single market. Returns total new snapshots."""
    market_id = market["id"]
    asset_id = market["token1"]

    # Check progress
    with progress_lock:
        state = progress.get(asset_id, {})

    if state.get("complete"):
        return 0

    # Resume from last scraped timestamp or start from earliest
    start_ts = state.get("last_ts", EARLIEST_TS)

    out_file = os.path.join(DATA_DIR, "data", f"{market_id}.jsonl")
    total_new = 0
    page = 0
    mode = "a" if os.path.isfile(out_file) else "w"

    while True:
        url = f"{CLOB_BASE}/orderbook-history?asset_id={asset_id}&startTs={start_ts}"

        attempt = 0
        data = None
        while True:
            try:
                resp = session.get(url, timeout=30)
                if resp.status_code == 429:
                    wait = min(60, 5 * (attempt + 1))
                    time.sleep(wait)
                    attempt += 1
                    continue
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as e:
                attempt += 1
                wait = min(60, 2 * attempt)
                if attempt % 5 == 0:
                    tprint(f"    Market {market_id}: retry {attempt}, error: {e}")
                time.sleep(wait)

        snapshots = data.get("data", [])

        if not snapshots:
            break

        # Write snapshots to JSONL
        with open(out_file, mode) as f:
            for snap in snapshots:
                f.write(json.dumps(snap, separators=(",", ":")) + "\n")
        mode = "a"

        total_new += len(snapshots)
        page += 1

        # Advance past the last timestamp
        last_ts = int(snapshots[-1]["timestamp"])
        readable_time = datetime.fromtimestamp(
            last_ts / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
        start_ts = last_ts + 1

        # Log every page for markets with data
        if page % 5 == 0:
            tprint(
                f"    Market {market_id}: page {page}, {total_new:,} snapshots, last: {readable_time}"
            )

        # Save progress periodically
        if page % 10 == 0:
            with progress_lock:
                progress[asset_id] = {
                    "last_ts": start_ts,
                    "complete": False,
                    "count": state.get("count", 0) + total_new,
                }
            save_progress(progress)

        # If we got fewer than 100, we've reached the end
        if len(snapshots) < 100:
            break

    # Mark complete if market is closed (no more data will come)
    closed = is_closed(market)
    with progress_lock:
        progress[asset_id] = {
            "last_ts": start_ts,
            "complete": closed,
            "count": state.get("count", 0) + total_new,
            "market_id": market_id,
        }
    save_progress(progress)

    return total_new


def orderbook_worker(markets_chunk, worker_id, progress):
    """Worker that processes a list of markets sequentially."""
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
                    f"  [Worker {worker_id}] Progress: {done:,}/{len(markets_chunk):,} markets | {with_data} with data | {skipped} empty | {rate:.1f} markets/s | ETA: {eta_mins:.0f}min"
                )

    elapsed = time.time() - start_time
    session.close()
    tprint(
        f"  [Worker {worker_id}] Finished: {total:,} snapshots from {with_data} markets ({skipped} empty) in {elapsed / 60:.1f}min"
    )
    return total


def update_orderbook(csv_file="markets.csv", num_workers=8):
    """Main entry point."""
    print("=" * 60)
    print("📖 Scraping Orderbook History Snapshots")
    print("=" * 60)

    os.makedirs(os.path.join(DATA_DIR, "data"), exist_ok=True)

    # Load markets and progress
    print(f"Loading markets from {csv_file}...")
    all_markets = load_markets(csv_file)
    print(f"  Total markets with token1: {len(all_markets):,}")

    progress = load_progress()
    already_done = sum(
        1 for m in all_markets if progress.get(m["token1"], {}).get("complete")
    )
    print(f"  Already completed: {already_done:,}")

    # Filter out completed markets
    remaining = [
        m for m in all_markets if not progress.get(m["token1"], {}).get("complete")
    ]
    print(f"  Remaining: {len(remaining):,}")

    if not remaining:
        print("✅ All markets fully scraped!")
        return

    # Sort: closed markets first, then active
    closed = [m for m in remaining if is_closed(m)]
    active = [m for m in remaining if not is_closed(m)]
    ordered = closed + active
    print(f"  Closed markets to scrape: {len(closed):,}")
    print(f"  Active markets to scrape: {len(active):,}")
    print(f"\nScraping with {num_workers} workers (Ctrl+C to stop safely)...")

    # Split markets across workers
    chunks = [[] for _ in range(num_workers)]
    for i, market in enumerate(ordered):
        chunks[i % num_workers].append(market)

    # Run workers
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

    print(f"\n{'=' * 60}")
    print(f"✅ Done! Total new snapshots: {grand_total:,}")
    print(f"   Data: {DATA_DIR}/data/")
    print(f"   Progress: {PROGRESS_FILE}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    update_orderbook()
