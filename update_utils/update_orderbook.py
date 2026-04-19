import os
import json
import gzip
import signal
import time
import csv
import threading
import requests
import zstandard as zstd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

EARLIEST_TS = 1762984233541
EARLIEST_DATE = datetime(2025, 11, 12, 21, 50, 33, tzinfo=timezone.utc)
NOW_TS = int(time.time() * 1000)

CLOB_BASE = "https://clob.polymarket.com"
DATA_DIR = "orderbook_snapshots"
PROGRESS_FILE = os.path.join(DATA_DIR, "progress.json")
_active_progress_file = PROGRESS_FILE
PAGE_SIZE = 1000

BIG_MARKET_THRESHOLD = 999999999
CHUNKS_PER_BIG_MARKET = 8

ZSTD_LEVEL = 3  # good balance of speed/ratio

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


def load_progress(path=None):
    path = path or _active_progress_file
    if os.path.isfile(path):
        with open(path, "r") as f:
            return json.load(f)
    return {}


def save_progress(progress, path=None):
    path = path or _active_progress_file
    with progress_lock:
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(progress, f)
        os.replace(tmp, path)


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
    data = fetch_page(session, asset_id, start_ts, market_id)
    if data is None:
        return 0
    return data.get("count", 0)


def write_snapshots_zst(zst_path, snapshots):
    """Append snapshots as a new zstd frame. Multi-frame zst files are valid."""
    cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
    data = "".join(json.dumps(snap, separators=(",", ":")) + "\n" for snap in snapshots)
    compressed = cctx.compress(data.encode("utf-8"))
    with open(zst_path, "ab") as f:
        f.write(compressed)


def fetch_chunk(session, asset_id, start_ts, end_ts, market_id, chunk_file):
    """Fetch a time range chunk. Writes compressed."""
    total = 0
    page = 0
    cursor = start_ts
    prev_cursor = None
    written_hashes = set()

    while True:
        data = fetch_page(session, asset_id, cursor, market_id, end_ts=end_ts)
        if data is None:
            break

        snapshots = data.get("data", [])
        if not snapshots:
            break

        new_cursor = int(snapshots[-1]["timestamp"])

        unique = []
        for snap in snapshots:
            h = snap["hash"]
            if h not in written_hashes:
                written_hashes.add(h)
                unique.append(snap)

        if not unique:
            # No new data — if cursor isn't moving we're done
            if new_cursor == cursor:
                break
            prev_cursor = cursor
            cursor = new_cursor
            if len(snapshots) < PAGE_SIZE:
                break
            continue

        write_snapshots_zst(chunk_file, unique)
        total += len(unique)
        page += 1
        prev_cursor = cursor
        cursor = new_cursor

        if len(snapshots) < PAGE_SIZE:
            break

    return (
        total,
        cursor,
        written_hashes and max(written_hashes, key=lambda h: h) or None,
    )


def fetch_chunk(session, asset_id, start_ts, end_ts, market_id, chunk_file):
    """Fetch a time range chunk. Writes compressed."""
    total = 0
    cursor = start_ts
    written_hashes = set()
    last_hash = None

    while True:
        data = fetch_page(session, asset_id, cursor, market_id, end_ts=end_ts)
        if data is None:
            break

        snapshots = data.get("data", [])
        if not snapshots:
            break

        new_cursor = int(snapshots[-1]["timestamp"])

        unique = []
        for snap in snapshots:
            h = snap["hash"]
            if h not in written_hashes:
                written_hashes.add(h)
                unique.append(snap)

        if unique:
            write_snapshots_zst(chunk_file, unique)
            total += len(unique)
            last_hash = unique[-1]["hash"]

        # If cursor didn't advance, we're stuck — stop
        if new_cursor == cursor:
            break

        cursor = new_cursor

        if len(snapshots) < PAGE_SIZE:
            break

    return total, cursor, last_hash


def get_out_path(market_id):
    """Return the .jsonl.zst path. Auto-migrates legacy .jsonl and .jsonl.gz files."""
    zst_path = os.path.join(DATA_DIR, "data", f"{market_id}.jsonl.zst")
    gz_path = os.path.join(DATA_DIR, "data", f"{market_id}.jsonl.gz")
    legacy_path = os.path.join(DATA_DIR, "data", f"{market_id}.jsonl")

    # Already zst — done
    if os.path.isfile(zst_path):
        return zst_path

    # Migrate .jsonl -> .zst
    if os.path.isfile(legacy_path):
        try:
            cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
            with open(legacy_path, "rb") as f_in, open(zst_path, "wb") as f_out:
                cctx.copy_stream(f_in, f_out)
            os.remove(legacy_path)
            tprint(f"    Market {market_id}: migrated .jsonl -> .jsonl.zst")
        except Exception as e:
            tprint(f"    Market {market_id}: .jsonl migration failed: {e}")
            return legacy_path

    # Migrate .gz -> .zst
    if os.path.isfile(gz_path):
        try:
            cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
            with gzip.open(gz_path, "rb") as f_in, open(zst_path, "wb") as f_out:
                cctx.copy_stream(f_in, f_out)
            os.remove(gz_path)
            tprint(f"    Market {market_id}: migrated .jsonl.gz -> .jsonl.zst")
        except Exception as e:
            tprint(f"    Market {market_id}: .gz migration failed: {e}")
            return gz_path

    return zst_path


def fetch_market_orderbook(session, market, progress):
    market_id = market["id"]
    asset_id = market["token1"]

    with progress_lock:
        state = progress.get(asset_id, {})

    if state.get("complete"):
        return 0

    start_ts = state.get("last_ts", EARLIEST_TS)
    last_hash = state.get("last_hash")

    out_file = get_out_path(market_id)

    # Probe to get count
    count = probe_market(session, asset_id, start_ts, market_id)

    if count == 0:
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
        page = 0
        cursor = start_ts
        # Track all hashes written this session so dedup works across pages,
        # not just against the single last-written hash.
        written_hashes = set()
        if last_hash:
            written_hashes.add(last_hash)
        last_written_hash = last_hash

        while True:
            data = fetch_page(session, asset_id, cursor, market_id)
            if data is None:
                break

            snapshots = data.get("data", [])
            if not snapshots:
                break

            new_cursor = int(snapshots[-1]["timestamp"])

            unique = []
            for snap in snapshots:
                h = snap["hash"]
                if h not in written_hashes:
                    written_hashes.add(h)
                    unique.append(snap)

            if not unique:
                # No new records on this page. If cursor isn't advancing, we're done.
                if new_cursor == cursor:
                    break
                cursor = new_cursor
                if len(snapshots) < PAGE_SIZE:
                    break
                continue

            write_snapshots_zst(out_file, unique)
            total_new += len(unique)
            page += 1
            last_written_hash = unique[-1]["hash"]

            # If cursor didn't advance, stop — fetching again would return the same data.
            if new_cursor == cursor:
                break

            cursor = new_cursor

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
                chunk_file = os.path.join(chunk_dir, f"chunk_{i}.jsonl.zst")
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

        # Merge chunks in order (zst frames concatenate validly)
        final_ts = start_ts
        final_hash = last_hash

        for i in range(num_chunks):
            chunk_file = os.path.join(chunk_dir, f"chunk_{i}.jsonl.zst")
            if not os.path.isfile(chunk_file):
                continue

            with open(chunk_file, "rb") as cf, open(out_file, "ab") as of:
                while True:
                    block = cf.read(8 * 1024 * 1024)
                    if not block:
                        break
                    of.write(block)

            if chunk_results[i]:
                chunk_total, chunk_last_ts, chunk_last_hash = chunk_results[i]
                total_new += chunk_total
                if chunk_last_ts > final_ts:
                    final_ts = chunk_last_ts
                    final_hash = chunk_last_hash

            os.remove(chunk_file)

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


def update_orderbook(
    csv_file="markets.csv", num_workers=200, market_ids_file=None, progress_file=None
):
    if progress_file:
        global _active_progress_file
        _active_progress_file = progress_file

    print("=" * 60, flush=True)
    print("📖 Orderbook History Snapshots", flush=True)
    print(f"   Progress: {_active_progress_file}", flush=True)
    print("=" * 60, flush=True)

    os.makedirs(os.path.join(DATA_DIR, "data"), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "chunks"), exist_ok=True)

    print(f"Loading markets from {csv_file}...", flush=True)
    all_markets = load_markets(csv_file)
    print(f"  Total markets with token1: {len(all_markets):,}", flush=True)

    if market_ids_file:
        with open(market_ids_file, "r") as f:
            allowed_ids = set(line.strip() for line in f if line.strip())
        all_markets = [m for m in all_markets if m["id"] in allowed_ids]
        print(
            f"  Filtered to {len(all_markets):,} markets from {market_ids_file}",
            flush=True,
        )

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


def migrate_gz_to_zst(num_workers=8):
    """Convert all .jsonl.gz files to .jsonl.zst. Parallelized."""
    data_dir = os.path.join(DATA_DIR, "data")
    gz_files = [f for f in os.listdir(data_dir) if f.endswith(".jsonl.gz")]

    if not gz_files:
        print("No .gz files to migrate.", flush=True)
        return

    print(
        f"Migrating {len(gz_files):,} .gz files to .zst with {num_workers} workers...",
        flush=True,
    )

    saved = [0]
    converted = [0]
    errors = [0]
    lock = threading.Lock()

    def convert(fname):
        gz_path = os.path.join(data_dir, fname)
        zst_path = os.path.join(data_dir, fname.replace(".jsonl.gz", ".jsonl.zst"))
        try:
            cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
            orig = os.path.getsize(gz_path)
            with gzip.open(gz_path, "rb") as f_in, open(zst_path, "wb") as f_out:
                cctx.copy_stream(f_in, f_out)
            new_size = os.path.getsize(zst_path)
            os.remove(gz_path)
            with lock:
                saved[0] += orig - new_size
                converted[0] += 1
                if converted[0] % 500 == 0:
                    tprint(
                        f"  {converted[0]:,}/{len(gz_files):,} converted, saved {saved[0] / 1024**3:.1f} GB..."
                    )
        except Exception as e:
            with lock:
                errors[0] += 1
                if errors[0] <= 10:
                    tprint(f"  Failed {fname}: {e}")

    with ThreadPoolExecutor(max_workers=num_workers) as ex:
        list(ex.map(convert, gz_files))

    print(
        f"Done. Converted {converted[0]:,} files, saved {saved[0] / 1024**3:.1f} GB, {errors[0]} errors",
        flush=True,
    )


def migrate_gz_to_zst_from_dir(source_dir, num_workers=8):
    """Convert .gz files from an external directory (e.g. /home/william/orderbook_gz) to .zst in data dir."""
    data_dir = os.path.join(DATA_DIR, "data")
    gz_files = [f for f in os.listdir(source_dir) if f.endswith(".jsonl.gz")]

    if not gz_files:
        print(f"No .gz files in {source_dir}.", flush=True)
        return

    print(
        f"Migrating {len(gz_files):,} .gz files from {source_dir} to .zst in {data_dir}...",
        flush=True,
    )

    converted = [0]
    errors = [0]
    lock = threading.Lock()

    def convert(fname):
        gz_path = os.path.join(source_dir, fname)
        zst_path = os.path.join(data_dir, fname.replace(".jsonl.gz", ".jsonl.zst"))
        if os.path.isfile(zst_path):
            os.remove(gz_path)
            with lock:
                converted[0] += 1
            return
        try:
            cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
            with gzip.open(gz_path, "rb") as f_in, open(zst_path, "wb") as f_out:
                cctx.copy_stream(f_in, f_out)
            os.remove(gz_path)
            with lock:
                converted[0] += 1
                if converted[0] % 500 == 0:
                    tprint(f"  {converted[0]:,}/{len(gz_files):,} converted...")
        except Exception as e:
            with lock:
                errors[0] += 1
                if errors[0] <= 10:
                    tprint(f"  Failed {fname}: {e}")

    with ThreadPoolExecutor(max_workers=num_workers) as ex:
        list(ex.map(convert, gz_files))

    print(f"Done. Converted {converted[0]:,} files, {errors[0]} errors", flush=True)


def rebuild_progress():
    """Rebuild progress.json from actual data files on disk."""
    import subprocess

    data_dir = os.path.join(DATA_DIR, "data")

    progress = {}
    if os.path.isfile(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)

    known_markets = {
        v.get("market_id"): k for k, v in progress.items() if v.get("market_id")
    }

    files = os.listdir(data_dir) if os.path.isdir(data_dir) else []
    print(f"Data files on disk: {len(files):,}", flush=True)
    print(f"Already in progress: {len(progress):,}", flush=True)

    missing = 0
    errors = 0
    for fname in files:
        market_id = (
            fname.replace(".jsonl.zst", "")
            .replace(".jsonl.gz", "")
            .replace(".jsonl", "")
        )
        if market_id in known_markets:
            continue

        fpath = os.path.join(data_dir, fname)
        last_line = None
        try:
            if fname.endswith(".zst"):
                dctx = zstd.ZstdDecompressor()
                with open(fpath, "rb") as f:
                    reader = dctx.stream_reader(f)
                    text = reader.read().decode("utf-8")
                    lines = text.strip().split("\n")
                    last_line = lines[-1] if lines else None
            elif fname.endswith(".gz"):
                result = subprocess.run(
                    ["bash", "-c", f"zcat '{fpath}' | tail -1"],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                last_line = result.stdout.strip()
            else:
                result = subprocess.run(
                    ["tail", "-1", fpath], capture_output=True, text=True, timeout=5
                )
                last_line = result.stdout.strip()

            if not last_line:
                continue

            snap = json.loads(last_line)
            asset_id = snap.get("asset_id", market_id)
            is_compressed = fname.endswith(".zst") or fname.endswith(".gz")

            progress[asset_id] = {
                "last_ts": int(snap["timestamp"]),
                "last_hash": snap.get("hash"),
                "complete": is_compressed,
                "market_id": market_id,
                "count": 0,
            }
            missing += 1
        except Exception as e:
            errors += 1
            if errors <= 20:
                print(f"  Error on {fname}: {e}", flush=True)

        if missing % 1000 == 0 and missing > 0:
            print(f"  Rebuilt {missing:,} entries ({errors} errors)...", flush=True)

    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)

    complete = sum(1 for v in progress.values() if v.get("complete"))
    print(f"Rebuilt {missing:,} missing entries ({errors} errors)", flush=True)
    print(f"Total in progress: {len(progress):,} ({complete:,} complete)", flush=True)


if __name__ == "__main__":
    update_orderbook()
