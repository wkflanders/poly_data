import os
import json
import signal
import pandas as pd
from flatten_json import flatten
from datetime import datetime, timezone
import subprocess
import time
import requests as req
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Global runtime timestamp - set once when program starts
RUNTIME_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

QUERY_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"

# Columns to save
COLUMNS_TO_SAVE = [
    "timestamp",
    "maker",
    "makerAssetId",
    "makerAmountFilled",
    "taker",
    "takerAssetId",
    "takerAmountFilled",
    "transactionHash",
]

if not os.path.isdir("goldsky"):
    os.mkdir("goldsky")

CURSOR_FILE = "goldsky/cursor_state.json"

# Shared shutdown flag - workers check this to stop cleanly
shutdown_event = threading.Event()

# Lock for printing
print_lock = threading.Lock()


def tprint(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)


def handle_shutdown(signum, frame):
    tprint("\n⚠ Stopping. Data is safe in goldsky/temp_chunks/")
    os._exit(0)


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def save_cursor(timestamp, last_id, sticky_timestamp=None):
    """Save cursor state to file for efficient resume."""
    state = {
        "last_timestamp": timestamp,
        "last_id": last_id,
        "sticky_timestamp": sticky_timestamp,
    }
    with open(CURSOR_FILE, "w") as f:
        json.dump(state, f)


def get_latest_cursor():
    """Get the latest cursor state for efficient resume.
    Returns (timestamp, last_id, sticky_timestamp) tuple."""
    if os.path.isfile(CURSOR_FILE):
        try:
            with open(CURSOR_FILE, "r") as f:
                state = json.load(f)
            timestamp = state.get("last_timestamp", 0)
            last_id = state.get("last_id")
            sticky_timestamp = state.get("sticky_timestamp")

            if sticky_timestamp is not None and last_id is None:
                print(
                    f"Warning: Invalid cursor state (sticky_timestamp={sticky_timestamp} but last_id=None), clearing sticky state"
                )
                sticky_timestamp = None

            if timestamp > 0:
                readable_time = datetime.fromtimestamp(
                    timestamp, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S UTC")
                print(
                    f"Resuming from cursor state: timestamp {timestamp} ({readable_time}), id: {last_id}, sticky: {sticky_timestamp}"
                )
                return timestamp, last_id, sticky_timestamp
        except Exception as e:
            print(f"Error reading cursor file: {e}")

    cache_file = "goldsky/orderFilled.csv"

    if not os.path.isfile(cache_file):
        print("No existing file found, starting from beginning of time (timestamp 0)")
        return 0, None, None

    try:
        result = subprocess.run(
            ["tail", "-n", "1", cache_file], capture_output=True, text=True, check=True
        )
        last_line = result.stdout.strip()
        if last_line:
            header_result = subprocess.run(
                ["head", "-n", "1", cache_file],
                capture_output=True,
                text=True,
                check=True,
            )
            headers = header_result.stdout.strip().split(",")

            if "timestamp" in headers:
                timestamp_index = headers.index("timestamp")
                values = last_line.split(",")
                if len(values) > timestamp_index:
                    last_timestamp = int(values[timestamp_index])
                    readable_time = datetime.fromtimestamp(
                        last_timestamp, tz=timezone.utc
                    ).strftime("%Y-%m-%d %H:%M:%S UTC")
                    print(
                        f"Resuming from CSV (no cursor file): timestamp {last_timestamp} ({readable_time})"
                    )
                    return last_timestamp - 1, None, None
    except Exception as e:
        print(f"Error reading latest file with tail: {e}")
        try:
            df = pd.read_csv(cache_file)
            if len(df) > 0 and "timestamp" in df.columns:
                last_timestamp = df.iloc[-1]["timestamp"]
                readable_time = datetime.fromtimestamp(
                    int(last_timestamp), tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S UTC")
                print(
                    f"Resuming from CSV (no cursor file): timestamp {last_timestamp} ({readable_time})"
                )
                return int(last_timestamp) - 1, None, None
        except Exception as e2:
            print(f"Error reading with pandas: {e2}")

    print("Falling back to beginning of time (timestamp 0)")
    return 0, None, None


def query_goldsky(session, where_clause, at_once=1000):
    """Execute a single GraphQL query. Retries forever with backoff until shutdown."""
    q_string = (
        """query MyQuery {
                    orderFilledEvents(orderBy: timestamp, orderDirection: asc
                                         first: """
        + str(at_once)
        + """
                                         where: {"""
        + where_clause
        + """}) {
                        fee
                        id
                        maker
                        makerAmountFilled
                        makerAssetId
                        orderHash
                        taker
                        takerAmountFilled
                        takerAssetId
                        timestamp
                        transactionHash
                    }
                }
            """
    )

    attempt = 0
    while not shutdown_event.is_set():
        try:
            resp = session.post(QUERY_URL, json={"query": q_string}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                raise Exception(f"GraphQL errors: {data['errors']}")
            return data["data"]["orderFilledEvents"]
        except Exception as e:
            attempt += 1
            wait = min(60, 2 * attempt)  # backoff up to 60s
            if attempt % 5 == 0:
                tprint(
                    f"    Query failing (attempt {attempt}): {e}. Retrying in {wait}s..."
                )
            time.sleep(wait)

    return []  # shutdown requested


def process_batch(events):
    """Flatten events into a DataFrame."""
    df = pd.DataFrame([flatten(x) for x in events]).reset_index(drop=True)
    df = df.sort_values(["timestamp", "id"], ascending=True).reset_index(drop=True)
    return df


def get_latest_remote_timestamp(session):
    """Query the subgraph for the most recent timestamp available."""
    q = "{ orderFilledEvents(first: 1, orderBy: timestamp, orderDirection: desc) { timestamp } }"
    try:
        resp = session.post(QUERY_URL, json={"query": q}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        events = data["data"]["orderFilledEvents"]
        if events:
            return int(events[0]["timestamp"])
    except Exception as e:
        print(f"Error fetching latest remote timestamp: {e}")
    return None


def scrape_range(start_ts, end_ts, worker_id, temp_dir, at_once=1000):
    """Scrape a specific time range. Writes directly to a temp file."""
    session = req.Session()
    session.headers.update({"Content-Type": "application/json"})

    temp_file = os.path.join(temp_dir, f"worker_{worker_id}.csv")
    file_started = False

    last_timestamp = start_ts
    last_id = None
    sticky_timestamp = None
    total_records = 0
    batch_count = 0

    while not shutdown_event.is_set():
        if sticky_timestamp is not None:
            where_clause = f'timestamp: "{sticky_timestamp}", id_gt: "{last_id}"'
        else:
            where_clause = (
                f'timestamp_gt: "{last_timestamp}", timestamp_lte: "{end_ts}"'
            )

        events = query_goldsky(session, where_clause, at_once)

        if shutdown_event.is_set():
            break

        if not events:
            if sticky_timestamp is not None:
                last_timestamp = sticky_timestamp
                sticky_timestamp = None
                last_id = None
                continue
            break

        df = process_batch(events)

        batch_last_timestamp = int(df.iloc[-1]["timestamp"])
        batch_last_id = df.iloc[-1]["id"]

        readable_time = datetime.fromtimestamp(
            batch_last_timestamp, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")

        should_break = False
        if len(df) >= at_once:
            sticky_timestamp = batch_last_timestamp
            last_id = batch_last_id
            tag = "STICKY"
        else:
            if sticky_timestamp is not None:
                last_timestamp = sticky_timestamp
                sticky_timestamp = None
                last_id = None
                tag = "STICKY COMPLETE"
            else:
                last_timestamp = batch_last_timestamp
                tag = ""
                should_break = True

        batch_count += 1

        df = df.drop_duplicates(subset=["id"])
        df_to_save = df[COLUMNS_TO_SAVE].copy()
        total_records += len(df_to_save)

        # Write immediately to temp file
        if not file_started:
            df_to_save.to_csv(temp_file, index=None)
            file_started = True
        else:
            df_to_save.to_csv(temp_file, index=None, mode="a", header=False)

        if batch_count % 50 == 0:
            tprint(
                f"  [Worker {worker_id}] Batch {batch_count}: {readable_time}, {total_records:,} records {f'[{tag}]' if tag else ''}"
            )

        if should_break:
            break

    session.close()
    tprint(
        f"  [Worker {worker_id}] Done: {total_records:,} records in {batch_count} batches"
    )
    return total_records


def scrape(at_once=1000, num_workers=16):
    print(f"Query URL: {QUERY_URL}")
    print(f"Runtime timestamp: {RUNTIME_TIMESTAMP}")
    print(f"Workers: {num_workers}")

    last_timestamp, last_id, sticky_timestamp = get_latest_cursor()

    output_file = "goldsky/orderFilled.csv"
    temp_dir = "goldsky/temp_chunks"

    # Clean up any leftover temp files from a killed run
    if os.path.isdir(temp_dir):
        leftover = [f for f in os.listdir(temp_dir) if f.endswith(".csv")]
        if leftover:
            print(
                f"⚠ Cleaning up {len(leftover)} leftover temp files from previous run"
            )
            for f in leftover:
                os.remove(os.path.join(temp_dir, f))

    os.makedirs(temp_dir, exist_ok=True)

    print(f"Output file: {output_file}")
    print(f"Temp dir: {temp_dir}")
    print(f"Saving columns: {COLUMNS_TO_SAVE}")

    # Drain sticky cursor first (single-threaded)
    session = req.Session()
    session.headers.update({"Content-Type": "application/json"})

    if sticky_timestamp is not None:
        print(f"\nDraining sticky cursor at timestamp {sticky_timestamp}...")
        while not shutdown_event.is_set():
            where_clause = f'timestamp: "{sticky_timestamp}", id_gt: "{last_id}"'
            events = query_goldsky(session, where_clause, at_once)

            if shutdown_event.is_set():
                break

            if not events:
                last_timestamp = sticky_timestamp
                sticky_timestamp = None
                last_id = None
                print(
                    f"Sticky cursor drained, advancing past timestamp {last_timestamp}"
                )
                break

            df = process_batch(events)
            df = df.drop_duplicates(subset=["id"])
            df_to_save = df[COLUMNS_TO_SAVE].copy()

            if os.path.isfile(output_file):
                df_to_save.to_csv(output_file, index=None, mode="a", header=None)
            else:
                df_to_save.to_csv(output_file, index=None)

            last_id = df.iloc[-1]["id"]
            print(f"  Sticky batch: {len(df)} records")

            if len(df) < at_once:
                last_timestamp = sticky_timestamp
                sticky_timestamp = None
                last_id = None
                print(
                    f"Sticky cursor drained, advancing past timestamp {last_timestamp}"
                )
                break

        save_cursor(last_timestamp, last_id, sticky_timestamp)

    if shutdown_event.is_set():
        print("Shutdown before scraping started. Exiting.")
        session.close()
        return

    # Get latest remote timestamp
    print("\nChecking latest available data...")
    remote_latest = get_latest_remote_timestamp(session)
    session.close()

    if remote_latest is None:
        print(
            "Could not determine latest remote timestamp. Falling back to single-threaded scrape."
        )
        num_workers = 1
        remote_latest = int(time.time())

    readable_start = datetime.fromtimestamp(last_timestamp, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    readable_end = datetime.fromtimestamp(remote_latest, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    gap = remote_latest - last_timestamp

    print(f"Time range to scrape: {readable_start} -> {readable_end}")
    print(f"Gap: {gap:,} seconds (~{gap // 86400} days)")

    if gap <= 0:
        print("Already up to date!")
        if os.path.isfile(CURSOR_FILE):
            os.remove(CURSOR_FILE)
        return

    effective_workers = min(num_workers, max(1, gap // 3600))
    chunk_size = gap // effective_workers

    ranges = []
    for i in range(effective_workers):
        chunk_start = last_timestamp + (i * chunk_size)
        chunk_end = (
            last_timestamp + ((i + 1) * chunk_size)
            if i < effective_workers - 1
            else remote_latest
        )
        ranges.append((chunk_start, chunk_end))

    print(
        f"\nScraping with {effective_workers} parallel workers (Ctrl+C to stop safely)..."
    )
    for i, (s, e) in enumerate(ranges):
        rs = datetime.fromtimestamp(s, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        re_ = datetime.fromtimestamp(e, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        print(f"  Worker {i}: {rs} -> {re_}")

    # Run workers in parallel - each writes to its own temp file
    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        futures = {}
        for i, (start_ts, end_ts) in enumerate(ranges):
            future = executor.submit(
                scrape_range, start_ts, end_ts, i, temp_dir, at_once
            )
            futures[future] = i

        for future in as_completed(futures):
            worker_id = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"Worker {worker_id} failed: {e}")

    if shutdown_event.is_set():
        print(f"\n⚠ Stopped early. Partial data saved in {temp_dir}/")
        print(
            f"  Temp files are safe. They will NOT be merged into {output_file} automatically."
        )
        print(f"  To merge manually when ready:")
        print(f"    for i in $(seq 0 {effective_workers - 1}); do")
        print(
            f"      [ -f {temp_dir}/worker_$i.csv ] && tail -n +2 {temp_dir}/worker_$i.csv >> {output_file}"
        )
        print(f"    done")
        print(f"  Or just re-run and it will resume from where {output_file} left off.")
        return

    # Merge temp files in order
    print("\nMerging temp files into main CSV...")
    total_records = 0
    file_exists = os.path.isfile(output_file)

    for i in range(effective_workers):
        temp_file = os.path.join(temp_dir, f"worker_{i}.csv")
        if not os.path.isfile(temp_file):
            continue

        if file_exists:
            subprocess.run(f"tail -n +2 '{temp_file}' >> '{output_file}'", shell=True)
        else:
            subprocess.run(f"cp '{temp_file}' '{output_file}'", shell=True)
            file_exists = True

        count_result = subprocess.run(
            f"wc -l < '{temp_file}'", shell=True, capture_output=True, text=True
        )
        lines = int(count_result.stdout.strip()) - 1
        total_records += lines
        print(f"  Merged worker {i}: {lines:,} records")

        os.remove(temp_file)

    try:
        os.rmdir(temp_dir)
    except OSError:
        pass

    if os.path.isfile(CURSOR_FILE):
        os.remove(CURSOR_FILE)

    print(f"\nFinished scraping orderFilledEvents")
    print(f"Total new records: {total_records:,}")
    print(f"Output file: {output_file}")


def update_goldsky():
    """Run scraping for orderFilledEvents"""
    print(f"\n{'=' * 50}")
    print(f"Starting to scrape orderFilledEvents")
    print(f"Runtime: {RUNTIME_TIMESTAMP}")
    print(f"{'=' * 50}")
    try:
        scrape()
        print(f"Successfully completed orderFilledEvents")
    except Exception as e:
        print(f"Error scraping orderFilledEvents: {str(e)}")
