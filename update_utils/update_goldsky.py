import os
import json
import pandas as pd
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from flatten_json import flatten
from datetime import datetime, timezone
import time
from update_utils.update_markets import update_markets

# Global runtime timestamp - set once when program starts
RUNTIME_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

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


def save_cursor(timestamp, last_id, sticky_timestamp=None):
    """Save cursor state to file for efficient resume."""
    state = {
        "last_timestamp": timestamp,
        "last_id": last_id,
        "sticky_timestamp": sticky_timestamp,
    }
    with open(CURSOR_FILE, "w") as f:
        json.dump(state, f)


def _read_latest_timestamp_from_csv(cache_file):
    """Read the latest valid timestamp from the CSV file.

    Robust to:
      - trailing NUL bytes (\\x00)
      - partially-written last line (crash during append)
      - random garbage at EOF
    """
    try:
        # Read header safely (strip NULs)
        with open(cache_file, "rb") as f:
            header = (
                f.readline()
                .replace(b"\x00", b"")
                .decode("utf-8", errors="ignore")
                .strip()
            )

        if not header:
            print("Empty header; falling back to timestamp 0")
            return 0

        headers = header.split(",")
        if "timestamp" not in headers:
            print("No 'timestamp' column in header; falling back to timestamp 0")
            return 0

        ts_idx = headers.index("timestamp")

        # Read from end and find last *valid* CSV line with numeric timestamp
        with open(cache_file, "rb") as f:
            f.seek(0, os.SEEK_END)
            end = f.tell()

            chunk = 1024 * 1024  # 1 MiB
            offset = 0
            buf = b""

            while end - offset > 0:
                read_size = min(chunk, end - offset)
                offset += read_size
                f.seek(end - offset)
                data = f.read(read_size)

                buf = data + buf

                if len(buf) > 8 * chunk:
                    buf = buf[-8 * chunk :]

                cleaned = buf.replace(b"\x00", b"")
                lines = cleaned.splitlines()

                for raw in reversed(lines):
                    if not raw:
                        continue
                    line = raw.decode("utf-8", errors="ignore").strip()
                    if not line or line == header:
                        continue

                    parts = line.split(",")
                    if len(parts) <= ts_idx:
                        continue

                    ts_str = parts[ts_idx].strip().strip('"')
                    if not ts_str.isdigit():
                        continue

                    return int(ts_str)

        print("Could not find a valid timestamp near EOF; falling back to timestamp 0")
        return 0

    except Exception as e:
        print(f"Error reading latest timestamp from CSV: {e}")
        return 0


def get_latest_cursor():
    """Get the latest cursor state for efficient resume.

    Priority:
      1. cursor_state.json  (exact resume, no duplicates)
      2. CSV tail scan       (robust to NUL bytes / partial writes)
      3. timestamp 0         (full re-scrape)

    Returns (timestamp, last_id, sticky_timestamp) tuple.
    """
    # --- 1. Try cursor state file (most efficient, preserves sticky state) ---
    if os.path.isfile(CURSOR_FILE):
        try:
            with open(CURSOR_FILE, "r") as f:
                state = json.load(f)
            timestamp = state.get("last_timestamp", 0)
            last_id = state.get("last_id")
            sticky_timestamp = state.get("sticky_timestamp")

            # Validate: sticky_timestamp requires last_id
            if sticky_timestamp is not None and last_id is None:
                print(
                    f"Warning: Invalid cursor state "
                    f"(sticky_timestamp={sticky_timestamp} but last_id=None), "
                    f"clearing sticky state"
                )
                sticky_timestamp = None

            if timestamp > 0:
                readable_time = datetime.fromtimestamp(
                    timestamp, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S UTC")
                print(
                    f"Resuming from cursor state: timestamp {timestamp} "
                    f"({readable_time}), id: {last_id}, sticky: {sticky_timestamp}"
                )
                return timestamp, last_id, sticky_timestamp
        except Exception as e:
            print(f"Error reading cursor file: {e}")

    # --- 2. Fallback: read from CSV (robust binary scan) ---
    cache_file = "goldsky/orderFilled.csv"

    if not os.path.isfile(cache_file):
        print("No existing file found, starting from beginning of time (timestamp 0)")
        return 0, None, None

    last_ts = _read_latest_timestamp_from_csv(cache_file)
    if last_ts > 0:
        readable_time = datetime.fromtimestamp(last_ts, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
        print(
            f"Resuming from CSV (no cursor file): timestamp {last_ts} ({readable_time})"
        )
        # Go back 1 second to ensure no data loss (may create some duplicates)
        return last_ts - 1, None, None

    # --- 3. Fallback: start from the beginning ---
    print("Falling back to beginning of time (timestamp 0)")
    return 0, None, None


def scrape(at_once=1000):
    QUERY_URL = (
        "https://api.goldsky.com/api/public/"
        "project_cl6mb8i9h0003e201j6li0diw/"
        "subgraphs/orderbook-subgraph/0.0.1/gn"
    )
    print(f"Query URL: {QUERY_URL}")
    print(f"Runtime timestamp: {RUNTIME_TIMESTAMP}")

    # Get starting cursor (includes sticky state for perfect resume)
    last_timestamp, last_id, sticky_timestamp = get_latest_cursor()
    count = 0
    total_records = 0

    print(f"\nStarting scrape for orderFilledEvents")

    output_file = "goldsky/orderFilled.csv"
    print(f"Output file: {output_file}")
    print(f"Saving columns: {COLUMNS_TO_SAVE}")

    while True:
        # Build the where clause based on cursor state
        if sticky_timestamp is not None:
            # Sticky mode: stay at this timestamp and paginate by id
            where_clause = f'timestamp: "{sticky_timestamp}", id_gt: "{last_id}"'
        else:
            # Normal mode: advance by timestamp
            where_clause = f'timestamp_gt: "{last_timestamp}"'

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

        query = gql(q_string)
        transport = RequestsHTTPTransport(url=QUERY_URL, verify=True, retries=3)
        client = Client(transport=transport)

        try:
            res = client.execute(query)
        except Exception as e:
            print(f"Query error: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)
            continue

        if not res["orderFilledEvents"] or len(res["orderFilledEvents"]) == 0:
            if sticky_timestamp is not None:
                # Exhausted events at sticky timestamp, advance to next
                last_timestamp = sticky_timestamp
                sticky_timestamp = None
                last_id = None
                continue
            print(f"No more data for orderFilledEvents")
            break

        df = pd.DataFrame([flatten(x) for x in res["orderFilledEvents"]]).reset_index(
            drop=True
        )

        # Sort by timestamp and id for consistent ordering
        df = df.sort_values(["timestamp", "id"], ascending=True).reset_index(drop=True)

        batch_last_timestamp = int(df.iloc[-1]["timestamp"])
        batch_last_id = df.iloc[-1]["id"]
        batch_first_timestamp = int(df.iloc[0]["timestamp"])

        readable_time = datetime.fromtimestamp(
            batch_last_timestamp, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")

        # Determine if we need sticky cursor for next iteration
        if len(df) >= at_once:
            # Batch is full — may be more events at the boundary timestamp
            sticky_timestamp = batch_last_timestamp
            last_id = batch_last_id
            if batch_first_timestamp == batch_last_timestamp:
                print(
                    f"Batch {count + 1}: Timestamp {batch_last_timestamp} "
                    f"({readable_time}), Records: {len(df)} "
                    f"[STICKY - continuing at same timestamp]"
                )
            else:
                print(
                    f"Batch {count + 1}: Timestamps "
                    f"{batch_first_timestamp}-{batch_last_timestamp} "
                    f"({readable_time}), Records: {len(df)} "
                    f"[STICKY - ensuring complete timestamp]"
                )
        else:
            # Batch not full — we have all events, can advance normally
            if sticky_timestamp is not None:
                last_timestamp = sticky_timestamp
                sticky_timestamp = None
                last_id = None
                print(
                    f"Batch {count + 1}: Timestamp {batch_last_timestamp} "
                    f"({readable_time}), Records: {len(df)} [STICKY COMPLETE]"
                )
            else:
                last_timestamp = batch_last_timestamp
                print(
                    f"Batch {count + 1}: Last timestamp {batch_last_timestamp} "
                    f"({readable_time}), Records: {len(df)}"
                )

        count += 1
        total_records += len(df)

        # Remove duplicates (by id to be safe)
        df = df.drop_duplicates(subset=["id"])

        # Filter to only the columns we want to save
        df_to_save = df[COLUMNS_TO_SAVE].copy()

        # Save to file
        if os.path.isfile(output_file):
            df_to_save.to_csv(output_file, index=None, mode="a", header=None)
        else:
            df_to_save.to_csv(output_file, index=None)

        # Save cursor state for efficient resume (no duplicates on restart)
        save_cursor(last_timestamp, last_id, sticky_timestamp)

        if len(df) < at_once and sticky_timestamp is None:
            break

    # Clear cursor file on successful completion
    if os.path.isfile(CURSOR_FILE):
        os.remove(CURSOR_FILE)

    print(f"Finished scraping orderFilledEvents")
    print(f"Total new records: {total_records}")
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
