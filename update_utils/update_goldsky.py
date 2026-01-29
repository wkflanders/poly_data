import os
import pandas as pd
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from flatten_json import flatten
from datetime import datetime, timezone
import subprocess
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


def get_latest_timestamp():
    """Get the latest timestamp from goldsky/orderFilled.csv, or 0 if missing/corrupt.

    Robust to:
      - trailing NUL bytes (\x00)
      - partially-written last line (crash during append)
      - random garbage at EOF
    """
    cache_file = "goldsky/orderFilled.csv"

    if not os.path.isfile(cache_file):
        print("No existing file found, starting from beginning of time (timestamp 0)")
        return 0

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

            # How much to scan from the end (increase if your lines are huge)
            chunk = 1024 * 1024  # 1 MiB
            offset = 0
            buf = b""

            while end - offset > 0:
                read_size = min(chunk, end - offset)
                offset += read_size
                f.seek(end - offset)
                data = f.read(read_size)

                # Prepend because we're moving backwards
                buf = data + buf

                # Keep buffer from growing without bound
                if len(buf) > 8 * chunk:
                    buf = buf[-8 * chunk :]

                # Remove NUL bytes before splitting into lines
                cleaned = buf.replace(b"\x00", b"")

                # Split into lines; scan from the bottom
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

                    last_ts = int(ts_str)
                    readable_time = datetime.fromtimestamp(
                        last_ts, tz=timezone.utc
                    ).strftime("%Y-%m-%d %H:%M:%S UTC")
                    print(f"Resuming from timestamp {last_ts} ({readable_time})")
                    return last_ts

            print(
                "Could not find a valid timestamp near EOF; falling back to timestamp 0"
            )
            return 0

    except Exception as e:
        print(f"Error reading latest timestamp: {e}")
        print("Falling back to beginning of time (timestamp 0)")
        return 0


def scrape(at_once=1000):
    QUERY_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"
    print(f"Query URL: {QUERY_URL}")
    print(f"Runtime timestamp: {RUNTIME_TIMESTAMP}")

    # Get starting timestamp from latest file
    last_value = get_latest_timestamp()
    count = 0
    total_records = 0

    print(f"\nStarting scrape for orderFilledEvents")

    output_file = "goldsky/orderFilled.csv"
    print(f"Output file: {output_file}")
    print(f"Saving columns: {COLUMNS_TO_SAVE}")

    while True:
        q_string = (
            """query MyQuery {
                        orderFilledEvents(orderBy: timestamp 
                                             first: """
            + str(at_once)
            + '''
                                             where: {timestamp_gt: "'''
            + str(last_value)
            + """"}) {
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
            print(f"No more data for orderFilledEvents")
            break

        df = pd.DataFrame([flatten(x) for x in res["orderFilledEvents"]]).reset_index(
            drop=True
        )

        # Sort by timestamp and update last_value
        df = df.sort_values("timestamp", ascending=True).reset_index(drop=True)
        last_value = df.iloc[-1]["timestamp"]

        readable_time = datetime.fromtimestamp(
            int(last_value), tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(
            f"Batch {count + 1}: Last timestamp {last_value} ({readable_time}), Records: {len(df)}"
        )

        count += 1
        total_records += len(df)

        # Remove duplicates
        df = df.drop_duplicates()

        # Filter to only the columns we want to save
        df_to_save = df[COLUMNS_TO_SAVE].copy()

        # Save to file
        if os.path.isfile(output_file):
            df_to_save.to_csv(output_file, index=None, mode="a", header=None)
        else:
            df_to_save.to_csv(output_file, index=None)

        if len(df) < at_once:
            break

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
