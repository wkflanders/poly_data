import requests
import csv
import json
import os
import time
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed


def count_csv_lines(csv_filename: str) -> int:
    """Count the number of data lines in CSV (excluding header)"""
    if not os.path.exists(csv_filename):
        return 0

    try:
        with open(csv_filename, "r", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)  # Skip header
            return sum(1 for row in reader if row)  # Count non-empty rows
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return 0


def fetch_market_tags(session, market_id):
    """Fetch tags for a single market. Returns (market_id, 'tag1|tag2|...')"""
    for attempt in range(3):
        try:
            resp = session.get(
                f"https://gamma-api.polymarket.com/markets/{market_id}/tags", timeout=15
            )
            if resp.status_code == 200:
                tags = resp.json()
                labels = [
                    t["label"] for t in tags if t.get("label") and t["label"] != "All"
                ]
                return (market_id, "|".join(labels))
            elif resp.status_code == 429:
                time.sleep(5 * (attempt + 1))
            else:
                return (market_id, "")
        except Exception:
            time.sleep(2 * (attempt + 1))
    return (market_id, "")


def backfill_tags(csv_filename: str, num_workers: int = 16):
    """Read existing CSV, fetch tags for markets missing them, rewrite CSV."""
    print(f"\n📋 Backfilling tags for {csv_filename}...")

    # Read all rows
    with open(csv_filename, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)

    # Check if tags column exists
    if "tags" not in headers:
        headers.append("tags")
        rows = [row + [""] for row in rows]

    tags_idx = headers.index("tags")
    id_idx = headers.index("id")

    # Find markets that need tags
    needs_tags = [(i, row[id_idx]) for i, row in enumerate(rows) if not row[tags_idx]]

    if not needs_tags:
        print("✓ All markets already have tags.")
        return

    print(
        f"  Fetching tags for {len(needs_tags):,} markets with {num_workers} workers..."
    )

    session = requests.Session()
    completed = 0
    total = len(needs_tags)

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {}
        for row_idx, market_id in needs_tags:
            future = executor.submit(fetch_market_tags, session, market_id)
            futures[future] = row_idx

        for future in as_completed(futures):
            row_idx = futures[future]
            try:
                market_id, tags_str = future.result()
                rows[row_idx][tags_idx] = tags_str
            except Exception:
                pass

            completed += 1
            if completed % 500 == 0:
                print(
                    f"  Progress: {completed:,}/{total:,} ({completed * 100 // total}%)"
                )

    session.close()

    # Rewrite CSV
    with open(csv_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"✓ Tags backfilled for {len(needs_tags):,} markets.")


def update_markets(csv_filename: str = "markets.csv", batch_size: int = 500):
    """
    Fetch markets ordered by creation date and save to CSV.
    Automatically resumes from the correct offset based on existing CSV lines.

    Args:
        csv_filename: Name of CSV file to save to
        batch_size: Number of markets to fetch per request
    """

    base_url = "https://gamma-api.polymarket.com/markets"

    # CSV headers for the required columns
    headers = [
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
        "closedTime",
        "tags",
    ]

    # Dynamically set offset based on existing records
    current_offset = count_csv_lines(csv_filename)
    file_exists = os.path.exists(csv_filename) and current_offset > 0

    if file_exists:
        # Check if existing file has the tags column
        with open(csv_filename, "r", encoding="utf-8") as f:
            existing_headers = f.readline().strip().split(",")
        if "tags" not in existing_headers:
            print(
                "⚠ Existing CSV missing 'tags' column - will add it during tag backfill."
            )

    if file_exists:
        print(
            f"Found {current_offset} existing records. Resuming from offset {current_offset}"
        )
        mode = "a"
    else:
        print(f"Creating new CSV file: {csv_filename}")
        mode = "w"

    total_fetched = 0

    with open(csv_filename, mode, newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)

        # Write headers only if file is new
        if mode == "w":
            writer.writerow(headers)

        while True:
            print(f"Fetching batch at offset {current_offset}...")

            try:
                params = {
                    "order": "createdAt",
                    "ascending": "true",
                    "limit": batch_size,
                    "offset": current_offset,
                }

                response = requests.get(base_url, params=params, timeout=30)

                # Handle different HTTP status codes
                if response.status_code == 500:
                    print(f"Server error (500) - retrying in 5 seconds...")
                    time.sleep(5)
                    continue
                elif response.status_code == 429:
                    print(f"Rate limited (429) - waiting 10 seconds...")
                    time.sleep(10)
                    continue
                elif response.status_code != 200:
                    print(f"API error {response.status_code}: {response.text}")
                    print("Retrying in 3 seconds...")
                    time.sleep(3)
                    continue

                markets = response.json()

                if not markets:
                    print(
                        f"No more markets found at offset {current_offset}. Completed!"
                    )
                    break

                batch_count = 0

                for market in markets:
                    try:
                        # Parse outcomes for answer1 and answer2
                        outcomes_str = market.get("outcomes", "[]")
                        if isinstance(outcomes_str, str):
                            outcomes = json.loads(outcomes_str)
                        else:
                            outcomes = outcomes_str

                        answer1 = outcomes[0] if len(outcomes) > 0 else ""
                        answer2 = outcomes[1] if len(outcomes) > 1 else ""

                        # Parse clobTokenIds for token1 and token2
                        clob_tokens_str = market.get("clobTokenIds", "[]")
                        if isinstance(clob_tokens_str, str):
                            clob_tokens = json.loads(clob_tokens_str)
                        else:
                            clob_tokens = clob_tokens_str

                        token1 = clob_tokens[0] if len(clob_tokens) > 0 else ""
                        token2 = clob_tokens[1] if len(clob_tokens) > 1 else ""

                        # Check for negative risk indicators
                        neg_risk = market.get("negRiskAugmented", False) or market.get(
                            "negRiskOther", False
                        )

                        # Create row with required columns
                        question_text = market.get("question", "") or market.get(
                            "title", ""
                        )

                        # Get ticker from events if available
                        ticker = ""
                        if market.get("events") and len(market.get("events", [])) > 0:
                            ticker = market["events"][0].get("ticker", "")

                        row = [
                            market.get("createdAt", ""),
                            market.get("id", ""),
                            question_text,
                            answer1,
                            answer2,
                            neg_risk,
                            market.get("slug", ""),
                            token1,
                            token2,
                            market.get("conditionId", ""),
                            market.get("volume", ""),
                            ticker,
                            market.get("closedTime", ""),
                            "",  # tags - backfilled after
                        ]

                        writer.writerow(row)
                        batch_count += 1

                    except (ValueError, KeyError, json.JSONDecodeError) as e:
                        print(
                            f"Error processing market {market.get('id', 'unknown')}: {e}"
                        )
                        continue

                total_fetched += batch_count
                current_offset += batch_count

                print(
                    f"Processed {batch_count} markets. Total new: {total_fetched}. Next offset: {current_offset}"
                )

                # Stop if we got fewer markets than expected (likely at the end)
                if len(markets) < batch_size:
                    print(
                        f"Received only {len(markets)} markets (less than batch size). Reached end."
                    )
                    break

            except requests.exceptions.RequestException as e:
                print(f"Network error: {e}")
                print(f"Retrying in 5 seconds...")
                time.sleep(5)
                continue
            except Exception as e:
                print(f"Unexpected error: {e}")
                print(f"Retrying in 3 seconds...")
                time.sleep(3)
                continue

    print(f"\nCompleted! Fetched {total_fetched} new markets.")
    print(f"Data saved to: {csv_filename}")
    print(f"Total records: {current_offset}")

    # Phase 2: backfill tags for any markets missing them
    backfill_tags(csv_filename)


# if __name__ == "__main__":
#     update_markets(batch_size=500)
