import requests
import json
import os
import time
from pathlib import Path
import polars as pl


PARQUET_PATH = Path("parquet/markets.parquet")
PROGRESS_PATH = Path("parquet/.markets_progress.json")


def load_progress() -> int:
    """Load the current offset from progress file."""
    if PROGRESS_PATH.exists():
        with open(PROGRESS_PATH, "r") as f:
            return json.load(f).get("offset", 0)
    return 0


def save_progress(offset: int):
    """Save current offset to progress file."""
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_PATH, "w") as f:
        json.dump({"offset": offset}, f)


def update_markets(batch_size: int = 500):
    """
    Fetch markets ordered by creation date and save to parquet.
    Automatically resumes from the correct offset.
    """
    base_url = "https://gamma-api.polymarket.com/markets"

    # Load existing data if any
    existing_df = None
    if PARQUET_PATH.exists():
        existing_df = pl.read_parquet(PARQUET_PATH)
        print(f"Found {len(existing_df)} existing records")

    current_offset = load_progress()
    if current_offset > 0:
        print(f"Resuming from offset {current_offset}")

    total_fetched = 0
    buffer = []

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

            if response.status_code == 500:
                print("Server error (500) - retrying in 5 seconds...")
                time.sleep(5)
                continue
            elif response.status_code == 429:
                print("Rate limited (429) - waiting 10 seconds...")
                time.sleep(10)
                continue
            elif response.status_code != 200:
                print(f"API error {response.status_code}: {response.text}")
                print("Retrying in 3 seconds...")
                time.sleep(3)
                continue

            markets = response.json()

            if not markets:
                print(f"No more markets found at offset {current_offset}. Completed!")
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
                    token1 = str(clob_tokens[0]) if len(clob_tokens) > 0 else ""
                    token2 = str(clob_tokens[1]) if len(clob_tokens) > 1 else ""

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

                    buffer.append(
                        {
                            "createdAt": market.get("createdAt", ""),
                            "id": str(market.get("id", "")),
                            "question": question_text,
                            "answer1": answer1,
                            "answer2": answer2,
                            "neg_risk": neg_risk,
                            "market_slug": market.get("slug", ""),
                            "token1": token1,
                            "token2": token2,
                            "condition_id": market.get("conditionId", ""),
                            "volume": market.get("volume", 0.0),
                            "ticker": ticker,
                            "closedTime": market.get("closedTime", "")
                            or market.get("endDate", ""),
                        }
                    )
                    batch_count += 1

                except (ValueError, KeyError, json.JSONDecodeError) as e:
                    print(f"Error processing market {market.get('id', 'unknown')}: {e}")
                    continue

            total_fetched += batch_count
            current_offset += batch_count

            print(
                f"Processed {batch_count} markets. Total new: {total_fetched}. Next offset: {current_offset}"
            )

            # Flush buffer every 5000 records
            if len(buffer) >= 5000:
                print(f"💾 Flushing {len(buffer)} records to parquet...")
                _flush_buffer(buffer, existing_df)
                existing_df = pl.read_parquet(PARQUET_PATH)  # Reload
                buffer.clear()
                save_progress(current_offset)

            # Stop if we got fewer markets than expected
            if len(markets) < batch_size:
                print(
                    f"Received only {len(markets)} markets (less than batch size). Reached end."
                )
                break

        except requests.exceptions.RequestException as e:
            print(f"Network error: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)
            continue
        except Exception as e:
            print(f"Unexpected error: {e}")
            print("Retrying in 3 seconds...")
            time.sleep(3)
            continue

    # Final flush
    if buffer:
        print(f"💾 Final flush: {len(buffer)} records")
        _flush_buffer(buffer, existing_df)

    save_progress(current_offset)

    # Cleanup progress file on completion
    if PROGRESS_PATH.exists():
        PROGRESS_PATH.unlink()

    final_count = len(pl.read_parquet(PARQUET_PATH)) if PARQUET_PATH.exists() else 0
    print(f"\n✅ Completed! Fetched {total_fetched} new markets.")
    print(f"Total records: {final_count}")
    print(f"Saved to: {PARQUET_PATH}")


def _flush_buffer(buffer: list, existing_df: pl.DataFrame | None):
    """Write buffer to parquet, combining with existing data."""
    PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)

    new_df = pl.DataFrame(
        buffer,
        schema={
            "createdAt": pl.Utf8,
            "id": pl.Utf8,
            "question": pl.Utf8,
            "answer1": pl.Utf8,
            "answer2": pl.Utf8,
            "neg_risk": pl.Boolean,
            "market_slug": pl.Utf8,
            "token1": pl.Utf8,
            "token2": pl.Utf8,
            "condition_id": pl.Utf8,
            "volume": pl.Float64,
            "ticker": pl.Utf8,
            "closedTime": pl.Utf8,
        },
    )

    if existing_df is not None:
        combined = pl.concat([existing_df, new_df])
    else:
        combined = new_df

    combined.write_parquet(PARQUET_PATH, compression="zstd")
    print(f"   ✓ Wrote {len(combined)} total records")
