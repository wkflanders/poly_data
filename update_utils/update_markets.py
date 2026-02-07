import json
import os
import time
from pathlib import Path

import polars as pl
import requests

PARQUET_PATH = Path("parquet/markets.parquet")


def _get_latest_created_at() -> str | None:
    """Get the latest createdAt from existing parquet, or None if no data."""
    if not PARQUET_PATH.exists():
        return None
    df = pl.read_parquet(PARQUET_PATH, columns=["createdAt"])
    if len(df) == 0:
        return None
    # createdAt is stored as Utf8 ISO format — lexicographic max works
    return df["createdAt"].max()


def _parse_market(market: dict) -> dict | None:
    """Parse a single market API response into a flat dict. Returns None on error."""
    try:
        outcomes_str = market.get("outcomes", "[]")
        if isinstance(outcomes_str, str):
            outcomes = json.loads(outcomes_str)
        else:
            outcomes = outcomes_str
        answer1 = outcomes[0] if len(outcomes) > 0 else ""
        answer2 = outcomes[1] if len(outcomes) > 1 else ""

        clob_tokens_str = market.get("clobTokenIds", "[]")
        if isinstance(clob_tokens_str, str):
            clob_tokens = json.loads(clob_tokens_str)
        else:
            clob_tokens = clob_tokens_str
        token1 = str(clob_tokens[0]) if len(clob_tokens) > 0 else ""
        token2 = str(clob_tokens[1]) if len(clob_tokens) > 1 else ""

        neg_risk = bool(
            market.get("negRiskAugmented", False)
            or market.get("negRiskOther", False)
        )

        question_text = market.get("question", "") or market.get("title", "")

        ticker = ""
        events = market.get("events") or []
        if events:
            ticker = events[0].get("ticker", "") or ""

        return {
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
            "volume": float(market.get("volume", 0.0) or 0.0),
            "ticker": ticker,
            "closedTime": market.get("closedTime", "")
            or market.get("endDate", "")
            or "",
        }
    except (ValueError, KeyError, json.JSONDecodeError, TypeError) as e:
        print(f"Error processing market {market.get('id', 'unknown')}: {e}")
        return None


SCHEMA = {
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
}


def _flush_buffer(buffer: list[dict]):
    """Append buffer to parquet, deduplicating by id."""
    PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)

    new_df = pl.DataFrame(buffer, schema=SCHEMA)

    if PARQUET_PATH.exists():
        existing_df = pl.read_parquet(PARQUET_PATH)
        combined = pl.concat([existing_df, new_df])
    else:
        combined = new_df

    # Deduplicate: keep first occurrence per id (existing data wins)
    combined = combined.unique(subset=["id"], keep="first")
    combined = combined.sort("createdAt")

    # Atomic write
    tmp = PARQUET_PATH.with_suffix(".parquet.tmp")
    combined.write_parquet(tmp, compression="zstd")
    os.replace(tmp, PARQUET_PATH)

    print(f"   ✓ Wrote {len(combined)} total records (deduped)")


def update_markets(batch_size: int = 500):
    """
    Fetch markets ordered by creation date and append to parquet.
    Resumes from the latest createdAt timestamp in existing data.
    Deduplicates by market id on every flush.
    """
    base_url = "https://gamma-api.polymarket.com/markets"

    latest_ts = _get_latest_created_at()
    if latest_ts:
        print(f"Found existing data up to createdAt={latest_ts}")
    else:
        print("No existing data, starting from scratch")

    # The API supports cursor-style pagination via createdAt + offset.
    # We start from offset 0 with ascending createdAt order.
    # If we have existing data, we use the latest timestamp as a filter
    # and rely on dedup to handle the overlap window.
    #
    # Unfortunately the gamma API doesn't support a "createdAt > X" filter,
    # so we use offset-based pagination but deduplicate on flush.
    # To find our starting offset, we count how many markets we already have.
    if PARQUET_PATH.exists():
        existing_count = len(pl.read_parquet(PARQUET_PATH, columns=["id"]))
        # Back up slightly to catch any markets created at the same timestamp
        current_offset = max(0, existing_count - 100)
        print(f"Existing records: {existing_count}, starting from offset {current_offset}")
    else:
        existing_count = 0
        current_offset = 0

    total_fetched = 0
    buffer: list[dict] = []
    consecutive_all_known = 0

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
                print(f"No more markets at offset {current_offset}. Done!")
                break

            current_offset += len(markets)

            batch_count = 0
            for market in markets:
                parsed = _parse_market(market)
                if parsed:
                    buffer.append(parsed)
                    batch_count += 1

            total_fetched += batch_count
            print(
                f"Processed {batch_count} markets. "
                f"Total new: {total_fetched}. Next offset: {current_offset}"
            )

            # Flush buffer every 5000 records
            if len(buffer) >= 5000:
                print(f"💾 Flushing {len(buffer)} records...")
                _flush_buffer(buffer)
                buffer.clear()

            # Stop if we got fewer markets than batch size (end of data)
            if len(markets) < batch_size:
                print(
                    f"Received {len(markets)} markets (< batch size {batch_size}). "
                    f"Reached end."
                )
                break

        except requests.exceptions.RequestException as e:
            print(f"Network error: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)
            continue
        except KeyboardInterrupt:
            print("\n🛑 Ctrl+C received. Flushing and exiting...")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            print("Retrying in 3 seconds...")
            time.sleep(3)
            continue

    # Final flush
    if buffer:
        print(f"💾 Final flush: {len(buffer)} records")
        _flush_buffer(buffer)
        buffer.clear()

    final_count = len(pl.read_parquet(PARQUET_PATH)) if PARQUET_PATH.exists() else 0
    print(f"\n✅ Completed! Fetched {total_fetched} markets this run.")
    print(f"Total records: {final_count}")
    print(f"Saved to: {PARQUET_PATH}")


if __name__ == "__main__":
    update_markets()