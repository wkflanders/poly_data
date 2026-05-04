import warnings

warnings.filterwarnings("ignore")

import io
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import polars as pl

from poly_utils.utils import get_markets, update_missing_tokens

# Number of source-CSV lines per processing batch. Smaller batches = more
# frequent offset checkpoints, so a kill/Ctrl-C only loses up to this many
# rows of work. 500k is a good balance: chunk fits comfortably in RAM and
# the per-batch fixed overhead is amortized.
BATCH_LINES = 500_000

SOURCE_CSV = "goldsky/orderFilled.csv"
PROCESSED_CSV = "processed/trades.csv"
# Byte offset into SOURCE_CSV pointing at the start of the next line to
# process. Updated atomically after each successful batch.
OFFSET_FILE = "processed/orderFilled.offset"


def get_markets_long():
    markets_df = get_markets()
    markets_df = markets_df.rename({"id": "market_id"})
    markets_long = markets_df.select(["market_id", "token1", "token2"]).melt(
        id_vars="market_id",
        value_vars=["token1", "token2"],
        variable_name="side",
        value_name="asset_id",
    )
    return markets_long


def get_processed_df(df, markets_long):
    # 2) Identify the non-USDC asset for each trade (the one that isn't 0)
    df = df.with_columns(
        pl.when(pl.col("makerAssetId") != "0")
        .then(pl.col("makerAssetId"))
        .otherwise(pl.col("takerAssetId"))
        .alias("nonusdc_asset_id")
    )
    # 3) Join once on that non-USDC asset to recover the market + side
    df = df.join(
        markets_long,
        left_on="nonusdc_asset_id",
        right_on="asset_id",
        how="left",
    )
    # 4) label columns and keep market_id
    df = df.with_columns(
        [
            pl.when(pl.col("makerAssetId") == "0")
            .then(pl.lit("USDC"))
            .otherwise(pl.col("side"))
            .alias("makerAsset"),
            pl.when(pl.col("takerAssetId") == "0")
            .then(pl.lit("USDC"))
            .otherwise(pl.col("side"))
            .alias("takerAsset"),
            pl.col("market_id"),
        ]
    )
    df = df[
        [
            "timestamp",
            "market_id",
            "maker",
            "makerAsset",
            "makerAmountFilled",
            "taker",
            "takerAsset",
            "takerAmountFilled",
            "transactionHash",
        ]
    ]
    df = df.with_columns(
        [
            (pl.col("makerAmountFilled") / 10**6).alias("makerAmountFilled"),
            (pl.col("takerAmountFilled") / 10**6).alias("takerAmountFilled"),
        ]
    )
    df = df.with_columns(
        [
            pl.when(pl.col("takerAsset") == "USDC")
            .then(pl.lit("BUY"))
            .otherwise(pl.lit("SELL"))
            .alias("taker_direction"),
            pl.when(pl.col("takerAsset") == "USDC")
            .then(pl.lit("SELL"))
            .otherwise(pl.lit("BUY"))
            .alias("maker_direction"),
        ]
    )
    df = df.with_columns(
        [
            pl.when(pl.col("makerAsset") != "USDC")
            .then(pl.col("makerAsset"))
            .otherwise(pl.col("takerAsset"))
            .alias("nonusdc_side"),
            pl.when(pl.col("takerAsset") == "USDC")
            .then(pl.col("takerAmountFilled"))
            .otherwise(pl.col("makerAmountFilled"))
            .alias("usd_amount"),
            pl.when(pl.col("takerAsset") != "USDC")
            .then(pl.col("takerAmountFilled"))
            .otherwise(pl.col("makerAmountFilled"))
            .alias("token_amount"),
            pl.when(pl.col("takerAsset") == "USDC")
            .then(pl.col("takerAmountFilled") / pl.col("makerAmountFilled"))
            .otherwise(pl.col("makerAmountFilled") / pl.col("takerAmountFilled"))
            .cast(pl.Float64)
            .alias("price"),
        ]
    )
    df = df[
        [
            "timestamp",
            "market_id",
            "maker",
            "taker",
            "nonusdc_side",
            "maker_direction",
            "taker_direction",
            "price",
            "usd_amount",
            "token_amount",
            "transactionHash",
        ]
    ]
    return df


# ---------------------------------------------------------------------------
# Offset checkpointing
# ---------------------------------------------------------------------------
def _load_offset():
    if not os.path.exists(OFFSET_FILE):
        return None
    with open(OFFSET_FILE) as f:
        return int(f.read().strip())


def _save_offset(offset):
    """Atomic offset write so an interrupted update doesn't corrupt the file."""
    tmp = OFFSET_FILE + ".tmp"
    with open(tmp, "w") as f:
        f.write(str(offset))
    os.replace(tmp, OFFSET_FILE)


def _find_checkpoint_offset(source_path, last_processed):
    """One-time migration: locate the byte offset of the line AFTER the last
    processed row by streaming through the source CSV.

    Only runs once — once an offset is saved, future runs skip directly there.
    Pure-Python line iteration is slower per row than polars, but for finding
    a single match it's competitive because there's no DataFrame construction
    or filter logic per batch.
    """
    print("📍 No offset checkpoint yet — scanning source CSV to locate last")
    print("   processed row. This is a one-time cost; future runs resume in O(1).")

    target_ts = int(last_processed["timestamp"].timestamp())
    target_hash = last_processed["transactionHash"]
    target_maker = last_processed["maker"]
    target_taker = last_processed["taker"]

    with open(source_path, "rb") as f:
        header_line = f.readline()
        header = header_line.decode("utf-8").rstrip("\r\n").split(",")
        try:
            ts_idx = header.index("timestamp")
            hash_idx = header.index("transactionHash")
            maker_idx = header.index("maker")
            taker_idx = header.index("taker")
        except ValueError as e:
            raise RuntimeError(f"Source CSV missing expected column: {e}")

        max_idx = max(ts_idx, hash_idx, maker_idx, taker_idx)
        rows = 0
        last_log = 0
        while True:
            line = f.readline()
            if not line:
                break
            rows += 1
            cols = line.decode("utf-8", errors="replace").rstrip("\r\n").split(",")
            if len(cols) <= max_idx:
                continue
            try:
                row_ts = int(cols[ts_idx])
            except ValueError:
                continue

            if row_ts < target_ts:
                if rows - last_log >= 1_000_000:
                    print(f"   ... scanned {rows:,} rows")
                    last_log = rows
                continue
            if row_ts > target_ts:
                # Walked past the target second without seeing the exact row.
                # The checkpoint row may have been deleted from source, or
                # processed/trades.csv is from a different file. Bail out
                # rather than silently corrupting state.
                raise RuntimeError(
                    f"Checkpoint row not found in {source_path} "
                    f"(target ts={target_ts} hash={target_hash[:16]}...). "
                    f"Source CSV may have been truncated or replaced. "
                    f"If you intend to reprocess from scratch, delete "
                    f"{PROCESSED_CSV} and {OFFSET_FILE} and retry."
                )
            # Same timestamp — verify hash/maker/taker
            if (
                cols[hash_idx] == target_hash
                and cols[maker_idx] == target_maker
                and cols[taker_idx] == target_taker
            ):
                offset_after = f.tell()
                print(
                    f"   ✓ Checkpoint located at row {rows:,}, "
                    f"byte offset {offset_after:,}"
                )
                return offset_after

    raise RuntimeError(
        f"Reached end of {source_path} without finding checkpoint "
        f"(ts={target_ts}, hash={target_hash[:16]}...)"
    )


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------
def _process_batch(
    lines,
    header_bytes,
    markets_long,
    schema_overrides,
    op_file,
    header_written,
):
    """Parse a chunk of raw CSV bytes with polars, transform, and append."""
    csv_bytes = header_bytes + b"".join(lines)
    chunk = pl.read_csv(
        io.BytesIO(csv_bytes),
        schema_overrides=schema_overrides,
    )
    chunk = chunk.with_columns(
        pl.from_epoch(pl.col("timestamp"), time_unit="s").alias("timestamp")
    )
    processed = get_processed_df(chunk, markets_long)

    if not header_written:
        processed.write_csv(op_file)
    else:
        with open(op_file, "a") as f:
            processed.write_csv(f, include_header=False)
    return len(processed)


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------
def process_live():
    print("=" * 60)
    print("🔄 Processing Live Trades")
    print("=" * 60)

    if not os.path.isdir("processed"):
        os.makedirs("processed")

    # Decide where to start reading from in SOURCE_CSV
    start_offset = _load_offset()

    if start_offset is None and os.path.exists(PROCESSED_CSV):
        # First run since switching to offset-based checkpointing.
        # Find the offset by scanning, then save it for future runs.
        print(f"✓ Found existing processed file: {PROCESSED_CSV}")
        result = subprocess.run(
            ["tail", "-n", "1", PROCESSED_CSV], capture_output=True, text=True
        )
        last_line = result.stdout.strip()
        splitted = last_line.split(",")
        last_processed = {
            "timestamp": pd.to_datetime(splitted[0]),
            "transactionHash": splitted[-1],
            "maker": splitted[2],
            "taker": splitted[3],
        }
        print(f"📍 Last processed row: {last_processed['timestamp']}")
        print(f"   Hash: {last_processed['transactionHash'][:16]}...")
        start_offset = _find_checkpoint_offset(SOURCE_CSV, last_processed)
        _save_offset(start_offset)
    elif start_offset is None:
        print("⚠ No existing processed file - processing from beginning")
        start_offset = 0
    else:
        print(f"📍 Resuming from byte offset {start_offset:,} in {SOURCE_CSV}")

    print("📋 Loading markets...")
    markets_long = get_markets_long()
    print("✓ Markets loaded")

    schema_overrides = {
        "takerAssetId": pl.Utf8,
        "makerAssetId": pl.Utf8,
    }

    # Capture the header bytes once — we prepend them to each in-memory batch
    # so polars can parse it as a self-contained CSV.
    with open(SOURCE_CSV, "rb") as fh:
        header_bytes = fh.readline()

    header_written = os.path.isfile(PROCESSED_CSV)
    total_processed = 0
    batch_num = 0

    with open(SOURCE_CSV, "rb") as fh:
        if start_offset > 0:
            fh.seek(start_offset)
        else:
            # Skip the header row only on first-ever run
            fh.readline()

        buffer = []
        while True:
            line = fh.readline()
            if not line:
                break
            buffer.append(line)

            if len(buffer) >= BATCH_LINES:
                batch_num += 1
                rows_out = _process_batch(
                    buffer,
                    header_bytes,
                    markets_long,
                    schema_overrides,
                    PROCESSED_CSV,
                    header_written,
                )
                header_written = True
                total_processed += rows_out
                offset_after = fh.tell()
                _save_offset(offset_after)
                print(
                    f"   Batch {batch_num}: {len(buffer):,} rows in, "
                    f"{rows_out:,} rows out  |  total {total_processed:,}  |  "
                    f"offset {offset_after:,}"
                )
                buffer = []

        # Final partial batch
        if buffer:
            batch_num += 1
            rows_out = _process_batch(
                buffer,
                header_bytes,
                markets_long,
                schema_overrides,
                PROCESSED_CSV,
                header_written,
            )
            total_processed += rows_out
            offset_after = fh.tell()
            _save_offset(offset_after)
            print(
                f"   Batch {batch_num} (final): {len(buffer):,} rows in, "
                f"{rows_out:,} rows out  |  total {total_processed:,}"
            )

    print(f"\n✓ Total rows processed: {total_processed:,}")
    print("=" * 60)
    print("✅ Processing complete!")
    print("=" * 60)


if __name__ == "__main__":
    process_live()
