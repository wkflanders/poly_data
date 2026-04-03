import warnings

warnings.filterwarnings("ignore")
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import polars as pl
from poly_utils.utils import get_markets, update_missing_tokens
import subprocess
import pandas as pd

BATCH_SIZE = 5_000_000


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
    # 3) Join once on that non-USDC asset to recover the market + side ("token1" or "token2")
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


def process_live():
    processed_file = "processed/trades.csv"

    print("=" * 60)
    print("🔄 Processing Live Trades")
    print("=" * 60)

    last_processed = {}

    if os.path.exists(processed_file):
        print(f"✓ Found existing processed file: {processed_file}")
        result = subprocess.run(
            ["tail", "-n", "1", processed_file], capture_output=True, text=True
        )
        last_line = result.stdout.strip()
        splitted = last_line.split(",")

        last_processed["timestamp"] = pd.to_datetime(splitted[0])
        last_processed["transactionHash"] = splitted[-1]
        last_processed["maker"] = splitted[2]
        last_processed["taker"] = splitted[3]

        print(f"📍 Resuming from: {last_processed['timestamp']}")
        print(f"   Last hash: {last_processed['transactionHash'][:16]}...")
    else:
        print("⚠ No existing processed file found - processing from beginning")

    print(f"\n📂 Reading: goldsky/orderFilled.csv (in batches of {BATCH_SIZE:,})")

    schema_overrides = {
        "takerAssetId": pl.Utf8,
        "makerAssetId": pl.Utf8,
    }

    # Load markets once
    print("📋 Loading markets...")
    markets_long = get_markets_long()
    print(f"✓ Markets loaded")

    if not os.path.isdir("processed"):
        os.makedirs("processed")

    op_file = "processed/trades.csv"
    header_written = os.path.isfile(op_file)
    found_start = not bool(last_processed)  # if no checkpoint, start immediately
    total_processed = 0
    batch_num = 0

    reader = pl.read_csv_batched(
        "goldsky/orderFilled.csv",
        schema_overrides=schema_overrides,
        batch_size=BATCH_SIZE,
    )

    while True:
        batches = reader.next_batches(1)
        if not batches:
            break

        chunk = batches[0]
        batch_num += 1

        chunk = chunk.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="s").alias("timestamp")
        )

        if not found_start:
            # Check if this chunk contains our checkpoint
            matches = chunk.filter(
                (pl.col("timestamp") == last_processed["timestamp"])
                & (pl.col("transactionHash") == last_processed["transactionHash"])
                & (pl.col("maker") == last_processed["maker"])
                & (pl.col("taker") == last_processed["taker"])
            )
            if len(matches) == 0:
                print(
                    f"   Batch {batch_num}: skipping ({len(chunk):,} rows, before checkpoint)"
                )
                continue

            # Found checkpoint - take everything after the matched row
            chunk = chunk.with_row_index()
            match_idx = chunk.filter(
                (pl.col("timestamp") == last_processed["timestamp"])
                & (pl.col("transactionHash") == last_processed["transactionHash"])
                & (pl.col("maker") == last_processed["maker"])
                & (pl.col("taker") == last_processed["taker"])
            ).row(0)[0]

            chunk = chunk.filter(pl.col("index") > match_idx).drop("index")
            found_start = True
            print(
                f"   Batch {batch_num}: found checkpoint, {len(chunk):,} new rows in this batch"
            )

            if len(chunk) == 0:
                continue
        else:
            print(f"   Batch {batch_num}: processing {len(chunk):,} rows")

        # Process this chunk
        processed = get_processed_df(chunk, markets_long)
        total_processed += len(processed)

        # Write
        if not header_written:
            processed.write_csv(op_file)
            header_written = True
        else:
            with open(op_file, "a") as f:
                processed.write_csv(f, include_header=False)

    print(f"\n✓ Total rows processed: {total_processed:,}")
    print("=" * 60)
    print("✅ Processing complete!")
    print("=" * 60)


if __name__ == "__main__":
    process_live()

