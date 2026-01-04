import warnings
warnings.filterwarnings("ignore")

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from poly_utils.utils import get_markets

import subprocess
import pandas as pd


ORDER_FILLED_PATH = "goldsky/orderFilled.csv"
PROCESSED_PATH = "processed/trades.csv"


def _build_markets_long() -> pl.DataFrame:
    markets_df = get_markets().rename({"id": "market_id"})
    return (
        markets_df
        .select(["market_id", "token1", "token2"])
        .melt(
            id_vars="market_id",
            value_vars=["token1", "token2"],
            variable_name="side",
            value_name="asset_id",
        )
        .with_columns([
            pl.col("asset_id").cast(pl.Utf8),
            pl.col("market_id").cast(pl.Utf8),
        ])
    )


def get_processed_df(df: pl.DataFrame, markets_long: pl.DataFrame) -> pl.DataFrame:
    # 1) Identify the non-USDC asset for each trade (the one that isn't 0)
    df = df.with_columns(
        pl.when(pl.col("makerAssetId") != "0")
        .then(pl.col("makerAssetId"))
        .otherwise(pl.col("takerAssetId"))
        .alias("nonusdc_asset_id")
    )

    # 2) Join once on that non-USDC asset to recover the market + side ("token1" or "token2")
    df = df.join(
        markets_long,
        left_on="nonusdc_asset_id",
        right_on="asset_id",
        how="left",
    )

    # 3) label columns and keep market_id
    df = df.with_columns([
        pl.when(pl.col("makerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("makerAsset"),
        pl.when(pl.col("takerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("takerAsset"),
        pl.col("market_id"),
    ])

    df = df[[
        "timestamp",
        "market_id",
        "maker",
        "makerAsset",
        "makerAmountFilled",
        "taker",
        "takerAsset",
        "takerAmountFilled",
        "transactionHash",
    ]]

    df = df.with_columns([
        (pl.col("makerAmountFilled") / 10**6).alias("makerAmountFilled"),
        (pl.col("takerAmountFilled") / 10**6).alias("takerAmountFilled"),
    ])

    df = df.with_columns([
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("BUY"))
        .otherwise(pl.lit("SELL"))
        .alias("taker_direction"),

        # reverse of taker_direction
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("SELL"))
        .otherwise(pl.lit("BUY"))
        .alias("maker_direction"),
    ])

    df = df.with_columns([
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
    ])

    return df[[
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
    ]]


def _find_skip_rows_after_header(order_filled_path: str, last_processed: dict) -> int:
    """
    Return how many DATA rows (after the header) to skip so we start *after*
    the last processed (timestamp, maker, taker, txhash) row.

    If not found, returns 0 (process from beginning).
    """
    if not last_processed:
        return 0

    # processed/trades.csv timestamp is ISO; orderFilled.csv timestamp is unix seconds
    target_ts = int(pd.Timestamp(last_processed["timestamp"]).timestamp())
    target_hash = (last_processed["transactionHash"] or "").lower()
    target_maker = (last_processed["maker"] or "").lower()
    target_taker = (last_processed["taker"] or "").lower()

    with open(order_filled_path, "r", encoding="utf-8") as f:
        _header = next(f, None)
        if _header is None:
            return 0

        for i, line in enumerate(f):
            parts = line.rstrip("\n").split(",")
            if len(parts) < 8:
                continue

            # orderFilled schema:
            # timestamp,maker,makerAssetId,makerAmountFilled,taker,takerAssetId,takerAmountFilled,transactionHash
            try:
                ts = int(parts[0])
            except ValueError:
                continue

            if ts != target_ts:
                continue
            if parts[1].lower() != target_maker:
                continue
            if parts[4].lower() != target_taker:
                continue
            if parts[7].lower() != target_hash:
                continue

            # skip i+1 rows after header (i is 0-based data-line index)
            return i + 1

    return 0


def process_live(batch_size: int = 500_000) -> None:
    print("=" * 60)
    print("🔄 Processing Live Trades")
    print("=" * 60)

    last_processed: dict = {}

    if os.path.exists(PROCESSED_PATH):
        print(f"✓ Found existing processed file: {PROCESSED_PATH}")
        result = subprocess.run(["tail", "-n", "1", PROCESSED_PATH], capture_output=True, text=True)
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

    print(f"\n📂 Reading (batched): {ORDER_FILLED_PATH}")

    schema_overrides = {
        "takerAssetId": pl.Utf8,
        "makerAssetId": pl.Utf8,
        "transactionHash": pl.Utf8,
        "maker": pl.Utf8,
        "taker": pl.Utf8,
    }

    skip_rows_after_header = _find_skip_rows_after_header(ORDER_FILLED_PATH, last_processed)
    if skip_rows_after_header > 0:
        print(f"↪️  Skipping {skip_rows_after_header:,} rows (resume checkpoint found)")
    elif last_processed:
        print("⚠️  Could not find exact checkpoint row in orderFilled.csv; processing from start (batched)")

    markets_long = _build_markets_long()

    if not os.path.isdir("processed"):
        os.makedirs("processed")

    op_file = PROCESSED_PATH
    wrote_header = not os.path.isfile(op_file)

    reader = pl.read_csv_batched(
        ORDER_FILLED_PATH,
        schema_overrides=schema_overrides,
        batch_size=batch_size,
        skip_rows_after_header=skip_rows_after_header,
        has_header=True,
    )

    total_in = 0
    total_out = 0

    while True:
        batches = reader.next_batches(1)
        if not batches:
            break

        df = batches[0]
        total_in += df.height

        # convert unix epoch to datetime
        df = df.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="s").alias("timestamp")
        )

        new_df = get_processed_df(df, markets_long)

        mode = "w" if wrote_header else "a"
        with open(op_file, mode=mode) as f:
            new_df.write_csv(f, include_header=wrote_header)
        wrote_header = False

        total_out += new_df.height
        print(f"✓ processed batch: in={df.height:,} out={new_df.height:,} (total out={total_out:,})")

        # help Python free objects promptly between batches
        del df, new_df

    print("=" * 60)
    print(f"✅ Processing complete! appended_rows={total_out:,} (read_rows={total_in:,})")
    print("=" * 60)


if __name__ == "__main__":
    process_live()
