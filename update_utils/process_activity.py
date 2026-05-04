"""
process_activity.py — Convert raw goldsky activity CSVs into processed CSVs
with market_id resolved (where applicable) and amounts converted from
6-decimal USDC BigInt to USD floats.

Inputs (from update_activity.py):
    goldsky/redemptions.csv
    goldsky/splits.csv
    goldsky/merges.csv
    goldsky/negRiskConversions.csv

Outputs:
    processed/redemptions.csv
    processed/splits.csv
    processed/merges.csv
    processed/conversions.csv

Unlike process_live.py, this re-processes from scratch each run. The activity
files are small enough (<1GB total expected) to fit comfortably in memory.

Usage:
    uv run python update_utils/process_activity.py             # all four
    uv run python update_utils/process_activity.py redemption  # subset
    uv run python update_utils/process_activity.py merge split # subset
"""

import warnings

warnings.filterwarnings("ignore")

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl

from poly_utils.utils import get_markets


# -----------------------------------------------------------------------------
# Per-entity processing config
# -----------------------------------------------------------------------------
ACTIVITY_CONFIG = {
    "redemption": {
        "input": "goldsky/redemptions.csv",
        "output": "processed/redemptions.csv",
        "user_field": "redeemer",
        "amount_field": "payout",
        "market_join_field": "condition",  # joins markets.condition_id
    },
    "split": {
        "input": "goldsky/splits.csv",
        "output": "processed/splits.csv",
        "user_field": "stakeholder",
        "amount_field": "amount",
        "market_join_field": "condition",
    },
    "merge": {
        "input": "goldsky/merges.csv",
        "output": "processed/merges.csv",
        "user_field": "stakeholder",
        "amount_field": "amount",
        "market_join_field": "condition",
    },
    "conversion": {
        "input": "goldsky/negRiskConversions.csv",
        "output": "processed/conversions.csv",
        "user_field": "stakeholder",
        "amount_field": "amount",
        # negRiskConversions are tied to a negRiskMarketId (event), not a
        # single market — there's no clean condition→market_id join.
        "market_join_field": None,
    },
}


def _load_markets_lookup():
    """Load (market_id, condition_id) lookup with condition_id lowercased."""
    m = get_markets().rename({"id": "market_id"})
    m = m.select(["market_id", "condition_id"])
    return m.with_columns(pl.col("condition_id").str.to_lowercase())


def process_one_activity(entity, markets_df=None):
    """Process a single activity stream end-to-end.

    Reads goldsky/{entity}.csv, converts amounts/timestamps, joins with
    markets on condition_id (when applicable), writes processed/{entity}.csv.
    """
    cfg = ACTIVITY_CONFIG[entity]
    if not os.path.isfile(cfg["input"]):
        print(f"  [{entity}] no input file at {cfg['input']}, skipping")
        return 0

    if markets_df is None and cfg["market_join_field"] is not None:
        markets_df = _load_markets_lookup()

    print(f"  [{entity}] reading {cfg['input']}...")
    df = pl.read_csv(
        cfg["input"],
        schema_overrides={
            "timestamp": pl.Int64,
            cfg["amount_field"]: pl.Utf8,
        },
    )
    n_in = len(df)

    # Convert timestamp (unix seconds) → datetime
    df = df.with_columns(
        pl.from_epoch(pl.col("timestamp"), time_unit="s").alias("timestamp")
    )

    # Convert amount: BigInt USDC (6 decimals) → float USD
    df = df.with_columns(
        (pl.col(cfg["amount_field"]).cast(pl.Float64) / 1_000_000.0).alias("amount_usd")
    )

    # Lowercase user column for consistent joins
    df = df.with_columns(pl.col(cfg["user_field"]).str.to_lowercase())

    # Extract tx_hash from id ("0xTXHASH_0xLOGINDEX" → "0xTXHASH")
    df = df.with_columns(
        pl.col("id").str.split_exact("_", 1).struct.field("field_0").alias("tx_hash")
    )

    # Resolve market_id where the entity has a condition
    if cfg["market_join_field"] is not None:
        df = df.with_columns(pl.col(cfg["market_join_field"]).str.to_lowercase())
        df = df.join(
            markets_df,
            left_on=cfg["market_join_field"],
            right_on="condition_id",
            how="left",
        )
    else:
        df = df.with_columns(pl.lit(None).cast(pl.Int64).alias("market_id"))

    # Rename user field to a uniform name
    df = df.rename({cfg["user_field"]: "user"})

    # Output schema — uniform base + entity-specific extras
    out_cols = ["timestamp", "market_id", "user", "amount_usd", "tx_hash"]
    if cfg["market_join_field"] == "condition":
        out_cols.append("condition")
    if entity == "conversion":
        out_cols += ["negRiskMarketId", "indexSet", "questionCount"]

    final = [c for c in out_cols if c in df.columns]
    df = df.select(final)

    os.makedirs("processed", exist_ok=True)
    df.write_csv(cfg["output"])

    n_unmapped = 0
    if "market_id" in df.columns and cfg["market_join_field"] is not None:
        n_unmapped = df.filter(pl.col("market_id").is_null()).height

    suffix = f" ({n_unmapped:,} with no market match)" if n_unmapped else ""
    print(f"  [{entity}] wrote {n_in:,} rows -> {cfg['output']}{suffix}")
    return n_in


def process_activity(only=None):
    """Process all four activity streams (or a subset)."""
    print("=" * 60)
    print("🔄 Processing activity events")
    print("=" * 60)

    entities = list(ACTIVITY_CONFIG) if only is None else list(only)
    bad = [e for e in entities if e not in ACTIVITY_CONFIG]
    if bad:
        raise ValueError(f"unknown entities: {bad}; valid: {list(ACTIVITY_CONFIG)}")

    # Load markets once if any entity needs the join
    needs_markets = any(
        ACTIVITY_CONFIG[e]["market_join_field"] is not None for e in entities
    )
    markets_df = None
    if needs_markets:
        print("📋 Loading markets...")
        markets_df = _load_markets_lookup()
        print(f"✓ {len(markets_df):,} markets loaded")

    total = 0
    for entity in entities:
        n = process_one_activity(entity, markets_df=markets_df)
        total += n

    print(f"\n✓ Total activity rows processed: {total:,}")
    print("=" * 60)
    print("✅ Activity processing complete!")
    print("=" * 60)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        process_activity(only=sys.argv[1:])
    else:
        process_activity()
