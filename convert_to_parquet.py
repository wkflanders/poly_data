#!/usr/bin/env python3
"""
Convert large CSV files to Parquet format using DuckDB.
DuckDB can handle files larger than available RAM through streaming.

Usage:
    python convert_to_parquet.py [--all | --trades | --orders | --markets]

Output:
    Creates a 'parquet/' directory with:
    - trades.parquet (partitioned by date for large datasets)
    - order_filled.parquet (partitioned by date)
    - markets.parquet (single file, small enough)
"""

import argparse
import duckdb
from pathlib import Path
import time

# Configure paths
BASE_DIR = Path(__file__).parent
PARQUET_DIR = BASE_DIR / "parquet"

# Source files
TRADES_CSV = BASE_DIR / "processed" / "trades.csv"
ORDER_FILLED_CSV = BASE_DIR / "goldsky" / "orderFilled.csv"
MARKETS_CSV = BASE_DIR / "markets.csv"


def get_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with memory-efficient settings."""
    conn = duckdb.connect(":memory:")
    # Limit memory usage to leave room for OS
    conn.execute("SET memory_limit = '32GB'")
    # Enable progress bar for long operations
    conn.execute("SET enable_progress_bar = true")
    return conn


def convert_markets(conn: duckdb.DuckDBPyConnection):
    """Convert markets.csv to Parquet (small file, single output)."""
    print("\n📊 Converting markets.csv...")
    start = time.time()

    output_path = PARQUET_DIR / "markets.parquet"

    conn.execute(f"""
        COPY (
            SELECT 
                createdAt,
                id,
                question,
                answer1,
                answer2,
                neg_risk,
                market_slug,
                token1,
                token2,
                condition_id,
                volume,
                ticker,
                closedTime
            FROM read_csv('{MARKETS_CSV}', 
                header=true,
                auto_detect=true,
                sample_size=10000
                columns={{
                    'token1': 'VARCHAR',
                    'token2': 'VARCHAR'
                }}
            )
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    elapsed = time.time() - start
    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"   ✅ Done in {elapsed:.1f}s → {output_path.name} ({size_mb:.1f} MB)")


def convert_trades(conn: duckdb.DuckDBPyConnection):
    """Convert trades.csv to partitioned Parquet files by month."""
    print("\n📈 Converting trades.csv (this may take a while)...")
    start = time.time()

    output_dir = PARQUET_DIR / "trades"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Partition by year-month for manageable file sizes and efficient date filtering
    conn.execute(f"""
        COPY (
            SELECT 
                timestamp,
                market_id,
                maker,
                taker,
                nonusdc_side,
                maker_direction,
                taker_direction,
                price,
                usd_amount,
                token_amount,
                transactionHash,
                -- Add partition column
                strftime(timestamp, '%Y-%m') as year_month
            FROM read_csv('{TRADES_CSV}',
                header=true,
                columns={{
                    'timestamp': 'TIMESTAMP',
                    'market_id': 'VARCHAR',
                    'maker': 'VARCHAR',
                    'taker': 'VARCHAR',
                    'nonusdc_side': 'VARCHAR',
                    'maker_direction': 'VARCHAR',
                    'taker_direction': 'VARCHAR',
                    'price': 'DOUBLE',
                    'usd_amount': 'DOUBLE',
                    'token_amount': 'DOUBLE',
                    'transactionHash': 'VARCHAR'
                }}
            )
        ) TO '{output_dir}' (
            FORMAT PARQUET, 
            COMPRESSION ZSTD,
            PARTITION_BY (year_month),
            OVERWRITE_OR_IGNORE
        )
    """)

    elapsed = time.time() - start
    # Calculate total size
    total_size = sum(f.stat().st_size for f in output_dir.rglob("*.parquet"))
    size_gb = total_size / (1024**3)
    print(f"   ✅ Done in {elapsed:.1f}s → trades/ ({size_gb:.2f} GB)")


def convert_order_filled(conn: duckdb.DuckDBPyConnection):
    """Convert orderFilled.csv to partitioned Parquet files by month."""
    print("\n📋 Converting orderFilled.csv (this may take a while)...")
    start = time.time()

    output_dir = PARQUET_DIR / "order_filled"
    output_dir.mkdir(parents=True, exist_ok=True)

    conn.execute(f"""
        COPY (
            SELECT 
                timestamp,
                maker,
                makerAssetId,
                makerAmountFilled,
                taker,
                takerAssetId,
                takerAmountFilled,
                transactionHash,
                -- Add partition column
                strftime(timestamp, '%Y-%m') as year_month
            FROM read_csv('{ORDER_FILLED_CSV}',
                header=true,
                columns={{
                    'timestamp': 'TIMESTAMP',
                    'maker': 'VARCHAR',
                    'makerAssetId': 'VARCHAR',
                    'makerAmountFilled': 'DOUBLE',
                    'taker': 'VARCHAR',
                    'takerAssetId': 'VARCHAR',
                    'takerAmountFilled': 'DOUBLE',
                    'transactionHash': 'VARCHAR'
                }}
            )
        ) TO '{output_dir}' (
            FORMAT PARQUET, 
            COMPRESSION ZSTD,
            PARTITION_BY (year_month),
            OVERWRITE_OR_IGNORE
        )
    """)

    elapsed = time.time() - start
    total_size = sum(f.stat().st_size for f in output_dir.rglob("*.parquet"))
    size_gb = total_size / (1024**3)
    print(f"   ✅ Done in {elapsed:.1f}s → order_filled/ ({size_gb:.2f} GB)")


def main():
    parser = argparse.ArgumentParser(
        description="Convert Polymarket CSV files to Parquet format"
    )
    parser.add_argument("--all", action="store_true", help="Convert all files")
    parser.add_argument("--trades", action="store_true", help="Convert trades.csv")
    parser.add_argument("--orders", action="store_true", help="Convert orderFilled.csv")
    parser.add_argument("--markets", action="store_true", help="Convert markets.csv")

    args = parser.parse_args()

    # Default to all if nothing specified
    if not any([args.all, args.trades, args.orders, args.markets]):
        args.all = True

    # Create output directory
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    print("🦆 DuckDB Parquet Converter")
    print("=" * 50)

    conn = get_connection()

    try:
        if args.all or args.markets:
            if MARKETS_CSV.exists():
                convert_markets(conn)
            else:
                print(f"⚠️  Skipping markets.csv (not found at {MARKETS_CSV})")

        if args.all or args.trades:
            if TRADES_CSV.exists():
                convert_trades(conn)
            else:
                print(f"⚠️  Skipping trades.csv (not found at {TRADES_CSV})")

        if args.all or args.orders:
            if ORDER_FILLED_CSV.exists():
                convert_order_filled(conn)
            else:
                print(f"⚠️  Skipping orderFilled.csv (not found at {ORDER_FILLED_CSV})")

    finally:
        conn.close()

    print("\n" + "=" * 50)
    print("✨ Conversion complete!")
    print(f"   Output directory: {PARQUET_DIR}")


if __name__ == "__main__":
    main()
