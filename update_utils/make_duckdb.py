"""
Build a consolidated DuckDB database from Polymarket data sources.
RESUMABLE: if interrupted, re-run and it picks up where it left off.

Usage:
    uv run python update_utils/make_duckdb.py

To force a full rebuild:
    uv run python update_utils/make_duckdb.py --fresh
"""

import os
import sys
import time
import duckdb

# =============================================================================
# Config
# =============================================================================
DB_PATH = "pm.duckdb"
MARKETS_PATH = "parquet/markets.parquet"
TRADES_PATH = "processed/trades.csv"
ORDERBOOK_PATH = "parquet/orderbook_snapshots_consolidated.parquet"

MEMORY_LIMIT = "24GB"
THREADS = 32
TEMP_DIR = "/tmp/duckdb_build"


def table_exists(con, name: str) -> bool:
    return con.sql(
        f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{name}'"
    ).fetchone()[0] > 0


def index_exists(con, name: str) -> bool:
    return con.sql(
        f"SELECT count(*) FROM duckdb_indexes() WHERE index_name = '{name}'"
    ).fetchone()[0] > 0


def build(fresh: bool = False):
    total_start = time.time()

    if fresh:
        for suffix in ["", ".wal"]:
            p = DB_PATH + suffix
            if os.path.exists(p):
                os.remove(p)
                print(f"Removed old {p}")

    os.makedirs(TEMP_DIR, exist_ok=True)

    con = duckdb.connect(DB_PATH)
    con.sql(f"SET memory_limit = '{MEMORY_LIMIT}'")
    con.sql(f"SET threads = {THREADS}")
    con.sql(f"SET temp_directory = '{TEMP_DIR}'")
    con.sql("SET preserve_insertion_order = false")

    # -----------------------------------------------------------------
    # 1. Markets
    # -----------------------------------------------------------------
    print("\n=== Markets ===")
    if table_exists(con, "markets"):
        count = con.sql("SELECT count(*) FROM markets").fetchone()[0]
        print(f"  Already exists ({count:,} rows), skipping.")
    else:
        t0 = time.time()
        con.sql(f"""
            CREATE TABLE markets AS
            SELECT DISTINCT ON (id)
                CAST(id AS BIGINT) AS id,
                CAST(createdAt AS TIMESTAMP) AS created_at,
                CASE WHEN closedTime = '' THEN NULL
                     ELSE CAST(closedTime AS TIMESTAMP)
                END AS closed_at,
                question,
                answer1,
                answer2,
                neg_risk,
                market_slug,
                token1,
                token2,
                condition_id,
                volume,
                ticker
            FROM read_parquet('{MARKETS_PATH}')
            ORDER BY id
        """)
        count = con.sql("SELECT count(*) FROM markets").fetchone()[0]
        print(f"  Loaded {count:,} rows in {time.time() - t0:.1f}s")

    for idx, cols in [
        ("idx_markets_id", "id"),
        ("idx_markets_token1", "token1"),
        ("idx_markets_token2", "token2"),
    ]:
        if index_exists(con, idx):
            print(f"  Index {idx} already exists, skipping.")
        else:
            t0 = time.time()
            unique = "UNIQUE " if idx == "idx_markets_id" else ""
            con.sql(f"CREATE {unique}INDEX {idx} ON markets ({cols})")
            print(f"  Created {idx} in {time.time() - t0:.1f}s")

    # -----------------------------------------------------------------
    # 2. Trades
    # -----------------------------------------------------------------
    print("\n=== Trades ===")
    if table_exists(con, "trades"):
        count = con.sql("SELECT count(*) FROM trades").fetchone()[0]
        print(f"  Already exists ({count:,} rows), skipping.")
    else:
        t0 = time.time()
        con.sql(f"""
            CREATE TABLE trades AS
            SELECT
                CAST(timestamp AS TIMESTAMP) AS timestamp,
                CAST(market_id AS BIGINT) AS market_id,
                maker,
                taker,
                nonusdc_side,
                maker_direction,
                taker_direction,
                price,
                usd_amount,
                token_amount,
                transactionHash AS tx_hash
            FROM read_csv('{TRADES_PATH}', header = true, auto_detect = true)
        """)
        count = con.sql("SELECT count(*) FROM trades").fetchone()[0]
        print(f"  Loaded {count:,} rows in {time.time() - t0:.1f}s")

    for idx, cols in [
        ("idx_trades_market_ts", "market_id, timestamp"),
        ("idx_trades_ts", "timestamp"),
    ]:
        if index_exists(con, idx):
            print(f"  Index {idx} already exists, skipping.")
        else:
            t0 = time.time()
            con.sql(f"CREATE INDEX {idx} ON trades ({cols})")
            print(f"  Created {idx} in {time.time() - t0:.1f}s")

    # -----------------------------------------------------------------
    # 3. Orderbook Snapshots
    # -----------------------------------------------------------------
    print("\n=== Orderbook Snapshots ===")
    if table_exists(con, "orderbook"):
        count = con.sql("SELECT count(*) FROM orderbook").fetchone()[0]
        print(f"  Already exists ({count:,} rows), skipping.")
    else:
        t0 = time.time()
        con.sql(f"""
            CREATE TABLE orderbook AS
            SELECT
                asset_id,
                market AS condition_id,
                epoch_ms(timestamp) AS timestamp,
                hash,
                bids_json,
                asks_json,
                num_bid_levels,
                num_ask_levels,
                best_bid,
                best_ask,
                spread,
                total_bid_size,
                total_ask_size,
                min_order_size,
                tick_size,
                neg_risk
            FROM read_parquet('{ORDERBOOK_PATH}')
        """)
        count = con.sql("SELECT count(*) FROM orderbook").fetchone()[0]
        print(f"  Loaded {count:,} rows in {time.time() - t0:.1f}s")

    if index_exists(con, "idx_ob_asset_ts"):
        print("  Index idx_ob_asset_ts already exists, skipping.")
    else:
        print("  Creating index (this will take a while for 5.6B rows)...")
        t0 = time.time()
        con.sql("CREATE INDEX idx_ob_asset_ts ON orderbook (asset_id, timestamp)")
        print(f"  Created idx_ob_asset_ts in {time.time() - t0:.1f}s")

    # -----------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("=== Final Summary ===")
    for table in ["markets", "trades", "orderbook"]:
        if table_exists(con, table):
            count = con.sql(f"SELECT count(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count:,} rows")
        else:
            print(f"  {table}: NOT CREATED")

    print("\n=== Column Types ===")
    for table in ["markets", "trades", "orderbook"]:
        if table_exists(con, table):
            print(f"\n  {table}:")
            cols = con.sql(f"DESCRIBE {table}").fetchall()
            for name, dtype, *_ in cols:
                print(f"    {name}: {dtype}")

    con.close()

    size_gb = os.path.getsize(DB_PATH) / (1024**3)
    print(f"\n  Database: {DB_PATH} ({size_gb:.1f} GB)")
    print(f"  Total time: {time.time() - total_start:.0f}s")
    print("=" * 60)


if __name__ == "__main__":
    fresh = "--fresh" in sys.argv
    build(fresh=fresh)