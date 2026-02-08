#!/usr/bin/env python3
"""
Build a DuckDB with ONLY markets + trades.
RESUMABLE: if interrupted, re-run and it picks up where it left off.

Logic:
- markets table from markets_with_tags.parquet
- trades table includes ONLY rows where trades.market_id exists in markets.id
  (no timestamp cutoff)

Usage:
    uv run python update_utils/make_duckdb.py

Force full rebuild:
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

MARKETS_PATH = "parquet/markets_with_tags.parquet"
TRADES_GLOB = "parquet/trades/year_month=*/data_*.parquet"

MEMORY_LIMIT = "48GB"
THREADS = 24
TEMP_DIR = "/tmp/duckdb_build"


def table_exists(con, name: str) -> bool:
    return con.sql(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        params=[name],
    ).fetchone()[0] > 0


def index_exists(con, name: str) -> bool:
    return con.sql(
        "SELECT count(*) FROM duckdb_indexes() WHERE index_name = ?",
        params=[name],
    ).fetchone()[0] > 0


def pct(n: int, d: int) -> float:
    return (100.0 * n / d) if d else 0.0


def build(fresh: bool = False):
    total_start = time.time()

    if fresh:
        for suffix in ("", ".wal"):
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
    # 1) Markets
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
                CAST(CAST(createdAt AS TIMESTAMPTZ) AS TIMESTAMP) AS created_at,
                CASE
                    WHEN closedTime IS NULL OR closedTime = '' THEN NULL
                    ELSE CAST(CAST(closedTime AS TIMESTAMPTZ) AS TIMESTAMP)
                END AS closed_at,
                question,
                answer1,
                answer2,
                neg_risk,
                market_slug,
                token1,
                token2,
                condition_id,
                CAST(volume AS DOUBLE) AS volume,
                ticker,
                tag_slugs
            FROM read_parquet('{MARKETS_PATH}')
            ORDER BY id
        """)
        count = con.sql("SELECT count(*) FROM markets").fetchone()[0]
        print(f"  Loaded {count:,} rows in {time.time() - t0:.1f}s")

    for idx, cols in [
        ("idx_markets_id", "id"),
        ("idx_markets_token1", "token1"),
        ("idx_markets_token2", "token2"),
        ("idx_markets_created_at", "created_at"),
    ]:
        if index_exists(con, idx):
            print(f"  Index {idx} already exists, skipping.")
        else:
            t0 = time.time()
            unique = "UNIQUE " if idx == "idx_markets_id" else ""
            con.sql(f"CREATE {unique}INDEX {idx} ON markets ({cols})")
            print(f"  Created {idx} in {time.time() - t0:.1f}s")

    # -----------------------------------------------------------------
    # 2) Trades (ONLY where market exists in markets)
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
                CAST(CAST(t.timestamp AS TIMESTAMPTZ) AS TIMESTAMP) AS timestamp,
                CAST(t.market_id AS BIGINT) AS market_id,
                t.maker,
                t.taker,
                t.nonusdc_side,
                t.maker_direction,
                t.taker_direction,
                CAST(t.price AS DOUBLE) AS price,
                CAST(t.usd_amount AS DOUBLE) AS usd_amount,
                CAST(t.token_amount AS DOUBLE) AS token_amount,
                t.transactionHash AS tx_hash
            FROM read_parquet('{TRADES_GLOB}', hive_partitioning=true) t
            INNER JOIN markets m
              ON CAST(t.market_id AS BIGINT) = m.id
        """)
        count = con.sql("SELECT count(*) FROM trades").fetchone()[0]
        print(f"  Loaded {count:,} rows in {time.time() - t0:.1f}s")

    for idx, cols in [
        ("idx_trades_market_ts", "market_id, timestamp"),
        ("idx_trades_ts", "timestamp"),
        ("idx_trades_tx_hash", "tx_hash"),
    ]:
        if index_exists(con, idx):
            print(f"  Index {idx} already exists, skipping.")
        else:
            t0 = time.time()
            con.sql(f"CREATE INDEX {idx} ON trades ({cols})")
            print(f"  Created {idx} in {time.time() - t0:.1f}s")

    # -----------------------------------------------------------------
    # 3) Dedup + Data Validation Checks
    # -----------------------------------------------------------------
    print("\n=== Dedup & Data Validation ===")

    markets_n = con.sql("SELECT count(*) FROM markets").fetchone()[0]
    trades_n = con.sql("SELECT count(*) FROM trades").fetchone()[0]

    # ---- Dedup checks
    markets_dup_ids = con.sql("""
        SELECT count(*) FROM (
            SELECT id
            FROM markets
            GROUP BY id
            HAVING count(*) > 1
        )
    """).fetchone()[0]

    trades_exact_dup_rows = con.sql("""
        SELECT coalesce(sum(cnt - 1), 0) FROM (
            SELECT
                timestamp, market_id, maker, taker, nonusdc_side,
                maker_direction, taker_direction, price, usd_amount,
                token_amount, tx_hash,
                count(*) AS cnt
            FROM trades
            GROUP BY
                timestamp, market_id, maker, taker, nonusdc_side,
                maker_direction, taker_direction, price, usd_amount,
                token_amount, tx_hash
            HAVING count(*) > 1
        )
    """).fetchone()[0]

    trades_key_dup_rows = con.sql("""
        SELECT coalesce(sum(cnt - 1), 0) FROM (
            SELECT
                tx_hash, market_id, timestamp, price, token_amount,
                count(*) AS cnt
            FROM trades
            GROUP BY tx_hash, market_id, timestamp, price, token_amount
            HAVING count(*) > 1
        )
    """).fetchone()[0]

    # ---- Core integrity checks
    orphan_trades = con.sql("""
        SELECT count(*)
        FROM trades t
        LEFT JOIN markets m ON t.market_id = m.id
        WHERE m.id IS NULL
    """).fetchone()[0]

    trades_null_ts = con.sql("SELECT count(*) FROM trades WHERE timestamp IS NULL").fetchone()[0]
    trades_null_market = con.sql("SELECT count(*) FROM trades WHERE market_id IS NULL").fetchone()[0]
    trades_null_tx = con.sql("SELECT count(*) FROM trades WHERE tx_hash IS NULL OR tx_hash = ''").fetchone()[0]

    trades_bad_price = con.sql("""
        SELECT count(*)
        FROM trades
        WHERE price IS NULL OR price < 0 OR price > 1
    """).fetchone()[0]

    trades_bad_size = con.sql("""
        SELECT count(*)
        FROM trades
        WHERE token_amount IS NULL OR token_amount < 0
    """).fetchone()[0]

    trades_bad_usd = con.sql("""
        SELECT count(*)
        FROM trades
        WHERE usd_amount IS NULL OR usd_amount < 0
    """).fetchone()[0]

    # nonusdc_side should usually be one of market token1/token2
    nonusdc_not_in_tokens = con.sql("""
        SELECT count(*)
        FROM trades t
        JOIN markets m ON t.market_id = m.id
        WHERE t.nonusdc_side IS NOT NULL
          AND t.nonusdc_side <> ''
          AND t.nonusdc_side NOT IN (m.token1, m.token2)
    """).fetchone()[0]

    trade_min_ts, trade_max_ts = con.sql("""
        SELECT min(timestamp), max(timestamp) FROM trades
    """).fetchone()
    market_min_ts, market_max_ts = con.sql("""
        SELECT min(ts), max(ts) FROM (
            SELECT created_at AS ts FROM markets
            UNION ALL
            SELECT closed_at  AS ts FROM markets WHERE closed_at IS NOT NULL
        )
    """).fetchone()

    print(f"  markets rows: {markets_n:,}")
    print(f"  trades rows : {trades_n:,}")

    print("\n  [Dedup]")
    print(f"    markets duplicate ids                : {markets_dup_ids:,}")
    print(f"    trades exact duplicate extra rows    : {trades_exact_dup_rows:,}")
    print(f"    trades key-duplicate extra rows      : {trades_key_dup_rows:,}")

    print("\n  [Integrity]")
    print(f"    orphan trades                        : {orphan_trades:,}")
    print(f"    trades null timestamp                : {trades_null_ts:,}")
    print(f"    trades null market_id                : {trades_null_market:,}")
    print(f"    trades null/empty tx_hash            : {trades_null_tx:,}")
    print(f"    trades bad price (<0 or >1 or null)  : {trades_bad_price:,}")
    print(f"    trades bad token_amount (<0/null)    : {trades_bad_size:,}")
    print(f"    trades bad usd_amount (<0/null)      : {trades_bad_usd:,}")
    print(
        f"    nonusdc_side not in (token1,token2)  : "
        f"{nonusdc_not_in_tokens:,} ({pct(nonusdc_not_in_tokens, trades_n):.4f}%)"
    )

    print("\n  [Coverage]")
    print(f"    trades timestamp range               : {trade_min_ts} -> {trade_max_ts}")
    print(f"    markets time range                   : {market_min_ts} -> {market_max_ts}")

    # -----------------------------------------------------------------
    # 4) Final summary
    # -----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("=== Final Summary ===")
    for table in ("markets", "trades"):
        if table_exists(con, table):
            count = con.sql(f"SELECT count(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count:,} rows")
        else:
            print(f"  {table}: NOT CREATED")

    print("\n=== Column Types ===")
    for table in ("markets", "trades"):
        if table_exists(con, table):
            print(f"\n  {table}:")
            for name, dtype, *_ in con.sql(f"DESCRIBE {table}").fetchall():
                print(f"    {name}: {dtype}")

    con.close()

    size_gb = os.path.getsize(DB_PATH) / (1024**3)
    print(f"\n  Database: {DB_PATH} ({size_gb:.2f} GB)")
    print(f"  Total time: {time.time() - total_start:.0f}s")
    print("=" * 60)


if __name__ == "__main__":
    build(fresh="--fresh" in sys.argv)
