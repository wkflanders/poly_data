"""
build_db.py — Convert poly_data into a single queryable DuckDB database.
Phase 3: orjson + numpy bulk + pyarrow streaming writes.
Worker memory bounded by BATCH_ROWS regardless of market size.

Deps:  uv add orjson zstandard pyarrow numpy
"""
import os
import sys
import io
import time
import shutil
import argparse
import threading
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from concurrent.futures.process import BrokenProcessPool

import duckdb
import orjson
import zstandard as zstd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# =============================================================================
# Config
# =============================================================================
ROOT = Path(".")
DB_PATH      = ROOT / "polydata.duckdb"
MARKETS_CSV  = ROOT / "markets.csv"
TRADES_CSV   = ROOT / "processed" / "trades.csv"
SNAP_DIR     = ROOT / "orderbook_snapshots" / "data"
TRADES_PARQ  = ROOT / "trades_parquet"
OB_FULL_PARQ = ROOT / "orderbook_full_parquet"
TMP_DIR      = ROOT / ".duckdb_tmp"

DUCKDB_THREADS         = 16
DUCKDB_MEMORY_LIMIT    = "24GB"
OB_PARALLEL_WORKERS    = 16
PARQUET_ROW_GROUP_FULL = 50_000
BATCH_ROWS             = 50_000

_LADDER_TYPE = pa.list_(pa.struct([
    ("price", pa.float64()),
    ("size",  pa.float64()),
]))

_SCHEMA = pa.schema([
    ("market_id",       pa.int64()),
    ("asset_id",        pa.string()),
    ("ts_ms",           pa.int64()),
    ("ts",              pa.timestamp("ms")),
    ("hash",            pa.string()),
    ("bids",            _LADDER_TYPE),
    ("asks",            _LADDER_TYPE),
    ("min_order_size",  pa.float64()),
    ("tick_size",       pa.float64()),
    ("neg_risk",        pa.bool_()),
    ("best_bid",        pa.float64()),
    ("best_ask",        pa.float64()),
    ("best_bid_size",   pa.float64()),
    ("best_ask_size",   pa.float64()),
    ("bid_levels",      pa.int64()),
    ("ask_levels",      pa.int64()),
    ("bid_size_total",  pa.float64()),
    ("ask_size_total",  pa.float64()),
])

# =============================================================================
# Logging
# =============================================================================
_print_lock = threading.Lock()
def tprint(*a, **k):
    with _print_lock:
        print(*a, **k, flush=True)

def banner(s):
    tprint("=" * 70); tprint(s); tprint("=" * 70)

# =============================================================================
# Phase 1 — markets
# =============================================================================
def build_markets():
    banner("Phase 1: markets table")
    if not MARKETS_CSV.exists():
        tprint(f"  ERROR: {MARKETS_CSV} not found"); return

    TMP_DIR.mkdir(exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"PRAGMA threads={DUCKDB_THREADS};")
    con.execute(f"PRAGMA temp_directory='{TMP_DIR.as_posix()}';")
    con.execute(f"""
        CREATE OR REPLACE TABLE markets AS
        SELECT
            CAST(id AS BIGINT) AS market_id,
            CASE WHEN createdAt IS NULL OR createdAt = ''
                 THEN NULL ELSE CAST(createdAt AS TIMESTAMP) END AS created_at,
            question, answer1, answer2,
            TRY_CAST(neg_risk AS BOOLEAN) AS neg_risk,
            market_slug, token1, token2, condition_id,
            TRY_CAST(volume AS DOUBLE) AS volume,
            ticker,
            CASE WHEN closedTime IS NULL OR closedTime = ''
                 THEN NULL ELSE CAST(closedTime AS TIMESTAMPTZ) END AS closed_time,
            tags
        FROM read_csv_auto('{MARKETS_CSV}', header=true,
                           types={{'id':'BIGINT','token1':'VARCHAR','token2':'VARCHAR',
                                   'createdAt':'VARCHAR','closedTime':'VARCHAR'}});
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_markets_token1    ON markets(token1);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_markets_market_id ON markets(market_id);")
    n = con.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
    con.close()
    tprint(f"  loaded {n:,} markets")

# =============================================================================
# Phase 2 — trades
# =============================================================================
def build_trades():
    banner("Phase 2: trades.csv → partitioned parquet")
    if not TRADES_CSV.exists():
        tprint(f"  ERROR: {TRADES_CSV} not found"); return

    sentinel = TRADES_PARQ / "_SUCCESS"
    if sentinel.exists():
        tprint(f"  already done. Delete {sentinel} to re-run."); return

    # Sentinel missing — either first run or a previous run was interrupted.
    # Wipe any partial output so we don't accumulate duplicate UUID-named
    # files in the same partition directories on rerun.
    if TRADES_PARQ.exists():
        shutil.rmtree(TRADES_PARQ)
    TRADES_PARQ.mkdir(exist_ok=True)
    TMP_DIR.mkdir(exist_ok=True)

    con = duckdb.connect(":memory:")
    con.execute(f"PRAGMA threads={DUCKDB_THREADS};")
    con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}';")
    con.execute(f"PRAGMA temp_directory='{TMP_DIR.as_posix()}';")
    con.execute("SET preserve_insertion_order=false;")

    t0 = time.time()
    con.execute(f"""
        COPY (
            SELECT
                CAST(timestamp AS TIMESTAMP)               AS ts,
                CAST(market_id AS BIGINT)                  AS market_id,
                maker, taker, nonusdc_side,
                maker_direction, taker_direction,
                CAST(price AS DOUBLE)                      AS price,
                CAST(usd_amount AS DOUBLE)                 AS usd_amount,
                CAST(token_amount AS DOUBLE)               AS token_amount,
                transactionHash                            AS tx_hash,
                CAST(EXTRACT(year  FROM CAST(timestamp AS TIMESTAMP)) AS INT) AS year,
                CAST(EXTRACT(month FROM CAST(timestamp AS TIMESTAMP)) AS INT) AS month
            FROM read_csv_auto('{TRADES_CSV}', header=true)
        )
        TO '{TRADES_PARQ}' (FORMAT PARQUET, COMPRESSION ZSTD,
         PARTITION_BY (year, month), OVERWRITE_OR_IGNORE,
         FILENAME_PATTERN 'trades_{{uuid}}');
    """)
    elapsed = time.time() - t0

    removed = 0
    for p in TRADES_PARQ.rglob("*.parquet"):
        try:
            sz = p.stat().st_size
            if sz < 12: p.unlink(); removed += 1; continue
            with open(p, "rb") as f:
                head = f.read(4); f.seek(-4, 2); tail = f.read(4)
            if head != b"PAR1" or tail != b"PAR1":
                p.unlink(); removed += 1
        except FileNotFoundError: pass
    if removed: tprint(f"  cleaned up {removed} corrupt parquet(s)")

    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{TRADES_PARQ}/**/*.parquet', hive_partitioning=true)").fetchone()[0]
    con.close()
    sentinel.touch()
    tprint(f"  wrote {n:,} trades in {elapsed/60:.1f} min")

# =============================================================================
# Phase 3 — orderbook (orjson + numpy bulk + pyarrow streaming)
# =============================================================================
def _flush_batch(writer, tmp_path, market_id, buf):
    n = len(buf['asset_ids'])
    if n == 0:
        return writer

    if buf['bid_prices_chunks']:
        bid_prices_np = np.concatenate(buf['bid_prices_chunks'])
        bid_sizes_np  = np.concatenate(buf['bid_sizes_chunks'])
    else:
        bid_prices_np = np.empty(0, dtype=np.float64)
        bid_sizes_np  = np.empty(0, dtype=np.float64)

    if buf['ask_prices_chunks']:
        ask_prices_np = np.concatenate(buf['ask_prices_chunks'])
        ask_sizes_np  = np.concatenate(buf['ask_sizes_chunks'])
    else:
        ask_prices_np = np.empty(0, dtype=np.float64)
        ask_sizes_np  = np.empty(0, dtype=np.float64)

    bid_off_np = np.asarray(buf['bid_offsets'], dtype=np.int32)
    ask_off_np = np.asarray(buf['ask_offsets'], dtype=np.int32)
    ts_ms_np   = np.asarray(buf['ts_ms'], dtype=np.int64)

    bids_struct = pa.StructArray.from_arrays(
        [pa.array(bid_prices_np), pa.array(bid_sizes_np)], names=["price", "size"])
    bids_list = pa.ListArray.from_arrays(pa.array(bid_off_np, type=pa.int32()), bids_struct)

    asks_struct = pa.StructArray.from_arrays(
        [pa.array(ask_prices_np), pa.array(ask_sizes_np)], names=["price", "size"])
    asks_list = pa.ListArray.from_arrays(pa.array(ask_off_np, type=pa.int32()), asks_struct)

    table = pa.table({
        "market_id":      pa.array(np.full(n, market_id, dtype=np.int64)),
        "asset_id":       pa.array(buf['asset_ids'], type=pa.string()),
        "ts_ms":          pa.array(ts_ms_np, type=pa.int64()),
        "ts":             pa.array(ts_ms_np, type=pa.timestamp("ms")),
        "hash":           pa.array(buf['hashes'], type=pa.string()),
        "bids":           bids_list,
        "asks":           asks_list,
        "min_order_size": pa.array(buf['min_order_sizes'], type=pa.float64()),
        "tick_size":      pa.array(buf['tick_sizes'],      type=pa.float64()),
        "neg_risk":       pa.array(buf['neg_risks'],       type=pa.bool_()),
        "best_bid":       pa.array(buf['best_bids'],       type=pa.float64()),
        "best_ask":       pa.array(buf['best_asks'],       type=pa.float64()),
        "best_bid_size":  pa.array(buf['best_bid_sizes'],  type=pa.float64()),
        "best_ask_size":  pa.array(buf['best_ask_sizes'],  type=pa.float64()),
        "bid_levels":     pa.array(buf['bid_levels'],      type=pa.int64()),
        "ask_levels":     pa.array(buf['ask_levels'],      type=pa.int64()),
        "bid_size_total": pa.array(buf['bid_size_totals'], type=pa.float64()),
        "ask_size_total": pa.array(buf['ask_size_totals'], type=pa.float64()),
    }, schema=_SCHEMA)

    if writer is None:
        writer = pq.ParquetWriter(str(tmp_path), _SCHEMA,
                                  compression="zstd", compression_level=1,
                                  use_dictionary=True)
    writer.write_table(table, row_group_size=PARQUET_ROW_GROUP_FULL)
    return writer


def _fresh_buffers():
    return {
        'asset_ids': [], 'ts_ms': [], 'hashes': [],
        # ladder data: chunked numpy arrays, concatenated at flush
        'bid_prices_chunks': [], 'bid_sizes_chunks': [],
        'ask_prices_chunks': [], 'ask_sizes_chunks': [],
        'bid_offsets': [0], 'ask_offsets': [0],
        'min_order_sizes': [], 'tick_sizes': [], 'neg_risks': [],
        'best_bids': [], 'best_asks': [],
        'best_bid_sizes': [], 'best_ask_sizes': [],
        'bid_levels': [], 'ask_levels': [],
        'bid_size_totals': [], 'ask_size_totals': [],
    }


def _output_path_for(zst_path: Path) -> Path:
    market_id = int(zst_path.name.replace(".jsonl.zst", ""))
    return OB_FULL_PARQ / f"market_id={market_id}" / "data.parquet"


def convert_one_market(zst_path_str: str) -> tuple[int, str, int]:
    """Returns (rows_written, status, bad_lines_skipped)."""
    zst_path = Path(zst_path_str)
    market_id = int(zst_path.name.replace(".jsonl.zst", ""))
    full_dir = OB_FULL_PARQ / f"market_id={market_id}"
    full_out = full_dir / "data.parquet"

    # Defensive: parent already filters these, but keep the guard in case of
    # races, partial runs, or direct calls.
    if full_out.exists():
        return (0, "skip", 0)

    full_dir.mkdir(parents=True, exist_ok=True)
    tmp_full = full_out.with_suffix(".parquet.tmp")

    loads = orjson.loads
    dctx = zstd.ZstdDecompressor()

    writer = None
    total_rows = 0
    bad_lines = 0

    buf = _fresh_buffers()
    cur_bid_off = 0
    cur_ask_off = 0

    try:
        with open(zst_path, "rb") as f:
            # CRITICAL: read_across_frames=True. update_orderbook.py writes one
            # zstd frame per API page (multi-frame .zst), and the python-zstandard
            # default of False silently stops at frame 1, dropping the rest.
            with dctx.stream_reader(f, read_across_frames=True) as stream:
                reader = io.BufferedReader(stream, buffer_size=1 << 20)

                for line in reader:
                    if not line or line == b"\n":
                        continue
                    try:
                        rec = loads(line)
                    except orjson.JSONDecodeError:
                        bad_lines += 1
                        continue
                    except Exception:
                        bad_lines += 1
                        continue

                    buf['asset_ids'].append(rec.get("asset_id"))
                    buf['ts_ms'].append(int(rec["timestamp"]))
                    buf['hashes'].append(rec.get("hash"))

                    # --- BIDS — bulk numpy conversion ---
                    raw_bids = rec.get("bids") or []
                    n_bids = len(raw_bids)
                    if n_bids:
                        prices_np = np.array([b["price"] for b in raw_bids], dtype=np.float64)
                        sizes_np  = np.array([b["size"]  for b in raw_bids], dtype=np.float64)
                        buf['bid_prices_chunks'].append(prices_np)
                        buf['bid_sizes_chunks'].append(sizes_np)
                        bb_idx = int(prices_np.argmax())
                        buf['best_bids'].append(float(prices_np[bb_idx]))
                        buf['best_bid_sizes'].append(float(sizes_np[bb_idx]))
                        buf['bid_size_totals'].append(float(sizes_np.sum()))
                        cur_bid_off += n_bids
                    else:
                        buf['best_bids'].append(None)
                        buf['best_bid_sizes'].append(None)
                        buf['bid_size_totals'].append(0.0)
                    buf['bid_offsets'].append(cur_bid_off)
                    buf['bid_levels'].append(n_bids)

                    # --- ASKS ---
                    raw_asks = rec.get("asks") or []
                    n_asks = len(raw_asks)
                    if n_asks:
                        prices_np = np.array([a["price"] for a in raw_asks], dtype=np.float64)
                        sizes_np  = np.array([a["size"]  for a in raw_asks], dtype=np.float64)
                        buf['ask_prices_chunks'].append(prices_np)
                        buf['ask_sizes_chunks'].append(sizes_np)
                        ba_idx = int(prices_np.argmin())
                        buf['best_asks'].append(float(prices_np[ba_idx]))
                        buf['best_ask_sizes'].append(float(sizes_np[ba_idx]))
                        buf['ask_size_totals'].append(float(sizes_np.sum()))
                        cur_ask_off += n_asks
                    else:
                        buf['best_asks'].append(None)
                        buf['best_ask_sizes'].append(None)
                        buf['ask_size_totals'].append(0.0)
                    buf['ask_offsets'].append(cur_ask_off)
                    buf['ask_levels'].append(n_asks)

                    mos = rec.get("min_order_size")
                    buf['min_order_sizes'].append(float(mos) if mos not in (None, "") else None)
                    ts_ = rec.get("tick_size")
                    buf['tick_sizes'].append(float(ts_) if ts_ not in (None, "") else None)
                    buf['neg_risks'].append(rec.get("neg_risk"))

                    total_rows += 1

                    if len(buf['asset_ids']) >= BATCH_ROWS:
                        writer = _flush_batch(writer, tmp_full, market_id, buf)
                        buf = _fresh_buffers()
                        cur_bid_off = 0
                        cur_ask_off = 0

        if len(buf['asset_ids']) > 0:
            writer = _flush_batch(writer, tmp_full, market_id, buf)
            buf = _fresh_buffers()

        if writer is None:
            empty = pa.table({c: [] for c in _SCHEMA.names}, schema=_SCHEMA)
            pq.write_table(empty, str(tmp_full),
                           compression="zstd", compression_level=1,
                           row_group_size=PARQUET_ROW_GROUP_FULL)
        else:
            writer.close()
            writer = None

        os.replace(tmp_full, full_out)

        if total_rows > 0:
            status = "ok_with_bad" if bad_lines else "ok"
        else:
            status = "empty"
        return (total_rows, status, bad_lines)

    finally:
        if writer is not None:
            try: writer.close()
            except Exception: pass
        if tmp_full.exists():
            try: tmp_full.unlink()
            except FileNotFoundError: pass


def _sweep_tmp_files():
    if not OB_FULL_PARQ.exists():
        return
    n = 0
    for p in OB_FULL_PARQ.rglob("*.parquet.tmp"):
        try: p.unlink(); n += 1
        except FileNotFoundError: pass
    if n: tprint(f"  swept {n} stale .parquet.tmp file(s)")


def _human_rate(rows_per_sec):
    if rows_per_sec >= 1e6:
        return f"{rows_per_sec/1e6:.1f}M rows/s"
    if rows_per_sec >= 1e3:
        return f"{rows_per_sec/1e3:.0f}K rows/s"
    return f"{rows_per_sec:.0f} rows/s"


def build_orderbook(workers: int = OB_PARALLEL_WORKERS):
    banner(f"Phase 3: orderbook → parquet  ({workers} workers)")
    if not SNAP_DIR.exists():
        tprint(f"  ERROR: {SNAP_DIR} not found"); return

    OB_FULL_PARQ.mkdir(parents=True, exist_ok=True)
    _sweep_tmp_files()

    # Filter already-done markets in the parent so we don't pay IPC overhead
    # to ship tens of thousands of no-op tasks through the worker pool.
    all_files = sorted(SNAP_DIR.glob("*.jsonl.zst"),
                       key=lambda p: p.stat().st_size, reverse=True)
    files = [f for f in all_files if not _output_path_for(f).exists()]
    already_done = len(all_files) - len(files)
    total = len(files)

    tprint(f"  {len(all_files):,} markets total, {already_done:,} already done, "
           f"{total:,} to process (largest first — whales process first, "
           f"rate climbs sharply after first ~100-200 markets)")

    if total == 0:
        tprint("  nothing to do"); return

    t0 = time.time()
    rows = ok = skip = empty = err = 0
    bad_total = 0
    bad_files = 0
    last_log = t0

    pending = list(files)
    INFLIGHT_TARGET = max(workers * 4, workers + 8)

    def _new_pool():
        return ProcessPoolExecutor(max_workers=workers, max_tasks_per_child=500)

    ex = _new_pool()
    in_flight: dict = {}

    def _log_now():
        nonlocal last_log
        done_count = ok + skip + empty + err
        now = time.time()
        if now - last_log >= 5 or done_count == total:
            last_log = now
            elapsed = now - t0
            mkt_rate = done_count / elapsed if elapsed else 0
            row_rate = rows / elapsed if elapsed else 0
            eta_min = (total - done_count) / mkt_rate / 60 if mkt_rate else 0
            bad_str = f" bad_lines={bad_total} ({bad_files}f)" if bad_total else ""
            tprint(f"  [{done_count:>6}/{total}] ok={ok} skip={skip} "
                   f"empty={empty} err={err}{bad_str}  rows={rows:,}  "
                   f"{_human_rate(row_rate)}  {mkt_rate:.1f} mkt/s  "
                   f"ETA~{eta_min:.0f}min")

    while pending or in_flight:
        try:
            while len(in_flight) < INFLIGHT_TARGET and pending:
                f = pending.pop(0)
                try:
                    in_flight[ex.submit(convert_one_market, str(f))] = f
                except BrokenProcessPool:
                    pending.insert(0, f); raise

            if not in_flight:
                continue

            done, _pending = wait(in_flight, return_when=FIRST_COMPLETED)
            for fut in done:
                f = in_flight.pop(fut)
                try:
                    n, status, bad = fut.result()
                except BrokenProcessPool:
                    err += 1
                    if err <= 30:
                        tprint(f"  CRASH {f.name} ({f.stat().st_size:,}B): worker died")
                except Exception as e:
                    err += 1
                    if err <= 30:
                        tprint(f"  ERROR {f.name}: {type(e).__name__}: {e}")
                else:
                    if   status == "skip":  skip += 1
                    elif status == "empty": empty += 1
                    else:
                        ok += 1
                        rows += n
                        if bad:
                            bad_total += bad
                            bad_files += 1
                            if bad_files <= 30:
                                tprint(f"  WARN  {f.name}: skipped {bad} corrupt line(s)")
                _log_now()
        except BrokenProcessPool:
            lost = list(in_flight.values())
            tprint(f"  POOL POISONED — recreating, requeuing {len(lost)} markets")
            for f in lost: pending.insert(0, f)
            in_flight.clear()
            try: ex.shutdown(wait=False, cancel_futures=True)
            except Exception: pass
            ex = _new_pool()

    try: ex.shutdown()
    except Exception: pass

    full_gb = sum(p.stat().st_size for p in OB_FULL_PARQ.rglob("*.parquet")) / 1e9
    tprint(f"  done in {(time.time()-t0)/60:.1f} min")
    if bad_total:
        tprint(f"  WARNING: skipped {bad_total} corrupt line(s) across {bad_files} file(s)")
    tprint(f"  orderbook_full_parquet: {full_gb:.1f} GB")

# =============================================================================
# Phase 4 — views
# =============================================================================
def build_views():
    banner("Phase 4: views")
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"PRAGMA threads={DUCKDB_THREADS};")
    con.execute(f"""
        CREATE OR REPLACE VIEW trades AS
        SELECT * EXCLUDE (year, month)
        FROM read_parquet('{TRADES_PARQ.as_posix()}/**/*.parquet', hive_partitioning=true);
    """)
    con.execute(f"""
        CREATE OR REPLACE VIEW orderbook AS
        SELECT market_id, asset_id, ts, ts_ms, hash, bids, asks,
               min_order_size, tick_size, neg_risk
        FROM read_parquet('{OB_FULL_PARQ.as_posix()}/**/*.parquet', hive_partitioning=true);
    """)
    con.execute(f"""
        CREATE OR REPLACE VIEW orderbook_top AS
        SELECT
            market_id, asset_id, ts, ts_ms, hash,
            best_bid, best_ask, best_bid_size, best_ask_size,
            (best_ask - best_bid)        AS spread,
            (best_ask + best_bid) / 2.0  AS mid,
            bid_levels, ask_levels,
            bid_size_total, ask_size_total
        FROM read_parquet('{OB_FULL_PARQ.as_posix()}/**/*.parquet', hive_partitioning=true);
    """)
    con.close()
    tprint("  registered views: trades, orderbook, orderbook_top")

# =============================================================================
# Phase 5 — verify
# =============================================================================
def verify():
    banner("Phase 5: verify")
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"PRAGMA threads={DUCKDB_THREADS};")
    def safe(q):
        try: return con.execute(q).fetchone()[0]
        except Exception: return "(missing)"
    fmt = lambda v: f"{v:,}" if isinstance(v, int) else v
    tprint(f"  markets:        {fmt(safe('SELECT COUNT(*) FROM markets'))}")
    tprint(f"  trades:         {fmt(safe('SELECT COUNT(*) FROM trades'))}")
    tprint(f"  orderbook:      {fmt(safe('SELECT COUNT(*) FROM orderbook'))}")
    con.close()


PHASES = {"markets": build_markets, "trades": build_trades,
          "orderbook": None, "views": build_views, "verify": verify}

def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("phases", nargs="*", default=["all"])
    p.add_argument("--workers", type=int, default=OB_PARALLEL_WORKERS)
    args = p.parse_args()

    valid = set(PHASES) | {"all"}
    bad = [x for x in args.phases if x not in valid]
    if bad: sys.exit(f"unknown phases: {bad}; valid: {sorted(valid)}")

    phases = list(PHASES) if "all" in args.phases else args.phases
    for phase in phases:
        if phase == "orderbook": build_orderbook(workers=args.workers)
        else: PHASES[phase]()

if __name__ == "__main__":
    main()
