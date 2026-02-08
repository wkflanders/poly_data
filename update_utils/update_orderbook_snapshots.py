"""
Fetch historical orderbook snapshots from Polymarket CLOB API (async, resumable, sharded, asset-throughput optimized).

Usage:
    uv run python update_utils/update_orderbook_snapshots.py

Run multiple processes on ONE machine safely via sharding:
    export NUM_SHARDS=8
    export SHARD_ID=0   # 0..NUM_SHARDS-1 (different per process)

Each shard writes to:
    parquet/orderbook_snapshots_shard_<SHARD_ID>/year_month=YYYY-MM/*.parquet
and progress:
    parquet/orderbook_snapshots_shard_<SHARD_ID>/.progress.json

Env overrides (optional):
    export WORKERS=70
    export FLUSH_EVERY=100000
    export PROGRESS_SAVE_INTERVAL_SEC=30
    export MAX_PAGES_PER_PASS=3

Notes:
- Ctrl-C once = graceful stop (flush + checkpoint + save progress)
- Ctrl-C twice = force cancel
- Resume: rerun the same shard (same SHARD_ID/NUM_SHARDS), continues from next_ts
"""

from __future__ import annotations

import asyncio
import aiohttp
import hashlib
import json
import os
import random
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

# =============================================================================
# Sharding config
# =============================================================================
NUM_SHARDS = int(os.getenv("NUM_SHARDS", "1"))
SHARD_ID = int(os.getenv("SHARD_ID", "0"))
if NUM_SHARDS < 1:
    raise ValueError("NUM_SHARDS must be >= 1")
if not (0 <= SHARD_ID < NUM_SHARDS):
    raise ValueError(f"SHARD_ID must be in [0, {NUM_SHARDS - 1}]")


def stable_shard(asset_id: str, num_shards: int) -> int:
    # Stable across processes/runs (unlike Python's built-in hash())
    h = hashlib.blake2b(asset_id.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(h, "little") % num_shards


# =============================================================================
# Configuration
# =============================================================================
BASE_URL = "https://clob.polymarket.com/orderbook-history"
EARLIEST_SNAPSHOT_TS = 1762984233541  # ms

MARKETS_PARQUET = Path("parquet/markets.parquet")

# Shard-specific output location (so processes don't collide)
OUTPUT_DIR = (
    Path(f"parquet/orderbook_snapshots_shard_{SHARD_ID}")
    if NUM_SHARDS > 1
    else Path("parquet/orderbook_snapshots")
)
PROGRESS_FILE = OUTPUT_DIR / ".progress.json"

SNAPSHOTS_PER_REQUEST = 1000
FLUSH_EVERY = int(os.getenv("FLUSH_EVERY", "100000"))
WORKERS = int(os.getenv("WORKERS", "70"))
PROGRESS_SAVE_INTERVAL_SEC = float(os.getenv("PROGRESS_SAVE_INTERVAL_SEC", "30"))

# Time-slicing knob: higher = more pages per touch, lower = more “finish small ones”
MAX_PAGES_PER_PASS = int(os.getenv("MAX_PAGES_PER_PASS", "3"))

# Consolidation (per shard)
CONSOLIDATED_PARQUET = (
    OUTPUT_DIR.parent / f"orderbook_snapshots_consolidated_shard_{SHARD_ID}.parquet"
    if NUM_SHARDS > 1
    else Path("parquet/orderbook_snapshots_consolidated.parquet")
)
DELETE_SHARDS_AFTER_CONSOLIDATE = False

# =============================================================================
# Webshare rotating proxy (env-driven)
# =============================================================================
_WEBSHARE_USER = os.getenv("WEBSHARE_PROXY_USER", "").strip()
_WEBSHARE_PASS = os.getenv("WEBSHARE_PROXY_PASS", "").strip()
_WEBSHARE_HOST = os.getenv("WEBSHARE_PROXY_HOST", "").strip()
_WEBSHARE_PORT = os.getenv("WEBSHARE_PROXY_PORT", "").strip()

# If all env vars are present, use the proxy; otherwise run direct.
PROXY_URL = None
if _WEBSHARE_USER and _WEBSHARE_PASS and _WEBSHARE_HOST and _WEBSHARE_PORT:
    # aiohttp supports credentials in the proxy URL
    PROXY_URL = f"http://{_WEBSHARE_USER}:{_WEBSHARE_PASS}@{_WEBSHARE_HOST}:{_WEBSHARE_PORT}"

# =============================================================================
# Global state
# =============================================================================
_shutdown = False
_total_snapshots = 0
_total_assets_done = 0

_lock = asyncio.Lock()
_progress_dirty = False

_last_print = 0.0
_done_in_window = 0
_window_start = 0.0


# =============================================================================
# Progress I/O (atomic)
# =============================================================================
def load_progress() -> dict:
    """
    {
      "version": 1,
      "assets": {
        "<asset_id>": {
          "next_ts": <int ms>,
          "fetched": <int>,
          "done": <bool>,
          "remaining_est": <int>,
          "updated_at": "<iso>"
        }
      }
    }
    """
    if PROGRESS_FILE.exists():
        try:
            with open(PROGRESS_FILE, "r") as f:
                data = json.load(f)
        except Exception:
            return {"version": 1, "assets": {}}

        if not isinstance(data, dict):
            return {"version": 1, "assets": {}}

        data.setdefault("version", 1)
        if "assets" not in data or not isinstance(data["assets"], dict):
            data["assets"] = {}
        return data

    return {"version": 1, "assets": {}}


def _write_progress_payload(payload: str):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    tmp = PROGRESS_FILE.with_suffix(PROGRESS_FILE.suffix + ".tmp")
    with open(tmp, "w") as f:
        f.write(payload)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, PROGRESS_FILE)


async def autosave_progress(progress: dict, requested_stop: asyncio.Event):
    global _progress_dirty
    while not requested_stop.is_set():
        await asyncio.sleep(PROGRESS_SAVE_INTERVAL_SEC)

        payload = None
        async with _lock:
            if _progress_dirty:
                payload = json.dumps(progress)
                _progress_dirty = False

        if payload is not None:
            try:
                await asyncio.to_thread(_write_progress_payload, payload)
            except Exception as e:
                print(f"WARNING: autosave progress failed: {e}")

    # final dirty save
    payload = None
    async with _lock:
        if _progress_dirty:
            payload = json.dumps(progress)
            _progress_dirty = False

    if payload is not None:
        try:
            await asyncio.to_thread(_write_progress_payload, payload)
        except Exception as e:
            print(f"WARNING: final progress save failed: {e}")


async def force_save_progress(progress: dict):
    try:
        async with _lock:
            payload = json.dumps(progress)
            global _progress_dirty
            _progress_dirty = False
        await asyncio.to_thread(_write_progress_payload, payload)
    except Exception as e:
        print(f"WARNING: forced progress save failed: {e}")


async def checkpoint_progress(
    progress: dict,
    asset_id: str,
    *,
    next_ts: int,
    fetched: int,
    done: bool,
    remaining_est: int | None,
):
    global _progress_dirty
    async with _lock:
        entry = progress.setdefault("assets", {}).get(asset_id, {})
        entry.update(
            {
                "next_ts": int(next_ts),
                "fetched": int(fetched),
                "done": bool(done),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        if remaining_est is not None:
            entry["remaining_est"] = int(remaining_est)
        progress["assets"][asset_id] = entry
        _progress_dirty = True


# =============================================================================
# Markets / assets
# =============================================================================
def get_asset_ids_from_markets() -> list[dict]:
    if not MARKETS_PARQUET.exists():
        raise FileNotFoundError(f"Markets file not found: {MARKETS_PARQUET}")

    df = pl.read_parquet(MARKETS_PARQUET)
    print(f"Loaded {len(df)} markets")

    assets: list[dict] = []
    for row in df.iter_rows(named=True):
        asset_id = row.get("token1")
        if asset_id and str(asset_id) != "0":
            assets.append(
                {
                    "asset_id": str(asset_id),
                    "market_id": str(row.get("id", "")),
                    "created_at": row.get("createdAt", ""),
                    "closed_time": row.get("closedTime", ""),
                }
            )

    seen: set[str] = set()
    unique: list[dict] = []
    for a in assets:
        if a["asset_id"] not in seen:
            seen.add(a["asset_id"])
            unique.append(a)
    return unique


def filter_assets_by_time(assets: list[dict], cutoff_ts_ms: int) -> list[dict]:
    filtered: list[dict] = []
    cutoff_dt = datetime.fromtimestamp(cutoff_ts_ms / 1000, tz=timezone.utc)

    for a in assets:
        created_at = a.get("created_at")
        closed_time = a.get("closed_time")

        created_dt = None
        if created_at:
            try:
                if isinstance(created_at, str):
                    created_dt = datetime.fromisoformat(
                        created_at.replace("Z", "+00:00")
                    )
                elif isinstance(created_at, datetime):
                    created_dt = created_at
            except Exception:
                pass

        closed_dt = None
        if closed_time:
            try:
                if isinstance(closed_time, str):
                    closed_dt = datetime.fromisoformat(
                        closed_time.replace("Z", "+00:00")
                    )
                elif isinstance(closed_time, datetime):
                    closed_dt = closed_time
            except Exception:
                pass

        include = False
        if created_dt and created_dt >= cutoff_dt:
            include = True
        elif closed_dt is None:
            include = True
        elif closed_dt and closed_dt >= cutoff_dt:
            include = True

        if include:
            filtered.append(a)

    return filtered


# =============================================================================
# Fetching (WITH ROTATING PROXY if env vars set)
# =============================================================================
async def fetch_orderbook_page(
    session: aiohttp.ClientSession,
    asset_id: str,
    start_ts: int,
    retries: int = 5,
) -> tuple[int, list[dict]]:
    url = f"{BASE_URL}?asset_id={asset_id}&startTs={start_ts}&limit={SNAPSHOTS_PER_REQUEST}"

    for attempt in range(retries):
        if _shutdown:
            return 0, []

        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=30),
                proxy=PROXY_URL,  # <-- only change needed to use Webshare rotating endpoint
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    return int(data.get("count", 0) or 0), (data.get("data", []) or [])

                if r.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(2**attempt)
                    continue

                if r.status == 400:
                    return 0, []

                return 0, []

        except asyncio.TimeoutError:
            await asyncio.sleep(2**attempt)
        except aiohttp.ClientError:
            await asyncio.sleep(2**attempt)

    return 0, []


# =============================================================================
# Writing shards
# =============================================================================
def _best_from_endpoints(levels: list[dict], *, want: str) -> float | None:
    if not levels:
        return None
    if len(levels) == 1:
        return float(levels[0]["price"])
    p0 = float(levels[0]["price"])
    p1 = float(levels[-1]["price"])
    return max(p0, p1) if want == "max" else min(p0, p1)


def flatten_snapshot(snap: dict) -> dict:
    timestamp_ms = int(snap.get("timestamp", 0))
    timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    year_month = timestamp_dt.strftime("%Y-%m")

    bids = snap.get("bids", []) or []
    asks = snap.get("asks", []) or []

    best_bid = _best_from_endpoints(bids, want="max")
    best_ask = _best_from_endpoints(asks, want="min")

    total_bid_size = 0.0
    for b in bids:
        total_bid_size += float(b["size"])

    total_ask_size = 0.0
    for a in asks:
        total_ask_size += float(a["size"])

    return {
        "asset_id": snap.get("asset_id", ""),
        "market": snap.get("market", ""),
        "timestamp": timestamp_ms,
        "timestamp_dt": timestamp_dt,
        "year_month": year_month,
        "hash": snap.get("hash", ""),
        "bids_json": json.dumps(bids),
        "asks_json": json.dumps(asks),
        "num_bid_levels": len(bids),
        "num_ask_levels": len(asks),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": (best_ask - best_bid)
        if (best_bid is not None and best_ask is not None)
        else None,
        "total_bid_size": total_bid_size,
        "total_ask_size": total_ask_size,
        "min_order_size": snap.get("min_order_size", ""),
        "tick_size": snap.get("tick_size", ""),
        "neg_risk": bool(snap.get("neg_risk", False)),
    }


def write_buffer(buffer: list[dict]) -> int:
    if not buffer:
        return 0

    try:
        df = pl.DataFrame(
            buffer,
            schema={
                "asset_id": pl.Utf8,
                "market": pl.Utf8,
                "timestamp": pl.Int64,
                "timestamp_dt": pl.Datetime("us", "UTC"),
                "year_month": pl.Utf8,
                "hash": pl.Utf8,
                "bids_json": pl.Utf8,
                "asks_json": pl.Utf8,
                "num_bid_levels": pl.Int64,
                "num_ask_levels": pl.Int64,
                "best_bid": pl.Float64,
                "best_ask": pl.Float64,
                "spread": pl.Float64,
                "total_bid_size": pl.Float64,
                "total_ask_size": pl.Float64,
                "min_order_size": pl.Utf8,
                "tick_size": pl.Utf8,
                "neg_risk": pl.Boolean,
            },
        )

        total_written = 0
        for ym in df["year_month"].unique().to_list():
            part = df.filter(pl.col("year_month") == ym).drop("year_month")
            partition_dir = OUTPUT_DIR / f"year_month={ym}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            ts = int(time.time() * 1_000_000)
            out_file = partition_dir / f"data_{ts}.parquet"
            part.write_parquet(out_file, compression="zstd")
            total_written += len(part)

        return total_written

    except Exception as e:
        print(f"ERROR writing: {e}")
        return 0


def _glob_shard_files() -> list[Path]:
    if not OUTPUT_DIR.exists():
        return []
    return sorted(OUTPUT_DIR.glob("year_month=*/*.parquet"))


def consolidate_orderbook_snapshots(
    out_file: Path = CONSOLIDATED_PARQUET,
    delete_shards: bool = DELETE_SHARDS_AFTER_CONSOLIDATE,
) -> int:
    shard_files = _glob_shard_files()
    if not shard_files:
        print("No shard parquet files found to consolidate.")
        return 0

    out_file.parent.mkdir(parents=True, exist_ok=True)
    print(f"Consolidating {len(shard_files)} shard files -> {out_file}")

    lf = pl.scan_parquet([str(p) for p in shard_files])
    lf.sink_parquet(str(out_file), compression="zstd")

    if delete_shards:
        for p in shard_files:
            try:
                p.unlink()
            except Exception as e:
                print(f"WARNING: could not delete {p}: {e}")

        for d in sorted(OUTPUT_DIR.glob("year_month=*"), reverse=True):
            try:
                if d.is_dir() and not any(d.iterdir()):
                    d.rmdir()
            except Exception:
                pass

    print("Consolidation done.")
    return len(shard_files)


# =============================================================================
# Asset processing (time-sliced)
# =============================================================================
async def process_asset_slice(
    session: aiohttp.ClientSession,
    asset_id: str,
    progress: dict,
    max_pages: int,
) -> tuple[bool, int]:
    """
    Process up to max_pages for this asset. Returns:
      (done, remaining_est_for_priority_queue)

    remaining_est is best-effort based on API `count` and how much we fetched in this cursor.
    """
    global _total_snapshots

    st = progress.setdefault("assets", {}).get(asset_id)
    if not st:
        await checkpoint_progress(
            progress,
            asset_id,
            next_ts=EARLIEST_SNAPSHOT_TS,
            fetched=0,
            done=False,
            remaining_est=None,
        )
        st = progress["assets"][asset_id]

    if st.get("done"):
        return True, 0

    current_ts = int(st.get("next_ts", EARLIEST_SNAPSHOT_TS) or EARLIEST_SNAPSHOT_TS)
    fetched_total = int(st.get("fetched", 0) or 0)

    buffer: list[dict] = []
    pages = 0

    total_count_from_cursor = 0
    fetched_from_cursor = 0

    completed_normally = False
    remaining_est: int | None = None

    try:
        while not _shutdown and pages < max_pages:
            count, snapshots = await fetch_orderbook_page(session, asset_id, current_ts)

            if pages == 0:
                total_count_from_cursor = count
                if total_count_from_cursor == 0:
                    completed_normally = True
                    remaining_est = 0
                    return True, 0

            if not snapshots:
                completed_normally = True
                remaining_est = 0
                return True, 0

            pages += 1

            for snap in snapshots:
                buffer.append(flatten_snapshot(snap))

            batch_n = len(snapshots)
            fetched_total += batch_n
            fetched_from_cursor += batch_n

            last_ts = max(int(s.get("timestamp", 0)) for s in snapshots)
            if last_ts <= current_ts:
                completed_normally = True
                remaining_est = 0
                return True, 0
            current_ts = last_ts + 1

            if len(buffer) >= FLUSH_EVERY:
                written = write_buffer(buffer)
                buffer.clear()
                async with _lock:
                    _total_snapshots += written
                await checkpoint_progress(
                    progress,
                    asset_id,
                    next_ts=current_ts,
                    fetched=fetched_total,
                    done=False,
                    remaining_est=None,
                )

            if (
                total_count_from_cursor
                and fetched_from_cursor >= total_count_from_cursor
            ):
                completed_normally = True
                remaining_est = 0
                break

        if completed_normally:
            remaining_est = 0
        elif total_count_from_cursor:
            remaining_est = max(0, total_count_from_cursor - fetched_from_cursor)
        else:
            remaining_est = 10**12  # unknown

        return completed_normally, int(remaining_est)

    finally:
        if buffer:
            written = write_buffer(buffer)
            buffer.clear()
            async with _lock:
                _total_snapshots += written

        if completed_normally and not _shutdown:
            await checkpoint_progress(
                progress,
                asset_id,
                next_ts=current_ts,
                fetched=fetched_total,
                done=True,
                remaining_est=0,
            )
        else:
            await checkpoint_progress(
                progress,
                asset_id,
                next_ts=current_ts,
                fetched=fetched_total,
                done=False,
                remaining_est=remaining_est,
            )


# =============================================================================
# Workers (priority queue)
# =============================================================================
async def worker(
    pq: asyncio.PriorityQueue,
    session: aiohttp.ClientSession,
    progress: dict,
    total_assets_target: int,
    start_time: float,
):
    global \
        _total_assets_done, \
        _total_snapshots, \
        _last_print, \
        _done_in_window, \
        _window_start

    if _window_start == 0.0:
        _window_start = time.time()

    while not _shutdown:
        try:
            remaining_est, _, asset_id = pq.get_nowait()
        except asyncio.QueueEmpty:
            break

        try:
            done, new_remaining = await process_asset_slice(
                session=session,
                asset_id=asset_id,
                progress=progress,
                max_pages=MAX_PAGES_PER_PASS,
            )

            if not done and not _shutdown:
                pq.put_nowait((new_remaining, random.random(), asset_id))
            else:
                async with _lock:
                    _total_assets_done += 1
                    _done_in_window += 1

        finally:
            pq.task_done()

        now = time.time()
        if now - _last_print >= 5.0:
            async with _lock:
                elapsed = now - start_time
                avg_done_rate = _total_assets_done / elapsed if elapsed > 0 else 0
                snap_rate = _total_snapshots / elapsed if elapsed > 0 else 0

                window_elapsed = now - _window_start
                rolling_done_rate = (
                    (_done_in_window / window_elapsed) if window_elapsed > 0 else 0
                )
                if window_elapsed >= 30:
                    _window_start = now
                    _done_in_window = 0

                print(
                    f"Done: {_total_assets_done}/{total_assets_target} "
                    f"(avg {avg_done_rate:.2f}/s, rolling {rolling_done_rate:.2f}/s), "
                    f"{_total_snapshots:,} snapshots ({snap_rate:.0f}/s)"
                )
            _last_print = now


def _install_signal_handlers(
    loop: asyncio.AbstractEventLoop,
    tasks: list[asyncio.Task],
    requested_stop: asyncio.Event,
):
    state = {"count": 0}

    def _on_signal(sig_name: str):
        global _shutdown
        state["count"] += 1

        if state["count"] == 1:
            _shutdown = True
            requested_stop.set()
            print(
                f"Received {sig_name}; graceful shutdown. Press Ctrl-C again to force cancel."
            )
            return

        print(f"Received {sig_name} again; force canceling tasks.")
        _shutdown = True
        requested_stop.set()
        for t in tasks:
            t.cancel()

    for sig, name in ((signal.SIGINT, "SIGINT"), (signal.SIGTERM, "SIGTERM")):
        try:
            loop.add_signal_handler(sig, _on_signal, name)
        except NotImplementedError:
            try:
                signal.signal(sig, lambda *_: _on_signal(name))
            except Exception:
                pass


# =============================================================================
# Main
# =============================================================================
async def update_orderbook_snapshots(workers: int = WORKERS):
    global \
        _shutdown, \
        _total_snapshots, \
        _total_assets_done, \
        _last_print, \
        _done_in_window, \
        _window_start
    _shutdown = False
    _total_snapshots = 0
    _total_assets_done = 0
    _last_print = 0.0
    _done_in_window = 0
    _window_start = 0.0

    requested_stop = asyncio.Event()

    print("=" * 60)
    print("Polymarket Orderbook Snapshot Fetcher (ASYNC, RESUMABLE, SHARDED)")
    if NUM_SHARDS > 1:
        print(f"Shard: {SHARD_ID}/{NUM_SHARDS}")
    print(f"Workers: {workers}")
    print(f"Flush every: {FLUSH_EVERY:,} snapshots")
    print(f"Max pages per pass: {MAX_PAGES_PER_PASS}")
    print(f"Output: {OUTPUT_DIR}")
    print("=" * 60)

    print(f"\nLoading markets from {MARKETS_PARQUET}")
    assets = get_asset_ids_from_markets()
    print(f"Found {len(assets)} unique asset IDs")

    assets = filter_assets_by_time(assets, EARLIEST_SNAPSHOT_TS)
    print(f"{len(assets)} assets after time filtering")

    if NUM_SHARDS > 1:
        assets = [
            a for a in assets if stable_shard(a["asset_id"], NUM_SHARDS) == SHARD_ID
        ]
        print(f"{len(assets)} assets after sharding")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    progress = load_progress()

    p_assets = progress.get("assets", {})
    done_count = sum(1 for st in p_assets.values() if st.get("done"))
    partial_count = sum(
        1
        for st in p_assets.values()
        if (not st.get("done")) and (int(st.get("fetched", 0) or 0) > 0)
    )
    print(f"Already done (per progress file): {done_count}")
    print(f"Partial (started but not done): {partial_count}")

    pq: asyncio.PriorityQueue = asyncio.PriorityQueue()

    for a in assets:
        aid = a["asset_id"]
        st = p_assets.get(aid, {})
        if st.get("done"):
            continue
        rem = int(st.get("remaining_est", 10**12) or 10**12)
        pq.put_nowait((rem, random.random(), aid))

    total_target = pq.qsize() + done_count
    print(f"Assets remaining (including partials): {pq.qsize()}")

    if pq.qsize() == 0:
        print("Nothing left to fetch.")
        consolidate_orderbook_snapshots()
        return

    start_time = time.time()

    connector = aiohttp.TCPConnector(
        force_close=False,
        limit=0,
        limit_per_host=0,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        keepalive_timeout=30,
    )

    loop = asyncio.get_running_loop()
    tasks: list[asyncio.Task] = []
    saver_task = asyncio.create_task(autosave_progress(progress, requested_stop))

    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            for _ in range(workers):
                tasks.append(
                    asyncio.create_task(
                        worker(
                            pq=pq,
                            session=session,
                            progress=progress,
                            total_assets_target=total_target,
                            start_time=start_time,
                        )
                    )
                )

            _install_signal_handlers(loop, tasks, requested_stop)
            await asyncio.gather(*tasks, return_exceptions=True)

    finally:
        requested_stop.set()
        try:
            await saver_task
        except Exception:
            pass

        await force_save_progress(progress)

        elapsed = time.time() - start_time
        print("=" * 60)
        print("Stopped." if _shutdown else "Complete.")
        print(f"Assets done (this run): {_total_assets_done}")
        print(f"Total snapshots written (this run): {_total_snapshots:,}")
        print(f"Time: {elapsed:.1f}s")
        print("=" * 60)

        if not _shutdown:
            try:
                consolidate_orderbook_snapshots()
            except Exception as e:
                print(f"WARNING: consolidation failed: {e}")
        else:
            print("Skipped consolidation on stop (shards preserved for fast resume).")
            print(
                "To consolidate later, run:\n"
                'uv run python -c "from update_utils.update_orderbook_snapshots import consolidate_orderbook_snapshots; consolidate_orderbook_snapshots()"'
            )


if __name__ == "__main__":
    asyncio.run(update_orderbook_snapshots())
