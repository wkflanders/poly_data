"""
Fetch historical orderbook snapshots from Polymarket CLOB API (async version).
Usage:
    uv run python update_utils/update_orderbook_snapshots.py
"""

import asyncio
import aiohttp
import json
import os
import random
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

# === Configuration ===
BASE_URL = "https://clob.polymarket.com/orderbook-history"
EARLIEST_SNAPSHOT_TS = 1762984233541

MARKETS_PARQUET = Path("parquet/markets.parquet")
OUTPUT_DIR = Path("parquet/orderbook_snapshots")
PROGRESS_FILE = Path("parquet/orderbook_snapshots/.progress.json")

SNAPSHOTS_PER_REQUEST = 1000
FLUSH_EVERY = 10000
WORKERS = 70
PROXIES_FILE = Path("proxies.json")
SAVE_PROGRESS_EVERY = 100

# Consolidation
CONSOLIDATED_PARQUET = Path("parquet/orderbook_snapshots_consolidated.parquet")
DELETE_SHARDS_AFTER_CONSOLIDATE = (
    False  # set True if you want to remove shards after merging
)


def load_proxies() -> list[str]:
    if PROXIES_FILE.exists():
        with open(PROXIES_FILE, "r") as f:
            return json.load(f)
    return []


PROXIES = load_proxies()

# Global state
_shutdown = False
_total_snapshots = 0
_total_assets = 0
_completion_counter = 0
_lock = asyncio.Lock()


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            with open(PROGRESS_FILE, "r") as f:
                data = json.load(f)
        except Exception:
            # corrupted / partially-written progress file -> start fresh
            return {"completed_assets": {}}

        if "completed_assets" not in data:
            data["completed_assets"] = {}
        return data

    return {"completed_assets": {}}


def save_progress(progress: dict):
    """
    Atomic progress save:
      1) write to temp
      2) fsync
      3) os.replace to final path (atomic on POSIX)
    """
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = PROGRESS_FILE.with_suffix(PROGRESS_FILE.suffix + ".tmp")

    with open(tmp, "w") as f:
        json.dump(progress, f)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp, PROGRESS_FILE)


def get_asset_ids_from_markets() -> list[dict]:
    if not MARKETS_PARQUET.exists():
        raise FileNotFoundError(f"Markets file not found: {MARKETS_PARQUET}")

    df = pl.read_parquet(MARKETS_PARQUET)
    print(f"Loaded {len(df)} markets")

    assets = []
    for row in df.iter_rows(named=True):
        market_id = row.get("id", "")
        created_at = row.get("createdAt", "")
        closed_time = row.get("closedTime", "")

        asset_id = row.get("token1")
        if asset_id and str(asset_id) != "0":
            assets.append(
                {
                    "asset_id": str(asset_id),
                    "market_id": str(market_id),
                    "created_at": created_at,
                    "closed_time": closed_time,
                }
            )

    # Dedup by asset_id
    seen = set()
    unique = []
    for a in assets:
        if a["asset_id"] not in seen:
            seen.add(a["asset_id"])
            unique.append(a)

    return unique


def filter_assets_by_time(assets: list[dict], cutoff_ts_ms: int) -> list[dict]:
    filtered = []
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


async def fetch_orderbook_page(
    session: aiohttp.ClientSession,
    asset_id: str,
    start_ts: int,
    proxy: str | None = None,
    retries: int = 5,
) -> tuple[int, list[dict]]:
    url = f"{BASE_URL}?asset_id={asset_id}&startTs={start_ts}&limit={SNAPSHOTS_PER_REQUEST}"

    for attempt in range(retries):
        if _shutdown:
            return 0, []

        try:
            async with session.get(
                url, proxy=proxy, timeout=aiohttp.ClientTimeout(total=30)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    return data.get("count", 0), data.get("data", [])

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


def flatten_snapshot(snap: dict) -> dict:
    timestamp_ms = int(snap.get("timestamp", 0))
    timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    year_month = timestamp_dt.strftime("%Y-%m")

    bids = snap.get("bids", [])
    asks = snap.get("asks", [])

    best_bid = max((float(b["price"]) for b in bids), default=None)
    best_ask = min((float(a["price"]) for a in asks), default=None)

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
        "total_bid_size": sum(float(b["size"]) for b in bids),
        "total_ask_size": sum(float(a["size"]) for a in asks),
        "min_order_size": snap.get("min_order_size", ""),
        "tick_size": snap.get("tick_size", ""),
        "neg_risk": snap.get("neg_risk", False),
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
            partition_df = df.filter(pl.col("year_month") == ym)
            partition_dir = OUTPUT_DIR / f"year_month={ym}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            ts = int(time.time() * 1_000_000)
            out_file = partition_dir / f"data_{id(buffer)}_{ts}.parquet"

            partition_df = partition_df.drop("year_month")
            partition_df.write_parquet(out_file, compression="zstd")
            total_written += len(partition_df)

        return total_written

    except Exception as e:
        print(f"   ❌ ERROR writing: {e}")
        return 0


def _glob_shard_files() -> list[Path]:
    if not OUTPUT_DIR.exists():
        return []
    return sorted(OUTPUT_DIR.glob("year_month=*/*.parquet"))


def consolidate_orderbook_snapshots(
    out_file: Path = CONSOLIDATED_PARQUET,
    delete_shards: bool = DELETE_SHARDS_AFTER_CONSOLIDATE,
) -> int:
    """
    Consolidate all shard parquet files into a single parquet file using Polars streaming.
    Returns number of shard files consolidated (not rows).
    """
    shard_files = _glob_shard_files()
    if not shard_files:
        print("ℹ️  No shard parquet files found to consolidate.")
        return 0

    out_file.parent.mkdir(parents=True, exist_ok=True)

    print(f"🧩 Consolidating {len(shard_files)} shard files → {out_file}")
    lf = pl.scan_parquet([str(p) for p in shard_files])
    lf.sink_parquet(str(out_file), compression="zstd")

    if delete_shards:
        print("🧹 Deleting shard files...")
        for p in shard_files:
            try:
                p.unlink()
            except Exception as e:
                print(f"   ⚠️  Could not delete {p}: {e}")

        for d in sorted(OUTPUT_DIR.glob("year_month=*"), reverse=True):
            try:
                if d.is_dir() and not any(d.iterdir()):
                    d.rmdir()
            except Exception:
                pass

    print("✅ Consolidation done.")
    return len(shard_files)


async def process_asset(
    session: aiohttp.ClientSession,
    asset_info: dict,
    proxy: str | None,
    progress: dict,
) -> tuple[str, int]:
    global _shutdown, _total_snapshots, _completion_counter

    if _shutdown:
        return asset_info["asset_id"], 0

    asset_id = asset_info["asset_id"]
    buffer: list[dict] = []
    current_ts = EARLIEST_SNAPSHOT_TS
    fetched_so_far = 0
    total_count = 0
    pages = 0

    while not _shutdown:
        count, snapshots = await fetch_orderbook_page(
            session, asset_id, current_ts, proxy
        )

        if pages == 0:
            total_count = count
            if total_count == 0:
                break

        if not snapshots:
            break

        pages += 1

        for snap in snapshots:
            buffer.append(flatten_snapshot(snap))

        fetched_so_far += len(snapshots)
        last_ts = max(int(s.get("timestamp", 0)) for s in snapshots)

        if len(buffer) >= FLUSH_EVERY:
            written = write_buffer(buffer)
            async with _lock:
                _total_snapshots += written
            buffer.clear()

        if fetched_so_far >= total_count:
            break
        if last_ts <= current_ts:
            break
        current_ts = last_ts + 1

    if buffer:
        written = write_buffer(buffer)
        async with _lock:
            _total_snapshots += written

    # Mark completed + occasional progress save (do NOT hold lock while writing file)
    should_save = False
    if not _shutdown:
        async with _lock:
            progress["completed_assets"][asset_id] = {
                "count": fetched_so_far,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
            _completion_counter += 1
            should_save = _completion_counter % SAVE_PROGRESS_EVERY == 0

    if should_save:
        await asyncio.to_thread(save_progress, progress)

    return asset_id, fetched_so_far


async def worker(
    queue: asyncio.Queue,
    session: aiohttp.ClientSession,
    proxy: str | None,
    progress: dict,
    todo_count: int,
    start_time: float,
):
    global _total_assets, _total_snapshots, _shutdown

    while not _shutdown:
        try:
            asset_info = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        try:
            await process_asset(session, asset_info, proxy, progress)
        finally:
            queue.task_done()

        async with _lock:
            _total_assets += 1
            if _total_assets % 10 == 0:
                elapsed = time.time() - start_time
                rate = _total_assets / elapsed if elapsed > 0 else 0
                snap_rate = _total_snapshots / elapsed if elapsed > 0 else 0
                print(
                    f"📈 Progress: {_total_assets}/{todo_count} assets ({rate:.1f}/s), "
                    f"{_total_snapshots:,} snapshots ({snap_rate:.0f}/s)"
                )


def _install_signal_handlers(
    loop: asyncio.AbstractEventLoop, tasks: list[asyncio.Task]
):
    """
    Best-effort signal handling: on SIGINT/SIGTERM set shutdown and cancel tasks.
    (SIGKILL cannot be handled.)
    """

    def _request_shutdown(sig_name: str):
        global _shutdown
        if _shutdown:
            return
        _shutdown = True
        print(f"\n🛑 Received {sig_name} → requesting shutdown...")
        for t in tasks:
            t.cancel()

    for sig, name in ((signal.SIGINT, "SIGINT"), (signal.SIGTERM, "SIGTERM")):
        try:
            loop.add_signal_handler(sig, _request_shutdown, name)
        except NotImplementedError:
            # Windows / some event loops
            try:
                signal.signal(sig, lambda *_: _request_shutdown(name))
            except Exception:
                pass


async def update_orderbook_snapshots(workers: int = WORKERS):
    global _shutdown, _total_snapshots, _total_assets, _completion_counter
    _shutdown = False
    _total_snapshots = 0
    _total_assets = 0
    _completion_counter = 0

    print("=" * 60)
    print("📊 Polymarket Orderbook Snapshot Fetcher (ASYNC)")
    print(f"   Workers: {workers}")
    print(f"   Proxies: {len(PROXIES)}")
    print("=" * 60)

    print(f"\n📂 Loading markets from {MARKETS_PARQUET}")
    assets = get_asset_ids_from_markets()
    print(f"   Found {len(assets)} unique asset IDs")

    assets = filter_assets_by_time(assets, EARLIEST_SNAPSHOT_TS)
    print(f"   {len(assets)} assets after time filtering")

    progress = load_progress()
    completed = set(progress.get("completed_assets", {}).keys())
    print(f"   {len(completed)} already completed")

    todo = [a for a in assets if a["asset_id"] not in completed]
    print(f"   {len(todo)} assets to fetch")

    random.shuffle(todo)

    if not todo:
        print("\n✅ All assets already fetched!")
        consolidate_orderbook_snapshots()
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    start_time = time.time()

    queue: asyncio.Queue = asyncio.Queue()
    for a in todo:
        queue.put_nowait(a)

    connector = aiohttp.TCPConnector(limit=0, limit_per_host=0)
    tasks: list[asyncio.Task] = []
    loop = asyncio.get_running_loop()

    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            for i in range(workers):
                proxy = PROXIES[i % len(PROXIES)] if PROXIES else None
                tasks.append(
                    asyncio.create_task(
                        worker(queue, session, proxy, progress, len(todo), start_time)
                    )
                )

            _install_signal_handlers(loop, tasks)

            await asyncio.gather(*tasks, return_exceptions=True)

    except KeyboardInterrupt:
        _shutdown = True
        print("\n🛑 KeyboardInterrupt → requesting shutdown...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    finally:
        # Always save progress and consolidate on exit / interrupt
        try:
            await asyncio.to_thread(save_progress, progress)
        except Exception as e:
            print(f"⚠️  Failed to save progress: {e}")

        try:
            consolidate_orderbook_snapshots()
        except Exception as e:
            print(f"⚠️  Consolidation failed: {e}")

        elapsed = time.time() - start_time
        print(f"\n{'=' * 60}")
        print(f"✅ {'Stopped' if _shutdown else 'Complete'}!")
        print(f"   Assets processed: {_total_assets}")
        print(f"   Total snapshots written: {_total_snapshots:,}")
        print(f"   Time: {elapsed:.1f}s")
        print(f"{'=' * 60}")


if __name__ == "__main__":
    asyncio.run(update_orderbook_snapshots())
