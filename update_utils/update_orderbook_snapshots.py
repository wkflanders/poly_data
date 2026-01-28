"""
Fetch historical orderbook snapshots from Polymarket CLOB API.

Usage:
    uv run python -c "from update_utils.update_orderbook_snapshots import update_orderbook_snapshots; update_orderbook_snapshots()"
    uv run python -c "from update_utils.update_orderbook_snapshots import update_orderbook_snapshots; update_orderbook_snapshots(workers=4)"
"""

import json
import time
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import polars as pl
import requests

# === Configuration ===
BASE_URL = "https://clob.polymarket.com/orderbook-history"
EARLIEST_SNAPSHOT_TS = 1762984233541

MARKETS_PARQUET = Path("parquet/markets.parquet")
OUTPUT_DIR = Path("parquet/orderbook_snapshots")
PROGRESS_FILE = Path("parquet/orderbook_snapshots/.progress.json")

SNAPSHOTS_PER_REQUEST = 1000
FLUSH_EVERY = 20000  # Per-worker flush threshold
REQUEST_DELAY = 0.01
WORKERS = 4

# Global for clean shutdown
_shutdown = False
_write_lock = threading.Lock()
_progress_lock = threading.Lock()
_print_lock = threading.Lock()

# Thread-local sessions
_tls = threading.local()


def safe_print(msg: str):
    with _print_lock:
        print(msg)


def _session() -> requests.Session:
    s = getattr(_tls, "s", None)
    if s is None:
        s = requests.Session()
        _tls.s = s
    return s


def _handle_signal(sig, frame):
    global _shutdown
    safe_print("\n⚠️  Shutdown requested, waiting for workers to finish...")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, "r") as f:
            data = json.load(f)
            if "in_progress" not in data:
                data["in_progress"] = {}
            if "completed_assets" not in data:
                data["completed_assets"] = {}
            return data
    return {"completed_assets": {}, "in_progress": {}}


def save_progress(progress: dict):
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def get_asset_ids_from_markets() -> list[dict]:
    if not MARKETS_PARQUET.exists():
        raise FileNotFoundError(f"Markets file not found: {MARKETS_PARQUET}")

    df = pl.read_parquet(MARKETS_PARQUET)
    safe_print(f"Loaded {len(df)} markets")

    assets = []
    for row in df.iter_rows(named=True):
        market_id = row.get("id", "")
        created_at = row.get("createdAt", "")
        closed_time = row.get("closedTime", "")

        for token_col in ["token1", "token2"]:
            asset_id = row.get(token_col)
            if asset_id and str(asset_id) != "0":
                assets.append(
                    {
                        "asset_id": str(asset_id),
                        "market_id": str(market_id),
                        "created_at": created_at,
                        "closed_time": closed_time,
                    }
                )

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
            except:
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
            except:
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


def fetch_orderbook_page(
    asset_id: str, start_ts: int, timeout: int = 30, retries: int = 5
) -> tuple[int, list[dict]]:
    url = f"{BASE_URL}?asset_id={asset_id}&startTs={start_ts}&limit={SNAPSHOTS_PER_REQUEST}"
    sess = _session()

    for attempt in range(retries):
        try:
            r = sess.get(url, timeout=timeout)

            if r.status_code == 200:
                data = r.json()
                return data.get("count", 0), data.get("data", [])

            if r.status_code in (429, 500, 502, 503, 504):
                wait = 2**attempt
                safe_print(f"    HTTP {r.status_code}, retrying in {wait}s...")
                time.sleep(wait)
                continue

            if r.status_code == 400:
                return 0, []

            safe_print(f"    HTTP {r.status_code}: {r.text[:100]}")
            return 0, []

        except requests.Timeout:
            safe_print(f"    Timeout, attempt {attempt + 1}/{retries}")
            time.sleep(2**attempt)
        except requests.RequestException as e:
            safe_print(f"    Request error: {e}")
            time.sleep(2**attempt)

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
        "spread": (best_ask - best_bid) if (best_bid and best_ask) else None,
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

            ts = int(time.time() * 1000)
            out_file = partition_dir / f"data_{ts}.parquet"

            partition_df = partition_df.drop("year_month")
            partition_df.write_parquet(out_file, compression="zstd")

            rows = len(partition_df)
            size_kb = out_file.stat().st_size / 1024
            safe_print(f"   ✓ Wrote {rows:,} rows to {ym} ({size_kb:.1f} KB)")
            total_written += rows

        return total_written

    except Exception as e:
        safe_print(f"   ❌ ERROR writing: {e}")
        return 0


def update_orderbook_snapshots(workers: int = WORKERS):
    global _shutdown

    safe_print("=" * 60)
    safe_print("📊 Polymarket Orderbook Snapshot Fetcher")
    safe_print(f"   Workers: {workers}")
    safe_print("=" * 60)

    safe_print(f"\n📂 Loading markets from {MARKETS_PARQUET}")
    assets = get_asset_ids_from_markets()
    safe_print(f"   Found {len(assets)} unique asset IDs")

    assets = filter_assets_by_time(assets, EARLIEST_SNAPSHOT_TS)
    safe_print(f"   {len(assets)} assets after time filtering")

    progress = load_progress()
    completed = set(progress.get("completed_assets", {}).keys())
    in_progress = progress.get("in_progress", {})
    safe_print(f"   {len(completed)} completed, {len(in_progress)} in progress")

    todo = [a for a in assets if a["asset_id"] not in completed]
    safe_print(f"   {len(todo)} assets to fetch")
    import random

    random.shuffle(todo)
    if not todo:
        safe_print("\n✅ All assets already fetched!")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total_snapshots = [0]  # Use list for mutable reference in threads
    total_assets = [0]
    start_time = time.time()

    def process_asset(asset_info: dict) -> tuple[str, int]:
        """Fetch all snapshots for one asset, flushing periodically."""
        if _shutdown:
            return asset_info["asset_id"], 0

        asset_id = asset_info["asset_id"]
        buffer = []

        # Check if resuming
        with _progress_lock:
            if asset_id in progress.get("in_progress", {}):
                current_ts = progress["in_progress"][asset_id]["last_ts"] + 1
                fetched_so_far = progress["in_progress"][asset_id]["fetched"]
                safe_print(f"\n🔄 Resuming {asset_id[:30]}... (from ts={current_ts})")
            else:
                current_ts = EARLIEST_SNAPSHOT_TS
                fetched_so_far = 0
                safe_print(f"\n🔍 Fetching {asset_id[:30]}...")

        total_count = 0
        pages = 0

        while not _shutdown:
            count, snapshots = fetch_orderbook_page(asset_id, current_ts)

            if pages == 0:
                total_count = count
                if total_count == 0:
                    safe_print(f"   - {asset_id[:30]}... no snapshots")
                    return asset_id, 0

            if not snapshots:
                break

            pages += 1

            for snap in snapshots:
                buffer.append(flatten_snapshot(snap))

            fetched_so_far += len(snapshots)
            last_ts = max(int(s.get("timestamp", 0)) for s in snapshots)
            last_dt = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            safe_print(
                f"      [{asset_id[:15]}] Page {pages}: {len(snapshots)}, ts={last_ts} ({last_dt}), {fetched_so_far}/{total_count}"
            )

            # Flush buffer if large enough
            if len(buffer) >= FLUSH_EVERY:
                with _write_lock:
                    safe_print(
                        f"\n💾 [{asset_id[:15]}] Flushing {len(buffer)} snapshots..."
                    )
                    written = write_buffer(buffer)
                    total_snapshots[0] += written
                buffer.clear()

                # Save progress mid-asset
                with _progress_lock:
                    progress["in_progress"][asset_id] = {
                        "last_ts": last_ts,
                        "fetched": fetched_so_far,
                    }
                    save_progress(progress)

            if fetched_so_far >= total_count:
                break

            if last_ts <= current_ts:
                break
            current_ts = last_ts + 1

            time.sleep(REQUEST_DELAY)

        # Flush remaining buffer
        if buffer:
            with _write_lock:
                safe_print(
                    f"\n💾 [{asset_id[:15]}] Flushing final {len(buffer)} snapshots..."
                )
                written = write_buffer(buffer)
                total_snapshots[0] += written

        if _shutdown:
            with _progress_lock:
                progress["in_progress"][asset_id] = {
                    "last_ts": current_ts,
                    "fetched": fetched_so_far,
                }
                save_progress(progress)
            return asset_id, fetched_so_far

        # Mark complete
        with _progress_lock:
            progress["completed_assets"][asset_id] = {
                "count": fetched_so_far,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
            if "in_progress" in progress and asset_id in progress["in_progress"]:
                del progress["in_progress"][asset_id]
            save_progress(progress)

        safe_print(f"   ✓ {asset_id[:30]}... -> {fetched_so_far} snapshots")
        return asset_id, fetched_so_far

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_asset, a): a for a in todo}

        for fut in as_completed(futures):
            if _shutdown:
                break

            asset_info = futures[fut]
            asset_id = asset_info["asset_id"]

            try:
                _, count = fut.result()
                total_assets[0] += 1

                elapsed = time.time() - start_time
                rate = total_assets[0] / elapsed if elapsed > 0 else 0
                safe_print(
                    f"📈 Progress: {total_assets[0]}/{len(todo)} assets ({rate:.2f}/s), {total_snapshots[0]:,} written"
                )

            except Exception as e:
                safe_print(f"   ❌ [{asset_id[:15]}] Error: {e}")

    # Always consolidate at end
    consolidate_orderbook_snapshots()

    elapsed = time.time() - start_time
    safe_print(f"\n{'=' * 60}")
    safe_print(f"✅ {'Stopped' if _shutdown else 'Complete'}!")
    safe_print(f"   Assets processed: {total_assets[0]}")
    safe_print(f"   Total snapshots written: {total_snapshots[0]:,}")
    safe_print(f"   Time: {elapsed:.1f}s")
    safe_print(f"{'=' * 60}")


def consolidate_orderbook_snapshots():
    import duckdb

    safe_print("🔧 Consolidating partition files...")

    for partition_dir in OUTPUT_DIR.glob("year_month=*"):
        part_files = list(partition_dir.glob("*.parquet"))
        if len(part_files) <= 1:
            continue

        safe_print(f"   {partition_dir.name}: {len(part_files)} files -> 1")

        out_file = partition_dir / "data_consolidated.parquet"
        glob_pattern = str(partition_dir / "*.parquet")

        duckdb.sql(f"""
            COPY (SELECT * FROM read_parquet('{glob_pattern}'))
            TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        for f in part_files:
            if f.name != "data_consolidated.parquet":
                f.unlink()

        out_file.rename(partition_dir / "data.parquet")

    safe_print("✅ Consolidation complete")
