from __future__ import annotations

import os
import time
import glob
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Iterable

import polars as pl
import requests

BASE = "https://gamma-api.polymarket.com"

# Tune these
WORKERS = 24  # start here; if you see lots of 429s, drop to 12-16
FLUSH_EVERY = 5000  # rows per parquet part written to disk


_tls = threading.local()


def _session() -> requests.Session:
    """One Session per thread (requests.Session isn't thread-safe)."""
    s = getattr(_tls, "s", None)
    if s is None:
        s = requests.Session()
        _tls.s = s
    return s


def fetch_market_tags(market_id: str, retries: int = 6) -> tuple[str, list[str]]:
    url = f"{BASE}/markets/{market_id}/tags"
    sess = _session()

    for attempt in range(retries):
        try:
            r = sess.get(url, timeout=30)

            if r.status_code == 200:
                data: Any = r.json()
                if not isinstance(data, list):
                    return market_id, []
                slugs = [
                    t.get("slug") for t in data if isinstance(t, dict) and t.get("slug")
                ]

                # de-dupe, stable order
                seen: set[str] = set()
                out: list[str] = []
                for s in slugs:
                    if s not in seen:
                        seen.add(s)
                        out.append(s)
                return market_id, out

            # backoff on transient / rate-limit
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.0 * (attempt + 1))
                continue

            # other errors: don't spin forever
            return market_id, []

        except requests.RequestException:
            time.sleep(1.0 * (attempt + 1))

    return market_id, []


def _iter_ids(markets_parquet: str) -> list[str]:
    # Only read `id` column (fast)
    ids = (
        pl.read_parquet(markets_parquet, columns=["id"])
        .select(pl.col("id").cast(pl.Utf8).alias("market_id"))
        .unique()
        .get_column("market_id")
        .to_list()
    )
    return [i for i in ids if i]  # drop null/empty


def _load_done(parts_dir: str) -> set[str]:
    done: set[str] = set()
    part_files = sorted(glob.glob(os.path.join(parts_dir, "part-*.parquet")))
    for fp in part_files:
        try:
            df = pl.read_parquet(fp, columns=["market_id"])
            done.update(df.get_column("market_id").cast(pl.Utf8).to_list())
        except Exception:
            pass
    return done


def _next_part_index(parts_dir: str) -> int:
    part_files = sorted(glob.glob(os.path.join(parts_dir, "part-*.parquet")))
    if not part_files:
        return 0
    last = os.path.basename(part_files[-1])
    # part-00012.parquet
    n = int(last.split("-")[1].split(".")[0])
    return n + 1


def _write_part(
    parts_dir: str, part_idx: int, market_ids: list[str], tag_slugs: list[list[str]]
) -> str:
    df = pl.DataFrame({"market_id": market_ids, "tag_slugs": tag_slugs})
    out_path = os.path.join(parts_dir, f"part-{part_idx:05d}.parquet")
    df.write_parquet(out_path)
    return out_path


def main(
    markets_parquet: str = "parquet/markets.parquet",
    parts_dir: str = "parquet/tags_parts",
) -> None:
    os.makedirs(parts_dir, exist_ok=True)

    ids = _iter_ids(markets_parquet)
    done = _load_done(parts_dir)
    todo = [mid for mid in ids if mid not in done]

    print(
        f"Need to fetch tags for {len(todo)} markets. (skipping {len(done)} already done)"
    )
    if not todo:
        print("Nothing to do.")
        return

    part_idx = _next_part_index(parts_dir)

    buf_market_id: list[str] = []
    buf_tag_slugs: list[list[str]] = []

    start = time.time()
    completed = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(fetch_market_tags, mid) for mid in todo]

        for fut in as_completed(futures):
            mid, slugs = fut.result()
            buf_market_id.append(mid)
            buf_tag_slugs.append(slugs)

            completed += 1
            if completed % 500 == 0:
                elapsed = time.time() - start
                rate = completed / elapsed if elapsed > 0 else 0.0
                print(
                    f"Fetched {completed}/{len(todo)}  ({rate:.1f}/s)  last={mid} tags={len(slugs)}"
                )

            if len(buf_market_id) >= FLUSH_EVERY:
                out_path = _write_part(
                    parts_dir, part_idx, buf_market_id, buf_tag_slugs
                )
                print(f"Wrote {len(buf_market_id)} rows -> {out_path}")
                part_idx += 1
                buf_market_id.clear()
                buf_tag_slugs.clear()

    # final flush
    if buf_market_id:
        out_path = _write_part(parts_dir, part_idx, buf_market_id, buf_tag_slugs)
        print(f"Wrote {len(buf_market_id)} rows -> {out_path}")

    print(f"Done. Parts written under: {parts_dir}")


if __name__ == "__main__":
    main()
