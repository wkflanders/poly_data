#!/usr/bin/env python3
from __future__ import annotations

"""
Fetch per-market tag slugs from Polymarket Gamma API with:
- resumable progress (skips market_ids already written in parquet parts)
- Webshare rotating proxies (ONE worker per proxy)
- fast, reliable Ctrl-C cancellation (1st Ctrl-C = graceful stop; 2nd Ctrl-C = hard exit)
- bounded queues (no 390k futures)

Run:
  uv run python update_utils/get_market_tags_proxied.py

Env (required for Webshare):
  export WEBSHARE_PROXY_USER="yhexwrmn-rotate"
  export WEBSHARE_PROXY_PASS="vmcscq5aaupr"
  export WEBSHARE_PROXY_HOST="p.webshare.io"
  export WEBSHARE_PROXY_PORT="80"

Optional tuning:
  export N_PROXIES=50              # number of rotating proxy identities to use (default 50)
  export FLUSH_EVERY=5000          # parquet rows per part (default 5000)
  export REQUEST_TIMEOUT_S=20      # per-request timeout (default 20)
  export RETRIES=6                 # per-market retries (default 6)
  export QUEUE_MAX=20000           # work/result queue max size (default 20000)
"""

import json
import os
import time
import glob
import threading
from pathlib import Path
from queue import Queue, Empty
from typing import Any

import polars as pl
import requests
from requests.adapters import HTTPAdapter

BASE = "https://gamma-api.polymarket.com"

MARKETS_PARQUET = Path(os.environ.get("MARKETS_PARQUET", "parquet/markets.parquet"))
PARTS_DIR = Path(os.environ.get("PARTS_DIR", "parquet/tags_parts"))

# Webshare rotating proxy config (required)
WEBSHARE_PROXY_USER = os.environ.get("WEBSHARE_PROXY_USER", "")
WEBSHARE_PROXY_PASS = os.environ.get("WEBSHARE_PROXY_PASS", "")
WEBSHARE_PROXY_HOST = os.environ.get("WEBSHARE_PROXY_HOST", "")
WEBSHARE_PROXY_PORT = os.environ.get("WEBSHARE_PROXY_PORT", "")

# How many distinct "workers" to run (each uses its own Session + proxy identity)
N_PROXIES = int(os.environ.get("N_PROXIES", "50"))

# Output batching
FLUSH_EVERY = int(os.environ.get("FLUSH_EVERY", "5000"))

# Request / retry tuning
REQUEST_TIMEOUT_S = float(os.environ.get("REQUEST_TIMEOUT_S", "20"))
RETRIES = int(os.environ.get("RETRIES", "6"))
SLEEP_429_BASE = float(os.environ.get("SLEEP_429_BASE", "0.25"))
SLEEP_5XX_BASE = float(os.environ.get("SLEEP_5XX_BASE", "0.25"))

# Queues
QUEUE_MAX = int(os.environ.get("QUEUE_MAX", "20000"))


def _require_webshare_env() -> None:
    missing = []
    for k, v in [
        ("WEBSHARE_PROXY_USER", WEBSHARE_PROXY_USER),
        ("WEBSHARE_PROXY_PASS", WEBSHARE_PROXY_PASS),
        ("WEBSHARE_PROXY_HOST", WEBSHARE_PROXY_HOST),
        ("WEBSHARE_PROXY_PORT", WEBSHARE_PROXY_PORT),
    ]:
        if not v:
            missing.append(k)
    if missing:
        raise SystemExit(
            "Missing Webshare env vars: "
            + ", ".join(missing)
            + "\nSet them like:\n"
            + '  export WEBSHARE_PROXY_USER="..."\n'
            + '  export WEBSHARE_PROXY_PASS="..."\n'
            + '  export WEBSHARE_PROXY_HOST="p.webshare.io"\n'
            + '  export WEBSHARE_PROXY_PORT="80"\n'
        )


def _make_webshare_proxy_url() -> str:
    # Webshare rotating proxy endpoint; each session will behave like a distinct "client"
    # because the username is "-rotate" (Webshare rotates IPs).
    return f"http://{WEBSHARE_PROXY_USER}:{WEBSHARE_PROXY_PASS}@{WEBSHARE_PROXY_HOST}:{WEBSHARE_PROXY_PORT}"


def _make_session(proxy_url: str) -> requests.Session:
    s = requests.Session()
    s.proxies = {"http": proxy_url, "https": proxy_url}

    # One thread uses one session; keep pools minimal but stable.
    adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1, max_retries=0)
    s.mount("http://", adapter)
    s.mount("https://", adapter)

    # A polite UA sometimes helps CDNs behave.
    s.headers.update({"User-Agent": "poly_data-tag-scraper/1.0"})
    return s


def _iter_ids(markets_parquet: Path) -> list[str]:
    ids = (
        pl.read_parquet(str(markets_parquet), columns=["id"])
        .select(pl.col("id").cast(pl.Utf8).alias("market_id"))
        .unique()
        .get_column("market_id")
        .to_list()
    )
    return [i for i in ids if i]


def _load_done(parts_dir: Path) -> set[str]:
    done: set[str] = set()
    part_files = sorted(glob.glob(str(parts_dir / "part-*.parquet")))
    for fp in part_files:
        try:
            df = pl.read_parquet(fp, columns=["market_id"])
            done.update(df.get_column("market_id").cast(pl.Utf8).to_list())
        except Exception:
            # ignore unreadable part; you can delete it manually if needed
            pass
    return done


def _next_part_index(parts_dir: Path) -> int:
    part_files = sorted(glob.glob(str(parts_dir / "part-*.parquet")))
    if not part_files:
        return 0
    last = os.path.basename(part_files[-1])
    n = int(last.split("-")[1].split(".")[0])
    return n + 1


def _write_part(parts_dir: Path, part_idx: int, market_ids: list[str], tag_slugs: list[list[str]]) -> Path:
    df = pl.DataFrame({"market_id": market_ids, "tag_slugs": tag_slugs})
    out_path = parts_dir / f"part-{part_idx:05d}.parquet"
    df.write_parquet(str(out_path))
    return out_path


def _dedupe_stable(xs: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in xs:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _fetch_tags_with_counts(sess: requests.Session, market_id: str) -> tuple[str, list[str], int]:
    """
    Returns (market_id, tag_slugs, status_code_bucket)
      status_code_bucket:
        200, 429, 500, 0 (other/error)
    """
    url = f"{BASE}/markets/{market_id}/tags"

    for attempt in range(RETRIES):
        try:
            r = sess.get(url, timeout=(5, REQUEST_TIMEOUT_S))
            code = r.status_code

            if code == 200:
                data: Any = r.json()
                if not isinstance(data, list):
                    return market_id, [], 200
                raw = [t.get("slug") for t in data if isinstance(t, dict) and t.get("slug")]
                return market_id, _dedupe_stable([str(x) for x in raw if x]), 200

            if code == 429:
                time.sleep(SLEEP_429_BASE * (attempt + 1))
                continue

            if code in (500, 502, 503, 504):
                time.sleep(SLEEP_5XX_BASE * (attempt + 1))
                continue

            return market_id, [], 0

        except requests.RequestException:
            time.sleep(0.1 * (attempt + 1))

    # exhausted retries
    return market_id, [], 0


def main() -> None:
    _require_webshare_env()
    PARTS_DIR.mkdir(parents=True, exist_ok=True)

    proxy_url = _make_webshare_proxy_url()

    ids = _iter_ids(MARKETS_PARQUET)
    done = _load_done(PARTS_DIR)
    todo = [mid for mid in ids if mid not in done]

    print(f"markets_parquet={MARKETS_PARQUET}")
    print(f"parts_dir={PARTS_DIR}")
    print(f"already_done={len(done)} remaining={len(todo)}")
    print(f"workers(one per proxy)={N_PROXIES} flush_every={FLUSH_EVERY}")

    if not todo:
        print("Nothing to do.")
        return

    part_idx = _next_part_index(PARTS_DIR)

    work_q: Queue[str] = Queue(maxsize=QUEUE_MAX)
    out_q: Queue[tuple[str, list[str]]] = Queue(maxsize=QUEUE_MAX)

    stop = threading.Event()

    stats_lock = threading.Lock()
    c200 = 0
    c429 = 0
    c5xx = 0
    cerr = 0

    # A worker = 1 session pinned to Webshare rotating proxy endpoint
    def worker_thread(worker_id: int) -> None:
        nonlocal c200, c429, c5xx, cerr
        sess = _make_session(proxy_url)

        while not stop.is_set():
            try:
                mid = work_q.get(timeout=0.5)
            except Empty:
                continue

            try:
                mid, slugs, bucket = _fetch_tags_with_counts(sess, mid)
                with stats_lock:
                    if bucket == 200:
                        c200 += 1
                    elif bucket == 429:
                        c429 += 1
                    elif bucket == 500:
                        c5xx += 1
                    else:
                        cerr += 1
                out_q.put((mid, slugs))
            finally:
                work_q.task_done()

    threads: list[threading.Thread] = []
    for i in range(N_PROXIES):
        t = threading.Thread(target=worker_thread, args=(i,), name=f"p{i:03d}", daemon=True)
        t.start()
        threads.append(t)

    todo_iter = iter(todo)
    fed = 0

    def feed_more(target_fill: int = int(QUEUE_MAX * 0.8)) -> None:
        nonlocal fed
        while not stop.is_set():
            if work_q.qsize() >= target_fill:
                return
            try:
                mid = next(todo_iter)
            except StopIteration:
                return
            work_q.put(mid)
            fed += 1

    buf_market_id: list[str] = []
    buf_tag_slugs: list[list[str]] = []

    start = time.time()
    completed = 0
    total = len(todo)
    last_log = 0.0

    print("Press Ctrl-C once to stop gracefully (flush + exit). Press again to hard-exit.")

    try:
        feed_more()

        while completed < total:
            feed_more()

            try:
                mid, slugs = out_q.get(timeout=1.0)
            except Empty:
                continue

            buf_market_id.append(mid)
            buf_tag_slugs.append(slugs)
            completed += 1

            now = time.time()
            if now - last_log >= 2.0:
                elapsed = now - start
                rate = completed / elapsed if elapsed > 0 else 0.0
                with stats_lock:
                    s200, s429, s5xx, serr = c200, c429, c5xx, cerr
                print(
                    f"Fetched {completed}/{total} ({rate:.1f}/s) "
                    f"workq={work_q.qsize()} outq={out_q.qsize()} "
                    f"200={s200} 429={s429} 5xx={s5xx} err={serr}"
                )
                last_log = now

            if len(buf_market_id) >= FLUSH_EVERY:
                out_path = _write_part(PARTS_DIR, part_idx, buf_market_id, buf_tag_slugs)
                print(f"Wrote {len(buf_market_id)} rows -> {out_path}")
                part_idx += 1
                buf_market_id.clear()
                buf_tag_slugs.clear()

    except KeyboardInterrupt:
        print("\n🛑 Ctrl-C received: stopping workers, flushing what we have...")

        # If user spams Ctrl-C, just bail immediately.
        try:
            time.sleep(0.1)
        except KeyboardInterrupt:
            print("🧨 Hard exit.")
            os._exit(130)

    finally:
        stop.set()

        # Final flush
        if buf_market_id:
            out_path = _write_part(PARTS_DIR, part_idx, buf_market_id, buf_tag_slugs)
            print(f"Wrote {len(buf_market_id)} rows -> {out_path}")

        print(f"Done. Parts written under: {PARTS_DIR}")


if __name__ == "__main__":
    main()
