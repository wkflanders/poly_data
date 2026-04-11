#!/usr/bin/env python3
"""
Splits remaining markets into N processes across tmux panes.

Usage:
  python launch_orderbook.py                    # 8 processes, 50 workers each
  python launch_orderbook.py --processes 4      # 4 processes
  python launch_orderbook.py --workers 100      # 100 workers per process

Lifecycle:
  1. On launch, checks for leftover progress_*.json from a previous run
  2. Validates them against actual data files on disk
  3. Merges validated entries into progress.json
  4. Splits remaining markets into chunks
  5. Each process gets its own progress_N.json
  6. Cancel anytime - re-run and it picks up automatically
"""

import json
import csv
import gzip
import os
import sys
import subprocess
import shutil
from datetime import datetime, timezone

NUM_PROCESSES = 8
WORKERS_PER_PROCESS = 50
SESSION_NAME = "orderbook"
CHUNK_DIR = "orderbook_chunks"
DATA_DIR = "orderbook_snapshots"
PROGRESS_FILE = os.path.join(DATA_DIR, "progress.json")


def validate_and_merge():
    """Find leftover progress_*.json files, validate against data on disk, merge into progress.json."""
    progress_files = (
        sorted(
            [
                f
                for f in os.listdir(DATA_DIR)
                if f.startswith("progress_") and f.endswith(".json")
            ]
        )
        if os.path.isdir(DATA_DIR)
        else []
    )

    if not progress_files:
        return

    print(f"Found {len(progress_files)} leftover progress files from previous run")
    print("Validating against data files on disk...")

    # Load main progress
    main = {}
    if os.path.isfile(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            main = json.load(f)

    data_dir = os.path.join(DATA_DIR, "data")
    existing_files = set(os.listdir(data_dir)) if os.path.isdir(data_dir) else set()

    merged = 0
    skipped = 0
    invalid = 0

    for fname in progress_files:
        path = os.path.join(DATA_DIR, fname)
        try:
            with open(path, "r") as f:
                chunk_progress = json.load(f)
        except Exception as e:
            print(f"  ⚠ Could not read {fname}: {e}")
            os.remove(path)
            continue

        for asset_id, state in chunk_progress.items():
            # Skip if already in main with same or better state
            if asset_id in main and main[asset_id].get("complete"):
                skipped += 1
                continue

            market_id = state.get("market_id", "")
            if not market_id:
                skipped += 1
                continue

            # Validate: if count > 0, data file must exist
            count = state.get("count", 0)
            if count > 0:
                jsonl_exists = f"{market_id}.jsonl" in existing_files
                gz_exists = f"{market_id}.jsonl.gz" in existing_files

                if not jsonl_exists and not gz_exists:
                    # Progress claims data but file missing — skip
                    invalid += 1
                    continue

                # If gz exists, it's definitely complete
                if gz_exists:
                    state["complete"] = True

            main[asset_id] = state
            merged += 1

        os.remove(path)
        print(f"  ✓ Processed {fname}: merged entries")

    # Also clean up any .tmp or .lock files
    for f in os.listdir(DATA_DIR):
        if f.endswith(".tmp") or f.endswith(".lock"):
            os.remove(os.path.join(DATA_DIR, f))

    # Save merged progress
    with open(PROGRESS_FILE, "w") as f:
        json.dump(main, f)

    complete = sum(1 for v in main.values() if v.get("complete"))
    print(
        f"  Merged: {merged:,} | Skipped (already done): {skipped:,} | Invalid (no data file): {invalid:,}"
    )
    print(f"  Total tracked: {len(main):,} ({complete:,} complete)")


def get_remaining_markets():
    cutoff = datetime(2025, 11, 12, tzinfo=timezone.utc)

    progress = {}
    if os.path.isfile(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            progress = json.load(f)

    remaining = []
    for row in csv.DictReader(open("markets.csv")):
        t = row.get("token1", "").strip()
        if not t:
            continue
        ct = row.get("closedTime", "").strip()
        if ct and ct.lower() not in ("", "none", "false"):
            try:
                dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if dt < cutoff:
                    continue
            except (ValueError, TypeError):
                pass
        if progress.get(t, {}).get("complete"):
            continue
        remaining.append(row["id"].strip())

    return remaining


def launch(num_procs, workers):
    # Step 1: validate and merge any leftover progress files
    validate_and_merge()

    # Step 2: figure out what's left
    remaining = get_remaining_markets()
    print(f"\nRemaining markets: {len(remaining):,}")
    print(f"Splitting into {num_procs} processes with {workers} workers each")
    print(f"Total concurrent requests: ~{num_procs * workers}")

    if not remaining:
        print("Nothing to do!")
        return

    # Step 3: clean up temp files
    if os.path.isdir("orderbook_snapshots/chunks"):
        subprocess.run("rm -rf orderbook_snapshots/chunks", shell=True)
        print("Cleaned up leftover chunk files")

    if os.path.isdir(CHUNK_DIR):
        subprocess.run(f"rm -rf {CHUNK_DIR}", shell=True)
    os.makedirs(CHUNK_DIR, exist_ok=True)

    # Step 4: split markets and create per-process progress files
    chunk_files = []
    for i in range(num_procs):
        chunk = remaining[i::num_procs]
        chunk_file = os.path.join(CHUNK_DIR, f"chunk_{i}.txt")
        with open(chunk_file, "w") as f:
            f.write("\n".join(chunk))
        chunk_files.append(chunk_file)
        print(f"  Chunk {i}: {len(chunk):,} markets -> {chunk_file}")

    # Kill existing session
    subprocess.run(f"tmux kill-session -t {SESSION_NAME} 2>/dev/null", shell=True)

    # Step 5: write shell scripts and launch tmux panes
    scripts = []
    for i in range(num_procs):
        # Each process gets its own progress file, seeded from main
        progress_file = os.path.join(DATA_DIR, f"progress_{i}.json")
        if os.path.isfile(PROGRESS_FILE):
            shutil.copy2(PROGRESS_FILE, progress_file)

        script_path = os.path.join(CHUNK_DIR, f"run_{i}.sh")
        with open(script_path, "w") as f:
            f.write(f"""#!/bin/bash
cd {os.getcwd()}
ulimit -n 65536
uv run python -c "
from update_utils.update_orderbook import update_orderbook
update_orderbook(num_workers={workers}, market_ids_file='{chunk_files[i]}', progress_file='{progress_file}')
"
echo "Process {i} finished. Press enter to close."
read
""")
        os.chmod(script_path, 0o755)
        scripts.append(script_path)

    # Create tmux session
    subprocess.run(
        f"tmux new-session -d -s {SESSION_NAME} -x 200 -y 50 'bash {scripts[0]}'",
        shell=True,
    )

    for i in range(1, num_procs):
        if i % 2 == 1:
            subprocess.run(
                f"tmux split-window -t {SESSION_NAME} -h 'bash {scripts[i]}'",
                shell=True,
            )
        else:
            subprocess.run(
                f"tmux split-window -t {SESSION_NAME} -v 'bash {scripts[i]}'",
                shell=True,
            )

    subprocess.run(f"tmux select-layout -t {SESSION_NAME} tiled", shell=True)

    print(f"\n✅ Launched {num_procs} processes in tmux session '{SESSION_NAME}'")
    print(f"   Attach:  tmux attach -t {SESSION_NAME}")
    print(f"   Kill:    tmux kill-session -t {SESSION_NAME}")
    print(
        f"   Re-run this script anytime — it validates, merges, and continues automatically."
    )


def main():
    num_procs = NUM_PROCESSES
    workers = WORKERS_PER_PROCESS

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--processes" and i + 1 < len(args):
            num_procs = int(args[i + 1])
            i += 2
        elif args[i] == "--workers" and i + 1 < len(args):
            workers = int(args[i + 1])
            i += 2
        else:
            i += 1

    launch(num_procs, workers)


if __name__ == "__main__":
    main()
