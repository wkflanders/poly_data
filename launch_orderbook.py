#!/usr/bin/env python3
"""
Splits remaining markets into N processes across tmux panes.

Usage:
  python launch_orderbook.py                    # 4 processes, 100 workers each
  python launch_orderbook.py --processes 8      # 8 processes
  python launch_orderbook.py --workers 50       # 50 workers per process
  python launch_orderbook.py --merge            # merge progress files after completion

Each process gets its own progress file to avoid corruption.
Run with --merge after all processes finish to combine them.
"""

import json
import csv
import os
import sys
import subprocess
import shutil
from datetime import datetime, timezone

NUM_PROCESSES = 4
WORKERS_PER_PROCESS = 100
SESSION_NAME = "orderbook"
CHUNK_DIR = "orderbook_chunks"
PROGRESS_DIR = "orderbook_snapshots"
MAIN_PROGRESS = os.path.join(PROGRESS_DIR, "progress.json")


def get_remaining_markets():
    cutoff = datetime(2025, 11, 12, tzinfo=timezone.utc)

    progress = {}
    if os.path.isfile(MAIN_PROGRESS):
        with open(MAIN_PROGRESS, "r") as f:
            progress = json.load(f)

    # Also check per-process progress files
    for f in os.listdir(PROGRESS_DIR) if os.path.isdir(PROGRESS_DIR) else []:
        if f.startswith("progress_") and f.endswith(".json"):
            with open(os.path.join(PROGRESS_DIR, f), "r") as fh:
                p = json.load(fh)
                progress.update(p)

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


def merge_progress():
    """Merge all per-process progress files into the main progress.json."""
    main = {}
    if os.path.isfile(MAIN_PROGRESS):
        with open(MAIN_PROGRESS, "r") as f:
            main = json.load(f)

    merged = 0
    for fname in sorted(os.listdir(PROGRESS_DIR)):
        if not fname.startswith("progress_") or not fname.endswith(".json"):
            continue
        path = os.path.join(PROGRESS_DIR, fname)
        with open(path, "r") as f:
            chunk_progress = json.load(f)
        main.update(chunk_progress)
        merged += len(chunk_progress)
        os.remove(path)
        print(f"  Merged {fname}: {len(chunk_progress):,} entries")

    with open(MAIN_PROGRESS, "w") as f:
        json.dump(main, f)

    complete = sum(1 for v in main.values() if v.get("complete"))
    print(f"\nMerged {merged:,} entries into {MAIN_PROGRESS}")
    print(f"Total tracked: {len(main):,} ({complete:,} complete)")


def launch(num_procs, workers):
    # First, merge any leftover per-process progress files from a previous run
    has_leftover = any(
        f.startswith("progress_") and f.endswith(".json")
        for f in (os.listdir(PROGRESS_DIR) if os.path.isdir(PROGRESS_DIR) else [])
    )
    if has_leftover:
        print("Found leftover progress files from previous run, merging first...")
        merge_progress()

    remaining = get_remaining_markets()
    print(f"Remaining markets: {len(remaining):,}")
    print(f"Splitting into {num_procs} processes with {workers} workers each")
    print(f"Total concurrent requests: ~{num_procs * workers}")

    if not remaining:
        print("Nothing to do!")
        return

    # Clean up old chunk files
    chunks_dir = "orderbook_snapshots/chunks"
    if os.path.isdir(chunks_dir):
        subprocess.run(f"rm -rf {chunks_dir}", shell=True)
        print("Cleaned up leftover chunk files")

    # Split markets into N chunk files
    os.makedirs(CHUNK_DIR, exist_ok=True)

    chunk_files = []
    for i in range(num_procs):
        chunk = remaining[i::num_procs]
        chunk_file = os.path.join(CHUNK_DIR, f"chunk_{i}.txt")
        with open(chunk_file, "w") as f:
            f.write("\n".join(chunk))
        chunk_files.append(chunk_file)
        print(f"  Chunk {i}: {len(chunk):,} markets -> {chunk_file}")

    # Kill existing session if any
    subprocess.run(f"tmux kill-session -t {SESSION_NAME} 2>/dev/null", shell=True)

    # Write a shell script per process to avoid quote escaping issues
    scripts = []
    for i in range(num_procs):
        progress_file = os.path.join(PROGRESS_DIR, f"progress_{i}.json")
        # Copy main progress so each process knows what's already done
        if os.path.isfile(MAIN_PROGRESS):
            shutil.copy2(MAIN_PROGRESS, progress_file)

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

    # Create tmux session with first pane
    subprocess.run(
        f"tmux new-session -d -s {SESSION_NAME} -x 200 -y 50 'bash {scripts[0]}'",
        shell=True,
    )

    # Add remaining panes
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

    # Tile evenly
    subprocess.run(f"tmux select-layout -t {SESSION_NAME} tiled", shell=True)

    print(f"\n✅ Launched {num_procs} processes in tmux session '{SESSION_NAME}'")
    print(f"   Attach:  tmux attach -t {SESSION_NAME}")
    print(f"   Kill:    tmux kill-session -t {SESSION_NAME}")
    print(f"   Merge:   python launch_orderbook.py --merge")


def main():
    num_procs = NUM_PROCESSES
    workers = WORKERS_PER_PROCESS
    do_merge = False

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--processes" and i + 1 < len(args):
            num_procs = int(args[i + 1])
            i += 2
        elif args[i] == "--workers" and i + 1 < len(args):
            workers = int(args[i + 1])
            i += 2
        elif args[i] == "--merge":
            do_merge = True
            i += 1
        else:
            i += 1

    if do_merge:
        merge_progress()
    else:
        launch(num_procs, workers)


if __name__ == "__main__":
    main()
