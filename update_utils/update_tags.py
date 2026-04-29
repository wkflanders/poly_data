import requests
import csv
import os
import time
import json

GAMMA_BASE = "https://gamma-api.polymarket.com"
MARKETS_CSV = "markets.csv"
CATEGORY_FILE = "category_lookup.json"
PROGRESS_FILE = "category_progress.json"

SKIP_TAGS = {"all", "games"}  # useless tags to ignore


def load_progress():
    progress = {"offset": 0}
    if os.path.isfile(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)
    lookup = {}
    if os.path.isfile(CATEGORY_FILE):
        with open(CATEGORY_FILE) as f:
            lookup = json.load(f)
    return progress["offset"], lookup


def save_progress(offset, lookup):
    tmp = PROGRESS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"offset": offset}, f)
    os.replace(tmp, PROGRESS_FILE)
    tmp = CATEGORY_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(lookup, f)
    os.replace(tmp, CATEGORY_FILE)


def extract_tags(event):
    # Prefer explicit category field
    category = event.get("category", "")
    if category:
        return category

    # Fall back to tags array, filtering noise
    tags = event.get("tags") or []
    labels = []
    for t in tags:
        label = t.get("label") or t.get("slug", "")
        if label and label.lower() not in SKIP_TAGS:
            labels.append(label)
    return "|".join(labels)


def fetch_categories(batch_size=500):
    offset, lookup = load_progress()

    if offset > 0:
        print(f"Resuming from offset {offset:,} ({len(lookup):,} markets already tagged)")
    else:
        print("Starting fresh...")

    try:
        while True:
            params = {
                "order": "createdAt",
                "ascending": "true",
                "limit": batch_size,
                "offset": offset,
            }
            try:
                resp = requests.get(f"{GAMMA_BASE}/events", params=params, timeout=30)
                if resp.status_code == 429:
                    print("Rate limited, waiting 10s...")
                    time.sleep(10)
                    continue
                if resp.status_code == 500:
                    time.sleep(5)
                    continue
                resp.raise_for_status()
                batch = resp.json()
            except requests.exceptions.RequestException as e:
                print(f"Network error: {e}, retrying in 5s...")
                time.sleep(5)
                continue

            if not batch:
                break

            for event in batch:
                tags_str = extract_tags(event)
                for market in event.get("markets", []):
                    mid = str(market.get("id", "")).strip()
                    if mid and tags_str:
                        lookup[mid] = tags_str

            offset += len(batch)
            save_progress(offset, lookup)
            print(f"  {offset:,} events, {len(lookup):,} markets tagged")

            if len(batch) < batch_size:
                break

    except KeyboardInterrupt:
        print(f"\nStopped at offset {offset:,}. Re-run to continue.")
        return False

    return True


def apply_categories(markets_csv=MARKETS_CSV):
    if not os.path.isfile(CATEGORY_FILE):
        print("No category lookup found — run fetch_categories first")
        return

    print("Loading category lookup...")
    with open(CATEGORY_FILE) as f:
        lookup = json.load(f)
    print(f"  {len(lookup):,} markets in lookup")

    tmp_csv = markets_csv + ".tmp"
    print(f"Streaming {markets_csv} -> adding tags column...")

    tagged = 0
    total = 0
    with open(markets_csv, "r", encoding="utf-8") as fin, \
         open(tmp_csv, "w", newline="", encoding="utf-8") as fout:

        reader = csv.DictReader(fin)
        fieldnames = list(reader.fieldnames or [])
        if "tags" not in fieldnames:
            fieldnames.append("tags")

        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            mid = str(row.get("id", "")).strip()
            row["tags"] = lookup.get(mid, "")
            if row["tags"]:
                tagged += 1
            writer.writerow(row)
            total += 1
            if total % 100000 == 0:
                print(f"  {total:,} rows written...")

    os.replace(tmp_csv, markets_csv)
    print(f"Done. {tagged:,}/{total:,} markets have tags")

    os.remove(CATEGORY_FILE)
    if os.path.isfile(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)


def update_tags(markets_csv=MARKETS_CSV):
    done = fetch_categories()
    if done:
        apply_categories(markets_csv)


if __name__ == "__main__":
    update_tags()
