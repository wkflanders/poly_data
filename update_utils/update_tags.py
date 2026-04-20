import requests
import csv
import os
import time

GAMMA_BASE = "https://gamma-api.polymarket.com"
MARKETS_CSV = "markets.csv"


def fetch_all_tags():
    """Fetch all tags from GET /tags."""
    while True:
        try:
            resp = requests.get(f"{GAMMA_BASE}/tags", timeout=30)
            if resp.status_code == 429:
                print("Rate limited, waiting 10s...")
                time.sleep(10)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching tags: {e}, retrying in 5s...")
            time.sleep(5)


def fetch_events_by_tag(tag_id, batch_size=500):
    """Fetch all events for a tag_id using GET /events?tag_id=..."""
    offset = 0
    events = []
    while True:
        params = {
            "tag_id": tag_id,
            "limit": batch_size,
            "offset": offset,
            "related_tags": "true",
        }
        try:
            resp = requests.get(f"{GAMMA_BASE}/events", params=params, timeout=30)
            if resp.status_code == 429:
                time.sleep(10)
                continue
            if resp.status_code == 500:
                time.sleep(5)
                continue
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            events.extend(batch)
            offset += len(batch)
            if len(batch) < batch_size:
                break
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}, retrying...")
            time.sleep(5)
    return events


def update_tags(markets_csv=MARKETS_CSV):
    """
    Fetch all tags from /tags, then for each tag fetch all events under it
    via /events?tag_id=..., extract the markets from each event, and write
    a 'tags' column back into markets.csv.
    """
    print("Fetching tags from GET /tags...")
    tags = fetch_all_tags()
    print(f"Found {len(tags)} tags:")
    for t in tags:
        print(f"  id={t.get('id')}  label={t.get('label') or t.get('slug')}")

    # market id (str) -> set of tag labels
    market_id_to_tags = {}

    for tag in tags:
        tag_id = tag.get("id")
        tag_label = tag.get("label") or tag.get("slug") or tag.get("name", "")
        if not tag_id or not tag_label:
            continue

        print(f"Fetching events for tag '{tag_label}'...")
        events = fetch_events_by_tag(tag_id)
        print(f"  {len(events)} events")

        for event in events:
            for market in event.get("markets", []):
                mid = str(market.get("id", ""))
                if not mid:
                    continue
                if mid not in market_id_to_tags:
                    market_id_to_tags[mid] = set()
                market_id_to_tags[mid].add(tag_label)

    print(f"\nTagged {len(market_id_to_tags):,} markets total")

    # Update markets.csv
    if not os.path.exists(markets_csv):
        print(f"{markets_csv} not found")
        return

    print(f"Updating {markets_csv}...")
    rows = []
    with open(markets_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        if "tags" not in fieldnames:
            fieldnames.append("tags")
        for row in reader:
            mid = str(row.get("id", "")).strip()
            row["tags"] = "|".join(sorted(market_id_to_tags.get(mid, [])))
            rows.append(row)

    with open(markets_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    tagged = sum(1 for r in rows if r.get("tags"))
    print(f"Done. {tagged:,}/{len(rows):,} markets have tags")


if __name__ == "__main__":
    update_tags()
