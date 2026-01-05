import json
import time
from pathlib import Path

import polars as pl

CSV = Path("processed/trades.csv")
OUT_DIR = Path("processed/trades_parquet")
STATE_PATH = OUT_DIR / "_state.json"

BATCH_ROWS = 1_000_000          # lower if you want less peak memory
COMPRESSION = "zstd"
LOG_EVERY = 1                 # batches

schema = {
    "timestamp": pl.Utf8,
    "market_id": pl.Int64,
    "maker": pl.Utf8,
    "taker": pl.Utf8,
    "nonusdc_side": pl.Utf8,
    "maker_direction": pl.Utf8,
    "taker_direction": pl.Utf8,
    "price": pl.Float64,
    "usd_amount": pl.Float64,
    "token_amount": pl.Float64,
    "transactionHash": pl.Utf8,
}


def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {"rows_done": 0, "next_part": 0}


def save_state(rows_done: int, next_part: int) -> None:
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps({"rows_done": rows_done, "next_part": next_part}, indent=2))
    tmp.replace(STATE_PATH)


def main() -> None:
    if not CSV.exists():
        raise FileNotFoundError(f"Missing input CSV: {CSV}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    st = load_state()
    rows_done = int(st.get("rows_done", 0))
    next_part = int(st.get("next_part", 0))

    print(f"📥 Input:  {CSV}")
    print(f"📦 Output: {OUT_DIR} (shards: part-########.parquet)")
    print(f"↩️  Resume: rows_done={rows_done:,} next_part={next_part:,}")

    t0 = time.time()
    total_read = 0
    total_written = 0
    batches = 0

    # Skip already-processed rows (data rows, not counting header)
    reader = pl.read_csv_batched(
        str(CSV),
        batch_size=BATCH_ROWS,
        has_header=True,
        schema_overrides=schema,
        ignore_errors=True,
        low_memory=True,
        skip_rows_after_header=rows_done,
    )

    while True:
        batch_list = reader.next_batches(1)
        if not batch_list:
            break

        batches += 1
        df = batch_list[0]
        total_read += df.height

        # Parse timestamp -> Datetime (strict=False => unparsable -> null)
        df = df.with_columns(
            pl.col("timestamp").str.to_datetime(strict=False).alias("timestamp")
        )

        out_path = OUT_DIR / f"part-{next_part:08d}.parquet"
        df.write_parquet(out_path, compression=COMPRESSION, statistics=True)

        total_written += df.height
        rows_done += df.height
        next_part += 1

        # Persist checkpoint after each successful shard write
        save_state(rows_done=rows_done, next_part=next_part)

        if batches % LOG_EVERY == 0:
            elapsed = time.time() - t0
            rps = total_read / elapsed if elapsed > 0 else 0.0
            print(
                f"✓ batch={batches:,} wrote={df.height:,} -> {out_path.name} | "
                f"rows_done={rows_done:,} | rate={rps:,.0f} rows/s"
            )

        del df

    elapsed = time.time() - t0
    rps = total_read / elapsed if elapsed > 0 else 0.0
    print("=" * 70)
    print(
        f"✅ Done. shards={next_part:,} new_rows={total_written:,} "
        f"elapsed={elapsed:,.1f}s avg_rate={rps:,.0f} rows/s"
    )
    print(f"📦 Parquet glob: {OUT_DIR}/part-*.parquet")
    print("=" * 70)


if __name__ == "__main__":
    main()
