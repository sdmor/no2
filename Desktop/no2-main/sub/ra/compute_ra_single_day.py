import os
import polars as pl
from google.cloud import storage
from io import BytesIO
from datetime import datetime

# ──────────────── Config ────────────────
BUCKET_NAME = "no2-app-data"
PREFIX = "data/days/"
OUTPUT_BUCKET = "no2-app-data"
OUTPUT_PREFIX = "data/rolling_avgs/"

# ──────────────── Helpers ────────────────

def bin_coord(expr, step=0.1):
    return (expr // step) * step

def load_daily_parquet(blob):
    print(f"📅 Downloading {blob.name}")
    content = blob.download_as_bytes()
    df = pl.read_parquet(BytesIO(content))

    df = df.filter(pl.col("qa_value") > 0.75)
    df = df.with_columns([
        bin_coord(pl.col("latitude"), 0.1).alias("lat_bin"),
        bin_coord(pl.col("longitude"), 0.1).alias("lon_bin"),
        pl.lit(datetime.strptime(blob.name.split("/")[-1][1:9], "%Y%m%d")).alias("date"),
    ])
    return df.select(["lat_bin", "lon_bin", "date", "nitrogendioxide_tropospheric_column"]).rename({
        "nitrogendioxide_tropospheric_column": "no2"
    })

def compute_7day_rolling(df):
    print("🌀 Computing rolling averages...")
    df = df.group_by(["lat_bin", "lon_bin", "date"]).agg(
        pl.col("no2").mean().alias("no2")
    )
    df = df.sort(["lat_bin", "lon_bin", "date"])
    df = df.with_columns(
        pl.col("no2")
        .rolling_mean(window_size=7, min_samples=7)
        .over(["lat_bin", "lon_bin"])
        .alias("no2_ra")
    )
    return df.drop_nulls("no2_ra")

def save_to_gcs(df, output_blob_name):
    buffer = BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)
    storage.Client().bucket(OUTPUT_BUCKET).blob(output_blob_name).upload_from_file(
        buffer, content_type='application/octet-stream'
    )
    print(f"✅ Saved {output_blob_name}")

# ──────────────── Main Process ────────────────

def process_single_day_output(target_date: str):
    client = storage.Client()
    blobs = list(client.list_blobs(BUCKET_NAME, prefix=PREFIX))

    filtered_blobs = [
        b for b in sorted(blobs, key=lambda b: b.name)
        if b.name.endswith(".parquet") and (
            datetime.strptime(target_date, "%Y%m%d") >= datetime.strptime(b.name.split("/")[-1][1:9], "%Y%m%d") >=
            datetime.strptime(target_date, "%Y%m%d") - timedelta(days=6)
        )
    ]

    if len(filtered_blobs) < 7:
        raise ValueError(f"❌ Only found {len(filtered_blobs)} days of data, need 7 for rolling average.")

    all_data = [load_daily_parquet(b) for b in filtered_blobs]
    full_df = pl.concat(all_data, how="vertical")
    rolling_df = compute_7day_rolling(full_df)

    # Save only the row for target date
    target = datetime.strptime(target_date, "%Y%m%d")
    daily = rolling_df.filter(pl.col("date") == target)
    out_name = f"{OUTPUT_PREFIX}ra_{target.strftime('%Y%m%d')}.parquet"
    save_to_gcs(daily, out_name)

if __name__ == "__main__":
    import sys
    import traceback
    from datetime import timedelta

    if len(sys.argv) != 2:
        raise ValueError("Usage: python compute_ra_single_day.py YYYYMMDD")

    try:
        process_single_day_output(sys.argv[1])
    except Exception:
        traceback.print_exc()
