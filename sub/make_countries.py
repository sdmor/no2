import os
import sys
import pandas as pd
import geopandas as gpd
from datetime import datetime
from google.cloud import storage
from io import BytesIO
from pyarrow import parquet

def process_parquet_in_chunks(blob, bucket, countries_folder, world, chunk_size=100000):
    """
    Process a Parquet file in chunks and update country Parquet files.
    
    Args:
        blob: GCS blob object of the Parquet file.
        bucket: GCS bucket object.
        countries_folder: GCS folder for output Parquet files.
        world: GeoDataFrame of the world geometries.
        chunk_size: Number of rows per chunk to process.
    """
    # Extract the date from the file name
    date_str = blob.name.split("/")[-1].replace("d", "").replace(".parquet", "")
    date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    
    print(f"Processing Parquet file in chunks: {blob.name}")
    parquet_data = blob.download_as_bytes()
    input_file = BytesIO(parquet_data)
    parquet_file = parquet.ParquetFile(input_file)
    
    # Process file in chunks
    for i, batch in enumerate(parquet_file.iter_batches(batch_size=chunk_size)):
        print(f"Processing chunk {i + 1}...")
        df = batch.to_pandas()
        
        # Convert DataFrame to GeoDataFrame
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df['longitude'], df['latitude']),
            crs="EPSG:4326"
        )
        
        # Spatial join with countries
        joined = gpd.sjoin(gdf, world, how="inner", predicate="within")
        
        # Group by country and calculate mean NO2
        grouped = joined.groupby("NAME")["nitrogendioxide_tropospheric_column"].mean().reset_index()
        grouped.rename(columns={"NAME": "Country", "nitrogendioxide_tropospheric_column": "NO2"}, inplace=True)
        grouped["Date"] = date

        # Reformat and save results to the corresponding country Parquet file
        for _, row in grouped.iterrows():
            country_file = f"{countries_folder}/{row['Country'].lower().replace(' ', '_')}.parquet"
            country_blob = bucket.blob(country_file)

            # Check if the country's file already exists in GCS
            if country_blob.exists():
                existing_data = country_blob.download_as_bytes()
                existing_df = pd.read_parquet(BytesIO(existing_data))
                existing_df = existing_df[existing_df["Date"] != date]
                updated_df = pd.concat([existing_df, pd.DataFrame([row.drop("Country")])], ignore_index=True)
                updated_df = updated_df.sort_values(by="Date", key=lambda x: pd.to_datetime(x))
            else:
                updated_df = pd.DataFrame([row.drop("Country")])

            # Save the updated file back to GCS
            parquet_buffer = BytesIO()
            updated_df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            country_blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
    print(f"Finished processing {blob.name}.")

def main(bucket_name, days_folder, countries_folder, geojson_path, start_date, end_date):
    # Initialize Google Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Load GeoJSON file
    world = gpd.read_file(geojson_path)
    print(f"GeoJSON file loaded. Found {len(world)} geometries.")

    # List all Parquet files in the days folder
    blobs = list(storage_client.list_blobs(bucket_name, prefix=days_folder))
    print(f"Found {len(blobs)} blobs in {days_folder}.")

    for blob in blobs:
        if blob.name.endswith(".parquet"):
            date_str = blob.name.split("/")[-1].replace("d", "").replace(".parquet", "")
            file_date = datetime.strptime(date_str, "%Y%m%d")
            if start_date <= file_date <= end_date:
                try:
                    print(f"Processing Parquet file: {blob.name}")
                    process_parquet_in_chunks(blob, bucket, countries_folder, world)
                except Exception as e:
                    print(f"Error processing Parquet file {blob.name}: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Usage: python3 make_countries.py <BUCKET_NAME> <DAYS_FOLDER> <COUNTRIES_FOLDER> <GEOJSON_PATH> <START_DATE: YYYY-MM-DD> <END_DATE: YYYY-MM-DD>")
        sys.exit(1)

    bucket_name = sys.argv[1]
    days_folder = sys.argv[2]
    countries_folder = sys.argv[3]
    geojson_path = os.path.abspath(sys.argv[4])
    start_date = datetime.strptime(sys.argv[5], "%Y-%m-%d")
    end_date = datetime.strptime(sys.argv[6], "%Y-%m-%d")

    # Debugging line to print the geojson_path
    print(f"Absolute GeoJSON path: {geojson_path}")

    main(bucket_name, days_folder, countries_folder, geojson_path, start_date, end_date)
