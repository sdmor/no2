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
    """
    try:
        print(f"Processing Parquet file: {blob.name}")
        
        # Extract the date from the file name
        date_str = blob.name.split("/")[-1].replace("d", "").replace(".parquet", "")
        date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
        print(f"Extracted date: {date}")

        # Download the Parquet data
        print(f"Downloading Parquet file: {blob.name}")
        parquet_data = blob.download_as_bytes()
        input_file = BytesIO(parquet_data)
        parquet_file = parquet.ParquetFile(input_file)
        print(f"Parquet file downloaded successfully: {blob.name}")

        # Process file in chunks
        for i, batch in enumerate(parquet_file.iter_batches(batch_size=chunk_size)):
            print(f"Processing chunk {i + 1} of file {blob.name}")
            df = batch.to_pandas()
            print(f"Chunk {i + 1} loaded into DataFrame with {len(df)} rows")

            # Convert DataFrame to GeoDataFrame
            gdf = gpd.GeoDataFrame(
                df,
                geometry=gpd.points_from_xy(df['longitude'], df['latitude']),
                crs="EPSG:4326"
            )
            print(f"Chunk {i + 1} converted to GeoDataFrame")

            # Spatial join with countries
            joined = gpd.sjoin(gdf, world, how="inner", predicate="within")
            print(f"Chunk {i + 1} spatially joined with countries")

            # Group by country and calculate mean NO2
            grouped = joined.groupby("NAME")["nitrogendioxide_tropospheric_column"].mean().reset_index()
            grouped.rename(columns={"NAME": "Country", "nitrogendioxide_tropospheric_column": "NO2"}, inplace=True)
            grouped["Date"] = date
            print(f"Chunk {i + 1} grouped by country and NO2 mean calculated")

            # Save results to corresponding country Parquet files
            for _, row in grouped.iterrows():
                country_file = f"{countries_folder}/{row['Country'].lower().replace(' ', '_')}.parquet"
                country_blob = bucket.blob(country_file)

                # Check if the country's file already exists in GCS
                if country_blob.exists():
                    print(f"Country file exists in GCS: {country_file}")
                    existing_data = country_blob.download_as_bytes()
                    existing_df = pd.read_parquet(BytesIO(existing_data))
                    existing_df = existing_df[existing_df["Date"] != date]
                    updated_df = pd.concat([existing_df, pd.DataFrame([row.drop("Country")])], ignore_index=True)
                    updated_df = updated_df.sort_values(by="Date", key=lambda x: pd.to_datetime(x))
                else:
                    print(f"Country file does not exist in GCS: {country_file}")
                    updated_df = pd.DataFrame([row.drop("Country")])

                # Save the updated file back to GCS
                parquet_buffer = BytesIO()
                updated_df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)
                country_blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
                print(f"Country file updated in GCS: {country_file}")

        print(f"Finished processing Parquet file: {blob.name}")
    except Exception as e:
        print(f"Error in process_parquet_in_chunks for file {blob.name}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def main(bucket_name, days_folder, countries_folder, geojson_path, start_date, end_date):
    try:
        print(f"Starting make_countries.py with bucket: {bucket_name}")
        
        # Initialize Google Cloud Storage client
        print("Initializing Google Cloud Storage client...")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        print("Google Cloud Storage client initialized")

        # Load GeoJSON file
        print(f"Loading GeoJSON file from: {geojson_path}")
        world = gpd.read_file(geojson_path)
        print(f"GeoJSON file loaded. Found {len(world)} geometries.")

        # List all Parquet files in the days folder
        print(f"Listing blobs in folder: {days_folder}")
        blobs = list(storage_client.list_blobs(bucket_name, prefix=days_folder))
        print(f"Found {len(blobs)} blobs in {days_folder}.")

        for blob in blobs:
            if blob.name.endswith(".parquet"):
                date_str = blob.name.split("/")[-1].replace("d", "").replace(".parquet", "")
                file_date = datetime.strptime(date_str, "%Y%m%d")
                if start_date <= file_date <= end_date:
                    try:
                        print(f"Processing blob: {blob.name}")
                        process_parquet_in_chunks(blob, bucket, countries_folder, world)
                    except Exception as e:
                        print(f"Error processing Parquet file {blob.name}: {e}")
                        import traceback
                        traceback.print_exc()
    except Exception as e:
        print(f"Error in main function: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

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

    print(f"Absolute GeoJSON path: {geojson_path}")
    main(bucket_name, days_folder, countries_folder, geojson_path, start_date, end_date)
