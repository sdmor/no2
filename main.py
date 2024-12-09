import os
import shutil
import subprocess
import pytz
from datetime import datetime, timedelta
from google.cloud import storage
import sys

# Configure timezone to EST
est = pytz.timezone('US/Eastern')
now_est = datetime.now(est)

# Environment variable for GCS bucket
BUCKET_NAME = os.getenv("BUCKET_NAME", "no2-app-data")
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

# Temporary local directories
download_directory = "./downloads"  # Local folder within the container
extracted_directory = os.path.join(download_directory, "extracted")  # Local folder for extracted files

# GCS directory for processed data
gcs_data_directory = "data"  # Path in the bucket

# Ensure the local downloads directory exists
os.makedirs(download_directory, exist_ok=True)

# Parse input dates
try:
    start_date = est.localize(datetime.strptime(sys.argv[1], "%Y-%m-%d"))
    end_date = est.localize(datetime.strptime(sys.argv[2], "%Y-%m-%d"))
except IndexError:
    print("Error: Please provide start_date and end_date in 'YYYY-MM-DD' format.")
    sys.exit(1)
except ValueError:
    print("Error: Invalid date format. Use 'YYYY-MM-DD'.")
    sys.exit(1)


def upload_to_gcs(local_path, gcs_path):
    """Uploads a file to GCS."""
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    print(f"Uploaded {local_path} to gs://{BUCKET_NAME}/{gcs_path}")


def process_dates():
    """Process data for the given date range."""
    current_date = start_date
    while current_date <= end_date:
        while True:
            year = current_date.strftime("%Y")
            month = current_date.strftime("%m")
            day = current_date.strftime("%d")
            formatted_date = f"{year}{month}{day}"
            
            is_nrt = (now_est - current_date).days < 10
            extension = "_nrt" if is_nrt else ""
            file_url = f"https://d1qb6yzwaaq4he.cloudfront.net/tropomi/no2/{year}/{month}/tropomi_no2_{year}{month}{day}{extension}.tar"
            tar_file_name = f"tropomi_no2_{formatted_date}{extension}.tar"
            tar_file_path = os.path.join(download_directory, tar_file_name)

            # Step 1: Attempt to download the file
            download_command = [
                "python3",
                "sub/download_data.py",
                file_url,
                download_directory,
            ]
            try:
                subprocess.run(download_command, check=True)
                print(f"Successfully downloaded data for {current_date.strftime('%Y-%m-%d')}.")
                break  # Exit the loop after a successful download
            except subprocess.CalledProcessError as e:
                print(f"Error downloading {file_url}: {e}")
                current_date += timedelta(days=1)
                if current_date > end_date:
                    print("Reached the end of the date range while trying to find a valid file.")
                    return
                print(f"Moving to the next date: {current_date.strftime('%Y-%m-%d')}")

        # Step 2: Extract the .tar file
        extract_command = [
            "python3",
            "sub/extract_data.py",
            tar_file_path,
            extracted_directory,
        ]
        try:
            subprocess.run(extract_command, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error extracting files: {e}")
            break

        # Step 3: Convert .nc files to Parquet
        parquet_file_name = f"d{formatted_date}.parquet"
        make_parquet_command = [
            "python3",
            "sub/make_parquet.py",
            BUCKET_NAME,
            extracted_directory,
            f"{gcs_data_directory}/days",
            parquet_file_name
        ]
        try:
            subprocess.run(make_parquet_command, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error creating Parquet file: {e}")
            break

        # Cleanup after processing the day's data
        print(f"Cleaning up after {current_date.strftime('%Y-%m-%d')}...")
        try:
            shutil.rmtree(extracted_directory)
            shutil.rmtree(download_directory)
            print(f"Temporary files for {current_date.strftime('%Y-%m-%d')} deleted successfully.")
        except Exception as e:
            print(f"Error cleaning up temporary files for {current_date.strftime('%Y-%m-%d')}: {e}")

        current_date += timedelta(days=1)


def process_countries():
    """Run make_countries.py to process country-level parquets."""
    geojson_path = os.path.abspath("geojson/ne_110m_admin_0_countries.geojson")  # Resolve absolute path to GeoJSON

    """Run make_countries.py to process country-level parquets."""
    make_countries_command = [
        "python3",
        "sub/make_countries.py",
        BUCKET_NAME,
        f"{gcs_data_directory}/days",  # GCS folder with day Parquets
        f"{gcs_data_directory}/countries",  # GCS folder for country Parquets
        geojson_path,
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    ]

    try:
        subprocess.run(make_countries_command, check=True, capture_output=True, text=True)
        print("make_countries.py executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running make_countries.py: {e}")
        print(f"Command output:\n{e.stderr}")

def main():
    """Main entry point for the script."""
    #process_dates()
    process_countries()


if __name__ == "__main__":
    main()