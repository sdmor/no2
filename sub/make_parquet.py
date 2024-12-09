import os
import sys
import h5py
import pandas as pd
from google.cloud import storage
from io import BytesIO

def process_nc_files_to_parquet(local_folder, bucket, gcs_output_path):
    """
    Processes .nc files in the local folder, filters data, and uploads the Parquet file directly to GCS.

    Args:
        local_folder (str): Directory containing .nc files.
        bucket: GCS bucket object.
        gcs_output_path (str): Full path in GCS to save the Parquet file.

    Returns:
        None
    """
    datasets_to_extract = [
        "PRODUCT/latitude",
        "PRODUCT/longitude",
        "PRODUCT/nitrogendioxide_tropospheric_column",
        "PRODUCT/qa_value"
    ]

    all_data = []

    # Process all .nc files
    for file in os.listdir(local_folder):
        if file.endswith(".nc"):
            file_path = os.path.join(local_folder, file)
            print(f"Processing file: {file_path}")

            try:
                with h5py.File(file_path, "r") as f:
                    data = {}
                    for dataset_name in datasets_to_extract:
                        if dataset_name in f:
                            dataset = f[dataset_name][:]
                            data[dataset_name.split("/")[-1]] = dataset.flatten()
                        else:
                            print(f"Dataset {dataset_name} not found in {file}. Skipping.")

                    if data:
                        df = pd.DataFrame(data)
                        # Filter rows based on qa_value > 75
                        df = df[df["qa_value"] > 75]
                        all_data.append(df)
            except Exception as e:
                print(f"Error processing file {file}: {e}")

    # Combine and save directly to GCS as Parquet
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        try:
            # Save the DataFrame to a Parquet file in memory
            parquet_buffer = BytesIO()
            combined_df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)

            # Upload the Parquet file to GCS
            blob = bucket.blob(gcs_output_path)
            blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
            print(f"Parquet file uploaded directly to GCS: gs://{bucket.name}/{gcs_output_path}")
        except Exception as e:
            print(f"Error saving Parquet file: {e}")
    else:
        print("No valid data found to save.")

def main(bucket_name, local_input_folder, gcs_output_folder, output_filename):
    # Initialize Google Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Validate bucket existence
    if not bucket.exists():
        print(f"Error: Bucket {bucket_name} does not exist.")
        sys.exit(1)

    # Construct the GCS output path
    gcs_output_path = os.path.join(gcs_output_folder, output_filename)
    print(f"Starting processing of .nc files in {local_input_folder}")
    print(f"Output will be saved to: gs://{bucket_name}/{gcs_output_path}")

    # Process files and upload to GCS
    process_nc_files_to_parquet(local_input_folder, bucket, gcs_output_path)

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python3 make_parquet.py <BUCKET_NAME> <LOCAL_INPUT_FOLDER> <GCS_OUTPUT_FOLDER> <OUTPUT_FILENAME>")
        sys.exit(1)

    bucket_name = sys.argv[1]
    local_input_folder = sys.argv[2]  # Example: downloads/extracted
    gcs_output_folder = sys.argv[3]  # Example: data/days
    output_filename = sys.argv[4]    # Example: d20241110.parquet

    main(bucket_name, local_input_folder, gcs_output_folder, output_filename)
