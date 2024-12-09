import os
import sys
import tarfile

def extract_tar_file(local_tar_path, local_extracted_path):
    """
    Extracts the contents of a .tar file into the specified directory.

    Args:
        local_tar_path (str): The local path to the .tar file.
        local_extracted_path (str): The local directory where extracted files will be saved.
    """
    print(f"Extracting {local_tar_path} to {local_extracted_path}...")
    os.makedirs(local_extracted_path, exist_ok=True)

    try:
        with tarfile.open(local_tar_path, "r") as tar:
            tar.extractall(path=local_extracted_path)
        print(f"Extraction complete!")
    except Exception as e:
        print(f"Failed to extract {local_tar_path}. Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Ensure proper arguments are provided
    if len(sys.argv) != 3:
        print("Usage: python3 extract_data.py <TAR_FILE_LOCAL_PATH> <EXTRACTED_LOCAL_PATH>")
        sys.exit(1)

    local_tar_path = sys.argv[1]
    local_extracted_path = sys.argv[2]

    # Step 1: Extract the .tar file locally
    try:
        extract_tar_file(local_tar_path, local_extracted_path)
    except Exception as e:
        print(f"Error extracting .tar file: {e}")
        sys.exit(1)

    print("Extraction process completed successfully.")
