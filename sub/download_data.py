import os
import sys
import requests

def download_file(url, save_directory):
    """
    Downloads a file from the given URL and saves it to the specified directory.

    Args:
        url (str): The URL of the file to download.
        save_directory (str): The directory to save the downloaded file.

    Returns:
        str: The path of the downloaded file.
    """
    # Extract the filename from the URL
    filename = os.path.basename(url)
    file_path = os.path.join(save_directory, filename)

    # Download the file
    print(f"Downloading file from {url}...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        print(f"Download complete! File saved at: {file_path}")
        return file_path
    else:
        print(f"Failed to download file. Status code: {response.status_code}")
        print(f"URL attempted: {url}")
        sys.exit(1)  # Exit with an error if the download fails

if __name__ == "__main__":
    # Ensure the URL and local save directory are provided as command-line arguments
    if len(sys.argv) != 3:
        print("Usage: python3 download_data.py <URL> <LOCAL_SAVE_DIRECTORY>")
        sys.exit(1)

    # Read the arguments
    url = sys.argv[1]
    save_directory = sys.argv[2]

    # Ensure the save directory exists
    os.makedirs(save_directory, exist_ok=True)

    # Step: Download the file locally
    try:
        download_file(url, save_directory)
    except Exception as e:
        print(f"Error downloading file: {e}")
        sys.exit(1)
