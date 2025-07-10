#!/usr/bin/env python3
import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

def download_file(url, local_path):
    """
    Download a file from a public S3 bucket with a progress bar.
    
    :param url: Direct URL to the file in the S3 bucket
    :param local_path: Path to save the file locally
    :return: True if file was downloaded, False if skipped
    """
    # Check if file already exists
    if os.path.exists(local_path):
        print(f"File {local_path} already exists, skipping download.")
        return False
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        with open(local_path, 'wb') as file, tqdm(
            desc=f"Downloading {os.path.basename(local_path)}",
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
        ) as progress_bar:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
                progress_bar.update(len(chunk))
        print(f"Downloaded {url} to {local_path}")
        return True
    except Exception as e:
        print(f"Error downloading file: {e}")
        return False

def list_and_download_csv_files(bucket_url, local_dir):
    """
    List all CSV files in a public S3 bucket and download them.
    
    :param bucket_url: Public URL of the S3 bucket
    :param local_dir: Local directory to save the CSV files
    :return: Tuple of (downloaded_count, skipped_count, total_csv_files)
    """
    downloaded_count = 0
    skipped_count = 0
    total_csv_files = 0
    
    try:
        response = requests.get(bucket_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'xml')
        contents = soup.find_all('Contents')
        
        for content in contents:
            key = content.find('Key').text
            if key.endswith('.csv'):
                total_csv_files += 1
                file_url = bucket_url.rstrip('/') + '/' + key
                local_path = os.path.join(local_dir, key)
                if download_file(file_url, local_path):
                    downloaded_count += 1
                else:
                    skipped_count += 1
    except Exception as e:
        print(f"Error listing or downloading files: {e}")
    
    return downloaded_count, skipped_count, total_csv_files

if __name__ == "__main__":
    bucket_url = "https://bts-flights-data.s3.us-west-2.amazonaws.com/"
    local_dir = "csv/"
    
    # Create directory only if it doesn't exist
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
        print(f"Created directory: {local_dir}")
    else:
        print(f"Directory {local_dir} already exists")
    
    print("Downloading CSV files from flight data bucket...")
    downloaded_count, skipped_count, total_csv_files = list_and_download_csv_files(bucket_url, local_dir)
    
    if total_csv_files == 0:
        print("No CSV files found in the bucket.")
    elif downloaded_count > 0:
        if skipped_count > 0:
            print(f"Download complete! {downloaded_count} file(s) downloaded, {skipped_count} file(s) skipped (already exist).")
        else:
            print(f"Download complete! {downloaded_count} file(s) downloaded.")
    else:
        print(f"No files downloaded - all {total_csv_files} CSV file(s) already exist locally.")
