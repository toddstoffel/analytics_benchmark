#!/usr/bin/env python3
import os
import argparse
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

def download_file(url, local_path):
    """
    Download a file from a public S3 bucket with a progress bar.
    
    :param url: Direct URL to the file in the S3 bucket
    :param local_path: Path to save the file locally
    """
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
    except Exception as e:
        print(f"Error downloading file: {e}")

def list_and_download_files(bucket_url, local_dir, file_type):
    """
    List all files of a specific type in a public S3 bucket and download them.
    
    :param bucket_url: Public URL of the S3 bucket
    :param local_dir: Local directory to save the files
    :param file_type: File type to download (e.g., 'csv', 'bson', or 'json')
    """
    try:
        response = requests.get(bucket_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'xml')
        contents = soup.find_all('Contents')
        
        for content in contents:
            key = content.find('Key').text
            if file_type == 'bson' and (key.endswith('.bson') or key.endswith('.json')):
                file_url = bucket_url.rstrip('/') + '/' + key
                local_path = os.path.join(local_dir, key)
                download_file(file_url, local_path)
            elif key.endswith(f'.{file_type}'):
                file_url = bucket_url.rstrip('/') + '/' + key
                local_path = os.path.join(local_dir, key)
                download_file(file_url, local_path)
    except Exception as e:
        print(f"Error listing or downloading files: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download files from a public S3 bucket.")
    parser.add_argument("file_type", nargs="?", choices=["csv", "bson"],
                        help="File type to download (csv or bson). If not supplied, you'll be prompted.")
    args = parser.parse_args()

    file_type = args.file_type
    if not file_type:
        file_type = input("Enter the file type to download (csv or bson): ").strip().lower()
    
    if file_type in ["csv", "bson"]:
        bucket_url = "https://bts-flights-data.s3.us-west-2.amazonaws.com/"
        local_dir = "csv/"
        
        os.makedirs(local_dir, exist_ok=True)

        list_and_download_files(bucket_url, local_dir, file_type)
    else:
        print("Invalid file type. Please enter 'csv' or 'bson'.")
