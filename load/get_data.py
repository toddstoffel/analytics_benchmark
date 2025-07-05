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

def list_and_download_csv_files(bucket_url, local_dir):
    """
    List all CSV files in a public S3 bucket and download them.
    
    :param bucket_url: Public URL of the S3 bucket
    :param local_dir: Local directory to save the CSV files
    """
    try:
        response = requests.get(bucket_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'xml')
        contents = soup.find_all('Contents')
        
        for content in contents:
            key = content.find('Key').text
            if key.endswith('.csv'):
                file_url = bucket_url.rstrip('/') + '/' + key
                local_path = os.path.join(local_dir, key)
                download_file(file_url, local_path)
    except Exception as e:
        print(f"Error listing or downloading files: {e}")

if __name__ == "__main__":
    bucket_url = "https://bts-flights-data.s3.us-west-2.amazonaws.com/"
    local_dir = "csv/"
    
    os.makedirs(local_dir, exist_ok=True)
    
    print("Downloading CSV files from flight data bucket...")
    list_and_download_csv_files(bucket_url, local_dir)
    print("Download complete!")
