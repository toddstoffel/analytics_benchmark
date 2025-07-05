#!/usr/bin/env python3
import os
import subprocess
import sys
import time
import mysql.connector
from mysql.connector import Error

def wait_for_tidb_connection(host="127.0.0.1", port=9030, user="root", password="", timeout=60):
    """
    Wait for TiDB to be ready for connections.
    """
    print("Waiting for TiDB to be ready...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            connection = mysql.connector.connect(
                host=host,
                port=port,
                user=user,
                password=password
            )
            if connection.is_connected():
                connection.close()
                print("TiDB is ready!")
                return True
        except Error:
            time.sleep(2)
    
    print(f"Timeout waiting for TiDB connection after {timeout} seconds")
    return False

def set_tiflash_replica():
    """
    Set TiFlash replica for the flights table.
    """
    try:
        connection = mysql.connector.connect(
            host="127.0.0.1",
            port=9030,
            user="root",
            password=""
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            print("Setting TiFlash replica for flights table...")
            cursor.execute("ALTER TABLE bts.flights SET TIFLASH REPLICA 1;")
            print("TiFlash replica set successfully")
            
    except Error as e:
        print(f"Error setting TiFlash replica: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def verify_csv_files():
    """
    Verify that the required CSV files exist in the expected location.
    """
    # Get the script's directory and find the project root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)  # Go up one level from load/ to project root
    csv_dir = os.path.join(project_root, "csv")
    
    required_files = ["bts.airlines.csv", "bts.airports.csv", "bts.flights.csv"]
    
    if not os.path.exists(csv_dir):
        print(f"Error: CSV directory '{csv_dir}' not found.")
        print("Please run 'python3 load/get_data.py' first to download the data.")
        sys.exit(1)
    
    missing_files = []
    for file in required_files:
        file_path = os.path.join(csv_dir, file)
        if not os.path.exists(file_path):
            missing_files.append(file)
    
    if missing_files:
        print(f"Error: Missing required CSV files: {missing_files}")
        print(f"Please ensure all CSV files are downloaded to the {csv_dir} directory.")
        sys.exit(1)
    
    print("All required CSV files found.")

def run_tidb_lightning():
    """
    Execute TiDB Lightning to load the data using existing configuration.
    """
    try:
        # Get the script's directory and project root
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        config_path = os.path.join(script_dir, "tidb-lightning.toml")
        
        # Check if configuration file exists
        if not os.path.exists(config_path):
            print(f"Error: {config_path} configuration file not found")
            sys.exit(1)
        
        print(f"Starting TiDB Lightning data import using {config_path}...")
        print("This may take several minutes. Progress will be shown below:")
        
        # Change to project root directory before running lightning
        original_cwd = os.getcwd()
        os.chdir(project_root)
        
        try:
            # Run TiDB Lightning with live output
            process = subprocess.Popen(
                ["tiup", "tidb-lightning", "-config", config_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            
            # Print output in real-time
            for line in process.stdout:
                print(line.rstrip())
            
            # Wait for process to complete and get return code
            return_code = process.wait()
            
            if return_code == 0:
                print("TiDB Lightning completed successfully!")
            else:
                print(f"TiDB Lightning failed with return code: {return_code}")
                sys.exit(1)
                
        finally:
            # Restore original working directory
            os.chdir(original_cwd)
        
    except subprocess.CalledProcessError as e:
        print(f"Error running TiDB Lightning: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("Error: tiup command not found. Please install TiUP first.")
        print("Run: curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh")
        sys.exit(1)

def main():
    """
    Main function to orchestrate the TiDB data loading process.
    """
    print("Starting TiDB data loading process...")
    
    # Verify CSV files exist
    verify_csv_files()
    
    # Wait for TiDB to be ready
    if not wait_for_tidb_connection():
        print("Error: Could not connect to TiDB. Please ensure TiDB is running.")
        sys.exit(1)
    
    # Run TiDB Lightning using existing configuration
    run_tidb_lightning()
    
    # Set TiFlash replica
    set_tiflash_replica()
    
    print("TiDB data loading completed successfully!")

if __name__ == "__main__":
    main()
