#!/usr/bin/env python3
import os
import subprocess
import sys
import time

def run_docker_command(command, description):
    """
    Execute a docker command and handle errors.
    """
    try:
        print(f"{description}...")
        result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True)
        print(f"✓ {description} completed successfully")
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Error during {description}")
        print(f"Command: {command}")
        print(f"Error: {e.stderr}")
        return False

def verify_csv_files():
    """
    Verify that the required CSV files exist in the expected location.
    """
    # Get the script's directory and find the project root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
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
    return csv_dir

def verify_schema_file():
    """
    Verify that the schema file exists.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    schema_path = os.path.join(project_root, "schemas", "columnstore.sql")
    
    if not os.path.exists(schema_path):
        print(f"Error: Schema file '{schema_path}' not found.")
        sys.exit(1)
    
    return schema_path

def load_columnstore_data():
    """
    Load data into MariaDB ColumnStore using Docker commands.
    """
    print("Starting MariaDB ColumnStore data loading process...")
    
    # Verify prerequisites
    csv_dir = verify_csv_files()
    schema_path = verify_schema_file()
    
    # Get project root for relative paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # Change to project root directory
    original_cwd = os.getcwd()
    os.chdir(project_root)
    
    try:
        # Step 1: Provision MariaDB ColumnStore
        if not run_docker_command(
            'docker exec -it mcs1 sh -c "provision mcs1"',
            "Provisioning MariaDB ColumnStore"
        ):
            sys.exit(1)
        
        # Step 2: Copy schema file to container
        if not run_docker_command(
            'docker cp schemas/columnstore.sql mcs1:/tmp',
            "Copying schema file to container"
        ):
            sys.exit(1)
        
        # Step 3: Copy CSV files to container
        csv_files = ["bts.airlines.csv", "bts.airports.csv", "bts.flights.csv"]
        for csv_file in csv_files:
            if not run_docker_command(
                f'docker cp csv/{csv_file} mcs1:/tmp',
                f"Copying {csv_file} to container"
            ):
                sys.exit(1)
        
        # Step 4: Create database schema
        if not run_docker_command(
            "docker exec mcs1 sh -c 'mariadb -P 3306 -h 127.0.0.1 -u admin -pC0lumnStore! < /tmp/columnstore.sql'",
            "Creating database schema"
        ):
            sys.exit(1)
        
        # Step 5: Import data using cpimport (fixed quoting)
        tables = ["airlines", "airports", "flights"]
        for i, table in enumerate(tables):
            csv_file = csv_files[i]
            # Use double quotes for the outer shell and escape inner quotes properly
            if not run_docker_command(
                f'docker exec mcs1 sh -c "cpimport -s \\",\\" -E \'\\\"\' bts {table} /tmp/{csv_file}"',
                f"Importing data into {table} table"
            ):
                sys.exit(1)
        
        print("✓ All data loaded successfully into MariaDB ColumnStore!")
        
    except KeyboardInterrupt:
        print("\n✗ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        sys.exit(1)
    finally:
        # Restore original working directory
        os.chdir(original_cwd)

def run_cpimport(table, csv_file):
    """
    Run cpimport command with proper escaping.
    """
    # Create a temporary script to avoid quoting issues
    script_content = f'cpimport -s "," -E \'"\' bts {table} /tmp/{csv_file}'
    
    # Write script to container
    with open('/tmp/cpimport_script.sh', 'w') as f:
        f.write(script_content)
    
    # Copy script to container
    if not run_docker_command(
        'docker cp /tmp/cpimport_script.sh mcs1:/tmp/',
        f"Copying import script for {table}"
    ):
        return False
    
    # Make script executable and run it
    if not run_docker_command(
        'docker exec mcs1 chmod +x /tmp/cpimport_script.sh',
        f"Making script executable for {table}"
    ):
        return False
    
    return run_docker_command(
        'docker exec mcs1 /tmp/cpimport_script.sh',
        f"Importing data into {table} table"
    )

def main():
    """
    Main function to orchestrate the MariaDB ColumnStore data loading process.
    """
    start_time = time.time()
    
    load_columnstore_data()
    
    end_time = time.time()
    total_time = end_time - start_time
    print(f"\nMariaDB ColumnStore data loading completed in {total_time:.2f} seconds!")

if __name__ == "__main__":
    main()