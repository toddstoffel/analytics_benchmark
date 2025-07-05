#!/usr/bin/env python3
import os
import requests
import time
import mysql.connector
from mysql.connector import Error
import logging
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# MySQL credentials and connection details
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 9030
MYSQL_DB = "bts"

# Required files for loading data
REQUIRED_FILES = [
    f"csv/{MYSQL_DB}.airlines.csv",
    f"csv/{MYSQL_DB}.airports.csv",
    f"csv/{MYSQL_DB}.flights.csv"
]

# Base URL for stream load
BASE_URL = f"http://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:8040/api/{MYSQL_DB}"

# SQL commands for table creation
SQL_COMMANDS = """
DROP DATABASE IF EXISTS `{db}`;
CREATE DATABASE `{db}`;
USE `{db}`;

CREATE TABLE `airlines` (
  `iata_code` varchar(2) DEFAULT NULL,
  `airline` varchar(30) DEFAULT NULL
) DUPLICATE KEY(`iata_code`)
DISTRIBUTED BY HASH(`iata_code`) BUCKETS 10
PROPERTIES (
  "replication_num" = "1"
);

CREATE TABLE `airports` (
  `iata_code` varchar(3) DEFAULT NULL,
  `airport` varchar(80) DEFAULT NULL,
  `city` varchar(30) DEFAULT NULL,
  `state` varchar(2) DEFAULT NULL,
  `country` varchar(30) DEFAULT NULL,
  `latitude` decimal(11,4) DEFAULT NULL,
  `longitude` decimal(11,4) DEFAULT NULL
) DUPLICATE KEY(`iata_code`)
DISTRIBUTED BY HASH(`iata_code`) BUCKETS 10
PROPERTIES (
  "replication_num" = "1"
);

CREATE TABLE `flights` (
  `year` smallint(6) DEFAULT NULL,
  `month` tinyint(4) DEFAULT NULL,
  `day` tinyint(4) DEFAULT NULL,
  `day_of_week` tinyint(4) DEFAULT NULL,
  `fl_date` date DEFAULT NULL,
  `carrier` varchar(2) DEFAULT NULL,
  `tail_num` varchar(6) DEFAULT NULL,
  `fl_num` smallint(6) DEFAULT NULL,
  `origin` varchar(5) DEFAULT NULL,
  `dest` varchar(5) DEFAULT NULL,
  `crs_dep_time` varchar(4) DEFAULT NULL,
  `dep_time` varchar(4) DEFAULT NULL,
  `dep_delay` decimal(13,2) DEFAULT NULL,
  `taxi_out` decimal(13,2) DEFAULT NULL,
  `wheels_off` varchar(4) DEFAULT NULL,
  `wheels_on` varchar(4) DEFAULT NULL,
  `taxi_in` decimal(13,2) DEFAULT NULL,
  `crs_arr_time` varchar(4) DEFAULT NULL,
  `arr_time` varchar(4) DEFAULT NULL,
  `arr_delay` decimal(13,2) DEFAULT NULL,
  `cancelled` decimal(13,2) DEFAULT NULL,
  `cancellation_code` varchar(20) DEFAULT NULL,
  `diverted` decimal(13,2) DEFAULT NULL,
  `crs_elapsed_time` decimal(13,2) DEFAULT NULL,
  `actual_elapsed_time` decimal(13,2) DEFAULT NULL,
  `air_time` decimal(13,2) DEFAULT NULL,
  `distance` decimal(13,2) DEFAULT NULL,
  `carrier_delay` decimal(13,2) DEFAULT NULL,
  `weather_delay` decimal(13,2) DEFAULT NULL,
  `nas_delay` decimal(13,2) DEFAULT NULL,
  `security_delay` decimal(13,2) DEFAULT NULL,
  `late_aircraft_delay` decimal(13,2) DEFAULT NULL
) DUPLICATE KEY(`year`, `month`, `day`, `day_of_week`, `fl_date`, `carrier`, `tail_num`, `fl_num`)
DISTRIBUTED BY HASH(`fl_date`) BUCKETS 10
PROPERTIES (
  "replication_num" = "1"
);
""".format(db=MYSQL_DB)

def execute_sql():
    """Execute SQL commands to set up the database and tables."""
    logging.info("Executing SQL commands...")
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            port=MYSQL_PORT
        )
        cursor = connection.cursor()
        for command in SQL_COMMANDS.split(";"):
            if command.strip():
                cursor.execute(command)
        connection.commit()
        logging.info("SQL commands executed successfully.")
    except Error as e:
        logging.error(f"Error executing SQL commands: {e}")
        exit(1)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def ensure_data_files():
    """Ensure the required data files are present."""
    if not os.path.isdir("csv") or not all(os.path.isfile(file) for file in REQUIRED_FILES):
        logging.info("The 'csv' folder or required files are missing. Creating the folder and downloading the files...")
        try:
            subprocess.run(["python3", "load/get_data.py", "csv"], check=True)
        except subprocess.CalledProcessError as e:
            logging.error(f"Error running 'get_data.py': {e}")
            exit(1)

def stream_load(file_path, url, headers, retries=3):
    """Stream load a file to the specified URL."""
    for attempt in range(retries):
        try:
            start_time = time.time()
            with open(file_path, 'rb') as file:
                response = requests.put(url, headers=headers, data=file)
            end_time = time.time()

            if response.status_code == 200:
                logging.info(f"Successfully loaded {file_path} in {end_time - start_time:.2f} seconds.")
                return
            else:
                logging.warning(f"Attempt {attempt + 1} failed for {file_path}. Response: {response.status_code} - {response.text}")
        except requests.RequestException as e:
            logging.error(f"Error loading {file_path}: {e}")
        time.sleep(2)  # Wait before retrying
    logging.error(f"Failed to load {file_path} after {retries} attempts.")

# Main execution
if __name__ == "__main__":
    # Execute SQL commands before loading data
    execute_sql()

    # Ensure the required data files are present
    ensure_data_files()

    # Stream load files
    stream_load(
        f"csv/{MYSQL_DB}.airlines.csv",
        f"{BASE_URL}/airlines/_stream_load",
        {
            "column_separator": ",",
            "columns": "iata_code,airline",
            "Expect": "100-continue"
        }
    )

    stream_load(
        f"csv/{MYSQL_DB}.airports.csv",
        f"{BASE_URL}/airports/_stream_load",
        {
            "column_separator": ",",
            "columns": "iata_code,airport,city,state,country,latitude,longitude",
            "Expect": "100-continue"
        }
    )

    stream_load(
        f"csv/{MYSQL_DB}.flights.csv",
        f"{BASE_URL}/flights/_stream_load",
        {
            "column_separator": ",",
            "trim_double_quotes": "true",
            "columns": "year,month,day,day_of_week,fl_date,carrier,tail_num,fl_num,origin,dest,crs_dep_time,dep_time,dep_delay,taxi_out,wheels_off,wheels_on,taxi_in,crs_arr_time,arr_time,arr_delay,cancelled,cancellation_code,diverted,crs_elapsed_time,actual_elapsed_time,air_time,distance,carrier_delay,weather_delay,nas_delay,security_delay,late_aircraft_delay",
            "enclose": "\"",
            "Expect": "100-continue"
        }
    )