#!/usr/bin/env python3
import os
import requests
import time
import subprocess

required_files = [
    "csv/bts.airlines.csv",
    "csv/bts.airports.csv",
    "csv/bts.flights.csv"
]

def ensure_data_files():
    if not os.path.isdir("csv") or not all(os.path.isfile(file) for file in required_files):
        print("The 'csv' folder or required files are missing. Creating the folder and downloading the files...")
        try:
            subprocess.run(["python3", "load/get_data.py", "csv"], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running 'get_data.py': {e}")
            exit(1)

ensure_data_files()

def stream_load(file_path, url, headers):
    start_time = time.time()
    with open(file_path, 'rb') as file:
        response = requests.put(url, headers=headers, data=file)
    end_time = time.time()

    if response.status_code != 200:
        print(f"Error loading {file_path} to {url}")
        print(f"Response: {response.status_code} - {response.text}")
    else:
        print(f"Successfully loaded {file_path} in {end_time - start_time:.2f} seconds.")

stream_load(
    "csv/bts.airlines.csv",
    "http://root:@127.0.0.1:8040/api/bts/airlines/_stream_load",
    {
        "column_separator": ",",
        "columns": "iata_code,airline",
        "Expect": "100-continue"
    }
)

stream_load(
    "csv/bts.airports.csv",
    "http://root:@127.0.0.1:8040/api/bts/airports/_stream_load",
    {
        "column_separator": ",",
        "columns": "iata_code,airport,city,state,country,latitude,longitude",
        "Expect": "100-continue"
    }
)

stream_load(
    "csv/bts.flights.csv",
    "http://root:@127.0.0.1:8040/api/bts/flights/_stream_load",
    {
        "column_separator": ",",
        "trim_double_quotes": "true",
        "columns": "year,month,day,day_of_week,fl_date,carrier,tail_num,fl_num,origin,dest,crs_dep_time,dep_time,dep_delay,taxi_out,wheels_off,wheels_on,taxi_in,crs_arr_time,arr_time,arr_delay,cancelled,cancellation_code,diverted,crs_elapsed_time,actual_elapsed_time,air_time,distance,carrier_delay,weather_delay,nas_delay,security_delay,late_aircraft_delay",
        "enclose": "\"",
        "Expect": "100-continue"
    }
)