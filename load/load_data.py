#!/usr/bin/env python3

import os
import sys
import time
import logging
import subprocess
import argparse
import requests
import difflib
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

DEFAULT_PORTS = {
    'clickhouse': 9000,
    'tidb': 4000,
    'doris': 9030,
    'starrocks': 9030,
    'columnstore': 3306
}

REQUIRED_CSV_FILES = ["bts.airlines.csv", "bts.airports.csv", "bts.flights.csv"]

CONNECTION_TIMEOUT = 60
STARROCKS_TIMEOUT = 120  # StarRocks needs more time to start up
DORIS_TIMEOUT = 120  # Doris needs more time for backend nodes to initialize
MAX_RETRIES = 3
RETRY_DELAY = 2

class DatabaseChoiceAction(argparse.Action):
    """Custom action that provides suggestions for invalid database choices"""
    
    def __call__(self, parser, namespace, values, option_string=None):
        valid_choices = ['clickhouse', 'tidb', 'doris', 'starrocks', 'columnstore']
        
        if values not in valid_choices:
            # Find closest matches
            close_matches = difflib.get_close_matches(values, valid_choices, n=3, cutoff=0.6)
            error_msg = f"invalid choice: '{values}' (choose from {', '.join(valid_choices)})"
            
            if close_matches:
                error_msg += f"\n\nDid you mean: {', '.join(close_matches)}?"
            
            parser.error(f"argument --database: {error_msg}")
        
        setattr(namespace, self.dest, values)

@dataclass
class DatabaseConfig:
    host: str = '127.0.0.1'
    port: int = 0
    user: str = 'root'
    password: str = ''
    database: str = 'bts'

def setup_logging(level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

def verify_csv_files() -> Path:
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    csv_dir = project_root / "csv"
    
    if not csv_dir.exists():
        raise FileNotFoundError(
            f"CSV directory '{csv_dir}' not found. "
            "Please run 'python3 load/get_data.py' first to download the data."
        )
    
    missing_files = [
        file for file in REQUIRED_CSV_FILES 
        if not (csv_dir / file).exists()
    ]
    
    if missing_files:
        raise FileNotFoundError(f"Missing required CSV files: {missing_files}")
    
    logging.getLogger(__name__).info("All required CSV files found.")
    return csv_dir

def run_docker_command(command: str, description: str) -> bool:
    logger = logging.getLogger(__name__)
    try:
        logger.info(f"{description}...")
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True, 
            check=True,
            timeout=300
        )
        logger.info(f"✓ {description} completed successfully")
        if result.stdout:
            logger.info(result.stdout)
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        logger.error(f"✗ Error during {description}")
        logger.error(f"Command: {command}")
        if hasattr(e, 'stderr') and e.stderr:
            logger.error(f"Error: {e.stderr}")
        return False

class DatabaseLoader(ABC):
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def create_database_and_tables(self) -> None:
        pass
    
    @abstractmethod
    def load_data(self, csv_dir: Path) -> None:
        pass
    
    def test_connection(self) -> bool:
        return True

class ClickHouseLoader(DatabaseLoader):
    
    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        
    @contextmanager
    def _get_client(self):
        try:
            from clickhouse_driver import Client
            client = Client(host=self.config.host, port=self.config.port)
            yield client
        except ImportError:
            self.logger.warning("clickhouse_driver not available, using CLI")
            yield None
        except Exception as e:
            self.logger.error(f"Error connecting to ClickHouse: {e}")
            yield None
        finally:
            if 'client' in locals():
                try:
                    client.disconnect()
                except:
                    pass
    
    def _execute_sql_command(self, sql_command: str) -> bool:
        try:
            cmd = [
                'clickhouse', 'client',
                '--host', self.config.host,
                '--port', str(self.config.port),
                '--query', sql_command
            ]
            subprocess.run(cmd, capture_output=True, text=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            with self._get_client() as client:
                if client:
                    try:
                        client.execute(sql_command)
                        return True
                    except Exception as e:
                        self.logger.error(f"Error executing SQL: {e}")
                        return False
                return False
    
    def create_database_and_tables(self) -> None:
        self.logger.info("Creating ClickHouse database and tables...")
        
        sql_commands = [
            f"DROP DATABASE IF EXISTS {self.config.database}",
            f"CREATE DATABASE {self.config.database}",
            f"""CREATE TABLE {self.config.database}.airlines (
                `iata_code` Nullable(String),
                `airline` Nullable(String)
            ) ENGINE = MergeTree()
            ORDER BY tuple()""",
            f"""CREATE TABLE {self.config.database}.airports (
                `iata_code` Nullable(String),
                `airport` Nullable(String),
                `city` Nullable(String),
                `state` Nullable(String),
                `country` Nullable(String),
                `latitude` Nullable(Decimal(11,4)),
                `longitude` Nullable(Decimal(11,4))
            ) ENGINE = MergeTree()
            ORDER BY tuple()""",
            f"""CREATE TABLE {self.config.database}.flights (
                `year` Nullable(Int16),
                `month` Nullable(Int8),
                `day` Nullable(Int8),
                `day_of_week` Nullable(Int8),
                `fl_date` Nullable(Date),
                `carrier` Nullable(String),
                `tail_num` Nullable(String),
                `fl_num` Nullable(Int16),
                `origin` Nullable(String),
                `dest` Nullable(String),
                `crs_dep_time` Nullable(String),
                `dep_time` Nullable(String),
                `dep_delay` Nullable(Decimal(13,2)),
                `taxi_out` Nullable(Decimal(13,2)),
                `wheels_off` Nullable(String),
                `wheels_on` Nullable(String),
                `taxi_in` Nullable(Decimal(13,2)),
                `crs_arr_time` Nullable(String),
                `arr_time` Nullable(String),
                `arr_delay` Nullable(Decimal(13,2)),
                `cancelled` Nullable(Decimal(13,2)),
                `cancellation_code` Nullable(String),
                `diverted` Nullable(Decimal(13,2)),
                `crs_elapsed_time` Nullable(Decimal(13,2)),
                `actual_elapsed_time` Nullable(Decimal(13,2)),
                `air_time` Nullable(Decimal(13,2)),
                `distance` Nullable(Decimal(13,2)),
                `carrier_delay` Nullable(Decimal(13,2)),
                `weather_delay` Nullable(Decimal(13,2)),
                `nas_delay` Nullable(Decimal(13,2)),
                `security_delay` Nullable(Decimal(13,2)),
                `late_aircraft_delay` Nullable(Decimal(13,2))
            ) ENGINE = MergeTree()
            ORDER BY tuple()"""
        ]
        
        for sql_command in sql_commands:
            if not self._execute_sql_command(sql_command):
                raise RuntimeError("Failed to create database and tables")
        
        self.logger.info("✅ Database and tables created successfully")
    
    def load_data(self, csv_dir: Path) -> None:
        self.logger.info("Starting data loading with ClickHouse...")
        
        file_table_mapping = {
            f"{self.config.database}.airlines.csv": "airlines",
            f"{self.config.database}.airports.csv": "airports", 
            f"{self.config.database}.flights.csv": "flights"
        }
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for csv_file, table_name in file_table_mapping.items():
                csv_file_path = csv_dir / csv_file
                if csv_file_path.exists():
                    future = executor.submit(self._load_file, csv_file_path, table_name)
                    futures.append(future)
                else:
                    self.logger.warning(f"{csv_file} not found, skipping...")
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error in concurrent loading: {e}")
                    raise
        
        self.logger.info("✅ All data loaded successfully into ClickHouse!")
    
    def _load_file(self, csv_file_path: Path, table_name: str) -> None:
        self.logger.info(f"Loading {csv_file_path.name} into table {table_name}...")
        
        start_time = time.time()
        
        cmd = [
            'clickhouse', 'client',
            '--host', self.config.host,
            '--port', str(self.config.port),
            '--max_memory_usage', '20000000000',
            '--max_threads', '16',
            '--max_insert_threads', '8',
            '--max_insert_block_size', '1000000',
            '--min_insert_block_size_rows', '100000',
            '--min_insert_block_size_bytes', '268435456',
            '--async_insert', '1',
            '--wait_for_async_insert', '0',
            '--async_insert_max_data_size', '1000000000',
            '--async_insert_busy_timeout_ms', '1000',
            '--query', f"INSERT INTO {self.config.database}.{table_name} FORMAT CSV",
            '--input_format_csv_empty_as_default', '1',
            '--input_format_csv_skip_first_lines', '0'
        ]
        
        try:
            with open(csv_file_path, 'rb') as f:
                subprocess.run(cmd, stdin=f, capture_output=True, check=True)
            
            total_time = time.time() - start_time
            
            count_cmd = [
                'clickhouse', 'client', 
                '--host', self.config.host, 
                '--port', str(self.config.port),
                '--query', f"SELECT COUNT(*) FROM {self.config.database}.{table_name}"
            ]
            result = subprocess.run(count_cmd, capture_output=True, text=True, check=True)
            row_count = int(result.stdout.strip())
            avg_rate = row_count / total_time if total_time > 0 else 0
            
            self.logger.info(f"✅ Successfully loaded {row_count:,} rows in {total_time:.1f}s "
                           f"(avg {avg_rate:,.0f} rows/sec)")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error loading {csv_file_path}: {e}")
            raise

    def _preprocess_csv_for_quotes(self, csv_file_path: Path) -> Path:
        """Preprocess CSV to handle optionally quoted fields properly"""
        import csv
        import tempfile
        
        self.logger.info(f"Preprocessing {csv_file_path.name} to handle quoted fields...")
        
        # Create a temporary file for the processed CSV
        temp_dir = csv_file_path.parent / "temp"
        temp_dir.mkdir(exist_ok=True)
        temp_file = temp_dir / f"processed_{csv_file_path.name}"
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as infile, \
                 open(temp_file, 'w', encoding='utf-8', newline='') as outfile:
                
                csv_reader = csv.reader(infile, quoting=csv.QUOTE_MINIMAL)
                csv_writer = csv.writer(outfile, quoting=csv.QUOTE_NONE, escapechar='\\')
                
                for row in csv_reader:
                    # Process each field to handle quotes and empty values
                    processed_row = []
                    for field in row:
                        # Convert empty string to NULL for StarRocks
                        if field == '':
                            processed_row.append('\\N')
                        else:
                            processed_row.append(field)
                    csv_writer.writerow(processed_row)
            
            self.logger.info(f"✅ Preprocessing completed: {temp_file}")
            return temp_file
            
        except Exception as e:
            self.logger.error(f"Error preprocessing CSV: {e}")
            raise

    def _cleanup_temp_files(self, csv_file_path: Path) -> None:
        """Clean up temporary processed files"""
        temp_dir = csv_file_path.parent / "temp"
        if temp_dir.exists():
            import shutil
            shutil.rmtree(temp_dir)
            self.logger.info("Cleaned up temporary files")

class TiDBLoader(DatabaseLoader):
    
    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
    
    @contextmanager
    def _get_connection(self):
        import mysql.connector
        from mysql.connector import Error
        
        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                autocommit=True,
                connect_timeout=CONNECTION_TIMEOUT
            )
            yield connection
        except Error as e:
            self.logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()
    
    def test_connection(self) -> bool:
        try:
            with self._get_connection() as connection:
                return connection.is_connected()
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def wait_for_connection(self, timeout: int = CONNECTION_TIMEOUT) -> bool:
        self.logger.info("Waiting for TiDB to be ready...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if self.test_connection():
                self.logger.info("TiDB is ready!")
                return True
            time.sleep(RETRY_DELAY)
        
        self.logger.error(f"Timeout waiting for TiDB connection after {timeout} seconds")
        return False
    
    def create_database_and_tables(self) -> None:
        self.logger.info("Creating TiDB database and tables...")
        
        sql_commands = [
            f"DROP DATABASE IF EXISTS `{self.config.database}`",
            f"CREATE DATABASE `{self.config.database}`",
            f"USE `{self.config.database}`",
            """CREATE TABLE `airlines` (
                `iata_code` varchar(2) DEFAULT NULL,
                `airline` varchar(30) DEFAULT NULL
            )""",
            """CREATE TABLE `airports` (
                `iata_code` varchar(3) DEFAULT NULL,
                `airport` varchar(80) DEFAULT NULL,
                `city` varchar(30) DEFAULT NULL,
                `state` varchar(2) DEFAULT NULL,
                `country` varchar(30) DEFAULT NULL,
                `latitude` decimal(11,4) DEFAULT NULL,
                `longitude` decimal(11,4) DEFAULT NULL
            )""",
            """CREATE TABLE `flights` (
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
            )"""
        ]
        
        with self._get_connection() as connection:
            cursor = connection.cursor()
            try:
                for sql_command in sql_commands:
                    cursor.execute(sql_command)
                self.logger.info("✅ Database and tables created successfully")
            except Exception as e:
                self.logger.error(f"❌ Error creating database and tables: {e}")
                raise
            finally:
                cursor.close()
    
    def load_data(self, csv_dir: Path) -> None:
        self.logger.info("Starting data loading with TiDB Lightning...")
        
        config_path = Path(__file__).parent / "tidb-lightning.toml"
        
        if not config_path.exists():
            self.logger.error(f"{config_path} configuration file not found")
            raise FileNotFoundError(f"TiDB Lightning config not found: {config_path}")
        
        self.logger.info("Starting TiDB Lightning data import...")
        
        project_root = Path(__file__).parent.parent
        original_cwd = Path.cwd()
        
        try:
            os.chdir(project_root)
            
            start_time = time.time()
            process = subprocess.Popen(
                ["tiup", "tidb-lightning", "-config", str(config_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            
            for line in process.stdout:
                self.logger.info(line.rstrip())
            
            return_code = process.wait()
            
            if return_code == 0:
                total_time = time.time() - start_time
                self.logger.info(f"✅ TiDB Lightning completed successfully in {total_time:.1f}s")
                
                # Get row counts for each table
                try:
                    with self._get_connection() as connection:
                        cursor = connection.cursor()
                        tables = ["airlines", "airports", "flights"]
                        for table in tables:
                            cursor.execute(f"SELECT COUNT(*) FROM {self.config.database}.{table}")
                            row_count = cursor.fetchone()[0]
                            self.logger.info(f"✅ Successfully loaded {row_count:,} rows into {table}")
                        cursor.close()
                except Exception as e:
                    self.logger.warning(f"Could not get row counts: {e}")
                
                self.logger.info("✅ All data loaded successfully into TiDB!")
            else:
                self.logger.error(f"TiDB Lightning failed with return code: {return_code}")
                raise RuntimeError(f"TiDB Lightning failed with code {return_code}")
                
        finally:
            os.chdir(original_cwd)
    
    def set_tiflash_replica(self) -> None:
        tables = ['airlines', 'airports', 'flights']
        
        try:
            with self._get_connection() as connection:
                cursor = connection.cursor()
                
                for table in tables:
                    self.logger.info(f"Setting TiFlash replica for {table} table...")
                    cursor.execute(f"ALTER TABLE {self.config.database}.{table} SET TIFLASH REPLICA 1;")
                    self.logger.info(f"✅ TiFlash replica set successfully for {table}")
                
                cursor.close()
        except Exception as e:
            self.logger.error(f"Error setting TiFlash replica: {e}")
            raise

class DorisLoader(DatabaseLoader):
    
    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self.base_url = f"http://{config.user}:{config.password}@{config.host}:8040/api/{config.database}"
    
    @contextmanager
    def _get_connection(self):
        import mysql.connector
        from mysql.connector import Error
        
        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.config.host,
                user=self.config.user,
                password=self.config.password,
                port=self.config.port,
                connection_timeout=CONNECTION_TIMEOUT,
                autocommit=True
            )
            yield connection
        except Error as e:
            self.logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()

    def test_connection(self) -> bool:
        try:
            with self._get_connection() as connection:
                if connection.is_connected():
                    cursor = connection.cursor()
                    try:
                        # Check basic connectivity
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                        
                        # Check if backend nodes are available
                        cursor.execute("SHOW BACKENDS")
                        backends = cursor.fetchall()
                        
                        if not backends:
                            self.logger.debug("No backend nodes found")
                            return False
                            
                        # Check if at least one backend is alive
                        alive_backends = 0
                        for backend in backends:
                            # Backend status is typically in the 'Alive' column
                            if len(backend) > 9 and backend[9] == 'true':  # Alive column
                                alive_backends += 1
                        
                        if alive_backends == 0:
                            self.logger.debug("No alive backend nodes found")
                            return False
                            
                        self.logger.debug(f"Found {alive_backends} alive backend nodes")
                        return True
                    except Exception as e:
                        self.logger.debug(f"Backend check failed: {e}")
                        return False
                    finally:
                        cursor.close()
        except Exception as e:
            self.logger.debug(f"Connection failed: {e}")
        return False
    
    def wait_for_connection(self, timeout: int = None) -> bool:
        """Wait for Doris to be ready"""
        if timeout is None:
            timeout = DORIS_TIMEOUT  # Use Doris-specific timeout
            
        self.logger.info("Waiting for DORIS to be ready (frontend + backend nodes)...")
        start_time = time.time()
        last_log_time = start_time
        
        while time.time() - start_time < timeout:
            try:
                if self.test_connection():
                    self.logger.info("✅ DORIS is ready!")
                    return True
            except Exception as e:
                self.logger.debug(f"Connection attempt failed: {e}")
            
            current_time = time.time()
            if current_time - last_log_time >= 20:
                elapsed = int(current_time - start_time)
                self.logger.info(f"Still waiting for backend nodes... ({elapsed}s)")
                last_log_time = current_time
            
            time.sleep(5)
        
        self.logger.error(f"❌ Timeout after {timeout}s. Please check if DORIS is properly started.")
        return False

    def create_database_and_tables(self) -> None:
        self.logger.info("Creating Doris database and tables...")
        
        # Doris-specific SQL
        sql_commands = [
            f"DROP DATABASE IF EXISTS `{self.config.database}`",
            f"CREATE DATABASE `{self.config.database}`",
            f"USE `{self.config.database}`",
            """CREATE TABLE `airlines` (
              `iata_code` varchar(2),
              `airline` varchar(30)
            ) DUPLICATE KEY(`iata_code`)
            DISTRIBUTED BY HASH(`iata_code`) BUCKETS AUTO
            PROPERTIES (
              "replication_num" = "1"
            )""",
            """CREATE TABLE `airports` (
              `iata_code` varchar(3),
              `airport` varchar(80),
              `city` varchar(30),
              `state` varchar(2),
              `country` varchar(30),
              `latitude` decimal(11,4),
              `longitude` decimal(11,4)
            ) DUPLICATE KEY(`iata_code`)
            DISTRIBUTED BY HASH(`iata_code`) BUCKETS AUTO
            PROPERTIES (
              "replication_num" = "1"
            )""",
            """CREATE TABLE `flights` (
              `year` smallint(6),
              `month` tinyint(4),
              `day` tinyint(4),
              `day_of_week` tinyint(4),
              `fl_date` date,
              `carrier` varchar(2),
              `tail_num` varchar(6),
              `fl_num` smallint(6),
              `origin` varchar(5),
              `dest` varchar(5),
              `crs_dep_time` varchar(4),
              `dep_time` varchar(4),
              `dep_delay` decimal(13,2),
              `taxi_out` decimal(13,2),
              `wheels_off` varchar(4),
              `wheels_on` varchar(4),
              `taxi_in` decimal(13,2),
              `crs_arr_time` varchar(4),
              `arr_time` varchar(4),
              `arr_delay` decimal(13,2),
              `cancelled` decimal(13,2),
              `cancellation_code` varchar(20),
              `diverted` decimal(13,2),
              `crs_elapsed_time` decimal(13,2),
              `actual_elapsed_time` decimal(13,2),
              `air_time` decimal(13,2),
              `distance` decimal(13,2),
              `carrier_delay` decimal(13,2),
              `weather_delay` decimal(13,2),
              `nas_delay` decimal(13,2),
              `security_delay` decimal(13,2),
              `late_aircraft_delay` decimal(13,2)
            ) DUPLICATE KEY(`year`, `month`, `day`, `day_of_week`)
            DISTRIBUTED BY HASH(`fl_date`) BUCKETS AUTO
            PROPERTIES (
              "replication_num" = "1"
            )"""
        ]
        
        try:
            with self._get_connection() as connection:
                cursor = connection.cursor()
                try:
                    for i, command in enumerate(sql_commands, 1):
                        if command.strip():
                            self.logger.info(f"Executing command {i}/{len(sql_commands)}")
                            cursor.execute(command)
                            time.sleep(0.5)
                    self.logger.info("✅ Database and tables created successfully")
                except Exception as e:
                    self.logger.error(f"❌ Error creating database and tables: {e}")
                    raise
                finally:
                    cursor.close()
        except Exception as e:
            self.logger.error(f"Error executing SQL commands: {e}")
            raise

    def load_data(self, csv_dir: Path) -> None:
        self.logger.info("Starting data loading with Doris Stream Load...")
        
        file_table_mapping = {
            "bts.airlines.csv": "airlines",
            "bts.airports.csv": "airports", 
            "bts.flights.csv": "flights"
        }
        
        for csv_file, table_name in file_table_mapping.items():
            csv_file_path = csv_dir / csv_file
            if csv_file_path.exists():
                self._load_file_via_stream_load(csv_file_path, table_name)
            else:
                self.logger.warning(f"{csv_file} not found, skipping...")
        
        self.logger.info("✅ All data loaded successfully into Doris!")

    def _load_file_via_stream_load(self, csv_file_path: Path, table_name: str) -> None:
        """Load a single CSV file using Doris Stream Load"""
        self.logger.info(f"Loading {csv_file_path.name} into table {table_name}...")
        
        start_time = time.time()
        url = f"http://{self.config.host}:8040/api/{self.config.database}/{table_name}/_stream_load"
        
        # Doris-optimized headers
        headers = {
            'label': f'doris_load_{table_name}_{int(start_time)}',
            'format': 'csv',
            'column_separator': ',',
            'timeout': '600',
            'max_filter_ratio': '0.1',
            'strict_mode': 'false',
            'Expect': '100-continue',
            'Content-Type': 'text/plain'
        }
        
        # Add Doris-specific CSV handling
        if table_name == 'flights':
            headers.update({
                'skip_header': '0',
                'enclose': '"',
                'escape': '\\',
                'trim_double_quotes': 'true',
                'max_filter_ratio': '0.2'
            })
        
        try:
            with open(csv_file_path, 'rb') as f:
                response = requests.put(
                    url,
                    headers=headers,
                    data=f,
                    auth=(self.config.user, self.config.password),
                    timeout=600
                )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('Status') == 'Success':
                    total_time = time.time() - start_time
                    
                    try:
                        with self._get_connection() as connection:
                            cursor = connection.cursor()
                            cursor.execute(f"SELECT COUNT(*) FROM {self.config.database}.{table_name}")
                            row_count = cursor.fetchone()[0]
                            cursor.close()
                            
                        avg_rate = row_count / total_time if total_time > 0 else 0
                        self.logger.info(f"✅ Successfully loaded {row_count:,} rows in {total_time:.1f}s "
                                       f"(avg {avg_rate:,.0f} rows/sec)")
                    except Exception as e:
                        self.logger.info(f"✅ File loaded successfully in {total_time:.1f}s")
                        self.logger.warning(f"Could not get row count: {e}")
                else:
                    self.logger.error(f"Stream Load failed: {result}")
                    raise RuntimeError(f"Stream Load failed for {table_name}")
            else:
                self.logger.error(f"HTTP error {response.status_code}: {response.text}")
                raise RuntimeError(f"HTTP error during Stream Load for {table_name}")
                
        except Exception as e:
            self.logger.error(f"Error loading {csv_file_path}: {e}")
            raise


class StarRocksLoader(DatabaseLoader):
    
    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self.base_url = f"http://{config.user}:{config.password}@{config.host}:8040/api/{config.database}"
    
    @contextmanager
    def _get_connection(self):
        import mysql.connector
        from mysql.connector import Error
        
        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.config.host,
                user=self.config.user,
                password=self.config.password,
                port=self.config.port,
                connection_timeout=CONNECTION_TIMEOUT,
                autocommit=True
            )
            yield connection
        except Error as e:
            self.logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()

    def test_connection(self) -> bool:
        try:
            with self._get_connection() as connection:
                if connection.is_connected():
                    cursor = connection.cursor()
                    try:
                        # Check basic connectivity
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                        
                        # Check if backend nodes are available
                        cursor.execute("SHOW BACKENDS")
                        backends = cursor.fetchall()
                        
                        if not backends:
                            self.logger.debug("No backend nodes found")
                            return False
                            
                        # Check if at least one backend is alive
                        alive_backends = 0
                        for backend in backends:
                            # Backend status is typically in the 'Alive' column (index 8)
                            if len(backend) > 8 and str(backend[8]).lower() == 'true':
                                alive_backends += 1
                        
                        if alive_backends == 0:
                            self.logger.debug("No alive backend nodes found")
                            return False
                            
                        self.logger.debug(f"Found {alive_backends} alive backend nodes")
                        return True
                    except Exception as e:
                        self.logger.debug(f"Backend check failed: {e}")
                        return False
                    finally:
                        cursor.close()
        except Exception as e:
            self.logger.debug(f"Connection failed: {e}")
        return False
    
    def wait_for_connection(self, timeout: int = None) -> bool:
        """Wait for StarRocks to be ready"""
        if timeout is None:
            timeout = 120  # StarRocks needs time for backend nodes to initialize
            
        self.logger.info("Waiting for STARROCKS to be ready (frontend + backend nodes)...")
        start_time = time.time()
        last_log_time = start_time
        
        while time.time() - start_time < timeout:
            try:
                if self.test_connection():
                    self.logger.info("✅ STARROCKS is ready!")
                    return True
            except Exception as e:
                self.logger.debug(f"Connection attempt failed: {e}")
            
            current_time = time.time()
            if current_time - last_log_time >= 20:
                elapsed = int(current_time - start_time)
                self.logger.info(f"Still waiting for backend nodes... ({elapsed}s)")
                last_log_time = current_time
            
            time.sleep(5)
        
        self.logger.error(f"❌ Timeout after {timeout}s. Please check if STARROCKS is properly started.")
        return False

    def create_database_and_tables(self) -> None:
        self.logger.info("Creating StarRocks database and tables...")
        
        # StarRocks-specific SQL
        sql_commands = [
            f"DROP DATABASE IF EXISTS `{self.config.database}`",
            f"CREATE DATABASE `{self.config.database}`",
            f"USE `{self.config.database}`",
            """CREATE TABLE `airlines` (
              `iata_code` varchar(2),
              `airline` varchar(30)
            ) DUPLICATE KEY(`iata_code`)
            DISTRIBUTED BY HASH(`iata_code`)
            PROPERTIES (
              "replication_num" = "1"
            )""",
            """CREATE TABLE `airports` (
              `iata_code` varchar(3),
              `airport` varchar(80),
              `city` varchar(30),
              `state` varchar(2),
              `country` varchar(30),
              `latitude` decimal(11,4),
              `longitude` decimal(11,4)
            ) DUPLICATE KEY(`iata_code`)
            DISTRIBUTED BY HASH(`iata_code`)
            PROPERTIES (
              "replication_num" = "1"
            )""",
            """CREATE TABLE `flights` (
              `year` smallint(6),
              `month` tinyint(4),
              `day` tinyint(4),
              `day_of_week` tinyint(4),
              `fl_date` date,
              `carrier` varchar(2),
              `tail_num` varchar(6),
              `fl_num` smallint(6),
              `origin` varchar(5),
              `dest` varchar(5),
              `crs_dep_time` varchar(4),
              `dep_time` varchar(4),
              `dep_delay` decimal(13,2),
              `taxi_out` decimal(13,2),
              `wheels_off` varchar(4),
              `wheels_on` varchar(4),
              `taxi_in` decimal(13,2),
              `crs_arr_time` varchar(4),
              `arr_time` varchar(4),
              `arr_delay` decimal(13,2),
              `cancelled` decimal(13,2),
              `cancellation_code` varchar(20),
              `diverted` decimal(13,2),
              `crs_elapsed_time` decimal(13,2),
              `actual_elapsed_time` decimal(13,2),
              `air_time` decimal(13,2),
              `distance` decimal(13,2),
              `carrier_delay` decimal(13,2),
              `weather_delay` decimal(13,2),
              `nas_delay` decimal(13,2),
              `security_delay` decimal(13,2),
              `late_aircraft_delay` decimal(13,2)
            ) DUPLICATE KEY(`year`, `month`, `day`, `day_of_week`)
            DISTRIBUTED BY HASH(`fl_date`)
            PROPERTIES (
              "replication_num" = "1"
            )"""
        ]
        
        try:
            with self._get_connection() as connection:
                cursor = connection.cursor()
                try:
                    for i, command in enumerate(sql_commands, 1):
                        if command.strip():
                            self.logger.info(f"Executing command {i}/{len(sql_commands)}")
                            cursor.execute(command)
                            time.sleep(0.5)
                    self.logger.info("✅ Database and tables created successfully")
                except Exception as e:
                    error_msg = str(e)
                    if "no available capacity" in error_msg.lower():
                        self.logger.error(f"❌ Error creating database and tables: {e}")
                        self.logger.error("💡 This error typically means StarRocks backend nodes are not fully initialized.")
                        self.logger.error("💡 Please wait longer for the cluster to be ready or restart the StarRocks container.")
                    else:
                        self.logger.error(f"❌ Error creating database and tables: {e}")
                    raise
                finally:
                    cursor.close()
        except Exception as e:
            self.logger.error(f"Error executing SQL commands: {e}")
            raise

    def load_data(self, csv_dir: Path) -> None:
        self.logger.info("Starting data loading with StarRocks Stream Load...")
        
        file_table_mapping = {
            "bts.airlines.csv": "airlines",
            "bts.airports.csv": "airports", 
            "bts.flights.csv": "flights"
        }
        
        for csv_file, table_name in file_table_mapping.items():
            csv_file_path = csv_dir / csv_file
            if csv_file_path.exists():
                self._load_file_via_stream_load(csv_file_path, table_name)
            else:
                self.logger.warning(f"{csv_file} not found, skipping...")
        
        self.logger.info("✅ All data loaded successfully into StarRocks!")

    def _load_file_via_stream_load(self, csv_file_path: Path, table_name: str) -> None:
        """Load a single CSV file using StarRocks Stream Load"""
        self.logger.info(f"Loading {csv_file_path.name} into table {table_name}...")
        
        start_time = time.time()
        url = f"http://{self.config.host}:8040/api/{self.config.database}/{table_name}/_stream_load"
        
        # StarRocks-optimized headers
        headers = {
            'label': f'starrocks_load_{table_name}_{int(start_time)}',
            'format': 'csv',
            'column_separator': ',',
            'timeout': '600',
            'max_filter_ratio': '0.1',
            'strict_mode': 'false',
            'Expect': '100-continue',
            'Content-Type': 'text/plain'
        }
        
        # Add StarRocks-specific CSV handling
        if table_name == 'flights':
            headers.update({
                'skip_header': '0',
                'enclose': '"',
                'escape': '\\',
                'trim_double_quotes': 'true',
                'max_filter_ratio': '0.2',
                'columns': 'year, month, day, day_of_week, fl_date, carrier, tail_num, fl_num, origin, dest, crs_dep_time, dep_time, dep_delay, taxi_out, wheels_off, wheels_on, taxi_in, crs_arr_time, arr_time, arr_delay, cancelled, cancellation_code, diverted, crs_elapsed_time, actual_elapsed_time, air_time, distance, carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay'
            })
        
        try:
            with open(csv_file_path, 'rb') as f:
                response = requests.put(
                    url,
                    headers=headers,
                    data=f,
                    auth=(self.config.user, self.config.password),
                    timeout=600
                )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('Status') == 'Success':
                    total_time = time.time() - start_time
                    
                    try:
                        with self._get_connection() as connection:
                            cursor = connection.cursor()
                            cursor.execute(f"SELECT COUNT(*) FROM {self.config.database}.{table_name}")
                            row_count = cursor.fetchone()[0]
                            cursor.close()
                            
                        avg_rate = row_count / total_time if total_time > 0 else 0
                        self.logger.info(f"✅ Successfully loaded {row_count:,} rows in {total_time:.1f}s "
                                       f"(avg {avg_rate:,.0f} rows/sec)")
                    except Exception as e:
                        self.logger.info(f"✅ File loaded successfully in {total_time:.1f}s")
                        self.logger.warning(f"Could not get row count: {e}")
                else:
                    self.logger.error(f"Stream Load failed: {result}")
                    raise RuntimeError(f"Stream Load failed for {table_name}")
            else:
                self.logger.error(f"HTTP error {response.status_code}: {response.text}")
                raise RuntimeError(f"HTTP error during Stream Load for {table_name}")
                
        except Exception as e:
            self.logger.error(f"Error loading {csv_file_path}: {e}")
            raise

class ColumnStoreLoader(DatabaseLoader):
    """
    MariaDB ColumnStore loader with optimized CSV loading via Docker volume mount.
    
    This loader uses a mounted CSV directory (configured in docker/columnstore.yml) 
    to avoid copying files to the container, resulting in faster data loading.
    """
    
    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        # ColumnStore uses admin/C0lumnStore! by default after provisioning
        # Override default credentials if they weren't explicitly set
        if config.user == 'root' and config.password == '':
            self.config.user = 'admin'
            self.config.password = 'C0lumnStore!'
    
    def test_connection(self) -> bool:
        try:
            import mysql.connector
            from mysql.connector import Error
            
            connection = mysql.connector.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                connection_timeout=10,
                charset='utf8mb4',
                collation='utf8mb4_general_ci'
            )
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            connection.close()
            return True
        except Exception as e:
            self.logger.debug(f"Connection test failed: {e}")
            return False
    
    def _ensure_columnstore_provisioned(self) -> bool:
        """Ensure ColumnStore is provisioned before attempting connections"""
        self.logger.info("Ensuring MariaDB ColumnStore is provisioned...")
        
        # Check if container exists and is running
        try:
            result = subprocess.run(
                'docker ps --filter "name=mcs1" --format "{{.Names}}"', 
                shell=True, capture_output=True, text=True, check=True
            )
            if 'mcs1' not in result.stdout:
                self.logger.error("MariaDB ColumnStore container 'mcs1' not found or not running")
                self.logger.error("Please start it with: docker compose -f docker/columnstore.yml up -d")
                return False
        except subprocess.CalledProcessError:
            self.logger.error("Could not check Docker container status")
            return False
        
        # Try to provision ColumnStore (this may fail if already provisioned, which is fine)
        self.logger.info("Provisioning MariaDB ColumnStore...")
        provision_result = subprocess.run(
            'docker exec mcs1 sh -c "provision mcs1" 2>/dev/null || true',
            shell=True, capture_output=True, text=True
        )
        if provision_result.returncode == 0:
            self.logger.info("✓ MariaDB ColumnStore provisioned successfully")
        else:
            self.logger.info("ℹ MariaDB ColumnStore likely already provisioned")
        
        return True
    
    def wait_for_connection(self, timeout: int = CONNECTION_TIMEOUT) -> bool:
        self.logger.info("Waiting for MariaDB ColumnStore to be ready...")
        
        # First ensure ColumnStore is provisioned
        if not self._ensure_columnstore_provisioned():
            return False
        
        start_time = time.time()
        last_progress = start_time
        
        while time.time() - start_time < timeout:
            if self.test_connection():
                elapsed = time.time() - start_time
                self.logger.info(f"✅ MariaDB ColumnStore is ready! ({elapsed:.1f}s)")
                return True
            
            # Show progress every 20 seconds to avoid spam
            current_time = time.time()
            if current_time - last_progress >= 20:
                elapsed = current_time - start_time
                remaining = timeout - elapsed
                self.logger.info(f"Still waiting for connection... ({elapsed:.0f}s elapsed, {remaining:.0f}s remaining)")
                last_progress = current_time
            
            time.sleep(2)
        
        self.logger.error(f"⏰ Timeout waiting for MariaDB ColumnStore connection after {timeout}s")
        return False
    
    def create_database_and_tables(self) -> None:
        self.logger.info("Creating MariaDB ColumnStore database and tables...")
        
        # Create database and tables using direct SQL
        sql_commands = [
            "DROP DATABASE IF EXISTS bts",
            "CREATE DATABASE bts",
            "USE bts",
            """CREATE TABLE airlines (
                iata_code varchar(2) DEFAULT NULL,
                airline varchar(30) DEFAULT NULL
            ) ENGINE=ColumnStore""",
            """CREATE TABLE airports (
                iata_code varchar(3) DEFAULT NULL,
                airport varchar(80) DEFAULT NULL,
                city varchar(30) DEFAULT NULL,
                state varchar(2) DEFAULT NULL,
                country varchar(30) DEFAULT NULL,
                latitude decimal(11,4) DEFAULT NULL,
                longitude decimal(11,4) DEFAULT NULL
            ) ENGINE=ColumnStore""",
            """CREATE TABLE flights (
                year smallint(6) DEFAULT NULL,
                month tinyint(4) DEFAULT NULL,
                day tinyint(4) DEFAULT NULL,
                day_of_week tinyint(4) DEFAULT NULL,
                fl_date date DEFAULT NULL,
                carrier varchar(2) DEFAULT NULL,
                tail_num varchar(6) DEFAULT NULL,
                fl_num smallint(6) DEFAULT NULL,
                origin varchar(5) DEFAULT NULL,
                dest varchar(5) DEFAULT NULL,
                crs_dep_time varchar(4) DEFAULT NULL,
                dep_time varchar(4) DEFAULT NULL,
                dep_delay decimal(13,2) DEFAULT NULL,
                taxi_out decimal(13,2) DEFAULT NULL,
                wheels_off varchar(4) DEFAULT NULL,
                wheels_on varchar(4) DEFAULT NULL,
                taxi_in decimal(13,2) DEFAULT NULL,
                crs_arr_time varchar(4) DEFAULT NULL,
                arr_time varchar(4) DEFAULT NULL,
                arr_delay decimal(13,2) DEFAULT NULL,
                cancelled decimal(13,2) DEFAULT NULL,
                cancellation_code varchar(20) DEFAULT NULL,
                diverted decimal(13,2) DEFAULT NULL,
                crs_elapsed_time decimal(13,2) DEFAULT NULL,
                actual_elapsed_time decimal(13,2) DEFAULT NULL,
                air_time decimal(13,2) DEFAULT NULL,
                distance decimal(13,2) DEFAULT NULL,
                carrier_delay decimal(13,2) DEFAULT NULL,
                weather_delay decimal(13,2) DEFAULT NULL,
                nas_delay decimal(13,2) DEFAULT NULL,
                security_delay decimal(13,2) DEFAULT NULL,
                late_aircraft_delay decimal(13,2) DEFAULT NULL
            ) ENGINE=ColumnStore"""
        ]
        
        try:
            import mysql.connector
            from mysql.connector import Error
            
            connection = mysql.connector.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                charset='utf8mb4',
                collation='utf8mb4_general_ci'
            )
            cursor = connection.cursor()
            
            for sql_command in sql_commands:
                self.logger.debug(f"Executing: {sql_command[:50]}...")
                cursor.execute(sql_command)
            
            connection.commit()
            cursor.close()
            connection.close()
            self.logger.info("✅ Database and tables created successfully")
            
        except Exception as e:
            self.logger.error(f"❌ Error creating database and tables: {e}")
            raise
    
    def load_data(self, csv_dir: Path) -> None:
        self.logger.info("Loading data into MariaDB ColumnStore...")
        
        # Check if container exists first
        try:
            result = subprocess.run(
                'docker ps --filter "name=mcs1" --format "{{.Names}}"', 
                shell=True, capture_output=True, text=True, check=True
            )
            if 'mcs1' not in result.stdout:
                raise RuntimeError("MariaDB ColumnStore container 'mcs1' not found or not running. "
                                 "Please start it with: docker compose -f docker/columnstore.yml up -d")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Could not check Docker container status: {e}")
        
        # Verify CSV files exist (no copying needed since they're mounted as a volume)
        csv_files = ["bts.airlines.csv", "bts.airports.csv", "bts.flights.csv"]
        for csv_file in csv_files:
            csv_path = csv_dir / csv_file
            if not csv_path.exists():
                raise FileNotFoundError(f"Required CSV file not found: {csv_path}")
        
        self.logger.info("Using mounted CSV directory for data import (faster than copying files)")
        
        # Import data using cpimport with mounted CSV directory
        tables = ["airlines", "airports", "flights"]
        for i, table in enumerate(tables):
            csv_file = csv_files[i]
            self._load_table(table, csv_file)
        
        self.logger.info("✅ All data loaded successfully into MariaDB ColumnStore!")
    
    def _load_table(self, table_name: str, csv_file: str) -> None:
        """Load data into a specific table and report detailed success metrics"""
        self.logger.info(f"Loading {csv_file} into table {table_name}...")
        
        start_time = time.time()
        
        # Use cpimport with mounted CSV directory (/var/lib/columnstore/csv)
        result = subprocess.run(
            f'docker exec mcs1 sh -c "cpimport -s \\",\\" -E \'\\\"\' bts {table_name} /var/lib/columnstore/csv/{csv_file}"',
            shell=True, capture_output=True, text=True
        )
        
        if result.returncode != 0:
            self.logger.warning(f"cpimport stderr: {result.stderr}")
            if "error" not in result.stderr.lower():
                self.logger.debug(f"cpimport completed with warnings for {table_name}")
            else:
                raise RuntimeError(f"Failed to import data into {table_name} table: {result.stderr}")
        
        total_time = time.time() - start_time
        
        # Get row count for success reporting
        try:
            import mysql.connector
            connection = mysql.connector.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                charset='utf8mb4',
                collation='utf8mb4_general_ci'
            )
            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            cursor.close()
            connection.close()
            
            avg_rate = row_count / total_time if total_time > 0 else 0
            
            self.logger.info(f"✅ Successfully loaded {row_count:,} rows in {total_time:.1f}s "
                           f"(avg {avg_rate:,.0f} rows/sec)")
            
        except Exception as e:
            # If we can't get row count, still report success with timing
            self.logger.info(f"✅ Successfully imported data into {table_name} in {total_time:.1f}s")
            self.logger.debug(f"Could not get row count for {table_name}: {e}")


def create_loader(database: str, config: DatabaseConfig) -> DatabaseLoader:
    loaders = {
        'clickhouse': ClickHouseLoader,
        'tidb': TiDBLoader,
        'doris': DorisLoader,
        'starrocks': StarRocksLoader,
        'columnstore': ColumnStoreLoader
    }
    
    if database not in loaders:
        raise ValueError(f"Unsupported database: {database}")
    
    return loaders[database](config)

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Load BTS data into specified database',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--database', action=DatabaseChoiceAction,
                       required=True, help='Database to load data into')
    parser.add_argument('--skip-schema', action='store_true', 
                       help='Skip database and table creation (assumes they already exist)')
    parser.add_argument('--host', default='127.0.0.1', 
                       help='Database host')
    parser.add_argument('--port', type=int, 
                       help='Database port (default varies by database)')
    parser.add_argument('--user', default='root', 
                       help='Database user')
    parser.add_argument('--password', default='', 
                       help='Database password')
    parser.add_argument('--database-name', default='bts',
                       help='Database name to create and use')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    parser.add_argument('--connection-timeout', type=int, default=60,
                       help='Timeout in seconds for database connection attempts')
    
    args = parser.parse_args()
    
    if args.port is None:
        args.port = DEFAULT_PORTS[args.database]
    
    log_level = getattr(logging, args.log_level.upper())
    logger = setup_logging(log_level)
    logger.info(f"🚀 Starting {args.database.upper()} data loading")
    
    try:
        csv_dir = verify_csv_files()
        
        config = DatabaseConfig(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database_name
        )
        
        loader = create_loader(args.database, config)
        
        # Wait for database to be ready
        if args.database in ['starrocks', 'doris', 'columnstore'] and hasattr(loader, 'wait_for_connection'):
            if not loader.wait_for_connection(args.connection_timeout):
                logger.error(f"Could not connect to {args.database}. Please ensure it is running and ready.")
                sys.exit(1)
        elif args.database == 'tidb' and hasattr(loader, 'wait_for_connection'):
            if not loader.wait_for_connection():
                logger.error("Could not connect to TiDB. Please ensure TiDB is running.")
                sys.exit(1)
        elif hasattr(loader, 'test_connection') and not loader.test_connection():
            logger.error(f"Could not connect to {args.database}. Please ensure it is running.")
            sys.exit(1)
        
        total_start_time = time.time()
        
        if not args.skip_schema:
            logger.info("Creating database and tables...")
            loader.create_database_and_tables()
        else:
            logger.info("Skipping database and table creation")
        
        logger.info("Starting data loading...")
        loader.load_data(csv_dir)
        
        if args.database == 'tidb' and hasattr(loader, 'set_tiflash_replica'):
            logger.info("Setting TiFlash replica...")
            loader.set_tiflash_replica()
        
        total_time = time.time() - total_start_time
        logger.info(f"🎉 {args.database.upper()} data loading completed successfully in {total_time:.2f} seconds")
        
    except KeyboardInterrupt:
        logger.warning("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Error during data loading: {e}")
        if log_level == logging.DEBUG:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
