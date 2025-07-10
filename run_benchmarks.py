import mysql.connector
import os
import time
import sys
import argparse
from typing import Dict, List, Tuple, Optional
from contextlib import contextmanager
import logging
import difflib

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

class DatabaseChoiceAction(argparse.Action):
    """Custom action that provides suggestions for invalid database choices"""
    
    def __call__(self, parser, namespace, values, option_string=None):
        valid_choices = ['doris', 'starrocks', 'clickhouse', 'tidb', 'columnstore']
        
        if values not in valid_choices:
            # Find closest matches
            close_matches = difflib.get_close_matches(values, valid_choices, n=3, cutoff=0.6)
            error_msg = f"invalid choice: '{values}' (choose from {', '.join(valid_choices)})"
            
            if close_matches:
                error_msg += f"\n\nDid you mean: {', '.join(close_matches)}?"
            
            parser.error(f"argument --database: {error_msg}")
        
        setattr(namespace, self.dest, values)

DATABASE_CONNECTIONS = {
    "doris": {
        "host": "127.0.0.1",
        "port": 9030,
        "user": "root",
        "password": "",
        "charset": "utf8mb4",
        "collation": "utf8mb4_general_ci",
        "database": "bts"
    },
    "starrocks": {
        "host": "127.0.0.1",
        "port": 9030,
        "user": "root",
        "password": "",
        "charset": "utf8mb4",
        "collation": "utf8mb4_general_ci",
        "database": "bts"
    },
    "clickhouse": {
        "host": "127.0.0.1",
        "port": 9004,
        "user": "default",
        "password": "",
        "database": "bts"
    },
    "tidb": {
        "host": "127.0.0.1",
        "port": 4000,
        "user": "root",
        "password": "",
        "charset": "utf8mb4",
        "collation": "utf8mb4_general_ci",
        "database": "bts"
    },
    "columnstore": {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "admin",
        "password": "C0lumnStore!",
        "charset": "utf8mb4",
        "collation": "utf8mb4_general_ci",
        "database": "bts"
    }
}

MAX_RETRIES = 3
RETRY_DELAY = 5

class DatabaseBenchmark:
    def __init__(self, database_type: str, config_overrides: Dict = None):
        self.database_type = database_type
        self.db_config = DATABASE_CONNECTIONS[database_type].copy()
        
        if config_overrides:
            self.db_config.update(config_overrides)
        
        self.conn = None
        self.cursor = None
        
    @contextmanager
    def connection(self):
        """Context manager for database connections"""
        try:
            self.conn = self._connect_with_fallback()
            self.cursor = self.conn.cursor(buffered=True)
            yield self.conn, self.cursor
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
    
    def _connect_with_fallback(self) -> mysql.connector.MySQLConnection:
        """Attempt connection with fallback to 'default' user if needed"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            logger.info(f"Connected successfully with user: {self.db_config['user']}")
            return conn
        except mysql.connector.Error as err:
            if self._is_auth_error(err) and self.db_config['user'] != 'default':
                logger.info(f"Authentication failed with '{self.db_config['user']}', trying 'default'...")
                self.db_config["user"] = "default"
                try:
                    conn = mysql.connector.connect(**self.db_config)
                    logger.info(f"Connected successfully with user: {self.db_config['user']}")
                    return conn
                except mysql.connector.Error as fallback_err:
                    logger.error(f"Failed to connect with both users: {fallback_err}")
                    sys.exit(1)
            else:
                logger.error(f"Database connection error: {err}")
                sys.exit(1)
    
    @staticmethod
    def _is_auth_error(err: mysql.connector.Error) -> bool:
        """Check if error is authentication related"""
        error_str = str(err).lower()
        auth_patterns = ["access denied", "authentication", "unknown user", "user", "password", "denied"]
        return any(pattern in error_str for pattern in auth_patterns)
    
    @staticmethod
    def _is_memory_error(err: mysql.connector.Error) -> bool:
        """Check if error is memory related"""
        error_str = str(err).lower()
        return "mem_limit_exceeded" in error_str or "memory not enough" in error_str
    
    @staticmethod
    def _is_timeout_error(err: mysql.connector.Error) -> bool:
        """Check if error is timeout related"""
        error_str = str(err).lower()
        return "timeout" in error_str or "cancelled" in error_str
    
    def _execute_statements(self, cursor, sql_script: str) -> None:
        """Execute SQL statements from script"""
        statements = [stmt.strip() for stmt in sql_script.split(";") if stmt.strip()]
        
        for statement in statements:
            cursor.execute(statement)
            
            if cursor.with_rows:
                cursor.fetchall()
            
            while cursor.nextset():
                pass
            
            self.conn.commit()
    
    def execute_query_with_retry(self, sql_script: str, filename: str) -> Tuple[bool, float]:
        """Execute query with retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                start_time = time.time()
                
                if not self.conn or not self.conn.is_connected():
                    self.conn = self._connect_with_fallback()
                    self.cursor = self.conn.cursor(buffered=True)
                
                self._execute_statements(self.cursor, sql_script)
                
                elapsed_time = time.time() - start_time
                minutes, seconds = divmod(elapsed_time, 60)
                logger.info(f"✅ Executed {filename} successfully in {int(minutes)}m {seconds:.2f}s (attempt {attempt + 1})")
                return True, elapsed_time
                
            except mysql.connector.Error as err:
                if self._is_memory_error(err) or self._is_timeout_error(err):
                    if attempt < MAX_RETRIES - 1:
                        error_type = "Memory" if self._is_memory_error(err) else "Timeout"
                        logger.warning(f"⚠️  {error_type} error on attempt {attempt + 1} for {filename}. Retrying in {RETRY_DELAY} seconds...")
                        time.sleep(RETRY_DELAY)
                        
                        # Reconnect for memory errors
                        if self._is_memory_error(err):
                            if self.cursor:
                                self.cursor.close()
                            if self.conn:
                                self.conn.close()
                            time.sleep(2)
                        continue
                    else:
                        error_type = "memory constraints" if self._is_memory_error(err) else "timeout"
                        logger.error(f"❌ Failed to execute {filename} after {MAX_RETRIES} attempts due to {error_type}: {err}")
                        return False, 0
                
                elif self._is_auth_error(err) and self.db_config["user"] == "root":
                    logger.warning(f"⚠️  Privilege error detected. This will be handled by the main loop.")
                    raise
                
                else:
                    logger.error(f"❌ Error executing {filename}: {err}")
                    return False, 0
        
        return False, 0
    
    def get_sql_files(self, queries_folder: str) -> List[str]:
        """Get sorted list of SQL files"""
        if not os.path.exists(queries_folder):
            logger.error(f"Queries folder not found: {queries_folder}")
            sys.exit(1)
        
        sql_files = [f for f in os.listdir(queries_folder) if f.endswith(".sql")]
        
        if not sql_files:
            logger.error(f"No SQL files found in {queries_folder}")
            sys.exit(1)
        
        # Sort by numeric prefix
        try:
            return sorted(sql_files, key=lambda x: int(x.split(".")[0]))
        except ValueError:
            # Fallback to alphabetical sort if numeric prefix fails
            return sorted(sql_files)
    
    def run_benchmarks(self, queries_folder: str = "queries/sql") -> None:
        """Run all benchmarks"""
        sql_files = self.get_sql_files(queries_folder)
        logger.info(f"🚀 Running {len(sql_files)} queries on {self.database_type.upper()}")
        
        successful_queries = []
        failed_queries = []
        total_time = 0
        
        with self.connection():
            for sql_file in sql_files:
                logger.info(f"\n🔄 Processing {sql_file}...")
                file_path = os.path.join(queries_folder, sql_file)
                
                try:
                    with open(file_path, "r", encoding="utf-8") as file:
                        sql_script = file.read()
                    
                    if not sql_script.strip():
                        logger.warning(f"⚠️  Empty SQL file: {sql_file}")
                        continue
                    
                    success, query_time = self.execute_query_with_retry(sql_script, sql_file)
                    
                    if success:
                        successful_queries.append((sql_file, query_time))
                        total_time += query_time
                    else:
                        failed_queries.append(sql_file)
                        
                except FileNotFoundError:
                    logger.error(f"❌ File not found: {sql_file}")
                    failed_queries.append(sql_file)
                except UnicodeDecodeError:
                    logger.error(f"❌ Unable to read file (encoding issue): {sql_file}")
                    failed_queries.append(sql_file)
                except mysql.connector.Error as err:
                    if self._is_auth_error(err) and self.db_config["user"] == "root":
                        logger.info(f"🔄 Privilege error detected. Switching to 'default' user and restarting...")
                        self._restart_with_default_user(sql_files, queries_folder)
                        return
                    else:
                        logger.error(f"❌ Error with {sql_file}: {err}")
                        failed_queries.append(sql_file)
        
        self._print_summary(successful_queries, failed_queries, total_time)
        
        if failed_queries:
            sys.exit(1)
    
    def _restart_with_default_user(self, sql_files: List[str], queries_folder: str) -> None:
        """Restart benchmark with default user"""
        self.db_config["user"] = "default"
        
        successful_queries = []
        failed_queries = []
        total_time = 0
        
        with self.connection():
            for sql_file in sql_files:
                logger.info(f"\n🔄 Processing {sql_file} (with default user)...")
                file_path = os.path.join(queries_folder, sql_file)
                
                try:
                    with open(file_path, "r", encoding="utf-8") as file:
                        sql_script = file.read()
                    
                    success, query_time = self.execute_query_with_retry(sql_script, sql_file)
                    
                    if success:
                        successful_queries.append((sql_file, query_time))
                        total_time += query_time
                    else:
                        failed_queries.append(sql_file)
                        
                except Exception as err:
                    logger.error(f"❌ Error executing {sql_file} with default user: {err}")
                    failed_queries.append(sql_file)
        
        self._print_summary(successful_queries, failed_queries, total_time)
        
        if failed_queries:
            sys.exit(1)
    
    def _print_summary(self, successful_queries: List[Tuple[str, float]], 
                      failed_queries: List[str], total_time: float) -> None:
        """Print benchmark summary"""
        total_queries = len(successful_queries) + len(failed_queries)
        
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 BENCHMARK SUMMARY ({self.database_type.upper()})")
        logger.info(f"{'='*60}")
        logger.info(f"✅ Successful queries: {len(successful_queries)}/{total_queries}")
        logger.info(f"❌ Failed queries: {len(failed_queries)}")
        
        if total_time > 0:
            minutes, seconds = divmod(total_time, 60)
            logger.info(f"⏱️  Total execution time: {int(minutes)}m {seconds:.2f}s")
        
        if successful_queries:
            logger.info(f"\n✅ SUCCESSFUL QUERIES:")
            for query, exec_time in successful_queries:
                minutes, seconds = divmod(exec_time, 60)
                logger.info(f"  {query}: {int(minutes)}m {seconds:.2f}s")
        
        if failed_queries:
            logger.info(f"\n❌ FAILED QUERIES:")
            for query in failed_queries:
                logger.info(f"  {query}")

def main():
    parser = argparse.ArgumentParser(description='Run database benchmarks')
    parser.add_argument('--database', action=DatabaseChoiceAction, 
                       required=True, help='Specify which database to connect to')
    parser.add_argument('--host', help='Database host (overrides default for selected database)')
    parser.add_argument('--port', type=int, help='Database port (overrides default for selected database)')
    parser.add_argument('--user', help='Database user (overrides default for selected database)')
    parser.add_argument('--password', help='Database password (overrides default for selected database)')
    parser.add_argument('--queries-folder', default='queries/sql', help='Path to SQL queries folder')
    
    args = parser.parse_args()
    
    # Prepare configuration overrides
    config_overrides = {}
    if args.host:
        config_overrides['host'] = args.host
        logger.info(f"🔧 Override host: {args.host}")
    if args.port:
        config_overrides['port'] = args.port
        logger.info(f"🔧 Override port: {args.port}")
    if args.user:
        config_overrides['user'] = args.user
        logger.info(f"🔧 Override user: {args.user}")
    if args.password:
        config_overrides['password'] = args.password
        logger.info(f"🔧 Override password: ***")
    
    # Run benchmarks
    benchmark = DatabaseBenchmark(args.database, config_overrides)
    benchmark.run_benchmarks(args.queries_folder)

if __name__ == "__main__":
    main()
