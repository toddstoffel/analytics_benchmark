# Analytics Database Performance Benchmark
## Description 
This simple data set uses the [on time performance](https://www.transtats.bts.gov/homepage.asp) dataset from the *Bureau of Transportation Statistics (BTS)* for US based commercial airline flights. This includes the following 3 tables:

#### CSV Files
*   **airlines**: Dimension table for airlines (30 records)
*   **airports**: Dimension table for airports (400 records)
*   **flights**: Fact table for airline departure data (38,083,735 records)

## Prerequisite
Before running the analytics benchmark, ensure you have the following installed and configured:

### Required Software
- **Docker** and **Docker Compose** (for running containerized database engines)
  - Install from: https://docs.docker.com/get-docker/
- **TiUP** (for TiDB installation and management)
  - Install with: `curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh`
- **Python 3.12+** with required packages:
  ```bash
  pip install mysql-connector-python pandas requests beautifulsoup4 tqdm
  ```

### System Requirements
- Minimum 16GB RAM (32GB+ recommended for optimal performance)
- At least 200GB free disk space for data storage
- Multi-core CPU for parallel processing

## Quick Start

### StarRocks
1. **Start StarRocks:**
   ```bash
   docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 -itd --name quickstart starrocks/allin1-ubuntu
   ```

2. **Get the data:**
   ```bash
   python3 load/get_data.py
   ```

3. **Load data:**
   ```bash
   python3 load/load_data.py
   ```

4. **Run benchmark:**
   ```bash
   python3 run_benchmarks.py
   ```

### Apache Doris
1. **Start Apache Doris:**
   ```bash
   docker compose -f docker/doris.yml up -d
   ```

2. **Get the data:**
   ```bash
   python3 load/get_data.py
   ```

3. **Load data:**
   ```bash
   python3 load/load_data.py
   ```

4. **Run benchmark:**
   ```bash
   python3 run_benchmarks.py
   ```

### PingCAP TiDB

1. **Start TiDB:**
   ```bash
   tiup playground --db.port 9030
   ```

2. **Get the data:**
   ```bash
   python3 load/get_data.py
   ```

3. **Create schema:**
   ```bash
   mysql -P 9030 -h 127.0.0.1 -u root < schemas/tidb.sql
   ```

4. **Load data:**
   ```bash
   python3 load/load_data_tidb.py
   ```

5. **Run benchmark:**
   ```bash
   python3 run_benchmarks.py
   ```

### MariaDB ColumnStore
1. **Start MariaDB ColumnStore:**
   ```bash
   docker compose -f docker/columnstore.yml up -d
   ```

2. **Get the data:**
   ```bash
   python3 load/get_data.py
   ```

3. **Load data:**
   ```bash
   python3 load/load_data_columnstore.py
   ```

4. **Run benchmark:**
   ```bash
   python3 run_benchmarks.py
   ```

## Run The Project
### Data Load Times
|Engine                |Time                 |
|:---------------------|:--------------------|
|MariaDB ColumnStore   |69 sec               |
|PingCap TiDB          |12 min 36 sec        |
|Apache Doris          |57 sec               |
|StarRocks             |44 sec               |

### Query Times
|Query |ColumnStore          |TiDB                 |Doris                |StarRocks            |
|:-----|:--------------------|:--------------------|:--------------------|:--------------------|
|1     |**Error**            |0.77 sec             |0.40 sec             |0.27 sec             |
|2     |5.28 sec             |1.05 sec             |0.64 sec             |0.83 sec             |
|3     |0.29 sec             |0.17 sec             |0.12 sec             |0.10 sec             |
|4     |0.10 sec             |0.07 sec             |0.03 sec             |0.04 sec             |
|5     |0.99 sec             |0.35 sec             |0.08 sec             |0.10 sec             |
|6     |13.81 sec            |4.00 sec             |0.90 sec             |0.77 sec             |
|7     |12.98 sec            |3.52 sec             |0.67 sec             |0.57 sec             |
|8     |**Error**            |0.47 sec             |0.14 sec             |0.10 sec             |
|9     |13.18 sec            |3.57 sec             |0.64 sec             |0.46 sec             |
|10    |2.70 sec             |0.48 sec             |0.14 sec             |0.10 sec             |

### Notes
- **MariaDB ColumnStore** was the only database engine that failed to complete all benchmark queries, encountering errors on queries 1 and 8. Despite being designed for analytical workloads, it was unable to handle certain complex query patterns in this benchmark