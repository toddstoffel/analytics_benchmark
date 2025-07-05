# Sample Flight Data
## Description 
This simple data set uses the [on time performance](https://www.transtats.bts.gov/homepage.asp) dataset from the *Bureau of Transportation Statistics (BTS)* for US based commercial airline flights. This includes the following 3 tables:

#### CSV Files
*   **airlines**: Dimension table for airlines (30 records)
*   **airports**: Dimension table for airports (400 records)
*   **flights**: Fact table for airline departure data (38,083,735 records)

## Prerequisite
Before running the analytics benchmark, ensure you have the following installed and configured:

### Database Engines
- **MariaDB Server** (with InnoDB and ColumnStore engines)
- **PingCap TiDB**
- **Apache Doris**
- **StarRocks**

### Required Software
- **Python 3.8+** with the following packages:
  - `mysql-connector-python` or `PyMySQL` (for MySQL/MariaDB connections)
  - `pandas` (for data manipulation)
  - `csv` module (built-in)

### System Requirements
- Minimum 16GB RAM (32GB+ recommended for optimal performance)
- At least 200GB free disk space for data storage
- Multi-core CPU for parallel processing

## Quick Start
1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd analytics_benchmark
   ```

2. **Install Python dependencies:**
   ```bash
   pip install mysql-connector-python pandas
   ```

3. **Start database services:**
   ```bash
   # Start MariaDB
   systemctl start mariadb
   
   # Start other database engines as per their documentation
   ```

4. **Download the flight data:**
   - Download the CSV files (airlines, airports, flights) from the BTS website
   - Place them in the `data/` directory

5. **Load data into databases:**
   ```bash
   # Load into MariaDB/MySQL
   python scripts/load_mysql.py
   
   # Load into other engines
   python scripts/load_<engine>.py
   ```

6. **Run benchmark queries:**
   ```bash
   python scripts/run_benchmark.py
   ```

## Run The Project
### Data Load Times
|Engine                |Time                 |
|:---------------------|:--------------------|
|MariaDB InnoDB        |9 min 48.34 sec      |
|MariaDB ColumnStore   |69 sec               |
|PingCap TiDB          |12 min 36 sec        |
|Apache Doris          |57 sec               |
|StarRocks             |44 sec               |

### Query Times
|Query |InnoDB               |ColumnStore          |TiDB                 |Doris                |StarRocks            |
|:-----|:--------------------|:--------------------|:--------------------|:--------------------|:--------------------|
|1     |1 min 59 sec         |**Error**            |0.77 sec             |0.40 sec             |0.27 sec             |
|2     |4 min                |5.28 sec             |1.05 sec             |0.64 sec             |0.83 sec             |
|3     |27 sec               |0.29 sec             |0.17 sec             |0.12 sec             |0.10 sec             |
|4     |28 sec               |0.10 sec             |0.07 sec             |0.03 sec             |0.04 sec             |
|5     |1 min 29 sec         |0.99 sec             |0.35 sec             |0.08 sec             |0.10 sec             |
|6     |1 min 15 sec         |13.81 sec            |4.00 sec             |0.90 sec             |0.77 sec             |
|7     |1 min 15 sec         |12.98 sec            |3.52 sec             |0.67 sec             |0.57 sec             |
|8     |1 min                |**Error**            |0.47 sec             |0.14 sec             |0.10 sec             |
|9     |1 min 19 sec         |13.18 sec            |3.57 sec             |0.64 sec             |0.46 sec             |
|10    |10 min 48 sec        |2.70 sec             |0.48 sec             |0.14 sec             |0.10 sec             |

### Notes
- **MariaDB ColumnStore** was the only database engine that failed to complete all benchmark queries, encountering errors on queries 1 and 8. Despite being designed for analytical workloads, it was unable to handle certain complex query patterns in this benchmark
