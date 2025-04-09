# Sample Flight Data
## Description 
This simple data set uses the [on time performance](https://www.transtats.bts.gov/homepage.asp) dataset from the *Bureau of Transportation Statistics (BTS)* for US based commercial airline flights. This includes the following 3 tables:

#### CSV Files
*   **airlines**: Dimension table for airlines (30 records)
*   **airports**: Dimension table for airports (400 records)
*   **flights**: Fact table for airline departure data (38,083,735 records)

#### BSON File
*   **flight_data**: Full flights data set with airlines and airports embedded in BSON format (38,083,735 records)

## Prerequisite
Replace this section 

## Quick Start
Replace this section

## Run The Project
### Data Load Times
|Engine                |Time                 |
|:---------------------|:--------------------|
|MariaDB InnoDB        |9 min 48.34 sec      |
|MariaDB ColumnStore   |69 sec               |
|PingCap TiDB          |12 min 36 sec        |
|Apache Doris          |57 sec               |
|StarRocks             |44 sec               |
|MongoDB               |4 min 51 sec         |

### Query Times
|Query |InnoDB               |ColumnStore          |TiDB                 |Doris                |StarRocks            |MongoDB              |
|:-----|:--------------------|:--------------------|:--------------------|:--------------------|:--------------------|:--------------------|
|1     |1 min 59 sec         |**Error**            |0.77 sec             |0.40 sec             |0.27 sec             |15.02 sec            |
|2     |4 min                |5.28 sec             |1.05 sec             |0.64 sec             |0.83 sec             |20.87 sec            |
|3     |27 sec               |0.29 sec             |0.17 sec             |0.12 sec             |0.10 sec             |0.29 sec             |
|4     |28 sec               |0.10 sec             |0.07 sec             |0.03 sec             |0.04 sec             |0.05 sec             |
|5     |1 min 29 sec         |0.99 sec             |0.35 sec             |0.08 sec             |0.10 sec             |1.15 sec             |
|6     |1 min 15 sec         |13.81 sec            |4.00 sec             |0.90 sec             |0.77 sec             |
|7     |1 min 15 sec         |12.98 sec            |3.52 sec             |0.67 sec             |0.57 sec             |
|8     |1 min                |**Error**            |0.47 sec             |0.14 sec             |0.10 sec             |
|9     |1 min 19 sec         |13.18 sec            |3.57 sec             |0.64 sec             |0.46 sec             |
|10    |10 min 48 sec        |2.70 sec             |0.48 sec             |0.14 sec             |0.10 sec             |

### Disk Usage
|Database System       |Size                 |
|:---------------------|:--------------------|
|MariaDB InnoDB        |15GB                 |
|MariaDB ColumnStore   |2GB                  |
|Apache Doris          |N/A                  |
|PingCap TiDB          |N/A                  |
|StarRocks             |N/A                  |
|MongoDB               |N/A                  |