# Sample Flight Data
## Description 
This simple data set uses the [on time performance](https://www.transtats.bts.gov/homepage.asp) dataset from the *Bureau of Transportation Statistics (BTS)* for US based commercial airline flights. This includes the following 3 tables:

### CSV Files
*   **airlines**: Dimension table for airlines (30 records)
*   **airports**: Dimension table for airports (400 records)
*   **flights**: Fact table for airline departure data (38,083,735 records)

### BSON File
*   **flight_data**: Full flights data set with airlines and airports embedded in BSON format (38,083,735 records)

## Prerequisite
Replace this section 

## Quick Start
Replace this section

### Run The Project
Replace this section

## Sample Queries
The following sample queries are provided in the [queries directory](/queries/):

1.   **1.sql / 1.js** : Provides a report of flight count, market share percentage, cancelled flights percentage, and diverted flights percentage by airline for 2020.
2.   **2.sql / 2.js** : Provides a report of the delay types by airline by year.
3.   **3.sql / 3.js** : Provides a report of the volume and average arrival delay for California based airports by airline in 2020.
4.   **4.sql / 4.js** : Provides a report of the average and maximum delay by month and hour in the day for bay area airports in 2020.
5.   **5.sql / 5.js** : Provides a report of the average and maximum delay by day and hour for bay area airports in November 2020.
6.   **6.sql / 6.js** :
7.   **7.sql / 7.js** :
8.   **8.sql / 8.js** :
9.   **9.sql / 9.js** :
10.  **10.sql / 10.js** :

### Data Load Times
|Engine                |Time                 |
|:---------------------|--------------------:|
|MariaDB InnoDB        |22 min               |
|MariaDB ColumnStore   |69 sec               |
|PingCap TiDB          |12 min 36 sec        |
|Apache Doris          |57 sec               |
|StarRocks             |6 min 15 sec         |
|MongoDB               |4 min 51 sec         |

### Query Times
|Query                 |MariaDB InnoDB       |MariaDB ColumnStore  |PingCap TiDB         |Apache Doris         |StarRocks            |MongoDB              |
|:--------------------:|--------------------:|--------------------:|--------------------:|--------------------:|--------------------:|--------------------:|
|1                     |1 min 56.69 sec      |0.46 sec             |1.15 sec             |0.18 sec             |0.22 sec             |15.02 sec            |
|2                     |4 min 14.47 sec      |1.52 sec             |18.82 sec            |0.72 sec             |0.74 sec             |20.87 sec            |
|3                     |26.87 sec            |0.21 sec             |0.77 sec             |0.13 sec             |0.17 sec             |0.29 sec             |
|4                     |27.41 sec            |0.09 sec             |5.32 sec             |0.09 sec             |0.05 sec             |0.05 sec             |
|5                     |1 min 32.10 sec      |0.42 sec             |5.46 sec             |0.12 sec             |0.11 sec             |1.15 sec             |

### Disk Usage
|Database System       |Size                 |
|:---------------------|--------------------:|
|MariaDB InnoDB        |15GB                 |
|MariaDB ColumnStore   |2GB                  |
|Apache Doris          |N/A                  |
|PingCap TiDB          |N/A                  |
|StarRocks             |N/A                  |
|MongoDB               |N/A                  |