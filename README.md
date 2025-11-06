# OLAP Workload Testing & Benchmarking Suite

A comprehensive benchmark framework for evaluating analytical database performance using real-world datasets and standardized testing methodologies.

![Docker](https://img.shields.io/badge/Docker-Required-blue?logo=docker)
![Python](https://img.shields.io/badge/Python-3.12+-green?logo=python)
![License](https://img.shields.io/badge/License-Open%20Source-brightgreen)
![Databases](https://img.shields.io/badge/Databases-5%20Tested-orange)
![Queries](https://img.shields.io/badge/Queries-20%20Benchmarks-purple)

## Table of Contents

- [Overview](#overview)
- [Why OLAP Benchmarking Matters](#why-olap-benchmarking-matters)
- [Dataset Description](#dataset-description)
- [Supported Database Engines](#supported-database-engines)
- [Prerequisites & System Requirements](#prerequisites--system-requirements)
- [Quick Start Guide](#quick-start-guide)
- [Reproducibility](#reproducibility)
- [Benchmark Methodology](#benchmark-methodology)
- [Performance Results](#performance-results)
- [Key Observations](#key-observations)
- [Limitations & Considerations](#limitations--considerations)
- [Contributing](#contributing)
- [FAQ](#faq)
- [License](#license)

## Overview

This benchmark suite provides **standardized testing for Online Analytical Processing (OLAP) workloads** across multiple database engines. The framework enables fair, reproducible performance comparisons using real-world analytical queries and datasets, helping organizations make informed decisions about analytical database technologies.

### Scope and Purpose

**This benchmark specifically focuses on open-source analytical databases that can be deployed on-premises and containerized with Docker.** While we acknowledge the existence of excellent cloud-native solutions like Snowflake, Amazon Redshift, Google BigQuery, and Azure Synapse Analytics, these managed services are outside the scope of this evaluation.

**Why Open-Source, On-Premises, and Dockerizable?**
- **Data Sovereignty**: Organizations requiring complete control over their data location and governance
- **Cost Predictability**: Fixed infrastructure costs vs. variable cloud consumption pricing
- **Compliance Requirements**: Industries with strict data residency and security regulations
- **Technology Independence**: Avoiding vendor lock-in with portable, standards-based solutions
- **Customization Flexibility**: Ability to modify and tune systems for specific use cases
- **Container Portability**: Docker deployment enables consistent environments across development, testing, and production
- **Infrastructure Agnostic**: Run on any Docker-compatible platform, from laptops to enterprise clusters

This benchmark evaluates databases that can be deployed in your own infrastructure using Docker containers, whether on bare metal, virtual machines, Kubernetes clusters, or private cloud environments, providing organizations with truly portable and self-managed analytical solutions.

### Quick Results Summary

**Top Performers:**
- 🥇 **ClickHouse**: 6.41s total, 100% success rate - Best overall performance
- 🥈 **StarRocks**: 6.76s total, 100% success rate - Excellent consistency  
- 🥉 **Apache Doris**: 7.66s total, 100% success rate - Balanced performance across all queries

**Key Findings:**
- ClickHouse and StarRocks dominate performance benchmarks
- TiDB/TiFlash: 12m 15s load time, 100% query success rate, but much slower data ingestion and analytical performance compared to OLAP-focused engines
- MariaDB ColumnStore shows compatibility issues with complex queries, but has the fastest data load time
- Real-world flight data (38M+ records) provides realistic testing scenarios

### OLAP vs OLTP

**OLAP systems** are designed to handle complex analytical queries that typically involve:
- Large-scale aggregations across millions of records
- Complex multi-table joins and dimensional analysis  
- Time-series analysis and trend identification
- Ad-hoc analytical queries with varying selectivity

Unlike **OLTP** (Online Transaction Processing) systems optimized for high-volume transactional workloads, OLAP databases excel at read-heavy analytical operations, making them essential for business intelligence, data analytics, and decision support systems.

## Why OLAP Benchmarking Matters

### Performance Evaluation
Modern organizations generate massive volumes of data requiring fast analytical processing. Benchmarking helps identify which database engines can handle your specific analytical workloads with acceptable performance characteristics.

### Technology Selection
With numerous analytical database options available, from traditional columnar stores to modern cloud-native solutions, standardized benchmarking provides objective performance metrics to guide technology decisions.

### Cost Optimization
Understanding performance characteristics across different systems enables better resource planning and cost optimization, especially in cloud environments where compute and storage costs directly impact operational expenses.

## Dataset Description

The benchmark utilizes the **Bureau of Transportation Statistics (BTS) On-Time Performance** dataset, representing typical analytical scenarios found in operational reporting and business intelligence applications.

### Schema Design (Star Schema)

| Table | Type | Records | Description |
|-------|------|---------|-------------|
| `airlines` | Dimension | 30 | Airline carrier information |
| `airports` | Dimension | 400 | Airport details and geographic data |
| `flights` | Fact | 38,083,735 | Flight departure/arrival data |

This star schema design reflects typical data warehouse patterns, enabling realistic testing of dimension-fact table joins, aggregations, and filtering operations.

## Supported Database Engines

**Tested Versions:**
- ClickHouse v23.8+
- Apache Doris v3.0+
- StarRocks v3.0+
- TiDB v7.0+ (with TiFlash)
- MariaDB ColumnStore v23.02.4+

### Purpose-Built OLAP Systems

- **Apache Doris**: Open-source MPP analytical database with vectorized execution engine, designed for real-time analytics and high-performance OLAP workloads
- **ClickHouse**: Open-source MPP columnar analytical database with vectorized execution, optimized for real-time analytics, time-series data, and high-performance OLAP workloads  
- **StarRocks**: Open-source MPP analytical database with vectorized execution engine, designed for real-time analytics and high-performance OLAP workloads

### Hybrid and Specialized Systems

- **MariaDB ColumnStore**: Columnar storage engine for analytical workloads within the MariaDB ecosystem
- **PingCap TiDB**: While primarily designed as an HTAP (Hybrid Transactional/Analytical Processing) system for OLTP workloads, TiDB includes **TiFlash**, a columnar storage extension that enables analytical query processing. TiFlash provides columnar replicas of TiDB data, allowing the system to handle both transactional and analytical workloads within a unified architecture.

> **Note:** TiDB is not primarily an OLAP product but offers analytical capabilities through TiFlash.

## Prerequisites & System Requirements

### Software Dependencies

**Docker & Docker Compose**
- Installation: https://docs.docker.com/get-docker/

**Python 3.12+ with required packages:**

- See [Quick Start Guide](#quick-start-guide) below

**TiUP (for TiDB cluster management):**
```bash
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
```

**ClickHouse Client:**
- macOS: `brew install clickhouse`
- Linux: `curl https://clickhouse.com/ | sh`
- Windows: Download from https://clickhouse.com/docs/en/getting-started/install

### Hardware Recommendations

| Component | Minimum | Recommended | Benchmark System |
|-----------|---------|-------------|------------------|
| Memory | 16GB RAM | 32GB+ RAM | 16GB (M1 Pro) |
| Storage | 200GB free space | SSD storage | 926GB SSD |
| CPU | Multi-core | High-performance multi-core | M1 Pro (10-core) |
| Network | Standard | High bandwidth for data loading | Localhost |

> **Note**: The benchmark results shown were obtained on a MacBook Pro M1 Pro with 16GB RAM, demonstrating that excellent performance can be achieved even on laptop-class hardware.

## Quick Start Guide

### 1. Environment Setup

```bash
git clone <repository-url>
cd analytics_benchmark

# Install Python dependencies
pip install -r requirements.txt
```

### 2. Data Preparation

```bash
python3 load/get_data.py
```

### 3. Database-Specific Execution

#### Apache Doris

```bash
# Start Apache Doris
docker compose -f docker/doris.yml up -d

# Load data (note: Doris requires additional time for backend nodes to initialize)
python3 load/load_data.py --database doris

# Run benchmark
python3 run_benchmarks.py --database doris
```

#### ClickHouse

```bash
# Start ClickHouse
docker compose -f docker/clickhouse.yml up -d

# Load data
python3 load/load_data.py --database clickhouse

# Run benchmark
python3 run_benchmarks.py --database clickhouse
```

#### MariaDB ColumnStore

```bash
# Start MariaDB ColumnStore
docker compose -f docker/columnstore.yml up -d

# Load data
python3 load/load_data.py --database columnstore

# Run benchmark
python3 run_benchmarks.py --database columnstore
```

#### TiDB with TiFlash

```bash
# Update TiUP to latest version
tiup update --all

# Start TiDB using TiUP (note: TiDB uses TiUP instead of Docker for optimal performance)
tiup playground

# Load data
python3 load/load_data.py --database tidb

# Run benchmark
python3 run_benchmarks.py --database tidb
```

> **Note**: TiDB uses TiUP for cluster management rather than Docker Compose. TiUP provides better resource management and TiFlash integration for analytical workloads.

#### StarRocks

```bash
# Start StarRocks
docker compose -f docker/starrocks.yml up -d

# Load data
python3 load/load_data.py --database starrocks

# Run benchmark
python3 run_benchmarks.py --database starrocks
```

### Important Notes on Database Startup

**Apache Doris & StarRocks**: These systems require additional time for backend nodes to initialize. The data loading script will automatically wait for all components to be ready (up to 2 minutes for each system). If you encounter "no available capacity" errors, the backend nodes may still be provisioning - wait longer or restart the containers.

**TiDB**: TiFlash columnar replicas need time to synchronize after initial data loading for optimal analytical performance.

**ClickHouse**: Generally starts quickly but may need a few seconds for metadata initialization.

**MariaDB ColumnStore**: Requires proper columnstore engine initialization which can take 30-60 seconds.

## Reproducibility

To reproduce the exact results shown in this benchmark:

1. **Environment**: Use the same hardware specifications listed in [Testing Environment](#testing-environment)
2. **Software Versions**: Ensure you're using the database versions specified in [Supported Database Engines](#supported-database-engines)
3. **Data Consistency**: The benchmark uses the same BTS dataset with 38,083,735 flight records
4. **Configuration**: Use the provided Docker configurations without modification
5. **Methodology**: Follow the cold cache approach - restart containers between test runs

**Note**: Results may vary slightly due to system load and hardware differences, but relative performance rankings should remain consistent.

## Benchmark Methodology

### Query Suite Design

The benchmark includes **a comprehensive set of 20 analytical queries** representing common OLAP patterns:

- Simple aggregations and filtering operations
- Multi-table joins with dimensional analysis  
- Time-based analysis and seasonal trending
- Complex analytical functions and window operations
- Advanced star schema pattern demonstrations
- Competitive analysis and market intelligence queries
- Aircraft utilization and operational efficiency analysis
- Weather impact and disruption pattern analysis
- Cross-airline performance benchmarking
- Varying data selectivity and aggregation complexity levels

**Example Query Patterns:**

The actual benchmark queries are significantly more complex than basic examples. Here are simplified representations of the analytical patterns used:

```sql
-- Pattern from Query 1: Multi-CTE airline market analysis
-- (Actual query calculates market share percentages with cross joins)
WITH flight_stats AS (
    SELECT a.airline, COUNT(*) AS volume, SUM(f.cancelled) AS cancelled
    FROM flights f JOIN airlines a ON f.carrier = a.iata_code
    WHERE f.year = 2020 GROUP BY a.airline
)
SELECT fs.airline, fs.volume,
       ROUND(100 * fs.volume / total.volume, 2) AS market_share_pct
FROM flight_stats fs CROSS JOIN (SELECT SUM(volume) AS volume FROM flight_stats) total;

-- Pattern from Query 12: Advanced window functions with performance categorization  
-- (Actual query includes month name conversion and performance categories)
WITH monthly_perf AS (
    SELECT a.airline, f.month, AVG(f.arr_delay) AS avg_delay
    FROM flights f JOIN airlines a ON f.carrier = a.iata_code
    GROUP BY a.airline, f.month HAVING COUNT(*) >= 100
)
SELECT airline, month, avg_delay,
       RANK() OVER (PARTITION BY month ORDER BY avg_delay ASC) AS rank,
       CASE WHEN RANK() OVER (...) <= 3 THEN 'Top Performer' ELSE 'Average' END AS category
FROM monthly_perf;
```

> **Note**: These are simplified representations. The actual queries in [`queries/sql/`](queries/sql/) are more sophisticated, featuring complex CTEs, advanced aggregations, comprehensive business logic, and performance optimizations. View the individual `.sql` files for complete query implementations.

### Performance Metrics

- **Query Execution Time**: Individual query performance measurement
- **Data Loading Performance**: Bulk data ingestion capabilities
- **Error Handling**: System stability and query completion rates
- **Resource Utilization**: Memory and CPU consumption patterns

### Testing Environment

- **Hardware**: MacBook Pro (M1 Pro, 2021)
  - **CPU**: Apple M1 Pro (10-core: 8 performance + 2 efficiency cores)
  - **Memory**: 16GB unified memory
  - **Storage**: 926GB SSD
- **OS**: macOS 15.5 (24F74)
- **Containerization**: Docker Desktop for Mac
- **Container Resources**: 
  - Memory limit: 8GB per database container
  - CPU limit: 6 cores per database container
  - Shared volumes for data persistence
- **Methodology**: Cold cache scenarios for realistic performance
- **Workload**: Single-user workload simulation
- **Resource Allocation**: Standardized hardware allocation across all databases
- **Network**: Localhost connections to eliminate network latency

## Performance Results

> **Last Updated**: July 2025 | **Dataset**: 38,083,735 flight records | **Test Environment**: MacBook Pro M1 Pro


### Data Load Performance

The following table shows data ingestion performance for the complete dataset (38,083,735 flight records):

**Load Speed Legend:**
 - 🟢 Fast (< 1.5 minutes)
 - 🟡 Moderate (1.5–5 minutes)
 - 🟠 Slow (5–10 minutes)
 - 🔴 Very Slow (> 10 minutes)

| Database      | Import Time   | Rows/s    | Status    | Notes                |
|---------------|---------------|-----------|-----------|----------------------|
| ClickHouse    | 🟢 57.00 sec  | 694,723   | ✅ Success | Native CSV, columnar |
| Doris         | 🟢 85.00 sec  | 447,940   | ✅ Success | Stream Load, BE warmup  |
| StarRocks     | 🟢 100.00 sec | 379,777   | ✅ Success | Vectorized ingest    |
| TiDB/TiFlash  | 🟡 159.31 sec | 239,080   | ✅ Success | Lightning, all tables replica |
| ColumnStore   | 🟢 39.00 sec  | 1,109,264 | ✅ Success | cpimport, conversion |

### Query Execution Performance

The following table shows execution times for each query in the benchmark suite:

**Performance Legend:**
- 🟢 Excellent (< 0.5 seconds)
- 🟡 Good (0.5 - 2.0 seconds)
- 🟠 Moderate (2.0 - 10.0 seconds)
- 🔴 Slow (> 10.0 seconds)
- ❌ Error

| Query | ClickHouse | ColumnStore | Doris | StarRocks | TiDB |
|-------|------------|-------------|-------|-----------|------|
| [1](queries/sql/1.sql) | 🟢 0.13 sec | ❌ Error | 🟢 0.19 sec | 🟢 0.13 sec | � 0.13 sec |
| [2](queries/sql/2.sql) | 🟡 0.54 sec | 🟠 6.35 sec | 🟡 0.53 sec | 🟡 0.99 sec | � 0.43 sec |
| [3](queries/sql/3.sql) | 🟢 0.05 sec | ❌ Error | 🟢 0.05 sec | 🟢 0.08 sec | 🟢 0.08 sec |
| [4](queries/sql/4.sql) | 🟢 0.01 sec | ❌ Error | 🟢 0.02 sec | 🟢 0.05 sec | 🟢 0.05 sec |
| [5](queries/sql/5.sql) | 🟢 0.10 sec | 🟡 0.99 sec | 🟢 0.06 sec | 🟢 0.08 sec | 🟢 0.18 sec |
| [6](queries/sql/6.sql) | 🟡 0.66 sec | 🔴 15.03 sec | 🟡 0.88 sec | 🟡 0.57 sec | � 0.75 sec |
| [7](queries/sql/7.sql) | 🟡 0.63 sec | 🔴 14.40 sec | 🟡 0.76 sec | 🟢 0.46 sec | � 0.71 sec |
| [8](queries/sql/8.sql) | 🟢 0.06 sec | 🟡 1.96 sec | 🟢 0.11 sec | 🟢 0.08 sec | 🟢 0.12 sec |
| [9](queries/sql/9.sql) | 🟡 0.68 sec | 🔴 14.27 sec | 🟡 0.77 sec | 🟢 0.42 sec | � 0.69 sec |
| [10](queries/sql/10.sql) | 🟢 0.09 sec | 🟠 2.57 sec | 🟢 0.14 sec | 🟢 0.11 sec | 🟢 0.15 sec |
| [11](queries/sql/11.sql) | 🟢 0.22 sec | 🟠 4.04 sec | 🟢 0.28 sec | 🟢 0.19 sec | � 0.31 sec |
| [12](queries/sql/12.sql) | 🟡 0.81 sec | 🔴 20.81 sec | 🟡 1.07 sec | 🟡 0.62 sec | � 0.96 sec |
| [13](queries/sql/13.sql) | 🟢 0.25 sec | 🟠 4.74 sec | 🟢 0.31 sec | 🟢 0.19 sec | � 1.18 sec |
| [14](queries/sql/14.sql) | 🟢 0.49 sec | 🟠 9.58 sec | 🟢 0.44 sec | 🟢 0.36 sec | 🔴 44.99 sec |
| [15](queries/sql/15.sql) | 🟢 0.40 sec | 🟠 9.05 sec | 🟢 0.48 sec | 🟢 0.27 sec | � 3.43 sec |
| [16](queries/sql/16.sql) | 🟢 0.24 sec | 🟠 6.32 sec | 🟢 0.33 sec | 🟡 0.90 sec | � 0.34 sec |
| [17](queries/sql/17.sql) | 🟢 0.16 sec | ❌ Error | 🟢 0.16 sec | 🟢 0.14 sec | 🟢 0.18 sec |
| [18](queries/sql/18.sql) | 🟢 0.33 sec | 🟠 5.74 sec | 🟢 0.40 sec | 🟡 0.66 sec | � 0.42 sec |
| [19](queries/sql/19.sql) | 🟢 0.31 sec | ❌ Error | 🟢 0.38 sec | 🟢 0.27 sec | � 1.63 sec |
| [20](queries/sql/20.sql) | 🟢 0.25 sec | 🟠 5.36 sec | 🟢 0.30 sec | 🟢 0.19 sec | � 0.43 sec |

## Key Observations

### Performance Leaders

- **ClickHouse**: Exceptional performance across all query types with 100% success rate and consistently fast execution times. Particularly strong for simple aggregations and filtering operations, with excellent scalability to complex analytical queries.
- **StarRocks**: Outstanding performance with 100% query success rate and highly competitive execution times. Demonstrates excellent consistency across different query patterns with total execution time of 6.75 seconds.
- **Apache Doris**: Balanced performance characteristics suitable for general analytical workloads with full query compatibility
- Data load performance varies widely: MariaDB ColumnStore and ClickHouse are fastest for ingestion, while TiDB/TiFlash is the slowest due to its architecture.

### System-Specific Insights

**ClickHouse Analysis**: Demonstrates outstanding performance with perfect 100% query success rate (20/20 queries) and fastest overall execution time (6.38 sec). Shows excellent optimization for both simple and complex analytical patterns, making it ideal for high-performance analytical workloads. Also among the fastest for bulk data ingestion (57.00 sec).

**StarRocks Analysis**: Delivers exceptional performance with perfect 100% query success rate (20/20 queries) and highly competitive total execution time (6.75 sec). Shows excellent vectorized execution capabilities and strong optimization for both simple and complex analytical patterns, making it a strong contender for high-performance analytical workloads. Data load speed is also competitive (100.00 sec).

**TiDB/TiFlash Analysis**: Achieves 100% query success rate (20/20 queries) with total execution time of 57.16 sec. TiDB shows significantly improved analytical performance with TiFlash replicas enabled across all tables, making it much more competitive with purpose-built OLAP systems. Most queries now execute in sub-second timeframes, with only complex analytical queries (queries 14, 15) showing longer execution times. The system provides the unique advantage of unified HTAP capabilities—enabling transactional and analytical workloads on the same dataset without ETL processes. Performance varies by query complexity, with simple to moderate queries executing very competitively and only the most complex analytical patterns taking longer. Data load speed (159.31 sec) reflects the overhead of TiDB Lightning bulk import and TiFlash replica creation across all three tables. TiDB/TiFlash is now a strong choice for organizations prioritizing hybrid transactional/analytical workloads with substantially improved analytical query performance.

**MariaDB ColumnStore**: Shows moderate compatibility challenges with complex analytical queries, achieving 75% success rate (15/20 queries). Failed queries primarily involve advanced CTEs and complex window functions, indicating some limitations with modern SQL analytical patterns. When successful, performance is generally slower than purpose-built OLAP systems, with total execution time of 121.21 sec for successful queries. Despite the fastest data load time (39.00 sec), query compatibility and execution speed lag behind OLAP-focused systems.

**Apache Doris**: Demonstrates excellent compatibility with 100% query success rate and balanced performance characteristics across all query complexity levels, making it suitable for comprehensive analytical workloads. Data load speed is competitive (85.00 sec).

### Performance Summary

| Database      | Query Time  | Success   | Failed      | Load Time   | Best For               |
|--------------|------------|-----------|------------|------------|------------------------|
| ClickHouse    | 6.41 sec   | 100%      | None       | 57.00 sec  | High-perf analytics    |
| StarRocks     | 6.76 sec   | 100%      | None       | 100.00 sec | Consistent performance |
| Doris         | 7.66 sec   | 100%      | None       | 85.00 sec  | Balanced workloads     |
| TiDB/TiFlash  | 57.16 sec  | 100%      | None       | 159.31 sec | HTAP scenarios         |
| ColumnStore   | 121.21 sec | 75%       | 1,3,4,17,19| 39.00 sec  | Legacy integration     |

*Query Time: total for successful queries only

## Limitations & Considerations

- Results reflect single-user scenarios and may not represent concurrent workload performance
- Container-based deployment may not reflect optimized production configurations
- Dataset size and query patterns may not represent all analytical workload characteristics
- Performance results are specific to the tested hardware and software configurations

## Contributing

We welcome contributions to improve this benchmark suite! Here's how you can help:

### Ways to Contribute

- **Add New Databases**: Implement connectors for additional OLAP systems
- **Expand Query Suite**: Add more analytical query patterns
- **Improve Documentation**: Enhance setup guides and analysis
- **Report Issues**: Submit bug reports or performance inconsistencies
- **Add Test Cases**: Contribute additional datasets or scenarios

### Getting Started

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-database`)
3. Make your changes and test thoroughly
4. Submit a pull request with detailed description

### Development Guidelines

- Follow existing code patterns and naming conventions
- Add comprehensive documentation for new features
- Include performance benchmarks for new database integrations
- Ensure Docker configurations are properly tested

**Contact**: For questions or collaboration opportunities, please open an issue or reach out via GitHub discussions.

## FAQ

### Common Questions

**Q: Why do some queries fail on certain databases?**
A: Different databases have varying SQL compatibility. ColumnStore, for example, has limitations with advanced CTEs and window functions. All failed queries are clearly marked with ❌ in the results.

**Q: Can I run this benchmark on my own hardware?**
A: Yes! The benchmark is designed to be portable. Results will vary based on your hardware configuration, but relative performance comparisons should remain consistent.

**Q: How often are these benchmarks updated?**
A: We aim to update benchmarks quarterly or when major database versions are released. Check the commit history for the latest updates.

**Q: Why is TiDB slower than other databases?**
A: TiDB is primarily an HTAP system designed for transactional workloads. Its analytical performance via TiFlash is slower but offers the unique advantage of unified transactional and analytical processing.

**Q: Can I add my own queries to the benchmark?**
A: Absolutely! Add your queries to the `queries/sql/` directory and they'll be automatically included in the benchmark run.

### Troubleshooting

**Issue**: "No available capacity" errors with Doris/StarRocks  
**Solution**: Backend nodes need time to initialize. Wait 2-3 minutes or restart containers.

**Issue**: ClickHouse connection failures  
**Solution**: Ensure the client is installed and the container is fully started before running benchmarks.

**Issue**: TiDB data loading is slow  
**Solution**: TiFlash needs time to create columnar replicas. This is normal and improves query performance.

**Issue**: "Couldn't find a tree builder with the features you requested: xml" error during data download  
**Solution**: This indicates `lxml` is missing. Run `pip install -r requirements.txt` again, which now includes `lxml` for XML parsing support.

## Related Projects

For additional context and comparison, consider these established analytical benchmarks:

- **[TPC-H](http://www.tpc.org/tpc-h/)**: Industry-standard decision support benchmark
- **[TPC-DS](http://www.tpc.org/tpcds/)**: Decision support benchmark with complex queries
- **[ClickBench](https://benchmark.clickhouse.com/)**: ClickHouse-focused analytical benchmark
- **[Apache Arrow DataFusion Benchmarks](https://github.com/apache/arrow-datafusion/tree/master/benchmarks)**: Modern analytical engine benchmarks

This benchmark complements existing efforts by focusing specifically on **Docker-deployable, on-premises solutions** with **real-world transportation data**, providing a practical evaluation framework for organizations considering self-hosted analytical databases.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**This benchmark provides a foundation for analytical database evaluation.** Production deployment decisions should consider additional factors including operational requirements, integration capabilities, support ecosystems, and total cost of ownership beyond raw performance metrics.