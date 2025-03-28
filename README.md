# Data Comparison Tool: SQL Server vs Snowflake

## Overview
The **Data Comparison Tool** is a high-performance PySpark-based application designed to compare datasets between **SQL Server** and **Snowflake** using **checksum-based validation**. It efficiently identifies mismatched, missing, or orphan records while ensuring optimal performance through parallel processing and adaptive query execution.

## Features
- **Checksum-based validation** (MD5 hashes for data integrity)
- **Full outer join** to detect mismatches and orphan records
- **Optimized performance** using parallel data loading and caching
- **Column normalization** for consistent data types
- **Secure & scalable** with AWS Secrets Manager and encrypted connections

## Prerequisites
### System Requirements
- **Java 8/11** (Required for Apache Spark)
- **Python 3.8+**
- **Apache Spark 3.3+**
- **64GB RAM** (Recommended)
- **16+ CPU Cores** (Recommended)

### Software Dependencies
Install the required Python packages:
```bash
pip install pyspark==3.3.1 boto3 py4j python-dotenv snowflake-connector-python==2.7.9
```

### Required JDBC Drivers
Ensure you have the following JDBC drivers in your Spark `jars` directory:
- **Snowflake:** `spark-snowflake_2.12-2.11.0-spark_3.3.jar`
- **SQL Server:** `mssql-jdbc-12.2.0.jre8.jar`

## Configuration
### Snowflake Connection
Create a `.env` file with the following details:
```ini
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=DEV_SANDBOX_DB
SNOWFLAKE_SCHEMA=JDHARMALINGAM
```

### SQL Server Connection
Define connection parameters in the code:
```python
jdbc_params = {
    "url": "jdbc:sqlserver://{server};databaseName={db}",
    "user": "hstmdevs",
    "password": "devsrock",
    "num_partitions": 16
}
```

### AWS Secrets Manager (Optional)
Use AWS Secrets Manager for secure credential storage:
```python
AWS_REGION = "us-east-1"
SECRET_NAME = "data-comparison-tool-secrets"
```

## Running the Tool
### Spark Session Configuration
Modify memory settings in `create_spark_session()`:
```python
.config("spark.executor.memory", "64g")  # Increase for larger datasets
.config("spark.driver.memory", "32g")
```

### Execution Command
Run the tool using `spark-submit`:
```bash
spark-submit \
  --packages net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3,com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8 \
  Datacomparewithchecksum.py
```

## Key Functionality
### Data Comparison Logic
- **Checksum-based validation** using MD5
- **Full outer join** to detect mismatches and orphan records
- **Column normalization** for consistent data types

### Performance Optimizations
- **Parallel data loading** with adaptive partitioning
- **Dynamic partitioning:** `num_partitions = os.cpu_count() * 2`
- **Memory caching** for improved performance

## Troubleshooting
### Column Conflicts
**Error:** `[COLUMN_ALREADY_EXISTS]`
**Solution:** Use column prefixing in `compare_datasets()`.

### Memory Issues
**Error:** `Java heap space`
**Solution:** Increase Spark memory allocation:
```python
.config("spark.executor.memory", "128g")
.config("spark.driver.memory", "64g")
```

### Connection Failures
**Error:** `JDBC connection refused`
**Solution:**
- Check network/firewall settings
- Validate credentials
- Verify JDBC URL format

## Output & Storage
- **Format:** Parquet with Zstandard compression
- **Partitions:** 8
- **Storage:** Columnar format for efficiency
- **Location:** `./test/` directory

## Security Considerations
### Credential Management
🚨 **Avoid hardcoded credentials!** Use AWS Secrets Manager or HashiCorp Vault.

### Data Encryption
- Enable SSL for JDBC connections
- Use **S3 server-side encryption** for output files
- Secure Spark I/O:
  ```python
  .config("spark.io.encryption.enabled", "true")
  ```

## Customization
### Comparison Filters
Modify filters in `load_configuration()`:
```python
config['COMPARE_COLUMNS'] = "col1,col2,col3"
config['CUSTOM_CONDITIONS'] = "created_at > '2023-01-01'"
```

### Output Format
Save results as CSV instead of Parquet:
```python
.write.format("csv")
.option("header", "true")
.save("s3a://bucket/comparison-results/")
```

### Performance Tuning
Modify partitioning:
```python
jdbc_params['num_partitions'] = os.cpu_count() * 2
```

## Contact & Support
📧 **Email:** [Your Team Email]
📖 **License:** [Specify License]

