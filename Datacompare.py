import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import md5, concat_ws, col, to_timestamp, when
from pyspark.storagelevel import StorageLevel
import boto3
import json
from pyspark.sql import functions as F

# Environment Setup
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'


# ------------------------ AWS Secrets Manager Integration ------------------------
def get_secret(secret_name, region_name="us-east-1"):
    """Retrieve secrets securely from AWS Secrets Manager"""
    try:
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        print(f"Secret Error: {str(e)}")
        raise


# ------------------------ Spark Session Configuration ------------------------
def create_spark_session():
    """Configure optimized Spark session with Snowflake/JDBC support"""
    return SparkSession.builder \
        .appName("DataComparison") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.executor.memory", "64g") \
        .config("spark.driver.memory", "32g") \
        .config("spark.default.parallelism", "1") \
        .config("spark.sql.shuffle.partitions", "500") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.jars.packages",
                "net.snowflake:snowflake-jdbc:3.13.23,"
                "com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8,"
                "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3") \
        .config("spark.network.timeout", "3600s") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "AES256") \
        .config("spark.hadoop.fs.s3a.retry.limit", "10") \
        .config("spark.hadoop.fs.s3a.retry.interval", "5s") \
        .config("spark.sql.files.openCostInBytes", "134217728") \
        .getOrCreate()


# ------------------------ Data Loading Functions ------------------------
def load_configuration(spark):
    """Load comparison configuration from Snowflake"""
    configuration = {
        "account": "*******",
        "user": "*******",
        "password": "*******",
        "sfDatabase": "*******",
        "sfSchema": "*******",
        "sfWarehouse": "*******",
        "tableName": "*******"
    }

    return spark.read.format("snowflake") \
        .options(**{
        "sfURL": f"https://{configuration['account']}.snowflakecomputing.com",
        "sfUser": configuration['user'],
        "sfPassword": configuration['password'],
        "sfDatabase": configuration["sfDatabase"],
        "sfSchema": configuration["sfSchema"],
        "sfWarehouse": configuration["sfWarehouse"],
        # "sfRole": configuration["sfRole"]
    }) \
        .option("dbtable", configuration["tableName"]) \
        .load()


def read_sql_server(spark, config, jdbc_params):
    """Optimized parallel read from SQL Server"""
    query = f"""
        (SELECT {config['COMPARE_COLUMNS']},
        NTILE({jdbc_params['num_partitions']}) OVER (ORDER BY CAST(FORMAT(CREATE_DATETIME, 'yyyyMMddHHmmss') AS BIGINT)) AS partition_id
          FROM {config['SOURCE_TABLE_NAME']}
         WHERE  {config['CUSTOM_CONDITIONS']}
        ) tmp
    """

    df = spark.read.format("jdbc") \
        .option("url", jdbc_params['url']) \
        .option("dbtable", query) \
        .option("user", jdbc_params['user']) \
        .option("password", jdbc_params['password']) \
        .option("fetchSize", "100000") \
        .option("partitionColumn", "partition_id") \
        .option("lowerBound", 1) \
        .option("upperBound", jdbc_params['num_partitions']) \
        .option("numPartitions", jdbc_params['num_partitions']) \
        .load()

    return df  # .persist(StorageLevel.MEMORY_AND_DISK_DESER)


def read_snowflake(spark, config, databaseName):
    """Optimized Snowflake read with predicate pushdown"""
    # sf_creds = get_secret(config['snowflake_secret'])

    query = f"""
        SELECT {config['COMPARE_COLUMNS']}
        FROM {config['TARGET_TABLE_NAME']}
        WHERE {config['CUSTOM_CONDITIONS']}
          AND  database_name = '{databaseName}'
    """
    sf_options = {
        "sfURL": "*******",
        "sfDatabase": "*******",
        "sfSchema": "*******",
        "sfWarehouse": "*******",
        "sfRole": "*******",
        "sfUser": "*******",
        "sfPassword": "*******",
        "sfAuth": "*******"
    }
    return spark.read.format("snowflake") \
        .options(**sf_options) \
        .option("query", query) \
        .load() \
        # .persist(StorageLevel.MEMORY_AND_DISK_DESER)


# ------------------------ Data Comparison Logic ------------------------
def compare_datasets(sql_df, sf_df, primary_key):
    """Efficient checksum-based comparison"""
    sql_df = sql_df.withColumn("src_checksum", md5(concat_ws("|", *sql_df.columns)))
    sf_df = sf_df.withColumn("tgt_checksum", md5(concat_ws("|", *sf_df.columns)))
    join_condition = F.expr(' AND '.join([f"sql.{pk} = sf.{pk}" for pk in primary_key]))

    comparison = sql_df.alias("sql").join(
        sf_df.alias("sf"),
        col(f"sql.{primary_key}") == col(f"sf.{primary_key}"),
        "full_outer"
    )

    return (
        comparison.filter("src_checksum != tgt_checksum OR src_checksum IS NULL OR tgt_checksum IS NULL"),
        comparison.filter("src_checksum = tgt_checksum").count()
    )


def normalize_columns(df, bool_cols, decimal_cols):
    for col_name in bool_cols:
        df = df.withColumn(col_name, when(col(col_name) == True, 1).otherwise(0))

    for col_name in decimal_cols:
        df = df.withColumn(col_name, col(col_name).cast("int"))

    return df


# Function to determine default values based on data type
def get_default_value(dtype):
    if dtype.startswith("decimal"):
        return 0  # Default for decimal types
    elif dtype == "string":
        return ""  # Default for strings
    elif dtype in ["int", "bigint", "smallint", "tinyint"]:
        return 0  # Default for integers
    elif dtype in ["float", "double"]:
        return 0.0  # Default for floating numbers
    elif dtype in ["boolean", "bool"]:
        return False  # Default for booleans
    elif dtype in ["date", "timestamp"]:
        return "1970-10-10 00:00:00"  # Default for datetime
    else:
        return None  # Fallback default for unknown types


def convert_date_columns(df, col_type_map, convert_to="timestamp"):
    for col_name, col_type in col_type_map.items():
        if "date" in col_type.lower() and convert_to == "timestamp":
            df = df.withColumn(col_name, to_timestamp(col(col_name)))
    return df


# ------------------------ Main Execution ------------------------
if __name__ == "__main__":
    spark = create_spark_session()

    try:
        # 1. Load Configuration
        config = load_configuration(spark).first().asDict()

        # 2. Load Data
        sql_servers = [
            {"server": "*******", "database": "*******"},
        ]

        user = "*******"
        password = "*******"
        driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

        for sqlConfig in sql_servers:
            print(f"Processing SQL Server: {sqlConfig['server']} | Database: {sqlConfig['database']}")

            jdbc_params = {
                "url": f"jdbc:sqlserver://{sqlConfig['server']};databaseName={sqlConfig['database']};encrypt=true;trustServerCertificate=true;",
                "user": user,
                "password": password,
                "num_partitions": 16  # Match cluster cores
            }

            try:
                # Read data from SQL Server
                sql_df = read_sql_server(spark, config, jdbc_params)

                # Read data from Snowflake
                sf_df = read_snowflake(spark, config, sqlConfig['database'])

                boolean_columns = [col for col, dtype in sql_df.dtypes if dtype == "boolean"]
                sql_df = normalize_columns(sql_df, boolean_columns, [])
                sf_df = normalize_columns(sf_df, [], boolean_columns)

                # Fill missing values dynamically
                for df_name in ["sql_df", "sf_df"]:
                    df = globals()[df_name]  # Get DataFrame dynamically by name
                    df = df.fillna({col: get_default_value(dtype) for col, dtype in df.dtypes})
                    globals()[df_name] = df  # Update back to the original DataFrame

                # # Convert SQL Server's DATE → TIMESTAMP
                sql_col_types = {col_name: dtype for col_name, dtype in sql_df.dtypes}
                sql_df = convert_date_columns(sf_df, sql_col_types, convert_to="timestamp")

                # Perform Comparison
                mismatches, match_count = compare_datasets(
                    sql_df,
                    sf_df,
                    primary_key="course_instance_row_id"  # Replace with actual PK column
                )

                print(f"Matched Records: {match_count}")
                #print(f"Mismatched/Orphan Records: {mismatches.count()}")

                # Save Results

                (mismatches
                 .select("sql.course_instance_row_id")
                 .drop("src_checksum", "tgt_checksum")  # Remove temporary columns
                 .repartition(8)
                 .write
                 .mode("overwrite")
                 .option("compression", "zstd")
                 .parquet(f"./test"))
                print(f"Comparison results saved for {config['database']}")

            except Exception as e:
                print(f"Error processing {config['server']} - {config['database']}: {e}")

    finally:
        spark.stop()
        print("Spark session closed")
