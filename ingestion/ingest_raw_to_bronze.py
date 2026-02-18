import yaml
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, BooleanType, DoubleType
from pyspark.sql.functions import current_date

with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    filename=config["logging"]["log_file"],
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting ingestion process")

spark = SparkSession.builder \
    .appName("RawToBronzeIngestion") \
    .getOrCreate()

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("signup_date", DateType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("effective_from", DateType(), True),
    StructField("effective_to", DateType(), True)
])

product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True)
])

transaction_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("is_fraud", BooleanType(), True)
])

def ingest_file(raw_path, bronze_path, schema, table_name):
    try:
        logging.info(f"Reading {table_name} from {raw_path}")

        df = spark.read \
            .schema(schema) \
            .option("header", True) \
            .option("mode", "PERMISSIVE") \
            .csv(raw_path)

        row_count = df.count()
        logging.info(f"{table_name} rows read: {row_count}")

        df = df.withColumn("ingestion_date", current_date())
        
        df.write \
            .mode("append") \
            .partitionBy("ingestion_date") \
            .parquet(bronze_path)

        logging.info(f"{table_name} written to Bronze successfully")

    except Exception as e:
        logging.error(f"Error processing {table_name}: {str(e)}")
        raise e

ingest_file(
    config["paths"]["raw_customers"],
    config["paths"]["bronze_customers"],
    customer_schema,
    "customers"
)

ingest_file(
    config["paths"]["raw_products"],
    config["paths"]["bronze_products"],
    product_schema,
    "products"
)

ingest_file(
    config["paths"]["raw_transactions"],
    config["paths"]["bronze_transactions"],
    transaction_schema,
    "transactions"
)

logging.info("Ingestion completed successfully")
spark.stop()