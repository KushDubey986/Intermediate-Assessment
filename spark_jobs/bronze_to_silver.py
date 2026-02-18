from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, avg, when
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeToSilver")

spark = SparkSession.builder \
    .appName("BronzeToSilverIncremental") \
    .getOrCreate()

bronze_customers = spark.read.parquet("/home/jovyan/work/data/bronze/customers")

latest_customer_date = bronze_customers.select(max("ingestion_date")).collect()[0][0]

customers_df = bronze_customers.filter(
    col("ingestion_date") == latest_customer_date
)

customers_df = customers_df.dropDuplicates()

customers_df.write \
    .mode("append") \
    .parquet("/home/jovyan/work/data/silver/customers")

logger.info("Customers Silver Incremental Done")

bronze_products = spark.read.parquet("/home/jovyan/work/data/bronze/products")

latest_product_date = bronze_products.select(max("ingestion_date")).collect()[0][0]

products_df = bronze_products.filter(
    col("ingestion_date") == latest_product_date
)

products_df = products_df.dropDuplicates()

products_df.write \
    .mode("append") \
    .parquet("/home/jovyan/work/data/silver/products")

logger.info("Products Silver Incremental Done")


bronze_transactions = spark.read.parquet("/home/jovyan/work/data/bronze/transactions")

latest_txn_date = bronze_transactions.select(max("ingestion_date")).collect()[0][0]

transactions_df = bronze_transactions.filter(
    col("ingestion_date") == latest_txn_date
)

transactions_df = transactions_df.filter(col("amount") > 0)

transactions_df = transactions_df.dropDuplicates(["transaction_id"])

transactions_df = transactions_df.withColumn(
    "is_fraud",
    when(col("amount") > 5000, True).otherwise(False)
)

transactions_df.write \
    .mode("append") \
    .parquet("/home/jovyan/work/data/silver/transactions")

logger.info("Transactions Silver Incremental Done")

spark.stop()
