from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SilverToGoldIncremental")

spark = SparkSession.builder \
    .appName("SilverToGoldIncremental") \
    .getOrCreate()

customers_silver = spark.read.parquet("/home/jovyan/work/data/silver/customers")
products_silver = spark.read.parquet("/home/jovyan/work/data/silver/products")
transactions_silver = spark.read.parquet("/home/jovyan/work/data/silver/transactions")

dim_customer = customers_silver \
    .withColumn("customer_sk", monotonically_increasing_id())

dim_customer.write \
    .mode("append") \
    .parquet("/home/jovyan/work/data/gold/dim_customer")

logger.info("Dim Customer Incremental Done")

dim_product = products_silver \
    .withColumn("product_sk", monotonically_increasing_id())

dim_product.write \
    .mode("append") \
    .parquet("/home/jovyan/work/data/gold/dim_product")

logger.info("Dim Product Incremental Done")

try:
    existing_fact = spark.read.parquet("/home/jovyan/work/data/gold/fact_transactions")
except:
    existing_fact = None

if existing_fact:
    new_transactions = transactions_silver.join(
        existing_fact.select("transaction_id"),
        on="transaction_id",
        how="left_anti"
    )
else:
    new_transactions = transactions_silver

fact_transactions = new_transactions.select(
    "transaction_id",
    "customer_id",
    "product_id",
    "amount",
    "transaction_date",
    "is_fraud"
)

fact_transactions.write \
    .mode("append") \
    .parquet("/home/jovyan/work/data/gold/fact_transactions")

logger.info("Fact Incremental Done")

spark.stop()
