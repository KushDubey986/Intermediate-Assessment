from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldToWarehouseIncremental")

spark = SparkSession.builder \
    .appName("GoldToPostgresIncremental") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://postgres-db:5432/warehouse"

connection_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

dim_customer_df = spark.read.parquet("/home/jovyan/work/data/gold/dim_customer")

dim_customer_df.select(
    "customer_id",
    "name",
    "region",
    "effective_from",
    "effective_to",
    "is_current"
).write.jdbc(
    url=jdbc_url,
    table="dim_customer",
    mode="append",
    properties=connection_properties
)

logger.info("Dim Customer Incremental Load Done")

dim_product_df = spark.read.parquet("/home/jovyan/work/data/gold/dim_product")

dim_product_df.select(
    "product_id",
    "product_name",
    "category"
).write.jdbc(
    url=jdbc_url,
    table="dim_product",
    mode="append",
    properties=connection_properties
)

logger.info("Dim Product Incremental Load Done")

fact_gold_df = spark.read.parquet("/home/jovyan/work/data/gold/fact_transactions")

existing_fact_ids = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "fact_transactions") \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .load() \
    .select("transaction_id")

new_fact = fact_gold_df.join(
    existing_fact_ids,
    on="transaction_id",
    how="left_anti"
)

new_fact.select(
    "transaction_id",
    "customer_id",
    "product_id",
    "amount",
    "transaction_date",
    "is_fraud"
).write.jdbc(
    url=jdbc_url,
    table="fact_transactions",
    mode="append",
    properties=connection_properties
)

logger.info("Fact Incremental Load Done")

spark.stop()