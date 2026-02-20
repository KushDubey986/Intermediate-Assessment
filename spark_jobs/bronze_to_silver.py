from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, lit, current_date
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeToSilver")

spark = SparkSession.builder \
    .appName("BronzeToSilverIncremental") \
    .getOrCreate()

bronze_customers = spark.read.parquet("/home/jovyan/work/data/bronze/customers")

latest_customer_date = bronze_customers.select(max("ingestion_date")).collect()[0][0]

incoming_customers = bronze_customers.filter(
    col("ingestion_date") == latest_customer_date
).select("customer_id","name","region")

silver_path = "/home/jovyan/work/data/silver/customers"

window_spec = Window.partitionBy("customer_id").orderBy(col("ingestion_date").desc())

incoming_customers = bronze_customers \
    .withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn") \
    .select("customer_id","name","region")

if os.path.exists(silver_path):

    silver_customers = spark.read.parquet(silver_path)

    current_silver = silver_customers.filter(col("is_current") == True)

    joined = incoming_customers.alias("new").join(
        current_silver.alias("old"),
        col("new.customer_id") == col("old.customer_id"),
        "left"
    )

    changed = joined.filter(
        (col("old.customer_id").isNotNull()) &
        (
            (col("new.name") != col("old.name")) |
            (col("new.region") != col("old.region"))
        )
    )

    unchanged = joined.filter(
        (col("old.customer_id").isNotNull()) &
        (col("new.name") == col("old.name")) &
        (col("new.region") == col("old.region"))
    )

    new_records = joined.filter(col("old.customer_id").isNull())

    expired = changed.select(
        col("old.customer_id").alias("customer_id"),
        col("old.name").alias("name"),
        col("old.region").alias("region"),
        col("old.effective_from"),
        current_date().alias("effective_to"),
        lit(False).alias("is_current")
    )

    inserted_updates = changed.select(
        col("new.customer_id").alias("customer_id"),
        col("new.name").alias("name"),
        col("new.region").alias("region"),
        current_date().alias("effective_from"),
        lit(None).cast("date").alias("effective_to"),
        lit(True).alias("is_current")
    )

    inserted_new = new_records.select(
        col("new.customer_id").alias("customer_id"),
        col("new.name").alias("name"),
        col("new.region").alias("region"),
        current_date().alias("effective_from"),
        lit(None).cast("date").alias("effective_to"),
        lit(True).alias("is_current")
    )

    unchanged_existing = silver_customers.join(
        expired.select("customer_id"),
        "customer_id",
        "left_anti"
    )

    final_df = unchanged_existing.unionByName(expired) \
        .unionByName(inserted_updates) \
        .unionByName(inserted_new)

else:

    final_df = incoming_customers.select(
        col("customer_id"),
        col("name"),
        col("region"),
        current_date().alias("effective_from"),
        lit(None).cast("date").alias("effective_to"),
        lit(True).alias("is_current")
    )

final_df.write.mode("overwrite").parquet(silver_path)

logger.info("Customers SCD Type 2 Applied")

spark.stop()