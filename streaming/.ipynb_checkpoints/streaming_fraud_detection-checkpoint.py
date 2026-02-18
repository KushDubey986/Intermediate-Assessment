from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType
from pyspark.sql.functions import to_timestamp

spark = SparkSession.builder \
    .appName("StreamingFraudDetection") \
    .getOrCreate()

schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("channel", StringType(), True)
])

streaming_df = spark.readStream \
    .schema(schema) \
    .option("header", True) \
    .csv("/home/jovyan/work/data/streaming")

streaming_df = streaming_df.withColumn(
    "transaction_date",
    to_timestamp(col("transaction_date"))
)

fraud_df = streaming_df \
    .withWatermark("transaction_date", "1 day") \
    .dropDuplicates(["transaction_id"]) \
    .withColumn(
        "is_fraud",
        when((col("amount") > 5000) & (col("channel") == "Online"), True).otherwise(False)
    )

query = fraud_df.writeStream \
    .format("parquet") \
    .option("path", "/home/jovyan/work/data/streaming_output") \
    .option("checkpointLocation", "/home/jovyan/work/data/checkpoints/fraud") \
    .outputMode("append") \
    .start()

query.awaitTermination()
