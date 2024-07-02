# Databricks notebook source
from pyspark.sql.functions import col, struct, from_json
from pyspark.sql.types import StructField, StructType, StringType

# COMMAND ----------

df = spark \
     .readStream \
     .format("kafka") \
     .option("kafka.bootstrap.servers", "SERVERNAME-HERE") \
     .option("subscribe", "transactions") \
     .option("maxOffsetsPerTrigger", "10")\
     .option("startingoffsets", "latest") \
     .option("kafka.security.protocol","SASL_SSL") \
     .option("kafka.sasl.mechanism", "PLAIN") \
     .option("kafka.sasl.jaas.config", """kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="USERNAME-HERE" password="PASSWORD-HERE";""") \
    .load()

# COMMAND ----------

source_file_schema = StructType(
  [
    StructField("customer_id", StringType(), True),
    StructField("month", StringType(), True),
    StructField("category", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("spend", StringType(), True),
    StructField("transaction_id", StringType(), True)
  ]
)

# COMMAND ----------

df_data = df.withColumn(
    "value", from_json(col("value").cast("string"), source_file_schema)
).select(
    col("value.customer_id"),
    col("value.month"),
    col("value.category"),
    col("value.payment_type"),
    col("value.spend"),
    col("value.transaction_id")
)

# COMMAND ----------

#set the output path for CSV files
output_path = 'abfss://landing@csadlsgen2storageacc24.dfs.core.windows.net/kafka_consumer_sink'

#set the checkpoint location
checkpoint_location = 'abfss://landing@csadlsgen2storageacc24.dfs.core.windows.net/kafka_consumer_sink_checkpoint'

streaming_query = (
    df_data.writeStream
    .format("csv")
    .option("path", output_path)
    .option("header", True)
    .option("checkpointLocation", checkpoint_location)
    .start()
)

# COMMAND ----------

display(df_data)
