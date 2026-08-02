# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

silver_table = spark.read.table("spark_kafka_projects.wikidata.wikimedia_data")

# COMMAND ----------

process_df = (
    silver_table
    .withColumn("event_timestamp", from_unixtime(col("timestamp")).cast("timestamp"))
    .withColumn("is_bot", coalesce(col("bot"), lit(False)))
    )

# COMMAND ----------

gold_table_window = (
    process_df
    #define a watermark to handel to drop late arriving data beyond 10 mins
    .withWatermark("event_timestamp", "10 minutes")
    .groupBy(
        window("event_timestamp", "5 minutes", "2 minutes"),
        "wiki",
        "is_bot",
        "type" # edit, catagories etc
    )
    .count()
)

# COMMAND ----------

gold_table_window.display()

# COMMAND ----------

final_gole_table = (
    gold_table_window
    .select(
        col("window.start").alias("window_start_timestamp"),
        col("window.end").alias("window_end_timestamp"),
        col("wiki"),    
        col("is_bot"),
        col("type"),
        col("count").alias("event_count")
    )
    )
final_gole_table.display()

# COMMAND ----------

final_gole_table.write.mode("append").option("mergeSchema", True).saveAsTable("spark_kafka_projects.wikidata.dashboard_aggregations")
