# Databricks notebook source
# Import necessary functions from PySpark
from pyspark.sql.functions import current_timestamp

# Define the file paths where you uploaded your CSVs in Databricks
# (Update these paths if your UI placed them in a custom FileStore folder)
customers_path = "file:/Workspace/Users/<databricks_user_id>/End-to-end Databricks/Files/ecommerce_customers.csv"
events_path = "file:/Workspace/Users/<databricks_user_id>/End-to-end Databricks/Files/ecommerce_events.csv"

# 1. Ingest Raw Customer CSV into a DataFrame
df_customers_raw = (
    spark.read.format("csv")
    .option("header", "true")        # Tells Spark that the first row contains column headers
    .option("inferSchema", "true")   # Automatically detects data types (strings, integers, etc.)
    .load(customers_path)            # Loads the file from storage
)

# Add an audit column tracking when the data entered the Bronze layer
df_bronze_customers = df_customers_raw.withColumn("ingested_at", current_timestamp())

# Write the DataFrame as an immutable Delta table named 'bronze_customers'
df_bronze_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_customers")

# 2. Ingest Raw Event CSV into a DataFrame
df_events_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(events_path)
)

# Add audit timestamp
df_bronze_events = df_events_raw.withColumn("ingested_at", current_timestamp())

# Write as Delta table 'bronze_events'
df_bronze_events.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_events")

print("Bronze tables created successfully: bronze_customers, bronze_events")

# COMMAND ----------

display(spark.sql("SHOW TABLES"))

# COMMAND ----------

display(spark.sql("SELECT * FROM bronze_customers"))

# COMMAND ----------

display(spark.sql("SELECT * FROM bronze_events"))