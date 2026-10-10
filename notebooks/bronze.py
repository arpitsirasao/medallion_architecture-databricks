# Databricks notebook source
from pyspark.sql.functions import current_timestamp, input_file_name

# 1. Ingest Raw Customer CSV from workspace files
df_customers_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("file:/Workspace/Users/<databricks_user_id>/End-to-end Databricks/Files/ecommerce_customers.csv") # Adjust path if stored in your specific workspace folder
)

df_bronze_customers = df_customers_raw \
    .withColumn("ingested_at", current_timestamp())

# COMMAND ----------

df_bronze_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_customers")

# COMMAND ----------

# 2. Ingest Raw Event CSV from workspace files
df_events_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("file:/Workspace/Users/<databricks_user_id>/End-to-end Databricks/Files/ecommerce_events.csv")
)

df_bronze_events = df_events_raw \
    .withColumn("ingested_at", current_timestamp())

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

# COMMAND ----------



# COMMAND ----------

