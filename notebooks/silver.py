# Databricks notebook source
from pyspark.sql.functions import col, to_timestamp, lower, trim

# Clean Customers Table
df_silver_customers = (
    spark.table("bronze_customers")
    .dropDuplicates(["customer_id"])
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("signup_date", to_timestamp(col("signup_timestamp")))
    .filter(col("customer_id").isNotNull())
)

df_silver_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_customers")

# Clean Events Table & Handle Outliers
df_silver_events = (
    spark.table("bronze_events")
    .dropDuplicates(["event_id"])
    .withColumn("event_timestamp", to_timestamp(col("timestamp")))
    .withColumn("amount", col("transaction_amount").cast("double"))
    .filter(col("amount") >= 0)  # Filter out negative amount anomalies
)

df_silver_events.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_events")

print("Silver tables built: silver_customers, silver_events")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add constraints to lock down data expectations in production
# MAGIC ALTER TABLE silver_events ADD CONSTRAINT valid_transaction_amount CHECK (amount >= 0);
# MAGIC ALTER TABLE silver_customers ADD CONSTRAINT valid_email_format CHECK (email LIKE '%@%.%');

# COMMAND ----------

