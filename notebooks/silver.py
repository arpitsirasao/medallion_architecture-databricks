# Databricks notebook source
from pyspark.sql.functions import col, to_timestamp, lower, trim

# Clean Customers Table
df_silver_customers = (
    spark.table("bronze_customers")                    # Read from Bronze table
    .dropDuplicates(["customer_id"])                 # Remove duplicate rows based on customer ID
    .withColumn("email", lower(trim(col("email"))))  # Normalize email text to lowercase and trim spaces
    .withColumn("signup_date", to_timestamp(col("signup_timestamp"))) # Convert string to proper timestamp
    .filter(col("customer_id").isNotNull())          # Drop rows where customer_id is missing
)

# Save cleaned customer data to Silver Delta table
df_silver_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_customers")

# Clean Events Table & Handle Outliers
df_silver_events = (
    spark.table("bronze_events")
    .dropDuplicates(["event_id"])                    # Remove duplicate transaction events
    .withColumn("event_timestamp", to_timestamp(col("timestamp"))) # Format timestamp
    .withColumn("amount", col("transaction_amount").cast("double")) # Ensure amount is numeric
    .filter(col("amount") >= 0)                      # Data quality guard: remove negative amount anomalies
)

# Save cleaned events data to Silver Delta table
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