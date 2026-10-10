# Databricks notebook source
from pyspark.sql.functions import sum, count, avg

# Join Silver tables and aggregate metrics for business reporting
df_gold_summary = (
    spark.table("silver_events")
    .join(spark.table("silver_customers"), "customer_id") # Join events and customer profiles on customer_id
    .groupBy("country", "membership_tier", "event_date")  # Group by business dimensions
    .agg(
        count("event_id").alias("total_transactions"),    # Count total purchase transactions
        sum("amount").alias("total_revenue"),             # Sum total sales revenue
        avg("amount").alias("avg_order_value")            # Calculate average order value
    )
)

# Write final aggregated dataset to the Gold layer Delta table
df_gold_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_daily_sales_summary")

print("Gold table created successfully: gold_daily_sales_summary")

# COMMAND ----------

display(spark.sql("SELECT * FROM gold_daily_sales_summary LIMIT 10"))