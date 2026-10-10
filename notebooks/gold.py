# Databricks notebook source
from pyspark.sql.functions import sum, count, avg

# Join Silver tables to build executive-ready summary metrics
df_gold_summary = (
    spark.table("silver_events")
    .join(spark.table("silver_customers"), "customer_id")
    .groupBy("country", "membership_tier", "event_date")
    .agg(
        count("event_id").alias("total_transactions"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value")
    )
)

df_gold_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_daily_sales_summary")

print("Gold table created successfully: gold_daily_sales_summary")

# COMMAND ----------

display(spark.sql("SELECT * FROM gold_daily_sales_summary LIMIT 10"))

# COMMAND ----------

