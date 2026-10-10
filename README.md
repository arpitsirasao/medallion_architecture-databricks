# End-to-End Databricks Lakehouse Project 🚀

A production-grade reference architecture demonstrating how to ingest multi-source data, build Bronze, Silver, and Gold layers with Delta Lake, enforce data quality constraints, and generate executive dashboards on Databricks.

## 📂 Repository Structure
- `data/`: Contains raw CSV source files (`ecommerce_customers.csv` and `ecommerce_events.csv`).
- `notebooks/`: Complete PySpark code walking through the Medallion architecture.

## ⚙️ Quickstart Guide
1. Clone this repository or import it into Databricks using **Git Folders**.
2. Open your Databricks workspace and run the pipeline notebook.
3. Explore the resulting Delta tables (`bronze_*`, `silver_*`, `gold_*`) via Catalog Explorer.