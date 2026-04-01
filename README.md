# BigMart Sales Pipeline 🛒

An end-to-end Data Engineering pipeline built on Databricks using 
PySpark, Delta Lake and Medallion Architecture.

## Architecture
Raw CSV → Bronze → Silver → Gold → Pipeline Orchestration

## Layers
- **Bronze** - Ingests raw BigMart CSV data into Delta table as-is
- **Silver** - Cleans data: handles nulls, fixes data types, 
  standardizes values
- **Gold** - Aggregates business metrics:
  - Sales by Outlet Type
  - Sales by Item Category
  - Top Outlets ranked by Total Sales
- **Orchestration** - Runs all 3 layers in sequence with 
  error handling using dbutils.notebook.run()

## Tech Stack
- Python
- PySpark
- Databricks Community Edition
- Delta Lake
- SQL

## Key Concepts Used
- Medallion Architecture (Bronze/Silver/Gold)
- PySpark DataFrames and Aggregations
- Window Functions and Ranking
- Delta Lake ACID transactions
- Pipeline Orchestration with error handling
