# Databricks notebook source
# MAGIC %md
# MAGIC # BigMart Sales Pipeline - Bronze Layer
# MAGIC ## Purpose: Ingest raw CSV data and save as Delta table
# MAGIC ### Source: Big Mart Sales CSV
# MAGIC ### Destination: bronze_big_mart_sales (Delta table)

# COMMAND ----------

# Read from existing Delta table
df_bronze = spark.table("workspace.default.big_mart_sales")

# Show shape and preview
print(f"Rows: {df_bronze.count()}, Columns: {len(df_bronze.columns)}")
print(f"Columns: {df_bronze.columns}")
df_bronze.display(5)

# COMMAND ----------

df_bronze.write.format("delta")\
    .mode("overwrite")\
        .saveAsTable("bronze_big_mart_sales")

print("Bronze table saved successfully!")

# COMMAND ----------

df_verify = spark.table("bronze_big_mart_sales")
print(f"Bronze table row count:{df_verify.count()}")
df_verify.display(3)

# COMMAND ----------

