# Databricks notebook source
# MAGIC %md
# MAGIC # BigMart Sales Pipeline - Silver Layer
# MAGIC ## Purpose: Clean and transform Bronze data
# MAGIC ### Source: bronze_big_mart_sales (Delta table)
# MAGIC ### Destination: silver_big_mart_sales (Delta table)
# MAGIC

# COMMAND ----------

df_silver = spark.table("bronze_big_mart_sales")
print(f"Rows loaded from Bronze:{df_silver.count()}")
df_silver.display(5)

# COMMAND ----------

from pyspark.sql.functions import col, sum, isnan, when
null_count = df_silver.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df_silver.columns])
null_count.display()

# COMMAND ----------

from pyspark.sql.functions import mean
mean_weight  = df_silver.select(mean(col("Item_Weight"))).collect()[0][0]
print(f"Mean Item_Weight: {mean_weight}")


# COMMAND ----------

from pyspark.sql.functions import when, lit, col

# Hardcode the value directly
mean_val = 12.86

# Fill nulls
df_silver = df_silver.withColumn(
    "Item_Weight",
    when(col("Item_Weight").isNull(), lit(mean_val))
    .otherwise(col("Item_Weight"))
)

# Verify nulls are gone
remaining_nulls = df_silver.filter(col("Item_Weight").isNull()).count()
print(f"Remaining nulls in Item_Weight: {remaining_nulls}")

# COMMAND ----------

from pyspark.sql.functions import mode
# Step 1: Find mode of Outlet_Size
mode_outlet_size  = df_silver.select(mode(col("Outlet_Size"))).collect()[0][0]
print(f"Mode Outlet_Size: {mode_outlet_size}")

df_silver = df_silver.withColumn("Outlet_Size", when(col("Outlet_Size").isNull(), lit(mode_outlet_size)).otherwise(col("Outlet_Size")))

# Step 3: Verify
remaining_nulls = df_silver.filter(col("Outlet_Size").isNull()).count()
print(f"Remaining nulls in Outlet_Size: {remaining_nulls}")


# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col, when, lit

# Standardize Item_Fat_Content values
df_silver= df_silver.withColumn("Item_Fat_Content", regexp_replace(col("Item_Fat_Content"), "^(reg|Reg)$", "Regular"))


df_silver = df_silver.withColumn("Item_Fat_Content",
    regexp_replace(col("Item_Fat_Content"), "^(LF|low fat)$", "Low Fat")
)

# Verify distinct values
df_silver.select("Item_Fat_Content").distinct().display()

# COMMAND ----------

# Save as Silver Delta table
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_big_mart_sales")

# Verify
print(f"Silver table rows: {spark.table('silver_big_mart_sales').count()}")
print("Silver layer complete!")

# COMMAND ----------

