# Databricks notebook source
# MAGIC %md
# MAGIC ##Table 1 — gold_sales_by_outlet_type

# COMMAND ----------

# correct name
silver_df = spark.table("silver_big_mart_sales")
print("Silver table loaded:", silver_df.count(), "rows")

# COMMAND ----------

from pyspark.sql import functions as f

gold_outlet_type = silver_df.groupBy("Outlet_Type").agg(
    f.round(f.sum("Item_Outlet_Sales"), 2).alias("total_sales"),
    f.count("Item_Identifier").alias("total_items_sold"),
    f.round(f.avg("Item_Outlet_Sales"), 2).alias("avg_sales_per_item")
)

gold_outlet_type = gold_outlet_type.orderBy(f.desc("total_sales"))

gold_outlet_type.write.format("Delta")

print("gold_sales_by_outlet_type created")
gold_outlet_type.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Table 2 — gold_sales_by_item_category

# COMMAND ----------

gold_item_category = silver_df.groupBy("Item_Type").agg(
    f.round(f.sum("Item_Outlet_Sales"), 2).alias("total_sales"),
    f.count("Item_Identifier").alias("total_items_sold"),
    f.round(f.avg("Item_Outlet_Sales"), 2).alias("avg_sales_per_item")
)
gold_item_category  = gold_item_category.orderBy(f.desc("total_sales"))
gold_item_category.write.format("Delta").mode("overwrite").saveAsTable("gold_sales_by_item_category")

print("gold_sales_by_item_category created")
gold_item_category.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##Table 3 — gold_top_outlets (with ranking)
# MAGIC

# COMMAND ----------

from pyspark.sql import Window

gold_outlets_base = silver_df.groupBy("Outlet_Identifier", "Outlet_Type", "Outlet_Location_Type").agg(
    f.round(f.sum("Item_Outlet_Sales"), 2).alias("total_sales"),
    f.count("Item_Identifier").alias("total_items_sold"))
window_spec = Window.orderBy(f.desc("total_sales"))

gold_top_outlets = gold_outlets_base.withColumn("sales_rank", f.rank().over(window_spec))
gold_top_outlets = gold_top_outlets.orderBy("sales_rank")
gold_top_outlets.write.format("delta").mode("overwrite").saveAsTable("gold_top_outlets")

print("gold_top_outlets created")
gold_top_outlets.display()

# COMMAND ----------

