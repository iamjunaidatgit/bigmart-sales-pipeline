# Databricks notebook source
# BigMart Sales Pipeline - Orchestration
# Runs Bronze → Silver → Gold in sequence

import time
from datetime import datetime

# COMMAND ----------

BRONZE_NOTEBOOK = "./01_Bronze_Ingestion"
SILVER_NOTEBOOK = "./02_Silver_Transformation"
GOLD_NOTEBOOK   = "./03_Gold_Aggregation"

TIMEOUT = 600  # 10 minutes per notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ##orchestration logic

# COMMAND ----------

def run_notebook(name, path):
    print(f"Starting {name}... [{datetime.now().strftime('%H:%M:%S')}]")
    try:
        result = dbutils.notebook.run(path, TIMEOUT, {})
        print(f"✅ {name} completed successfully")
        return True
    except Exception as e:
        print(f"❌ {name} FAILED: {str(e)}")
        return False

# COMMAND ----------

print("=" * 50)
print("BigMart Pipeline Started")
print(f"Time: {datetime.now()}")
print("=" * 50)

start_time = time.time()

# Run in sequence — stop if any layer fails
if run_notebook("Bronze Ingestion", BRONZE_NOTEBOOK):
    if run_notebook("Silver Transformation", SILVER_NOTEBOOK):
        if run_notebook("Gold Aggregation", GOLD_NOTEBOOK):
            print("\n✅ Full Pipeline Completed Successfully!")
        
end_time = time.time()
print(f"\nTotal time: {round(end_time - start_time, 2)} seconds")

# COMMAND ----------

