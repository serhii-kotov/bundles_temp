# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Altria UPC List - Cigarettes (Bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Libraries

# COMMAND ----------

from datetime import datetime

from open_commerce_data_pipelines.altria.pipelines.bronze.product import UpcListProductImportPipelineConfig, \
    UpcListCigaretteProductImportPipeline

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Widget Definitions

# COMMAND ----------

dbutils.widgets.text("bronze_database_path", "", "Bronze-Tier Database Path")
dbutils.widgets.text("input_path", "", "UPC List File Path")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration Parameters

# COMMAND ----------

BRONZE_DATABASE_LOCATION = dbutils.widgets.get("bronze_database_path")
if not BRONZE_DATABASE_LOCATION:
    raise RuntimeError("bronze_database_path is required")
print(f"BRONZE_DATABASE_LOCATION={BRONZE_DATABASE_LOCATION}")

INPUT_LOCATION = dbutils.widgets.get("input_path")
if not INPUT_LOCATION:
    raise RuntimeError("input_path is required")
print(f"INPUT_LOCATION={INPUT_LOCATION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Capture File Metadata

# COMMAND ----------

file_info = dbutils.fs.ls(INPUT_LOCATION)[0]
INPUT_FILE_UPDATED_AT = datetime.fromtimestamp(file_info.modificationTime / 1000)
print(f"INPUT_FILE_UPDATED_AT={INPUT_FILE_UPDATED_AT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Invocation

# COMMAND ----------

config = UpcListProductImportPipelineConfig(input_file_updated_at=INPUT_FILE_UPDATED_AT,
                                            input_location=INPUT_LOCATION,
                                            output_database_location=BRONZE_DATABASE_LOCATION)

pipeline = UpcListCigaretteProductImportPipeline(config)

# COMMAND ----------

pipeline.run()
