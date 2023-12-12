# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Data Lake Reset Utility

# COMMAND ----------

# DBTITLE 1,Widget Definitions
dbutils.widgets.text("s3_prefix", "", "S3 Prefix")
dbutils.widgets.text("databases", "", "Databases")
dbutils.widgets.text("output_prefix", "", "Output Prefix")

# COMMAND ----------

# DBTITLE 1,Configuration Parameters
S3_PREFIX = dbutils.widgets.get("s3_prefix")
if not S3_PREFIX:
    raise RuntimeError("s3_prefix is required")
print(f"S3_PREFIX={S3_PREFIX}")

DATABASES: str = dbutils.widgets.get("databases")
if not DATABASES:
    raise RuntimeError("databases is required")
print(f"DATABASES={DATABASES}")

OUTPUT_PREFIX = dbutils.widgets.get("output_prefix")
print(f"OUTPUT_PREFIX={OUTPUT_PREFIX}")

# COMMAND ----------

# DBTITLE 1,Remove Files
for database in DATABASES.split(","):
    paths = dbutils.fs.ls(f"{S3_PREFIX}-{database}/")

    for path in paths:
        if "changes-stage-1" in path.path and "quarantine" not in path.path:
            continue

        if OUTPUT_PREFIX and not path.name.startswith(OUTPUT_PREFIX):
            continue

        print(f"Removing {path.path}")
        dbutils.fs.rm(path.path, True)
