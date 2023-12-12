# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Checkpoint Renaming
# MAGIC This notebook is responsible for renaming checkpoint folder paths so that we are no longer using

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Widget Definitions

# COMMAND ----------

dbutils.widgets.text("included_paths", "", "Search Paths")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration Parameters

# COMMAND ----------

INCLUDED_PATHS: str = dbutils.widgets.get("included_paths")
if not INCLUDED_PATHS:
    raise RuntimeError("included_paths is required")
print(f"INCLUDED_PATHS={INCLUDED_PATHS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename Checkpoints

# COMMAND ----------

for path in INCLUDED_PATHS.split(","):
    print(f"Moving checkpoints in: {path}")

    checkpoint_paths = [x.path
                        for x in dbutils.fs.ls(path)
                        if "checkpoint" in x.path and "_checkpoint" not in x.path]
    if len(checkpoint_paths) > 0:
        for checkpoint_path in checkpoint_paths:
            dbutils.fs.mv(checkpoint_path,
                          str(checkpoint_path).replace("checkpoint", "_checkpoint"),
                          recurse=True)
