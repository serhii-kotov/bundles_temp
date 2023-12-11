# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Nightly Optimizations
# MAGIC This notebook is responsible for managing the optimizations of Delta Tables for customer data lakes.

# COMMAND ----------

# DBTITLE 1,Import Libraries
import importlib
import inspect
import pkgutil

from abc import ABC
from datetime import datetime, date, timedelta
from functools import reduce

from open_commerce_data_pipelines.core.pipelines import bronze, silver, gold
from open_commerce_data_pipelines.core.pipelines.bronze.changes import CdcChangesBronzeStage1PipelineConfig, \
    CdcChangesBronzeStage1Pipeline
from open_commerce_data_pipelines.core.pipelines.bronze.kinesis import KinesisPipeline, KinesisPipelineConfig
from pyspark.sql.functions import col
from sprak.pipelines import DataPipeline
from sprak.pipelines import SingleSourceDataPipelineConfig, SingleSourceDataPipeline, MergeDataPipelineConfig

# COMMAND ----------

# DBTITLE 1,Widget Definitions
dbutils.widgets.dropdown("run_all_dates", "NO", ["YES", "NO"], "Run All Dates?")
dbutils.widgets.text("target_date", "", "Optimization Date")
dbutils.widgets.text("bronze_database_path", "", "Bronze Database Path")
dbutils.widgets.text("gold_database_path", "", "Gold Database Path")
dbutils.widgets.text("output_prefix", "", "Output Prefix")
dbutils.widgets.text("silver_database_path", "", "Silver Database Path")
dbutils.widgets.dropdown("vacuum_only", "NO", ["YES", "NO"], "Vacuum Only?")

# COMMAND ----------

# DBTITLE 1,Disable Delta Check
# MAGIC %sql
# MAGIC
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false;

# COMMAND ----------

# DBTITLE 1,Configuration Parameters
BRONZE_DATABASE_LOCATION = dbutils.widgets.get("bronze_database_path")
if not BRONZE_DATABASE_LOCATION:
    raise RuntimeError("bronze_database_path is required")
print(f"BRONZE_DATABASE_LOCATION={BRONZE_DATABASE_LOCATION}")

GOLD_DATABASE_LOCATION = dbutils.widgets.get("gold_database_path")
if not GOLD_DATABASE_LOCATION:
    raise RuntimeError("gold_database_path is required")
print(f"GOLD_DATABASE_LOCATION={GOLD_DATABASE_LOCATION}")

OUTPUT_PREFIX = dbutils.widgets.get("output_prefix")
print(f"OUTPUT_PREFIX={OUTPUT_PREFIX}")

SILVER_DATABASE_LOCATION = dbutils.widgets.get("silver_database_path")
if not SILVER_DATABASE_LOCATION:
    raise RuntimeError("silver_database_path is required")
print(f"SILVER_DATABASE_LOCATION={SILVER_DATABASE_LOCATION}")

target_date_str = dbutils.widgets.get("target_date")
TARGET_DATE = (datetime.strptime(target_date_str, "%Y-%m-%d").date()
               if target_date_str
               else date.today() - timedelta(days=1))
print(f"TARGET_DATE={TARGET_DATE}")

RUN_ALL_DATES = dbutils.widgets.get("run_all_dates") == "YES"
print(f"RUN_ALL_DATES={RUN_ALL_DATES}")

VACUUM_ONLY = dbutils.widgets.get("vacuum_only") == "YES"
print(f"VACUUM_ONLY={VACUUM_ONLY}")

# COMMAND ----------

# DBTITLE 1,Utility Functions
def recursive_pipeline_search(module):
    """Recursively collect a list of pipeline classes from within the provided module."""

    results = list(map(lambda x: x[1],
                       filter(lambda x: (issubclass(x[1], DataPipeline) and
                                         x[1].output_name is not None and
                                         ABC not in x[1].__bases__),
                              inspect.getmembers(module, inspect.isclass))))

    try:
        modules = list(map(lambda x: importlib.import_module('.' + x.name, module.__name__),
                        pkgutil.iter_modules(module.__path__)))
    except AttributeError:
        modules = []

    return reduce(lambda acc, x: [*acc, *recursive_pipeline_search(x)],
                  modules,
                  results)


# COMMAND ----------

# DBTITLE 1,Bronze-Tier Optimizations
bronze_pipelines = recursive_pipeline_search(bronze)
bronze_single_source_config = SingleSourceDataPipelineConfig(input_database_location=BRONZE_DATABASE_LOCATION,
                                                             output_database_location=BRONZE_DATABASE_LOCATION,
                                                             output_prefix=OUTPUT_PREFIX)
bronze_changes_config = CdcChangesBronzeStage1PipelineConfig(changes_raw_input_region="region",
                                                             input_location="stream",
                                                             output_database_location=BRONZE_DATABASE_LOCATION)
bronze_kinesis_config = KinesisPipelineConfig(changes_raw_input_region="region",
                                              input_location="stream",
                                              output_database_location=BRONZE_DATABASE_LOCATION)
optimized_bronze_locations = []

for pipeline_class in bronze_pipelines:
    print(f"Optimizing: {pipeline_class.__name__}")

    if pipeline_class is CdcChangesBronzeStage1Pipeline:
        pipeline = CdcChangesBronzeStage1Pipeline(bronze_changes_config)
    elif pipeline_class is KinesisPipeline:
        pipeline = KinesisPipeline(bronze_kinesis_config)
    else:
        pipeline = pipeline_class(bronze_single_source_config)

    if pipeline.output_table.location in optimized_bronze_locations:
        continue

    pipeline.output_table.ensure_exists()

    if VACUUM_ONLY:
        pipeline.output_table.vacuum()
    elif not RUN_ALL_DATES and pipeline_class.partition_by and "day_id" in pipeline_class.partition_by:
        pipeline.output_table.optimize(col("day_id") == TARGET_DATE)
    else:
        pipeline.output_table.optimize()

    optimized_bronze_locations.append(pipeline.output_table.location)

# COMMAND ----------

# DBTITLE 1,Silver-Tier Optimizations
silver_pipelines = recursive_pipeline_search(silver)
silver_single_source_config = SingleSourceDataPipelineConfig(input_database_location=BRONZE_DATABASE_LOCATION,
                                                             output_database_location=SILVER_DATABASE_LOCATION,
                                                             output_prefix=OUTPUT_PREFIX)
silver_merge_config = MergeDataPipelineConfig(left_input_database_location=BRONZE_DATABASE_LOCATION,
                                              right_input_database_location=BRONZE_DATABASE_LOCATION,
                                              output_database_location=SILVER_DATABASE_LOCATION,
                                              output_prefix=OUTPUT_PREFIX)
optimized_silver_locations = []

for pipeline_class in silver_pipelines:
    print(f"Optimizing: {pipeline_class.__name__}")

    if issubclass(pipeline_class, SingleSourceDataPipeline):
        pipeline = pipeline_class(silver_single_source_config)
    else:
        pipeline = pipeline_class(silver_merge_config)

    if pipeline.output_table.location in optimized_silver_locations:
        continue

    pipeline.output_table.ensure_exists()

    if VACUUM_ONLY:
        pipeline.output_table.vacuum()
    elif not RUN_ALL_DATES and pipeline_class.partition_by and "day_id" in pipeline_class.partition_by:
        pipeline.output_table.optimize(col("day_id") == TARGET_DATE)
    else:
        pipeline.output_table.optimize()

    optimized_silver_locations.append(pipeline.output_table.location)

# COMMAND ----------

# DBTITLE 1,Gold-Tier Optimizations
gold_pipelines = recursive_pipeline_search(gold)
gold_single_source_config = SingleSourceDataPipelineConfig(input_database_location=SILVER_DATABASE_LOCATION,
                                                           output_database_location=GOLD_DATABASE_LOCATION,
                                                           output_prefix=OUTPUT_PREFIX)
gold_merge_config = MergeDataPipelineConfig(left_input_database_location=SILVER_DATABASE_LOCATION,
                                            right_input_database_location=SILVER_DATABASE_LOCATION,
                                            output_database_location=GOLD_DATABASE_LOCATION,
                                            output_prefix=OUTPUT_PREFIX)
optimized_gold_locations = []

for pipeline_class in gold_pipelines:
    print(f"Optimizing: {pipeline_class.__name__}")

    if issubclass(pipeline_class, SingleSourceDataPipeline):
        pipeline = pipeline_class(gold_single_source_config)
    else:
        pipeline = pipeline_class(gold_merge_config)

    pipeline.output_table.ensure_exists()

    if pipeline.output_table.location in optimized_gold_locations:
        continue

    if VACUUM_ONLY:
        pipeline.output_table.vacuum()
    elif not RUN_ALL_DATES and pipeline_class.partition_by and "day_id" in pipeline_class.partition_by:
        pipeline.output_table.optimize(col("day_id") == TARGET_DATE)
    else:
        pipeline.output_table.optimize()

    optimized_gold_locations.append(pipeline.output_table.location)
