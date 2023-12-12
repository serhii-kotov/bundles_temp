# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # DataDog

# COMMAND ----------

# DBTITLE 1,Import Libraries
from datetime import datetime, date, timedelta

from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.metrics_api import MetricsApi
from datadog_api_client.v2.model.metric_intake_type import MetricIntakeType
from datadog_api_client.v2.model.metric_payload import MetricPayload
from datadog_api_client.v2.model.metric_point import MetricPoint
from datadog_api_client.v2.model.metric_resource import MetricResource
from datadog_api_client.v2.model.metric_series import MetricSeries
# pylint: disable=redefined-builtin
from pyspark.sql.functions import array_max, get_json_object, round, sum

# COMMAND ----------

# DBTITLE 1,Widget Definitions
dbutils.widgets.text("data_lake_env", "", "Environment")
dbutils.widgets.text("data_lake_stage", "", "Stage")
dbutils.widgets.text("target_date", "", "Optimization Date")

# COMMAND ----------

# DBTITLE 1,Configuration Parameters
DATA_LAKE_ENV = dbutils.widgets.get("data_lake_env")
if not DATA_LAKE_ENV:
    raise RuntimeError("DATA_LAKE_ENV is required")
print(f"DATA_LAKE_ENV={DATA_LAKE_ENV}")

DATA_LAKE_STAGE = dbutils.widgets.get("data_lake_stage")
if not DATA_LAKE_STAGE:
    raise RuntimeError("DATA_LAKE_STAGE is required")
print(f"DATA_LAKE_STAGE={DATA_LAKE_STAGE}")

target_date_str = dbutils.widgets.get("target_date")
TARGET_DATE = (datetime.strptime(target_date_str, "%Y-%m-%d").date()
               if target_date_str
               else date.today() - timedelta(days=1))
print(f"TARGET_DATE={TARGET_DATE}")

OVERWATCH_BUCKET = f"s3a://oc-{DATA_LAKE_ENV}-{DATA_LAKE_STAGE}-data-lake-overwatch"
print(f"OVERWATCH_BUCKET={OVERWATCH_BUCKET}")

# COMMAND ----------

# DBTITLE 1,Get Clients/Proxies
datadog_configuration = Configuration()
datadog_api_client = ApiClient(datadog_configuration)
datadog_metrics_api_client = MetricsApi(datadog_api_client)

# COMMAND ----------

# DBTITLE 1,Report Databricks-Only Costs
costs = (spark
         .read
         .format("delta")
         .load(f"{OVERWATCH_BUCKET}/global_share/jobruncostpotentialfact_gold/")
         .withColumn("day_id", array_max("running_days"))
         .where(f"day_id = '{TARGET_DATE}'")
         .withColumn("environment", get_json_object("cluster_tags", "$.Environment"))
         .withColumn("stage", get_json_object("cluster_tags", "$.Stage"))
         .groupBy("day_id", "job_id", "job_name", "task_key", "environment", "stage")
         .agg(round(sum("total_dbu_cost"), 2).alias("total_cost"))
         .collect())

cost_metric = MetricPayload(series=[MetricSeries(metric="databricks.tasks.cost_per_day",
                                                 points=[
                                                     MetricPoint(
                                                         timestamp=int(datetime.utcnow().timestamp()),
                                                         value=cost.total_cost,
                                                     ),
                                                 ],
                                                 resources=[
                                                     MetricResource(
                                                         name=cost.job_name,
                                                         type="job_name",
                                                     ),
                                                     MetricResource(
                                                         name=cost.task_key,
                                                         type="task_key",
                                                     ),
                                                 ],
                                                 tags=[
                                                     f"environment:{DATA_LAKE_ENV}",
                                                     f"stage:{DATA_LAKE_STAGE}",
                                                     "component:databricks",
                                                     "namespace:oc",
                                                     "managedbydatabricks:true"
                                                 ],
                                                 type=MetricIntakeType.GAUGE,
                                                 unit="dollar")
                                    for cost in costs])

datadog_metrics_api_client.submit_metrics(body=cost_metric)
