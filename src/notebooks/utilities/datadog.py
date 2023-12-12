# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # DataDog

# COMMAND ----------

# DBTITLE 1,Import Libraries
import boto3

from botocore.config import Config
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v1.api.service_checks_api import ServiceChecksApi
from datadog_api_client.v1.model.service_check import ServiceCheck
from datadog_api_client.v1.model.service_check_status import ServiceCheckStatus
from datadog_api_client.v1.model.service_checks import ServiceChecks
from datadog_api_client.v2.api.metrics_api import MetricsApi

# COMMAND ----------

# DBTITLE 1,Widget Definitions

dbutils.widgets.text("data_lake_env", "", "Environment")
dbutils.widgets.text("data_lake_stage", "", "Stage")

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

# COMMAND ----------

# DBTITLE 1,Constant Definitions
REPL_INST_CRITICAL_STATUSES = ["deleted", "deleting", "failed", "storage-full", "incompatible-credentials",
                               "incompatible-network"]
REPL_INST_WARNING_STATUSES = ["creating", "modifying", "upgrading", "rebooting", "resetting-master-credentials",
                              "maintenance"]
REPL_INST_OK_STATUSES = ["available"]

REPL_TASK_CRITICAL_STATUSES = ["deleting", "failed", "failed-move", "stopped", "stopping"]
REPL_TASK_WARNING_STATUSES = ["moving", "creating", "modifying", "starting"]
REPL_TASK_OK_STATUSES = ["ready", "running", "testing"]

TABLES_WITH_DQM = [f"s3a://oc-{DATA_LAKE_ENV}-{DATA_LAKE_STAGE}-data-lake-bronze/changes-stage-1"]

# COMMAND ----------

# DBTITLE 1,Get Clients/Proxies
aws_config = Config(region_name="us-east-1")
aws_dms_client = boto3.client("dms", config=aws_config)

datadog_configuration = Configuration()
datadog_api_client = ApiClient(datadog_configuration)
datadog_service_check_api_client = ServiceChecksApi(datadog_api_client)
datadog_metrics_api_client = MetricsApi(datadog_api_client)

# COMMAND ----------

# DBTITLE 1,Report Replication Instance Status
repl_inst_response = aws_dms_client.describe_replication_instances()

for repl_inst in repl_inst_response["ReplicationInstances"]:
    if repl_inst["ReplicationInstanceStatus"] in REPL_INST_CRITICAL_STATUSES:
        repl_inst_service_check_status = ServiceCheckStatus.CRITICAL
    elif repl_inst["ReplicationInstanceStatus"] in REPL_INST_OK_STATUSES:
        repl_inst_service_check_status = ServiceCheckStatus.OK
    else:
        repl_inst_service_check_status = ServiceCheckStatus.WARNING

    repl_inst_service_check = ServiceChecks([
        ServiceCheck(
            check="aws.dms.replication_instance.status",
            host_name=repl_inst["ReplicationInstanceArn"].lower(),
            status=repl_inst_service_check_status,
            tags=[
                    f"environment:{DATA_LAKE_ENV}",
                    f"stage:{DATA_LAKE_STAGE}",
                    "component:databricks",
                    "namespace:oc",
                    "managedbydatabricks:true"
            ],
        ),
    ])

    datadog_service_check_api_client.submit_service_check(repl_inst_service_check)

# COMMAND ----------

# DBTITLE 1,Report Replication Instance
repl_tasks_response = aws_dms_client.describe_replication_tasks()

for repl_task in repl_tasks_response["ReplicationTasks"]:
    if repl_task["Status"] in REPL_TASK_CRITICAL_STATUSES:
        repl_task_service_check_status = ServiceCheckStatus.CRITICAL
    elif repl_task["Status"] in REPL_TASK_OK_STATUSES:
        repl_task_service_check_status = ServiceCheckStatus.OK
    else:
        repl_task_service_check_status = ServiceCheckStatus.WARNING

    repl_task_service_check = ServiceChecks([
        ServiceCheck(
            check="aws.dms.replication_task.status",
            host_name=repl_task["ReplicationTaskArn"].lower(),
            status=repl_task_service_check_status,
            tags=[
                    f"environment:{DATA_LAKE_ENV}",
                    f"stage:{DATA_LAKE_STAGE}",
                    "component:databricks",
                    "namespace:oc",
                    "managedbydatabricks:true"
            ],
        ),
    ])

    datadog_service_check_api_client.submit_service_check(repl_task_service_check)
