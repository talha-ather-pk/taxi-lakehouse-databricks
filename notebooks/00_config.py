# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuration and helpers
# MAGIC
# MAGIC Shared by every notebook (`%run ./00_config`). The same code runs in two places:
# MAGIC
# MAGIC | | Databricks (Free Edition, serverless) | Local (Spark 4 + Delta Lake, `scripts/run_local.py`) |
# MAGIC |---|---|---|
# MAGIC | Tables | Unity Catalog: `workspace.taxi_bronze.*`, `taxi_silver.*`, `taxi_gold.*` | Delta folders under `lakehouse/bronze|silver|gold/` |
# MAGIC | Landing files | Volume `/Volumes/workspace/taxi_bronze/landing/` | `landing/` |
# MAGIC
# MAGIC Only the helpers below know the difference; the pipeline notebooks call `read_table`, `write_table`,
# MAGIC `delta_table`, `table_exists`, `tbl` (for SQL) and `list_landing`.

# COMMAND ----------

import os
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
spark.conf.set("spark.sql.session.timeZone", "UTC")   # trip times are local wall-clock times; keep them as-is

STUDY_START, STUDY_END = "2025-01-01", "2025-06-30"
HOLIDAYS = {"2025-01-01": "New Year's Day", "2025-01-20": "Martin Luther King Jr. Day",
            "2025-02-17": "Presidents' Day", "2025-05-26": "Memorial Day"}
CBD_FEE_START = "2025-03-01"   # Downtown congestion fee start date (policy, public knowledge)

if IS_DATABRICKS:
    CATALOG = "workspace"
    for _s in ("taxi_bronze", "taxi_silver", "taxi_gold"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{_s}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.taxi_bronze.landing")
    LANDING = f"/Volumes/{CATALOG}/taxi_bronze/landing"
else:
    LAKE = os.environ["TAXI_LAKE"].replace("\\", "/")
    LANDING = os.environ["TAXI_LANDING"].replace("\\", "/")


def tbl(layer, name):
    """Table reference usable inside SQL text."""
    if IS_DATABRICKS:
        return f"{CATALOG}.taxi_{layer}.{name}"
    return f"delta.`{LAKE}/{layer}/{name}`"


def _path(layer, name):
    return f"{LAKE}/{layer}/{name}"


def table_exists(layer, name):
    if IS_DATABRICKS:
        return spark.catalog.tableExists(f"{CATALOG}.taxi_{layer}.{name}")
    return DeltaTable.isDeltaTable(spark, _path(layer, name))


def read_table(layer, name, version=None):
    if version is not None:
        return spark.sql(f"SELECT * FROM {tbl(layer, name)} VERSION AS OF {int(version)}")
    if IS_DATABRICKS:
        return spark.table(f"{CATALOG}.taxi_{layer}.{name}")
    return spark.read.format("delta").load(_path(layer, name))


def write_table(df, layer, name, mode="overwrite", partition_by=None, replace_where=None,
                merge_schema=False, overwrite_schema=False):
    w = df.write.format("delta").mode(mode)
    if partition_by:
        w = w.partitionBy(partition_by)
    if replace_where:
        w = w.option("replaceWhere", replace_where)
    if merge_schema:
        w = w.option("mergeSchema", "true")
    if overwrite_schema:
        w = w.option("overwriteSchema", "true")
    if IS_DATABRICKS:
        w.saveAsTable(f"{CATALOG}.taxi_{layer}.{name}")
    else:
        w.save(_path(layer, name))


def delta_table(layer, name):
    if IS_DATABRICKS:
        return DeltaTable.forName(spark, f"{CATALOG}.taxi_{layer}.{name}")
    return DeltaTable.forPath(spark, _path(layer, name))


def list_landing(subdir):
    """[(file_name, full_path, size_bytes)] for files in the landing area, sorted by name."""
    base = f"{LANDING}/{subdir}"
    if IS_DATABRICKS:
        try:
            items = dbutils.fs.ls(base)
        except Exception:
            return []
        return sorted((i.name, i.path, i.size) for i in items if not i.name.endswith("/"))
    if not os.path.isdir(base):
        return []
    return sorted((n, f"{base}/{n}", os.path.getsize(f"{base}/{n}")) for n in os.listdir(base)
                  if os.path.isfile(f"{base}/{n}"))


def last_operation_metrics(layer, name):
    """operationMetrics of the latest commit (rows inserted/updated by a MERGE, etc.)."""
    h = delta_table(layer, name).history(1).select("version", "operation", "operationMetrics").collect()[0]
    return h["version"], h["operation"], dict(h["operationMetrics"] or {})


if "display" not in globals():          # local runs: print instead of the Databricks display widget
    def display(df, n=20):
        df.show(n, truncate=False)

print(f"Running on {'Databricks' if IS_DATABRICKS else 'local Spark ' + spark.version}; landing = {LANDING}")
