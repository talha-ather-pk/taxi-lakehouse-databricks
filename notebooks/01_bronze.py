# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: land raw files as Delta, incrementally
# MAGIC
# MAGIC * Picks up **only files not loaded before** (tracked in `taxi_bronze.ingest_log`), so the job can run
# MAGIC   on a schedule and each monthly file is read exactly once.
# MAGIC * Keeps every row as delivered, including bad ones (they are filtered in silver, where each rejection is counted).
# MAGIC * Handles the feed's **schema drift**: column names are conformed to snake_case (`Airport_fee` in newer files
# MAGIC   becomes `airport_fee`), types are widened to one bronze schema, columns that did not exist yet
# MAGIC   (`cbd_congestion_fee` before March) are added as nulls, and every change is written to the log.
# MAGIC * Adds lineage columns: `_source_file`, `_batch_id`, `_ingested_at`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import datetime as dt

RENAME = {"vendorid": "vendor_id", "tpep_pickup_datetime": "pickup_datetime",
          "tpep_dropoff_datetime": "dropoff_datetime", "ratecodeid": "ratecode_id",
          "pulocationid": "pu_location_id", "dolocationid": "do_location_id"}
BRONZE_SCHEMA = {
    "vendor_id": "int", "trip_id": "string", "pickup_datetime": "timestamp", "dropoff_datetime": "timestamp",
    "passenger_count": "double", "trip_distance": "double", "ratecode_id": "int", "store_and_fwd_flag": "string",
    "pu_location_id": "int", "do_location_id": "int", "payment_type": "int", "fare_amount": "double",
    "extra": "double", "mta_tax": "double", "tip_amount": "double", "tolls_amount": "double",
    "improvement_surcharge": "double", "total_amount": "double", "airport_fee": "double",
    "cbd_congestion_fee": "double",
}


def conform(raw_df):
    """Return (dataframe in the bronze schema, list of schema notes)."""
    notes, cols = [], []
    src = {c.lower(): (c, t) for c, t in raw_df.dtypes}
    for c, t in raw_df.dtypes:
        target = RENAME.get(c.lower(), c.lower())
        if c != target and c.lower() not in RENAME and c != c.lower():
            notes.append(f"renamed {c} -> {target}")
    for target, ttype in BRONZE_SCHEMA.items():
        match = [k for k in src if RENAME.get(k, k) == target]
        if not match:
            cols.append(F.lit(None).cast(ttype).alias(target))
            notes.append(f"missing {target} (added as null)")
            continue
        name, stype = src[match[0]]
        family = lambda t: "int" if t in ("int", "bigint", "smallint", "tinyint") else ("ts" if t.startswith("timestamp") else t)
        if family(stype) != family(ttype):           # integer width changes are routine and not logged
            notes.append(f"cast {target} {stype} -> {ttype}")
        cols.append(F.col(f"`{name}`").cast(ttype).alias(target))
    extra = [c for c in src if RENAME.get(c, c) not in BRONZE_SCHEMA]
    for k in extra:                                   # unexpected new columns are kept, not dropped
        cols.append(F.col(f"`{src[k][0]}`").alias(k))
        notes.append(f"new column {k} kept")
    return raw_df.select(*cols), notes

# COMMAND ----------

# MAGIC %md ## Which files are new?

# COMMAND ----------

landed = [f for f in list_landing("trips") if f[0].endswith(".parquet")]
loaded = set()
if table_exists("bronze", "ingest_log"):
    loaded = {r.file_name for r in read_table("bronze", "ingest_log").select("file_name").collect()}
new_files = [f for f in landed if f[0] not in loaded]
batch_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S")
print(f"{len(landed)} files in landing, {len(loaded)} already loaded, {len(new_files)} new -> batch {batch_id}")
for f in new_files:
    print("  new:", f[0], f"{f[2] / 1e6:.1f} MB")

# COMMAND ----------

# MAGIC %md ## Load each new file into `taxi_bronze.trips`

# COMMAND ----------

log_rows = []
for name, path, size in new_files:
    raw = spark.read.parquet(path)
    n = raw.count()                                   # from Parquet footers: cheap
    df, notes = conform(raw)
    df = (df.withColumn("_source_file", F.lit(name))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ingested_at", F.current_timestamp()))
    write_table(df, "bronze", "trips", mode="append", merge_schema=True)
    log_rows.append((name, int(size), n, batch_id, "; ".join(notes) or "none",
                     ", ".join(raw.columns)))
    print(f"{name}: {n:,} rows | schema: {'; '.join(notes) or 'as expected'}")

if log_rows:
    log_df = spark.createDataFrame(log_rows, "file_name string, file_bytes long, rows long, batch_id string, "
                                             "schema_notes string, source_columns string") \
                  .withColumn("ingested_at", F.current_timestamp())
    write_table(log_df, "bronze", "ingest_log", mode="append")
else:
    print("Nothing new to load.")

# COMMAND ----------

# MAGIC %md ## Reference data (small, reloaded every run)

# COMMAND ----------

ref = dict((n, p) for n, p, _ in list_landing("reference"))
zones = spark.read.option("header", True).option("inferSchema", True).csv(ref["zones.csv"])
weather = spark.read.option("header", True).option("inferSchema", True).csv(ref["weather_daily.csv"])
write_table(zones, "bronze", "zones", overwrite_schema=True)
write_table(weather, "bronze", "weather", overwrite_schema=True)
print(f"zones: {zones.count()} rows, weather: {weather.count()} days")

# COMMAND ----------

display(spark.sql(f"""
    SELECT file_name, rows, batch_id, schema_notes
    FROM {tbl('bronze', 'ingest_log')} ORDER BY file_name"""))
display(spark.sql(f"SELECT COUNT(*) AS bronze_rows, COUNT(DISTINCT _source_file) AS files FROM {tbl('bronze', 'trips')}"))
