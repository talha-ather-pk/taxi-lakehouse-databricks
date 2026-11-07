# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: validate, deduplicate, enrich, MERGE
# MAGIC
# MAGIC Processes only bronze batches that silver has not seen (`taxi_silver.batch_log`).
# MAGIC
# MAGIC 1. **Validate.** Each row gets the first rule it breaks; rejected rows go to `taxi_silver.trips_rejected`
# MAGIC    with the reason, so nothing disappears silently.
# MAGIC 2. **Deduplicate.** Exact re-transmitted records are dropped; if a trip arrives twice with different values,
# MAGIC    the most recently ingested version wins.
# MAGIC 3. **Fix and flag.** Unknown zone IDs are kept and labelled "Unknown"; missing or zero passenger counts become
# MAGIC    null with a flag. Derived columns (duration, speed, tip %, date parts) and zone names are added.
# MAGIC 4. **MERGE** into `taxi_silver.trips` on `trip_id`: new trips are inserted, re-sent corrected trips update the
# MAGIC    existing row. Re-running a batch changes nothing (idempotent).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

done = set()
if table_exists("silver", "batch_log"):
    done = {r.batch_id for r in read_table("silver", "batch_log").select("batch_id").collect()}
bronze = read_table("bronze", "trips")
batches = sorted(r._batch_id for r in bronze.select("_batch_id").distinct().collect() if r._batch_id not in done)
print(f"bronze batches not yet in silver: {batches or 'none'}")

# COMMAND ----------

# MAGIC %md ## Reference tables

# COMMAND ----------

zones = (read_table("bronze", "zones")
         .select(F.col("LocationID").cast("int").alias("location_id"), F.col("Borough").alias("borough"),
                 F.col("Zone").alias("zone"), "service_zone", "zone_type"))
write_table(zones, "silver", "zones", overwrite_schema=True)
weather = (read_table("bronze", "weather")
           .select(F.to_date("date").alias("date"), F.col("precip_mm").cast("double"), F.col("temp_c").cast("double"))
           .withColumn("is_rainy", F.col("precip_mm") >= 2.0))
write_table(weather, "silver", "weather", overwrite_schema=True)
known_ids = [r.location_id for r in zones.where("borough <> 'Unknown'").select("location_id").collect()]

# COMMAND ----------

# MAGIC %md ## Validation rules (first failing rule is the reason)

# COMMAND ----------

duration_min = (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0
speed_mph = F.col("trip_distance") / (duration_min / 60.0)
RULES = [  # (reason, condition that makes the row INVALID)
    ("out_of_period", F.col("pickup_datetime").isNull()
     | ~F.to_date("pickup_datetime").between(F.lit(STUDY_START), F.lit(STUDY_END))),
    ("bad_timestamps", F.col("dropoff_datetime").isNull() | (F.col("dropoff_datetime") <= F.col("pickup_datetime"))),
    ("negative_amount", (F.col("fare_amount") < 0) | (F.col("total_amount") < 0)),
    ("zero_distance", F.col("trip_distance").isNull() | (F.col("trip_distance") <= 0)),
    ("implausible_speed", (duration_min > 360) | (speed_mph > 80) | (F.col("trip_distance") > 100)),
    ("fare_outlier", F.col("fare_amount") > 400),
]
reason = F.lit(None).cast("string")
for name, cond in reversed(RULES):
    reason = F.when(cond, F.lit(name)).otherwise(reason)

PAYMENT = {1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute"}
pay_map = F.create_map(*[x for k, v in PAYMENT.items() for x in (F.lit(k), F.lit(v))])
BUSINESS_COLS = [c for c in bronze.columns if not c.startswith("_")]


def enrich(df):
    pu = zones.select(*[F.col(c).alias(f"pu_{c}") for c in ("location_id", "borough", "zone", "service_zone")])
    do = zones.select(*[F.col(c).alias(f"do_{c}") for c in ("location_id", "borough", "zone", "service_zone")])
    out = (df.join(F.broadcast(pu), "pu_location_id", "left")
             .join(F.broadcast(do), "do_location_id", "left")
             .withColumn("pu_borough", F.coalesce("pu_borough", F.lit("Unknown")))
             .withColumn("do_borough", F.coalesce("do_borough", F.lit("Unknown")))
             .withColumn("pu_zone", F.coalesce("pu_zone", F.lit("Unknown")))
             .withColumn("do_zone", F.coalesce("do_zone", F.lit("Unknown")))
             .withColumn("zone_known", F.col("pu_location_id").isin(known_ids) & F.col("do_location_id").isin(known_ids))
             .withColumn("passenger_count_missing", F.col("passenger_count").isNull() | (F.col("passenger_count") <= 0))
             .withColumn("passenger_count", F.when(F.col("passenger_count") > 0, F.col("passenger_count").cast("int")))
             .withColumn("airport_fee", F.coalesce("airport_fee", F.lit(0.0)))
             .withColumn("cbd_congestion_fee", F.coalesce("cbd_congestion_fee", F.lit(0.0)))
             .withColumn("pickup_date", F.to_date("pickup_datetime"))
             .withColumn("pickup_month", F.date_format("pickup_datetime", "yyyy-MM"))
             .withColumn("pickup_hour", F.hour("pickup_datetime"))
             .withColumn("day_of_week", F.date_format("pickup_datetime", "E"))
             .withColumn("is_weekend", F.dayofweek("pickup_datetime").isin(1, 7))
             .withColumn("duration_min", F.round(duration_min, 2))
             .withColumn("speed_mph", F.round(speed_mph, 2))
             .withColumn("payment_desc", F.coalesce(pay_map[F.col("payment_type")], F.lit("Unknown")))
             .withColumn("tip_pct", F.when(F.col("payment_type") == 1, F.round(F.col("tip_amount") / F.col("fare_amount"), 4)))
             .withColumn("is_airport_trip", (F.col("pu_borough") == "Airports") | (F.col("do_borough") == "Airports"))
             .withColumn("touches_downtown", (F.col("pu_borough") == "Downtown") | (F.col("do_borough") == "Downtown")))
    return out

# COMMAND ----------

# MAGIC %md ## Process each new batch

# COMMAND ----------

from pyspark.sql.window import Window

for b in batches:
    batch = bronze.where(F.col("_batch_id") == b)
    rows_in = batch.count()
    # 1. exact duplicate records (same values in every business column)
    distinct = batch.dropDuplicates(BUSINESS_COLS)
    exact_dups = rows_in - distinct.count()
    # 2. validation
    checked = distinct.withColumn("reject_reason", reason)
    rejected = checked.where(F.col("reject_reason").isNotNull())
    valid = checked.where(F.col("reject_reason").isNull()).drop("reject_reason")
    write_table(rejected.withColumn("_rejected_at", F.current_timestamp()), "silver", "trips_rejected",
                mode="append", merge_schema=True)
    n_rejected = read_table("silver", "trips_rejected").where(F.col("_batch_id") == b).count()
    # 3. same trip_id twice in the batch with different values: keep the latest ingested
    w = Window.partitionBy("trip_id").orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
    latest = valid.withColumn("_rn", F.row_number().over(w))
    key_dups = latest.where("_rn > 1").count()
    clean = enrich(latest.where("_rn = 1").drop("_rn"))
    # 4. MERGE into silver
    if not table_exists("silver", "trips"):
        write_table(clean.limit(0), "silver", "trips", partition_by="pickup_month")
    (delta_table("silver", "trips").alias("t")
        .merge(clean.alias("s"), "t.trip_id = s.trip_id")
        .whenMatchedUpdateAll(condition="s._ingested_at > t._ingested_at")
        .whenNotMatchedInsertAll()
        .execute())
    version, op, m = last_operation_metrics("silver", "trips")
    inserted, updated = int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0))
    months = sorted(r.pickup_month for r in clean.select("pickup_month").distinct().collect())
    log = spark.createDataFrame(
        [(b, rows_in, exact_dups, n_rejected, key_dups, inserted, updated, int(version), ",".join(months))],
        "batch_id string, rows_in long, exact_duplicates long, rejected long, same_key_duplicates long, "
        "inserted long, updated long, silver_version long, months_touched string"
    ).withColumn("processed_at", F.current_timestamp())
    write_table(log, "silver", "batch_log", mode="append")
    print(f"batch {b}: {rows_in:,} in | {exact_dups:,} exact duplicates | {n_rejected:,} rejected | "
          f"{inserted:,} inserted | {updated:,} updated (silver v{version}) | months {months}")

# COMMAND ----------

display(spark.sql(f"""
    SELECT reject_reason, COUNT(*) AS rows
    FROM {tbl('silver', 'trips_rejected')} GROUP BY reject_reason ORDER BY rows DESC"""))
display(spark.sql(f"SELECT COUNT(*) AS silver_trips, MIN(pickup_date) AS first_day, MAX(pickup_date) AS last_day "
                  f"FROM {tbl('silver', 'trips')}"))
