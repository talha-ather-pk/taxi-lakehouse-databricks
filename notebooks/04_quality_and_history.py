# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Data quality, reconciliation and Delta history
# MAGIC
# MAGIC * **Reconciliation:** every bronze row is accounted for (duplicate, rejected with a reason, superseded, or in silver),
# MAGIC   and gold totals equal silver totals. The notebook fails loudly if they do not.
# MAGIC * **Checked against the truth:** the generator recorded how many bad rows of each kind it injected
# MAGIC   (`true_effects.json`); silver's rejection counts are compared with it.
# MAGIC * **Delta features:** table history (one commit per load), time travel to show what the 13 May vendor
# MAGIC   correction changed, and `OPTIMIZE ... ZORDER BY` to compact small files.
# MAGIC * Writes `taxi_gold.data_quality` and `taxi_gold.pipeline_batches` for the Power BI data-quality page.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import json

ingest = read_table("bronze", "ingest_log")
batches = read_table("silver", "batch_log")
bronze_rows = read_table("bronze", "trips").count()
silver_rows = read_table("silver", "trips").count()
rej = {r.reject_reason: r["count"] for r in read_table("silver", "trips_rejected").groupBy("reject_reason").count().collect()}
b = batches.agg(*[F.sum(c).alias(c) for c in ("rows_in", "exact_duplicates", "rejected", "same_key_duplicates",
                                               "inserted", "updated")]).collect()[0].asDict()
superseded = b["rows_in"] - b["exact_duplicates"] - b["rejected"] - b["same_key_duplicates"] - b["inserted"]
gold = read_table("gold", "daily_kpis").agg(F.sum("trips").alias("t"), F.sum("revenue").alias("r")).collect()[0]
silver_rev = read_table("silver", "trips").agg(F.round(F.sum("total_amount"), 2).alias("r")).collect()[0]["r"]

checks = [
    ("bronze rows = rows in landed files", bronze_rows == ingest.agg(F.sum("rows")).collect()[0][0]),
    ("every bronze row processed by silver", b["rows_in"] == bronze_rows),
    ("silver rows = rows inserted by MERGE", silver_rows == b["inserted"]),
    ("rejected rows = rows in trips_rejected", b["rejected"] == sum(rej.values())),
    ("gold trips = silver trips", gold["t"] == silver_rows),
    ("gold revenue = silver revenue (within daily rounding)", abs(gold["r"] - silver_rev) < 1.0),
]
for name, ok in checks:
    print(("PASS  " if ok else "FAIL  ") + name)
assert all(ok for _, ok in checks), "reconciliation failed"
print(f"\nbronze {bronze_rows:,} = exact duplicates {b['exact_duplicates']:,} + rejected {b['rejected']:,} "
      f"+ same-key duplicates {b['same_key_duplicates']:,} + superseded by corrections {superseded:,} "
      f"+ silver {silver_rows:,}")

# COMMAND ----------

# MAGIC %md ## Rejections vs what was injected

# COMMAND ----------

truth = None
ref = dict((n, p) for n, p, _ in list_landing("reference"))
if "true_effects.json" in ref:
    truth = json.loads(" ".join(r.value for r in spark.read.text(ref["true_effects.json"]).collect()))
    loaded_files = [r.file_name for r in ingest.select("file_name").collect()]
    per_file = [truth["injected_by_file"][f] for f in loaded_files if f in truth["injected_by_file"]]
    injected = {k: sum(d[k] for d in per_file) for k in per_file[0]} if per_file else {}
    rows = [(k, int(rej.get(k, 0)), int(injected[k])) for k in truth["injected_rejects"]]
    rows.append(("exact duplicates", int(b["exact_duplicates"]), int(injected["duplicates"])))
    cmp = spark.createDataFrame(rows, "problem string, found long, injected long")                .withColumn("match", F.col("found") == F.col("injected"))
    display(cmp)
    if set(truth["file_rows"]) <= set(loaded_files):
        exp = truth["expected_silver_rows_after_all_files"]
        print(f"all files loaded: silver rows {silver_rows:,} vs expected {exp:,} -> {'MATCH' if silver_rows == exp else 'DIFFERENT'}")

# COMMAND ----------

# MAGIC %md ## Delta history and time travel

# COMMAND ----------

hist = delta_table("silver", "trips").history().select("version", "timestamp", "operation", "operationMetrics")
display(hist.select("version", "operation",
                    F.col("operationMetrics.numTargetRowsInserted").alias("inserted"),
                    F.col("operationMetrics.numTargetRowsUpdated").alias("updated"),
                    F.col("operationMetrics.numOutputRows").alias("output_rows")).orderBy("version"))

# what did the vendor correction change? compare 13 May before and after the re-send was merged
resend = batches.join(ingest.where(F.col("file_name").contains("resend")).select("batch_id").distinct(), "batch_id")
restatement = None
if resend.count():
    v_after = resend.agg(F.max("silver_version")).collect()[0][0]
    q = "SELECT COUNT(*) AS trips, ROUND(SUM(trip_distance), 1) AS miles, ROUND(SUM(total_amount), 2) AS revenue " \
        "FROM {t} WHERE pickup_date = DATE'2025-05-13' AND vendor_id = 2"
    before = spark.sql(q.format(t=f"{tbl('silver', 'trips')} VERSION AS OF {v_after - 1}")).collect()[0]
    after = spark.sql(q.format(t=f"{tbl('silver', 'trips')} VERSION AS OF {v_after}")).collect()[0]
    restatement = (before, after)
    print(f"13 May, vendor 2 (silver v{v_after - 1} -> v{v_after}): trips {before.trips:,} -> {after.trips:,}, "
          f"miles {before.miles:,} -> {after.miles:,}, revenue ${before.revenue:,.2f} -> ${after.revenue:,.2f} "
          f"(+${after.revenue - before.revenue:,.2f})")

# COMMAND ----------

# MAGIC %md ## Compact small files

# COMMAND ----------

files_before = spark.sql(f"DESCRIBE DETAIL {tbl('silver', 'trips')}").collect()[0]["numFiles"]
spark.sql(f"OPTIMIZE {tbl('silver', 'trips')} ZORDER BY (pickup_date, pu_location_id)")
files_after = spark.sql(f"DESCRIBE DETAIL {tbl('silver', 'trips')}").collect()[0]["numFiles"]
print(f"silver.trips files: {files_before} -> {files_after} after OPTIMIZE ZORDER BY (pickup_date, pu_location_id)")

# COMMAND ----------

# MAGIC %md ## Tables for the Power BI data-quality page

# COMMAND ----------

dq = [("Raw files", "Rows received", bronze_rows), ("Bronze", "Rows landed", bronze_rows),
      ("Silver", "Exact duplicates removed", b["exact_duplicates"]),
      ("Silver", "Same trip sent twice (older copy dropped)", b["same_key_duplicates"]),
      ("Silver", "Replaced by vendor corrections", superseded)]
dq += [("Silver", f"Rejected: {k.replace('_', ' ')}", v) for k, v in sorted(rej.items(), key=lambda x: -x[1])]
dq += [("Silver", "Clean trips", silver_rows), ("Gold", "Trips in gold tables", gold["t"])]
soft = read_table("silver", "trips").agg(F.sum((~F.col("zone_known")).cast("int")).alias("u"),
                                         F.sum(F.col("passenger_count_missing").cast("int")).alias("p")).collect()[0]
dq += [("Silver (kept, flagged)", "Unknown zone ID", soft.u), ("Silver (kept, flagged)", "Missing passenger count", soft.p)]
dq_df = spark.createDataFrame([(i + 1, s, m, int(v)) for i, (s, m, v) in enumerate(dq)],
                              "step int, layer string, metric string, rows long")
if truth:
    inj = {f"Rejected: {k.replace('_', ' ')}": injected[k] for k in truth["injected_rejects"]}
    inj["Exact duplicates removed"] = injected["duplicates"]
    inj["Unknown zone ID"] = injected["unknown_zone"]
    inj["Missing passenger count"] = injected["missing_passenger_count"]
    inj_df = spark.createDataFrame(list(inj.items()), "metric string, injected long")
    dq_df = dq_df.join(inj_df, "metric", "left")
write_table(dq_df.orderBy("step"), "gold", "data_quality", overwrite_schema=True)

pb = (batches.join(ingest.groupBy("batch_id").agg(F.concat_ws(", ", F.sort_array(F.collect_list("file_name"))).alias("files"),
                                                  F.concat_ws(" | ", F.sort_array(F.collect_list(F.concat("file_name", F.lit(": "), "schema_notes")))).alias("schema_notes")), "batch_id", "left")
      .select("batch_id", "files", "rows_in", "exact_duplicates", "rejected", "same_key_duplicates", "inserted", "updated",
              "silver_version", "months_touched", "processed_at", "schema_notes"))
write_table(pb, "gold", "pipeline_batches", overwrite_schema=True)
display(read_table("gold", "data_quality").orderBy("step"))
