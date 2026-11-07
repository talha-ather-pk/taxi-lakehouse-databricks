# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: business tables
# MAGIC
# MAGIC Rebuilds **only the months touched by new silver batches** (Delta `replaceWhere` on `pickup_month`), so a
# MAGIC new monthly file costs one month of compute, not the whole history. A full rebuild runs the first time.
# MAGIC
# MAGIC | Table | Grain | Used for |
# MAGIC |---|---|---|
# MAGIC | `daily_kpis` | day | trips, revenue, fares, speed, tips, weather, holidays |
# MAGIC | `zone_daily` | day × pick-up zone | zone revenue and ranking |
# MAGIC | `hourly_demand` | month × weekday/weekend × hour | demand curve, speed by hour |
# MAGIC | `payment_summary` | month × payment type | fares and tips by payment type |
# MAGIC | `borough_flows` | month × pick-up borough × drop-off borough | where trips go |
# MAGIC | `downtown_daily` | day × (touches Downtown or not) | congestion-fee analysis |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

log = read_table("silver", "batch_log")
built = set()
if table_exists("gold", "build_log"):
    built = {r.batch_id for r in read_table("gold", "build_log").select("batch_id").collect()}
pending = log.where(~F.col("batch_id").isin(list(built) or [""])).collect()
months = sorted({m for r in pending for m in (r.months_touched or "").split(",") if m})
full_rebuild = not table_exists("gold", "daily_kpis")
print(f"silver batches not yet in gold: {[r.batch_id for r in pending] or 'none'} -> months to rebuild: {months or 'none'}"
      f"{' (first run: full build)' if full_rebuild else ''}")

# COMMAND ----------

silver = read_table("silver", "trips")
if not full_rebuild:
    silver = silver.where(F.col("pickup_month").isin(months or [""]))
weather = read_table("silver", "weather")
hol = spark.createDataFrame(list(HOLIDAYS.items()), "date string, holiday string").select(F.to_date("date").alias("date"), "holiday")

card = F.col("payment_type") == 1
money = dict(trips=F.count("*"), revenue=F.round(F.sum("total_amount"), 2), fare_revenue=F.round(F.sum("fare_amount"), 2),
             tips=F.round(F.sum("tip_amount"), 2))
# additive columns, so averages can be recomputed exactly at any level (e.g. in Power BI)
sums = dict(distance_mi=F.round(F.sum("trip_distance"), 2), duration_min=F.round(F.sum("duration_min"), 2),
            card_trips=F.sum(card.cast("int")), card_fares=F.round(F.sum(F.when(card, F.col("fare_amount"))), 2),
            card_tips=F.round(F.sum(F.when(card, F.col("tip_amount"))), 2))

daily = (silver.groupBy("pickup_date", "pickup_month")
         .agg(*[v.alias(k) for k, v in money.items()], *[v.alias(k) for k, v in sums.items()],
              F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
              F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
              F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
              F.round(F.sum("trip_distance") / (F.sum("duration_min") / 60), 2).alias("avg_speed_mph"),
              F.round(F.avg(F.when(F.col("payment_type") == 1, 1).otherwise(0)), 4).alias("card_share"),
              F.round(F.sum(F.when(F.col("payment_type") == 1, F.col("tip_amount"))) /
                      F.sum(F.when(F.col("payment_type") == 1, F.col("fare_amount"))), 4).alias("card_tip_pct"),
              F.sum(F.col("is_airport_trip").cast("int")).alias("airport_trips"),
              F.round(F.sum("cbd_congestion_fee"), 2).alias("cbd_fees"))
         .withColumnRenamed("pickup_date", "date")
         .join(weather, "date", "left").join(hol, "date", "left")
         .withColumn("day_of_week", F.date_format("date", "E"))
         .withColumn("is_weekend", F.dayofweek("date").isin(1, 7)))

zone_daily = (silver.groupBy(F.col("pickup_date").alias("date"), "pickup_month", F.col("pu_location_id").alias("location_id"),
                             F.col("pu_zone").alias("zone"), F.col("pu_borough").alias("borough"))
              .agg(*[v.alias(k) for k, v in money.items()], *[v.alias(k) for k, v in sums.items()],
                   F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                   F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi")))

days_per_group = (silver.select("pickup_month", "pickup_date", "is_weekend").distinct()
                  .groupBy("pickup_month", "is_weekend").agg(F.count("*").alias("days")))
hourly = (silver.groupBy("pickup_month", "is_weekend", "pickup_hour")
          .agg(F.count("*").alias("trips"), F.round(F.sum("total_amount"), 2).alias("revenue"),
               F.round(F.sum("trip_distance"), 2).alias("distance_mi"), F.round(F.sum("duration_min"), 2).alias("duration_min"),
               F.round(F.sum("trip_distance") / (F.sum("duration_min") / 60), 2).alias("avg_speed_mph"),
               F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
               F.round(F.avg("fare_amount"), 2).alias("avg_fare"))
          .join(days_per_group, ["pickup_month", "is_weekend"])
          .withColumn("trips_per_day", F.round(F.col("trips") / F.col("days"), 1))
          .withColumn("day_type", F.when(F.col("is_weekend"), "Weekend").otherwise("Weekday")))

payment = (silver.groupBy("pickup_month", "payment_type", "payment_desc")
           .agg(*[v.alias(k) for k, v in money.items()],
                F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
                F.round(F.sum("tip_amount") / F.sum("fare_amount"), 4).alias("tip_pct_of_fare"),
                F.round(F.avg(F.when(F.col("tip_amount") > 0, 1).otherwise(0)), 4).alias("share_with_tip")))

flows = (silver.groupBy("pickup_month", "pu_borough", "do_borough")
         .agg(*[v.alias(k) for k, v in money.items()],
              F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
              F.round(F.avg("duration_min"), 2).alias("avg_duration_min")))

downtown = (silver.groupBy(F.col("pickup_date").alias("date"), "pickup_month",
                           F.when(F.col("touches_downtown"), "Touches Downtown").otherwise("Elsewhere").alias("segment"))
            .agg(*[v.alias(k) for k, v in money.items()], F.round(F.sum("cbd_congestion_fee"), 2).alias("cbd_fees"))
            .withColumn("fee_in_force", F.col("date") >= F.lit(CBD_FEE_START)))

GOLD = {"daily_kpis": daily, "zone_daily": zone_daily, "hourly_demand": hourly,
        "payment_summary": payment, "borough_flows": flows, "downtown_daily": downtown}

# COMMAND ----------

if full_rebuild or months:
    where = None if full_rebuild else "pickup_month IN ({})".format(", ".join(f"'{m}'" for m in months))
    for name, df in GOLD.items():
        if full_rebuild or not table_exists("gold", name):
            write_table(df, "gold", name, partition_by="pickup_month", overwrite_schema=True)
        else:
            write_table(df, "gold", name, partition_by="pickup_month", replace_where=where)
        print(f"gold.{name}: {'full build' if full_rebuild else 'rebuilt ' + ', '.join(months)} "
              f"-> {read_table('gold', name).count():,} rows")
    if pending:
        write_table(spark.createDataFrame([(r.batch_id,) for r in pending], "batch_id string")
                    .withColumn("built_at", F.current_timestamp()), "gold", "build_log", mode="append")
else:
    print("Gold is up to date.")

# COMMAND ----------

display(spark.sql(f"""
    SELECT pickup_month, SUM(trips) AS trips, ROUND(SUM(revenue), 0) AS revenue, ROUND(SUM(revenue) / SUM(trips), 2) AS revenue_per_trip
    FROM {tbl('gold', 'daily_kpis')} GROUP BY pickup_month ORDER BY pickup_month"""))
