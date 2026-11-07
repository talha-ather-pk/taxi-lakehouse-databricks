# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Analysis on the gold tables
# MAGIC
# MAGIC Business questions answered with Spark SQL on gold (and silver where trip-level detail is needed).
# MAGIC Small regressions run on daily aggregates (181 rows) with NumPy. Results are saved to `taxi_gold.findings`.
# MAGIC
# MAGIC 1. How big is the business and how is it trending?
# MAGIC 2. When is demand highest, and how slow is traffic then?
# MAGIC 3. Did the Downtown congestion fee (from 1 March 2025) reduce trips?
# MAGIC 4. How much does rain change demand and speed?
# MAGIC 5. Tips and payments; airports; top zones.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import numpy as np
import pandas as pd

findings = []


def note(key, value, text):
    findings.append((key, float(value), text))
    print(f"{key}: {text}")


def ols(y, X, names):
    """OLS with classical standard errors; returns {name: (coef, se)}."""
    X = np.column_stack([np.ones(len(y)), X])
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ beta
    s2 = resid @ resid / (len(y) - X.shape[1])
    se = np.sqrt(np.diag(s2 * np.linalg.inv(X.T @ X)))
    return {n: (beta[i + 1], se[i + 1]) for i, n in enumerate(names)}

# COMMAND ----------

# MAGIC %md ## 1 · Size and trend

# COMMAND ----------

monthly = spark.sql(f"""
    SELECT pickup_month, SUM(trips) AS trips, ROUND(SUM(revenue), 0) AS revenue,
           ROUND(SUM(revenue) / SUM(trips), 2) AS revenue_per_trip, ROUND(SUM(trips) / COUNT(*), 0) AS trips_per_day
    FROM {tbl('gold', 'daily_kpis')} GROUP BY pickup_month ORDER BY pickup_month""")
display(monthly)
m = monthly.toPandas()
note("total_trips", m.trips.sum(), f"{m.trips.sum():,.0f} clean trips, ${m.revenue.sum() / 1e6:,.1f}M revenue, "
     f"${m.revenue.sum() / m.trips.sum():.2f} per trip (Jan-Jun 2025)")
note("trips_per_day_jan_jun", m.trips_per_day.iloc[-1] / m.trips_per_day.iloc[0] - 1,
     f"trips per day {m.trips_per_day.iloc[0]:,.0f} in January -> {m.trips_per_day.iloc[-1]:,.0f} in June "
     f"({m.trips_per_day.iloc[-1] / m.trips_per_day.iloc[0] - 1:+.1%})")

# COMMAND ----------

# MAGIC %md ## 2 · Demand by hour and traffic speed

# COMMAND ----------

hourly = spark.sql(f"""
    SELECT day_type, pickup_hour, ROUND(SUM(trips) / SUM(days), 0) AS trips_per_day,
           ROUND(SUM(avg_speed_mph * trips) / SUM(trips), 1) AS avg_speed_mph
    FROM {tbl('gold', 'hourly_demand')} GROUP BY day_type, pickup_hour ORDER BY day_type, pickup_hour""")
display(hourly)
h = hourly.toPandas()
wd, we = h[h.day_type == "Weekday"], h[h.day_type == "Weekend"]
pk, pk_we = wd.loc[wd.trips_per_day.idxmax()], we.loc[we.trips_per_day.idxmax()]
note("weekday_peak_hour", pk.pickup_hour, f"weekday peak {int(pk.pickup_hour)}:00-{int(pk.pickup_hour) + 1}:00 with "
     f"{pk.trips_per_day:,.0f} trips per hour at {pk.avg_speed_mph} mph (fastest hour: {wd.avg_speed_mph.max()} mph)")
note("weekend_peak_hour", pk_we.pickup_hour, f"weekend peak {int(pk_we.pickup_hour)}:00 with {pk_we.trips_per_day:,.0f} "
     f"trips per hour; weekend 0:00-3:00 has {we[we.pickup_hour < 3].trips_per_day.sum() / wd[wd.pickup_hour < 3].trips_per_day.sum():.1f}x "
     f"the weekday late-night demand")

# COMMAND ----------

# MAGIC %md ## 3 · Congestion fee: trips touching Downtown vs elsewhere, before and after 1 March
# MAGIC
# MAGIC Difference-in-differences on the daily ratio of Downtown trips to other trips: weather, weekday, holidays and growth hit
# MAGIC both groups, so they cancel out in the ratio. Regression of log(ratio) on a fee dummy plus weekday dummies.

# COMMAND ----------

dd = read_table("gold", "downtown_daily").toPandas()
piv = dd.pivot_table(index="date", columns="segment", values="trips").reset_index()
piv["date"] = pd.to_datetime(piv["date"])
piv["log_ratio"] = np.log(piv["Touches Downtown"] / piv["Elsewhere"])
piv["post"] = (piv.date >= CBD_FEE_START).astype(float)
dow = pd.get_dummies(piv.date.dt.dayofweek, prefix="d", drop_first=True).astype(float)
r = ols(piv.log_ratio.to_numpy(), np.column_stack([piv.post, dow]), ["post"] + list(dow.columns))
eff, se = r["post"]
note("cbd_fee_effect", np.exp(eff) - 1, f"the congestion fee cut trips touching Downtown by {1 - np.exp(eff):.1%} "
     f"(95% CI {1 - np.exp(eff + 1.96 * se):.1%} to {1 - np.exp(eff - 1.96 * se):.1%}) relative to the rest of the city")
share_pre = piv.loc[piv.post == 0, "Touches Downtown"].sum() / piv.loc[piv.post == 0, ["Touches Downtown", "Elsewhere"]].sum().sum()
share_post = piv.loc[piv.post == 1, "Touches Downtown"].sum() / piv.loc[piv.post == 1, ["Touches Downtown", "Elsewhere"]].sum().sum()
fees = spark.sql(f"SELECT SUM(cbd_fees) AS f FROM {tbl('gold', 'daily_kpis')}").collect()[0]["f"]
note("cbd_fees_collected", fees, f"Downtown share of trips {share_pre:.1%} before -> {share_post:.1%} after; "
     f"${fees / 1e6:,.2f}M in congestion fees collected Mar-Jun")

# COMMAND ----------

# MAGIC %md ## 4 · Rain

# COMMAND ----------

d = read_table("gold", "daily_kpis").toPandas()
d["date"] = pd.to_datetime(d["date"])
d = d.sort_values("date")
X = pd.concat([d.is_rainy.astype(float).rename("rain"), d.holiday.notna().astype(float).rename("holiday"),
               pd.get_dummies(d.date.dt.dayofweek, prefix="d", drop_first=True).astype(float),
               pd.get_dummies(d.date.dt.month, prefix="m", drop_first=True).astype(float)], axis=1)
rt = ols(np.log(d.trips.to_numpy()), X.to_numpy(), list(X.columns))
rs = ols(np.log(d.avg_speed_mph.to_numpy()), X.to_numpy(), list(X.columns))
note("rain_demand_effect", np.exp(rt["rain"][0]) - 1,
     f"rainy days (2 mm+) bring {np.exp(rt['rain'][0]) - 1:+.1%} trips (95% CI {np.exp(rt['rain'][0] - 1.96 * rt['rain'][1]) - 1:+.1%} "
     f"to {np.exp(rt['rain'][0] + 1.96 * rt['rain'][1]) - 1:+.1%}), same weekday and month")
note("rain_speed_effect", np.exp(rs["rain"][0]) - 1, f"and traffic is {1 - np.exp(rs['rain'][0]):.1%} slower")
note("rainy_days", d.is_rainy.sum(), f"{int(d.is_rainy.sum())} rainy days of {len(d)}")

# COMMAND ----------

# MAGIC %md ## 5 · Payments, tips, airports, zones

# COMMAND ----------

pay = spark.sql(f"""
    SELECT payment_desc, SUM(trips) AS trips, ROUND(SUM(trips) / SUM(SUM(trips)) OVER (), 4) AS share,
           ROUND(SUM(fare_revenue) / SUM(trips), 2) AS avg_fare, ROUND(SUM(tips) / SUM(fare_revenue), 4) AS tip_pct_of_fare
    FROM {tbl('gold', 'payment_summary')} GROUP BY payment_desc ORDER BY trips DESC""")
display(pay)
p = pay.toPandas().set_index("payment_desc")
note("card_tip_pct", p.loc["Credit card", "tip_pct_of_fare"],
     f"card riders tip {p.loc['Credit card', 'tip_pct_of_fare']:.1%} of the fare; card is {p.loc['Credit card', 'share']:.0%} of trips. "
     f"Cash tips are not recorded (cash shows {p.loc['Cash', 'tip_pct_of_fare']:.0%}), so tip analysis uses card trips only")
card = spark.sql(f"""
    SELECT pickup_month, ROUND(SUM(CASE WHEN payment_type = 1 THEN trips END) / SUM(trips), 4) AS card_share
    FROM {tbl('gold', 'payment_summary')} GROUP BY pickup_month ORDER BY pickup_month""").toPandas()
note("card_share_trend", card.card_share.iloc[-1] - card.card_share.iloc[0],
     f"card share rose from {card.card_share.iloc[0]:.1%} in January to {card.card_share.iloc[-1]:.1%} in June")

air = spark.sql(f"""
    SELECT SUM(CASE WHEN pu_borough = 'Airports' OR do_borough = 'Airports' THEN trips END) / SUM(trips) AS trip_share,
           SUM(CASE WHEN pu_borough = 'Airports' OR do_borough = 'Airports' THEN revenue END) / SUM(revenue) AS rev_share,
           SUM(CASE WHEN pu_borough = 'Airports' OR do_borough = 'Airports' THEN revenue END) /
           SUM(CASE WHEN pu_borough = 'Airports' OR do_borough = 'Airports' THEN trips END) AS rev_per_trip
    FROM {tbl('gold', 'borough_flows')}""").collect()[0]
note("airport_revenue_share", air.rev_share, f"airport trips are {air.trip_share:.1%} of trips but {air.rev_share:.1%} of revenue "
     f"(${air.rev_per_trip:.2f} per trip)")

zones = spark.sql(f"""
    SELECT zone, borough, SUM(trips) AS trips, ROUND(SUM(revenue), 0) AS revenue
    FROM {tbl('gold', 'zone_daily')} WHERE borough <> 'Unknown'
    GROUP BY zone, borough ORDER BY revenue DESC""")
display(zones.limit(10))
z = zones.toPandas()
top10 = z.head(10).revenue.sum() / z.revenue.sum()
note("top10_zone_share", top10, f"the top 10 of {len(z)} pick-up zones generate {top10:.0%} of revenue; #1 is "
     f"{z.iloc[0].zone} ({z.iloc[0].borough}) with ${z.iloc[0].revenue / 1e6:,.2f}M")
dt_share = spark.sql(f"""SELECT SUM(CASE WHEN pu_borough = 'Downtown' THEN revenue END) / SUM(revenue) AS s
                         FROM {tbl('gold', 'borough_flows')}""").collect()[0]["s"]
note("downtown_pickup_revenue_share", dt_share, f"Downtown pick-ups generate {dt_share:.0%} of revenue")

# COMMAND ----------

f_df = spark.createDataFrame(findings, "key string, value double, finding string")
write_table(f_df, "gold", "findings", overwrite_schema=True)
display(f_df)
