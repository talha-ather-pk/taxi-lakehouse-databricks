"""
Calder City taxi lakehouse: README / site charts from the exported gold tables.

Input : data_clean/gold/*.csv (written by scripts/run_local.py)
Output: images/chart_*.png and a smaller copy in site/img/
Run   : python scripts/make_charts.py   (from the project root)
"""
from pathlib import Path
import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter, PercentFormatter
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
G = ROOT / "data_clean" / "gold"
IMG, SITE = ROOT / "images", ROOT / "site" / "img"
IMG.mkdir(exist_ok=True)
SITE.mkdir(parents=True, exist_ok=True)
NAVY, PRIMARY, COMPARE, GOLD, GOOD, WARN, BAD, SLATE = ("#0B1F3A", "#1F4E79", "#B8C2CF", "#C8A24A", "#0E7C86",
                                                        "#D08C2E", "#B5523B", "#64748B")
plt.rcParams.update({"font.family": "Segoe UI", "font.size": 10, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.titleweight": "bold", "axes.titlesize": 12,
                     "axes.titlelocation": "left", "axes.edgecolor": "#CBD5E1"})
thousands = FuncFormatter(lambda v, _: f"{v / 1e3:.0f}K" if abs(v) >= 1e3 else f"{v:.0f}")


def save(fig, name):
    fig.tight_layout()
    fig.savefig(IMG / name, dpi=150)
    fig.savefig(SITE / name, dpi=110)
    plt.close(fig)
    print(f"wrote images/{name}")


daily = pd.read_csv(G / "daily_kpis.csv", parse_dates=["date"]).sort_values("date")
hourly = pd.read_csv(G / "hourly_demand.csv")
dq = pd.read_csv(G / "data_quality.csv").sort_values("step")
dd = pd.read_csv(G / "downtown_daily.csv", parse_dates=["date"])
pay = pd.read_csv(G / "payment_summary.csv")
zones = pd.read_csv(G / "zone_daily.csv")
fnd = pd.read_csv(G / "findings.csv").set_index("key")
truth = json.loads((ROOT / "data_raw" / "true_effects.json").read_text())

# 1. pipeline: where every raw row went ------------------------------------------------
rows = dq[dq.layer.str.startswith("Silver") & ~dq.layer.str.contains("kept")]
removed = rows[rows.metric != "Clean trips"]
removed = removed[removed.rows > 0].sort_values("rows")
raw_rows = int(dq.loc[dq.metric == "Rows received", "rows"].iloc[0])
clean = int(dq.loc[dq.metric == "Clean trips", "rows"].iloc[0])
fig, ax = plt.subplots(figsize=(9, 4.2))
ax.barh(removed.metric.str.replace("Rejected: ", ""), removed.rows, color=[BAD if "Rejected" in m else WARN for m in removed.metric])
for i, (v, inj) in enumerate(zip(removed.rows, removed.get("injected", pd.Series([np.nan] * len(removed))))):
    tag = f"  {v:,}" + (f"  = injected" if pd.notna(inj) and int(inj) == v else "")
    ax.text(v, i, tag, va="center", fontsize=9, color="#334155")
ax.xaxis.set_major_formatter(thousands)
ax.set_xlim(0, removed.rows.max() * 1.55)
ax.set_title(f"{raw_rows:,} raw rows → {clean:,} clean trips: every removed row has a reason")
ax.text(0, -0.2, "Red: failed a validation rule. Orange: duplicates, and old versions replaced by the vendor's corrections.\n"
        "\"= injected\": the count equals the number of bad rows the generator planted.", transform=ax.transAxes, fontsize=8, color=SLATE)
save(fig, "chart_pipeline.png")

# 2. demand and speed by hour -------------------------------------------------------------
h = hourly.groupby(["day_type", "pickup_hour"]).agg(trips=("trips", "sum"), days=("days", "sum"),
                                                     dist=("distance_mi", "sum"), dur=("duration_min", "sum")).reset_index()
h["per_day"] = h.trips / h.days
h["speed"] = h.dist / (h.dur / 60)
fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 3.8))
for dt_, c in [("Weekday", PRIMARY), ("Weekend", GOLD)]:
    x = h[h.day_type == dt_]
    a1.plot(x.pickup_hour, x.per_day, color=c, lw=2.4, label=dt_)
    a2.plot(x.pickup_hour, x.speed, color=c, lw=2.4, label=dt_)
wd = h[h.day_type == "Weekday"].set_index("pickup_hour")
pk = wd.per_day.idxmax()
a1.text(pk, wd.per_day.max() * 1.03, f"{pk}:00 peak: {wd.per_day.max():,.0f} trips/hour", ha="center", fontsize=9,
        color="#334155")
a1.set_ylim(0, wd.per_day.max() * 1.15)
a1.set_title("Trips per hour (average day)")
a2.set_ylim(wd.speed.min() - 1.5, None)
a2.set_title("Average traffic speed, mph")
for a in (a1, a2):
    a.set_xticks(range(0, 24, 3))
    a.set_xticklabels([f"{i}:00" for i in range(0, 24, 3)])
    a.legend(frameon=False)
a1.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}"))
slow = wd.speed.idxmin()
a2.annotate(f"{wd.speed.min():.1f} mph at {slow}:00", (slow, wd.speed.min()), (slow - 4, wd.speed.min() - 0.1), va="top",
            arrowprops=dict(arrowstyle="-", color=SLATE), fontsize=9, color="#334155")
save(fig, "chart_hourly.png")

# 3. Downtown congestion fee: daily ratio of Downtown trips to other trips -----------------
piv = dd.pivot_table(index="date", columns="segment", values="trips")
piv["ratio"] = piv["Touches Downtown"] / piv["Elsewhere"]
wkg = piv["ratio"].resample("W-SUN", label="left", closed="left")
wk = wkg.mean()[wkg.count() == 7]                      # full Monday-Sunday weeks only
wk.index = wk.index + pd.Timedelta(days=3)             # plot at mid-week
pre = piv.loc[piv.index < "2025-03-01", "ratio"].mean()
post = piv.loc[piv.index >= "2025-03-01", "ratio"].mean()
fig, ax = plt.subplots(figsize=(9, 3.8))
ax.plot(piv.index, piv.ratio, color=COMPARE, lw=0.9, label="Daily")
ax.plot(wk.index, wk.values, color=PRIMARY, lw=2.4, label="Weekly average")
ax.hlines(pre, piv.index.min(), pd.Timestamp("2025-02-28"), color=NAVY, ls="--", lw=1.2)
ax.hlines(post, pd.Timestamp("2025-03-01"), piv.index.max(), color=BAD, ls="--", lw=1.2)
ax.axvline(pd.Timestamp("2025-03-01"), color=GOLD, lw=1.5)
ax.text(pd.Timestamp("2025-03-03"), ax.get_ylim()[1] * 0.995, "$2.50 Downtown fee starts 1 Mar", va="top", fontsize=9, color="#8A6D1F")
ax.set_ylabel("Downtown trips per other trip", color=SLATE)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
ax.legend(frameon=False, loc="lower left")
ax.set_title(f"After the congestion fee, trips touching Downtown fell {-fnd.loc['cbd_fee_effect', 'value']:.1%} "
             f"relative to the rest of the city")
save(fig, "chart_downtown_fee.png")

# 4. rain: trips per day by weekday, rainy vs dry ---------------------------------------
daily["wd"] = daily.date.dt.dayofweek
order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
nh = daily[daily.holiday.isna()]
r = nh.groupby(["wd", "is_rainy"]).trips.mean().unstack()
fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 3.8), gridspec_kw={"width_ratios": [1.6, 1]})
x = np.arange(7)
a1.bar(x - 0.2, r[False], 0.4, color=COMPARE, label="Dry")
a1.bar(x + 0.2, r[True], 0.4, color=PRIMARY, label="Rain (2 mm+)")
a1.set_xticks(x, order)
a1.yaxis.set_major_formatter(thousands)
a1.legend(frameon=False, ncol=2, loc="upper left")
a1.set_ylim(0, r.values.max() * 1.18)
a1.set_title("Average trips per day, holidays excluded")
eff = [fnd.loc["rain_demand_effect", "value"], fnd.loc["rain_speed_effect", "value"]]
a2.bar(["Trips", "Traffic speed"], eff, color=[GOOD, BAD], width=0.55)
for i, v in enumerate(eff):
    a2.text(i, v + (0.004 if v > 0 else -0.004), f"{v:+.1%}", ha="center", va="bottom" if v > 0 else "top", fontweight="bold")
a2.axhline(0, color="#CBD5E1", lw=1)
a2.yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
a2.set_ylim(min(eff) * 1.45, max(eff) * 1.45)
a2.set_title("Rain effect, same weekday and month")
save(fig, "chart_rain.png")

# 5. daily trips with rain days ----------------------------------------------------------
fig, ax = plt.subplots(figsize=(11, 3.6))
ax.plot(daily.date, daily.trips, color=PRIMARY, lw=1.3)
rain = daily[daily.is_rainy == True]  # noqa: E712
ax.scatter(rain.date, rain.trips, s=14, color=GOLD, zorder=3, label="Rainy day")
for _, row in daily[daily.holiday.notna()].iterrows():
    ax.annotate(row.holiday.replace("Martin Luther King Jr. Day", "MLK Day"), (row.date, row.trips),
                (row.date, 11300), fontsize=8, color=SLATE, ha="center", arrowprops=dict(arrowstyle="-", color="#CBD5E1"))
ax.set_ylim(10500, None)
ax.yaxis.set_major_formatter(thousands)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
ax.legend(frameon=False, loc="upper left")
m = daily.groupby(daily.date.dt.month).trips.mean()
ax.set_title(f"Daily trips, Jan–Jun 2025: Fridays peak, Sundays dip; {m.iloc[0]:,.0f}/day in January → {m.iloc[-1]:,.0f}/day in June")
save(fig, "chart_daily.png")

# 6. top zones ----------------------------------------------------------------------------
z = zones[zones.borough != "Unknown"].groupby(["zone", "borough"]).agg(revenue=("revenue", "sum"), trips=("trips", "sum")) \
    .reset_index().sort_values("revenue", ascending=False)
top = z.head(12).iloc[::-1]
colors = {"Downtown": PRIMARY, "Airports": GOLD, "Riverside": GOOD, "Eastport": "#7A8CA5", "Northgate": "#5B8E7D",
          "Westfield": "#8C6BB1"}
fig, ax = plt.subplots(figsize=(9, 4.6))
ax.barh(top.zone + "  ·  " + top.borough, top.revenue, color=[colors.get(b, SLATE) for b in top.borough])
for i, v in enumerate(top.revenue):
    ax.text(v, i, f"  ${v / 1e6:.2f}M", va="center", fontsize=9)
ax.set_xlim(0, top.revenue.max() * 1.18)
ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"${v / 1e6:.0f}M"))
ax.set_title(f"Top 12 pick-up zones by revenue (top 10 = {fnd.loc['top10_zone_share', 'value']:.0%} of all revenue)")
save(fig, "chart_zones.png")
