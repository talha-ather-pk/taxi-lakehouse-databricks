"""
Generate a synthetic taxi trip dataset for "Calder City", a fictional city whose taxi
regulator publishes monthly trip files (modelled on the layout of public taxi trip records).

The data is simulated, not real. Six months (Jan to Jun 2025, ~3.5 million trips) with
documented effects, so every finding can be checked against the truth (TRUE_EFFECTS below):
  * 62 zones in 5 boroughs + 2 airports, each with a type (business, nightlife, residential,
    mixed, airport) that drives when trips start and end there
  * weekday / weekend hourly demand curves, day-of-week and month factors, public holidays
  * rain: more trips, slower traffic
  * a Downtown congestion fee ($2.50) from 1 Mar 2025 that removes 7% of trips touching Downtown
  * speeds by hour and borough, metered fares, airport flat fare, surcharges, tolls
  * card tips from tip-screen presets; cash tips are not recorded (tip_amount = 0), as in real data

The raw monthly files also contain the problems a real feed has, each injected into its own
set of rows and counted, so the silver layer's rejections can be checked row for row:
  negative amounts, zero distance, drop-off before pick-up, meter-clock dates outside the
  period, impossible speed/duration, fare typos, exact duplicate records, unknown zone IDs,
  missing passenger counts, and schema drift (a new column from March, a renamed column and
  a changed type from April). On 13 May one vendor's meters under-recorded distance; the vendor
  re-sends corrected records (plus a few it had missed) in a separate file, which the pipeline
  must apply as an upsert, not append.

Output (data_raw/):
  trips/trips_2025-01.parquet ... trips_2025-06.parquet, trips/trips_2025-05_resend.parquet
  zones.csv, weather_daily.csv, true_effects.json
Run: python scripts/generate_data.py   (from the project root, ~1 minute)
"""
from pathlib import Path
import json
import numpy as np
import pandas as pd

SEED = 11
rng = np.random.default_rng(SEED)
ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data_raw"
TRIPS_DIR = RAW / "trips"
TRIPS_DIR.mkdir(parents=True, exist_ok=True)

START, END = pd.Timestamp("2025-01-01"), pd.Timestamp("2025-06-30")
CBD_FEE_START = pd.Timestamp("2025-03-01")
CBD_FEE, CBD_KEEP = 2.50, 0.93          # fee and share of Downtown trips that still happen
RAIN_DEMAND, RAIN_SPEED = 1.12, 0.88
BASE_DAILY = 19000

# ------------------------------------------------------------------ zones
BOROUGHS = {  # centre (km), zone count, mix of zone types
    "Downtown": ((0, 0), 14, {"business": .55, "nightlife": .30, "mixed": .15}),
    "Riverside": ((-5, 4), 12, {"residential": .45, "nightlife": .25, "mixed": .30}),
    "Northgate": ((2, 9), 12, {"residential": .65, "mixed": .25, "business": .10}),
    "Eastport": ((8, -1), 12, {"residential": .50, "mixed": .30, "nightlife": .20}),
    "Westfield": ((-9, -4), 12, {"residential": .75, "mixed": .25}),
}
STREETS = ["Harbor", "Mill", "Union", "Grove", "Station", "Market", "Cedar", "Kings", "Bridge",
           "Foundry", "Park", "Canal", "Elm", "Quarry", "Ferry", "Chapel", "Orchard", "Granite",
           "Lantern", "Beacon", "Willow", "Copper", "Signal", "Arbor", "Hollow", "Summit"]
SUFFIX = {"business": ["Financial District", "Exchange", "Plaza", "Center", "Tower Row"],
          "nightlife": ["Quarter", "Arts District", "Theatre Row", "Docks", "Village"],
          "residential": ["Heights", "Gardens", "Park", "Hill", "Terrace", "Commons"],
          "mixed": ["Square", "Crossing", "Junction", "Yards", "Market"]}
rows, used = [], set()
loc = 1
for b, ((cx, cy), n, mix) in BOROUGHS.items():
    types = rng.choice(list(mix), n, p=list(mix.values()))
    for t in types:
        while True:
            name = f"{rng.choice(STREETS)} {rng.choice(SUFFIX[t])}"
            if name not in used:
                used.add(name)
                break
        spread = 1.6 if b == "Downtown" else 2.6
        rows.append(dict(LocationID=loc, Borough=b, Zone=name, zone_type=t,
                         x=cx + rng.normal(0, spread), y=cy + rng.normal(0, spread)))
        loc += 1
rows.append(dict(LocationID=loc, Borough="Airports", Zone="Calder International Airport",
                 zone_type="airport", x=14.0, y=-12.0)); CCI = loc; loc += 1
rows.append(dict(LocationID=loc, Borough="Airports", Zone="Lakeshore Regional Airport",
                 zone_type="airport", x=-6.0, y=13.0)); LRA = loc; loc += 1
zones = pd.DataFrame(rows)
zones["service_zone"] = np.where(zones.Borough == "Downtown", "Congestion Zone",
                                 np.where(zones.Borough == "Airports", "Airports", "Boro Zone"))
N_Z = len(zones)
UNKNOWN_IDS = [98, 99]  # used by meters for "unknown" / "outside city" (not in the lookup)
zones_out = pd.concat([zones, pd.DataFrame([
    dict(LocationID=98, Borough="Unknown", Zone="Unknown", zone_type="unknown", service_zone="N/A"),
    dict(LocationID=99, Borough="Unknown", Zone="Outside Calder City", zone_type="unknown",
         service_zone="N/A")])], ignore_index=True)
zones_out[["LocationID", "Borough", "Zone", "service_zone", "zone_type"]].to_csv(RAW / "zones.csv", index=False)

is_dt = (zones.Borough == "Downtown").to_numpy()
is_air = (zones.zone_type == "airport").to_numpy()
xy = zones[["x", "y"]].to_numpy()
dist_km = np.sqrt(((xy[:, None, :] - xy[None, :, :]) ** 2).sum(-1))
zone_size = rng.lognormal(0, 0.35, N_Z)  # zone popularity

# hourly attractiveness by zone type (index 0..23), separate weekday / weekend
h = np.arange(24)
def bump(c, w): return np.exp(-0.5 * ((h - c) / w) ** 2)
TYPE_PU = {  # where trips START at each hour
    ("business", 0): 0.15 + 0.4 * bump(9, 2) + 1.4 * bump(18, 2.2) + 0.5 * bump(13, 1.5),
    ("business", 1): 0.15 + 0.25 * bump(14, 4),
    ("nightlife", 0): 0.2 + 0.4 * bump(19, 3) + 0.9 * bump(23.5, 2.5) + 0.4 * bump(1, 1.5),
    ("nightlife", 1): 0.3 + 0.6 * bump(14, 4) + 1.6 * bump(24, 2.5) + 0.9 * bump(1.5, 1.8),
    ("residential", 0): 0.1 + 1.1 * bump(8, 1.6) + 0.3 * bump(18, 3),
    ("residential", 1): 0.1 + 0.5 * bump(11, 3) + 0.3 * bump(18, 3),
    ("mixed", 0): 0.2 + 0.6 * bump(8.5, 2) + 0.6 * bump(17.5, 2.5) + 0.3 * bump(13, 2),
    ("mixed", 1): 0.2 + 0.6 * bump(14, 4) + 0.3 * bump(21, 3),
    ("airport", 0): 0.15 + 0.8 * bump(8, 3) + 1.0 * bump(16, 4) + 0.7 * bump(21, 2.5),
    ("airport", 1): 0.15 + 0.8 * bump(12, 4) + 0.8 * bump(19, 3),
}
TYPE_DO = {  # where trips END by hour bucket (night, am, midday, pm)
    "business": [0.2, 1.8, 1.0, 0.5], "nightlife": [1.5, 0.3, 0.7, 1.3],
    "residential": [1.6, 0.5, 0.6, 1.3], "mixed": [0.8, 0.9, 1.0, 1.0], "airport": [0.4, 1.0, 1.0, 0.9],
}
def hour_bucket(hr): return np.select([hr < 6, hr < 11, hr < 16], [0, 1, 2], 3)

# overall hourly volume curve
HOURLY = {0: 0.25 + 0.15 * bump(1, 2) + 1.0 * bump(8.5, 1.5) + 0.65 * bump(13, 3) + 1.15 * bump(18.5, 2.2)
          + 0.45 * bump(22.5, 1.8),
          1: 0.35 + 0.6 * bump(1, 2.2) + 0.55 * bump(12, 3.5) + 0.75 * bump(19, 3) + 0.65 * bump(23, 2)}
HOURLY = {k: v / v.sum() for k, v in HOURLY.items()}
SPEED_BY_HOUR = 21 - 7 * bump(8.5, 1.6) - 8 * bump(17.5, 2.0) - 3 * bump(13, 3) + 4 * bump(3, 2.5)
BORO_SPEED = {"Downtown": 0.72, "Riverside": 0.95, "Northgate": 1.05, "Eastport": 1.0,
              "Westfield": 1.12, "Airports": 1.25}
boro_speed = zones.Borough.map(BORO_SPEED).to_numpy()

# ------------------------------------------------------------------ calendar & weather
days = pd.date_range(START, END, freq="D")
DOW = np.array([0.92, 0.98, 1.00, 1.06, 1.14, 1.08, 0.86])
MONTH = {1: 0.94, 2: 0.95, 3: 1.00, 4: 1.02, 5: 1.04, 6: 1.03}
HOLIDAYS = {"2025-01-01": ("New Year's Day", 0.78), "2025-01-20": ("Martin Luther King Jr. Day", 0.86),
            "2025-02-17": ("Presidents' Day", 0.88), "2025-05-26": ("Memorial Day", 0.80)}
rain_p = np.array([0.30, 0.28, 0.33, 0.38, 0.35, 0.30])[days.month - 1]
wet = rng.random(len(days)) < rain_p
precip = np.where(wet, np.round(rng.gamma(1.4, 5.0, len(days)), 1), 0.0)
precip = np.where(wet & (precip < 0.3), 0.3, precip)
rainy = precip >= 2.0
temp = np.round(np.interp(days.day_of_year, [1, 30, 75, 120, 181], [-1, 0, 7, 15, 25])
                + rng.normal(0, 3, len(days)), 1)
weather = pd.DataFrame({"date": days.strftime("%Y-%m-%d"), "precip_mm": precip, "temp_c": temp})
weather.to_csv(RAW / "weather_daily.csv", index=False)

trend = 1 + 0.01 * ((days - START).days / 30.4)
holiday_f = np.array([HOLIDAYS.get(d.strftime("%Y-%m-%d"), (None, 1.0))[1] for d in days])
day_vol = BASE_DAILY * DOW[days.day_of_week] * np.array([MONTH[m] for m in days.month]) * trend \
          * holiday_f * np.where(rainy, RAIN_DEMAND, 1.0) * rng.lognormal(0, 0.03, len(days))
day_trips = rng.poisson(day_vol)
# planned trips include the Downtown trips later removed by the fee; thinning happens below


def simulate(day_idx_arr):
    """Simulate clean trips for the given array of day indexes (one entry per trip)."""
    n = len(day_idx_arr)
    d = days[day_idx_arr]
    weekend = np.asarray(d.day_of_week >= 5).astype(int)
    hr = np.empty(n, int)
    for wk in (0, 1):
        m = weekend == wk
        hr[m] = rng.choice(24, m.sum(), p=HOURLY[wk])
    # pickup zone by (weekend, hour)
    pu = np.empty(n, int)
    for wk in (0, 1):
        for hh in range(24):
            m = (weekend == wk) & (hr == hh)
            if not m.any():
                continue
            w = zone_size * np.array([TYPE_PU[(t, wk)][hh] for t in zones.zone_type])
            pu[m] = rng.choice(N_Z, m.sum(), p=w / w.sum())
    # dropoff zone by (pickup zone, hour bucket): gravity with distance decay
    bk = hour_bucket(hr)
    do = np.empty(n, int)
    for b in range(4):
        attract = zone_size * np.array([TYPE_DO[t][b] for t in zones.zone_type])
        for z in range(N_Z):
            m = (bk == b) & (pu == z)
            if not m.any():
                continue
            w = attract * np.exp(-dist_km[z] / 6.5)
            if is_air[z]:
                w = attract * np.exp(-dist_km[z] / 25)      # airport trips go far
            w[is_air] *= 0.25 if not is_air[z] else 0.02
            w[z] *= 1.6                                      # short trips within the zone
            do[m] = rng.choice(N_Z, m.sum(), p=w / w.sum())
    minute = rng.integers(0, 60, n); sec = rng.integers(0, 60, n)
    pickup = pd.to_datetime(d.to_numpy()) + pd.to_timedelta(hr * 3600 + minute * 60 + sec, unit="s")
    # distance (miles)
    km = dist_km[pu, do]
    miles = np.where(pu == do, rng.gamma(2.0, 0.45, n) + 0.2,
                     km * 1.25 / 1.609 * rng.lognormal(0, 0.15, n))
    miles = np.round(np.clip(miles, 0.2, 60), 2)
    # speed and duration
    rain_day = rainy[day_idx_arr]
    spd = SPEED_BY_HOUR[hr] * np.sqrt(boro_speed[pu] * boro_speed[do]) \
          * np.where(rain_day, RAIN_SPEED, 1.0) * np.where(weekend == 1, 1.08, 1.0) * rng.lognormal(0, 0.18, n)
    spd = np.clip(spd, 4, 55)
    dur_min = miles / spd * 60 + rng.uniform(0.8, 2.5, n)
    dropoff = pickup + pd.to_timedelta(np.round(dur_min * 60), unit="s")
    # fares
    airport_flat = ((pu == CCI - 1) & is_dt[do]) | ((do == CCI - 1) & is_dt[pu])
    rate = np.where(airport_flat, 2, 1)
    negotiated = (~airport_flat) & (rng.random(n) < 0.004)
    rate = np.where(negotiated, 5, rate)
    fare = 3.00 + 2.50 * miles + 0.45 * dur_min
    fare = np.where(rate == 2, 55.0, fare)
    fare = np.where(rate == 5, np.round(fare * rng.uniform(0.85, 1.1, n) / 5) * 5, fare)
    fare = np.round(fare, 2)
    weekday = weekend == 0
    extra = np.where((hr >= 20) | (hr < 6), 1.0, 0.0) + np.where(weekday & (hr >= 16) & (hr < 20), 2.5, 0.0)
    mta = np.full(n, 0.5); improve = np.full(n, 1.0)
    airport_fee = np.where(is_air[pu], 1.75, 0.0)
    touches_dt = is_dt[pu] | is_dt[do]
    cbd = np.where(touches_dt & (pickup >= CBD_FEE_START), CBD_FEE, 0.0)
    tolls = np.where((is_air[pu] | is_air[do]) & (rng.random(n) < 0.35), 6.94, 0.0)
    # payment: card share drifts up over the half-year
    month_i = np.asarray(d.month)
    p_card = 0.70 + 0.01 * (month_i - 1)
    u = rng.random(n)
    pay = np.where(u < p_card, 1, np.where(u < p_card + 0.265, 2, np.where(u < p_card + 0.285, 3, 4)))
    preset = rng.choice([0, 0.15, 0.20, 0.25, 0.30], n, p=[0.08, 0.22, 0.38, 0.20, 0.12])
    tip = np.where(pay == 1, np.round(fare * preset + rng.choice([0, 0, 0, 0.5, 1.0], n), 2), 0.0)
    total = np.round(fare + extra + mta + improve + airport_fee + cbd + tolls + tip, 2)
    pax = rng.choice([1, 2, 3, 4, 5, 6], n, p=[0.70, 0.17, 0.05, 0.03, 0.03, 0.02]).astype(float)
    vendor = rng.choice([1, 2], n, p=[0.42, 0.58])
    return pd.DataFrame({
        "VendorID": vendor, "tpep_pickup_datetime": pickup, "tpep_dropoff_datetime": dropoff,
        "passenger_count": pax, "trip_distance": miles, "RatecodeID": rate.astype(float),
        "store_and_fwd_flag": np.where(rng.random(n) < 0.004, "Y", "N"),
        "PULocationID": zones.LocationID.to_numpy()[pu], "DOLocationID": zones.LocationID.to_numpy()[do],
        "payment_type": pay, "fare_amount": fare, "extra": extra, "mta_tax": mta, "tip_amount": tip,
        "tolls_amount": tolls, "improvement_surcharge": improve, "total_amount": total,
        "airport_fee": airport_fee, "cbd_congestion_fee": cbd, "_touches_dt": touches_dt,
        "_dur_min": dur_min,
    })


# ------------------------------------------------------------------ simulate all trips
day_idx = np.repeat(np.arange(len(days)), day_trips)
trips = simulate(day_idx)
fee_on = (trips.tpep_pickup_datetime >= CBD_FEE_START).to_numpy()
drop = trips._touches_dt.to_numpy() & fee_on & (rng.random(len(trips)) > CBD_KEEP)
trips = trips.loc[~drop].sort_values("tpep_pickup_datetime").reset_index(drop=True)
trips["trip_id"] = [f"V{v}-{i:08d}" for v, i in zip(trips.VendorID, rng.permutation(len(trips)) + 10_000_000)]
print(f"clean trips simulated: {len(trips):,}")
# file month = month the record is reported in (set before meter-clock errors are injected)
trips["_file_month"] = trips.tpep_pickup_datetime.dt.month

# ------------------------------------------------------------------ 13 May vendor-2 meter fault
fault = (trips.VendorID == 2) & (trips.tpep_pickup_datetime.dt.strftime("%Y-%m-%d") == "2025-05-13") \
        & trips.tpep_pickup_datetime.dt.hour.between(7, 14)
corrected = trips.loc[fault].copy()                 # the true records, re-sent later
faulty = trips.loc[fault].copy()
under = 0.82
faulty["trip_distance"] = np.round(faulty.trip_distance * under, 2)
metered = faulty.RatecodeID == 1
new_fare = np.round(3.00 + 2.50 * faulty.trip_distance + 0.45 * faulty._dur_min, 2)
fare_diff = np.where(metered, new_fare - faulty.fare_amount, 0.0)
tip_diff = np.where(faulty.payment_type == 1, np.round(fare_diff * (faulty.tip_amount / faulty.fare_amount), 2), 0)
faulty["fare_amount"] = np.round(faulty.fare_amount + fare_diff, 2)
faulty["tip_amount"] = np.round(faulty.tip_amount + tip_diff, 2)
faulty["total_amount"] = np.round(faulty.total_amount + fare_diff + tip_diff, 2)
trips.loc[fault, faulty.columns] = faulty
# trips the vendor's meters failed to upload on 13 May (only in the re-send file)
missed = simulate(np.full(400, int(np.where(days == "2025-05-13")[0][0])))
missed = missed.loc[missed.tpep_pickup_datetime.dt.hour.between(7, 14)].copy()
missed["VendorID"] = 2
missed["trip_id"] = [f"V2-{i:08d}" for i in range(90_000_000, 90_000_000 + len(missed))]
resend = pd.concat([corrected, missed], ignore_index=True)

# ------------------------------------------------------------------ inject data problems
INJECT = {"negative_amount": 0.0035, "zero_distance": 0.0070, "bad_timestamps": 0.0012,
          "out_of_period": 0.0005, "implausible_speed": 0.0008, "fare_outlier": 0.0001}
idx_free = np.setdiff1d(np.arange(len(trips)), np.where(fault)[0])
rng.shuffle(idx_free)
pos, injected_ids, injected_months = 0, {}, {}
money = ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
         "total_amount", "airport_fee", "cbd_congestion_fee"]
for kind, share in INJECT.items():
    k = int(len(trips) * share)
    ix = idx_free[pos:pos + k]; pos += k
    injected_ids[kind] = trips.loc[ix, "trip_id"].tolist()
    injected_months[kind] = trips.loc[ix, "_file_month"].to_numpy()
    if kind == "negative_amount":            # voided / refunded records
        trips.loc[ix, money] = -trips.loc[ix, money].abs()
    elif kind == "zero_distance":
        trips.loc[ix, "trip_distance"] = 0.0
    elif kind == "bad_timestamps":
        trips.loc[ix, "tpep_dropoff_datetime"] = trips.loc[ix, "tpep_pickup_datetime"] \
            - pd.to_timedelta(rng.integers(0, 3600, k), unit="s")
    elif kind == "out_of_period":            # meter clock not set
        shift = rng.choice([-16 * 365, 365], k)
        for c in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
            trips.loc[ix, c] = trips.loc[ix, c] + pd.to_timedelta(shift, unit="D")
    elif kind == "implausible_speed":        # meter left running / GPS jump
        half = k // 2
        trips.loc[ix[:half], "tpep_dropoff_datetime"] = trips.loc[ix[:half], "tpep_pickup_datetime"] \
            + pd.to_timedelta(np.round(rng.uniform(7, 30, half) * 3600), unit="s")
        trips.loc[ix[half:], "trip_distance"] = np.round(trips.loc[ix[half:], "trip_distance"] * 45 + 80, 2)
    elif kind == "fare_outlier":             # extra digits typed
        trips.loc[ix, "fare_amount"] = np.round(trips.loc[ix, "fare_amount"] * 100, 2)
        trips.loc[ix, "total_amount"] = np.round(trips.loc[ix, "total_amount"] + trips.loc[ix, "fare_amount"] * 0.99, 2)
# soft problems (rows kept, values fixed or flagged)
rest = idx_free[pos:]
k_unknown = int(len(trips) * 0.004)
ix_u = rest[:k_unknown]
which = rng.random(k_unknown) < 0.5
trips.loc[ix_u[which], "PULocationID"] = rng.choice(UNKNOWN_IDS, which.sum())
trips.loc[ix_u[~which], "DOLocationID"] = rng.choice(UNKNOWN_IDS, (~which).sum())
k_pax = int(len(trips) * 0.025)
ix_p = rest[k_unknown:k_unknown + k_pax]
trips.loc[ix_p, "passenger_count"] = np.where(rng.random(k_pax) < 0.6, np.nan, 0.0)
trips.loc[ix_p, "store_and_fwd_flag"] = None
# exact duplicate records (re-transmitted)
k_dup = int(len(trips) * 0.003)
dup_ix = rng.choice(rest[k_unknown + k_pax:], k_dup, replace=False)
dups = trips.loc[dup_ix]
soft_months = {"duplicates": trips.loc[dup_ix, "_file_month"].to_numpy(),
               "unknown_zone": trips.loc[ix_u, "_file_month"].to_numpy(),
               "missing_passenger_count": trips.loc[ix_p, "_file_month"].to_numpy()}
trips = pd.concat([trips, dups], ignore_index=True)

# ------------------------------------------------------------------ write monthly files
COLS = ["VendorID", "trip_id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
        "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
        "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "total_amount", "airport_fee", "cbd_congestion_fee"]


def to_file_layout(df, month):
    out = df[COLS].copy()
    out["passenger_count"] = out["passenger_count"].astype("Int64") if month < 4 else out["passenger_count"]
    out["RatecodeID"] = out["RatecodeID"].astype("Int64")
    if month < 3:                                    # the CBD fee column appears in March
        out = out.drop(columns="cbd_congestion_fee")
    if month >= 4:                                   # system upgrade in April: renamed column
        out = out.rename(columns={"airport_fee": "Airport_fee"})
    return out.sample(frac=1, random_state=month).reset_index(drop=True)


PQ = dict(coerce_timestamps="us", allow_truncated_timestamps=True)  # Spark reads micros, not nanos
file_rows = {}
for m in range(1, 7):
    part = trips.loc[trips._file_month == m]
    name = f"trips_2025-{m:02d}.parquet"
    to_file_layout(part, m).to_parquet(TRIPS_DIR / name, index=False, **PQ)
    file_rows[name] = len(part)
    print(f"{name}: {len(part):,} rows")
to_file_layout(resend, 5).to_parquet(TRIPS_DIR / "trips_2025-05_resend.parquet", index=False, **PQ)
file_rows["trips_2025-05_resend.parquet"] = len(resend)
print(f"trips_2025-05_resend.parquet: {len(resend):,} rows "
      f"({len(corrected):,} corrections, {len(missed):,} missed trips)")

# ------------------------------------------------------------------ truth
clean_final = len(trips) - k_dup - sum(len(v) for v in injected_ids.values()) + len(missed)
TRUE_EFFECTS = {
    "note": "Parameters used by the simulator. Synthetic data; values are exact by construction.",
    "period": ["2025-01-01", "2025-06-30"],
    "cbd_fee": {"start": "2025-03-01", "fee_usd": CBD_FEE, "trips_touching_downtown_change": CBD_KEEP - 1},
    "rain": {"threshold_mm": 2.0, "demand_multiplier": RAIN_DEMAND, "speed_multiplier": RAIN_SPEED},
    "day_of_week_factor_mon_to_sun": DOW.tolist(),
    "holidays": {k: {"name": v[0], "factor": v[1]} for k, v in HOLIDAYS.items()},
    "trend_per_month": 0.01,
    "card_tip_presets": {"0": 0.08, "0.15": 0.22, "0.20": 0.38, "0.25": 0.20, "0.30": 0.12},
    "cash_tips_recorded": False,
    "airport_flat_fare_downtown_cci": 55.0,
    "vendor2_meter_fault_2025_05_13": {"distance_under_recording": 1 - under,
                                       "affected_trips": int(len(corrected)), "missed_trips": int(len(missed))},
    "injected_rejects": {k: len(v) for k, v in injected_ids.items()},
    "injected_duplicates": int(k_dup),
    "unknown_zone_rows": int(k_unknown),
    "missing_or_zero_passenger_count": int(k_pax),
    "file_rows": file_rows,
    "injected_by_file": {f"trips_2025-{m:02d}.parquet": {
        **{k: int((v == m).sum()) for k, v in injected_months.items()},
        **{k: int((v == m).sum()) for k, v in soft_months.items()}} for m in range(1, 7)},
    "expected_silver_rows_after_all_files": int(clean_final),
}
(RAW / "true_effects.json").write_text(json.dumps(TRUE_EFFECTS, indent=2))
print(json.dumps({k: TRUE_EFFECTS[k] for k in ["injected_rejects", "injected_duplicates",
                                               "expected_silver_rows_after_all_files"]}, indent=1))
