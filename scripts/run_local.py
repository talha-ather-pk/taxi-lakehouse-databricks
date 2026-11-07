"""
Run the Databricks notebooks locally (Spark 4 + Delta Lake) to simulate how the pipeline is used:

  round 1  initial    : Jan-May files land  -> bronze, silver, gold built from scratch
  round 2  incremental: June file + the vendor's 13 May re-send land -> only those are processed
  round 3  rerun      : nothing new lands   -> the job finds nothing to do (idempotent)

The notebooks in notebooks/ are Databricks "source" notebooks (.py with # COMMAND ---------- cells);
this runner executes their code cells in order and skips %md / %run cells (00_config is run first).
Then 05_analysis runs once, and the gold tables are exported to data_clean/gold/*.csv for Power BI and the README charts.

Run: python scripts/run_local.py            (from the project root; ~10-15 minutes)
     python scripts/run_local.py --keep     (keep existing lakehouse; only run what is pending)
"""
import argparse
import contextlib
import io
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from local_spark import get_spark, LAKE, LANDING  # noqa: E402

NB = ROOT / "notebooks"
RAW = ROOT / "data_raw"
OUT = ROOT / "data_clean"
PIPELINE = ["01_bronze.py", "02_silver.py", "03_gold.py", "04_quality_and_history.py"]
ROUNDS = {
    "initial": [f"trips_2025-0{m}.parquet" for m in range(1, 6)],
    "incremental": ["trips_2025-06.parquet", "trips_2025-05_resend.parquet"],
    "rerun": [],
}


def code_cells(path):
    text = path.read_text(encoding="utf-8")
    for cell in text.split("# COMMAND ----------"):
        lines = [l for l in cell.splitlines() if l.strip() and l.strip() != "# Databricks notebook source"]
        if not lines or all(l.startswith("# MAGIC") for l in lines):
            continue
        yield cell


class Tee(io.TextIOBase):
    def __init__(self, *streams):
        self.streams = streams

    def write(self, s):
        for st in self.streams:
            st.write(s)
        return len(s)

    def flush(self):
        for st in self.streams:
            if not st.closed:
                st.flush()


def run_notebook(spark, name, log):
    ns = {"spark": spark, "__name__": "__notebook__"}
    for cell in code_cells(NB / "00_config.py"):
        exec(compile(cell, "00_config.py", "exec"), ns)
    t0 = time.time()
    print(f"\n=== {name} ===")
    for i, cell in enumerate(code_cells(NB / name)):
        exec(compile(cell, f"{name}[cell {i}]", "exec"), ns)
    print(f"--- {name} finished in {time.time() - t0:.0f}s")


def stage(files):
    (LANDING / "trips").mkdir(parents=True, exist_ok=True)
    (LANDING / "reference").mkdir(parents=True, exist_ok=True)
    for f in ["zones.csv", "weather_daily.csv", "true_effects.json"]:
        shutil.copy2(RAW / f, LANDING / "reference" / f)
    for f in files:
        shutil.copy2(RAW / "trips" / f, LANDING / "trips" / f)


def export_gold(spark):
    gold_dir = OUT / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)
    tables = ["daily_kpis", "zone_daily", "hourly_demand", "payment_summary", "borough_flows",
              "downtown_daily", "data_quality", "pipeline_batches", "findings"]
    for t in tables:
        if not (LAKE / "gold" / t).exists():
            continue
        df = spark.read.format("delta").load(str(LAKE / "gold" / t)).toPandas()
        sort = [c for c in ("step", "date", "pickup_month", "processed_at") if c in df.columns]
        if sort:
            df = df.sort_values(sort)
        df.to_csv(gold_dir / f"{t}.csv", index=False)
        print(f"exported gold.{t}: {len(df):,} rows")
    for layer, t in [("silver", "zones"), ("silver", "weather"), ("bronze", "ingest_log"), ("silver", "batch_log")]:
        df = spark.read.format("delta").load(str(LAKE / layer / t)).toPandas()
        df.to_csv(gold_dir / f"{layer}_{t}.csv", index=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="keep the existing lakehouse and landing area")
    ap.add_argument("--rounds", default="initial,incremental,rerun")
    ap.add_argument("--skip-analysis", action="store_true")
    args = ap.parse_args()
    if not args.keep:
        for d in (LAKE, LANDING):
            shutil.rmtree(d, ignore_errors=True)
    OUT.mkdir(exist_ok=True)
    spark = get_spark()
    with open(OUT / "pipeline_run_log.txt", "w", encoding="utf-8") as log, \
            contextlib.redirect_stdout(Tee(sys.__stdout__, log)):
        for rnd in args.rounds.split(","):
            print(f"\n################ ROUND: {rnd}  (landing: {', '.join(ROUNDS[rnd]) or 'no new files'})")
            stage(ROUNDS[rnd])
            for nb in PIPELINE:
                run_notebook(spark, nb, log)
        if not args.skip_analysis:
            run_notebook(spark, "05_analysis.py", log)
        print()
        export_gold(spark)
    spark.stop()


if __name__ == "__main__":
    main()
