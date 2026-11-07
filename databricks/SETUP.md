# Running the pipeline on Databricks (Free Edition)

The notebooks in [`../notebooks`](../notebooks) are Databricks source notebooks. On Databricks they write Unity
Catalog tables (`workspace.taxi_bronze.*`, `taxi_silver.*`, `taxi_gold.*`); locally they write Delta folders.
[`00_config.py`](../notebooks/00_config.py) is the only place that knows the difference.

## 1. Workspace

1. Sign up at <https://www.databricks.com/learn/free-edition> (free, no credit card). The workspace has a
   `workspace` catalog and serverless compute.
2. **Optional, for the CLI:** in your workspace go to *Settings → Developer → Access tokens → Generate new token*,
   then install the CLI and log in:
   ```bash
   winget install Databricks.DatabricksCLI     # macOS: brew install databricks
   databricks auth login --host https://<your-workspace>.cloud.databricks.com
   ```

## 2. Landing files

Run `00_config` once (it creates the schemas and the volume `workspace.taxi_bronze.landing`), then upload:

| Local file | Volume path |
|---|---|
| `data_raw/zones.csv`, `data_raw/weather_daily.csv`, `data_raw/true_effects.json` | `/Volumes/workspace/taxi_bronze/landing/reference/` |
| `data_raw/trips/trips_2025-01.parquet` … `trips_2025-05.parquet` | `/Volumes/workspace/taxi_bronze/landing/trips/` |

UI: *Catalog → workspace → taxi_bronze → Volumes → landing → Upload to this volume*. CLI:

```bash
databricks fs cp -r data_raw/trips/ dbfs:/Volumes/workspace/taxi_bronze/landing/trips/ --overwrite   # then delete June + resend to simulate the first load
databricks fs cp data_raw/zones.csv dbfs:/Volumes/workspace/taxi_bronze/landing/reference/
```

## 3. Run

**With the bundle (recommended):**
```bash
databricks bundle deploy
databricks bundle run taxi_lakehouse
```
This creates the job *Calder taxi lakehouse: bronze → silver → gold* with four dependent tasks.

**Without the CLI:** *Workspace → Import* the five notebooks (or the whole `notebooks` folder) and run
`01_bronze`, `02_silver`, `03_gold`, `04_quality_and_history` in order, then `05_analysis`.

## 4. Incremental load

Upload `trips_2025-06.parquet` and `trips_2025-05_resend.parquet` to the `trips/` folder and run the job again.
Only the two new files are read; silver inserts June and updates the 13 May trips; gold rebuilds May and June
only. Run it a third time: nothing changes.

## 5. Power BI

*Partner Connect → Power BI*, or in Power BI Desktop *Get data → Azure Databricks* with the SQL warehouse's
server hostname and HTTP path, then select the `workspace.taxi_gold` tables. The repo's dashboard reads the same
tables exported to CSV (`data_clean/gold/`), so it opens without a Databricks connection.

## Serverless notes

The code avoids features serverless compute does not allow: no RDDs, no `cache()`, no `sparkContext`, file lineage
via column values rather than `input_file_name()`, and only `spark.sql.session.timeZone` is set.
