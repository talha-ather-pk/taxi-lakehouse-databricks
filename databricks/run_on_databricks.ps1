# Runs the whole walkthrough on a Databricks workspace (Free Edition works):
# creates the schemas + landing volume, uploads the files, deploys the bundle and runs the job three times
# (initial load -> incremental load -> re-run with nothing new).
#
# Prerequisites: Databricks CLI installed and logged in:
#   databricks auth login --host https://<your-workspace>.cloud.databricks.com
#   python scripts/generate_data.py     # creates data_raw/ if it is not there
#
# Usage (from the repo root):  powershell -File databricks/run_on_databricks.ps1 [-Profile DEFAULT]
param([string]$Profile = "DEFAULT", [switch]$SkipUpload)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
$vol = "dbfs:/Volumes/workspace/taxi_bronze/landing"

function Db { & databricks @args --profile $Profile; if ($LASTEXITCODE -ne 0) { throw "databricks $args failed" } }
function DbTry { $ErrorActionPreference = "Continue"; & databricks @args --profile $Profile 2>&1 | Out-Null }   # tolerates "already exists"
function Step($t) { Write-Host "`n=== $t ===" -ForegroundColor Cyan }

if (-not (Test-Path data_raw/trips/trips_2025-01.parquet)) { throw "data_raw/trips is missing: run python scripts/generate_data.py first" }

Step "Workspace user"
Db current-user me --output json | ConvertFrom-Json | ForEach-Object { $_.userName }

Step "Schemas and landing volume"
foreach ($s in "taxi_bronze", "taxi_silver", "taxi_gold") { DbTry schemas create $s workspace }
DbTry volumes create workspace taxi_bronze landing MANAGED
Db volumes read workspace.taxi_bronze.landing --output json | ConvertFrom-Json | ForEach-Object { $_.full_name }

if (-not $SkipUpload) {
    Step "Upload reference files and the first five months"
    foreach ($f in "zones.csv", "weather_daily.csv", "true_effects.json") { Db fs cp "data_raw/$f" "$vol/reference/$f" --overwrite }
    foreach ($m in "01", "02", "03", "04", "05") { Db fs cp "data_raw/trips/trips_2025-$m.parquet" "$vol/trips/trips_2025-$m.parquet" --overwrite }
}

Step "Deploy the bundle"
Db bundle validate
Db bundle deploy

Step "Run 1: initial load (Jan - May)"
Db bundle run taxi_lakehouse

Step "Run 2: incremental load (June + 13 May re-send)"
foreach ($f in "trips_2025-06.parquet", "trips_2025-05_resend.parquet") { Db fs cp "data_raw/trips/$f" "$vol/trips/$f" --overwrite }
Db bundle run taxi_lakehouse

Step "Run 3: re-run with no new files"
Db bundle run taxi_lakehouse

Step "Done"
Write-Host "Open the workspace: Jobs & Pipelines -> 'Calder taxi lakehouse' for the run graph, Catalog -> workspace.taxi_gold for the tables."
Write-Host "Then open notebooks/05_analysis (deployed under Workspace -> Users -> you -> .bundle) and Run all to write gold.findings."
