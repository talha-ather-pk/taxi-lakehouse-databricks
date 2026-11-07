"""
Calder City taxi lakehouse: generate the Power BI dashboard as a Power BI Project (PBIP) on the gold tables.

Writes powerbi/CalderTaxi.pbip:
  CalderTaxi.SemanticModel/  star schema over the gold tables exported by scripts/run_local.py
                             (data_clean/gold/*.csv): zone x day, hourly, payments, borough flows,
                             Downtown segments, data-quality and pipeline-run tables; measures in TMDL
  CalderTaxi.Report/         custom theme, sidebar navigation, 5 report pages, a Zone Detail
                             drill-through page and a day tooltip page (PBIR JSON)

On Databricks, point the same model at the workspace.taxi_gold tables (see databricks/SETUP.md).
Open the .pbip in Power BI Desktop (Nov 2025 release or later) and click Refresh.
Run: python scripts/build_pbip.py   (from the project root)
"""
import csv
import json
import shutil
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PBI = ROOT / "powerbi"
NAME = "CalderTaxi"
SM = PBI / f"{NAME}.SemanticModel"
RP = PBI / f"{NAME}.Report"
DATA_FOLDER = str(ROOT / "data_clean" / "gold") + "\\"
BASE_THEME = "CY26SU04"
THEME_SRC = Path(r"C:\Program Files\Microsoft Power BI Desktop\bin\WebView2Resources"
                 r"\minerva\sharedresources\BaseThemes") / f"{BASE_THEME}.json"
CUSTOM_THEME = "Calder_Executive.json"
SCHEMA = "https://developer.microsoft.com/json-schemas/fabric"
V_VISUAL, V_PAGE, V_REPORT = "2.4.0", "2.0.0", "3.0.0"

# =========================================================================
# Design system
# =========================================================================
NAVY = "#0B1F3A"        # sidebar, primary text on light
NAVY_2 = "#16314F"      # active nav item
INK = "#0F172A"
SLATE = "#64748B"
SLATE_2 = "#94A3B8"
MIST = "#CBD5E1"
LINE = "#E6E9F0"
CANVAS = "#F3F5F9"
WHITE = "#FFFFFF"
GOLD = "#C8A24A"
PRIMARY = "#1F4E79"     # main series
COMPARE = "#B8C2CF"     # prior year / comparison series
GOOD, WARN, BAD, NEUTRAL = "#0E7C86", "#D08C2E", "#B5523B", "#94A3B8"
DATA_COLORS = [PRIMARY, GOOD, GOLD, "#7A8CA5", BAD, "#5B8E7D", "#8C6BB1", WARN]
FONT, FONT_SB, FONT_LIGHT = "Segoe UI", "Segoe UI Semibold", "Segoe UI Light"


def gid(*parts):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "calder/" + "/".join(parts)))


def hid(*parts):
    return gid(*parts).replace("-", "")[:20]


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def write_json(path, obj):
    write(path, json.dumps(obj, indent=2, ensure_ascii=False) + "\n")


# =========================================================================
# Semantic model
# =========================================================================
S, I, D, DT, B = "string", "int64", "double", "dateTime", "boolean"
MTYPE = {S: "type text", I: "Int64.Type", D: "type number", DT: "type date", B: "logical"}


def cols(*spec):
    """(name, type[, hidden[, extra]]) -> column tuples. Every column summarizeBy none."""
    out = []
    for name, dtype, *rest in spec:
        hidden = bool(rest and rest[0])
        extra = dict(rest[1]) if len(rest) > 1 else {}
        if dtype == DT:
            extra.setdefault("formatString", "dd mmm yyyy")
        out.append((name, dtype, hidden, extra))
    return out


# table -> (csv file or None, columns loaded, extra M steps (list of (name, expr using {prev})), derived columns)
TABLES = {
    "fact_zone_daily": ("zone_daily.csv", cols(
        ("date", DT, True), ("location_id", I, True), ("trips", I, True), ("revenue", D, True),
        ("fare_revenue", D, True), ("tips", D, True), ("distance_mi", D, True), ("duration_min", D, True),
        ("card_trips", I, True), ("card_fares", D, True), ("card_tips", D, True)), [], []),
    "fact_hourly": ("hourly_demand.csv", cols(
        ("pickup_month", S, True), ("pickup_hour", I), ("day_type", S, True), ("trips", I, True), ("revenue", D, True),
        ("distance_mi", D, True), ("duration_min", D, True), ("days", I, True)),
        [("HourLabel", 'Table.AddColumn({prev}, "hour", each Text.PadStart(Text.From([pickup_hour]), 2, "0") & ":00", type text)')],
        cols(("hour", S, False, {"sortByColumn": "pickup_hour"}))),
    "fact_payment": ("payment_summary.csv", cols(
        ("pickup_month", S, True), ("payment_type", I, True), ("payment_desc", S, False, {"sortByColumn": "payment_type"}),
        ("trips", I, True), ("revenue", D, True), ("fare_revenue", D, True), ("tips", D, True)), [], []),
    "fact_flows": ("borough_flows.csv", cols(
        ("pickup_month", S, True), ("pu_borough", S, True), ("do_borough", S), ("trips", I, True), ("revenue", D, True),
        ("fare_revenue", D, True)), [], []),
    "fact_downtown": ("downtown_daily.csv", cols(
        ("date", DT, True), ("segment", S), ("trips", I, True), ("revenue", D, True), ("cbd_fees", D, True)), [], []),
    "dim_zone": ("silver_zones.csv", cols(
        ("location_id", I, True), ("borough", S, True), ("zone", S), ("service_zone", S), ("zone_type", S)),
        [("Ztype", 'Table.TransformColumns({prev}, {{"zone_type", Text.Proper, type text}})')], []),
    "dim_borough": (None, cols(("borough", S, False, {"isKey": None})), [], []),
    "dim_date": ("silver_weather.csv", cols(
        ("date", DT, False, {"isKey": None}), ("precip_mm", D, True), ("temp_c", D, True), ("is_rainy", S, True)),
        [("Weather", 'Table.AddColumn({prev}, "weather", each if [is_rainy] = "True" then "Rain (2 mm+)" else "Dry", type text)'),
         ("Holiday", 'Table.AddColumn({prev}, "holiday", each Record.FieldOrDefault([#"2025-01-01" = "New Year\'s Day", '
                     '#"2025-01-20" = "Martin Luther King Jr. Day", #"2025-02-17" = "Presidents\' Day", '
                     '#"2025-05-26" = "Memorial Day"], Date.ToText([date], "yyyy-MM-dd"), null), type text)'),
         ("Month", 'Table.AddColumn({prev}, "pickup_month", each Date.ToText([date], "yyyy-MM"), type text)'),
         ("WeekStart", 'Table.AddColumn({prev}, "week_start", each Date.StartOfWeek([date], Day.Monday), type date)'),
         ("WdNum", 'Table.AddColumn({prev}, "weekday_num", each Date.DayOfWeek([date], Day.Monday) + 1, Int64.Type)'),
         ("Wd", 'Table.AddColumn({prev}, "weekday", each Date.ToText([date], "ddd", "en-US"), type text)'),
         ("DayType", 'Table.AddColumn({prev}, "day_type", each if [weekday_num] >= 6 then "Weekend" else "Weekday", type text)'),
         ("Fee", 'Table.AddColumn({prev}, "fee_period", each if [date] >= #date(2025, 3, 1) then "After fee (Mar–Jun)" '
                 'else "Before fee (Jan–Feb)", type text)')],
        cols(("weather", S), ("holiday", S), ("pickup_month", S, True), ("week_start", DT, False, {"formatString": "dd mmm"}),
             ("weekday_num", I, True), ("weekday", S, False, {"sortByColumn": "weekday_num"}), ("day_type", S, True),
             ("fee_period", S))),
    "dim_month": (None, cols(("pickup_month", S, True, {"isKey": None}), ("month_num", I, True),
                             ("month", S, False, {"sortByColumn": "month_num"})), [], []),
    "dim_daytype": (None, cols(("day_type", S, False, {"isKey": None})), [], []),
    "data_quality": ("data_quality.csv", cols(
        ("step", I, True), ("layer", S), ("metric", S, False, {"sortByColumn": "step"}), ("rows", I, True),
        ("injected", I, True)), [], []),
    "pipeline_batches": ("pipeline_batches.csv", cols(
        ("batch_id", S), ("files", S), ("rows_in", I, True), ("exact_duplicates", I, True), ("rejected", I, True),
        ("same_key_duplicates", I, True), ("inserted", I, True), ("updated", I, True), ("silver_version", I),
        ("months_touched", S), ("schema_notes", S)),
        [("Sorted", 'Table.Sort({prev}, {{"batch_id", Order.Ascending}})'),
         ("Idx", 'Table.AddIndexColumn({prev}, "run_order", 1, 1, Int64.Type)'),
         ("Run", 'Table.AddColumn({prev}, "run", each if [run_order] = 1 then "1 · Initial load" else '
                 'Text.From([run_order]) & " · Incremental load", type text)')],
        cols(("run_order", I, True), ("run", S, False, {"sortByColumn": "run_order"}))),
    "findings": ("findings.csv", cols(("key", S), ("value", D, True), ("finding", S)), [], []),
}

CUSTOM_M = {
    "dim_borough": [
        "let",
        '    Source = Csv.Document(File.Contents(DataFolder & "silver_zones.csv"), [Delimiter = ",", Encoding = 65001, '
        'QuoteStyle = QuoteStyle.Csv]),',
        "    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),",
        '    Kept = Table.SelectColumns(Promoted, {"borough"}),',
        "    Distinct = Table.Distinct(Kept),",
        '    Typed = Table.TransformColumnTypes(Distinct, {{"borough", type text}})',
        "in",
        "    Typed"],
    "dim_month": [
        "let",
        "    Months = List.Transform({1..6}, each #date(2025, _, 1)),",
        '    Base = Table.FromList(Months, Splitter.SplitByNothing(), type table [d = date]),',
        '    Key = Table.AddColumn(Base, "pickup_month", each Date.ToText([d], "yyyy-MM"), type text),',
        '    Num = Table.AddColumn(Key, "month_num", each Date.Month([d]), Int64.Type),',
        '    Label = Table.AddColumn(Num, "month", each Date.ToText([d], "MMM yyyy", "en-US"), type text),',
        '    Out = Table.RemoveColumns(Label, {"d"})',
        "in",
        "    Out"],
    "dim_daytype": ['#table(type table [day_type = text], {{"Weekday"}, {"Weekend"}})'],
}

RELATIONSHIPS = [
    ("fact_zone_daily", "date", "dim_date", "date"), ("fact_zone_daily", "location_id", "dim_zone", "location_id"),
    ("dim_zone", "borough", "dim_borough", "borough"),
    ("fact_downtown", "date", "dim_date", "date"),
    ("dim_date", "pickup_month", "dim_month", "pickup_month"), ("dim_date", "day_type", "dim_daytype", "day_type"),
    ("fact_hourly", "pickup_month", "dim_month", "pickup_month"), ("fact_hourly", "day_type", "dim_daytype", "day_type"),
    ("fact_payment", "pickup_month", "dim_month", "pickup_month"),
    ("fact_flows", "pickup_month", "dim_month", "pickup_month"), ("fact_flows", "pu_borough", "dim_borough", "borough"),
]

# ---------------------------------------------------------------- measures
CUR, CUR2, PCT, INT, DEC1, DEC2 = (r"\$#,0;(\$#,0);\$#,0", r"\$#,0.00;(\$#,0.00);\$#,0.00",
                                   "0.0%;-0.0%;0.0%", "#,0", "#,0.0", "#,0.00")
SPCT = "+0.0%;-0.0%;0.0%"
MEASURES = []   # (name, dax, format, folder, hidden)


def m(name, dax, fmt=None, folder="", hidden=False):
    MEASURES.append((name, dax, fmt, folder, hidden))


def finding(key):
    return f'CALCULATE ( MAX ( findings[value] ), findings[key] = "{key}" )'


# Trips & revenue (pick-up zone x day, filtered by borough, month, day type) --------------
m("Trips", "SUM ( fact_zone_daily[trips] )", INT, "1 Trips")
m("Revenue", "SUM ( fact_zone_daily[revenue] )", CUR, "1 Trips")
m("Fare Revenue", "SUM ( fact_zone_daily[fare_revenue] )", CUR, "1 Trips")
m("Revenue per Trip", "DIVIDE ( [Revenue], [Trips] )", CUR2, "1 Trips")
m("Avg Fare", "DIVIDE ( [Fare Revenue], [Trips] )", CUR2, "1 Trips")
m("Avg Distance (mi)", "DIVIDE ( SUM ( fact_zone_daily[distance_mi] ), [Trips] )", DEC2, "1 Trips")
m("Avg Speed (mph)", "DIVIDE ( SUM ( fact_zone_daily[distance_mi] ), SUM ( fact_zone_daily[duration_min] ) / 60 )", DEC1, "1 Trips")
m("Avg Duration (min)", "DIVIDE ( SUM ( fact_zone_daily[duration_min] ), [Trips] )", DEC1, "1 Trips")
m("Card Tip %", "DIVIDE ( SUM ( fact_zone_daily[card_tips] ), SUM ( fact_zone_daily[card_fares] ) )", PCT, "1 Trips")
m("Card Share", "DIVIDE ( SUM ( fact_zone_daily[card_trips] ), [Trips] )", PCT, "1 Trips")
m("Days", "DISTINCTCOUNT ( fact_zone_daily[date] )", INT, "1 Trips")
m("Trips per Day", "DIVIDE ( [Trips], [Days] )", INT, "1 Trips")
m("Revenue per Day", "DIVIDE ( [Revenue], [Days] )", CUR, "1 Trips")
m("Trips per Day Growth",
  "VAR f = MIN ( dim_date[pickup_month] ) VAR l = MAX ( dim_date[pickup_month] ) RETURN "
  "DIVIDE ( CALCULATE ( [Trips per Day], dim_date[pickup_month] = l ), CALCULATE ( [Trips per Day], dim_date[pickup_month] = f ) ) - 1",
  SPCT, "1 Trips")
m("Zone Revenue Rank", "IF ( ISINSCOPE ( dim_zone[zone] ), RANKX ( ALLSELECTED ( dim_zone[zone], dim_zone[borough] ), [Revenue], , DESC, DENSE ) )",
  INT, "1 Trips")
m("Zones", "DISTINCTCOUNT ( dim_zone[location_id] )", INT, "1 Trips", hidden=True)
m("Rainy Days", 'CALCULATE ( COUNTROWS ( dim_date ), dim_date[weather] = "Rain (2 mm+)" )', INT, "1 Trips")
m("Trips per Day (Dry)", 'CALCULATE ( [Trips per Day], dim_date[weather] = "Dry", ISBLANK ( dim_date[holiday] ) )', INT, "1 Trips")
m("Trips per Day (Rain)", 'CALCULATE ( [Trips per Day], dim_date[weather] = "Rain (2 mm+)", ISBLANK ( dim_date[holiday] ) )', INT, "1 Trips")
m("Precipitation (mm)", "SUM ( dim_date[precip_mm] )", DEC1, "1 Trips")
# Hourly -------------------------------------------------------------------------------
m("Trips per Hour", "DIVIDE ( SUM ( fact_hourly[trips] ), SUM ( fact_hourly[days] ) )", INT, "2 Demand")
m("Speed by Hour (mph)", "DIVIDE ( SUM ( fact_hourly[distance_mi] ), SUM ( fact_hourly[duration_min] ) / 60 )", DEC1, "2 Demand")
m("Peak Trips per Hour",
  'MAXX ( VALUES ( fact_hourly[pickup_hour] ), CALCULATE ( [Trips per Hour], fact_hourly[day_type] = "Weekday" ) )', INT, "2 Demand")
m("Peak Hour Label",
  'VAR t = ADDCOLUMNS ( VALUES ( fact_hourly[pickup_hour] ), "@v", CALCULATE ( [Trips per Hour], fact_hourly[day_type] = "Weekday" ) ) '
  'VAR h = MAXX ( TOPN ( 1, t, [@v], DESC ), fact_hourly[pickup_hour] ) RETURN "Weekdays, " & h & ":00–" & ( h + 1 ) & ":00"',
  None, "2 Demand")
m("Slowest Speed (mph)",
  'MINX ( VALUES ( fact_hourly[pickup_hour] ), CALCULATE ( [Speed by Hour (mph)], fact_hourly[day_type] = "Weekday" ) )', DEC1, "2 Demand")
m("Slowest Hour Label",
  'VAR t = ADDCOLUMNS ( VALUES ( fact_hourly[pickup_hour] ), "@v", CALCULATE ( [Speed by Hour (mph)], fact_hourly[day_type] = "Weekday" ) ) '
  'VAR h = MAXX ( TOPN ( 1, t, [@v], ASC ), fact_hourly[pickup_hour] ) RETURN "Weekdays at " & h & ":00 (night: " & '
  'FORMAT ( MAXX ( t, [@v] ), "0" ) & ")"', None, "2 Demand")
m("Late Night Weekend Multiple",
  'VAR we = CALCULATE ( [Trips per Hour], fact_hourly[day_type] = "Weekend", fact_hourly[pickup_hour] < 3 ) '
  'VAR wd = CALCULATE ( [Trips per Hour], fact_hourly[day_type] = "Weekday", fact_hourly[pickup_hour] < 3 ) RETURN DIVIDE ( we, wd )',
  '0.0"x"', "2 Demand")
m("Hour Heat",
  'VAR v = [Trips per Hour] VAR mx = CALCULATE ( MAXX ( SUMMARIZE ( fact_hourly, fact_hourly[day_type], fact_hourly[pickup_hour] ), '
  '[Trips per Hour] ), ALLSELECTED ( fact_hourly ), ALLSELECTED ( dim_daytype ) ) VAR r = DIVIDE ( v, mx ) RETURN '
  'SWITCH ( TRUE (), ISBLANK ( v ), BLANK (), r > 0.85, "#1F4E79", r > 0.65, "#4F7BA6", r > 0.45, "#8FB0CF", r > 0.25, "#C9D8E8", "#EEF3F8" )',
  None, "8 Formatting", hidden=True)
m("Hour Heat Font", 'IF ( DIVIDE ( [Trips per Hour], CALCULATE ( MAXX ( SUMMARIZE ( fact_hourly, fact_hourly[day_type], '
  'fact_hourly[pickup_hour] ), [Trips per Hour] ), ALLSELECTED ( fact_hourly ), ALLSELECTED ( dim_daytype ) ) ) > 0.65, "#FFFFFF", "#1E293B" )',
  None, "8 Formatting", hidden=True)
m("Rain Demand Effect", finding("rain_demand_effect"), SPCT, "2 Demand")
m("Rain Speed Effect", finding("rain_speed_effect"), SPCT, "2 Demand")
# Downtown fee ------------------------------------------------------------------------
m("Downtown Trips", 'CALCULATE ( SUM ( fact_downtown[trips] ), fact_downtown[segment] = "Touches Downtown" )', INT, "3 Downtown fee")
m("Other Trips", 'CALCULATE ( SUM ( fact_downtown[trips] ), fact_downtown[segment] = "Elsewhere" )', INT, "3 Downtown fee")
m("Downtown Share", "DIVIDE ( [Downtown Trips], [Downtown Trips] + [Other Trips] )", PCT, "3 Downtown fee")
m("Downtown Ratio", "DIVIDE ( [Downtown Trips], [Other Trips] )", DEC2, "3 Downtown fee")
m("Full Week", "NOT ( ISINSCOPE ( dim_date[week_start] ) && COUNTROWS ( dim_date ) < 7 )", None, "9 Helpers", hidden=True)
m("Downtown Ratio Before Fee", "IF ( [Full Week] && MIN ( dim_date[date] ) < DATE ( 2025, 3, 1 ), [Downtown Ratio] )", DEC2, "3 Downtown fee")
m("Downtown Ratio After Fee", "IF ( [Full Week] && MAX ( dim_date[date] ) >= DATE ( 2025, 3, 1 ), [Downtown Ratio] )", DEC2, "3 Downtown fee")
m("Downtown Share Before", "CALCULATE ( [Downtown Share], dim_date[date] < DATE ( 2025, 3, 1 ) )", PCT, "3 Downtown fee")
m("Downtown Share After", "CALCULATE ( [Downtown Share], dim_date[date] >= DATE ( 2025, 3, 1 ) )", PCT, "3 Downtown fee")
m("Segment Trips per Day", "DIVIDE ( SUM ( fact_downtown[trips] ), DISTINCTCOUNT ( fact_downtown[date] ) )", INT, "3 Downtown fee")
m("Segment Index (Jan-Feb = 100)",
  "DIVIDE ( [Segment Trips per Day], CALCULATE ( [Segment Trips per Day], REMOVEFILTERS ( dim_date ), REMOVEFILTERS ( dim_month ), "
  "dim_date[date] < DATE ( 2025, 3, 1 ) ) ) * 100", DEC1, "3 Downtown fee")
m("Fee Effect", finding("cbd_fee_effect"), SPCT, "3 Downtown fee")
m("Fees Collected", "SUM ( fact_downtown[cbd_fees] )", CUR, "3 Downtown fee")
m("Downtown Trips per Day", "DIVIDE ( [Downtown Trips], DISTINCTCOUNT ( fact_downtown[date] ) )", INT, "3 Downtown fee")
# Payments, airports, flows -----------------------------------------------------------
m("Payment Trips", "SUM ( fact_payment[trips] )", INT, "4 Payments")
m("Payment Share", "DIVIDE ( [Payment Trips], CALCULATE ( [Payment Trips], REMOVEFILTERS ( fact_payment[payment_desc], fact_payment[payment_type] ) ) )",
  PCT, "4 Payments")
m("Tip % of Fare", "DIVIDE ( SUM ( fact_payment[tips] ), SUM ( fact_payment[fare_revenue] ) )", PCT, "4 Payments")
m("Card Share (Payments)", "CALCULATE ( [Payment Share], fact_payment[payment_type] = 1 )", PCT, "4 Payments")
m("Flow Trips", "SUM ( fact_flows[trips] )", INT, "4 Payments")
m("Airport Trips", 'CALCULATE ( [Flow Trips], FILTER ( fact_flows, fact_flows[pu_borough] = "Airports" || fact_flows[do_borough] = "Airports" ) )',
  INT, "4 Payments")
m("Airport Revenue", 'CALCULATE ( SUM ( fact_flows[revenue] ), FILTER ( fact_flows, fact_flows[pu_borough] = "Airports" || fact_flows[do_borough] = "Airports" ) )',
  CUR, "4 Payments")
m("Airport Trip Share", "DIVIDE ( [Airport Trips], [Flow Trips] )", PCT, "4 Payments")
m("Airport Revenue Share", "DIVIDE ( [Airport Revenue], SUM ( fact_flows[revenue] ) )", PCT, "4 Payments")
m("Airport Revenue per Trip", "DIVIDE ( [Airport Revenue], [Airport Trips] )", CUR2, "4 Payments")
m("Flow Share", "DIVIDE ( [Flow Trips], CALCULATE ( [Flow Trips], ALLSELECTED ( fact_flows[do_borough] ), ALLSELECTED ( dim_borough ) ) )",
  PCT, "4 Payments")
m("Flow Heat", 'VAR s = [Flow Share] RETURN SWITCH ( TRUE (), ISBLANK ( s ), BLANK (), s > 0.12, "#1F4E79", s > 0.06, "#4F7BA6", '
  's > 0.03, "#8FB0CF", s > 0.01, "#C9D8E8", "#F3F6FA" )', None, "8 Formatting", hidden=True)
m("Flow Heat Font", 'IF ( [Flow Share] > 0.06, "#FFFFFF", "#1E293B" )', None, "8 Formatting", hidden=True)
m("Top 10 Zone Share", finding("top10_zone_share"), PCT, "4 Payments")
# Data pipeline -----------------------------------------------------------------------
m("DQ Rows", "SUM ( data_quality[rows] )", INT, "5 Pipeline")
m("DQ Injected", "SUM ( data_quality[injected] )", INT, "5 Pipeline")
m("Raw Rows", 'CALCULATE ( [DQ Rows], data_quality[metric] = "Rows received" )', INT, "5 Pipeline")
m("Rejected Rows", 'CALCULATE ( [DQ Rows], LEFT ( data_quality[metric], 9 ) = "Rejected:" )', INT, "5 Pipeline")
m("Duplicate Rows", 'CALCULATE ( [DQ Rows], data_quality[metric] IN { "Exact duplicates removed", "Same trip sent twice (older copy dropped)" } )',
  INT, "5 Pipeline")
m("Corrected Rows", 'CALCULATE ( [DQ Rows], data_quality[metric] = "Replaced by vendor corrections" )', INT, "5 Pipeline")
m("Clean Trips", 'CALCULATE ( [DQ Rows], data_quality[metric] = "Clean trips" )', INT, "5 Pipeline")
m("Gold Trips", 'CALCULATE ( [DQ Rows], data_quality[metric] = "Trips in gold tables" )', INT, "5 Pipeline")
m("Removed Rows (Chart)",
  'CALCULATE ( [DQ Rows], data_quality[layer] = "Silver", data_quality[metric] <> "Clean trips" )', INT, "5 Pipeline")
m("Unaccounted Rows", "[Raw Rows] - [Rejected Rows] - [Duplicate Rows] - [Corrected Rows] - [Clean Trips]", INT, "5 Pipeline")
m("Reconciled Label", 'IF ( [Unaccounted Rows] = 0 && [Gold Trips] = [Clean Trips], "✓ 0 rows unaccounted for", "✗ check pipeline" )',
  None, "5 Pipeline")
m("DQ Match", 'IF ( ISBLANK ( [DQ Injected] ), BLANK (), IF ( [DQ Rows] = [DQ Injected], "✓", "✗" ) )', None, "5 Pipeline")
m("Removed Share", "DIVIDE ( [Raw Rows] - [Clean Trips], [Raw Rows] )", PCT, "5 Pipeline")
m("Batch Rows In", "SUM ( pipeline_batches[rows_in] )", INT, "5 Pipeline")
m("Batch Inserted", "SUM ( pipeline_batches[inserted] )", INT, "5 Pipeline")
m("Batch Updated", "SUM ( pipeline_batches[updated] )", INT, "5 Pipeline")
m("Batch Rejected", "SUM ( pipeline_batches[rejected] )", INT, "5 Pipeline")
m("Batch Duplicates", "SUM ( pipeline_batches[exact_duplicates] ) + SUM ( pipeline_batches[same_key_duplicates] )", INT, "5 Pipeline")
m("Removed Color", f'IF ( LEFT ( SELECTEDVALUE ( data_quality[metric] ), 9 ) = "Rejected:", "{BAD}", "{WARN}" )', None, "8 Formatting", hidden=True)
m("Match Color", f'IF ( [DQ Match] = "✓", "{GOOD}", "{BAD}" )', None, "8 Formatting", hidden=True)
# Dynamic text -----------------------------------------------------------------------


def _sel(colref, all_text):
    return (f'IF ( ISFILTERED ( {colref} ), CONCATENATEX ( VALUES ( {colref} ), {colref}, ", ", {colref}, ASC ), '
            f'"{all_text}" )')


m("Filter Context",
  f'{_sel("dim_month[month]", "Jan – Jun 2025")} & "   ·   " & {_sel("dim_borough[borough]", "All boroughs")} & '
  f'"   ·   " & {_sel("dim_daytype[day_type]", "All days")}', None, "7 Dynamic text")
m("Blank Text", '""', None, "9 Helpers", hidden=True)
m("Growth Label", 'FORMAT ( [Trips per Day Growth], "▲ 0.0%;▼ 0.0%;● 0.0%" ) & " trips/day, first to last month"', None, "9 Helpers", hidden=True)
m("Growth Color", f'IF ( [Trips per Day Growth] >= 0, "{GOOD}", "{BAD}" )', None, "8 Formatting", hidden=True)
m("Note Revenue", 'FORMAT ( [Revenue per Day], "$#,0" ) & " per day"', None, "9 Helpers", hidden=True)
m("Note Per Trip", '"Fare " & FORMAT ( [Avg Fare], "$0.00" ) & " + surcharges, tolls, tips"', None, "9 Helpers", hidden=True)
m("Note Speed", 'FORMAT ( [Avg Distance (mi)], "0.0" ) & " mi in " & FORMAT ( [Avg Duration (min)], "0" ) & " min on average"', None, "9 Helpers", hidden=True)
m("Note Tips", '"Card trips only (cash not recorded)"', None, "9 Helpers", hidden=True)
m("Note Peak", "[Peak Hour Label]", None, "9 Helpers", hidden=True)
m("Note Slow", "[Slowest Hour Label]", None, "9 Helpers", hidden=True)
m("Note Late", '"Weekend vs weekday, 0:00–3:00"', None, "9 Helpers", hidden=True)
m("Note Rain", '"Same weekday & month"', None, "9 Helpers", hidden=True)
m("Note Rain Speed", '"Speed on rainy days"', None, "9 Helpers", hidden=True)
m("Note Fee", '"Relative to rest of city"', None, "9 Helpers", hidden=True)
m("Note Share Before", '"1 Jan – 28 Feb"', None, "9 Helpers", hidden=True)
m("Note Share After", '"1 Mar – 30 Jun"', None, "9 Helpers", hidden=True)
m("Note Fees", '"$2.50 per trip touching Downtown"', None, "9 Helpers", hidden=True)
m("Note DT per Day", '"Touching Downtown"', None, "9 Helpers", hidden=True)
m("Note Card", '"Of all trips, selected period"', None, "9 Helpers", hidden=True)
m("Note Airport", 'FORMAT ( [Airport Trip Share], "0.0%" ) & " of trips · " & FORMAT ( [Airport Revenue per Trip], "$0" ) & "/trip"', None, "9 Helpers", hidden=True)
m("Note Top10", '"Share of revenue, of 64 zones"', None, "9 Helpers", hidden=True)
m("Note Raw", '"Rows in the monthly files + re-send"', None, "9 Helpers", hidden=True)
m("Note Rejected", 'FORMAT ( DIVIDE ( [Rejected Rows], [Raw Rows] ), "0.0%" ) & " of raw rows, 6 rules"', None, "9 Helpers", hidden=True)
m("Note Dups", '"+ " & FORMAT ( [Corrected Rows], "#,0" ) & " corrected by vendor"', None, "9 Helpers", hidden=True)
m("Note Clean", '"In silver and gold"', None, "9 Helpers", hidden=True)
m("Title Tooltip", 'FORMAT ( MAX ( dim_date[date] ), "dddd d mmm yyyy" ) & IF ( ISBLANK ( MAX ( dim_date[holiday] ) ), "", "  ·  " & MAX ( dim_date[holiday] ) )',
  None, "7 Dynamic text")
m("Title Daily", '"Trips per day  ·  gold bars are rainy days (" & [Rainy Days] & " of " & COUNTROWS ( dim_date ) & ")"', None, "7 Dynamic text")
m("Title Zone", 'SELECTEDVALUE ( dim_zone[zone], "Multiple zones" ) & "  ·  " & SELECTEDVALUE ( dim_zone[borough], "" ) & " zone detail"',
  None, "7 Dynamic text")
m("Title Fee", '"Downtown trips per other trip, weekly:  fell " & FORMAT ( -[Fee Effect], "0.0%" ) & " after the fee (difference-in-differences)"',
  None, "7 Dynamic text")
m("Weather Label", 'IF ( HASONEVALUE ( dim_date[date] ), MAX ( dim_date[weather] ) & "  ·  " & FORMAT ( [Precipitation (mm)], "0.0" ) & " mm" )',
  None, "7 Dynamic text")
# Formatting -------------------------------------------------------------------------
m("Day Color", f'IF ( MAX ( dim_date[weather] ) = "Rain (2 mm+)", "{GOLD}", "{PRIMARY}" )', None, "8 Formatting", hidden=True)
m("Borough Color", f'SWITCH ( SELECTEDVALUE ( dim_borough[borough] ), "Downtown", "{PRIMARY}", "Airports", "{GOLD}", "{COMPARE}" )',
  None, "8 Formatting", hidden=True)


def q(name):
    return name if name.replace("_", "").isalnum() else f"'{name}'"


def col_tmdl(table, name, dtype, hidden, extra):
    out = [f"\tcolumn {q(name)}", f"\t\tdataType: {dtype}"]
    if hidden:
        out.append("\t\tisHidden")
    for k, v in extra.items():
        out.append(f"\t\t{k}" if v is None else f"\t\t{k}: {v}")
    if dtype == I and "formatString" not in extra:
        out.append("\t\tformatString: 0")
    out += [f"\t\tlineageTag: {gid(table, name)}", "\t\tsummarizeBy: none", f"\t\tsourceColumn: {name}",
            "", "\t\tannotation SummarizationSetBy = User", ""]
    return out


def m_partition(table):
    csv, columns, steps_extra, _ = TABLES[table]
    if csv is None:
        return CUSTOM_M[table]
    types = ", ".join(f'{{"{n}", {MTYPE[t]}}}' for n, t, _, _ in columns if t != B)
    steps = [
        ("Source", f'Csv.Document(File.Contents(DataFolder & "{csv}"), [Delimiter = ",", Encoding = 65001, '
                   'QuoteStyle = QuoteStyle.Csv])'),
        ("Promoted", "Table.PromoteHeaders(Source, [PromoteAllScalars = true])"),
        ("Kept", "Table.SelectColumns(Promoted, {" + ", ".join(f'"{n}"' for n, *_ in columns) + "})"),
        ("Blanks", 'Table.TransformColumns(Kept, {}, each if _ = "" then null else _)'),
        ("Typed", f'Table.TransformColumnTypes(Blanks, {{{types}}}, "en-US")'),
    ]
    prev = "Typed"
    for name, expr in steps_extra:
        steps.append((name, expr.format(prev=prev)))
        prev = name
    lines = [f"    {n} = {e}," for n, e in steps]
    lines[-1] = lines[-1].rstrip(",")
    return ["let"] + lines + ["in", f"    {prev}"]


def tmdl_table(table):
    _, columns, _, derived = TABLES[table]
    out = [f"table {table}", f"\tlineageTag: {gid(table)}", ""]
    if table == "dim_date":
        out.insert(1, "\tdataCategory: Time")
    for c in columns + derived:
        out += col_tmdl(table, *c)
    out += [f"\tpartition {table} = m", "\t\tmode: import", "\t\tsource ="]
    out += ["\t\t\t\t" + line for line in m_partition(table)]
    out += ["", "\tannotation PBI_ResultType = Table", ""]
    return "\n".join(out)


def tmdl_measures():
    out = ["table _Measures", f"\tlineageTag: {gid('_Measures')}", ""]
    for name, dax, fmt, folder, hidden in MEASURES:
        out.append(f"\tmeasure {q(name)} = {dax}")
        if fmt:
            out.append(f"\t\tformatString: {fmt}")
        if hidden:
            out.append("\t\tisHidden")
        out += [f"\t\tdisplayFolder: {folder}", f"\t\tlineageTag: {gid('m', name)}", ""]
    out += ["\tcolumn Value", "\t\tdataType: string", "\t\tisHidden", f"\t\tlineageTag: {gid('_Measures', 'Value')}",
            "\t\tsummarizeBy: none", "\t\tsourceColumn: Value", "",
            "\t\tannotation SummarizationSetBy = Automatic", "",
            "\tpartition _Measures = m", "\t\tmode: import", "\t\tsource =",
            '\t\t\t\t#table(type table [Value = text], {})', "",
            "\tannotation PBI_ResultType = Table", ""]
    return "\n".join(out)


def build_semantic_model():
    if SM.exists():
        shutil.rmtree(SM)
    write_json(SM / "definition.pbism", {
        "$schema": f"{SCHEMA}/item/semanticModel/definitionProperties/1.0.0/schema.json",
        "version": "4.2", "settings": {}})
    write_json(SM / ".platform", {
        "$schema": f"{SCHEMA}/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "SemanticModel", "displayName": NAME},
        "config": {"version": "2.0", "logicalId": gid("sm")}})
    d = SM / "definition"
    write(d / "database.tmdl", "database\n\tcompatibilityLevel: 1600\n\n")
    tables = ["_Measures", *TABLES]
    write(d / "model.tmdl", "\n".join([
        "model Model", "\tculture: en-US", "\tdefaultPowerBIDataSourceVersion: powerBI_V3",
        "\tdiscourageImplicitMeasures", "\tsourceQueryCulture: en-US", "\tdataAccessOptions",
        "\t\tlegacyRedirects", "\t\treturnErrorValuesAsNull", "",
        f"annotation PBI_QueryOrder = {json.dumps(['DataFolder', *tables])}", "",
        "annotation __PBI_TimeIntelligenceEnabled = 0", "",
        *[f"ref table {t}" for t in tables], ""]))
    write(d / "expressions.tmdl", "\n".join([
        f'expression DataFolder = "{DATA_FOLDER}" meta [IsParameterQuery = true, Type = "Text", '
        "IsParameterQueryRequired = true]",
        f"\tlineageTag: {gid('DataFolder')}", "", "\tannotation PBI_ResultType = Text", ""]))
    rels = []
    for ft, fc, tt, tc in RELATIONSHIPS:
        rels += [f"relationship {gid('rel', ft, fc, tt)}"]
        if tc == "date":
            rels += ["\tjoinOnDateBehavior: datePartOnly"]
        rels += [f"\tfromColumn: {ft}.{fc}", f"\ttoColumn: {tt}.{tc}", ""]
    write(d / "relationships.tmdl", "\n".join(rels))
    write(d / "tables" / "_Measures.tmdl", tmdl_measures())
    for t in TABLES:
        write(d / "tables" / f"{t}.tmdl", tmdl_table(t))


# =========================================================================
# Theme
# =========================================================================
def sc(c):
    return {"solid": {"color": c}}


THEME = {
    "name": "Calder Executive",
    "dataColors": DATA_COLORS,
    "foreground": INK, "foregroundNeutralSecondary": SLATE, "foregroundNeutralTertiary": SLATE_2,
    "background": WHITE, "backgroundLight": "#F6F8FB", "backgroundNeutral": LINE,
    "tableAccent": PRIMARY, "good": GOOD, "neutral": WARN, "bad": BAD,
    "maximum": PRIMARY, "center": "#7FA3C7", "minimum": "#EAF0F7", "null": "#F1F3F7",
    "hyperlink": PRIMARY, "visitedHyperlink": PRIMARY,
    "textClasses": {
        "callout": {"fontSize": 24, "fontFace": FONT_SB, "color": INK},
        "title": {"fontSize": 11, "fontFace": FONT_SB, "color": INK},
        "header": {"fontSize": 10, "fontFace": FONT_SB, "color": "#334155"},
        "label": {"fontSize": 9, "fontFace": FONT, "color": "#475569"},
    },
    "visualStyles": {
        "*": {"*": {
            "background": [{"show": True, "color": sc(WHITE), "transparency": 0}],
            "border": [{"show": True, "color": sc(LINE), "radius": 12, "width": 1}],
            "dropShadow": [{"show": True, "color": sc(NAVY), "position": "Outer", "preset": "Custom",
                            "transparency": 90, "shadowBlur": 14, "shadowDistance": 3, "shadowSpread": 0,
                            "angle": 90}],
            "padding": [{"top": 12, "bottom": 10, "left": 14, "right": 14}],
            "title": [{"show": True, "fontColor": sc(INK), "fontSize": 11, "fontFamily": FONT_SB,
                       "titleWrap": True}],
            "subTitle": [{"fontColor": sc(SLATE), "fontSize": 9, "fontFamily": FONT}],
            "categoryAxis": [{"showAxisTitle": False, "gridlineStyle": "dotted", "labelColor": sc(SLATE),
                              "fontSize": 9, "concatenateLabels": False}],
            "valueAxis": [{"showAxisTitle": False, "gridlineStyle": "dotted", "gridlineColor": sc(LINE),
                           "labelColor": sc(SLATE), "fontSize": 9}],
            "legend": [{"show": True, "position": "TopLeft", "labelColor": sc("#475569"), "fontSize": 9,
                        "showTitle": False}],
            "labels": [{"color": sc("#334155"), "fontSize": 9}],
            "lineStyles": [{"strokeWidth": 3}],
            "outspacePane": [{"backgroundColor": sc(WHITE), "foregroundColor": sc(INK), "transparency": 0,
                              "border": True, "borderColor": sc(LINE)}],
        }},
        "page": {"*": {"background": [{"color": sc(CANVAS), "transparency": 0}],
                       "outspace": [{"color": sc("#E7EBF1"), "transparency": 0}]}},
        "card": {"*": {"labels": [{"color": sc(INK), "fontSize": 24, "fontFamily": FONT_SB}],
                       "categoryLabels": [{"show": False}],
                       "title": [{"alignment": "center", "fontSize": 9, "fontColor": sc(SLATE),
                                  "fontFamily": FONT_SB}]}},
        "tableEx": {"*": {
            "grid": [{"gridVertical": False, "gridHorizontal": True, "gridHorizontalColor": sc("#EEF1F6"),
                      "outlineColor": sc(LINE), "rowPadding": 5}],
            "columnHeaders": [{"fontColor": sc("#475569"), "backColor": sc("#F6F8FB"), "fontFamily": FONT_SB,
                               "fontSize": 9, "wordWrap": True}],
            "values": [{"fontColorPrimary": sc("#1E293B"), "backColorPrimary": sc(WHITE),
                        "fontColorSecondary": sc("#1E293B"), "backColorSecondary": sc("#FAFBFD"), "fontSize": 9}],
            "total": [{"fontFamily": FONT_SB, "backColor": sc("#F6F8FB"), "fontColor": sc(INK)}]}},
        "pivotTable": {"*": {
            "grid": [{"gridVertical": False, "gridHorizontal": True, "gridHorizontalColor": sc("#EEF1F6"),
                      "outlineColor": sc(LINE), "rowPadding": 5}],
            "columnHeaders": [{"fontColor": sc("#475569"), "backColor": sc("#F6F8FB"), "fontFamily": FONT_SB,
                               "fontSize": 9}],
            "rowHeaders": [{"fontColor": sc("#1E293B"), "fontSize": 9, "showExpandCollapseButtons": False,
                            "legacyStyleDisabled": True}],
            "values": [{"fontColorPrimary": sc("#1E293B"), "backColorPrimary": sc(WHITE),
                        "backColorSecondary": sc(WHITE), "fontSize": 9}],
            "subTotals": [{"fontFamily": FONT_SB, "backgroundColor": sc("#F6F8FB")}]}},
        "slicer": {"*": {
            "header": [{"show": True, "fontColor": sc(MIST), "textSize": 8, "fontFamily": FONT_SB,
                        "outline": "None"}],
            "items": [{"fontColor": sc(INK), "background": sc(WHITE), "textSize": 9, "outline": "None"}],
            "background": [{"show": False}], "border": [{"show": False}], "dropShadow": [{"show": False}],
            "padding": [{"top": 0, "bottom": 0, "left": 0, "right": 0}]}},
        "textbox": {"*": {"background": [{"show": False}], "border": [{"show": False}],
                          "dropShadow": [{"show": False}], "padding": [{"top": 0, "bottom": 0, "left": 0,
                                                                         "right": 0}]}},
        "actionButton": {"*": {"background": [{"show": False}], "border": [{"show": False}],
                               "dropShadow": [{"show": False}], "visualHeader": [{"show": False}]}},
    },
}


# =========================================================================
# Report helpers (PBIR)
# =========================================================================
def lit(v):
    if isinstance(v, bool):
        s = "true" if v else "false"
    elif isinstance(v, (int, float)):
        s = f"{v}D"
    else:
        s = "'" + str(v).replace("'", "''") + "'"
    return {"expr": {"Literal": {"Value": s}}}


def lit_int(v):
    return {"expr": {"Literal": {"Value": f"{v}L"}}}


def col(table, column):
    return {"Column": {"Expression": {"SourceRef": {"Entity": table}}, "Property": column}}


def mea(name):
    assert any(x[0] == name for x in MEASURES), f"unknown measure {name}"
    return {"Measure": {"Expression": {"SourceRef": {"Entity": "_Measures"}}, "Property": name}}


def ref(field):
    k = next(iter(field))
    return f"{field[k]['Expression']['SourceRef']['Entity']}.{field[k]['Property']}"


def proj(field, display=None):
    p = {"field": field, "queryRef": ref(field), "nativeQueryRef": field[next(iter(field))]["Property"]}
    if display:
        p["displayName"] = display
    return p


def solid(color):
    """Colour from a hex string or from a measure (conditional formatting)."""
    return {"solid": {"color": lit(color) if isinstance(color, str) else {"expr": color}}}


def text_or_measure(v):
    return lit(v) if isinstance(v, str) else {"expr": v}


def plain_container(**over):
    """No card chrome: for elements that sit on the sidebar or overlay a tile."""
    o = {"background": [{"properties": {"show": lit(False)}}],
         "border": [{"properties": {"show": lit(False)}}],
         "dropShadow": [{"properties": {"show": lit(False)}}],
         "visualHeader": [{"properties": {"show": lit(False)}}],
         "title": [{"properties": {"show": lit(False)}}],
         "padding": [{"properties": {"top": lit(0), "bottom": lit(0), "left": lit(0), "right": lit(0)}}]}
    o.update(over)
    return o


class Page:
    def __init__(self, key, title, kind=None, width=1280, height=720):
        self.key, self.name, self.title = key, hid("page", key), title
        self.kind, self.width, self.height = kind, width, height
        self.visuals = []

    def add(self, vid, x, y, w, h, visual, extra=None):
        v = {"$schema": f"{SCHEMA}/item/report/definition/visualContainer/{V_VISUAL}/schema.json",
             "name": hid(self.key, vid),
             "position": {"x": round(x, 2), "y": round(y, 2), "z": len(self.visuals) * 100,
                          "height": round(h, 2), "width": round(w, 2), "tabOrder": len(self.visuals) * 100},
             "visual": visual}
        if extra:
            v.update(extra)
        self.visuals.append(v)


def visual(vtype, roles=None, title=None, subtitle=None, sort=None, objects=None, container=None,
           tooltip_page=None):
    v = {"visualType": vtype, "drillFilterOtherVisuals": True}
    if roles:
        v["query"] = {"queryState": {r: {"projections": [proj(f) if "Column" in f or "Measure" in f
                                                         else proj(*f) for f in fs]}
                                     for r, fs in roles.items()}}
        if sort:
            v["query"]["sortDefinition"] = {"sort": [{"field": f, "direction": d} for f, d in sort],
                                            "isDefaultSort": False}
    if objects:
        v["objects"] = objects
    vco = dict(container or {})
    if title is not None:
        vco.setdefault("title", [{"properties": {"show": lit(True), "text": text_or_measure(title)}}])
    if subtitle:
        vco["subTitle"] = [{"properties": {"show": lit(True), "text": lit(subtitle)}}]
    if tooltip_page:
        vco["visualTooltip"] = [{"properties": {"show": lit(True), "type": lit("ReportPage"),
                                                "section": lit(tooltip_page)}}]
    if vco:
        v["visualContainerObjects"] = vco
    return v


def textbox(paragraphs, container=None, align="left"):
    """paragraphs: list of lists of (text, size, color, bold) runs."""
    paras = []
    for runs in paragraphs:
        paras.append({"horizontalTextAlignment": align, "textRuns": [{"value": t, "textStyle": {
            "fontSize": f"{size}pt", "color": color, "fontFamily": FONT_SB if bold else FONT,
            **({"fontWeight": "bold"} if bold else {})}} for t, size, color, bold in runs]})
    return {"visualType": "textbox", "drillFilterOtherVisuals": True,
            "objects": {"general": [{"properties": {"paragraphs": paras}}]},
            "visualContainerObjects": container or plain_container()}


def panel(color, radius=0):
    """A coloured rectangle (sidebar, accents) made from an empty textbox container."""
    return {"visualType": "textbox", "drillFilterOtherVisuals": True,
            "objects": {"general": [{"properties": {"paragraphs": [{"textRuns": [{"value": ""}]}]}}]},
            "visualContainerObjects": plain_container(
                background=[{"properties": {"show": lit(True), "color": solid(color), "transparency": lit(0)}}],
                border=[{"properties": {"show": lit(radius > 0), "color": solid(color), "radius": lit(radius)}}])}


def card(measure, *, size=24, color=INK, display=None, millions=False, exact=False, title=None,
         container=None, font=FONT_SB):
    lab = {"color": solid(color), "fontSize": lit(size), "fontFamily": lit(font)}
    if millions:
        lab.update(labelDisplayUnits=lit(1000000), labelPrecision=lit_int(2))
    elif exact:
        lab.update(labelDisplayUnits=lit(1))
    return visual("card", {"Values": [(mea(measure), display)] if display else [mea(measure)]}, title=title,
                  objects={"labels": [{"properties": lab}],
                           "categoryLabels": [{"properties": {"show": lit(False)}}]},
                  container=container)


def series_colors(colors):
    return [{"properties": {"fill": solid(c)}, "selector": {"metadata": ref(mea(n))}} for n, c in colors]


def measure_fill(name):
    return [{"properties": {"fill": solid(mea(name))},
             "selector": {"data": [{"dataViewWildcard": {"matchingOption": 1}}]}}]


def cf(measure_ref, *, back=None, font=None):
    """Conditional formatting on one value column of a table/matrix (field-value rule)."""
    props = {}
    if back:
        props["backColor"] = solid(mea(back))
    if font:
        props["fontColor"] = solid(mea(font))
    return {"properties": props,
            "selector": {"data": [{"dataViewWildcard": {"matchingOption": 1}}], "metadata": measure_ref}}


def labels(show=True, **extra):
    extra.setdefault("labelPosition", "OutsideEnd")
    return [{"properties": {"show": lit(show), **{k: (lit(v) if not isinstance(v, dict) else v)
                                                   for k, v in extra.items()}}}]


# =========================================================================
# Layout
# =========================================================================
W, H = 1280, 720
SIDEBAR_W = 208
X0, X1 = 224, 1264          # content area
GAP = 12
NAV = [("overview", "Overview"), ("demand", "Demand & Traffic"), ("fee", "Downtown Fee"),
       ("payments", "Payments & Zones"), ("pipeline", "Data Pipeline")]
SLICERS = [(col("dim_month", "month"), "MONTH", "month"),
           (col("dim_borough", "borough"), "PICK-UP BOROUGH", "borough"),
           (col("dim_daytype", "day_type"), "DAY TYPE", "daytype")]
PAGE_NAMES = {k: hid("page", k) for k, _ in NAV}
PAGE_NAMES["zone"] = hid("page", "zone")
PAGE_NAMES["tooltip"] = hid("page", "tooltip")


def sidebar(p, active):
    p.add("sb_bg", 0, 0, SIDEBAR_W, H, panel(NAVY))
    p.add("sb_brand", 20, 22, 180, 52, textbox([
        [("CALDER TAXI", 13, GOLD, True)],
        [("Lakehouse Analytics", 9, SLATE_2, False)]]))
    p.add("sb_rule", 20, 82, 168, 1, panel("#22395A"))
    p.add("sb_navlabel", 20, 94, 168, 16, textbox([[("DASHBOARDS", 7, "#7C8DA6", True)]]))
    for i, (key, label) in enumerate(NAV):
        y = 114 + i * 40
        is_active = key == active
        if is_active:
            p.add(f"nav_bg_{key}", 12, y, 184, 34, panel(NAVY_2, radius=8))
            p.add(f"nav_mark_{key}", 12, y + 8, 3, 18, panel(GOLD, radius=1))
        p.add(f"nav_label_{key}", 28, y + 8, 160, 20, textbox(
            [[(label, 10, WHITE if is_active else "#A9B6C9", is_active)]]))
        p.add(f"nav_{key}", 12, y, 184, 34, {
            "visualType": "actionButton", "drillFilterOtherVisuals": True,
            "objects": {
                "icon": [{"properties": {"show": lit(False)}}, {"properties": {"show": lit(False)}, "selector": {"id": "default"}}],
                "text": [{"properties": {"show": lit(False)}}, {"properties": {"show": lit(False)}, "selector": {"id": "default"}}],
                "fill": [{"properties": {"show": lit(False)}}, {"properties": {"show": lit(False)}, "selector": {"id": "default"}}],
                "outline": [{"properties": {"show": lit(False)}}, {"properties": {"show": lit(False)}, "selector": {"id": "default"}}]},
            "visualContainerObjects": {
                **plain_container(),
                "visualLink": [{"properties": {"show": lit(True), "type": lit("PageNavigation"),
                                               "navigationSection": lit(PAGE_NAMES[key])}}]}})
    y2 = 114 + len(NAV) * 40 + 10
    p.add("sb_rule2", 20, y2, 168, 1, panel("#22395A"))
    p.add("sb_filterlabel", 20, y2 + 12, 168, 16, textbox([[("FILTERS", 7, "#7C8DA6", True)]]))
    for i, (f, label, g) in enumerate(SLICERS):
        y = y2 + 30 + i * 58
        v = visual("slicer", {"Values": [(f, label)]}, objects={
            "data": [{"properties": {"mode": lit("Dropdown")}}],
            "header": [{"properties": {"show": lit(True), "fontColor": solid(MIST), "textSize": lit(8),
                                       "fontFamily": lit(FONT_SB)}}],
            "items": [{"properties": {"fontColor": solid(INK), "background": solid(WHITE), "textSize": lit(9)}}]},
            container=plain_container())
        v["syncGroup"] = {"groupName": g, "fieldChanges": True, "filterChanges": True}
        p.add(f"slicer_{g}", 20, y, 168, 54, v)
    p.add("sb_foot", 20, 650, 176, 56, textbox([
        [("Synthetic data · 3.5M trips · Jan – Jun 2025", 7, "#7C8DA6", False)],
        [("Ctrl + click a menu item in Desktop", 7, "#7C8DA6", False)],
        [("Right-click a zone to drill through", 7, "#7C8DA6", False)]]))


def header(p, title):
    p.add("hdr_title", X0, 14, 620, 34, textbox([[(title, 18, INK, True)]]))
    p.add("hdr_context", X0, 46, 900, 22, visual(
        "card", {"Values": [mea("Blank Text")]},
        objects={"labels": [{"properties": {"fontSize": lit(1), "color": solid(CANVAS)}}],
                 "categoryLabels": [{"properties": {"show": lit(False)}}]},
        container=plain_container(title=[{"properties": {
            "show": lit(True), "text": {"expr": mea("Filter Context")}, "fontColor": solid(SLATE),
            "fontSize": lit(9), "fontFamily": lit(FONT), "alignment": lit("left")}}])))


def frame():
    """White rounded card with shadow (the tile background)."""
    return {"visualType": "textbox", "drillFilterOtherVisuals": True,
            "objects": {"general": [{"properties": {"paragraphs": [{"textRuns": [{"value": ""}]}]}}]},
            "visualContainerObjects": plain_container(
                background=[{"properties": {"show": lit(True), "color": solid(WHITE), "transparency": lit(0)}}],
                border=[{"properties": {"show": lit(True), "color": solid(LINE), "radius": lit(12)}}],
                dropShadow=[{"properties": {"show": lit(True), "color": solid(NAVY), "position": lit("Outer"),
                                            "preset": lit("Custom"), "transparency": lit(90), "shadowBlur": lit(14),
                                            "shadowDistance": lit(3), "shadowSpread": lit(0), "angle": lit(90)}}])}


def kpi_tiles(p, tiles, y=76, h=104):
    """tiles: (value measure, label, sub-measure, sub-colour measure or hex, number style)."""
    n = len(tiles)
    w = (X1 - X0 - (n - 1) * GAP) / n
    for i, (val, label, sub, sub_color, style) in enumerate(tiles):
        x = X0 + i * (w + GAP)
        p.add(f"kpi_{i}_frame", x, y, w, h, frame())
        p.add(f"kpi_{i}_rule", x + w / 2 - 14, y + 12, 28, 3, panel(GOLD))
        p.add(f"kpi_{i}_label", x + 8, y + 20, w - 16, 18,
              textbox([[(label.upper(), 7.5, SLATE, True)]], align="center"))
        p.add(f"kpi_{i}_value", x + 6, y + 38, w - 12, 40, card(
            val, size=22, millions=style == "M", exact=style == "N", container=plain_container()))
        is_hex = sub_color.startswith("#")
        sub_card = card(sub, size=8.5, font=FONT_SB, color=sub_color if is_hex else SLATE,
                        container=plain_container())
        if not is_hex:          # colour from a measure: teal = better, terracotta = worse
            sub_card["objects"]["labels"][0]["properties"]["color"] = solid(mea(sub_color))
        p.add(f"kpi_{i}_sub", x + 6, y + 78, w - 12, 20, sub_card)


# =========================================================================
# Pages
# =========================================================================
ZONE = (col("dim_zone", "zone"), "Zone")
DATE = (col("dim_date", "date"), "Date")
CW = X1 - X0                      # content width


def heat_cf(value_measure, back, font):
    return [cf(ref(mea(value_measure)), back=back, font=font)]


def page_overview():
    p = Page("overview", "Overview")
    sidebar(p, "overview")
    header(p, "Calder City Taxi  ·  Jan – Jun 2025")
    kpi_tiles(p, [
        ("Trips", "Trips", "Growth Label", "Growth Color", "N"),
        ("Revenue", "Revenue", "Note Revenue", SLATE, "M"),
        ("Revenue per Trip", "Revenue per trip", "Note Per Trip", SLATE, ""),
        ("Avg Speed (mph)", "Average speed (mph)", "Note Speed", SLATE, ""),
        ("Card Tip %", "Card tip (% of fare)", "Note Tips", SLATE, "")])
    p.add("daily", X0, 192, CW, 250, visual(
        "clusteredColumnChart", {"Category": [DATE], "Y": [(mea("Trips"), "Trips")],
                                 "Tooltips": [(mea("Revenue"), "Revenue"), (mea("Precipitation (mm)"), "Rain (mm)")]},
        title=mea("Title Daily"), subtitle="Weekly rhythm: Friday peaks, Sunday dips · hover a day for weather and holiday",
        objects={"dataPoint": measure_fill("Day Color"),
                 "categoryAxis": [{"properties": {"axisType": lit("Scalar")}}]},
        tooltip_page=PAGE_NAMES["tooltip"]))
    p.add("borough", X0, 454, 400, 250, visual(
        "clusteredBarChart", {"Category": [(col("dim_borough", "borough"), "Borough")],
                              "Y": [(mea("Revenue"), "Revenue")],
                              "Tooltips": [(mea("Trips"), "Trips"), (mea("Revenue per Trip"), "Revenue per trip")]},
        title="Revenue by pick-up borough", subtitle="Downtown and the airports carry the business",
        sort=[(mea("Revenue"), "Descending")],
        objects={"dataPoint": measure_fill("Borough Color"),
                 "labels": labels(True, labelDisplayUnits=1000000, labelPrecision=lit_int(1))}))
    p.add("zones", X0 + 400 + GAP, 454, CW - 400 - GAP, 250, visual(
        "tableEx", {"Values": [(mea("Zone Revenue Rank"), "#"), ZONE, (col("dim_zone", "borough"), "Borough"),
                               (mea("Trips"), "Trips"), (mea("Revenue"), "Revenue"), (mea("Revenue per Trip"), "Per trip"),
                               (mea("Avg Speed (mph)"), "Speed")]},
        title="Pick-up zones ranked by revenue", subtitle="Right-click a zone → Drill through → Zone Detail",
        sort=[(mea("Revenue"), "Descending")],
        objects={"columnWidth": [{"properties": {"value": lit(w)}, "selector": {"metadata": r}}
                                 for r, w in [(ref(mea("Zone Revenue Rank")), 36), (ref(col("dim_zone", "zone")), 170)]]}))
    return p


def page_demand():
    p = Page("demand", "Demand & Traffic")
    sidebar(p, "demand")
    header(p, "When People Ride, and How Fast Traffic Moves")
    kpi_tiles(p, [
        ("Peak Trips per Hour", "Peak trips per hour", "Note Peak", SLATE, "N"),
        ("Slowest Speed (mph)", "Slowest traffic (mph)", "Note Slow", SLATE, ""),
        ("Late Night Weekend Multiple", "Weekend late-night demand", "Note Late", SLATE, ""),
        ("Rain Demand Effect", "Rain: trips", "Note Rain", GOOD, ""),
        ("Rain Speed Effect", "Rain: traffic speed", "Note Rain Speed", BAD, "")])
    HOUR = (col("fact_hourly", "hour"), "Hour")
    DAYT = (col("dim_daytype", "day_type"), "Day type")
    HOURN = (col("fact_hourly", "pickup_hour"), "Hour of day")
    p.add("heat", X0, 192, CW, 158, visual(
        "pivotTable", {"Rows": [DAYT], "Columns": [HOUR], "Values": [(mea("Trips per Hour"), "Trips per hour")]},
        title="Trips per hour on an average day", subtitle="Darker = busier · weekday evening rush vs weekend nightlife",
        sort=[(col("fact_hourly", "hour"), "Ascending")],
        objects={"values": heat_cf("Trips per Hour", "Hour Heat", "Hour Heat Font"),
                 "subTotals": [{"properties": {"rowSubtotals": lit(False), "columnSubtotals": lit(False)}}],
                 "columnHeaders": [{"properties": {"fontSize": lit(7)}}]}))
    p.add("hourly", X0, 362, 512, 342, visual(
        "lineChart", {"Category": [HOURN], "Series": [DAYT], "Y": [(mea("Trips per Hour"), "Trips per hour")]},
        title="Demand curve by hour", subtitle="Weekdays: two commuter peaks · weekends: late-night trips",
        sort=[(col("fact_hourly", "pickup_hour"), "Ascending")],
        objects={"categoryAxis": [{"properties": {"axisType": lit("Scalar")}}],"dataPoint": [{"properties": {"fill": solid(PRIMARY)}, "selector": {"data": [{"scopeId": {"Comparison": {
                     "ComparisonKind": 0, "Left": col("dim_daytype", "day_type"), "Right": {"Literal": {"Value": "'Weekday'"}}}}}]}},
                                {"properties": {"fill": solid(GOLD)}, "selector": {"data": [{"scopeId": {"Comparison": {
                     "ComparisonKind": 0, "Left": col("dim_daytype", "day_type"), "Right": {"Literal": {"Value": "'Weekend'"}}}}}]}}]}))
    p.add("speed", X0 + 512 + GAP, 362, CW - 512 - GAP, 166, visual(
        "lineChart", {"Category": [HOURN], "Series": [DAYT], "Y": [(mea("Speed by Hour (mph)"), "Speed (mph)")]},
        title="Average traffic speed by hour (mph)", subtitle="Rush hours are the slowest",
        sort=[(col("fact_hourly", "pickup_hour"), "Ascending")],
        objects={"categoryAxis": [{"properties": {"axisType": lit("Scalar")}}],
"dataPoint": [{"properties": {"fill": solid(PRIMARY)}, "selector": {"data": [{"scopeId": {"Comparison": {
                     "ComparisonKind": 0, "Left": col("dim_daytype", "day_type"), "Right": {"Literal": {"Value": "'Weekday'"}}}}}]}},
                                {"properties": {"fill": solid(GOLD)}, "selector": {"data": [{"scopeId": {"Comparison": {
                     "ComparisonKind": 0, "Left": col("dim_daytype", "day_type"), "Right": {"Literal": {"Value": "'Weekend'"}}}}}]}}]}))
    p.add("rain", X0 + 512 + GAP, 540, CW - 512 - GAP, 164, visual(
        "clusteredColumnChart", {"Category": [(col("dim_date", "weekday"), "Weekday")],
                                 "Y": [(mea("Trips per Day (Dry)"), "Dry"), (mea("Trips per Day (Rain)"), "Rain (2 mm+)")]},
        title="Trips per day: dry vs rainy days", subtitle="Holidays excluded · rain lifts demand every weekday",
        sort=[(col("dim_date", "weekday"), "Ascending")],
        objects={"dataPoint": series_colors([("Trips per Day (Dry)", COMPARE), ("Trips per Day (Rain)", PRIMARY)])}))
    return p


def page_fee():
    p = Page("fee", "Downtown Fee")
    sidebar(p, "fee")
    header(p, "Did the Downtown Congestion Fee Reduce Trips?")
    kpi_tiles(p, [
        ("Fee Effect", "Effect on Downtown trips", "Note Fee", BAD, ""),
        ("Downtown Share Before", "Downtown share, before", "Note Share Before", SLATE, ""),
        ("Downtown Share After", "Downtown share, after", "Note Share After", SLATE, ""),
        ("Fees Collected", "Fees collected", "Note Fees", SLATE, "M"),
        ("Downtown Trips per Day", "Downtown trips per day", "Note DT per Day", SLATE, "N")])
    p.add("ratio", X0, 192, CW, 262, visual(
        "lineChart", {"Category": [(col("dim_date", "week_start"), "Week starting")],
                      "Y": [(mea("Downtown Ratio Before Fee"), "Before the fee"), (mea("Downtown Ratio After Fee"), "After the fee")]},
        title=mea("Title Fee"),
        subtitle="Ratio of trips touching Downtown to all other trips · weather, weekday and growth affect both, so they cancel",
        objects={"dataPoint": series_colors([("Downtown Ratio Before Fee", NAVY), ("Downtown Ratio After Fee", BAD)]),
                 "categoryAxis": [{"properties": {"axisType": lit("Scalar")}}],
                 "valueAxis": [{"properties": {"start": lit(0.9)}}]}))
    SEG = (col("fact_downtown", "segment"), "Segment")
    p.add("index", X0, 466, 520, 238, visual(
        "lineChart", {"Category": [(col("dim_month", "month"), "Month")], "Series": [SEG],
                      "Y": [(mea("Segment Index (Jan-Feb = 100)"), "Trips per day, Jan–Feb = 100")]},
        title="Trips per day indexed to Jan–Feb = 100", subtitle="Both grow with the season; Downtown falls behind from March",
        sort=[(col("dim_month", "month"), "Ascending")],
        objects={"dataPoint": [{"properties": {"fill": solid(PRIMARY)}, "selector": {"data": [{"scopeId": {"Comparison": {
                     "ComparisonKind": 0, "Left": col("fact_downtown", "segment"), "Right": {"Literal": {"Value": "'Touches Downtown'"}}}}}]}},
                                {"properties": {"fill": solid(COMPARE)}, "selector": {"data": [{"scopeId": {"Comparison": {
                     "ComparisonKind": 0, "Left": col("fact_downtown", "segment"), "Right": {"Literal": {"Value": "'Elsewhere'"}}}}}]}}],
                 "labels": labels(True, labelPrecision=lit_int(0), labelPosition="Above")}))
    p.add("table", X0 + 520 + GAP, 466, CW - 520 - GAP, 238, visual(
        "pivotTable", {"Rows": [(col("dim_date", "fee_period"), "Period")], "Columns": [SEG],
                       "Values": [(mea("Segment Trips per Day"), "Trips/day")]},
        title="Trips per day, before and after", subtitle="Elsewhere keeps growing; Downtown barely moves",
        sort=[(col("dim_date", "fee_period"), "Descending")],
        objects={"subTotals": [{"properties": {"rowSubtotals": lit(False), "columnSubtotals": lit(False)}}]}))
    return p


def page_payments():
    p = Page("payments", "Payments & Zones")
    sidebar(p, "payments")
    header(p, "Payments, Tips, Airports and Trip Flows")
    kpi_tiles(p, [
        ("Card Share", "Paid by card", "Note Card", SLATE, ""),
        ("Card Tip %", "Card tip (% of fare)", "Note Tips", SLATE, ""),
        ("Airport Revenue Share", "Airport share of revenue", "Note Airport", SLATE, ""),
        ("Top 10 Zone Share", "Top 10 zones", "Note Top10", SLATE, ""),
        ("Avg Fare", "Average fare", "Note Per Trip", SLATE, "")])
    PAY = (col("fact_payment", "payment_desc"), "Payment type")
    p.add("pay", X0, 192, 330, 250, visual(
        "clusteredBarChart", {"Category": [PAY], "Y": [(mea("Payment Share"), "Share of trips")],
                              "Tooltips": [(mea("Payment Trips"), "Trips"), (mea("Tip % of Fare"), "Tip % of fare")]},
        title="How riders pay", subtitle="Share of trips",
        sort=[(mea("Payment Share"), "Descending")],
        objects={"dataPoint": [{"properties": {"fill": solid(PRIMARY)}}], "labels": labels(True, labelPrecision=lit_int(1))}))
    p.add("cardtrend", X0 + 330 + GAP, 192, 330, 250, visual(
        "lineChart", {"Category": [(col("dim_month", "month"), "Month")], "Y": [(mea("Card Share (Payments)"), "Card share")]},
        title="Card share by month", subtitle="Riders keep moving from cash to card",
        sort=[(col("dim_month", "month"), "Ascending")],
        objects={"dataPoint": [{"properties": {"fill": solid(PRIMARY)}}], "labels": labels(True, labelPrecision=lit_int(1), labelPosition="Above")}))
    p.add("tips", X0 + 660 + 2 * GAP, 192, CW - 660 - 2 * GAP, 250, visual(
        "tableEx", {"Values": [PAY, (mea("Payment Trips"), "Trips"), (mea("Tip % of Fare"), "Tip % of fare")]},
        title="Tips by payment type", subtitle="Cash tips are not recorded by the meter",
        sort=[(mea("Payment Trips"), "Descending")]))
    p.add("flows", X0, 454, 620, 250, visual(
        "pivotTable", {"Rows": [(col("dim_borough", "borough"), "From")], "Columns": [(col("fact_flows", "do_borough"), "To")],
                       "Values": [(mea("Flow Share"), "Share of trips")]},
        title="Where trips go: pick-up borough → drop-off borough", subtitle="Share of all trips · darker = more",
        objects={"values": heat_cf("Flow Share", "Flow Heat", "Flow Heat Font"),
                 "subTotals": [{"properties": {"rowSubtotals": lit(False), "columnSubtotals": lit(False)}}]}))
    p.add("perTrip", X0 + 620 + GAP, 454, CW - 620 - GAP, 250, visual(
        "clusteredBarChart", {"Category": [(col("dim_borough", "borough"), "Borough")], "Y": [(mea("Revenue per Trip"), "Revenue per trip")]},
        title="Revenue per trip by pick-up borough", subtitle="Airport trips are long and carry flat fares and tolls",
        sort=[(mea("Revenue per Trip"), "Descending")],
        objects={"dataPoint": measure_fill("Borough Color"), "labels": labels(True, labelPrecision=lit_int(2))}))
    return p


def page_pipeline():
    p = Page("pipeline", "Data Pipeline")
    sidebar(p, "pipeline")
    header(p, "Data Pipeline  ·  bronze → silver → gold")
    kpi_tiles(p, [
        ("Raw Rows", "Raw rows received", "Note Raw", SLATE, "N"),
        ("Rejected Rows", "Rejected (with reason)", "Note Rejected", BAD, "N"),
        ("Duplicate Rows", "Duplicates removed", "Note Dups", WARN, "N"),
        ("Clean Trips", "Clean trips", "Note Clean", SLATE, "N"),
        ("Removed Share", "Rows removed", "Reconciled Label", GOOD, "")])
    METRIC = (col("data_quality", "metric"), "Check")
    p.add("removed", X0, 192, 500, 300, visual(
        "clusteredBarChart", {"Category": [METRIC], "Y": [(mea("Removed Rows (Chart)"), "Rows")],
                              "Tooltips": [(mea("DQ Injected"), "Injected by the generator")]},
        title="Why rows were removed in silver", subtitle="Red = failed a validation rule · orange = duplicate or corrected",
        sort=[(mea("Removed Rows (Chart)"), "Descending")],
        objects={"dataPoint": measure_fill("Removed Color"), "labels": labels(True, labelDisplayUnits=1),
                 "categoryAxis": [{"properties": {"fontSize": lit(8), "maxMarginFactor": lit(45)}}]}))
    p.add("checks", X0 + 500 + GAP, 192, CW - 500 - GAP, 300, visual(
        "tableEx", {"Values": [(col("data_quality", "layer"), "Layer"), METRIC, (mea("DQ Rows"), "Rows found"),
                               (mea("DQ Injected"), "Injected"), (mea("DQ Match"), "Match")]},
        title="Every step, checked against what the generator injected",
        subtitle="Synthetic data: the true number of bad rows is known",
        sort=[(col("data_quality", "metric"), "Ascending")],
        objects={"values": [cf(ref(mea("DQ Match")), font="Match Color")],
                 "total": [{"properties": {"totals": lit(False)}}]}))
    p.add("batches", X0, 504, CW, 200, visual(
        "tableEx", {"Values": [(col("pipeline_batches", "run"), "Run"), (col("pipeline_batches", "files"), "Files loaded"),
                               (mea("Batch Rows In"), "Rows in"), (mea("Batch Duplicates"), "Duplicates"),
                               (mea("Batch Rejected"), "Rejected"), (mea("Batch Inserted"), "Inserted"),
                               (mea("Batch Updated"), "Updated"), (col("pipeline_batches", "silver_version"), "Silver version"),
                               (col("pipeline_batches", "months_touched"), "Gold months rebuilt")]},
        title="Pipeline runs: only new files are read; corrections update rows in place (Delta MERGE)",
        subtitle="A third run with no new files changed nothing",
        sort=[(col("pipeline_batches", "run"), "Ascending")]))
    return p


def page_zone():
    p = Page("zone", "Zone Detail", kind="Drillthrough")
    sidebar(p, None)
    p.add("hdr_title", X0 + 48, 14, 760, 34, card("Title Zone", size=1, container=plain_container(
        title=[{"properties": {"show": lit(True), "text": {"expr": mea("Title Zone")}, "fontColor": solid(INK),
                               "fontSize": lit(18), "fontFamily": lit(FONT_SB), "alignment": lit("left")}}])))
    p.visuals[-1]["visual"]["objects"]["labels"][0]["properties"]["color"] = solid(CANVAS)
    p.add("back", X0, 16, 36, 30, {
        "visualType": "actionButton", "drillFilterOtherVisuals": True,
        "objects": {"icon": [{"properties": {"shapeType": lit("back"), "lineColor": solid(PRIMARY),
                                             "lineWeight": lit(2)}, "selector": {"id": "default"}}],
                    "fill": [{"properties": {"show": lit(True), "fillColor": solid(WHITE)}, "selector": {"id": "default"}}],
                    "outline": [{"properties": {"show": lit(False)}},
                                {"properties": {"show": lit(False)}, "selector": {"id": "default"}}]},
        "visualContainerObjects": {**plain_container(
            border=[{"properties": {"show": lit(True), "color": solid(LINE), "radius": lit(8)}}]),
            "visualLink": [{"properties": {"show": lit(True), "type": lit("Back")}}]}})
    tmp = Page("tmp", "")
    header(tmp, "")
    ctx = tmp.visuals[1]
    ctx["name"] = hid("zone", "hdr_context")
    p.visuals.append(ctx)
    kpi_tiles(p, [
        ("Trips", "Trips", "Growth Label", "Growth Color", "N"),
        ("Revenue", "Revenue", "Note Revenue", SLATE, "M"),
        ("Revenue per Trip", "Revenue per trip", "Note Per Trip", SLATE, ""),
        ("Avg Speed (mph)", "Average speed (mph)", "Note Speed", SLATE, ""),
        ("Card Tip %", "Card tip (% of fare)", "Note Tips", SLATE, "")])
    p.add("daily", X0, 192, CW, 250, visual(
        "clusteredColumnChart", {"Category": [DATE], "Y": [(mea("Trips"), "Trips")], "Tooltips": [(mea("Revenue"), "Revenue")]},
        title="Daily pick-ups in this zone", subtitle="Gold bars are rainy days",
        objects={"dataPoint": measure_fill("Day Color"), "categoryAxis": [{"properties": {"axisType": lit("Scalar")}}]},
        tooltip_page=PAGE_NAMES["tooltip"]))
    p.add("weekday", X0, 454, 520, 250, visual(
        "clusteredColumnChart", {"Category": [(col("dim_date", "weekday"), "Weekday")], "Y": [(mea("Trips per Day"), "Trips per day")]},
        title="Average pick-ups per day by weekday", subtitle="When this zone is busiest",
        sort=[(col("dim_date", "weekday"), "Ascending")],
        objects={"dataPoint": [{"properties": {"fill": solid(PRIMARY)}}], "labels": labels(True)}))
    p.add("profile", X0 + 520 + GAP, 454, CW - 520 - GAP, 250, visual(
        "tableEx", {"Values": [(col("dim_zone", "borough"), "Borough"), (col("dim_zone", "zone_type"), "Zone type"),
                               (col("dim_zone", "service_zone"), "Service zone"), (mea("Trips per Day"), "Trips/day"),
                               (mea("Avg Fare"), "Avg fare")]},
        title="Zone profile", subtitle="Type drives when trips start here",
        objects={"total": [{"properties": {"totals": lit(False)}}]}))
    return p


def page_tooltip():
    p = Page("tooltip", "Tooltip · Day", kind="Tooltip", width=320, height=220)
    p.add("bg", 0, 0, 320, 220, panel(WHITE))
    p.add("title", 14, 10, 292, 30, card("Title Tooltip", size=1, container=plain_container(
        title=[{"properties": {"show": lit(True), "text": {"expr": mea("Title Tooltip")}, "fontColor": solid(INK),
                               "fontSize": lit(11), "fontFamily": lit(FONT_SB), "alignment": lit("left")}}])))
    p.visuals[-1]["visual"]["objects"]["labels"][0]["properties"]["color"] = solid(WHITE)
    for i, (mname, label) in enumerate([("Trips", "TRIPS"), ("Revenue", "REVENUE"),
                                        ("Avg Speed (mph)", "SPEED (MPH)"), ("Precipitation (mm)", "RAIN (MM)")]):
        x, y = 14 + (i % 2) * 150, 46 + (i // 2) * 84
        p.add(f"t{i}", x, y, 142, 76, card(mname, size=15, title=label, exact=mname == "Trips"))
    return p


PAGES = [page_overview, page_demand, page_fee, page_payments, page_pipeline, page_zone, page_tooltip]
DRILL_FIELD = col("dim_zone", "zone")


def top_zone():
    """Zone with the most revenue: the default for the drill-through page when opened directly."""
    rev = {}
    with open(ROOT / "data_clean" / "gold" / "zone_daily.csv", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["borough"] != "Unknown":
                rev[r["zone"]] = rev.get(r["zone"], 0) + float(r["revenue"])
    return max(rev, key=rev.get)


DEFAULT_ZONE = top_zone()


def build_report():
    if RP.exists():
        shutil.rmtree(RP)
    write_json(RP / "definition.pbir", {
        "$schema": f"{SCHEMA}/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0", "datasetReference": {"byPath": {"path": f"../{NAME}.SemanticModel"}}})
    write_json(RP / ".platform", {
        "$schema": f"{SCHEMA}/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Report", "displayName": NAME},
        "config": {"version": "2.0", "logicalId": gid("report")}})
    base = RP / "StaticResources" / "SharedResources" / "BaseThemes" / f"{BASE_THEME}.json"
    base.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(THEME_SRC, base)
    write_json(RP / "StaticResources" / "RegisteredResources" / CUSTOM_THEME, THEME)
    write_json(PBI / "theme" / CUSTOM_THEME, THEME)        # standalone copy, importable in any report
    version = {"visual": V_VISUAL, "report": V_REPORT, "page": V_PAGE}
    d = RP / "definition"
    write_json(d / "version.json", {
        "$schema": f"{SCHEMA}/item/report/definition/versionMetadata/1.0.0/schema.json", "version": "2.0.0"})
    write_json(d / "report.json", {
        "$schema": f"{SCHEMA}/item/report/definition/report/{V_REPORT}/schema.json",
        "themeCollection": {
            "baseTheme": {"name": BASE_THEME, "reportVersionAtImport": version, "type": "SharedResources"},
            "customTheme": {"name": CUSTOM_THEME, "reportVersionAtImport": version, "type": "RegisteredResources"}},
        "resourcePackages": [
            {"name": "SharedResources", "type": "SharedResources",
             "items": [{"name": BASE_THEME, "path": f"BaseThemes/{BASE_THEME}.json", "type": "BaseTheme"}]},
            {"name": "RegisteredResources", "type": "RegisteredResources",
             "items": [{"name": CUSTOM_THEME, "path": CUSTOM_THEME, "type": "CustomTheme"}]}],
        "objects": {"outspacePane": [{"properties": {"expanded": lit(False)}}]},
        "settings": {"useStylableVisualContainerHeader": True, "defaultDrillFilterOtherVisuals": True,
                     "allowChangeFilterTypes": True, "useEnhancedTooltips": True,
                     "exportDataMode": "AllowSummarized"}})
    pages = [f() for f in PAGES]
    write_json(d / "pages" / "pages.json", {
        "$schema": f"{SCHEMA}/item/report/definition/pagesMetadata/1.0.0/schema.json",
        "pageOrder": [p.name for p in pages], "activePageName": pages[0].name})
    for p in pages:
        page = {"$schema": f"{SCHEMA}/item/report/definition/page/{V_PAGE}/schema.json",
                "name": p.name, "displayName": p.title, "displayOption": "FitToPage",
                "height": p.height, "width": p.width}
        if p.kind == "Drillthrough":
            fname, pname = hid("filter", "zone"), hid("param", "zone")
            page.update({
                "type": "Drillthrough", "visibility": "HiddenInViewMode",
                "pageBinding": {"name": hid("binding", "zone"), "type": "Drillthrough",
                                "parameters": [{"name": pname, "boundFilter": fname,
                                                "fieldExpr": DRILL_FIELD}]},
                "filterConfig": {"filters": [{"name": fname, "field": DRILL_FIELD, "type": "Categorical",
                                              # default store when the page is opened directly;
                                              # a drill-through replaces it
                                              "filter": {"Version": 2,
                                                         "From": [{"Name": "s", "Entity": "dim_zone", "Type": 0}],
                                                         "Where": [{"Condition": {"In": {
                                                             "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "s"}},
                                                                                         "Property": "zone"}}],
                                                             "Values": [[{"Literal": {"Value": "'" + DEFAULT_ZONE.replace("'", "''") + "'"}}]]}}}]}}]}})
        elif p.kind == "Tooltip":
            page.update({"type": "Tooltip", "visibility": "HiddenInViewMode",
                         "pageBinding": {"name": hid("binding", "tooltip"), "type": "Tooltip",
                                         "parameters": []}})
        write_json(d / "pages" / p.name / "page.json", page)
        for v in p.visuals:
            write_json(d / "pages" / p.name / "visuals" / v["name"] / "visual.json", v)
    return pages


if __name__ == "__main__":
    build_semantic_model()
    pages = build_report()
    write_json(PBI / f"{NAME}.pbip", {
        "$schema": f"{SCHEMA}/pbip/pbipProperties/1.0.0/schema.json", "version": "1.0",
        "artifacts": [{"report": {"path": f"{NAME}.Report"}}], "settings": {"enableAutoRecovery": True}})
    write(PBI / ".gitignore", "**/.pbi/localSettings.json\n**/.pbi/cache.abf\n")
    print(f"wrote {PBI / (NAME + '.pbip')}")
    print(f"  model : {len(TABLES) + 1} tables, {len(MEASURES)} measures "
          f"({sum(1 for x in MEASURES if not x[4])} visible), {len(RELATIONSHIPS)} relationships")
    print(f"  report: {len(pages)} pages, {sum(len(p.visuals) for p in pages)} visuals")
