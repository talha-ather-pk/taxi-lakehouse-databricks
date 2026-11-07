"""
Local Spark session with Delta Lake, for running the Databricks notebooks on a laptop.
Not needed on Databricks, where `spark` already exists.

Setup used here (Windows 11): Java 17 (Eclipse Temurin), `pip install pyspark delta-spark`, and the Delta jars
(delta-spark_4.2_2.13 and delta-storage, 4.4.1) in SPARK_JARS. On Windows, Hadoop also needs HADOOP_HOME with a
bin/winutils.exe, and its local file system calls a native library for directory listing; LOCAL_FS_IMPL points
to a small replacement class that lists directories in plain Java. On macOS/Linux leave both unset.
Environment variables override the defaults below.
"""
import glob
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAKE = ROOT / "lakehouse"
LANDING = ROOT / "landing"

JAVA_HOME = os.environ.get("JAVA_HOME") or next(iter(glob.glob(r"C:\Program Files\Eclipse Adoptium\jdk-17*")), None)
SPARK_JARS = os.environ.get("SPARK_JARS", str(Path.home() / "spark-jars"))
HADOOP_HOME = os.environ.get("HADOOP_HOME", str(Path.home() / "hadoop") if os.name == "nt" else "")
LOCAL_FS_IMPL = os.environ.get("LOCAL_FS_IMPL", "local.winfs.WinLocalFileSystem" if os.name == "nt" else "")


def get_spark(app="calder-taxi-lakehouse"):
    if JAVA_HOME:
        os.environ["JAVA_HOME"] = JAVA_HOME
    if HADOOP_HOME:
        os.environ["HADOOP_HOME"] = HADOOP_HOME
    os.environ.setdefault("PYSPARK_PYTHON", "python")
    os.environ["TAXI_LAKE"] = str(LAKE)
    os.environ["TAXI_LANDING"] = str(LANDING)
    from pyspark.sql import SparkSession

    jars = sorted(glob.glob(os.path.join(SPARK_JARS, "*.jar")))
    b = (SparkSession.builder.master("local[6]").appName(app)
         .config("spark.driver.memory", "6g")
         .config("spark.sql.shuffle.partitions", "24")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.showConsoleProgress", "false"))
    if jars:
        b = b.config("spark.driver.extraClassPath", os.pathsep.join(jars))
    else:  # no local jars: let Spark download Delta from Maven
        from delta import configure_spark_with_delta_pip
        b = configure_spark_with_delta_pip(b)
    if LOCAL_FS_IMPL:
        b = b.config("spark.hadoop.fs.file.impl", LOCAL_FS_IMPL)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
