from pyspark.sql.types import *
from pyspark.sql import SparkSession

from pathlib import Path
import os

fire_schema = StructType([
    StructField("CallNumber", IntegerType(), True),
    StructField("UnitID", StringType(), True),
    StructField("IncidentNumber", IntegerType(), True),
    StructField("CallType", StringType(), True),
    StructField("CallDate", StringType(), True),
    StructField("WatchDate", StringType(), True),
    StructField("CallFinalDisposition", StringType(), True),
    StructField("AvailableDtTm", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Zipcode", IntegerType(), True),
    StructField("Battalion", StringType(), True),
    StructField("StationArea", StringType(), True),
    StructField("OriginalPriority", StringType(), True),
    StructField("Priority", StringType(), True),
    StructField("FinalPriority", IntegerType(), True),
    StructField("ALSUnit", BooleanType(), True),
    StructField("CallTypeGroup", StringType(), True),
    StructField("NumAlarms", IntegerType(), True),
    StructField("UnitType", StringType(), True),
    StructField("UnitSequenceInCallDispatch", IntegerType(), True),
    StructField("FirePreventionDistrict", StringType(), True),
    StructField("SupervisorDistrict", StringType(), True),
    StructField("Neighborhood", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("RowID", StringType(), True),
    StructField("Delay", FloatType(), True),
])

sf_fire_file = os.path.join(
    Path(__file__).resolve().parents[0],
    "ch3",
    "data",
    "sf-fire-calls.csv"
)

spark = (SparkSession
        .builder
        .appName("Fire")
        .getOrCreate())

fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

parquet_dir = os.path.join(
    Path(__file__).resolve().parents[0],
    "ch3",
    "output"
)

os.makedirs(parquet_dir, exist_ok=True)
fire_df.write.format("parquet").save(os.path.join(parquet_dir, "output.parquet"))