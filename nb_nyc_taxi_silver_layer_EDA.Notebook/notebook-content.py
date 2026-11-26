# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4e2ecbec-4240-484a-8911-17a9f79e6e04",
# META       "default_lakehouse_name": "lh_nyc_taxi",
# META       "default_lakehouse_workspace_id": "9d45f741-cd26-4f6e-98e7-bc144549bf28",
# META       "known_lakehouses": [
# META         {
# META           "id": "4e2ecbec-4240-484a-8911-17a9f79e6e04"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Imports for the silver layer notebook

# ---- Standard Python ----
import json                                   # build the output returned to the pipeline
import uuid                                   # generate a unique run id
from datetime import datetime, timezone       # timestamps in UTC

# ---- Spark ----
from pyspark.sql import DataFrame             # type name used in function signatures
from pyspark.sql import functions as F        # column functions: F.col, F.lit, F.when, F.to_timestamp ...
from pyspark.sql import types as T            # data types for casting: T.IntegerType, T.DoubleType ...
from pyspark.sql.window import Window         # window functions, used for de-duplicating rows
from pyspark.sql.functions import col

# ---- Delta Lake ----
from delta.tables import DeltaTable           # MERGE into silver Delta tables


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# lets understand yellow taxi dataset

# CELL ********************

yellow_df = spark.read.parquet('Files/raw/nyc_taxi/yellow/year=2026/month=01/yellow_tripdata_2026-01.parquet')
display(yellow_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(yellow_df.select(col('DOLocationID')).distinct().orderBy('DOLocationID'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(yellow_df.select(col('payment_type')).distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(yellow_df.select(col('payment_type')).distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mapping_df = spark.read.csv(
    'Files/Mapping/taxi_zone_lookup.csv',
    header = True,
    inferSchema=True)
    
display(mapping_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_df = (
    yellow_df.alias('y')
    .join(
        mapping_df.alias('m'),
        col('y.DOLocationID') == col('m.LocationID'),
        'right'
    )
)

display(
    joined_df.select(col('DOLocationID'), col('Borough'), col('Zone'), col('service_zone'))\
    .filter(col('DOLocationID').isin(yellow_df.select(col('DOLocationID')).distinct()))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

green_df = spark.read.parquet('Files/raw/nyc_taxi/green/year=2026/month=05/green_tripdata_2026-05.parquet')
display(green_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EDA step 1: set up and look at the schema
# yellow_df is the January 2026 yellow taxi file loaded above. Nothing here changes the data.

df = yellow_df
total = df.count()                              # total rows, reused by the cells below
pu = F.col("tpep_pickup_datetime")              # short names for the two time columns
do = F.col("tpep_dropoff_datetime")

print(f"ROWS: {total:,}   COLUMNS: {len(df.columns)}")

# Column names and data types
for name, dtype in df.dtypes:
    print(f"   {name:<24} {dtype}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EDA step 2: nulls in each column
nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).first().asDict()

for c, n in nulls.items():
    print(f"   {c:<24} {n:>10,}  {n / total * 100:6.2f}%")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EDA step 3: duplicates

# 3a. Rows that are identical in every column
exact_dupes = total - df.distinct().count()

# 3b. Rows that share the same trip key.
#     TLC gives no trip id, so the key is built from columns.
trip_key = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"]
key_dupes = total - df.dropDuplicates(trip_key).count()

print(f"   exact duplicate rows (all columns)   {exact_dupes:>10,}  {exact_dupes / total * 100:6.2f}%")
print(f"   extra rows sharing the trip key      {key_dupes:>10,}  {key_dupes / total * 100:6.2f}%")
print(f"   trip key = {trip_key}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EDA step 4: min, max and mean of every numeric column
num_cols = [c for c, t in df.dtypes if t in ("int", "bigint", "double", "float")]

stats = df.select(
    [F.min(c).alias(c + "__min") for c in num_cols]
    + [F.max(c).alias(c + "__max") for c in num_cols]
    + [F.avg(c).alias(c + "__avg") for c in num_cols]
).first().asDict()

for c in num_cols:
    print(f"   {c:<24} min={stats[c + '__min']!s:<12} max={stats[c + '__max']!s:<14} mean={stats[c + '__avg']:.2f}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EDA step 5: percentiles, to see how extreme the top values are
# Each list is [50%, 99%, 99.9%]
for c in ["trip_distance", "fare_amount", "total_amount"]:
    print(f"   {c:<24} {df.approxQuantile(c, [0.5, 0.99, 0.999], 0.001)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EDA step 6: pickup date range (the file should only hold January 2026)
dates = df.select(F.min(pu).alias("min_pu"), F.max(pu).alias("max_pu")).first()

print(f"   earliest pickup  {dates['min_pu']}")
print(f"   latest pickup    {dates['max_pu']}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EDA step 7: rule checks - how many rows break each rule
minutes = (do.cast("timestamp").cast("long") - pu.cast("timestamp").cast("long")) / 60   # trip length in minutes

checks = {
    "pickup outside January 2026":    (pu < F.lit("2026-01-01").cast("timestamp")) | (pu >= F.lit("2026-02-01").cast("timestamp")),
    "dropoff before pickup":          do < pu,
    "zero duration (same second)":    do == pu,
    "duration over 24 hours":         minutes > 24 * 60,
    "trip_distance = 0":              F.col("trip_distance") == 0,
    "trip_distance < 0":              F.col("trip_distance") < 0,
    "trip_distance > 100 miles":      F.col("trip_distance") > 100,
    "fare_amount < 0":                F.col("fare_amount") < 0,
    "fare_amount = 0":                F.col("fare_amount") == 0,
    "total_amount < 0":               F.col("total_amount") < 0,
    "total_amount = 0":               F.col("total_amount") == 0,
    "passenger_count = 0":            F.col("passenger_count") == 0,
    "passenger_count > 6":            F.col("passenger_count") > 6,
    "pickup zone 264/265 (unknown)":  F.col("PULocationID").isin(264, 265),
    "dropoff zone 264/265 (unknown)": F.col("DOLocationID").isin(264, 265),
    "pickup zone = dropoff zone":     F.col("PULocationID") == F.col("DOLocationID"),
}

# One pass over the data counts every rule at once
names = list(checks)
result = df.select([F.sum(checks[n].cast("int")).alias(f"c{i}") for i, n in enumerate(names)]).first()

for i, n in enumerate(names):
    v = result[f"c{i}"] or 0
    print(f"   {n:<34} {v:>10,}  {v / total * 100:6.2f}%")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EDA step 8: values present in each code column, with counts
for c in ["VendorID", "RatecodeID", "payment_type", "store_and_fwd_flag", "passenger_count"]:
    print(f"   {c}")
    for r in df.groupBy(c).count().orderBy(c).collect():
        print(f"      {r[c]!s:<8} {r['count']:>10,}  {r['count'] / total * 100:6.2f}%")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
