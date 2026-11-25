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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "a498d6f7-d248-49de-8c13-b8f6f1f6a608",
# META       "known_warehouses": [
# META         {
# META           "id": "a498d6f7-d248-49de-8c13-b8f6f1f6a608",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # NYC Taxi – raw ingestion to Files
# 
# Downloads the monthly TLC trip files for every active dataset in `metadata_dataset_config` and lands them as-is under `Files/raw/nyc_taxi/<dataset>/year=YYYY/month=MM/`.
# 
# Every run checks the current month plus the previous `rollback_months` months. Each file is tracked in `file_control` (one row per dataset / year / month):
# 
# | ingestion_status | meaning |
# |---|---|
# | PENDING | registered, not attempted yet |
# | SUCCEEDED | landed in Files; ETag, size and row count recorded |
# | NOT_AVAILABLE | TLC has not published the month yet – checked again next run |
# | FAILED | download or copy failed – retried until `max_retries` is reached |
# 
# A SUCCEEDED file is downloaded again only when its ETag or size on the TLC host has changed. The new copy then replaces the file in Files.
# 
# Re-running is safe: nothing is inserted or downloaded twice, and a run that stopped partway carries on from the file it stopped at.

# PARAMETERS CELL ********************

# Parameters - can be overridden from a pipeline or Airflow run
rollback_months = 10     # the current month plus this many previous months are checked on every run
dataset_ids     = ""     # comma separated, e.g. "DS001,DS002". Blank = all active datasets
max_retries     = 3      # FAILED files stop being picked up after this many attempts
force_reload    = False  # True = download again even if the file is unchanged
run_id          = ""     # blank = generated below

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Imports and constants
import os, json, uuid, time, shutil, tempfile
from datetime import datetime, timezone

import requests
import pyarrow.parquet as pq
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from delta.tables import DeltaTable
from pyspark.sql import functions as F

CONFIG_TABLE  = "metadata_and_confg_details.metadata_dataset_config"
CONTROL_TABLE = "metadata_and_confg_details.file_control"
DOWNLOAD_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"   # TLC public file host
RAW_ROOT      = "Files/raw/nyc_taxi"
LH_MOUNT      = "/lakehouse/default"                                # default lakehouse (lh_nyc_taxi)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run id, and the months to check: the current month plus the previous rollback_months months
run_id = run_id or f"ING_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:6]}"

today = datetime.now(timezone.utc)
periods = []
year, month = today.year, today.month
for _ in range(rollback_months + 1):
    periods.append((year, month))
    year, month = (year - 1, 12) if month == 1 else (year, month - 1)

print(f"run_id : {run_id}")
print(f"window : {periods[-1][0]}-{periods[-1][1]:02d} to {periods[0][0]}-{periods[0][1]:02d} ({len(periods)} months)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Active datasets from config
cfg_df = spark.table(CONFIG_TABLE).filter(F.col("is_active") == True)

display(cfg_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if dataset_ids.strip():
    ids = [x.strip() for x in dataset_ids.split(",") if x.strip()]
    cfg_df = cfg_df.filter(F.col("dataset_id").isin(ids))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cfg_df.select("dataset_id", "dataset_name", "file_prefix", "file_format").orderBy("dataset_id").collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

datasets = [r.asDict() for r in cfg_df.select("dataset_id", "dataset_name", "file_prefix", "file_format").orderBy("dataset_id").collect()]
datasets

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not datasets:
    raise ValueError("No active datasets found in config for the given filter")

display(cfg_df.select("dataset_id", "dataset_name", "file_prefix", "file_format"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

datasets

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Expected files for every month in the window
rows = []
for d in datasets:
    folder = d["dataset_name"].strip().lower()          # yellow, green, fhv, hvfhv
    for y, m in periods:
        file_name = f'{d["file_prefix"]}_{y}-{m:02d}.{d["file_format"]}'
        rows.append((
            f'{d["dataset_id"]}_{y}{m:02d}',
            d["dataset_id"], y, m, file_name,
            f"{DOWNLOAD_BASE}/{file_name}",
            f"{RAW_ROOT}/{folder}/year={y}/month={m:02d}/{file_name}",
        ))

print(f"{len(rows)} file(s) in the window")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Where the files come from
# 
# TLC keeps the monthly trip files in an S3 bucket and serves them through CloudFront (`DOWNLOAD_BASE`). It is a public file link, not an API:
# 
# - No login or key is needed.
# - There is no way to list the files, so each URL is built from the naming pattern, e.g. `yellow_tripdata_2026-01.parquet`.
# - A month that is not published yet returns 403 (sometimes 404). Both are treated as NOT_AVAILABLE.
# - S3 gives every file an ETag and a size. When TLC replaces a file the ETag changes, and that is how a corrected month is detected.
# - An ETag ending in `-13` means the file was uploaded in 13 parts. A re-upload of identical data can change the ETag, which only costs one extra download.

# CELL ********************

# just testing how to get etag and file size from api 
url = f"{DOWNLOAD_BASE}/yellow_tripdata_2026-01.parquet"
h = requests.head(url, timeout=30, allow_redirects=True)

print("HTTP status   :", h.status_code)
for k in ("ETag", "Last-Modified", "Content-Length"):
    print(f"{k:14}:", h.headers.get(k))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expected_df = spark.createDataFrame(
    rows,
    "file_id string, dataset_id string, data_year int, data_month int, "
    "file_name string, download_url string, landing_path string"
)
expected_df.createOrReplaceTempView("vw_expected_files")

display(spark.sql('SELECT * FROM vw_expected_files order by file_id'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Months not in the control table yet are inserted as PENDING.
# Rows that have not loaded yet get their file name, URL and path refreshed. SUCCEEDED rows are left alone.
# A re-run never creates a second row for the same file
spark.sql(f"""
    MERGE INTO {CONTROL_TABLE} t
    USING vw_expected_files s
       ON t.file_id = s.file_id
    WHEN MATCHED AND t.ingestion_status <> 'SUCCEEDED' THEN UPDATE SET
        file_name    = s.file_name,
        download_url = s.download_url,
        landing_path = s.landing_path,
        updated_at   = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (
        unique_run_id, file_id, dataset_id, file_name, download_url, landing_path,
        ingestion_status, transformation_status,
        current_etag, previous_etag, current_content_length, previous_content_length,
        data_year, data_month, record_count, total_retries, error_message,
        ingested_at, created_at, updated_at
    ) VALUES (
        '{run_id}', s.file_id, s.dataset_id, s.file_name, s.download_url, s.landing_path,
        'PENDING', 'PENDING',
        NULL, NULL, NULL, NULL,
        s.data_year, s.data_month, NULL, 0, NULL,
        NULL, current_timestamp(), current_timestamp()
    )
""")

print(f"{len(rows)} file(s) in the window registered")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Work list for this run:
#   1. every file in the window - SUCCEEDED ones are only checked for a changed ETag / size
#   2. any older file that has not SUCCEEDED yet
ctl_df = spark.table(CONTROL_TABLE).filter(F.col("dataset_id").isin([d["dataset_id"] for d in datasets]))

display(ctl_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(expected_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

in_window = ctl_df.join(expected_df.select("file_id"), "file_id", "inner")
not_done  = ctl_df.filter(F.col("ingestion_status") != "SUCCEEDED")
work_df   = in_window.unionByName(not_done).dropDuplicates(["file_id"])

if not force_reload:
    work_df = work_df.filter(~((F.col("ingestion_status") == "FAILED") & (F.col("total_retries") >= max_retries)))

work = [r.asDict() for r in work_df.orderBy("dataset_id", "data_year", "data_month").collect()]

print(f"{len(work)} file(s) to check")
display(work_df.select("file_id", "file_name", "ingestion_status", "total_retries", "current_etag").orderBy("file_id"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# HTTP session with retries, and a check of what TLC currently has for a file
http = requests.Session()
http.mount("https://", HTTPAdapter(max_retries=Retry(
    total=3, backoff_factor=5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET", "HEAD"]
)))


def source_info(url):
    """ETag and size of the file on TLC's host. None when the month isn't published yet."""
    r = http.head(url, timeout=30, allow_redirects=True)
    if r.status_code in (403, 404):
        return None
    r.raise_for_status()
    length = r.headers.get("Content-Length")
    return {"etag": r.headers.get("ETag"), "length": int(length) if length else None}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Download one file and put it in lakehouse Files
#
# HOW THIS WORKS
#   1. The file is downloaded into a staging folder inside the lakehouse:
#          Files/_staging/nyc_taxi/<run_id>/
#   2. It is checked there: not empty, fully downloaded, valid parquet.
#   3. Only a file that passes every check is moved to its real path
#      under Files/raw/nyc_taxi/...
#   4. A file that fails a check is deleted from staging and the error is raised,
#      so the loop marks it FAILED. The file already in the lakehouse is not touched.
#
# WHEN THE STAGING FOLDER IS CREATED AND DELETED
#   - prepare_staging() runs once before the loop (see the "Process every file" cell).
#   - remove_staging()  runs once after the loop, even if the loop stops with an error.
#   - Folders left behind by a crashed run are cleared by prepare_staging() on the next run.
#
# Spark ignores folder names that start with "_", so staging files are never read as data.

STAGING_ROOT = os.path.join(LH_MOUNT, "Files/_staging/nyc_taxi")   # one sub-folder per run
STAGING_DIR = os.path.join(STAGING_ROOT, run_id)                   # this run's folder
STAGING_MAX_AGE_HOURS = 6                                          # older folders are leftovers


def prepare_staging():
    """Run once before the loop: clear old leftovers, then create this run's staging folder."""
    if os.path.isdir(STAGING_ROOT):
        cutoff = time.time() - STAGING_MAX_AGE_HOURS * 3600
        for name in os.listdir(STAGING_ROOT):
            old_folder = os.path.join(STAGING_ROOT, name)
            # Only folders older than the cutoff are removed,
            # so another run that is still in progress keeps its files.
            if os.path.isdir(old_folder) and os.path.getmtime(old_folder) < cutoff:
                shutil.rmtree(old_folder, ignore_errors=True)
    os.makedirs(STAGING_DIR, exist_ok=True)


def remove_staging():
    """Run once after the loop: delete this run's staging folder and anything left in it."""
    shutil.rmtree(STAGING_DIR, ignore_errors=True)


def land_file(url, landing_path):
    """Download, check and land one file. Returns rows, size and ETag for file_control."""
    os.makedirs(STAGING_DIR, exist_ok=True)      # harmless if prepare_staging() already made it
    staged_file = os.path.join(STAGING_DIR, os.path.basename(landing_path))

    try:
        # ---- Step 1: download into the staging folder, 8 MB at a time ----
        with http.get(url, stream=True, timeout=(15, 600)) as r:
            r.raise_for_status()                              # stop on any HTTP error
            etag = r.headers.get("ETag")
            expected_size = r.headers.get("Content-Length")   # size the host says it is sending
            is_compressed = r.headers.get("Content-Encoding") # if set, sizes cannot be compared
            with open(staged_file, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)

        # ---- Step 2: check the staged file ----
        size = os.path.getsize(staged_file)

        # 2a. An empty file means the host answered but sent no content
        if size == 0:
            raise IOError(f"Empty download (0 bytes): {url}")

        # 2b. A size different from Content-Length means the download was cut short
        if expected_size and not is_compressed and int(expected_size) != size:
            raise IOError(f"Incomplete download: got {size} of {expected_size} bytes: {url}")

        # 2c. Reading the parquet footer proves the file is valid and gives the row count
        row_count = pq.ParquetFile(staged_file).metadata.num_rows

        # ---- Step 3: move the checked file to its real path ----
        target = os.path.join(LH_MOUNT, landing_path)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        try:
            os.replace(staged_file, target)      # one step: swaps out any older copy
        except OSError:
            # Fallback if the lakehouse mount refuses to overwrite in one step
            if os.path.exists(target):
                os.remove(target)
            os.rename(staged_file, target)

        return {"rows": row_count, "size": size, "etag": etag}

    finally:
        # ---- Step 4: a file still in staging here has failed, so remove it ----
        if os.path.exists(staged_file):
            os.remove(staged_file)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Control table updates - one small function per outcome
#
# All three functions run a SQL UPDATE on the file control table.
# Values are written into the SQL text through sql_value(), which turns a
# Python value into a safe SQL literal. (The ":name" parameter style is not
# used here because this runtime does not bind parameters for a Delta UPDATE.)
#
# transformation_status tells the silver notebook what to pick up:
#   PENDING             - row registered, file not landed yet (set when the row is created)
#   READY TO TRANSFORM  - file landed in bronze and waiting for silver (set by mark_succeeded)
# The silver notebook reads only rows with READY TO TRANSFORM and should set its
# own status on the row when it has finished with the file.


def sql_value(value):
    """Turn a Python value into a SQL literal that is safe to place in a statement."""
    if value is None:
        return "NULL"                                  # Python None -> SQL NULL
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)                              # numbers need no quotes
    text = str(value)
    text = text.replace("\\", "\\\\")                  # escape backslashes first
    text = text.replace("'", "\\'")                    # then escape single quotes
    return f"'{text}'"                                 # wrap the text in single quotes


def mark_not_available(file_id):
    """The month is not published on the source yet."""
    spark.sql(f"""
        UPDATE {CONTROL_TABLE}
        SET ingestion_status = 'NOT_AVAILABLE',
            error_message    = 'Not published yet',
            unique_run_id    = {sql_value(run_id)},
            updated_at       = current_timestamp()
        WHERE file_id = {sql_value(file_id)}
    """)


def mark_succeeded(file_id, res):
    """The file was downloaded, checked and landed. res holds rows, size and etag."""
    # The ETag and size held so far move to the previous_ columns.
    # SQL reads the old values first, so previous_etag gets the old current_etag.
    #
    # transformation_status is set to READY TO TRANSFORM on every successful load,
    # including a reload of a republished file, so silver processes the new copy.
    spark.sql(f"""
        UPDATE {CONTROL_TABLE}
        SET ingestion_status        = 'SUCCEEDED',
            transformation_status   = 'READY TO TRANSFORM',
            previous_etag           = current_etag,
            previous_content_length = current_content_length,
            current_etag            = {sql_value(res["etag"])},
            current_content_length  = CAST({sql_value(res["size"])} AS BIGINT),
            record_count            = CAST({sql_value(res["rows"])} AS BIGINT),
            total_retries           = 0,
            error_message           = NULL,
            ingested_at             = current_timestamp(),
            unique_run_id           = {sql_value(run_id)},
            updated_at              = current_timestamp()
        WHERE file_id = {sql_value(file_id)}
    """)


def mark_failed(file_id, error):
    """The file could not be loaded. Keeps the first 1000 characters of the error."""
    # transformation_status is left as it is: a failed download does not change
    # whether the copy already in bronze still needs transforming.
    spark.sql(f"""
        UPDATE {CONTROL_TABLE}
        SET ingestion_status = 'FAILED',
            total_retries    = coalesce(total_retries, 0) + 1,
            error_message    = {sql_value(str(error)[:1000])},
            unique_run_id    = {sql_value(run_id)},
            updated_at       = current_timestamp()
        WHERE file_id = {sql_value(file_id)}
    """)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Decide what to do with one file, and do it
def is_unchanged(w, src):
    """True when the file already landed has the same ETag and size as the one on TLC's host."""
    return (w["ingestion_status"] == "SUCCEEDED"
            and src["etag"] == w["current_etag"]
            and src["length"] == w["current_content_length"])


def process_file(w):
    """Returns (outcome, rows, size) for one control row."""
    src = source_info(w["download_url"])

    if src is None:
        if w["ingestion_status"] != "SUCCEEDED":
            mark_not_available(w["file_id"])
        return "NOT_AVAILABLE", None, None

    if not force_reload and is_unchanged(w, src):
        return "UNCHANGED", None, None

    # new file, earlier failure, or TLC has republished it - download and replace the file in Files
    res = land_file(w["download_url"], w["landing_path"])
    mark_succeeded(w["file_id"], res)
    outcome = "RELOADED" if w["ingestion_status"] == "SUCCEEDED" else "LOADED"
    return outcome, res["rows"], res["size"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Process every file. The file control row is saved after each one,
# so a re-run carries on from where the last one stopped
results = []

# Before the loop: clear staging leftovers from older runs and create this run's staging folder
prepare_staging()

try:
    for i, w in enumerate(work, 1):
        t0 = time.time()
        try:
            # HEAD check, then download + validate + move if the file is new or changed
            outcome, rows_loaded, size = process_file(w)
        except Exception as e:
            # Any error for this file: record it and carry on with the next file
            outcome, rows_loaded, size = "FAILED", None, None
            mark_failed(w["file_id"], e)

        results.append({"file_name": w["file_name"], "outcome": outcome})

        size_mb = f'{size / 1024 / 1024:,.0f} MB' if size else "-"
        rows_txt = f'{rows_loaded:,}' if rows_loaded is not None else "-"
        print(f'[{i:>2}/{len(work)}] {w["file_name"]:<34} {outcome:<14} rows={rows_txt:<12} {size_mb:<8} {time.time() - t0:,.0f}s')
finally:
    # After the loop: delete this run's staging folder.
    # "finally" makes this run even if the loop stops with an error.
    remove_staging()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run summary
display(
    spark.table(CONTROL_TABLE)
         .groupBy("dataset_id", "ingestion_status")
         .agg(F.count("*").alias("files"), F.sum("record_count").alias("records"))
         .orderBy("dataset_id", "ingestion_status")
)

outcomes = [r["outcome"] for r in results]
summary = {
    "run_id": run_id,
    "checked": len(results),
    "loaded": outcomes.count("LOADED"),
    "reloaded": outcomes.count("RELOADED"),
    "unchanged": outcomes.count("UNCHANGED"),
    "not_available": outcomes.count("NOT_AVAILABLE"),
    "failed": [r["file_name"] for r in results if r["outcome"] == "FAILED"],
}
print(json.dumps(summary, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Output returned to the pipeline / Airflow
ctl_now = spark.table(CONTROL_TABLE).filter(F.col("dataset_id").isin([d["dataset_id"] for d in datasets]))

failed_files = [r.file_name for r in ctl_now.filter(F.col("ingestion_status") == "FAILED")
                                             .orderBy("file_id").select("file_name").collect()]

output = {
    "run_id": run_id,
    "total_files_loaded": outcomes.count("LOADED") + outcomes.count("RELOADED"),                  # downloaded in this run
    "total_succeeded_files": ctl_now.filter(F.col("ingestion_status") == "SUCCEEDED").count(),   # in Files now, all runs
    "total_failed_files": len(failed_files),
    "failed_files": failed_files,                                                                 # includes files out of retries
}
print(json.dumps(output, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql('SELECT * FROM metadata_and_confg_details.file_control ORDER BY dataset_id, data_year, data_month'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Return the output to the pipeline / Airflow. Keep this as the last cell, nothing after it runs
notebookutils.notebook.exit(json.dumps(output))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
