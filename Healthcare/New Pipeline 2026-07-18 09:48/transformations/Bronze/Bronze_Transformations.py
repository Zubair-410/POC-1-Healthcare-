from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    date_format,
    md5,
    concat_ws,
    count,
    max as max_,
    min as min_,
    expr,
    sha2,
    coalesce
)


# ==========================================================
# COMMON BATCH ID HELPER (CACHED SINGLETON)
# ==========================================================

_BATCH_ID = None

def get_current_batch_id():
    """
    Returns the current RUNNING batch ID from the control table.
    Caches the batch_id in _BATCH_ID to query the control table only once
    per pipeline execution run, preventing redundant query overhead across tables.
    Uses .first() to avoid driver list creation.
    """
    global _BATCH_ID

    if _BATCH_ID is None:
        batch = (
            spark.read.table("healthcare.control.pipeline_batches")
                .filter(col("status") == "RUNNING")
                .orderBy(col("start_time").desc())
                .first()
        )

        if batch is None:
            raise Exception("No RUNNING batch found in healthcare.control.pipeline_batches.")

        _BATCH_ID = batch["batch_id"]

    return _BATCH_ID


# ==========================================================
# FILE INVENTORY TABLE (AUDIT & MONITORING ONLY)
# ==========================================================

@dp.materialized_view(
    name="bronze.file_inventory",
    comment="Derived audit view aggregating file processing metrics from all Bronze tables."
)
def file_inventory():
    """
    Derived file inventory from Bronze table metadata.
    
    This view aggregates file-level metrics from all Bronze tables
    instead of manually maintaining a separate inventory table.
    """
    from pyspark.sql.functions import lit as lit_func
    
    patients = (
        dp.read("bronze.bronze_patients")
        .withColumn("bronze_table", lit_func("bronze_patients"))
        .withColumn("silver_table", lit_func("silver_patients"))
    )
    
    staff = (
        dp.read("bronze.bronze_staff")
        .withColumn("bronze_table", lit_func("bronze_staff"))
        .withColumn("silver_table", lit_func("silver_staff"))
    )
    
    schedule = (
        dp.read("bronze.bronze_staff_schedule")
        .withColumn("bronze_table", lit_func("bronze_staff_schedule"))
        .withColumn("silver_table", lit_func("silver_staff_schedule"))
    )
    
    services = (
        dp.read("bronze.bronze_services_weekly")
        .withColumn("bronze_table", lit_func("bronze_services_weekly"))
        .withColumn("silver_table", lit_func("silver_services_weekly"))
    )
    
    all_bronze = (
        patients
        .select("file_name", "file_path", "file_size", "file_modification_time", 
                "source_format", "ingestion_timestamp", "processed_timestamp", 
                "batch_id", "bronze_table", "silver_table")
        .unionByName(
            staff.select("file_name", "file_path", "file_size", "file_modification_time", 
                        "source_format", "ingestion_timestamp", "processed_timestamp", 
                        "batch_id", "bronze_table", "silver_table")
        )
        .unionByName(
            schedule.select("file_name", "file_path", "file_size", "file_modification_time", 
                           "source_format", "ingestion_timestamp", "processed_timestamp", 
                           "batch_id", "bronze_table", "silver_table")
        )
        .unionByName(
            services.select("file_name", "file_path", "file_size", "file_modification_time", 
                           "source_format", "ingestion_timestamp", "processed_timestamp", 
                           "batch_id", "bronze_table", "silver_table")
        )
    )
    
    return (
        all_bronze
        .groupBy(
            "file_name", 
            "file_path", 
            "file_size", 
            "file_modification_time",
            "source_format",
            "bronze_table",
            "silver_table"
        )
        .agg(
            md5(col("file_path")).alias("file_path_hash"),
            min_("ingestion_timestamp").alias("arrival_time"),
            min_("ingestion_timestamp").alias("processing_start_time"),
            max_("processed_timestamp").alias("processing_end_time"),
            count("*").alias("record_count"),
            max_("batch_id").alias("batch_id"),
            lit_func("COMPLETED").alias("process_status")
        )
        .withColumn("file_id", md5(concat_ws("|", col("file_path"), col("bronze_table"))))
        .select(
            "file_id",
            "file_name",
            "file_path",
            "file_path_hash",
            "source_format",
            "arrival_time",
            "processing_start_time",
            "processing_end_time",
            "process_status",
            "record_count",
            "bronze_table",
            "silver_table",
            "file_size",
            "file_modification_time",
            "batch_id"
        )
    )


# ==========================================================
# HELPER FUNCTIONS
# ==========================================================

def read_bronze_stream(path, format_type, schema_exprs):
    """
    Helper function to ingest raw streams using Auto Loader
    """
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", format_type)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
    )
    
    if format_type == "csv":
        reader = (
            reader
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
        )
    elif format_type == "json":
        reader = (
            reader
            .option("multiLine", "true")
            .option("cloudFiles.inferColumnTypes", "true")
        )
        
    df = reader.load(path)
    
    existing_cols = df.columns
    
    select_cols = []
    for col_name, col_type in schema_exprs:
        if col_name in existing_cols:
            select_cols.append(col(col_name).cast(col_type))
        else:
            select_cols.append(lit(None).cast(col_type).alias(col_name))
            
    select_cols.extend([
        col("_metadata.file_name").alias("file_name"),
        col("_metadata.file_path").alias("file_path"),
        col("_metadata.file_size").alias("file_size"),
        col("_metadata.file_modification_time").alias("file_modification_time")
    ])
    
    return (
        df.select(*select_cols)
        .withColumn("source_format", lit(format_type))
        .withColumn("ingestion_timestamp", current_timestamp())
    )


def add_batch_lineage(df):
    """
    Helper to append batch control columns and immutable record_hash at Bronze streaming ingestion time.
    Uses a temporary UUID column to generate record_hash, which is dropped immediately afterwards.
    Retrieves current cached RUNNING batch_id.
    """
    batch_id = get_current_batch_id()

    return (
        df
        .withColumn("batch_id", lit(batch_id))
        .withColumn("processed_timestamp", current_timestamp())
        .withColumn("pipeline_stage", lit("BRONZE"))
        .withColumn("_uuid", expr("uuid()"))
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "|",
                    lit(batch_id),
                    coalesce(col("file_name"), lit("NULL")),
                    coalesce(col("file_modification_time").cast("string"), lit("NULL")),
                    col("_uuid")
                ),
                256
            )
        )
        .drop("_uuid")
    )


# ==========================================================
# BRONZE PATIENTS
# ==========================================================

@dp.table(
    name="bronze.bronze_patients",
    comment="Raw patient healthcare data ingestion with batch lineage and permanent record_hash"
)
def bronze_patients():

    csv_df = read_bronze_stream(
        "s3://zubair-s3-demo/raw_dataset/hospital_dataset/patients/csv/",
        "csv",
        [
            ("patient_id", "string"),
            ("name", "string"),
            ("age", "int"),
            ("arrival_date", "date"),
            ("departure_date", "date"),
            ("service", "string"),
            ("satisfaction", "int")
        ]
    )

    json_df = read_bronze_stream(
        "s3://zubair-s3-demo/raw_dataset/hospital_dataset/patients/json/",
        "json",
        [
            ("patient_id", "string"),
            ("name", "string"),
            ("age", "int"),
            ("arrival_date", "date"),
            ("departure_date", "date"),
            ("service", "string"),
            ("satisfaction", "int")
        ]
    )

    return add_batch_lineage(
        csv_df.unionByName(json_df, allowMissingColumns=True)
    )


# ==========================================================
# BRONZE STAFF
# ==========================================================

@dp.table(
    name="bronze.bronze_staff",
    comment="Raw staff data ingestion with batch lineage and permanent record_hash"
)
def bronze_staff():

    csv_df = read_bronze_stream(
        "s3://zubair-s3-demo/raw_dataset/hospital_dataset/staff/csv/",
        "csv",
        [
            ("staff_id", "string"),
            ("staff_name", "string"),
            ("role", "string"),
            ("service", "string")
        ]
    )

    json_df = read_bronze_stream(
        "s3://zubair-s3-demo/raw_dataset/hospital_dataset/staff/json/",
        "json",
        [
            ("staff_id", "string"),
            ("staff_name", "string"),
            ("role", "string"),
            ("service", "string")
        ]
    )

    return add_batch_lineage(
        csv_df.unionByName(json_df, allowMissingColumns=True)
    )


# ==========================================================
# BRONZE STAFF SCHEDULE
# ==========================================================

@dp.table(
    name="bronze.bronze_staff_schedule",
    comment="Staff scheduling ingestion with batch lineage and permanent record_hash"
)
def bronze_staff_schedule():

    csv_df = read_bronze_stream(
        "s3://zubair-s3-demo/raw_dataset/hospital_dataset/staff_schedule/csv/",
        "csv",
        [
            ("week", "int"),
            ("staff_id", "string"),
            ("staff_name", "string"),
            ("role", "string"),
            ("service", "string"),
            ("present", "int")
        ]
    )

    json_df = read_bronze_stream(
        "s3://zubair-s3-demo/raw_dataset/hospital_dataset/staff_schedule/json/",
        "json",
        [
            ("week", "int"),
            ("staff_id", "string"),
            ("staff_name", "string"),
            ("role", "string"),
            ("service", "string"),
            ("present", "int")
        ]
    )

    return add_batch_lineage(
        csv_df.unionByName(json_df, allowMissingColumns=True)
    )


# ==========================================================
# BRONZE SERVICES WEEKLY
# ==========================================================

@dp.table(
    name="bronze.bronze_services_weekly",
    comment="Healthcare service metrics ingestion with batch lineage and permanent record_hash"
)
def bronze_services_weekly():

    csv_df = read_bronze_stream(
        "s3://zubair-s3-demo/raw_dataset/hospital_dataset/services_weekly/csv/",
        "csv",
        [
            ("week", "int"),
            ("month", "int"),
            ("service", "string"),
            ("available_beds", "int"),
            ("patients_request", "int"),
            ("patients_admitted", "int"),
            ("patients_refused", "int"),
            ("patient_satisfaction", "int"),
            ("staff_morale", "int"),
            ("event", "string")
        ]
    )

    json_df = read_bronze_stream(
        "s3://zubair-s3-demo/raw_dataset/hospital_dataset/services_weekly/json/",
        "json",
        [
            ("week", "int"),
            ("month", "int"),
            ("service", "string"),
            ("available_beds", "int"),
            ("patients_request", "int"),
            ("patients_admitted", "int"),
            ("patients_refused", "int"),
            ("patient_satisfaction", "int"),
            ("staff_morale", "int"),
            ("event", "string")
        ]
    )

    return add_batch_lineage(
        csv_df.unionByName(json_df, allowMissingColumns=True)
    )
