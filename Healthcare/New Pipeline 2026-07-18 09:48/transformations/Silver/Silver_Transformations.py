from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    row_number,
    count,
    min as min_,
    max as max_,
    coalesce,
    round,
    greatest,
    count_distinct,
    when
)

from pyspark.sql.window import Window

# Import shared validation functions and normalization pipelines
from validation_helpers import (
    process_patients_bronze,
    process_staff_bronze,
    process_staff_schedule_bronze,
    process_services_bronze,
    VALIDATION_VERSION
)


# ==========================================================
# SILVER PATIENTS
# ==========================================================

@dp.table(
    name="silver.silver_patients",
    comment="Cleaned patient data with batch lineage and post-validation deduplication",
    table_properties={
        "pipelines.ignoreDeletes": "true"
    }
)
def silver_patients():

    df = dp.read("bronze.bronze_patients")
    
    # Step 1: Apply shared normalization and validation BEFORE deduplication (uses pre-existing record_hash from Bronze)
    df_validated = process_patients_bronze(df)
    
    # Step 2: Filter strictly for VALID records first
    df_valid_only = df_validated.filter(col("validation_passed") == True)
    
    # Step 3: Window-based deduplication ONLY on valid records (valid patient_id + arrival_date)
    window_spec = Window.partitionBy("patient_id", "arrival_date").orderBy(
        col("file_modification_time").desc(),
        col("processed_timestamp").desc(),
        col("file_name").desc()
    )
    
    return (
        df_valid_only
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")

        .withColumn("bronze_processed_timestamp", col("processed_timestamp"))
        .withColumn("silver_processed_timestamp", current_timestamp())
        .drop("processed_timestamp")

        .withColumn("pipeline_stage", lit("SILVER"))
        .withColumn("validation_version", lit(VALIDATION_VERSION))
        
        # Drop validation flag columns (preserve record_hash and bronze_record_id for lineage)
        .drop(
            "valid_patient_id",
            "valid_arrival_date",
            "valid_departure_date",
            "valid_patient_name",
            "valid_service",
            "valid_age",
            "valid_satisfaction",
            "valid_dates",
            "valid_length_of_stay",
            "validation_passed"
        )
    )


# ==========================================================
# SILVER STAFF
# ==========================================================

@dp.table(
    name="silver.silver_staff",
    comment="Cleaned staff data with batch lineage and post-validation deduplication",
    table_properties={
        "pipelines.ignoreDeletes": "true"
    }
)
def silver_staff():

    df = dp.read("bronze.bronze_staff")
    
    # Step 1: Apply shared normalization and validation BEFORE deduplication (uses pre-existing record_hash from Bronze)
    df_validated = process_staff_bronze(df)
    
    # Step 2: Filter strictly for VALID records first
    df_valid_only = df_validated.filter(col("validation_passed") == True)
    
    # Step 3: Window-based deduplication ONLY on valid records (valid staff_id)
    window_spec = Window.partitionBy("staff_id").orderBy(
        col("file_modification_time").desc(),
        col("processed_timestamp").desc(),
        col("file_name").desc()
    )
    
    return (
        df_valid_only
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")

        .withColumn("bronze_processed_timestamp", col("processed_timestamp"))
        .withColumn("silver_processed_timestamp", current_timestamp())
        .drop("processed_timestamp")

        .withColumn("pipeline_stage", lit("SILVER"))
        .withColumn("validation_version", lit(VALIDATION_VERSION))
        
        # Drop validation flag columns
        .drop(
            "valid_staff_id",
            "valid_staff_name",
            "valid_role",
            "valid_service",
            "valid_role_value",
            "validation_passed"
        )
    )


# ==========================================================
# SILVER STAFF SCHEDULE
# ==========================================================

@dp.table(
    name="silver.silver_staff_schedule",
    comment="Validated staff schedule with batch lineage and post-validation deduplication",
    table_properties={
        "pipelines.ignoreDeletes": "true"
    }
)
def silver_staff_schedule():

    df = dp.read("bronze.bronze_staff_schedule")
    
    # Step 1: Apply shared normalization and validation BEFORE deduplication (uses pre-existing record_hash from Bronze)
    df_validated = process_staff_schedule_bronze(df)
    
    # Step 2: Filter strictly for VALID records first
    df_valid_only = df_validated.filter(col("validation_passed") == True)
    
    # Step 3: Window-based deduplication ONLY on valid records (valid week + staff_id + service)
    window_spec = Window.partitionBy("week", "staff_id", "service").orderBy(
        col("file_modification_time").desc(),
        col("processed_timestamp").desc(),
        col("file_name").desc()
    )
    
    return (
        df_valid_only
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")

        .withColumn("bronze_processed_timestamp", col("processed_timestamp"))
        .withColumn("silver_processed_timestamp", current_timestamp())
        .drop("processed_timestamp")

        .withColumn("pipeline_stage", lit("SILVER"))
        .withColumn("validation_version", lit(VALIDATION_VERSION))
        
        # Drop validation flag columns
        .drop(
            "valid_staff_id",
            "valid_presence",
            "valid_week",
            "valid_role",
            "valid_service",
            "valid_role_value",
            "validation_passed"
        )
    )


# ==========================================================
# SILVER SERVICES
# ==========================================================

@dp.table(
    name="silver.silver_services",
    comment="Cleaned healthcare operational metrics with batch lineage and post-validation deduplication",
    table_properties={
        "pipelines.ignoreDeletes": "true"
    }
)
def silver_services():

    df = dp.read("bronze.bronze_services_weekly")
    
    # Step 1: Apply shared normalization, KPI calculation, and validation BEFORE deduplication (uses pre-existing record_hash from Bronze)
    df_validated = process_services_bronze(df)
    
    # Step 2: Filter strictly for VALID records first
    df_valid_only = df_validated.filter(col("validation_passed") == True)
    
    # Step 3: Window-based deduplication ONLY on valid records (valid week + service)
    window_spec = Window.partitionBy("week", "service").orderBy(
        col("file_modification_time").desc(),
        col("processed_timestamp").desc(),
        col("file_name").desc()
    )
    
    return (
        df_valid_only
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
        
        .withColumn("bronze_processed_timestamp", col("processed_timestamp"))
        .withColumn("silver_processed_timestamp", current_timestamp())
        .drop("processed_timestamp")

        .withColumn("pipeline_stage", lit("SILVER"))
        .withColumn("validation_version", lit(VALIDATION_VERSION))

        # Drop validation flag columns
        .drop(
            "valid_service",
            "valid_week",
            "valid_month",
            "valid_available_beds",
            "valid_patients_request",
            "valid_patients_admitted",
            "valid_patients_refused",
            "valid_patient_satisfaction",
            "valid_staff_morale",
            "valid_admitted_vs_beds",
            "valid_requests_vs_admitted",
            "valid_requests_vs_refused",
            "valid_request_balance",
            "valid_event",
            "valid_occupancy_rate",
            "valid_admission_rate",
            "valid_refusal_rate",
            "validation_passed"
        )
    )


# ==========================================================
# OBSERVABILITY & MONITORING
# ==========================================================

def get_dataset_metrics(dataset_name, bronze_table, silver_table, quarantine_table, process_fn):
    """
    Computes data quality and reconciliation metrics per batch.
    Reads pre-existing record_hash directly from Bronze Delta tables.
    
    Reconciliation Equation:
    records_received (Bronze) = records_valid (Silver) + records_quarantined (Quarantine) + duplicates_removed
    
    where duplicates_removed = records_valid_raw - records_valid (explicitly measured from deduplication stage).
    """
    bronze_df = dp.read(bronze_table)
    silver_df = dp.read(silver_table)
    quar_df = dp.read(quarantine_table)
    
    # Evaluate validation pipeline on bronze data to capture pre-dedup valid count
    val_df = process_fn(bronze_df)
    
    b_metrics = (
        bronze_df
        .groupBy("batch_id")
        .agg(
            count("*").alias("records_received"),
            count_distinct("record_hash").alias("records_distinct"),
            min_("processed_timestamp").alias("processing_start")
        )
    )
    
    v_raw_metrics = (
        val_df
        .filter(col("validation_passed") == True)
        .groupBy("batch_id")
        .agg(
            count("*").alias("records_valid_raw")
        )
    )
    
    s_metrics = (
        silver_df
        .groupBy("batch_id")
        .agg(
            count("*").alias("records_valid"),
            max_("silver_processed_timestamp").alias("silver_end")
        )
    )
    
    q_metrics = (
        quar_df
        .groupBy("batch_id")
        .agg(
            count("*").alias("records_quarantined"),
            max_("rejection_timestamp").alias("quar_end")
        )
    )
    
    return (
        b_metrics
        .join(v_raw_metrics, "batch_id", "left")
        .join(s_metrics, "batch_id", "left")
        .join(q_metrics, "batch_id", "left")
        .select(
            col("batch_id"),
            lit(dataset_name).alias("dataset_name"),
            coalesce(col("records_received"), lit(0)).alias("records_received"),
            coalesce(col("records_valid"), lit(0)).alias("records_valid"),
            coalesce(col("records_quarantined"), lit(0)).alias("records_quarantined"),
            coalesce(col("records_distinct"), lit(0)).alias("records_distinct"),
            # Duplicates removed = records_valid_raw - records_valid (explicit deduplication metric)
            greatest(
                lit(0),
                coalesce(col("records_valid_raw"), lit(0)) - coalesce(col("records_valid"), lit(0))
            ).alias("duplicates_removed"),
            col("processing_start"),
            coalesce(
                when(col("silver_end").isNotNull() & col("quar_end").isNotNull(), greatest(col("silver_end"), col("quar_end")))
                .when(col("silver_end").isNotNull(), col("silver_end"))
                .otherwise(col("quar_end")),
                col("processing_start")
            ).alias("processing_end")
        )
        .withColumn("duration_seconds", col("processing_end").cast("long") - col("processing_start").cast("long"))
        .withColumn(
            "success_rate", 
            round(
                when(col("records_received") == 0, 0.0)
                .otherwise(col("records_valid") / col("records_received")),
                4
            )
        )
        .withColumn(
            "failure_rate", 
            round(
                when(col("records_received") == 0, 0.0)
                .otherwise(col("records_quarantined") / col("records_received")),
                4
            )
        )
        .withColumn(
            "reconciliation_passed",
            (col("records_received") == (col("records_valid") + col("records_quarantined") + col("duplicates_removed")))
        )
    )


@dp.table(
    name="silver.data_quality_metrics",
    comment="Data quality metrics aggregated per batch and dataset with exact reconciliation"
)
def data_quality_metrics():
    patients = get_dataset_metrics("patients", "bronze.bronze_patients", "silver.silver_patients", "silver.quarantine_patients", process_patients_bronze)
    staff = get_dataset_metrics("staff", "bronze.bronze_staff", "silver.silver_staff", "silver.quarantine_staff", process_staff_bronze)
    schedule = get_dataset_metrics("staff_schedule", "bronze.bronze_staff_schedule", "silver.silver_staff_schedule", "silver.quarantine_staff_schedule", process_staff_schedule_bronze)
    services = get_dataset_metrics("services", "bronze.bronze_services_weekly", "silver.silver_services", "silver.quarantine_services", process_services_bronze)
    
    return (
        patients
        .unionByName(staff)
        .unionByName(schedule)
        .unionByName(services)
    )


@dp.table(
    name="silver.serialization_errors",
    comment="Runtime processing and serialization errors captured during pipeline execution"
)
def serialization_errors():
    from pyspark.sql.functions import lit as lit_func, col
    
    datasets = [
        ("patients", "bronze.bronze_patients"),
        ("staff", "bronze.bronze_staff"),
        ("staff_schedule", "bronze.bronze_staff_schedule"),
        ("services", "bronze.bronze_services_weekly")
    ]
    
    error_dfs = []
    for dataset_name, table_name in datasets:
        df = dp.read(table_name)
        if "_rescued_data" in df.columns:
            err_df = (
                df
                .filter(col("_rescued_data").isNotNull())
                .select(
                    col("batch_id"),
                    lit_func(dataset_name).alias("dataset"),
                    lit_func("SERIALIZATION_ERROR").alias("error_type"),
                    col("_rescued_data").alias("error_message"),
                    lit_func("Malformed JSON/CSV record rescued by Auto Loader").alias("stacktrace"),
                    col("processed_timestamp").alias("timestamp"),
                    lit_func("BRONZE").alias("pipeline_stage")
                )
            )
            error_dfs.append(err_df)
            
    if error_dfs:
        result_df = error_dfs[0]
        for next_df in error_dfs[1:]:
            result_df = result_df.unionByName(next_df)
        return result_df
    else:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        schema = StructType([
            StructField("batch_id", StringType(), True),
            StructField("dataset", StringType(), True),
            StructField("error_type", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("stacktrace", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("pipeline_stage", StringType(), True)
        ])
        return spark.createDataFrame([], schema)