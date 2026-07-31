from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    struct,
    concat_ws,
    sha2
)

# Import shared validation functions and normalization pipelines
from validation_helpers import (
    process_patients_bronze,
    build_patients_failed_rules,
    process_staff_bronze,
    build_staff_failed_rules,
    process_staff_schedule_bronze,
    build_staff_schedule_failed_rules,
    process_services_bronze,
    build_services_failed_rules,
    VALIDATION_VERSION
)


# ==========================================================
# QUARANTINE PATIENTS
# ==========================================================

@dp.table(
    name="silver.quarantine_patients",
    comment="Quarantined patient records that failed validation (no deduplication applied)",
    table_properties={
        "pipelines.ignoreDeletes": "true"
    }
)
def quarantine_patients():

    df = dp.read("bronze.bronze_patients")
    
    # Store original un-normalized record as record_payload for audit
    df_with_payload = df.withColumn("record_payload", struct("*"))

    # Step 1: Apply shared hashing, normalization, and validation (SAME single source of truth)
    df_validated = process_patients_bronze(df_with_payload)

    # Step 2: Build failed_rules and rejection_reasons using shared function
    df_with_rules = build_patients_failed_rules(df_validated)

    # Step 3: Filter strictly for INVALID records (NO deduplication to preserve 100% failure audit)
    return (
        df_with_rules
        .filter(col("validation_passed") == False)
        
        .withColumn(
            "quarantine_id",
            sha2(
                concat_ws(
                    "|",
                    col("batch_id"),
                    lit("bronze.bronze_patients"),
                    col("record_hash")
                ),
                256
            )
        )
        
        .withColumn("source_table", lit("bronze.bronze_patients"))
        .withColumn("rejection_timestamp", current_timestamp())
        .withColumn("bronze_processed_timestamp", col("processed_timestamp"))
        .withColumn("pipeline_stage", lit("QUARANTINE"))
        .withColumn("validation_version", lit(VALIDATION_VERSION))
        
        # Drop validation flag columns
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
            "validation_passed",
            "processed_timestamp"
        )
    )


# ==========================================================
# QUARANTINE STAFF
# ==========================================================

@dp.table(
    name="silver.quarantine_staff",
    comment="Quarantined staff records that failed validation (no deduplication applied)",
    table_properties={
        "pipelines.ignoreDeletes": "true"
    }
)
def quarantine_staff():

    df = dp.read("bronze.bronze_staff")
    
    # Store original un-normalized record as record_payload for audit
    df_with_payload = df.withColumn("record_payload", struct("*"))

    # Step 1: Apply shared hashing, normalization, and validation (SAME single source of truth)
    df_validated = process_staff_bronze(df_with_payload)

    # Step 2: Build failed_rules and rejection_reasons using shared function
    df_with_rules = build_staff_failed_rules(df_validated)

    # Step 3: Filter strictly for INVALID records (NO deduplication to preserve 100% failure audit)
    return (
        df_with_rules
        .filter(col("validation_passed") == False)
        
        .withColumn(
            "quarantine_id",
            sha2(
                concat_ws(
                    "|",
                    col("batch_id"),
                    lit("bronze.bronze_staff"),
                    col("record_hash")
                ),
                256
            )
        )
        
        .withColumn("source_table", lit("bronze.bronze_staff"))
        .withColumn("rejection_timestamp", current_timestamp())
        .withColumn("bronze_processed_timestamp", col("processed_timestamp"))
        .withColumn("pipeline_stage", lit("QUARANTINE"))
        .withColumn("validation_version", lit(VALIDATION_VERSION))
        
        # Drop validation flag columns
        .drop(
            "valid_staff_id",
            "valid_staff_name",
            "valid_role",
            "valid_service",
            "valid_role_value",
            "validation_passed",
            "processed_timestamp"
        )
    )


# ==========================================================
# QUARANTINE STAFF SCHEDULE
# ==========================================================

@dp.table(
    name="silver.quarantine_staff_schedule",
    comment="Quarantined staff schedule records that failed validation (no deduplication applied)",
    table_properties={
        "pipelines.ignoreDeletes": "true"
    }
)
def quarantine_staff_schedule():

    df = dp.read("bronze.bronze_staff_schedule")
    
    # Store original un-normalized record as record_payload for audit
    df_with_payload = df.withColumn("record_payload", struct("*"))

    # Step 1: Apply shared hashing, normalization, and validation (SAME single source of truth)
    df_validated = process_staff_schedule_bronze(df_with_payload)

    # Step 2: Build failed_rules and rejection_reasons using shared function
    df_with_rules = build_staff_schedule_failed_rules(df_validated)

    # Step 3: Filter strictly for INVALID records (NO deduplication to preserve 100% failure audit)
    return (
        df_with_rules
        .filter(col("validation_passed") == False)
        
        .withColumn(
            "quarantine_id",
            sha2(
                concat_ws(
                    "|",
                    col("batch_id"),
                    lit("bronze.bronze_staff_schedule"),
                    col("record_hash")
                ),
                256
            )
        )
        
        .withColumn("source_table", lit("bronze.bronze_staff_schedule"))
        .withColumn("rejection_timestamp", current_timestamp())
        .withColumn("bronze_processed_timestamp", col("processed_timestamp"))
        .withColumn("pipeline_stage", lit("QUARANTINE"))
        .withColumn("validation_version", lit(VALIDATION_VERSION))
        
        # Drop validation flag columns
        .drop(
            "valid_staff_id",
            "valid_presence",
            "valid_week",
            "valid_role",
            "valid_service",
            "valid_role_value",
            "validation_passed",
            "processed_timestamp"
        )
    )


# ==========================================================
# QUARANTINE SERVICES
# ==========================================================

@dp.table(
    name="silver.quarantine_services",
    comment="Quarantined services records that failed validation (no deduplication applied)",
    table_properties={
        "pipelines.ignoreDeletes": "true"
    }
)
def quarantine_services():

    df = dp.read("bronze.bronze_services_weekly")
    
    # Store original un-normalized record as record_payload for audit
    df_with_payload = df.withColumn("record_payload", struct("*"))

    # Step 1: Apply shared hashing, normalization, KPI calculation, and validation (SAME single source of truth)
    df_validated = process_services_bronze(df_with_payload)

    # Step 2: Build failed_rules and rejection_reasons using shared function (with KPI validation)
    df_with_rules = build_services_failed_rules(df_validated, include_kpi_validation=True)

    # Step 3: Filter strictly for INVALID records (NO deduplication to preserve 100% failure audit)
    return (
        df_with_rules
        .filter(col("validation_passed") == False)
        
        .withColumn(
            "quarantine_id",
            sha2(
                concat_ws(
                    "|",
                    col("batch_id"),
                    lit("bronze.bronze_services_weekly"),
                    col("record_hash")
                ),
                256
            )
        )
        
        .withColumn("source_table", lit("bronze.bronze_services_weekly"))
        .withColumn("rejection_timestamp", current_timestamp())
        .withColumn("bronze_processed_timestamp", col("processed_timestamp"))
        .withColumn("pipeline_stage", lit("QUARANTINE"))
        .withColumn("validation_version", lit(VALIDATION_VERSION))
        
        # Drop validation flag columns (including KPI validation flags)
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
            "validation_passed",
            "processed_timestamp"
        )
    )
