from pyspark.sql.functions import col

# ==========================================================
# Fetch active RUNNING batch
# ==========================================================

batch_row = (
    spark.read.table("healthcare.control.pipeline_batches")
         .filter(col("status") == "RUNNING")
         .orderBy(col("start_time").desc())
         .first()
)

if batch_row:

    batch_id = batch_row["batch_id"]

    # ======================================================
    # Total processed records across all Bronze tables
    # ======================================================

    processed_records = (
        spark.sql(f"""
            SELECT
                COALESCE(SUM(record_count),0) AS total_records
            FROM
            (
                SELECT COUNT(*) AS record_count
                FROM healthcare.bronze.bronze_patients
                WHERE batch_id = '{batch_id}'

                UNION ALL

                SELECT COUNT(*)
                FROM healthcare.bronze.bronze_staff
                WHERE batch_id = '{batch_id}'

                UNION ALL

                SELECT COUNT(*)
                FROM healthcare.bronze.bronze_staff_schedule
                WHERE batch_id = '{batch_id}'

                UNION ALL

                SELECT COUNT(*)
                FROM healthcare.bronze.bronze_services_weekly
                WHERE batch_id = '{batch_id}'
            )
        """).first()["total_records"]
    )

    # ======================================================
    # Total distinct source files processed
    # ======================================================

    source_files = (
        spark.sql(f"""
            SELECT
                COUNT(DISTINCT file_path) AS total_files
            FROM
            (
                SELECT file_path
                FROM healthcare.bronze.bronze_patients
                WHERE batch_id = '{batch_id}'

                UNION

                SELECT file_path
                FROM healthcare.bronze.bronze_staff
                WHERE batch_id = '{batch_id}'

                UNION

                SELECT file_path
                FROM healthcare.bronze.bronze_staff_schedule
                WHERE batch_id = '{batch_id}'

                UNION

                SELECT file_path
                FROM healthcare.bronze.bronze_services_weekly
                WHERE batch_id = '{batch_id}'
            )
        """).first()["total_files"]
    )

    # ======================================================
    # Update Control Table
    # ======================================================

    spark.sql(f"""
        UPDATE healthcare.control.pipeline_batches
        SET
            status = 'COMPLETED',
            end_time = current_timestamp(),
            source_files = {source_files},
            processed_records = {processed_records}
        WHERE batch_id = '{batch_id}'
    """)

    print(f"""
Batch Updated Successfully

Batch ID           : {batch_id}
Status             : COMPLETED
Source Files       : {source_files}
Processed Records  : {processed_records}
""")

else:

    print("No RUNNING batch found.")