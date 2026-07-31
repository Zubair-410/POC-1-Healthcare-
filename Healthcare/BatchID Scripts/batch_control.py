from datetime import datetime

# ------------------------------------------------------------------
# Step 1: Mark any stale RUNNING batches as FAILED
# ------------------------------------------------------------------

spark.sql("""
UPDATE healthcare.control.pipeline_batches
SET
    status = 'FAILED',
    end_time = current_timestamp()
WHERE status = 'RUNNING'
""")

# ------------------------------------------------------------------
# Step 2: Generate Batch ID
# ------------------------------------------------------------------

batch_id = datetime.now().strftime("%Y%m%d%H%M%S")

# ------------------------------------------------------------------
# Step 3: Insert New RUNNING Batch
# ------------------------------------------------------------------

spark.sql(f"""
INSERT INTO healthcare.control.pipeline_batches
(
    batch_id,
    pipeline_name,
    start_time,
    end_time,
    status,
    source_files,
    processed_records
)
VALUES
(
    '{batch_id}',
    'Healthcare_ETL',
    current_timestamp(),
    NULL,
    'RUNNING',
    0,
    0
)
""")

print("=" * 60)
print("Healthcare ETL Batch Started")
print(f"Batch ID      : {batch_id}")
print("Status        : RUNNING")
print("=" * 60)