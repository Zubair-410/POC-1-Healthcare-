# Databricks notebook source
# DBTITLE 1,Introduction
# MAGIC %md
# MAGIC # Healthcare ETL Pipeline Architecture
# MAGIC
# MAGIC ```
# MAGIC                      SOURCE FILES
# MAGIC                            │
# MAGIC                            ▼
# MAGIC                   BRONZE INGESTION
# MAGIC                            │
# MAGIC                            ▼
# MAGIC              Read Files into Bronze Tables
# MAGIC                            │
# MAGIC                            ▼
# MAGIC              Add Batch & File Metadata
# MAGIC                            │
# MAGIC                            ▼
# MAGIC                 Store Raw Bronze Tables
# MAGIC                            │
# MAGIC                            ▼
# MAGIC ────────────────────────────────────────────────────
# MAGIC                    SILVER PIPELINE
# MAGIC ────────────────────────────────────────────────────
# MAGIC                            │
# MAGIC                            ▼
# MAGIC                Read Bronze Tables
# MAGIC                            │
# MAGIC                            ▼
# MAGIC              Normalize Raw Data
# MAGIC                            │
# MAGIC                            ▼
# MAGIC       Calculate Business KPIs (Services)
# MAGIC                            │
# MAGIC                            ▼
# MAGIC         Apply Validation Rules
# MAGIC                            │
# MAGIC              ┌─────────────┴─────────────┐
# MAGIC              │                           │
# MAGIC              ▼                           ▼
# MAGIC      Valid Records               Invalid Records
# MAGIC              │                           │
# MAGIC              ▼                           ▼
# MAGIC  Window-based Deduplication     Failed Rule Detection
# MAGIC              │                           │
# MAGIC              ▼                           ▼
# MAGIC  Add Metadata & Lineage        Quarantine Processing
# MAGIC              │                           │
# MAGIC              ▼                           ▼
# MAGIC      Final Silver Tables      Final Quarantine Tables
# MAGIC              │
# MAGIC              ▼
# MAGIC ────────────────────────────────────────────────────
# MAGIC                     GOLD PIPELINE
# MAGIC ────────────────────────────────────────────────────
# MAGIC              │
# MAGIC              ▼
# MAGIC       Read Silver Tables
# MAGIC              │
# MAGIC              ▼
# MAGIC     Aggregate Business Metrics
# MAGIC              │
# MAGIC              ▼
# MAGIC  Generate Analytics Tables
# MAGIC              │
# MAGIC              ├──────────────► Patient Flow
# MAGIC              ├──────────────► Staff Efficiency
# MAGIC              ├──────────────► Bed Utilization
# MAGIC              ├──────────────► Operational Efficiency
# MAGIC              └──────────────► Bottleneck Analysis
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Task 1 — Generate Batch ID

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare.control.pipeline_batches
# MAGIC ORDER BY start_time DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC **Task 3 — Update Status**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC batch_id,
# MAGIC status,
# MAGIC start_time,
# MAGIC end_time
# MAGIC FROM healthcare.control.pipeline_batches
# MAGIC ORDER BY start_time DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC **Bronze Layer**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare.bronze.bronze_patients
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC **Bronze Layer Metadata**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC file_name,
# MAGIC file_path,
# MAGIC batch_id,
# MAGIC processed_timestamp
# MAGIC FROM healthcare.bronze.bronze_patients;

# COMMAND ----------

# MAGIC %md
# MAGIC **Duplicate Records Count in Bronze**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC patient_id,
# MAGIC COUNT(*) as duplicate_count
# MAGIC FROM healthcare.bronze.bronze_patients
# MAGIC GROUP BY patient_id
# MAGIC HAVING COUNT(*)>1;

# COMMAND ----------

# MAGIC %md
# MAGIC **File Inventory**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare.bronze.file_inventory;

# COMMAND ----------

# MAGIC %md
# MAGIC **Silver Layer**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare.silver.silver_patients
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC **Check duplicates**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC patient_id,
# MAGIC COUNT(*)
# MAGIC FROM healthcare.silver.silver_patients
# MAGIC GROUP BY patient_id
# MAGIC HAVING COUNT(*)>1;

# COMMAND ----------

# MAGIC %md
# MAGIC Standardized Department

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT service
# MAGIC FROM healthcare.silver.silver_patients;

# COMMAND ----------

# MAGIC %md
# MAGIC Length of Stay

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC patient_id,
# MAGIC arrival_date,
# MAGIC departure_date
# MAGIC length_of_stay
# MAGIC FROM healthcare.silver.silver_patients
# MAGIC LIMIT 10;

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Null Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare.silver.quarantine_patients;

# COMMAND ----------

# MAGIC %md
# MAGIC **Gold Layer**
# MAGIC Patient Flow

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare.gold.patient_flow;

# COMMAND ----------

# MAGIC %md
# MAGIC Bed Utilization

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare.gold.bed_utilization;

# COMMAND ----------

# MAGIC %md
# MAGIC Staff Efficiency

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare.gold.staff_efficiency;

# COMMAND ----------

# MAGIC %md
# MAGIC Operational Efficiency

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare.gold.operational_efficiency;

# COMMAND ----------

# MAGIC %md
# MAGIC Bottleneck Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare.gold.bottleneck_analysis;