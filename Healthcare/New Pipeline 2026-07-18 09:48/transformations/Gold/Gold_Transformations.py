from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    count,
    avg,
    min,
    max,
    sum,
    round,
    when,
    current_timestamp,
    lit
)


# ==========================================================
# GOLD PATIENT FLOW
# ==========================================================

@dp.table(
    name="gold.patient_flow",
    comment="Patient flow analytics with batch lineage",
    cluster_by=["service", "batch_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days",
        "delta.logRetentionDuration": "interval 30 days"
    }
)
def patient_flow():

    df = dp.read(
        "silver.silver_patients"
    )

    return (

        df

        .groupBy(
            "service",
            "batch_id"
        )

        .agg(

            count("*").alias(
                "total_patients"
            ),

            round(
                avg("length_of_stay"),
                2
            ).alias(
                "avg_length_of_stay"
            ),

            round(
                avg("satisfaction"),
                2
            ).alias(
                "avg_patient_satisfaction"
            ),

            min("length_of_stay").alias(
                "min_los"
            ),

            max("length_of_stay").alias(
                "max_los"
            )

        )

        .withColumn(
            "processed_timestamp",
            current_timestamp()
        )

        .withColumn(
            "pipeline_stage",
            lit("GOLD")
        )

    )


# ==========================================================
# GOLD STAFF EFFICIENCY
# ==========================================================

@dp.table(
    name="gold.staff_efficiency",
    comment="Staff efficiency analytics with batch lineage",
    cluster_by=["service", "role", "batch_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days",
        "delta.logRetentionDuration": "interval 30 days"
    }
)
def staff_efficiency():

    df = dp.read(
        "silver.silver_staff_schedule"
    )

    return (

        df

        .groupBy(
            "service",
            "role",
            "batch_id"
        )

        .agg(

            count("*").alias(
                "total_schedule_records"
            ),

            sum("present").alias(
                "days_present"
            ),

            round(
                avg("present") * 100,
                2
            ).alias(
                "attendance_rate"
            )

        )

        .withColumn(
            "processed_timestamp",
            current_timestamp()
        )

        .withColumn(
            "pipeline_stage",
            lit("GOLD")
        )

    )


# ==========================================================
# GOLD BED UTILIZATION
# ==========================================================

@dp.table(
    name="gold.bed_utilization",
    comment="Bed utilization analytics with batch lineage",
    cluster_by=["service", "batch_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days",
        "delta.logRetentionDuration": "interval 30 days"
    }
)
def bed_utilization():

    df = dp.read(
        "silver.silver_services"
    )

    return (

        df

        .groupBy(
            "service",
            "batch_id"
        )

        .agg(

            round(
                avg("occupancy_pct"),
                2
            ).alias(
                "avg_occupancy_pct"
            ),

            round(
                avg("capacity_gap"),
                2
            ).alias(
                "avg_capacity_gap"
            ),

            round(
                avg("admission_rate") * 100,
                2
            ).alias(
                "avg_admission_rate_pct"
            ),

            round(
                avg("refusal_rate") * 100,
                2
            ).alias(
                "avg_refusal_rate_pct"
            )

        )

        .withColumn(
            "processed_timestamp",
            current_timestamp()
        )

        .withColumn(
            "pipeline_stage",
            lit("GOLD")
        )

    )


# ==========================================================
# GOLD OPERATIONAL EFFICIENCY DASHBOARD
# ==========================================================

@dp.table(
    name="gold.operational_efficiency",
    comment="Healthcare operational efficiency dashboard metrics with batch lineage",
    cluster_by=["service", "batch_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days",
        "delta.logRetentionDuration": "interval 30 days"
    }
)
def operational_efficiency():

    services = dp.read(
        "silver.silver_services"
    )


    attendance = (

        dp.read(
            "silver.silver_staff_schedule"
        )

        .groupBy(
            "service",
            "batch_id"
        )

        .agg(

            round(
                avg("present") * 100,
                2
            ).alias(
                "attendance_rate"
            )

        )

    )


    patient_metrics = (

        dp.read(
            "silver.silver_patients"
        )

        .groupBy(
            "service",
            "batch_id"
        )

        .agg(

            round(
                avg("satisfaction"),
                2
            ).alias(
                "patient_satisfaction"
            ),

            round(
                avg("length_of_stay"),
                2
            ).alias(
                "avg_length_of_stay"
            ),

            count("*").alias(
                "total_patients"
            )

        )

    )


    service_metrics = (

        services

        .groupBy(
            "service",
            "batch_id"
        )

        .agg(

            round(
                avg("occupancy_pct"),
                2
            ).alias(
                "occupancy_pct"
            ),

            round(
                avg("admission_rate") * 100,
                2
            ).alias(
                "admission_rate_pct"
            ),

            round(
                avg("refusal_rate") * 100,
                2
            ).alias(
                "refusal_rate_pct"
            ),

            round(
                avg("staff_morale"),
                2
            ).alias(
                "staff_morale"
            ),

            round(
                avg("patient_satisfaction"),
                2
            ).alias(
                "weekly_patient_satisfaction"
            ),

            round(
                avg("capacity_gap"),
                2
            ).alias(
                "capacity_gap"
            )

        )

    )


    return (

        service_metrics

        .join(
            attendance,
            [
                "service",
                "batch_id"
            ],
            "left"
        )

        .join(
            patient_metrics,
            [
                "service",
                "batch_id"
            ],
            "left"
        )

        .withColumn(
            "severity",
            when(
                (col("occupancy_pct") > 90) | (col("capacity_gap") > 30),
                lit("CRITICAL")
            )
            .when(
                col("refusal_rate_pct") > 30,
                lit("HIGH")
            )
            .when(
                col("attendance_rate") < 70,
                lit("MEDIUM")
            )
            .when(
                col("staff_morale") < 50,
                lit("LOW")
            )
            .otherwise(
                lit("NORMAL")
            )
        )

        .withColumn(
            "processed_timestamp",
            current_timestamp()
        )

        .withColumn(
            "pipeline_stage",
            lit("GOLD")
        )

    )



# ==========================================================
# GOLD BOTTLENECK ANALYSIS
# ==========================================================

@dp.table(
    name="gold.bottleneck_analysis",
    comment="Healthcare bottleneck identification with batch lineage",
    cluster_by=["service", "batch_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days",
        "delta.logRetentionDuration": "interval 30 days"
    }
)
def bottleneck_analysis():

    df = dp.read(
        "gold.operational_efficiency"
    )

    return (

        df

        .withColumn(
            "avg_occupancy_pct",
            col("occupancy_pct")
        )

        .withColumn(
            "avg_capacity_gap",
            col("capacity_gap")
        )

        .withColumn(
            "avg_refusal_rate",
            round(col("refusal_rate_pct") / 100.0, 2)
        )

        .withColumn(
            "avg_admission_rate",
            round(col("admission_rate_pct") / 100.0, 2)
        )

        .withColumn(
            "bottleneck_flag",
            when(
                (col("avg_occupancy_pct") > 90) |
                (col("avg_refusal_rate") > 0.20),
                "YES"
            )
            .otherwise("NO")
        )

        .withColumn(
            "processed_timestamp",
            current_timestamp()
        )

        .withColumn(
            "pipeline_stage",
            lit("GOLD")
        )

    )