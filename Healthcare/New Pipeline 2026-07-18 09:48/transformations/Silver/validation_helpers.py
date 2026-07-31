from pyspark.sql.functions import (
    col,
    trim,
    lower,
    to_date,
    datediff,
    when,
    array,
    filter as array_filter,
    lit,
    round,
    coalesce
)

# Constants
VALIDATION_VERSION = "v1.2"
VALID_ROLES = ["doctor", "nurse", "technician", "administrator"]


# ==========================================================
# NORMALIZATION HELPERS
# ==========================================================

def normalize_patients(df):
    """Apply standard normalization to raw patient data."""
    return (
        df
        .withColumn("service", lower(trim(col("service"))))
        .withColumn("age", col("age").cast("integer"))
        .withColumn("arrival_date", to_date(col("arrival_date")))
        .withColumn("departure_date", to_date(col("departure_date")))
        .withColumn("satisfaction", col("satisfaction").cast("integer"))
        .withColumn("length_of_stay", datediff(col("departure_date"), col("arrival_date")))
    )


def normalize_staff(df):
    """Apply standard normalization to raw staff data."""
    return (
        df
        .withColumn("role", lower(trim(col("role"))))
        .withColumn("service", lower(trim(col("service"))))
    )


def normalize_staff_schedule(df):
    """Apply standard normalization to raw staff schedule data."""
    return (
        df
        .withColumn("week", col("week").cast("integer"))
        .withColumn("present", col("present").cast("integer"))
        .withColumn("role", lower(trim(col("role"))))
        .withColumn("service", lower(trim(col("service"))))
    )


def normalize_services(df):
    """Apply standard normalization to raw services weekly data."""
    return (
        df
        .withColumn("service", lower(trim(col("service"))))
        .withColumn("week", col("week").cast("integer"))
        .withColumn("month", col("month").cast("integer"))
        .withColumn("available_beds", col("available_beds").cast("integer"))
        .withColumn("patients_request", col("patients_request").cast("integer"))
        .withColumn("patients_admitted", col("patients_admitted").cast("integer"))
        .withColumn("patients_refused", col("patients_refused").cast("integer"))
        .withColumn("patient_satisfaction", col("patient_satisfaction").cast("integer"))
        .withColumn("staff_morale", col("staff_morale").cast("integer"))
        .withColumn("event", lower(trim(col("event"))))
    )


# ==========================================================
# UNIFIED PIPELINE PROCESSING HELPERS
# ==========================================================

def process_patients_bronze(df):
    """Normalize and validate Bronze patient records (uses pre-existing record_hash from Bronze)."""
    df_normalized = normalize_patients(df)
    return validate_patients(df_normalized)


def process_staff_bronze(df):
    """Normalize and validate Bronze staff records (uses pre-existing record_hash from Bronze)."""
    df_normalized = normalize_staff(df)
    return validate_staff(df_normalized)


def process_staff_schedule_bronze(df):
    """Normalize and validate Bronze staff schedule records (uses pre-existing record_hash from Bronze)."""
    df_normalized = normalize_staff_schedule(df)
    return validate_staff_schedule(df_normalized)


def process_services_bronze(df):
    """Normalize, calculate KPIs, and validate Bronze services weekly records (uses pre-existing record_hash from Bronze)."""
    df_normalized = normalize_services(df)
    df_base_val = validate_services(df_normalized)
    df_kpis = calculate_service_kpis(df_base_val)
    return validate_services_with_kpis(df_kpis)


# ==========================================================
# SHARED VALIDATION FUNCTIONS
# ==========================================================

def validate_patients(df):
    """Apply validation rules to normalized patient data."""
    return (
        df
        .withColumn("valid_patient_id", col("patient_id").isNotNull())
        .withColumn("valid_arrival_date", col("arrival_date").isNotNull())
        .withColumn("valid_departure_date", col("departure_date").isNotNull())
        .withColumn("valid_patient_name", col("name").isNotNull())
        .withColumn("valid_service", col("service").isNotNull() & (trim(col("service")) != ""))
        .withColumn("valid_age", col("age").between(0, 110))
        .withColumn("valid_satisfaction", col("satisfaction").between(0, 100))
        .withColumn("valid_dates", col("departure_date") >= col("arrival_date"))
        .withColumn("valid_length_of_stay", col("length_of_stay").between(0, 180))
        .withColumn(
            "validation_passed",
            coalesce(
                col("valid_patient_id") & col("valid_arrival_date") & col("valid_departure_date") &
                col("valid_patient_name") & col("valid_service") & col("valid_age") &
                col("valid_satisfaction") & col("valid_dates") & col("valid_length_of_stay"),
                lit(False)
            )
        )
    )


def build_patients_failed_rules(df):
    """Build failed_rules and rejection_reasons for quarantined patients."""
    return (
        df
        .withColumn(
            "failed_rules",
            array_filter(
                array(
                    when(~col("valid_patient_id"), lit("valid_patient_id")),
                    when(~col("valid_arrival_date"), lit("valid_arrival_date")),
                    when(~col("valid_departure_date"), lit("valid_departure_date")),
                    when(~col("valid_patient_name"), lit("valid_patient_name")),
                    when(~col("valid_service"), lit("valid_service")),
                    when(~col("valid_age"), lit("valid_age")),
                    when(~col("valid_satisfaction"), lit("valid_satisfaction")),
                    when(~col("valid_dates"), lit("valid_dates")),
                    when(~col("valid_length_of_stay"), lit("valid_length_of_stay"))
                ),
                lambda x: x.isNotNull()
            )
        )
        .withColumn(
            "rejection_reasons",
            array_filter(
                array(
                    when(~col("valid_patient_id"), lit("Patient ID cannot be null")),
                    when(~col("valid_arrival_date"), lit("Arrival date cannot be null")),
                    when(~col("valid_departure_date"), lit("Departure date cannot be null")),
                    when(~col("valid_patient_name"), lit("Patient name cannot be null")),
                    when(~col("valid_service"), lit("Service cannot be null or empty")),
                    when(~col("valid_age"), lit("Age must be between 0 and 110")),
                    when(~col("valid_satisfaction"), lit("Satisfaction must be between 0 and 100")),
                    when(~col("valid_dates"), lit("Departure date must be >= arrival date")),
                    when(~col("valid_length_of_stay"), lit("Length of stay must be between 0 and 180 days"))
                ),
                lambda x: x.isNotNull()
            )
        )
    )


def validate_staff(df):
    """Apply validation rules to normalized staff data."""
    return (
        df
        .withColumn("valid_staff_id", col("staff_id").isNotNull())
        .withColumn("valid_staff_name", col("staff_name").isNotNull() & (trim(col("staff_name")) != ""))
        .withColumn("valid_role", col("role").isNotNull() & (trim(col("role")) != ""))
        .withColumn("valid_service", col("service").isNotNull() & (trim(col("service")) != ""))
        .withColumn("valid_role_value", col("role").isin(*VALID_ROLES))
        .withColumn(
            "validation_passed",
            coalesce(
                col("valid_staff_id") & col("valid_staff_name") & col("valid_role") &
                col("valid_service") & col("valid_role_value"),
                lit(False)
            )
        )
    )


def build_staff_failed_rules(df):
    """Build failed_rules and rejection_reasons for quarantined staff."""
    return (
        df
        .withColumn(
            "failed_rules",
            array_filter(
                array(
                    when(~col("valid_staff_id"), lit("valid_staff_id")),
                    when(~col("valid_staff_name"), lit("valid_staff_name")),
                    when(~col("valid_role"), lit("valid_role")),
                    when(~col("valid_service"), lit("valid_service")),
                    when(~col("valid_role_value"), lit("valid_role_value"))
                ),
                lambda x: x.isNotNull()
            )
        )
        .withColumn(
            "rejection_reasons",
            array_filter(
                array(
                    when(~col("valid_staff_id"), lit("Staff ID cannot be null")),
                    when(~col("valid_staff_name"), lit("Staff name cannot be null or empty")),
                    when(~col("valid_role"), lit("Role cannot be null or empty")),
                    when(~col("valid_service"), lit("Service cannot be null or empty")),
                    when(~col("valid_role_value"), lit("Role must be one of: " + ", ".join(VALID_ROLES)))
                ),
                lambda x: x.isNotNull()
            )
        )
    )


def validate_staff_schedule(df):
    """Apply validation rules to normalized staff schedule data."""
    return (
        df
        .withColumn("valid_staff_id", col("staff_id").isNotNull())
        .withColumn("valid_presence", col("present").isin(0, 1))
        .withColumn("valid_week", col("week") > 0)
        .withColumn("valid_role", col("role").isNotNull() & (trim(col("role")) != ""))
        .withColumn("valid_service", col("service").isNotNull() & (trim(col("service")) != ""))
        .withColumn("valid_role_value", col("role").isin(*VALID_ROLES))
        .withColumn(
            "validation_passed",
            coalesce(
                col("valid_staff_id") & col("valid_presence") & col("valid_week") &
                col("valid_role") & col("valid_service") & col("valid_role_value"),
                lit(False)
            )
        )
    )


def build_staff_schedule_failed_rules(df):
    """Build failed_rules and rejection_reasons for quarantined schedule."""
    return (
        df
        .withColumn(
            "failed_rules",
            array_filter(
                array(
                    when(~col("valid_staff_id"), lit("valid_staff_id")),
                    when(~col("valid_presence"), lit("valid_presence")),
                    when(~col("valid_week"), lit("valid_week")),
                    when(~col("valid_role"), lit("valid_role")),
                    when(~col("valid_service"), lit("valid_service")),
                    when(~col("valid_role_value"), lit("valid_role_value"))
                ),
                lambda x: x.isNotNull()
            )
        )
        .withColumn(
            "rejection_reasons",
            array_filter(
                array(
                    when(~col("valid_staff_id"), lit("Staff ID cannot be null")),
                    when(~col("valid_presence"), lit("Present must be 0 or 1")),
                    when(~col("valid_week"), lit("Week must be greater than 0")),
                    when(~col("valid_role"), lit("Role cannot be null or empty")),
                    when(~col("valid_service"), lit("Service cannot be null or empty")),
                    when(~col("valid_role_value"), lit("Role must be one of: " + ", ".join(VALID_ROLES)))
                ),
                lambda x: x.isNotNull()
            )
        )
    )


def validate_services(df):
    """Apply validation rules to normalized services data."""
    return (
        df
        .withColumn("valid_service", col("service").isNotNull() & (trim(col("service")) != ""))
        .withColumn("valid_week", col("week").between(1, 53))
        .withColumn("valid_month", col("month").between(1, 12))
        .withColumn("valid_available_beds", col("available_beds") > 0)
        .withColumn("valid_patients_request", col("patients_request") >= 0)
        .withColumn("valid_patients_admitted", col("patients_admitted") >= 0)
        .withColumn("valid_patients_refused", col("patients_refused") >= 0)
        .withColumn("valid_patient_satisfaction", col("patient_satisfaction").between(0, 100))
        .withColumn("valid_staff_morale", col("staff_morale").between(0, 100))
        .withColumn("valid_admitted_vs_beds", col("patients_admitted") <= col("available_beds"))
        .withColumn("valid_requests_vs_admitted", col("patients_request") >= col("patients_admitted"))
        .withColumn("valid_requests_vs_refused", col("patients_request") >= col("patients_refused"))
        .withColumn("valid_request_balance", col("patients_request") == (col("patients_admitted") + col("patients_refused")))
        .withColumn("valid_event", col("event").isNotNull() & (trim(col("event")) != ""))
        .withColumn(
            "validation_passed",
            coalesce(
                col("valid_service") & col("valid_week") & col("valid_month") & col("valid_available_beds") &
                col("valid_patients_request") & col("valid_patients_admitted") & col("valid_patients_refused") &
                col("valid_patient_satisfaction") & col("valid_staff_morale") & col("valid_admitted_vs_beds") &
                col("valid_requests_vs_admitted") & col("valid_requests_vs_refused") & col("valid_request_balance") &
                col("valid_event"),
                lit(False)
            )
        )
    )


def calculate_service_kpis(df):
    """Calculate occupancy_rate, admission_rate, refusal_rate, capacity_gap, occupancy_pct on services data."""
    return (
        df
        .withColumn(
            "occupancy_rate",
            round(
                col("patients_admitted") /
                when(col("available_beds") == 0, None).otherwise(col("available_beds")),
                2
            )
        )
        .withColumn(
            "admission_rate",
            round(
                col("patients_admitted") /
                when(col("patients_request") == 0, None).otherwise(col("patients_request")),
                2
            )
        )
        .withColumn(
            "refusal_rate",
            round(
                col("patients_refused") /
                when(col("patients_request") == 0, None).otherwise(col("patients_request")),
                2
            )
        )
        .withColumn(
            "capacity_gap",
            when(
                col("patients_request") > col("available_beds"),
                col("patients_request") - col("available_beds")
            ).otherwise(0)
        )
        .withColumn(
            "occupancy_pct",
            round(col("occupancy_rate") * 100, 2)
        )
    )


def validate_services_with_kpis(df):
    """Apply validation INCLUDING KPI range checks. Call AFTER KPI calculations."""
    df_validated = df if "validation_passed" in df.columns else validate_services(df)
    return (
        df_validated
        .withColumn("valid_occupancy_rate", col("occupancy_rate").isNotNull() & (col("occupancy_rate") <= 1))
        .withColumn("valid_admission_rate", col("admission_rate").isNotNull() & col("admission_rate").between(0, 1))
        .withColumn("valid_refusal_rate", col("refusal_rate").isNotNull() & col("refusal_rate").between(0, 1))
        .withColumn(
            "validation_passed",
            coalesce(
                col("validation_passed") & col("valid_occupancy_rate") &
                col("valid_admission_rate") & col("valid_refusal_rate"),
                lit(False)
            )
        )
    )


def build_services_failed_rules(df, include_kpi_validation=False):
    """Build failed_rules and rejection_reasons for quarantined services."""
    base_rules = [
        when(~col("valid_service"), lit("valid_service")),
        when(~col("valid_week"), lit("valid_week")),
        when(~col("valid_month"), lit("valid_month")),
        when(~col("valid_available_beds"), lit("valid_available_beds")),
        when(~col("valid_patients_request"), lit("valid_patients_request")),
        when(~col("valid_patients_admitted"), lit("valid_patients_admitted")),
        when(~col("valid_patients_refused"), lit("valid_patients_refused")),
        when(~col("valid_patient_satisfaction"), lit("valid_patient_satisfaction")),
        when(~col("valid_staff_morale"), lit("valid_staff_morale")),
        when(~col("valid_admitted_vs_beds"), lit("valid_admitted_vs_beds")),
        when(~col("valid_requests_vs_admitted"), lit("valid_requests_vs_admitted")),
        when(~col("valid_requests_vs_refused"), lit("valid_requests_vs_refused")),
        when(~col("valid_request_balance"), lit("valid_request_balance")),
        when(~col("valid_event"), lit("valid_event"))
    ]
    
    base_reasons = [
        when(~col("valid_service"), lit("Service cannot be null or empty")),
        when(~col("valid_week"), lit("Week must be between 1 and 53")),
        when(~col("valid_month"), lit("Month must be between 1 and 12")),
        when(~col("valid_available_beds"), lit("Available beds must be greater than 0")),
        when(~col("valid_patients_request"), lit("Patients requested must be >= 0")),
        when(~col("valid_patients_admitted"), lit("Patients admitted must be >= 0")),
        when(~col("valid_patients_refused"), lit("Patients refused must be >= 0")),
        when(~col("valid_patient_satisfaction"), lit("Patient satisfaction must be between 0 and 100")),
        when(~col("valid_staff_morale"), lit("Staff morale must be between 0 and 100")),
        when(~col("valid_admitted_vs_beds"), lit("Patients admitted cannot exceed available beds")),
        when(~col("valid_requests_vs_admitted"), lit("Patients requested must be >= patients admitted")),
        when(~col("valid_requests_vs_refused"), lit("Patients requested must be >= patients refused")),
        when(~col("valid_request_balance"), lit("Patients requested must equal admitted + refused")),
        when(~col("valid_event"), lit("Event cannot be null or empty"))
    ]
    
    if include_kpi_validation:
        kpi_rules = [
            when(~col("valid_occupancy_rate"), lit("valid_occupancy_rate")),
            when(~col("valid_admission_rate"), lit("valid_admission_rate")),
            when(~col("valid_refusal_rate"), lit("valid_refusal_rate"))
        ]
        kpi_reasons = [
            when(~col("valid_occupancy_rate"), lit("Occupancy rate must be <= 1")),
            when(~col("valid_admission_rate"), lit("Admission rate must be between 0 and 1")),
            when(~col("valid_refusal_rate"), lit("Refusal rate must be between 0 and 1"))
        ]
        all_rules = base_rules + kpi_rules
        all_reasons = base_reasons + kpi_reasons
    else:
        all_rules = base_rules
        all_reasons = base_reasons
    
    return (
        df
        .withColumn("failed_rules", array_filter(array(*all_rules), lambda x: x.isNotNull()))
        .withColumn("rejection_reasons", array_filter(array(*all_reasons), lambda x: x.isNotNull()))
    )
