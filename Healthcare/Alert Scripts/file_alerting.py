# Databricks notebook source
# ==========================================================
# DAILY FILE COUNT MONITORING NOTEBOOK
# ==========================================================
#
# FEATURES
# ----------------------------------------------------------
# 1. Dataset-Level Validation (patients, staff, staff_schedule, services_weekly)
# 2. Strict Exact-Count Anomaly Detection (ALLOW_MULTIPLE_FILES)
# 3. Databricks Job Metadata Capture (job_id, run_id)
# 4. Intelligent Alert Decision (SUCCESS vs ALERT)
# 5. Configurable Notification Policy (SEND_SUCCESS_EMAIL)
# 6. Multi-Recipient Email Support (RECEIVER_EMAILS)
# 7. Deterministic Latest File Selection (Sorted by Modification Time)
# 8. Delta Table Audit Persistence (healthcare.monitoring.file_monitoring_audit)
# 9. Accurate Time Zone Labeling (Cluster Time)
# 10. Executive Dashboard-Style HTML Email Report
# 11. Robust Exception Capture & Emergency Error Reporting
# 12. Structured Logging
# ==========================================================

import logging
import smtplib
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# ==========================================================
# CONFIGURATION
# ==========================================================
SENDER_EMAIL = "zs7919320@gmail.com"
RECEIVER_EMAILS = [
    "zubair.syed1@tcs.com"
]

BASE_PATH = "s3://zubair-s3-demo/raw_dataset/hospital_dataset/"

EXPECTED_DATASETS = {
    "patients": 1,
    "staff": 1,
    "staff_schedule": 1,
    "services_weekly": 1
}

# Operational Controls
ALLOW_MULTIPLE_FILES = False # If False, receiving > expected files triggers an anomaly ALERT
SEND_SUCCESS_EMAIL = True    # Set to False to send emails ONLY on ALERT or ERROR
PERSIST_TO_DELTA = True      # Set to True to persist execution details to Delta audit table

DELTA_AUDIT_TABLE = "healthcare.monitoring.file_monitoring_audit"


# ==========================================================
# LOGGING SETUP
# ==========================================================
def setup_logging():
    logger = logging.getLogger("HealthcareFileMonitoring")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger

logger = setup_logging()


# ==========================================================
# SECRET & JOB METADATA RETRIEVAL
# ==========================================================
def get_smtp_password():
    try:
        return dbutils.secrets.get(
            scope="healthcare",
            key="smtp_password"
        )
    except Exception as e:
        logger.error(f"Failed to retrieve SMTP password from secret scope: {str(e)}")
        raise


def get_job_context():
    """
    Safely retrieves Databricks Job metadata (Job ID, Run ID) if executing within a Databricks Job workflow.
    Uses .isDefined() and .get() on Scala Option objects to ensure maximum runtime compatibility.
    """
    job_id = "N/A"
    run_id = "N/A"
    try:
        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        
        # Check Job ID Option
        if hasattr(context, "jobId") and context.jobId().isDefined():
            job_id = str(context.jobId().get())
            
        # Check Run ID Option (trying currentRunId first, then fallback to id)
        if hasattr(context, "currentRunId") and context.currentRunId().isDefined():
            run_id = str(context.currentRunId().get())
        elif hasattr(context, "id") and context.id().isDefined():
            run_id = str(context.id().get())
    except Exception:
        pass
    return {"job_id": job_id, "run_id": run_id}


# ==========================================================
# DATASET VALIDATION FUNCTIONS
# ==========================================================
def get_today_files(base_path, dataset_name):
    """
    Lists files for today's date under the specified dataset folder in S3.
    Returns a list of dicts sorted descending by modification time (newest first).
    """
    s3_folder_path = f"{base_path.rstrip('/')}/{dataset_name}/"
    today = datetime.now().date()
    today_files = []

    try:
        files = dbutils.fs.ls(s3_folder_path)
        for file in files:
            # Convert modification time from milliseconds to datetime object
            modified_dt = datetime.fromtimestamp(file.modificationTime / 1000)
            if modified_dt.date() == today:
                today_files.append({
                    "name": file.name,
                    "modified_time": modified_dt,
                    "size": file.size
                })
    except Exception as e:
        logger.error(f"Error accessing path {s3_folder_path}: {str(e)}")
        raise RuntimeError(f"Directory access failure at {s3_folder_path}: {str(e)}")

    # Deterministic sorting by modification time (newest file first)
    today_files.sort(key=lambda x: x["modified_time"], reverse=True)
    return today_files


def validate_dataset(base_path, dataset_name, expected_count):
    """
    Validates a single dataset folder against expected daily arrival count.
    Returns a validation result dictionary.
    """
    s3_path = f"{base_path.rstrip('/')}/{dataset_name}/"
    error_msg = None
    files_received = []

    try:
        files_received = get_today_files(base_path, dataset_name)
    except Exception as e:
        error_msg = str(e)

    received_count = len(files_received)
    
    if error_msg:
        is_received = False
        status = "Error"
        reason = error_msg
    elif received_count == expected_count:
        is_received = True
        status = "Received"
        reason = "Expected file received today"
    elif received_count == 0:
        is_received = False
        status = "Missing"
        reason = "No file received today"
    elif received_count < expected_count:
        is_received = False
        status = "Incomplete"
        reason = f"Only {received_count}/{expected_count} expected files received today"
    else:  # received_count > expected_count
        is_received = ALLOW_MULTIPLE_FILES
        status = "Multiple Files" if not ALLOW_MULTIPLE_FILES else "Received (Multiple)"
        reason = f"Multiple files received today ({received_count} files arrived)"

    latest_file_name = files_received[0]["name"] if files_received else "N/A"
    last_mod_str = files_received[0]["modified_time"].strftime("%Y-%m-%d %H:%M:%S") if files_received else "N/A"

    return {
        "dataset": dataset_name,
        "s3_path": s3_path,
        "expected_count": expected_count,
        "received_count": received_count,
        "status": status,
        "is_received": is_received,
        "latest_file_name": latest_file_name,
        "last_modified_time": last_mod_str,
        "reason": reason,
        "error_message": error_msg
    }


def build_summary(validation_results, start_time, email_sent_flag=True):
    """
    Aggregates dataset validation results into an executive summary and audit records.
    """
    now = datetime.now()
    exec_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    monitoring_date = now.strftime("%Y-%m-%d")
    exec_time_str = now.strftime("%H:%M:%S Local Cluster Time")
    duration = round(time.time() - start_time, 2)
    job_ctx = get_job_context()

    total_expected = sum(r["expected_count"] for r in validation_results)
    total_received = sum(r["received_count"] for r in validation_results)
    missing_datasets = [r for r in validation_results if not r["is_received"]]
    missing_count = len(missing_datasets)

    overall_status = "SUCCESS" if missing_count == 0 else "ALERT"

    # Audit records prepared for Delta monitoring table insertion
    audit_records = []
    for r in validation_results:
        audit_records.append({
            "execution_timestamp": exec_timestamp,
            "monitoring_date": monitoring_date,
            "job_id": job_ctx["job_id"],
            "run_id": job_ctx["run_id"],
            "dataset": r["dataset"],
            "s3_path": r["s3_path"],
            "expected_files": r["expected_count"],
            "received_files": r["received_count"],
            "status": r["status"],
            "latest_file_name": r["latest_file_name"],
            "last_modified_time": r["last_modified_time"],
            "reason": r["reason"],
            "error_message": r["error_message"],
            "overall_result": overall_status,
            "execution_duration_seconds": duration,
            "email_sent": email_sent_flag
        })

    return {
        "execution_timestamp": exec_timestamp,
        "monitoring_date": monitoring_date,
        "execution_time": exec_time_str,
        "job_id": job_ctx["job_id"],
        "run_id": job_ctx["run_id"],
        "duration_seconds": duration,
        "total_expected_files": total_expected,
        "total_received_files": total_received,
        "total_expected_datasets": len(validation_results),
        "total_received_datasets": len(validation_results) - missing_count,
        "missing_datasets_count": missing_count,
        "missing_datasets": missing_datasets,
        "overall_status": overall_status,
        "dataset_results": validation_results,
        "audit_records": audit_records
    }


# ==========================================================
# DELTA TABLE AUDIT PERSISTENCE
# ==========================================================
def save_audit_to_delta(audit_records, table_name=DELTA_AUDIT_TABLE):
    """
    Persists structured audit records into Delta monitoring table.
    """
    if not audit_records:
        return

    try:
        audit_df = spark.createDataFrame(audit_records)
        (
            audit_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(table_name)
        )
        logger.info(f"Successfully persisted {len(audit_records)} audit record(s) to Delta table: {table_name}")
    except Exception as e:
        logger.warning(f"Could not persist audit records to Delta table '{table_name}': {str(e)}")


# ==========================================================
# REPORT GENERATION & EMAIL SENDING
# ==========================================================
def generate_html_report(summary):
    """
    Generates a professional executive HTML monitoring report with dynamic theme colors.
    """
    status = summary["overall_status"]
    is_success = (status == "SUCCESS")

    # Theme colors
    header_bg = "#2e7d32" if is_success else "#c62828"
    status_color = "#2e7d32" if is_success else "#c62828"
    badge_bg = "#e8f5e9" if is_success else "#ffebee"

    # KPI summary cards
    kpi_cards = f"""
    <table width="100%" cellpadding="10" style="border-collapse:collapse; font-family:Arial; margin-bottom:20px;">
    <tr>
        <td align="center" style="background:#f5f5f5; border-radius:8px; padding:15px; width:16%;">
            <h2 style="margin:0; color:#37474f; font-size:22px;">{summary['total_expected_files']}</h2>
            <div style="font-weight:bold; color:#546e7a; font-size:11px; margin-top:4px;">Expected Files</div>
        </td>
        <td align="center" style="background:#e8f5e9; border-radius:8px; padding:15px; width:16%;">
            <h2 style="margin:0; color:#2e7d32; font-size:22px;">{summary['total_received_files']}</h2>
            <div style="font-weight:bold; color:#1b5e20; font-size:11px; margin-top:4px;">Received Files</div>
        </td>
        <td align="center" style="background:#ffebee; border-radius:8px; padding:15px; width:16%;">
            <h2 style="margin:0; color:#c62828; font-size:22px;">{summary['missing_datasets_count']}</h2>
            <div style="font-weight:bold; color:#b71c1c; font-size:11px; margin-top:4px;">Missing / Anomaly</div>
        </td>
        <td align="center" style="background:{badge_bg}; border-radius:8px; padding:15px; width:20%;">
            <h2 style="margin:0; color:{status_color}; font-size:20px;">{status}</h2>
            <div style="font-weight:bold; color:{status_color}; font-size:11px; margin-top:4px;">Monitoring Status</div>
        </td>
        <td align="center" style="background:#f5f5f5; border-radius:8px; padding:15px; width:16%;">
            <h3 style="margin:0; color:#37474f; font-size:14px;">{summary['monitoring_date']}</h3>
            <div style="font-weight:bold; color:#546e7a; font-size:11px; margin-top:4px;">Execution Date</div>
        </td>
        <td align="center" style="background:#f5f5f5; border-radius:8px; padding:15px; width:16%;">
            <h3 style="margin:0; color:#37474f; font-size:13px;">{summary['execution_time']}</h3>
            <div style="font-weight:bold; color:#546e7a; font-size:11px; margin-top:4px;">Execution Time</div>
        </td>
    </tr>
    </table>
    """

    # Detailed Dataset Monitoring Table
    table_rows = ""
    for r in summary["dataset_results"]:
        row_bg = "#e8f5e9" if r["is_received"] else "#ffebee"
        
        if r["status"] == "Received":
            status_badge = '<span style="color:#2e7d32; font-weight:bold;">✅ Received</span>'
        elif r["status"] == "Missing":
            status_badge = '<span style="color:#c62828; font-weight:bold;">❌ Missing</span>'
        elif r["status"] == "Multiple Files":
            status_badge = '<span style="color:#e65100; font-weight:bold;">⚠️ Multiple Files</span>'
        elif r["status"] == "Incomplete":
            status_badge = '<span style="color:#e65100; font-weight:bold;">⚠️ Incomplete</span>'
        else:
            status_badge = f'<span style="color:#c62828; font-weight:bold;">❌ {r["status"]}</span>'
        
        table_rows += f"""
        <tr style="background-color:{row_bg};">
            <td style="padding:10px; border:1px solid #ddd; font-weight:bold;">{r['dataset']}</td>
            <td style="padding:10px; border:1px solid #ddd; text-align:center;">{r['expected_count']}</td>
            <td style="padding:10px; border:1px solid #ddd; text-align:center;">{r['received_count']}</td>
            <td style="padding:10px; border:1px solid #ddd; text-align:center;">{status_badge}</td>
            <td style="padding:10px; border:1px solid #ddd; font-family:monospace;">{r['latest_file_name']}</td>
            <td style="padding:10px; border:1px solid #ddd; text-align:center;">{r['last_modified_time']}</td>
        </tr>
        """

    monitoring_table = f"""
    <table style="border-collapse:collapse; width:100%; font-family:Arial, sans-serif; margin-bottom:25px;">
        <thead>
            <tr style="background-color:{header_bg}; color:white;">
                <th style="padding:10px; border:1px solid #ddd; text-align:left;">Dataset</th>
                <th style="padding:10px; border:1px solid #ddd; text-align:center;">Expected</th>
                <th style="padding:10px; border:1px solid #ddd; text-align:center;">Received</th>
                <th style="padding:10px; border:1px solid #ddd; text-align:center;">Status</th>
                <th style="padding:10px; border:1px solid #ddd; text-align:left;">Latest File Name</th>
                <th style="padding:10px; border:1px solid #ddd; text-align:center;">Last Modified Time</th>
            </tr>
        </thead>
        <tbody>
            {table_rows}
        </tbody>
    </table>
    """

    # Missing Datasets / Anomaly Section (if any missing or anomaly)
    missing_section = ""
    if summary["missing_datasets"]:
        missing_rows = ""
        for m in summary["missing_datasets"]:
            missing_rows += f"""
            <tr>
                <td style="padding:8px; border:1px solid #ffcdd2; font-weight:bold; color:#c62828;">{m['dataset']}</td>
                <td style="padding:8px; border:1px solid #ffcdd2; font-family:monospace; font-size:12px;">{m['s3_path']}</td>
                <td style="padding:8px; border:1px solid #ffcdd2; color:#b71c1c;">{m['reason']}</td>
            </tr>
            """

        missing_section = f"""
        <h2 style="color:#c62828; border-bottom:2px solid #c62828; padding-bottom:5px; margin-top:20px;">
            ⚠️ Missing Dataset & Anomaly Details
        </h2>
        <table style="border-collapse:collapse; width:100%; font-family:Arial, sans-serif; background:#fff5f5; margin-bottom:25px;">
            <thead>
                <tr style="background-color:#c62828; color:white;">
                    <th style="padding:8px; border:1px solid #ffcdd2; text-align:left;">Dataset Name</th>
                    <th style="padding:8px; border:1px solid #ffcdd2; text-align:left;">Expected S3 Path</th>
                    <th style="padding:8px; border:1px solid #ffcdd2; text-align:left;">Reason / Details</th>
                </tr>
            </thead>
            <tbody>
                {missing_rows}
            </tbody>
        </table>
        """

    # Execution Metadata Section
    metadata_section = f"""
    <h2 style="color:#1976D2; border-bottom:2px solid #1976D2; padding-bottom:5px; margin-top:20px;">
        ⚙️ Execution & Job Metadata
    </h2>
    <table style="border-collapse:collapse; width:100%; font-family:Arial, sans-serif; background:#fafafa;">
        <tr>
            <td style="padding:8px; border:1px solid #ddd; font-weight:bold; width:30%;">Execution Timestamp</td>
            <td style="padding:8px; border:1px solid #ddd;">{summary['execution_timestamp']}</td>
        </tr>
        <tr>
            <td style="padding:8px; border:1px solid #ddd; font-weight:bold;">Monitoring Date</td>
            <td style="padding:8px; border:1px solid #ddd;">{summary['monitoring_date']}</td>
        </tr>
        <tr>
            <td style="padding:8px; border:1px solid #ddd; font-weight:bold;">Databricks Job ID</td>
            <td style="padding:8px; border:1px solid #ddd;">{summary['job_id']}</td>
        </tr>
        <tr>
            <td style="padding:8px; border:1px solid #ddd; font-weight:bold;">Databricks Run ID</td>
            <td style="padding:8px; border:1px solid #ddd;">{summary['run_id']}</td>
        </tr>
        <tr>
            <td style="padding:8px; border:1px solid #ddd; font-weight:bold;">Expected Datasets Count</td>
            <td style="padding:8px; border:1px solid #ddd;">{summary['total_expected_datasets']}</td>
        </tr>
        <tr>
            <td style="padding:8px; border:1px solid #ddd; font-weight:bold;">Received Datasets Count</td>
            <td style="padding:8px; border:1px solid #ddd;">{summary['total_received_datasets']}</td>
        </tr>
        <tr>
            <td style="padding:8px; border:1px solid #ddd; font-weight:bold;">Monitoring Duration</td>
            <td style="padding:8px; border:1px solid #ddd;">{summary['duration_seconds']} seconds</td>
        </tr>
        <tr>
            <td style="padding:8px; border:1px solid #ddd; font-weight:bold;">Overall Result</td>
            <td style="padding:8px; border:1px solid #ddd; font-weight:bold; color:{status_color};">{status}</td>
        </tr>
    </table>
    """

    # Footer section
    footer = """
    <br><hr>
    <p style="color:#666666; font-size:12px; text-align:center;">
        Generated Automatically by Databricks Healthcare Monitoring Platform
        <br><br>
        <strong>Architecture Workflow:</strong><br>
        AWS S3 &nbsp;→&nbsp; Daily File Monitoring &nbsp;→&nbsp; Dataset Validation &nbsp;→&nbsp; Alert Decision &nbsp;→&nbsp; Email Notification &nbsp;→&nbsp; Delta Audit
    </p>
    """

    return f"""
    <html>
    <body style="font-family:Arial,Helvetica,sans-serif; background-color:#f4f6f8; padding:20px; margin:0;">
        <div style="max-width:1100px; margin:auto; background:white; padding:25px; border-radius:10px; box-shadow:0px 2px 8px rgba(0,0,0,0.15);">
            <div style="background:{header_bg}; color:white; padding:20px; border-radius:8px; text-align:center;">
                <h1 style="margin:0; font-size:24px;">📊 Healthcare Daily File Arrival Report</h1>
                <p style="margin-top:8px; font-size:14px;">Automated AWS S3 File Arrival Monitoring</p>
            </div>
            <br>
            {kpi_cards}
            <h2 style="color:#1976D2; border-bottom:2px solid #1976D2; padding-bottom:5px;">
                📁 Dataset Arrival Details
            </h2>
            {monitoring_table}
            {missing_section}
            {metadata_section}
            {footer}
        </div>
    </body>
    </html>
    """


def send_email(subject, html_body, recipients=RECEIVER_EMAILS):
    """
    Sends the HTML report via Gmail SMTP securely to multiple recipients.
    """
    if not recipients:
        logger.warning("No email recipients configured. Skipping email dispatch.")
        return

    try:
        app_password = get_smtp_password()

        msg = MIMEMultipart("alternative")
        msg["From"] = SENDER_EMAIL
        msg["To"] = ", ".join(recipients) if isinstance(recipients, list) else recipients
        msg["Subject"] = subject

        msg.attach(MIMEText(html_body, "html"))

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(SENDER_EMAIL, app_password)
        server.send_message(msg)
        server.quit()

        logger.info(f"Email sent successfully to {msg['To']}. Subject: {subject}")
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")
        raise


def generate_error_email(error_message):
    """
    Generates an emergency system error email report when an uncaught exception occurs.
    """
    exec_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    subject = "Healthcare File Monitoring - SYSTEM ERROR"
    
    html_body = f"""
    <html>
    <body style="font-family:Arial,sans-serif; background-color:#f4f6f8; padding:20px;">
        <div style="max-width:800px; margin:auto; background:white; padding:25px; border-radius:10px; border-top:5px solid #d32f2f;">
            <h1 style="color:#d32f2f;">🚨 Healthcare Monitoring Pipeline Error</h1>
            <p>An unexpected error occurred during execution of the Daily File Count Monitoring script.</p>
            <h3>Execution Timestamp: {exec_timestamp}</h3>
            <h3>Error Details:</h3>
            <div style="background:#ffebee; border:1px solid #ffcdd2; padding:15px; border-radius:5px; font-family:monospace; color:#b71c1c;">
                {error_message}
            </div>
            <br><hr>
            <p style="font-size:12px; color:#666; text-align:center;">Databricks Healthcare Monitoring System Emergency Alert</p>
        </div>
    </body>
    </html>
    """
    try:
        send_email(subject, html_body)
    except Exception as e:
        logger.error(f"Failed to send emergency error email: {str(e)}")


# ==========================================================
# MAIN EXECUTION CONTROLLER
# ==========================================================
def main():
    start_time = time.time()
    logger.info("Daily File Count Monitoring pipeline started.")

    try:
        validation_results = []
        for dataset_name, expected_count in EXPECTED_DATASETS.items():
            logger.info(f"Validating dataset: {dataset_name} (Expected: {expected_count})")
            res = validate_dataset(BASE_PATH, dataset_name, expected_count)
            validation_results.append(res)
            logger.info(f"Dataset {dataset_name} status: {res['status']} (Received: {res['received_count']}/{expected_count})")

        # Determine overall status
        missing_count = sum(1 for r in validation_results if not r["is_received"])
        overall_status = "SUCCESS" if missing_count == 0 else "ALERT"

        # Determine whether to send email based on SEND_SUCCESS_EMAIL configuration
        should_send_email = SEND_SUCCESS_EMAIL or (overall_status != "SUCCESS")

        # Build executive summary & audit records
        summary = build_summary(validation_results, start_time, email_sent_flag=should_send_email)

        # Persist audit records to Delta table if enabled
        if PERSIST_TO_DELTA:
            save_audit_to_delta(summary["audit_records"], DELTA_AUDIT_TABLE)

        # Log missing datasets or anomalies
        if summary["missing_datasets"]:
            for m in summary["missing_datasets"]:
                logger.warning(f"DATASET ANOMALY DETECTED: {m['dataset']} - Status: {m['status']} at {m['s3_path']} (Reason: {m['reason']})")

        # Determine subject line & send email if configured
        received_ds = summary["total_received_datasets"]
        total_ds = summary["total_expected_datasets"]
        
        if summary["overall_status"] == "SUCCESS":
            subject = f"Healthcare File Monitoring - SUCCESS ({received_ds}/{total_ds} Files Received)"
        else:
            subject = f"Healthcare File Monitoring - ALERT ({received_ds}/{total_ds} Files Received)"

        if should_send_email:
            html_report = generate_html_report(summary)
            send_email(subject, html_report)
            logger.info(f"Notification email dispatched. Result: {summary['overall_status']}")
        else:
            logger.info(f"Monitoring result is SUCCESS and SEND_SUCCESS_EMAIL is False. Email notification skipped.")

        logger.info(f"Daily File Monitoring pipeline completed successfully. Overall Result: {summary['overall_status']}")
        
        return summary

    except Exception as e:
        error_details = str(e)
        logger.error(f"FATAL PIPELINE EXCEPTION: {error_details}")
        generate_error_email(error_details)
        raise RuntimeError(f"Monitoring pipeline failed: {error_details}") from e


# Execute main function
monitoring_summary = main()