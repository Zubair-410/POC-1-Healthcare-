# ==========================================================
# HEALTHCARE ALERTING NOTEBOOK
# ==========================================================
#
# FEATURES
# ----------------------------------------------------------
# 1. Reads Gold Layer
# 2. Computes Unified Severity Classification (Single Source of Truth)
# 3. Detects Services Requiring Attention by Severity Level
# 4. Generates AI Analysis using Databricks Model Serving
# 5. Produces Professional HTML Report
# 6. Sends Gmail Alert
# ==========================================================

from pyspark.sql.functions import *

import smtplib

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from openai import OpenAI


# ==========================================================
# CONFIGURATION & CONSTANTS
# ==========================================================
SENDER_EMAIL = "zs7919320@gmail.com"

APP_PASSWORD = dbutils.secrets.get(
    scope="healthcare",
    key="smtp_password"
)

RECEIVER_EMAIL = "saimarikanti8@gmail.com"

SEVERITY_COLORS = {
    "CRITICAL": "#ffebee",  # Light Red
    "HIGH": "#fff3e0",      # Light Orange
    "MEDIUM": "#fffde7",    # Light Yellow
    "LOW": "#e3f2fd",       # Light Blue
    "NORMAL": "#ffffff"     # White
}

# ==========================================================
# DATABRICKS MODEL SERVING
# ==========================================================

DATABRICKS_TOKEN = (
    dbutils.notebook.entry_point
    .getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .get()
)

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url="https://dbc-18ea1f06-7772.cloud.databricks.com/serving-endpoints"
)


# ==========================================================
# READ GOLD TABLE (SINGLE SOURCE OF TRUTH)
# ==========================================================

# Read Gold table where severity is pre-computed ONCE in the Gold transformation pipeline
df = spark.read.table(
    "healthcare.gold.operational_efficiency"
)

# Filter services requiring attention (Severity pre-computed in Gold layer, not NORMAL)
services_requiring_attention = df.filter(col("severity") != "NORMAL")

# Count metrics for each severity level from pre-computed Gold layer column
critical_count = df.filter(col("severity") == "CRITICAL").count()
high_count = df.filter(col("severity") == "HIGH").count()
medium_count = df.filter(col("severity") == "MEDIUM").count()
low_count = df.filter(col("severity") == "LOW").count()
total_attention_count = services_requiring_attention.count()


# ==========================================================
# ALERTS EVALUATION
# ==========================================================

if total_attention_count == 0:

    print(
        "No healthcare operational alerts detected."
    )

else:

    rows = services_requiring_attention.collect()

    # ======================================================
    # KPI SUMMARY CARDS
    # ======================================================

    summary_cards = f"""
    <table width="100%" cellpadding="10" style="border-collapse:collapse; font-family:Arial; margin-bottom:20px;">
    <tr>
        <td align="center" style="background:#ffebee; border-radius:8px; padding:15px; width:20%;">
            <h2 style="margin:0; color:#c62828; font-size:24px;">{critical_count}</h2>
            <div style="font-weight:bold; color:#b71c1c; font-size:12px; margin-top:5px;">Critical Services</div>
        </td>
        <td align="center" style="background:#fff3e0; border-radius:8px; padding:15px; width:20%;">
            <h2 style="margin:0; color:#ef6c00; font-size:24px;">{high_count}</h2>
            <div style="font-weight:bold; color:#e65100; font-size:12px; margin-top:5px;">High Priority</div>
        </td>
        <td align="center" style="background:#fffde7; border-radius:8px; padding:15px; width:20%;">
            <h2 style="margin:0; color:#f57f17; font-size:24px;">{medium_count}</h2>
            <div style="font-weight:bold; color:#f57f17; font-size:12px; margin-top:5px;">Medium Priority</div>
        </td>
        <td align="center" style="background:#e3f2fd; border-radius:8px; padding:15px; width:20%;">
            <h2 style="margin:0; color:#1565c0; font-size:24px;">{low_count}</h2>
            <div style="font-weight:bold; color:#0d47a1; font-size:12px; margin-top:5px;">Low Priority</div>
        </td>
        <td align="center" style="background:#f5f5f5; border-radius:8px; padding:15px; width:20%;">
            <h2 style="margin:0; color:#37474f; font-size:24px;">{total_attention_count}</h2>
            <div style="font-weight:bold; color:#263238; font-size:12px; margin-top:5px;">Total Requiring Attention</div>
        </td>
    </tr>
    </table>
    """

    # ======================================================
    # SERVICE TABLE
    # ======================================================

    service_table = """
    <table style="
        border-collapse:collapse;
        width:100%;
        font-family:Arial;
    ">

    <tr style="
        background-color:#1976D2;
        color:white;
    ">

        <th style="padding:10px;border:1px solid #ddd;">
        Service
        </th>

        <th style="padding:10px;border:1px solid #ddd;">
        Severity
        </th>

        <th style="padding:10px;border:1px solid #ddd;">
        Occupancy %
        </th>

        <th style="padding:10px;border:1px solid #ddd;">
        Attendance %
        </th>

        <th style="padding:10px;border:1px solid #ddd;">
        Refusal %
        </th>

        <th style="padding:10px;border:1px solid #ddd;">
        Staff Morale
        </th>

        <th style="padding:10px;border:1px solid #ddd;">
        Avg LOS
        </th>

        <th style="padding:10px;border:1px solid #ddd;">
        Capacity Gap
        </th>

    </tr>
    """

    alert_text = ""

    for row in rows:

        service = row["service"]

        severity = row["severity"]

        occupancy = row["occupancy_pct"]

        attendance = row["attendance_rate"]

        refusal = row["refusal_rate_pct"]

        morale = row["staff_morale"]

        los = row["avg_length_of_stay"]

        capacity_gap = row["capacity_gap"]

        row_color = SEVERITY_COLORS.get(severity, "#ffffff")

        service_table += f"""

        <tr style="background-color:{row_color};">

            <td style="padding:8px;border:1px solid #ddd;font-weight:bold;">
            {service}
            </td>

            <td style="padding:8px;border:1px solid #ddd;font-weight:bold;">
            {severity}
            </td>

            <td style="padding:8px;border:1px solid #ddd;">
            {occupancy}
            </td>

            <td style="padding:8px;border:1px solid #ddd;">
            {attendance}
            </td>

            <td style="padding:8px;border:1px solid #ddd;">
            {refusal}
            </td>

            <td style="padding:8px;border:1px solid #ddd;">
            {morale}
            </td>

            <td style="padding:8px;border:1px solid #ddd;">
            {los}
            </td>

            <td style="padding:8px;border:1px solid #ddd;">
            {capacity_gap}
            </td>

        </tr>

        """

        alert_text += f"""

Service: {service}

Severity: {severity}

Occupancy Percentage: {occupancy}

Attendance Rate: {attendance}

Refusal Rate Percentage: {refusal}

Staff Morale: {morale}

Average Length Of Stay: {los}

Capacity Gap: {capacity_gap}

"""

    service_table += "</table>"


    # ======================================================
    # AI ANALYSIS
    # ======================================================

    response = client.chat.completions.create(

        model="databricks-gpt-oss-120b",

        messages=[

            {
                "role": "system",

                "content": """

You are a senior healthcare operations analyst.

Return ONLY VALID HTML.

Do not return markdown.
Do not return code blocks.
Do not return explanations.

Generate a professional healthcare executive report.

Use inline CSS.

Required Sections:

1. Executive Summary
2. Operational Risks (Differentiate clearly between Critical Risks, High Risks, and Medium/Low Risks based on the assigned Severity column; do not treat all services as critical)
3. Capacity Analysis
4. Staffing Analysis
5. Recommendations

Use:

<h2>
<h3>
<p>
<ul>
<li>
<table>
<tr>
<th>
<td>

Severity Colors:

Critical = red (#c62828)
High = orange (#ef6c00)
Medium = goldenrod (#f57f17)
Low = blue (#1565c0)

The output must be directly embeddable
inside an HTML email.

"""
            },

            {
                "role": "user",

                "content": alert_text
            }

        ],

        max_tokens=2500
    )

    ai_analysis_html = ""

    for item in response.choices[0].message.content:

        if item["type"] == "text":

            ai_analysis_html = item["text"]

            break


    # ======================================================
    # HTML EMAIL
    # ======================================================

    html_body = f"""

    <html>

    <body style="
        font-family:Arial,Helvetica,sans-serif;
        background-color:#f4f6f8;
        padding:20px;
        margin:0;
    ">

    <div style="
        max-width:1200px;
        margin:auto;
        background:white;
        padding:25px;
        border-radius:10px;
        box-shadow:0px 2px 8px rgba(0,0,0,0.15);
    ">

    <div style="
        background:#1976D2;
        color:white;
        padding:25px;
        border-radius:8px;
        text-align:center;
    ">

    <h1 style="margin:0;">
    🏥 Healthcare Operational Alert
    </h1>

    <p style="
        margin-top:10px;
        font-size:14px;
    ">
    Automated Healthcare Monitoring Platform
    </p>

    </div>

    <br>

    {summary_cards}

    <br>

    <h2 style="
        color:#1976D2;
        border-bottom:2px solid #1976D2;
        padding-bottom:5px;
    ">
    Service Severity Metrics
    </h2>

    {service_table}

    <br>

    <h2 style="
        color:#1976D2;
        border-bottom:2px solid #1976D2;
        padding-bottom:5px;
    ">
    AI Executive Analysis
    </h2>

    <div style="
        background:#fafafa;
        border:1px solid #dddddd;
        border-radius:8px;
        padding:20px;
    ">

    {ai_analysis_html}

    </div>

    <br>

    <h2 style="
        color:#1976D2;
        border-bottom:2px solid #1976D2;
        padding-bottom:5px;
    ">
    Monitoring Rules Hierarchy
    </h2>

    <table style="
        border-collapse:collapse;
        width:100%;
        font-family:Arial;
    ">

    <tr style="
        background:#1976D2;
        color:white;
    ">

    <th style="
        padding:10px;
        border:1px solid #ddd;
    ">
    Severity Level
    </th>

    <th style="
        padding:10px;
        border:1px solid #ddd;
    ">
    Threshold / Condition
    </th>

    </tr>

    <tr style="background:#ffebee;">
    <td style="padding:10px;border:1px solid #ddd;font-weight:bold;color:#b71c1c;">
    CRITICAL
    </td>
    <td style="padding:10px;border:1px solid #ddd;">
    Occupancy > 90% OR Capacity Gap > 30
    </td>
    </tr>

    <tr style="background:#fff3e0;">
    <td style="padding:10px;border:1px solid #ddd;font-weight:bold;color:#e65100;">
    HIGH
    </td>
    <td style="padding:10px;border:1px solid #ddd;">
    Refusal Rate > 30%
    </td>
    </tr>

    <tr style="background:#fffde7;">
    <td style="padding:10px;border:1px solid #ddd;font-weight:bold;color:#f57f17;">
    MEDIUM
    </td>
    <td style="padding:10px;border:1px solid #ddd;">
    Attendance Rate < 70%
    </td>
    </tr>

    <tr style="background:#e3f2fd;">
    <td style="padding:10px;border:1px solid #ddd;font-weight:bold;color:#0d47a1;">
    LOW
    </td>
    <td style="padding:10px;border:1px solid #ddd;">
    Staff Morale < 50%
    </td>
    </tr>

    </table>

    <br>

    <hr>

    <p style="
        color:#666666;
        font-size:12px;
        text-align:center;
    ">

    Generated Automatically by Databricks Healthcare Analytics Platform

    <br><br>

    Architecture

    <br>

    S3 → Bronze → Silver → Gold → AI Analysis → Gmail Alert

    </p>

    </div>

    </body>

    </html>

    """


    # ======================================================
    # SEND EMAIL
    # ======================================================

    msg = MIMEMultipart("alternative")

    msg["From"] = SENDER_EMAIL

    msg["To"] = RECEIVER_EMAIL

    msg["Subject"] = (
        f"Healthcare Operational Alert - {critical_count} Critical Services | {total_attention_count} Total Alerts"
    )

    msg.attach(
        MIMEText(
            html_body,
            "html"
        )
    )

    server = smtplib.SMTP(
        "smtp.gmail.com",
        587
    )

    server.starttls()

    server.login(
        SENDER_EMAIL,
        APP_PASSWORD
    )

    server.send_message(msg)

    server.quit()

    print(
        f"Alert email sent successfully. "
        f"{critical_count} critical service(s) and {total_attention_count} total service(s) requiring attention."
    )