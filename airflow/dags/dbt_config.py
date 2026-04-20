from __future__ import annotations
import os
from datetime import timedelta

# dbt binary + project paths

DBT_EXECUTABLE: str = os.getenv(
    "DBT_EXECUTABLE",
    "/Users/aygbedu/airflow/dags/launchpad_capstone_project/capstone-env/bin/dbt",
)

DBT_PROJECT_DIR: str = os.getenv(
    "DBT_PROJECT_DIR",
    "/Users/aygbedu/airflow/dags/launchpad_capstone_project/supplychain360-unified-data-platform/dbt/supplychain360model",                  # ← update
)

DBT_PROFILES_DIR: str = os.getenv(
    "DBT_PROFILES_DIR",
    "/Users/aygbedu/.dbt",
)

DBT_PROFILE: str = os.getenv("DBT_PROFILE", "supplychain360")
DBT_TARGET: str  = os.getenv("DBT_TARGET",  "dev")

# Optional: extra env vars injected into every BashOperator
# e.g. BigQuery SA key, Snowflake credentials, etc.

DBT_ENV: dict[str, str] = {
    # "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/sa-key.json",
}

# Layer definitions  (executed top → bottom)

DBT_LAYERS: list[dict] = [
    {
        "name":         "seed",
        "select":       None,          # seeds don't use --select
        "command":      "seed",        # override default "run" command
        "run_tests":    False,
        "full_refresh": False,
        "description":  "Load static CSV seed files into the warehouse",
    },
    {
        "name":         "staging",
        "select":       "staging.*",   # all models under models/staging/
        "command":      "run",
        "run_tests":    True,
        "full_refresh": False,
        "description":  "Light renaming and casting of raw source tables",
    },
    {
        "name":         "intermediate",
        "select":       "intermediate.*",
        "command":      "run",
        "run_tests":    False,
        "full_refresh": False,
        "description":  "Business logic joins and aggregations",
    },
    {
        "name":         "marts",
        "select":       "marts.*",
        "command":      "run",
        "run_tests":    True,
        "full_refresh": False,
        "description":  "Fact and dimension models consumed by BI tools",
    },
]

# Airflow connection IDs

AIRBYTE_CONN_ID: str = "airbyte_default"

# One entry per Airbyte connection:
#   key   = logical name  → becomes the Airflow task_id suffix
#   value = Airbyte connection UUID
AIRBYTE_CONNECTIONS: dict[str, str] = {
    "raw_inventory":   "106b2fa7-2beb-4a42-9bcc-33c29f59789f",
    "raw_products":    "bcff0015-3f27-4cf0-a408-ca571ec28e2e",
    "raw_sales":       "0e43c847-f5b7-4a16-bd56-2c04465563e8",
    "raw_stores":      "03f26ac3-981f-4f55-9e39-b4b5a50c1002",
    "raw_suppliers":   "4d418f9a-64a7-44f5-9180-c978d540adef",
    "raw_warehouses":  "d817597c-35f1-4682-9a91-d75ab22ea101"
}

# DAG schedules

INGESTION_SCHEDULE: str   = "0 2 * * *"   # 02:00 UTC daily
TRANSFORMATION_SCHEDULE   = None           # sensor-driven

# Sensor settings

SENSOR_POKE_INTERVAL: float = 60.0     # seconds
SENSOR_TIMEOUT: float       = 7200.0   # 2 hours

# Shared Airflow default_args
DEFAULT_ARGS: dict = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}