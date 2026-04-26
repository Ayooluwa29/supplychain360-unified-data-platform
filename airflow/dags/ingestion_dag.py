from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
import sys
import os

sys.path.append(os.path.dirname(__file__))

import dbt_config as cfg

with DAG(
    dag_id="ingestion_dag",
    schedule=cfg.INGESTION_SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=cfg.DEFAULT_ARGS,
    tags=["ingestion", "airbyte"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    sync_tasks = [
        AirbyteTriggerSyncOperator(
            task_id=f"sync_{name}",
            airbyte_conn_id=cfg.AIRBYTE_CONN_ID,
            connection_id=conn_uuid,
            asynchronous=False,   # blocks until sync completes
        )
        for name, conn_uuid in cfg.AIRBYTE_CONNECTIONS.items()
    ]

    start >> sync_tasks >> end