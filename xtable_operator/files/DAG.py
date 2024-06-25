# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from airflow import DAG
from datetime import datetime, timedelta
from xtable_operator.operator import XtableOperator
from pathlib import Path

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "xtableDag", max_active_runs=3, schedule_interval="@once", default_args=default_args
) as dag:
    path = Path("/usr/local/airflow/tmp")
    path.mkdir(exist_ok=True)
    op = XtableOperator(
        task_id="xtableTask",
        tmp_path=path,
        dataset_config={
            "sourceFormat": "DELTA",
            "targetFormats": ["ICEBERG"],
            "datasets": [
                {
                    "tableBasePath": "/example/datalake/table/random_numbers",
                    "tableName": "random_numbers",
                    "namespace": "table",
                }
            ],
        },
    )
