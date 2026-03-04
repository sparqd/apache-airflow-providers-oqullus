from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.oqullus.notifications.teams import (
    send_teams_failure_notification,
    send_teams_retry_notification,
    send_teams_success_notification,
)
from airflow.providers.oqullus.operators.spark_kubernetes import OqullusSparkKubernetesOperator

TEAMS_CONN_ID = "teams_default"

SPARK_TEMPLATE_SPEC = {
    "spark": {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": "sample-oqullus-local",
            "namespace": "spark-team-a",
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": "public.ecr.aws/quantdata/spark:ci-3.5.3-11.1.4-rc1",
            "mainApplicationFile": "local:///opt/app/run_notebook.py",
            "sparkVersion": "3.5.3",
            "restartPolicy": {"type": "Never"},
            "driver": {
                "cores": 1,
                "memory": "2g",
                "serviceAccount": "spark-team-a",
                "env": [
                    {"name": "PM_INPUT_NOTEBOOK", "value": "s3a://bucket/input/notebook.ipynb"},
                ],
            },
            "executor": {
                "instances": 1,
                "cores": 1,
                "memory": "2g",
                "serviceAccount": "spark-team-a",
            },
        },
    }
}

with DAG(
    dag_id="sample_oqullus_local",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    on_failure_callback=send_teams_failure_notification(teams_conn_id=TEAMS_CONN_ID),
) as dag:
    run_notebook = OqullusSparkKubernetesOperator(
        task_id="run_notebook",
        namespace="spark-team-a",
        template_spec=SPARK_TEMPLATE_SPEC,
        get_logs=True,
        delete_on_termination=False,
        retries=2,
        retry_delay=timedelta(minutes=1),
        on_retry_callback=send_teams_retry_notification(teams_conn_id=TEAMS_CONN_ID),
        on_success_callback=send_teams_success_notification(teams_conn_id=TEAMS_CONN_ID),
    )

    run_notebook
