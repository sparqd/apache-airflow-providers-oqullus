from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.oqullus.notifications.multi import send_multi_channel_notification

TEAMS_CONN_ID = "teams_default"
SMTP_CONN_ID = "smtp_default"

NOTIFICATION_CHANNELS = [
    {"channel": "teams", "teams_conn_id": TEAMS_CONN_ID},
    {"channel": "teams", "teams_conn_id": "teams_secondary"},
    {
        "channel": "email",
        "smtp_conn_id": SMTP_CONN_ID,
        "to": ["ops@company.com"],
    },
]

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
    on_failure_callback=send_multi_channel_notification(
        event="failure",
        notifications=NOTIFICATION_CHANNELS,
    ),
) as dag:
    run_notebook = SparkKubernetesOperator(
        task_id="run_notebook",
        namespace="spark-team-a",
        template_spec=SPARK_TEMPLATE_SPEC,
        get_logs=True,
        delete_on_termination=False,
        retries=2,
        retry_delay=timedelta(minutes=1),
        on_execute_callback=send_multi_channel_notification(
            event="execute",
            notifications=NOTIFICATION_CHANNELS,
        ),
        on_retry_callback=send_multi_channel_notification(
            event="retry",
            notifications=NOTIFICATION_CHANNELS,
        ),
        on_success_callback=send_multi_channel_notification(
            event="success",
            notifications=NOTIFICATION_CHANNELS,
        ),
    )

    run_notebook
