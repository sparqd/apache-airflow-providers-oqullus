from __future__ import annotations

from typing import Any

from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator


class OqullusSparkKubernetesOperator(SparkKubernetesOperator):
    """
    Spark Kubernetes operator with built-in XCom push for Spark application definition.

    This operator injects PM output notebook path and pushes it to XCom.
    """

    XCOM_OUTPUT_NOTEBOOK_KEY = "output_notebook_path"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def _build_output_notebook_path(self, context: dict[str, Any]) -> str:
        workspace_id = Variable.get("OQULLUS_WORKSPACE_ID", default_var=None)
        if not workspace_id:
            raise ValueError(
                "Missing Oqullus workspace id. Set Airflow Variable 'OQULLUS_WORKSPACE_ID'."
            )
        ti = context["task_instance"]
        dag_id = context["dag"].dag_id
        task_id = getattr(context.get("task"), "task_id", ti.task_id)
        run_id = context["run_id"]
        try_number = ti.try_number
        return (
            f"s3a://qd-platform-{workspace_id}-workspace/output/notebooks/"
            f"{dag_id}/{task_id}/{run_id}/{try_number}.ipynb"
        )

    def _get_spark_application_root(self, application: dict[str, Any]) -> dict[str, Any]:
        if "spark" in application and isinstance(application["spark"], dict):
            return application["spark"]
        return application

    def _set_or_replace_env_var(
        self,
        *,
        env: list[dict[str, Any]],
        name: str,
        value: str,
    ) -> None:
        for env_var in env:
            if env_var.get("name") == name:
                env_var["value"] = value
                env_var.pop("valueFrom", None)
                return
        env.append({"name": name, "value": value})

    def _inject_driver_output_notebook(
        self,
        *,
        application: dict[str, Any],
        output_notebook_path: str,
    ) -> None:
        spark_app = self._get_spark_application_root(application)
        spec = spark_app.setdefault("spec", {})
        driver = spec.setdefault("driver", {})
        env = driver.setdefault("env", [])
        if not isinstance(env, list):
            env = []
            driver["env"] = env
        self._set_or_replace_env_var(
            env=env,
            name="PM_OUTPUT_NOTEBOOK",
            value=output_notebook_path,
        )

    def _prepare_template_spec(self, context: dict[str, Any], output_notebook_path: str) -> None:
        template_spec = getattr(self, "template_spec", None)
        if not isinstance(template_spec, dict):
            return
        self._inject_driver_output_notebook(
            application=template_spec,
            output_notebook_path=output_notebook_path,
        )

    def execute(self, context: dict[str, Any]) -> Any:
        output_notebook_path = self._build_output_notebook_path(context)
        self._prepare_template_spec(context, output_notebook_path)
        context["ti"].xcom_push(key=self.XCOM_OUTPUT_NOTEBOOK_KEY, value=output_notebook_path)
        return super().execute(context)
