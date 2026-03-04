from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote, urlencode


def build_oqullus_log_url(context: dict[str, Any]) -> str:
    """Build Oqullus workflow log URL from callback context."""
    base_url = os.environ.get("OQULLUS_UI_BASE_URL", "").strip().rstrip("/")
    if not base_url:
        raise ValueError("Missing env var OQULLUS_UI_BASE_URL for Oqullus log URL generation.")

    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    query = urlencode({"run_id": run_id, "task_id": task_id, "tab": "log"})
    return f"{base_url}/admin/workflow/{quote(dag_id, safe='')}?{query}"


def with_oqullus_context(context: dict[str, Any]) -> dict[str, Any]:
    """Add Oqullus-specific derived fields to callback context."""
    rendered_context = dict(context)
    rendered_context["oqullus_log_url"] = build_oqullus_log_url(context)
    return rendered_context
