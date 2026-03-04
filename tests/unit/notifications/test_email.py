from __future__ import annotations

from types import SimpleNamespace

import airflow.providers.oqullus.notifications.email as email_notifications


class _FakeSmtpHook:
    instances: list["_FakeSmtpHook"] = []

    def __init__(self, smtp_conn_id: str) -> None:
        self.smtp_conn_id = smtp_conn_id
        self.entered = False
        self.exited = False
        self.sent_calls: list[dict] = []
        _FakeSmtpHook.instances.append(self)

    def __enter__(self) -> "_FakeSmtpHook":
        self.entered = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.exited = True

    def send_email_smtp(self, **kwargs) -> None:
        self.sent_calls.append(kwargs)


def _build_context() -> dict:
    return {
        "dag": SimpleNamespace(dag_id="workflow_a"),
        "task_instance": SimpleNamespace(task_id="task_x", try_number=2, max_tries=4),
        "run_id": "manual__2026-03-04T00:00:00+00:00",
    }


def test_email_notifier_renders_and_sends_with_context_manager(monkeypatch) -> None:
    _FakeSmtpHook.instances = []
    monkeypatch.setattr(email_notifications, "SmtpHook", _FakeSmtpHook)
    monkeypatch.setenv("OQULLUS_UI_BASE_URL", "https://dev.sparq-qd.com")

    notifier = email_notifications.OqullusEmailNotifier(
        smtp_conn_id="smtp_test",
        to=["ops@company.com"],
        subject="Workflow {dag.dag_id} - {task_instance.task_id}",
        html_content='Run {run_id} <a href="{oqullus_log_url}">logs</a>',
    )
    notifier.notify(_build_context())

    hook = _FakeSmtpHook.instances[0]
    assert hook.smtp_conn_id == "smtp_test"
    assert hook.entered is True
    assert hook.exited is True
    assert len(hook.sent_calls) == 1
    send_kwargs = hook.sent_calls[0]
    assert send_kwargs["to"] == ["ops@company.com"]
    assert send_kwargs["subject"] == "Workflow workflow_a - task_x"
    assert "manual__2026-03-04T00:00:00+00:00" in send_kwargs["html_content"]
    assert "https://dev.sparq-qd.com/admin/workflow/workflow_a?" in send_kwargs["html_content"]


def test_send_oqullus_success_email_notification_factory(monkeypatch) -> None:
    _FakeSmtpHook.instances = []
    monkeypatch.setattr(email_notifications, "SmtpHook", _FakeSmtpHook)
    monkeypatch.setenv("OQULLUS_UI_BASE_URL", "https://dev.sparq-qd.com")

    callback = email_notifications.send_oqullus_success_email_notification(
        smtp_conn_id="smtp_default",
        to=["data@company.com"],
    )
    assert callable(callback)

    callback(_build_context())
    hook = _FakeSmtpHook.instances[0]
    assert len(hook.sent_calls) == 1
    send_kwargs = hook.sent_calls[0]
    assert "[Oqullus Workflow] Succeeded: workflow_a / task_x" == send_kwargs["subject"]
    assert "Oqullus Workflow Task Succeeded" in send_kwargs["html_content"]


def test_send_oqullus_success_email_notification_direct_context(monkeypatch) -> None:
    _FakeSmtpHook.instances = []
    monkeypatch.setattr(email_notifications, "SmtpHook", _FakeSmtpHook)
    monkeypatch.setenv("OQULLUS_UI_BASE_URL", "https://dev.sparq-qd.com")

    result = email_notifications.send_oqullus_success_email_notification(
        context=_build_context(),
        smtp_conn_id="smtp_default",
        to=["data@company.com"],
    )
    assert result is None
    assert len(_FakeSmtpHook.instances) == 1
    assert len(_FakeSmtpHook.instances[0].sent_calls) == 1
