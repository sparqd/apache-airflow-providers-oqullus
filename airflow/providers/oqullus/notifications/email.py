from __future__ import annotations

from collections.abc import Callable, Sequence
from functools import cached_property
from typing import Any

from airflow.providers.common.compat.notifier import BaseNotifier
from airflow.providers.oqullus.notifications.utils import with_oqullus_context
from airflow.providers.smtp.hooks.smtp import SmtpHook


class OqullusEmailNotifier(BaseNotifier):
    """
    SMTP email notifier for Oqullus Workflow callbacks.

    :param smtp_conn_id: Airflow SMTP connection id.
    :param to: Email recipient or list of recipients.
    :param subject: Email subject template.
    :param html_content: Email HTML body template.
    :param from_email: Optional sender override.
    :param cc: Optional CC recipient(s).
    :param bcc: Optional BCC recipient(s).
    """

    template_fields = (
        "smtp_conn_id",
        "to",
        "subject",
        "html_content",
        "from_email",
        "cc",
        "bcc",
    )

    def __init__(
        self,
        *,
        to: str | Sequence[str],
        subject: str,
        html_content: str,
        smtp_conn_id: str = "smtp_default",
        from_email: str | None = None,
        cc: str | Sequence[str] | None = None,
        bcc: str | Sequence[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.smtp_conn_id = smtp_conn_id
        self.to = to
        self.subject = subject
        self.html_content = html_content
        self.from_email = from_email
        self.cc = cc
        self.bcc = bcc

    @cached_property
    def hook(self) -> SmtpHook:
        """SMTP hook."""
        return SmtpHook(smtp_conn_id=self.smtp_conn_id)

    def notify(self, context: dict[str, Any]) -> None:
        """Send email notification."""
        rendered_context = with_oqullus_context(context)
        rendered_subject = self.subject.format(**rendered_context)
        rendered_html = self.html_content.format(**rendered_context)
        self.hook.send_email_smtp(
            to=self.to,
            subject=rendered_subject,
            html_content=rendered_html,
            from_email=self.from_email,
            cc=self.cc,
            bcc=self.bcc,
        )


def send_oqullus_email_notification(**kwargs: Any) -> OqullusEmailNotifier:
    """Build SMTP email notifier for Oqullus Workflow callbacks."""
    return OqullusEmailNotifier(**kwargs)


def build_failure_email_templates() -> tuple[str, str]:
    subject = "[Oqullus Workflow] Failed: {dag.dag_id} / {task_instance.task_id}"
    html = (
        "<p><strong>Oqullus Workflow Task Failed</strong></p>"
        "<ul>"
        "<li><strong>Workflow:</strong> {dag.dag_id}</li>"
        "<li><strong>Task:</strong> {task_instance.task_id}</li>"
        "<li><strong>Run:</strong> {run_id}</li>"
        "<li><strong>Try:</strong> {task_instance.try_number}</li>"
        "</ul>"
        '<p><a href="{oqullus_log_url}">View Logs</a></p>'
    )
    return subject, html


def build_retry_email_templates() -> tuple[str, str]:
    subject = "[Oqullus Workflow] Retrying: {dag.dag_id} / {task_instance.task_id}"
    html = (
        "<p><strong>Oqullus Workflow Task Retrying</strong></p>"
        "<ul>"
        "<li><strong>Workflow:</strong> {dag.dag_id}</li>"
        "<li><strong>Task:</strong> {task_instance.task_id}</li>"
        "<li><strong>Run:</strong> {run_id}</li>"
        "<li><strong>Try:</strong> {task_instance.try_number}</li>"
        "<li><strong>Max Retries:</strong> {task_instance.max_tries}</li>"
        "</ul>"
        '<p><a href="{oqullus_log_url}">View Logs</a></p>'
    )
    return subject, html


def build_success_email_templates() -> tuple[str, str]:
    subject = "[Oqullus Workflow] Succeeded: {dag.dag_id} / {task_instance.task_id}"
    html = (
        "<p><strong>Oqullus Workflow Task Succeeded</strong></p>"
        "<ul>"
        "<li><strong>Workflow:</strong> {dag.dag_id}</li>"
        "<li><strong>Task:</strong> {task_instance.task_id}</li>"
        "<li><strong>Run:</strong> {run_id}</li>"
        "</ul>"
        '<p><a href="{oqullus_log_url}">View Logs</a></p>'
    )
    return subject, html


def build_execute_email_templates() -> tuple[str, str]:
    subject = "[Oqullus Workflow] Started: {dag.dag_id} / {task_instance.task_id}"
    html = (
        "<p><strong>Oqullus Workflow Task Started</strong></p>"
        "<ul>"
        "<li><strong>Workflow:</strong> {dag.dag_id}</li>"
        "<li><strong>Task:</strong> {task_instance.task_id}</li>"
        "<li><strong>Run:</strong> {run_id}</li>"
        "<li><strong>Try:</strong> {task_instance.try_number}</li>"
        "</ul>"
        '<p><a href="{oqullus_log_url}">View Logs</a></p>'
    )
    return subject, html


def _send_predefined_email(
    *,
    context: dict[str, Any] | None,
    builder: Callable[[], tuple[str, str]],
    **kwargs: Any,
) -> Callable[[dict[str, Any]], None] | None:
    def _notify(ctx: dict[str, Any]) -> None:
        subject, html_content = builder()
        send_oqullus_email_notification(
            subject=subject,
            html_content=html_content,
            **kwargs,
        ).notify(ctx)

    if context is not None:
        _notify(context)
        return None
    return _notify


def send_oqullus_failure_email_notification(
    context: dict[str, Any] | None = None,
    **kwargs: Any,
) -> Callable[[dict[str, Any]], None] | None:
    """Send or build a failure email callback."""
    return _send_predefined_email(context=context, builder=build_failure_email_templates, **kwargs)


def send_oqullus_retry_email_notification(
    context: dict[str, Any] | None = None,
    **kwargs: Any,
) -> Callable[[dict[str, Any]], None] | None:
    """Send or build a retry email callback."""
    return _send_predefined_email(context=context, builder=build_retry_email_templates, **kwargs)


def send_oqullus_success_email_notification(
    context: dict[str, Any] | None = None,
    **kwargs: Any,
) -> Callable[[dict[str, Any]], None] | None:
    """Send or build a success email callback."""
    return _send_predefined_email(context=context, builder=build_success_email_templates, **kwargs)


def send_oqullus_execute_email_notification(
    context: dict[str, Any] | None = None,
    **kwargs: Any,
) -> Callable[[dict[str, Any]], None] | None:
    """Send or build an execute/start email callback."""
    return _send_predefined_email(context=context, builder=build_execute_email_templates, **kwargs)
