from __future__ import annotations

from collections.abc import Callable
from functools import cached_property
from typing import Any

from airflow.providers.common.compat.notifier import BaseNotifier
from airflow.providers.oqullus.hooks.teams_webhook import TeamsWebhookHook


def _render_template_value(value: Any, context: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return value.format(**context)
    if isinstance(value, list):
        return [_render_template_value(item, context) for item in value]
    if isinstance(value, dict):
        return {key: _render_template_value(item, context) for key, item in value.items()}
    return value


class TeamsNotifier(BaseNotifier):
    """
    Teams BaseNotifier.

    :param teams_conn_id: Airflow connection ID storing Teams webhook URL secret.
    :param text: Plain text message for MessageCard payloads.
    :param title: MessageCard title.
    :param summary: MessageCard summary.
    :param theme_color: MessageCard theme color hex string.
    :param adaptive_card: Adaptive Card body JSON (without attachments envelope).
    :param proxy: Optional HTTPS proxy.
    """

    template_fields = (
        "teams_conn_id",
        "text",
        "title",
        "summary",
        "theme_color",
        "adaptive_card",
    )

    def __init__(
        self,
        teams_conn_id: str = "teams_default",
        text: str = "",
        title: str | None = None,
        summary: str | None = None,
        theme_color: str | None = "0078D4",
        adaptive_card: dict[str, Any] | None = None,
        proxy: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.teams_conn_id = teams_conn_id
        self.text = text
        self.title = title
        self.summary = summary
        self.theme_color = theme_color
        self.adaptive_card = adaptive_card
        self.proxy = proxy

    @cached_property
    def hook(self) -> TeamsWebhookHook:
        """Teams webhook hook."""
        return TeamsWebhookHook(
            http_conn_id=self.teams_conn_id,
            message=self.text,
            title=self.title,
            summary=self.summary,
            theme_color=self.theme_color,
            adaptive_card=self.adaptive_card,
            proxy=self.proxy,
        )

    def notify(self, context: dict[str, Any]) -> None:
        """Send a message/card to a Teams channel."""
        self.hook.http_conn_id = self.teams_conn_id
        self.hook.message = self.text
        self.hook.title = self.title
        self.hook.summary = self.summary
        self.hook.theme_color = self.theme_color
        self.hook.adaptive_card = self.adaptive_card
        self.hook.proxy = self.proxy
        self.hook.execute()


def send_teams_notification(**kwargs: Any) -> TeamsNotifier:
    """Build a Teams notifier for DAG/task callbacks."""
    return TeamsNotifier(**kwargs)


def send_teams_failure_notification(
    *,
    teams_conn_id: str = "teams_default",
    **kwargs: Any,
) -> TeamsNotifier:
    """Build a Teams failure notifier using the default failure Adaptive Card template."""
    return send_teams_notification(
        teams_conn_id=teams_conn_id,
        adaptive_card=build_failure_adaptive_card_template(),
        **kwargs,
    )


def send_teams_retry_notification(
    *,
    teams_conn_id: str = "teams_default",
    **kwargs: Any,
) -> TeamsNotifier:
    """Build a Teams retry notifier using the default retry Adaptive Card template."""
    return send_teams_notification(
        teams_conn_id=teams_conn_id,
        adaptive_card=build_retry_adaptive_card_template(),
        **kwargs,
    )


def send_teams_success_notification(
    *,
    teams_conn_id: str = "teams_default",
    **kwargs: Any,
) -> TeamsNotifier:
    """Build a Teams success notifier using the default success Adaptive Card template."""
    return send_teams_notification(
        teams_conn_id=teams_conn_id,
        adaptive_card=build_success_adaptive_card_template(),
        **kwargs,
    )


def send_teams_webhook_notification(
    *,
    teams_conn_id: str = "teams_default",
    message: str,
    title: str | None = None,
    summary: str | None = None,
    theme_color: str | None = "0078D4",
) -> Callable[[dict[str, Any]], None]:
    """
    Build an Airflow callback that sends a Teams notification through a webhook URL.

    Message/title/summary support Python ``str.format`` against Airflow callback context.
    """

    def _notify(context: dict[str, Any]) -> None:
        rendered_message = message.format(**context)
        rendered_title = title.format(**context) if title else None
        rendered_summary = summary.format(**context) if summary else None

        send_teams_notification(
            teams_conn_id=teams_conn_id,
            text=rendered_message,
            title=rendered_title,
            summary=rendered_summary,
            theme_color=theme_color,
        ).notify(context)

    return _notify


def send_teams_adaptive_card_notification(
    *,
    teams_conn_id: str = "teams_default",
    card_template: dict[str, Any],
) -> Callable[[dict[str, Any]], None]:
    """Build an Airflow callback that sends an Adaptive Card to Teams."""

    def _notify(context: dict[str, Any]) -> None:
        rendered_card = _render_template_value(card_template, context)
        send_teams_notification(
            teams_conn_id=teams_conn_id,
            adaptive_card=rendered_card,
        ).notify(context)

    return _notify


def build_failure_adaptive_card_template() -> dict[str, Any]:
    """Default Adaptive Card template for task/DAG failure notifications."""
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.2",
        "body": [
            {
                "type": "TextBlock",
                "text": "Airflow Task Failed",
                "weight": "Bolder",
                "size": "Large",
                "color": "Attention",
            },
            {
                "type": "FactSet",
                "facts": [
                    {"title": "DAG", "value": "{dag.dag_id}"},
                    {"title": "Task", "value": "{task_instance.task_id}"},
                    {"title": "Run", "value": "{run_id}"},
                    {"title": "Try", "value": "{task_instance.try_number}"},
                ],
            },
            {
                "type": "TextBlock",
                "text": "[View Logs]({task_instance.log_url})",
                "wrap": True,
                "color": "Warning",
            },
        ],
    }


def build_success_adaptive_card_template() -> dict[str, Any]:
    """Default Adaptive Card template for task/DAG success notifications."""
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.2",
        "body": [
            {
                "type": "TextBlock",
                "text": "Airflow Task Succeeded",
                "weight": "Bolder",
                "size": "Large",
                "color": "Good",
            },
            {
                "type": "FactSet",
                "facts": [
                    {"title": "DAG", "value": "{dag.dag_id}"},
                    {"title": "Task", "value": "{task_instance.task_id}"},
                    {"title": "Run", "value": "{run_id}"},
                ],
            },
            {
                "type": "TextBlock",
                "text": "[View Logs]({task_instance.log_url})",
                "wrap": True,
                "color": "Good",
            },
        ],
    }


def build_retry_adaptive_card_template() -> dict[str, Any]:
    """Default Adaptive Card template for task retry notifications."""
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.2",
        "body": [
            {
                "type": "TextBlock",
                "text": "Airflow Task Retrying",
                "weight": "Bolder",
                "size": "Large",
                "color": "Warning",
            },
            {
                "type": "FactSet",
                "facts": [
                    {"title": "DAG", "value": "{dag.dag_id}"},
                    {"title": "Task", "value": "{task_instance.task_id}"},
                    {"title": "Run", "value": "{run_id}"},
                    {"title": "Try", "value": "{task_instance.try_number}"},
                    {"title": "Max Retries", "value": "{task_instance.max_tries}"},
                ],
            },
            {
                "type": "TextBlock",
                "text": "[View Logs]({task_instance.log_url})",
                "wrap": True,
                "color": "Warning",
            },
        ],
    }
