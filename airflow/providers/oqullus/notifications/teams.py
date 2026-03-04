from __future__ import annotations

from collections.abc import Callable
from functools import cached_property
from typing import Any

from airflow.providers.common.compat.notifier import BaseNotifier
from airflow.providers.oqullus.hooks.teams_webhook import TeamsWebhookHook
from airflow.providers.oqullus.notifications.utils import with_oqullus_context


def _render_template_value(value: Any, context: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return value.format_map(context)
    if isinstance(value, list):
        return [_render_template_value(item, context) for item in value]
    if isinstance(value, dict):
        return {key: _render_template_value(item, context) for key, item in value.items()}
    return value


class TeamsNotifier(BaseNotifier):
    """
    Teams BaseNotifier.

    :param teams_conn_id: Oqullus Workflow connection ID storing Teams webhook URL secret.
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
    """Build a Teams notifier for workflow/task callbacks."""
    return TeamsNotifier(**kwargs)


def send_teams_failure_notification(
    context: dict[str, Any] | None = None,
    *,
    teams_conn_id: str = "teams_default",
    **kwargs: Any,
) -> Callable[[dict[str, Any]], None] | None:
    """
    Send or build a Teams failure callback using the default failure Adaptive Card.

    Supports both usages:
    - `on_failure_callback=send_teams_failure_notification(teams_conn_id="...")`
    - `on_failure_callback=partial(send_teams_failure_notification, teams_conn_id="...")`
    """

    def _notify(ctx: dict[str, Any]) -> None:
        rendered_context = with_oqullus_context(ctx)
        rendered_card = _render_template_value(
            build_failure_adaptive_card_template(), rendered_context
        )
        send_teams_notification(
            teams_conn_id=teams_conn_id,
            adaptive_card=rendered_card,
            **kwargs,
        ).notify(rendered_context)

    if context is not None:
        _notify(context)
        return None
    return _notify


def send_teams_retry_notification(
    context: dict[str, Any] | None = None,
    *,
    teams_conn_id: str = "teams_default",
    **kwargs: Any,
) -> Callable[[dict[str, Any]], None] | None:
    """
    Send or build a Teams retry callback using the default retry Adaptive Card.

    Supports both usages:
    - `on_retry_callback=send_teams_retry_notification(teams_conn_id="...")`
    - `on_retry_callback=partial(send_teams_retry_notification, teams_conn_id="...")`
    """

    def _notify(ctx: dict[str, Any]) -> None:
        rendered_context = with_oqullus_context(ctx)
        rendered_card = _render_template_value(build_retry_adaptive_card_template(), rendered_context)
        send_teams_notification(
            teams_conn_id=teams_conn_id,
            adaptive_card=rendered_card,
            **kwargs,
        ).notify(rendered_context)

    if context is not None:
        _notify(context)
        return None
    return _notify


def send_teams_success_notification(
    context: dict[str, Any] | None = None,
    *,
    teams_conn_id: str = "teams_default",
    **kwargs: Any,
) -> Callable[[dict[str, Any]], None] | None:
    """
    Send or build a Teams success callback using the default success Adaptive Card.

    Supports both usages:
    - `on_success_callback=send_teams_success_notification(teams_conn_id="...")`
    - `on_success_callback=partial(send_teams_success_notification, teams_conn_id="...")`
    """

    def _notify(ctx: dict[str, Any]) -> None:
        rendered_context = with_oqullus_context(ctx)
        rendered_card = _render_template_value(
            build_success_adaptive_card_template(), rendered_context
        )
        send_teams_notification(
            teams_conn_id=teams_conn_id,
            adaptive_card=rendered_card,
            **kwargs,
        ).notify(rendered_context)

    if context is not None:
        _notify(context)
        return None
    return _notify


def send_teams_execute_notification(
    context: dict[str, Any] | None = None,
    *,
    teams_conn_id: str = "teams_default",
    **kwargs: Any,
) -> Callable[[dict[str, Any]], None] | None:
    """
    Send or build a Teams execute callback using the default execute Adaptive Card.

    Supports both usages:
    - `on_execute_callback=send_teams_execute_notification(teams_conn_id="...")`
    - `on_execute_callback=partial(send_teams_execute_notification, teams_conn_id="...")`
    """

    def _notify(ctx: dict[str, Any]) -> None:
        rendered_context = with_oqullus_context(ctx)
        rendered_card = _render_template_value(
            build_execute_adaptive_card_template(), rendered_context
        )
        send_teams_notification(
            teams_conn_id=teams_conn_id,
            adaptive_card=rendered_card,
            **kwargs,
        ).notify(rendered_context)

    if context is not None:
        _notify(context)
        return None
    return _notify


def send_teams_webhook_notification(
    *,
    teams_conn_id: str = "teams_default",
    message: str,
    title: str | None = None,
    summary: str | None = None,
    theme_color: str | None = "0078D4",
) -> Callable[[dict[str, Any]], None]:
    """
    Build an Oqullus Workflow callback that sends a Teams notification.

    Message/title/summary support Python ``str.format`` against callback context.
    """

    def _notify(context: dict[str, Any]) -> None:
        rendered_context = with_oqullus_context(context)
        rendered_message = message.format_map(rendered_context)
        rendered_title = title.format_map(rendered_context) if title else None
        rendered_summary = summary.format_map(rendered_context) if summary else None

        send_teams_notification(
            teams_conn_id=teams_conn_id,
            text=rendered_message,
            title=rendered_title,
            summary=rendered_summary,
            theme_color=theme_color,
        ).notify(rendered_context)

    return _notify


def send_teams_adaptive_card_notification(
    *,
    teams_conn_id: str = "teams_default",
    card_template: dict[str, Any],
) -> Callable[[dict[str, Any]], None]:
    """Build an Oqullus Workflow callback that sends an Adaptive Card to Teams."""

    def _notify(context: dict[str, Any]) -> None:
        rendered_context = with_oqullus_context(context)
        rendered_card = _render_template_value(card_template, rendered_context)
        send_teams_notification(
            teams_conn_id=teams_conn_id,
            adaptive_card=rendered_card,
        ).notify(rendered_context)

    return _notify


def build_failure_adaptive_card_template() -> dict[str, Any]:
    """Default Adaptive Card template for task/workflow failure notifications."""
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.2",
        "body": [
            {
                "type": "TextBlock",
                "text": "Oqullus Workflow Task Failed",
                "weight": "Bolder",
                "size": "Large",
                "color": "Attention",
            },
            {
                "type": "FactSet",
                "facts": [
                    {"title": "Workflow", "value": "{dag.dag_id}"},
                    {"title": "Task", "value": "{task_instance.task_id}"},
                    {"title": "Run", "value": "{run_id}"},
                    {"title": "Try", "value": "{task_instance.try_number}"},
                ],
            },
            {
                "type": "TextBlock",
                "text": "[View Logs]({oqullus_log_url})",
                "wrap": True,
                "color": "Warning",
            },
        ],
    }


def build_execute_adaptive_card_template() -> dict[str, Any]:
    """Default Adaptive Card template for task execution start notifications."""
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.2",
        "body": [
            {
                "type": "TextBlock",
                "text": "Oqullus Workflow Task Started",
                "weight": "Bolder",
                "size": "Large",
                "color": "Accent",
            },
            {
                "type": "FactSet",
                "facts": [
                    {"title": "Workflow", "value": "{dag.dag_id}"},
                    {"title": "Task", "value": "{task_instance.task_id}"},
                    {"title": "Run", "value": "{run_id}"},
                    {"title": "Try", "value": "{task_instance.try_number}"},
                ],
            },
            {
                "type": "TextBlock",
                "text": "[View Logs]({oqullus_log_url})",
                "wrap": True,
                "color": "Accent",
            },
        ],
    }


def build_success_adaptive_card_template() -> dict[str, Any]:
    """Default Adaptive Card template for task/workflow success notifications."""
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.2",
        "body": [
            {
                "type": "TextBlock",
                "text": "Oqullus Workflow Task Succeeded",
                "weight": "Bolder",
                "size": "Large",
                "color": "Good",
            },
            {
                "type": "FactSet",
                "facts": [
                    {"title": "Workflow", "value": "{dag.dag_id}"},
                    {"title": "Task", "value": "{task_instance.task_id}"},
                    {"title": "Run", "value": "{run_id}"},
                ],
            },
            {
                "type": "TextBlock",
                "text": "[View Logs]({oqullus_log_url})",
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
                "text": "Oqullus Workflow Task Retrying",
                "weight": "Bolder",
                "size": "Large",
                "color": "Warning",
            },
            {
                "type": "FactSet",
                "facts": [
                    {"title": "Workflow", "value": "{dag.dag_id}"},
                    {"title": "Task", "value": "{task_instance.task_id}"},
                    {"title": "Run", "value": "{run_id}"},
                    {"title": "Try", "value": "{task_instance.try_number}"},
                    {"title": "Max Retries", "value": "{task_instance.max_tries}"},
                ],
            },
            {
                "type": "TextBlock",
                "text": "[View Logs]({oqullus_log_url})",
                "wrap": True,
                "color": "Warning",
            },
        ],
    }
