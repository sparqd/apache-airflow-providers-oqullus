from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from airflow.providers.oqullus.notifications.email import (
    send_oqullus_execute_email_notification,
    send_oqullus_failure_email_notification,
    send_oqullus_retry_email_notification,
    send_oqullus_success_email_notification,
)
from airflow.providers.oqullus.notifications.teams import (
    send_teams_execute_notification,
    send_teams_failure_notification,
    send_teams_retry_notification,
    send_teams_success_notification,
)

log = logging.getLogger(__name__)

SUPPORTED_EVENTS = {"execute", "retry", "success", "failure"}


def _get_channel(config: dict[str, Any]) -> str:
    channel = str(config.get("channel") or config.get("type") or "").strip().lower()
    if channel not in {"teams", "email"}:
        raise ValueError(
            f"Unsupported notification channel '{channel}'. Use one of: teams, email."
        )
    return channel


def _build_channel_kwargs(config: dict[str, Any]) -> dict[str, Any]:
    kwargs = dict(config)
    for key in ("channel", "type", "enabled"):
        kwargs.pop(key, None)
    return kwargs


def _dispatch_channel_notification(
    *,
    context: dict[str, Any],
    event: str,
    config: dict[str, Any],
) -> None:
    channel = _get_channel(config)
    kwargs = _build_channel_kwargs(config)

    if channel == "teams":
        teams_dispatch = {
            "execute": send_teams_execute_notification,
            "retry": send_teams_retry_notification,
            "success": send_teams_success_notification,
            "failure": send_teams_failure_notification,
        }
        teams_dispatch[event](context=context, **kwargs)
        return

    email_dispatch = {
        "execute": send_oqullus_execute_email_notification,
        "retry": send_oqullus_retry_email_notification,
        "success": send_oqullus_success_email_notification,
        "failure": send_oqullus_failure_email_notification,
    }
    email_dispatch[event](context=context, **kwargs)


def send_multi_channel_notification(
    context: dict[str, Any] | None = None,
    *,
    event: str,
    notifications: list[dict[str, Any]],
    continue_on_error: bool = True,
) -> Callable[[dict[str, Any]], None] | None:
    """
    Send/build a callback that dispatches multiple notifications from JSON-like configs.

    :param event: One of ``execute``, ``retry``, ``success``, ``failure``.
    :param notifications: List of channel configs. Example:
      ``[{\"channel\": \"teams\", \"teams_conn_id\": \"teams_a\"},``
      ``{\"channel\": \"email\", \"smtp_conn_id\": \"smtp_default\", \"to\": [\"ops@x.com\"]}]``
    :param continue_on_error: If True, logs channel errors and continues.
    """
    event_normalized = event.strip().lower()
    if event_normalized not in SUPPORTED_EVENTS:
        raise ValueError(
            f"Unsupported event '{event}'. Use one of: execute, retry, success, failure."
        )

    def _notify(ctx: dict[str, Any]) -> None:
        for index, config in enumerate(notifications):
            if not config.get("enabled", True):
                continue
            try:
                _dispatch_channel_notification(
                    context=ctx,
                    event=event_normalized,
                    config=config,
                )
            except Exception:
                if continue_on_error:
                    log.exception(
                        "Failed to send notification for event=%s at config index=%s",
                        event_normalized,
                        index,
                    )
                    continue
                raise

    if context is not None:
        _notify(context)
        return None
    return _notify
