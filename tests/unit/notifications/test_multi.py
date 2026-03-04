from __future__ import annotations

from unittest.mock import Mock

import pytest

from airflow.providers.oqullus.notifications.multi import send_multi_channel_notification


def test_send_multi_channel_notification_raises_for_invalid_event() -> None:
    with pytest.raises(ValueError, match="Unsupported event"):
        send_multi_channel_notification(event="unknown", notifications=[])


def test_send_multi_channel_notification_dispatches_teams_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_success = Mock()
    monkeypatch.setattr(
        "airflow.providers.oqullus.notifications.multi.send_teams_success_notification",
        mock_success,
    )

    callback = send_multi_channel_notification(
        event="success",
        notifications=[
            {
                "channel": "teams",
                "teams_conn_id": "teams_a",
                "theme_color": "00FF00",
            }
        ],
    )
    assert callback is not None

    context = {"run_id": "r1"}
    callback(context)

    mock_success.assert_called_once_with(
        context=context,
        teams_conn_id="teams_a",
        theme_color="00FF00",
    )


def test_send_multi_channel_notification_dispatches_email_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_failure = Mock()
    monkeypatch.setattr(
        "airflow.providers.oqullus.notifications.multi.send_oqullus_failure_email_notification",
        mock_failure,
    )

    callback = send_multi_channel_notification(
        event="failure",
        notifications=[
            {
                "channel": "email",
                "smtp_conn_id": "smtp_default",
                "to": ["ops@company.com"],
            }
        ],
    )
    assert callback is not None

    context = {"run_id": "r1"}
    callback(context)

    mock_failure.assert_called_once_with(
        context=context,
        smtp_conn_id="smtp_default",
        to=["ops@company.com"],
    )


def test_send_multi_channel_notification_skips_disabled_config(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_success = Mock()
    monkeypatch.setattr(
        "airflow.providers.oqullus.notifications.multi.send_teams_success_notification",
        mock_success,
    )

    callback = send_multi_channel_notification(
        event="success",
        notifications=[
            {"channel": "teams", "teams_conn_id": "teams_a", "enabled": False},
        ],
    )
    assert callback is not None

    callback({"run_id": "r1"})
    mock_success.assert_not_called()


def test_send_multi_channel_notification_continue_on_error_true(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_teams = Mock(side_effect=RuntimeError("teams failed"))
    mock_email = Mock()
    monkeypatch.setattr(
        "airflow.providers.oqullus.notifications.multi.send_teams_failure_notification",
        mock_teams,
    )
    monkeypatch.setattr(
        "airflow.providers.oqullus.notifications.multi.send_oqullus_failure_email_notification",
        mock_email,
    )

    callback = send_multi_channel_notification(
        event="failure",
        notifications=[
            {"channel": "teams", "teams_conn_id": "teams_a"},
            {"channel": "email", "smtp_conn_id": "smtp_default", "to": ["ops@company.com"]},
        ],
        continue_on_error=True,
    )
    assert callback is not None

    context = {"run_id": "r1"}
    callback(context)

    mock_teams.assert_called_once()
    mock_email.assert_called_once_with(
        context=context,
        smtp_conn_id="smtp_default",
        to=["ops@company.com"],
    )


def test_send_multi_channel_notification_continue_on_error_false_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_teams = Mock(side_effect=RuntimeError("teams failed"))
    monkeypatch.setattr(
        "airflow.providers.oqullus.notifications.multi.send_teams_failure_notification",
        mock_teams,
    )

    callback = send_multi_channel_notification(
        event="failure",
        notifications=[{"channel": "teams", "teams_conn_id": "teams_a"}],
        continue_on_error=False,
    )
    assert callback is not None

    with pytest.raises(RuntimeError, match="teams failed"):
        callback({"run_id": "r1"})
