from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

from airflow.providers.oqullus.hooks.teams_webhook import TeamsWebhookHook


def _build_conn(*, webhook_url: str) -> SimpleNamespace:
    return SimpleNamespace(
        extra_dejson={"webhook_url": webhook_url},
        password=None,
        host="",
        schema="http",
        port=None,
        login=None,
    )


def test_teams_webhook_parses_base_url_and_endpoint(monkeypatch) -> None:
    webhook_url = (
        "https://default.example.com:443/powerautomate/automations/direct/workflows/abc/"
        "triggers/manual/paths/invoke?api-version=1&sig=token"
    )
    monkeypatch.setattr(
        TeamsWebhookHook,
        "get_connection",
        lambda self, conn_id: _build_conn(webhook_url=webhook_url),
    )

    hook = TeamsWebhookHook(http_conn_id="teams_test", message="hello")

    assert hook._resolved_base_url == "https://default.example.com:443"
    assert hook.base_url == "https://default.example.com:443"
    assert (
        hook.endpoint
        == "powerautomate/automations/direct/workflows/abc/triggers/manual/paths/invoke"
        "?api-version=1&sig=token"
    )


def test_teams_webhook_set_base_url_uses_resolved_webhook_base(monkeypatch) -> None:
    webhook_url = "https://default.example.com:443/powerautomate/invoke?api-version=1&sig=token"
    monkeypatch.setattr(
        TeamsWebhookHook,
        "get_connection",
        lambda self, conn_id: _build_conn(webhook_url=webhook_url),
    )

    hook = TeamsWebhookHook(http_conn_id="teams_test", message="hello")
    # Simulate a fresh URL init path that would call _set_base_url(connection)
    hook.base_url = ""
    hook._base_url_initialized = False
    url = hook.url_from_endpoint("some/endpoint")

    assert url == "https://default.example.com:443/some/endpoint"
    assert hook.base_url == "https://default.example.com:443"
    assert hook._base_url_initialized is True


def test_teams_webhook_execute_calls_run_with_expected_endpoint(monkeypatch) -> None:
    webhook_url = "https://default.example.com:443/powerautomate/invoke?api-version=1&sig=token"
    monkeypatch.setattr(
        TeamsWebhookHook,
        "get_connection",
        lambda self, conn_id: _build_conn(webhook_url=webhook_url),
    )

    hook = TeamsWebhookHook(http_conn_id="teams_test", message="hello")
    run_mock = Mock()
    monkeypatch.setattr(hook, "run", run_mock)

    hook.execute()

    run_mock.assert_called_once()
    kwargs = run_mock.call_args.kwargs
    assert kwargs["endpoint"] == "powerautomate/invoke?api-version=1&sig=token"
    assert kwargs["headers"] == {"Content-type": "application/json"}
