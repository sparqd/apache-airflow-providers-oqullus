from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlsplit

from airflow.providers.http.hooks.http import HttpHook


class TeamsCommonHandler:
    """Contains common payload and URL handling for Teams webhook calls."""

    def split_webhook_url(self, webhook_url: str) -> tuple[str, str]:
        """Split a full Teams webhook URL into base URL and endpoint."""
        parsed = urlsplit(webhook_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Expected a valid Teams webhook URL.")

        endpoint = parsed.path.lstrip("/")
        if parsed.query:
            endpoint = f"{endpoint}?{parsed.query}"
        if not endpoint:
            raise ValueError("Expected a valid Teams webhook URL path.")

        return f"{parsed.scheme}://{parsed.netloc}", endpoint

    def build_teams_payload(
        self,
        *,
        message: str,
        title: str | None = None,
        summary: str | None = None,
        theme_color: str | None = None,
    ) -> str:
        """Build a MessageCard payload accepted by Teams incoming webhooks."""
        if not message:
            raise ValueError("Teams message cannot be empty.")

        payload: dict[str, Any] = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "summary": summary or title or "Airflow notification",
            "text": message,
        }
        if title:
            payload["title"] = title
        if theme_color:
            payload["themeColor"] = theme_color

        return json.dumps(payload)

    def build_adaptive_card_payload(self, *, card: dict[str, Any]) -> str:
        """Build an Adaptive Card message payload accepted by Teams workflows/webhooks."""
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "contentUrl": None,
                    "content": card,
                }
            ],
        }
        return json.dumps(payload)


class TeamsWebhookHook(HttpHook):
    """Post messages to a Microsoft Teams channel via incoming webhook URL."""

    conn_name_attr = "http_conn_id"
    default_conn_name = "teams_default"
    conn_type = "http"
    hook_name = "Teams"

    def __init__(
        self,
        *,
        webhook_url: str,
        message: str,
        title: str | None = None,
        summary: str | None = None,
        theme_color: str | None = "0078D4",
        adaptive_card: dict[str, Any] | None = None,
        proxy: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(method="POST", **kwargs)
        self.handler = TeamsCommonHandler()
        self.webhook_url = webhook_url
        self.message = message
        self.title = title
        self.summary = summary
        self.theme_color = theme_color
        self.adaptive_card = adaptive_card
        self.proxy = proxy

        self.base_url, self.endpoint = self.handler.split_webhook_url(webhook_url)

    def execute(self) -> None:
        """Execute the Teams webhook call."""
        proxies = {"https": self.proxy} if self.proxy else {}
        if self.adaptive_card:
            teams_payload = self.handler.build_adaptive_card_payload(card=self.adaptive_card)
        else:
            teams_payload = self.handler.build_teams_payload(
                message=self.message,
                title=self.title,
                summary=self.summary,
                theme_color=self.theme_color,
            )

        self.run(
            endpoint=self.endpoint,
            data=teams_payload,
            headers={"Content-type": "application/json"},
            extra_options={"proxies": proxies},
        )
