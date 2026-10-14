"""Плагин Airflow для оркестрации источников данных sydafa.

Предоставляет SydafaActivateOperator для явного пробуждения (Lazy Start)
и продления TTL СУБД-источников перед выполнением пайплайнов загрузки.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin

logger = logging.getLogger("airflow.plugins.sydafa")


class SydafaActivateOperator(BaseOperator):
    """Оператор явного пробуждения источника данных в sydafa.

    Отправляет POST /sources/{source}/activate?ttl_minutes={ttl_minutes}.
    Гарантирует, что СУБД запущена, порт слушается, и фиксирует честный TTL.
    """

    template_fields = ("source", "ttl_minutes", "sydafa_url")

    def __init__(
        self,
        *,
        source: str,
        ttl_minutes: int = 15,
        sydafa_url: str | None = None,
        timeout_seconds: int = 180,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.source = source
        self.ttl_minutes = ttl_minutes
        self.sydafa_url = sydafa_url or os.environ.get("SYDAFA_URL", "http://sydafa:8000")
        self.timeout_seconds = timeout_seconds

    def execute(self, context: Any) -> dict[str, Any]:
        base_url = self.sydafa_url.rstrip("/")
        query = urllib.parse.urlencode({"ttl_minutes": self.ttl_minutes})
        url = f"{base_url}/sources/{self.source}/activate?{query}"

        self.log.info(
            "Активация источника '%s' в sydafa (%s, ttl=%d мин)...",
            self.source,
            url,
            self.ttl_minutes,
        )

        req = urllib.request.Request(url, method="POST", data=b"")
        req.add_header("Content-Type", "application/json")

        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                status_code = resp.getcode()
                raw_data = resp.read().decode("utf-8")
                payload = json.loads(raw_data)
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Ошибка активации источника '{self.source}' в sydafa (HTTP {exc.code}): {error_body}"
            ) from exc
        except Exception as exc:
            raise RuntimeError(
                f"Не удалось связаться с sydafa по адресу {url}: {exc}"
            ) from exc

        if status_code != 200:
            raise RuntimeError(
                f"Неожиданный код ответа при активации '{self.source}': {status_code}, тело: {payload}"
            )

        state = payload.get("state")
        activated_until = payload.get("activated_until")
        self.log.info(
            "Источник '%s' успешно активирован (state=%s, activated_until=%s)",
            self.source,
            state,
            activated_until,
        )
        return payload


class SydafaPlugin(AirflowPlugin):
    """Регистрация плагина в Airflow."""

    name = "sydafa_plugin"
    operators = [SydafaActivateOperator]
