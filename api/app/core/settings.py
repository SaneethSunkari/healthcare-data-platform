from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_csv(value: str | None, default: list[str]) -> list[str]:
    if not value:
        return default
    items = [item.strip() for item in value.split(",")]
    cleaned = [item for item in items if item]
    return cleaned or default


@dataclass(frozen=True)
class Settings:
    app_env: str
    api_key: str | None
    require_api_key: bool
    cors_origins: list[str]
    allowed_hosts: list[str]
    query_timeout_ms: int
    connect_timeout_s: int
    max_query_rows: int
    request_id_header: str = "X-Request-ID"

    @property
    def api_key_configured(self) -> bool:
        return bool(self.api_key)


@lru_cache
def get_settings() -> Settings:
    app_env = os.getenv("APP_ENV", "development").strip().lower() or "development"
    api_key = (os.getenv("APP_API_KEY") or "").strip() or None
    require_api_key = _parse_bool(
        os.getenv("REQUIRE_API_KEY"),
        default=app_env == "production",
    )

    settings = Settings(
        app_env=app_env,
        api_key=api_key,
        require_api_key=require_api_key,
        cors_origins=_parse_csv(
            os.getenv("CORS_ALLOW_ORIGINS"),
            default=[
                "http://localhost:3000",
                "http://127.0.0.1:3000",
                "http://localhost:8000",
                "http://127.0.0.1:8000",
            ],
        ),
        allowed_hosts=_parse_csv(
            os.getenv("ALLOWED_HOSTS"),
            default=["localhost", "127.0.0.1", "testserver"],
        ),
        query_timeout_ms=max(1000, int(os.getenv("QUERY_TIMEOUT_MS", "8000"))),
        connect_timeout_s=max(1, int(os.getenv("DB_CONNECT_TIMEOUT_S", "5"))),
        max_query_rows=max(1, int(os.getenv("MAX_QUERY_ROWS", "200"))),
    )

    validate_settings(settings)
    return settings


def validate_settings(settings: Settings) -> None:
    if settings.require_api_key and not settings.api_key_configured:
        raise RuntimeError(
            "APP_API_KEY must be configured when REQUIRE_API_KEY is enabled."
        )

    if settings.app_env == "production" and "*" in settings.cors_origins:
        raise RuntimeError("Wildcard CORS is not allowed in production mode.")

    if settings.app_env == "production" and "*" in settings.allowed_hosts:
        raise RuntimeError("Wildcard ALLOWED_HOSTS is not allowed in production mode.")
