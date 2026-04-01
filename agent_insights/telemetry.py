"""Sentry telemetry configuration."""

import os
from importlib.metadata import PackageNotFoundError, version

import sentry_sdk

_INITIALIZED = False


def _sample_rate(var_name: str, default: float | None = None) -> float | None:
    """Read and validate a sample-rate env var in the inclusive range [0, 1]."""
    raw = os.environ.get(var_name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return None
    if 0.0 <= value <= 1.0:
        return value
    return None


def _release() -> str | None:
    override = os.environ.get("SENTRY_RELEASE", "").strip()
    if override:
        return override
    try:
        return version("agent-insights")
    except PackageNotFoundError:
        return None


def _before_send(event, hint):  # noqa: ARG001
    """Strip sensitive request headers before events are sent to Sentry."""
    request = event.get("request")
    if not isinstance(request, dict):
        return event

    headers = request.get("headers")
    if not isinstance(headers, dict):
        return event

    for key in list(headers.keys()):
        if key.lower() in {"authorization", "cookie", "x-api-key"}:
            headers[key] = "[Filtered]"
    return event


def init_sentry(
    component: str, command: str | None = None, enable_flask: bool = False
) -> bool:
    """Initialize Sentry exactly once when SENTRY_DSN is configured."""
    global _INITIALIZED

    dsn = (
        os.environ.get("SENTRY_DSN", "").strip()
        or os.environ.get("AGENT_INSIGHTS_SENTRY_DSN", "").strip()
        or os.environ.get("CLAUDE_RETRO_SENTRY_DSN", "").strip()
    )
    if not dsn:
        return False

    if not _INITIALIZED:
        integrations = []
        if enable_flask:
            from sentry_sdk.integrations.flask import FlaskIntegration

            integrations.append(FlaskIntegration())

        kwargs = {
            "dsn": dsn,
            "integrations": integrations,
            "before_send": _before_send,
            "send_default_pii": False,
            "release": _release(),
        }

        kwargs["environment"] = (
            os.environ.get("SENTRY_ENVIRONMENT", "local").strip() or "local"
        )

        traces_rate = _sample_rate("SENTRY_TRACES_SAMPLE_RATE", default=0.1)
        if traces_rate is not None:
            kwargs["traces_sample_rate"] = traces_rate

        profiles_rate = _sample_rate("SENTRY_PROFILES_SAMPLE_RATE")
        if profiles_rate is not None:
            kwargs["profiles_sample_rate"] = profiles_rate

        sentry_sdk.init(**kwargs)
        _INITIALIZED = True

    sentry_sdk.set_tag("component", component)
    if command:
        sentry_sdk.set_tag("command", command)
    return True
