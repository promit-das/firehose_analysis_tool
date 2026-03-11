from __future__ import annotations

import base64
import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import Settings

_TRUSTSTORE_READY = False


class LLMError(RuntimeError):
    """Raised when an LLM request fails."""


def _ensure_truststore() -> None:
    global _TRUSTSTORE_READY
    if _TRUSTSTORE_READY:
        return

    # CIRCUIT environments can rely on OS trust stores for TLS.
    try:
        import truststore  # type: ignore

        truststore.inject_into_ssl()
    except Exception:
        pass
    _TRUSTSTORE_READY = True


def _post_json(url: str, headers: dict[str, str], payload: dict[str, Any]) -> dict[str, Any]:
    _ensure_truststore()
    encoded = json.dumps(payload).encode("utf-8")
    request = Request(url=url, data=encoded, headers=headers, method="POST")
    try:
        with urlopen(request, timeout=120) as response:  # noqa: S310
            body = response.read().decode("utf-8")
            return json.loads(body)
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise LLMError(f"LLM HTTP error {exc.code}: {detail[:1000]}") from exc
    except URLError as exc:
        raise LLMError(f"Failed to reach LLM endpoint: {exc.reason}") from exc


def _fetch_circuit_api_key(settings: Settings) -> str:
    if settings.circuit_api_key:
        return settings.circuit_api_key

    if not (
        settings.circuit_api_client_id
        and settings.circuit_api_client_secret
        and settings.circuit_api_url
    ):
        raise LLMError(
            "CIRCUIT credentials are missing. Set CIRCUIT_API_KEY, or set all of "
            "CIRCUIT_API_CLIENT_ID, CIRCUIT_API_CLIENT_SECRET, and CIRCUIT_API_URL."
        )

    _ensure_truststore()
    credentials = f"{settings.circuit_api_client_id}:{settings.circuit_api_client_secret}"
    basic_token = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
    body = urlencode({"grant_type": "client_credentials"}).encode("utf-8")
    request = Request(
        url=settings.circuit_api_url,
        data=body,
        headers={
            "Accept": "*/*",
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {basic_token}",
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=60) as response:  # noqa: S310
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise LLMError(f"CIRCUIT token HTTP error {exc.code}: {detail[:1000]}") from exc
    except URLError as exc:
        raise LLMError(f"Failed to reach CIRCUIT token endpoint: {exc.reason}") from exc

    token = payload.get("access_token")
    if not token:
        raise LLMError("CIRCUIT token endpoint did not return access_token")
    return str(token)


def _openai_chat(settings: Settings, system_prompt: str, user_prompt: str) -> str:
    if not settings.openai_api_key:
        raise LLMError("OPENAI_API_KEY is required for provider=openai")

    base = settings.llm_base_url or "https://api.openai.com/v1"
    url = f"{base.rstrip('/')}/chat/completions"
    payload = {
        "model": settings.llm_model,
        "temperature": settings.llm_temperature,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    response = _post_json(
        url,
        {
            "Authorization": f"Bearer {settings.openai_api_key}",
            "Content-Type": "application/json",
        },
        payload,
    )

    try:
        return response["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError) as exc:
        raise LLMError("Unexpected OpenAI response format") from exc


def _circuit_chat(settings: Settings, system_prompt: str, user_prompt: str) -> str:
    if not settings.circuit_app_key:
        raise LLMError("CIRCUIT_APP_KEY is required for provider=circuit")
    circuit_api_key = _fetch_circuit_api_key(settings)

    base = settings.llm_base_url or "https://chat-ai.cisco.com/openai"
    url = f"{base.rstrip('/')}/deployments/{settings.llm_model}/chat/completions"
    payload = {
        "user": json.dumps({"appkey": settings.circuit_app_key}),
        "temperature": settings.llm_temperature,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    response = _post_json(
        url,
        {
            "api-key": circuit_api_key,
            "Content-Type": "application/json",
        },
        payload,
    )

    try:
        return response["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError) as exc:
        raise LLMError("Unexpected CIRCUIT response format") from exc


def _azure_openai_chat(settings: Settings, system_prompt: str, user_prompt: str) -> str:
    endpoint = settings.azure_openai_endpoint or settings.llm_base_url
    if not endpoint:
        raise LLMError("AZURE_OPENAI_ENDPOINT or LLM_BASE_URL is required for provider=azure_openai")
    if not settings.azure_openai_api_key:
        raise LLMError("AZURE_OPENAI_API_KEY is required for provider=azure_openai")

    url = (
        f"{endpoint.rstrip('/')}/openai/deployments/{settings.llm_model}/chat/completions"
        "?api-version=2024-10-21"
    )
    payload = {
        "temperature": settings.llm_temperature,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    response = _post_json(
        url,
        {
            "api-key": settings.azure_openai_api_key,
            "Content-Type": "application/json",
        },
        payload,
    )

    try:
        return response["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError) as exc:
        raise LLMError("Unexpected Azure OpenAI response format") from exc


def _anthropic_chat(settings: Settings, system_prompt: str, user_prompt: str) -> str:
    if not settings.anthropic_api_key:
        raise LLMError("ANTHROPIC_API_KEY is required for provider=anthropic")

    base = settings.llm_base_url or "https://api.anthropic.com"
    url = f"{base.rstrip('/')}/v1/messages"
    payload = {
        "model": settings.llm_model,
        "max_tokens": 1800,
        "temperature": settings.llm_temperature,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_prompt}],
    }
    response = _post_json(
        url,
        {
            "x-api-key": settings.anthropic_api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
        payload,
    )

    try:
        content = response["content"]
        if not isinstance(content, list):
            raise TypeError("content field is not a list")
        for item in content:
            if item.get("type") == "text":
                return item.get("text", "").strip()
    except (KeyError, TypeError, AttributeError) as exc:
        raise LLMError("Unexpected Anthropic response format") from exc

    raise LLMError("Anthropic response did not contain text content")


def generate_report_text(settings: Settings, system_prompt: str, user_prompt: str) -> str:
    if settings.llm_provider == "circuit":
        return _circuit_chat(settings, system_prompt, user_prompt)
    if settings.llm_provider == "openai":
        return _openai_chat(settings, system_prompt, user_prompt)
    if settings.llm_provider == "azure_openai":
        return _azure_openai_chat(settings, system_prompt, user_prompt)
    if settings.llm_provider == "anthropic":
        return _anthropic_chat(settings, system_prompt, user_prompt)
    raise LLMError(f"Unsupported provider: {settings.llm_provider}")
