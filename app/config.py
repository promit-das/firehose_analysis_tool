from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


class ConfigError(RuntimeError):
    """Raised when required runtime configuration is invalid."""


def _read_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _as_bool(raw: str | None, default: bool) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _clean_optional(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


@dataclass(frozen=True)
class Settings:
    repo_root: Path
    database_path: Path
    report_output_dir: Path
    extractor_rules_path: Path
    base_prompt_path: Path
    llm_provider: str
    llm_model: str
    llm_base_url: str | None
    llm_temperature: float
    openai_api_key: str | None
    circuit_api_key: str | None
    circuit_app_key: str | None
    circuit_api_client_id: str | None
    circuit_api_client_secret: str | None
    circuit_api_url: str | None
    azure_openai_api_key: str | None
    azure_openai_endpoint: str | None
    anthropic_api_key: str | None
    default_app_id: str | None
    default_tenant_id: str | None
    llm_required_on_startup: bool
    default_delay_breach_ms: int

    @classmethod
    def from_env(cls, repo_root: Path | None = None) -> "Settings":
        root = repo_root or Path(__file__).resolve().parents[1]
        dotenv_values = _read_dotenv(root / ".env")

        def get(name: str, default: str | None = None) -> str | None:
            return os.environ.get(name, dotenv_values.get(name, default))

        llm_temperature_raw = get("LLM_TEMPERATURE", "0")
        try:
            llm_temperature = float(llm_temperature_raw or "0")
        except ValueError as exc:
            raise ConfigError("LLM_TEMPERATURE must be a number") from exc

        database_path = Path(get("DATABASE_PATH", "data/firehose.duckdb") or "data/firehose.duckdb")
        report_output_dir = Path(get("REPORT_OUTPUT_DIR", "output/runs") or "output/runs")

        settings = cls(
            repo_root=root,
            database_path=(root / database_path).resolve(),
            report_output_dir=(root / report_output_dir).resolve(),
            extractor_rules_path=(root / "app" / "extractor_rules.json").resolve(),
            base_prompt_path=(root / "prompts" / "base_prompt.md").resolve(),
            llm_provider=(get("LLM_PROVIDER", "") or "").strip().lower(),
            llm_model=(get("LLM_MODEL", "") or "").strip(),
            llm_base_url=_clean_optional(get("LLM_BASE_URL")),
            llm_temperature=llm_temperature,
            openai_api_key=_clean_optional(get("OPENAI_API_KEY")),
            circuit_api_key=_clean_optional(get("CIRCUIT_API_KEY")),
            circuit_app_key=_clean_optional(get("CIRCUIT_APP_KEY")),
            circuit_api_client_id=_clean_optional(get("CIRCUIT_API_CLIENT_ID")),
            circuit_api_client_secret=_clean_optional(get("CIRCUIT_API_CLIENT_SECRET")),
            circuit_api_url=_clean_optional(get("CIRCUIT_API_URL")),
            azure_openai_api_key=_clean_optional(get("AZURE_OPENAI_API_KEY")),
            azure_openai_endpoint=_clean_optional(get("AZURE_OPENAI_ENDPOINT")),
            anthropic_api_key=_clean_optional(get("ANTHROPIC_API_KEY")),
            default_app_id=_clean_optional(get("DEFAULT_APP_ID")),
            default_tenant_id=_clean_optional(get("DEFAULT_TENANT_ID")),
            llm_required_on_startup=_as_bool(get("LLM_REQUIRED_ON_STARTUP", "1"), default=True),
            default_delay_breach_ms=int(get("DEFAULT_DELAY_BREACH_MS", "30000") or "30000"),
        )

        settings.validate_startup()
        return settings

    def validate_startup(self) -> None:
        if not self.llm_provider:
            raise ConfigError("Missing required config: LLM_PROVIDER")
        if self.llm_provider not in {"circuit", "openai", "azure_openai", "anthropic"}:
            raise ConfigError("LLM_PROVIDER must be one of: circuit, openai, azure_openai, anthropic")
        if not self.llm_model:
            raise ConfigError("Missing required config: LLM_MODEL")

        if self.llm_required_on_startup:
            self.validate_reporting_credentials()

    def validate_reporting_credentials(self) -> None:
        if self.llm_provider == "circuit":
            if not self.circuit_app_key:
                raise ConfigError("Missing required app key for circuit: CIRCUIT_APP_KEY")
            has_static_api_key = bool(self.circuit_api_key)
            has_oauth_config = bool(
                self.circuit_api_client_id and self.circuit_api_client_secret and self.circuit_api_url
            )
            if not has_static_api_key and not has_oauth_config:
                raise ConfigError(
                    "For provider=circuit, set CIRCUIT_API_KEY or "
                    "all of CIRCUIT_API_CLIENT_ID, CIRCUIT_API_CLIENT_SECRET, CIRCUIT_API_URL"
                )
        if self.llm_provider == "openai" and not self.openai_api_key:
            raise ConfigError("Missing required secret for openai: OPENAI_API_KEY")
        if self.llm_provider == "azure_openai":
            if not self.azure_openai_api_key:
                raise ConfigError("Missing required secret for azure_openai: AZURE_OPENAI_API_KEY")
            if not (self.azure_openai_endpoint or self.llm_base_url):
                raise ConfigError(
                    "Missing required endpoint for azure_openai: AZURE_OPENAI_ENDPOINT or LLM_BASE_URL"
                )
        if self.llm_provider == "anthropic" and not self.anthropic_api_key:
            raise ConfigError("Missing required secret for anthropic: ANTHROPIC_API_KEY")
