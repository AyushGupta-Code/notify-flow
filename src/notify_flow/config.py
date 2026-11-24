"""Runtime configuration. Every setting can be overridden with an NF_* environment variable."""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="NF_", env_file=".env", extra="ignore")

    service_name: str = "notify-flow"
    log_level: str = "INFO"

    database_url: str = "postgresql+asyncpg://postgres@localhost:5433/notify_flow"

    # Event transport: "kafka" or "sqs"
    queue_backend: str = "kafka"
    kafka_bootstrap: str = "localhost:9092"
    kafka_topic: str = "member-events"
    kafka_group: str = "notify-flow-ingest"
    sqs_queue_url: str = ""
    aws_endpoint_url: str | None = None  # LocalStack / moto endpoint
    aws_region: str = "us-east-1"

    # Orchestrator: "temporal" or "stepfunctions"
    orchestrator: str = "temporal"
    temporal_address: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "member-notifications"
    sfn_state_machine_arn: str = ""

    # LLM
    llm_provider: str = "mock"  # mock | ollama | anthropic
    llm_model: str = "mock-copywriter-1"
    llm_base_url: str = "http://localhost:11434"
    anthropic_api_key: str = ""
    llm_timeout_s: float = 4.0
    llm_max_attempts: int = 3
    llm_backoff_base_s: float = 0.2
    breaker_failure_threshold: int = 5
    breaker_reset_s: float = 30.0

    prompt_registry_path: Path = REPO_ROOT / "prompts" / "registry.yaml"
    channel_model_path: Path = REPO_ROOT / "models" / "channel_lr_v1.json"

    # Workflow timing (seconds). Small values are used in tests and load runs.
    engagement_wait_s: int = 48 * 3600
    quiet_hours_start: int = 21  # local hour, inclusive
    quiet_hours_end: int = 8  # local hour, exclusive
    quiet_hours_enabled: bool = True

    metrics_port: int = 9101


@lru_cache
def get_settings() -> Settings:
    return Settings()
