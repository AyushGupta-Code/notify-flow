"""Compose message copy: LLM first, versioned template on any failure or guardrail miss.

The LLM never sees a direct identifier. It writes copy with a {first_name} placeholder;
the name is filled in by the send activity, so it is not in the prompt, the model output,
or the workflow history.
"""

import json
import time
from typing import Any

from notify_flow.llm.guardrails import check_copy
from notify_flow.llm.providers import LLMError, LLMProvider
from notify_flow.llm.registry import Registry
from notify_flow.llm.resilience import CircuitBreaker, CircuitOpenError, call_with_retries
from notify_flow.log import get_logger
from notify_flow.metrics import COPY_GENERATED, LLM_LATENCY, LLM_REQUESTS
from notify_flow.types import CopyResult

log = get_logger(__name__)


class Personalizer:
    def __init__(
        self,
        registry: Registry,
        provider: LLMProvider,
        breaker: CircuitBreaker,
        *,
        timeout_s: float = 4.0,
        max_attempts: int = 3,
        backoff_base_s: float = 0.2,
    ):
        self.registry = registry
        self.provider = provider
        self.breaker = breaker
        self.timeout_s = timeout_s
        self.max_attempts = max_attempts
        self.backoff_base_s = backoff_base_s

    def build_prompt(self, campaign: str, channel: str, context: dict[str, Any]) -> str:
        c = self.registry.campaigns[campaign]
        assert c.prompt is not None
        # Whitelist: only the fields this prompt version declares reach the model.
        minimal = {k: context[k] for k in c.prompt.context_fields if k in context}
        minimal.update(
            campaign=campaign, channel=channel, max_chars=self.registry.channel_limits[channel]
        )
        return (
            f"{c.prompt.instructions}\n\n<context>{json.dumps(minimal, sort_keys=True)}</context>\n"
        )

    def _template(
        self, campaign: str, channel: str, reason: str, context: dict[str, Any]
    ) -> CopyResult:
        c = self.registry.campaigns[campaign]
        body = c.templates.get(channel) or c.templates["default"]
        body = body.replace("{session_count}", str(context.get("session_count", "")))
        COPY_GENERATED.labels(source="template", reason=reason).inc()
        return CopyResult(
            body,
            "template",
            reason,
            c.active_prompt,
            None,
            c.template_version,
            self.registry.registry_version,
        )

    async def compose(
        self,
        campaign: str,
        channel: str,
        context: dict[str, Any],
        member_values: list[str] | None = None,
    ) -> CopyResult:
        c = self.registry.campaigns[campaign]
        if not c.llm_allowed or c.prompt is None:
            return self._template(campaign, channel, "policy", context)

        prompt = self.build_prompt(campaign, channel, context)
        started = time.perf_counter()
        try:
            text = await call_with_retries(
                lambda: self.provider.generate(prompt, max_tokens=200),
                breaker=self.breaker,
                timeout_s=self.timeout_s,
                max_attempts=self.max_attempts,
                backoff_base_s=self.backoff_base_s,
                retry_on=(LLMError, TimeoutError),
            )
        except CircuitOpenError:
            LLM_REQUESTS.labels(self.provider.name, self.provider.model, "circuit_open").inc()
            log.warning("llm.circuit_open", campaign=campaign)
            return self._template(campaign, channel, "circuit_open", context)
        except (LLMError, TimeoutError) as exc:
            LLM_REQUESTS.labels(self.provider.name, self.provider.model, "error").inc()
            log.warning("llm.failed", campaign=campaign, error=type(exc).__name__)
            return self._template(campaign, channel, "llm_error", context)
        finally:
            LLM_LATENCY.labels(self.provider.name).observe(time.perf_counter() - started)

        check = check_copy(
            text,
            channel=channel,
            limits=self.registry.channel_limits,
            banned=self.registry.banned_phrases,
            member_values=member_values,
        )
        if not check.passed:
            LLM_REQUESTS.labels(self.provider.name, self.provider.model, "guardrail").inc()
            log.warning("llm.guardrail_rejected", campaign=campaign, failures=check.failures)
            return self._template(campaign, channel, "guardrail", context)

        LLM_REQUESTS.labels(self.provider.name, self.provider.model, "ok").inc()
        COPY_GENERATED.labels(source="llm", reason="ok").inc()
        return CopyResult(
            text,
            "llm",
            "ok",
            c.active_prompt,
            self.provider.model,
            c.template_version,
            self.registry.registry_version,
        )
