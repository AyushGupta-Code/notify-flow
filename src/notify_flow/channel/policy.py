"""Channel selection: a logistic-regression engagement model with a rule-based fallback.

The model is stored as plain JSON coefficients (no pickle) and served with numpy, so the
runtime does not depend on scikit-learn. Training lives in scripts/train_channel_model.py.
"""

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from notify_flow.log import get_logger
from notify_flow.metrics import CHANNEL_DECISIONS

log = get_logger(__name__)

CHANNELS = ("push", "sms", "email")
RULE_ORDER = ("push", "sms", "email")


@dataclass(frozen=True)
class MemberFeatures:
    program_week: int
    sessions_last_7d: int
    days_since_last_session: int
    open_rate_sms: float
    open_rate_push: float
    open_rate_email: float
    local_hour: int
    preferred_channel: str | None
    eligible: tuple[str, ...]


def feature_vector(f: MemberFeatures, channel: str) -> list[float]:
    """Must stay in sync with FEATURES in the training script."""
    return [
        min(f.program_week, 52) / 52.0,
        min(f.sessions_last_7d, 14) / 14.0,
        min(f.days_since_last_session, 60) / 60.0,
        {"sms": f.open_rate_sms, "push": f.open_rate_push, "email": f.open_rate_email}[channel],
        math.sin(2 * math.pi * f.local_hour / 24),
        math.cos(2 * math.pi * f.local_hour / 24),
        1.0 if f.preferred_channel == channel else 0.0,
    ]


@dataclass(frozen=True)
class ChannelDecision:
    channel: str
    escalation_channel: str | None
    source: str  # "model" | "rules"
    model_version: str | None
    scores: dict[str, float]


class ChannelModel:
    def __init__(self, version: str, weights: dict[str, dict[str, Any]]):
        self.version = version
        self.weights = weights

    @classmethod
    def load(cls, path: Path) -> "ChannelModel":
        raw = json.loads(path.read_text())
        return cls(raw["version"], raw["channels"])

    def score(self, f: MemberFeatures, channel: str) -> float:
        w = self.weights[channel]
        x = feature_vector(f, channel)
        z = float(w["intercept"]) + sum(a * b for a, b in zip(w["coef"], x, strict=True))
        return 1.0 / (1.0 + math.exp(-z))


def rule_based(f: MemberFeatures) -> ChannelDecision:
    order = list(RULE_ORDER)
    if f.preferred_channel in order:
        order.remove(f.preferred_channel)
        order.insert(0, f.preferred_channel)
    ranked = [c for c in order if c in f.eligible]
    if not ranked:
        raise ValueError("member has no eligible channel")
    return ChannelDecision(ranked[0], ranked[1] if len(ranked) > 1 else None, "rules", None, {})


class ChannelPolicy:
    """Model first; any model problem (missing file, bad features, NaN) falls back to rules."""

    def __init__(self, model: ChannelModel | None):
        self.model = model

    @classmethod
    def from_path(cls, path: Path) -> "ChannelPolicy":
        try:
            return cls(ChannelModel.load(path))
        except (OSError, KeyError, ValueError) as exc:
            log.warning("channel_model.load_failed", error=type(exc).__name__)
            return cls(None)

    def decide(self, f: MemberFeatures) -> ChannelDecision:
        decision: ChannelDecision | None = None
        if self.model is not None and f.eligible:
            try:
                scores = {c: self.model.score(f, c) for c in f.eligible}
                if any(math.isnan(s) for s in scores.values()):
                    raise ValueError("nan score")
                ranked = sorted(scores, key=lambda c: scores[c], reverse=True)
                decision = ChannelDecision(
                    ranked[0],
                    ranked[1] if len(ranked) > 1 else None,
                    "model",
                    self.model.version,
                    {k: round(v, 4) for k, v in scores.items()},
                )
            except (KeyError, ValueError, TypeError, OverflowError) as exc:
                log.warning("channel_model.score_failed", error=type(exc).__name__)
        if decision is None:
            decision = rule_based(f)
        CHANNEL_DECISIONS.labels(
            decision.source, decision.model_version or "none", decision.channel
        ).inc()
        return decision
