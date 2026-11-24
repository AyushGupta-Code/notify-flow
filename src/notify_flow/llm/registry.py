"""Versioned prompt and template registry, loaded from prompts/registry.yaml."""

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(frozen=True)
class PromptVersion:
    version: str
    instructions: str
    context_fields: tuple[str, ...]


@dataclass(frozen=True)
class Campaign:
    name: str
    llm_allowed: bool
    active_prompt: str | None
    template_version: str
    templates: dict[str, str]  # channel -> text with {first_name}
    prompts: dict[str, PromptVersion] = field(default_factory=dict)

    @property
    def prompt(self) -> PromptVersion | None:
        return self.prompts.get(self.active_prompt) if self.active_prompt else None


@dataclass(frozen=True)
class Registry:
    registry_version: str
    channel_limits: dict[str, int]
    banned_phrases: tuple[str, ...]
    campaigns: dict[str, Campaign]


def load_registry(path: Path) -> Registry:
    raw = yaml.safe_load(path.read_text())
    campaigns = {}
    for name, c in raw["campaigns"].items():
        prompts = {
            v: PromptVersion(
                version=v,
                instructions=p["instructions"].strip(),
                context_fields=tuple(p["context_fields"]),
            )
            for v, p in (c.get("prompts") or {}).items()
        }
        campaigns[name] = Campaign(
            name=name,
            llm_allowed=bool(c["llm_allowed"]),
            active_prompt=c.get("active_prompt"),
            template_version=c["template_version"],
            templates=dict(c["templates"]),
            prompts=prompts,
        )
    return Registry(
        registry_version=str(raw["registry_version"]),
        channel_limits={k: int(v) for k, v in raw["channel_limits"].items()},
        banned_phrases=tuple(p.lower() for p in raw["banned_phrases"]),
        campaigns=campaigns,
    )
