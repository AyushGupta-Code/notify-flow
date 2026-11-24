"""Offline eval for message copy.

    python -m evals.run_eval --provider mock --min-pass-rate 0.95 --out eval-report.json

Part 1 generates copy for every case x channel through the real Personalizer (prompt
registry, whitelist, guardrail, fallback) and scores the *model output* against the rubric.
Part 2 checks the checker on known-bad and known-good outputs (recall / false positives).
Exits non-zero if either falls below its threshold, so CI blocks regressions.
"""

import argparse
import asyncio
import json
import sys
from collections import Counter
from pathlib import Path

import yaml

from notify_flow.config import REPO_ROOT, get_settings
from notify_flow.llm.guardrails import check_copy
from notify_flow.llm.personalize import Personalizer
from notify_flow.llm.providers import build_provider
from notify_flow.llm.registry import load_registry
from notify_flow.llm.resilience import CircuitBreaker

HERE = Path(__file__).parent
CHANNELS = ("sms", "push", "email")


class Recorder:
    """Wraps a provider and keeps the raw output so we score the model, not the fallback."""

    def __init__(self, inner):
        self.inner, self.name, self.model = inner, inner.name, inner.model
        self.last: str | None = None

    async def generate(self, prompt, max_tokens=200):
        self.last = None
        self.last = await self.inner.generate(prompt, max_tokens)
        return self.last


async def run(provider_kind: str, model: str) -> dict:
    s = get_settings()
    reg = load_registry(REPO_ROOT / "prompts" / "registry.yaml")
    rec = Recorder(build_provider(provider_kind, model, s.llm_base_url, s.anthropic_api_key))
    pers = Personalizer(
        reg, rec, CircuitBreaker(1000, 1), timeout_s=30, max_attempts=1, backoff_base_s=0
    )
    cases = yaml.safe_load((HERE / "cases.yaml").read_text())
    rows, failures = [], Counter()
    for case in cases:
        for ch in CHANNELS:
            result = await pers.compose(
                case["campaign"], ch, case["context"], member_values=case["member_values"]
            )
            if result.reason == "policy":
                rows.append(
                    {
                        "case": case["id"],
                        "channel": ch,
                        "scored": False,
                        "source": "template",
                        "reason": "policy",
                    }
                )
                continue
            raw = rec.last or ""
            chk = check_copy(
                raw,
                channel=ch,
                limits=reg.channel_limits,
                banned=reg.banned_phrases,
                member_values=case["member_values"],
            )
            failures.update(chk.failures)
            rows.append(
                {
                    "case": case["id"],
                    "channel": ch,
                    "scored": True,
                    "passed": chk.passed,
                    "failures": chk.failures,
                    "source": result.source,
                    "reason": result.reason,
                    "prompt_version": result.prompt_version,
                    "output": raw,
                }
            )
    scored = [r for r in rows if r["scored"]]
    passed = sum(r["passed"] for r in scored)

    neg = yaml.safe_load((HERE / "negative_cases.yaml").read_text())
    bad = [n for n in neg if n["expect"]]
    good = [n for n in neg if not n["expect"]]
    caught, false_pos, misses = 0, 0, []
    for n in neg:
        chk = check_copy(
            n["text"],
            channel=n["channel"],
            limits=reg.channel_limits,
            banned=reg.banned_phrases,
            member_values=n.get("member_values"),
        )
        if n["expect"]:
            ok = (not chk.passed) and all(
                any(f.startswith(e) for f in chk.failures) for e in n["expect"]
            )
            caught += ok
            if not ok:
                misses.append({"text": n["text"], "got": chk.failures, "want": n["expect"]})
        elif not chk.passed:
            false_pos += 1
            misses.append({"text": n["text"], "got": chk.failures, "want": []})

    return {
        "provider": provider_kind,
        "model": rec.model,
        "registry_version": reg.registry_version,
        "generation": {
            "scored": len(scored),
            "passed": passed,
            "pass_rate": round(passed / len(scored), 4) if scored else None,
            "policy_template_only": len(rows) - len(scored),
            "failures_by_check": dict(failures),
        },
        "checker": {
            "bad_cases": len(bad),
            "caught": caught,
            "recall": round(caught / len(bad), 4),
            "good_cases": len(good),
            "false_positives": false_pos,
            "misses": misses,
        },
        "rows": rows,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--provider", default="mock")
    ap.add_argument("--model", default="mock-copywriter-1")
    ap.add_argument("--min-pass-rate", type=float, default=0.95)
    ap.add_argument("--min-checker-recall", type=float, default=1.0)
    ap.add_argument("--out", default="eval-report.json")
    a = ap.parse_args()
    report = asyncio.run(run(a.provider, a.model))
    Path(a.out).write_text(json.dumps(report, indent=2) + "\n")
    g, c = report["generation"], report["checker"]
    print(
        f"generation: {g['passed']}/{g['scored']} passed (pass_rate={g['pass_rate']}), "
        f"{g['policy_template_only']} template-only by policy, failures={g['failures_by_check']}"
    )
    print(
        f"checker: caught {c['caught']}/{c['bad_cases']} known-bad (recall={c['recall']}), "
        f"{c['false_positives']}/{c['good_cases']} false positives on known-good"
    )
    ok = (
        (g["pass_rate"] or 0) >= a.min_pass_rate
        and c["recall"] >= a.min_checker_recall
        and c["false_positives"] == 0
    )
    print("EVAL", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
