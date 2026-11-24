import json

from notify_flow.channel.policy import ChannelModel, ChannelPolicy, MemberFeatures
from notify_flow.config import REPO_ROOT

MODEL = REPO_ROOT / "models" / "channel_lr_v1.json"


def feats(**kw):
    base = dict(program_week=4, sessions_last_7d=2, days_since_last_session=5,
                open_rate_sms=0.2, open_rate_push=0.9, open_rate_email=0.1, local_hour=12,
                preferred_channel=None, eligible=("push", "sms", "email"))
    base.update(kw)
    return MemberFeatures(**base)


def test_model_decision_records_version_and_respects_eligibility():
    p = ChannelPolicy.from_path(MODEL)
    d = p.decide(feats())
    assert d.source == "model" and d.model_version == "channel-lr-v1"
    assert d.channel == "push"  # highest push open rate
    d2 = p.decide(feats(eligible=("sms", "email")))
    assert d2.channel in ("sms", "email") and "push" not in d2.scores


def test_missing_model_falls_back_to_rules(tmp_path):
    p = ChannelPolicy.from_path(tmp_path / "nope.json")
    d = p.decide(feats(preferred_channel="email"))
    assert d.source == "rules" and d.model_version is None
    assert d.channel == "email" and d.escalation_channel == "push"


def test_corrupt_weights_fall_back_to_rules(tmp_path):
    bad = json.loads(MODEL.read_text())
    bad["channels"]["push"]["coef"] = [1.0]  # wrong length
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(bad))
    d = ChannelPolicy(ChannelModel.load(path)).decide(feats())
    assert d.source == "rules"


def test_trained_model_metadata():
    raw = json.loads(MODEL.read_text())
    assert raw["version"] == "channel-lr-v1"
    assert all(v > 0.6 for v in raw["heldout_auc"].values())
