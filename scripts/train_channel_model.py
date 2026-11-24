"""Train the channel-engagement model on synthetic data and write models/channel_lr_v1.json.

The synthetic generator has a known ground truth (a logistic function of the features plus
noise), so held-out AUC measures how well the pipeline recovers it. It says nothing about
real member behaviour.

    python scripts/train_channel_model.py --seed 42 --n 20000
"""

import argparse
import json
import math
import random
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

from notify_flow.channel.policy import CHANNELS, MemberFeatures, feature_vector

FEATURES = [
    "program_week",
    "sessions_last_7d",
    "days_since_last_session",
    "channel_open_rate",
    "hour_sin",
    "hour_cos",
    "is_preferred",
]
TRUE_W = {  # hidden ground truth used only to generate data
    "push": [-0.3, 1.2, -1.5, 2.5, 0.2, 0.6, 0.8],
    "sms": [0.2, 0.8, -0.8, 2.0, -0.4, 0.3, 1.0],
    "email": [0.5, 0.4, -0.4, 2.2, 0.1, -0.5, 0.6],
}
TRUE_B = {"push": -1.6, "sms": -1.4, "email": -2.0}


def synth_member(rng: random.Random) -> MemberFeatures:
    return MemberFeatures(
        program_week=rng.randint(1, 52),
        sessions_last_7d=rng.randint(0, 10),
        days_since_last_session=rng.randint(0, 45),
        open_rate_sms=rng.betavariate(2, 4),
        open_rate_push=rng.betavariate(2, 3),
        open_rate_email=rng.betavariate(1.5, 5),
        local_hour=rng.randint(7, 21),
        preferred_channel=rng.choice([None, *CHANNELS]),
        eligible=CHANNELS,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--n", type=int, default=20000)
    ap.add_argument("--out", default="models/channel_lr_v1.json")
    ap.add_argument("--version", default="channel-lr-v1")
    args = ap.parse_args()
    rng = random.Random(args.seed)

    out = {
        "version": args.version,
        "features": FEATURES,
        "seed": args.seed,
        "n": args.n,
        "channels": {},
        "heldout_auc": {},
    }
    for ch in CHANNELS:
        X, y = [], []
        for _ in range(args.n):
            f = synth_member(rng)
            x = feature_vector(f, ch)
            z = TRUE_B[ch] + sum(a * b for a, b in zip(TRUE_W[ch], x, strict=True))
            p = 1 / (1 + math.exp(-(z + rng.gauss(0, 0.5))))
            X.append(x)
            y.append(1 if rng.random() < p else 0)
        Xa, ya = np.array(X), np.array(y)
        Xtr, Xte, ytr, yte = train_test_split(Xa, ya, test_size=0.25, random_state=args.seed)
        clf = LogisticRegression(max_iter=1000).fit(Xtr, ytr)
        auc = roc_auc_score(yte, clf.predict_proba(Xte)[:, 1])
        out["channels"][ch] = {
            "coef": [round(float(c), 6) for c in clf.coef_[0]],
            "intercept": round(float(clf.intercept_[0]), 6),
        }
        out["heldout_auc"][ch] = round(float(auc), 4)
        print(f"{ch}: held-out AUC={auc:.4f} positives={ya.mean():.3f}")
    Path(args.out).write_text(json.dumps(out, indent=2) + "\n")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
