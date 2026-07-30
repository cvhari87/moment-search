#!/usr/bin/env python3
"""Calibrate CONFIDENCE_THRESHOLD / TEXT_CONFIDENCE_THRESHOLD against the live
index, instead of guessing numbers.

    python benchmark/calibrate_thresholds.py

Reads benchmark/queries.jsonl (labeled TRUE positives) and benchmark/
negative_queries.jsonl (queries with nothing relevant indexed), hits the
admin-only GET /admin/debug/retrieve for each to get Gate 1's raw inputs
(best_visual, best_text — the same numbers gate_citations() in
src/rag/search.py thresholds on), and reports whether a clean separating
threshold exists per branch. Gate 1 rejects only when BOTH branches are
below their threshold (an OR-pass), so a query is a false accept if EITHER
of its raw scores clears that branch's threshold — the report checks both
branches independently for that reason.

If positive and negative distributions overlap (a negative genuinely
outscores a true positive on the SAME branch), no per-branch cosine
threshold can separate them without also rejecting real queries — the
script says so explicitly instead of printing a threshold that would
quietly break a passing case.
"""
from __future__ import annotations

import json
import os
import pathlib
import urllib.error
import urllib.parse
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
BASE = os.getenv("BASE_URL", "http://localhost:8100").rstrip("/")
ADMIN = os.getenv("ADMIN_TOKEN", "")


def _load(name: str) -> list[str]:
    path = ROOT / "benchmark" / name
    out = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line)["query"])
    return out


def _debug_retrieve(q: str) -> dict:
    url = f"{BASE}/admin/debug/retrieve?q={urllib.parse.quote(q)}"
    req = urllib.request.Request(url, method="GET")
    if ADMIN:
        req.add_header("authorization", f"Bearer {ADMIN}")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raise SystemExit(f"GET /admin/debug/retrieve failed ({e.code}): "
                         f"{e.read().decode()}\nSet ADMIN_TOKEN.")


def main():
    positives = _load("queries.jsonl")
    negatives = _load("negative_queries.jsonl")

    pos_scores = [(_q, _debug_retrieve(_q)) for _q in positives]
    neg_scores = [(_q, _debug_retrieve(_q)) for _q in negatives]

    print(f"{'query':<70} {'visual':>8} {'text':>8}")
    print("-- positives " + "-" * 78)
    for q, s in pos_scores:
        print(f"{q[:68]:<70} {s['best_visual']:>8.4f} {s['best_text']:>8.4f}")
    print("-- negatives " + "-" * 78)
    for q, s in neg_scores:
        print(f"{q[:68]:<70} {s['best_visual']:>8.4f} {s['best_text']:>8.4f}")

    for branch in ("best_visual", "best_text"):
        pos_vals = [s[branch] for _, s in pos_scores]
        neg_vals = [s[branch] for _, s in neg_scores]
        pos_min, neg_max = min(pos_vals), max(neg_vals)
        print(f"\n[{branch}] positives min={pos_min:.4f}  negatives max={neg_max:.4f}")
        if neg_max < pos_min:
            print(f"  clean separation on this branch alone -> "
                 f"{(pos_min + neg_max) / 2:.4f}")
        else:
            worst_neg_q = neg_scores[neg_vals.index(neg_max)][0]
            worst_pos_q = pos_scores[pos_vals.index(pos_min)][0]
            print(f"  no separation on this branch alone: negative "
                 f"{worst_neg_q!r} ({neg_max:.4f}) outscores positive "
                 f"{worst_pos_q!r} ({pos_min:.4f}). Not fatal by itself — "
                 f"gate_citations() rejects only when BOTH branches are "
                 f"below threshold, so this branch can still be raised as "
                 f"long as the OTHER branch protects every real positive. "
                 f"See the AND-aware recommendation below.")

    # AND-aware recommendation: gate_citations() rejects iff
    # best_visual < tv AND best_text < tt. A candidate (tv, tt) is valid iff
    # it rejects every negative and accepts every positive under that exact
    # rule — not two independent per-branch checks.
    text_pos = [s["best_text"] for _, s in pos_scores]
    text_neg = [s["best_text"] for _, s in neg_scores]
    vis_neg = [s["best_visual"] for _, s in neg_scores]
    tt = (min(text_pos) + max(text_neg)) / 2
    tv = max(vis_neg) + 0.01  # margin above the noisiest negative's visual score

    def rejects(s):
        return s["best_visual"] < tv and s["best_text"] < tt

    false_rejects = [q for q, s in pos_scores if rejects(s)]
    false_accepts = [q for q, s in neg_scores if not rejects(s)]

    print(f"\n[AND-aware] CONFIDENCE_THRESHOLD={tv:.4f}  "
         f"TEXT_CONFIDENCE_THRESHOLD={tt:.4f}")
    if not false_rejects and not false_accepts:
        print("  verified against the full labeled set: 0 false rejects, "
             "0 false accepts.")
    else:
        if false_rejects:
            print(f"  WOULD FALSE-REJECT real queries: {false_rejects}")
        if false_accepts:
            print(f"  WOULD STILL FALSE-ACCEPT negatives: {false_accepts}")
        print("  Do not ship these numbers as-is — widen the query sets "
             "or add a reranking check instead.")


if __name__ == "__main__":
    main()
