#!/usr/bin/env python3
"""
Herbal Data Pipeline — Evaluation Harness

Samples source_comments, presents them for manual human labelling,
then compares NLP (GPT-4o-mini) extraction output against the manual
labels and reports per-field precision/recall.

Run before scaling extraction to the full dataset.
"""

from __future__ import annotations

import json
import logging
import os
import random
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

import openai
import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("evaluate")

openai.api_key = config.OPENAI_API_KEY


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ManualLabel:
    """A single manual label for one source_comment."""
    comment_id: str
    comment_body: str
    has_remedy_claim: bool
    remedy: str | None = None
    condition: str | None = None
    method: str | None = None
    dosage: str | None = None
    directionality: str | None = None  # improves|worsens|neutral|unclear
    negation: bool | None = None
    hedging: bool | None = None
    sentiment: str | None = None
    cultural_tag: str | None = None
    notes: str | None = None
    labelled_by: str | None = None
    labelled_at: str | None = None


@dataclass
class Nlpextract:
    """NLP extraction result for one comment (mirrors ManualLabel schema)."""
    comment_id: str
    has_remedy_claim: bool
    remedy: str | None = None
    condition: str | None = None
    method: str | None = None
    dosage: str | None = None
    directionality: str | None = None
    negation: bool | None = None
    hedging: bool | None = None
    sentiment: str | None = None
    cultural_tag: str | None = None


@dataclass
class EvaluationResult:
    """Per-field evaluation result."""
    field: str
    true_positives: int = 0
    false_positives: int = 0
    false_negatives: int = 0
    precision: float = 0.0
    recall: float = 0.0
    f1: float = 0.0

    def compute(self):
        p = self.true_positives / (self.true_positives + self.false_positives)
        r = self.true_positives / (self.true_positives + self.false_negatives)
        self.precision = round(p, 4) if (self.true_positives + self.false_positives) > 0 else 0.0
        self.recall = round(r, 4) if (self.true_positives + self.false_negatives) > 0 else 0.0
        self.f1 = round(2 * p * r / (p + r), 4) if (p + r) > 0 else 0.0


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_db_connection():
    return psycopg2.connect(config.DATABASE_URL)


# ---------------------------------------------------------------------------
# Sample for labelling
# ---------------------------------------------------------------------------

SAMPLE_SIZE = 300
LABEL_FILE = "./state/manual_labels.json"


def sample_comments(n: int = SAMPLE_SIZE) -> list[dict]:
    """
    Randomly sample n source_comments that have at least 50 characters
    (to avoid removed/deleted comment noise).
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, body, platform, external_id
                FROM source_comments
                WHERE LENGTH(body) >= 50
                ORDER BY RANDOM()
                LIMIT %s
                """,
                (n,),
            )
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def load_existing_labels() -> dict[str, ManualLabel]:
    """Load previously saved manual labels from disk."""
    if not os.path.exists(LABEL_FILE):
        return {}
    with open(LABEL_FILE, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    return {item["comment_id"]: ManualLabel(**item) for item in raw}


def save_labels(labels: dict[str, ManualLabel]) -> None:
    """Persist manual labels to disk."""
    os.makedirs(os.path.dirname(LABEL_FILE), exist_ok=True)
    with open(LABEL_FILE, "w", encoding="utf-8") as fh:
        json.dump(
            [asdict(l) for l in labels.values()],
            fh,
            indent=2,
            default=str,
        )


# ---------------------------------------------------------------------------
# Manual labelling interface
# ---------------------------------------------------------------------------

def interactive_label(comment: dict, existing: ManualLabel | None) -> ManualLabel | None:
    """
    Present a comment to the human for labelling via interactive CLI prompt.
    Returns a ManualLabel, or None to skip.
    """
    print("\n" + "=" * 80)
    print(f"Comment ID: {comment['id']}")
    print(f"Platform:   {comment['platform']}")
    print(f"URL:        reddit.com/{comment['external_id']}")
    print("-" * 80)
    print(comment["body"][:1500])
    print("-" * 80)

    if existing:
        print("[EXISTING LABEL — will be overwritten]")
        print(f"  has_remedy_claim: {existing.has_remedy_claim}")
        if existing.has_remedy_claim:
            print(f"  remedy: {existing.remedy}")
            print(f"  condition: {existing.condition}")
            print(f"  directionality: {existing.directionality}")
            print(f"  negation: {existing.negation}, hedging: {existing.hedging}")
            print(f"  sentiment: {existing.sentiment}, cultural_tag: {existing.cultural_tag}")

    has_claim = input("Does this comment contain a natural remedy claim? [y/N/s=skip]: ").strip().lower()
    if has_claim == "s":
        return None
    has_claim_bool = has_claim == "y"

    if not has_claim_bool:
        label = ManualLabel(
            comment_id=comment["id"],
            comment_body=comment["body"],
            has_remedy_claim=False,
            labelled_by=os.getenv("LABEL_USER", "human"),
            labelled_at=datetime.now(timezone.utc).isoformat(),
        )
        return label

    remedy = input("  Remedy name: ").strip() or None
    condition = input("  Condition treated: ").strip() or None

    method_input = input("  Method [tea/tincture/capsule/topical/raw/decoction/poultice/oil/syrup/supplement/other]: ").strip().lower()
    method = method_input if method_input else None

    dosage = input("  Dosage (amount): ").strip() or None

    dir_input = input("  Directionality [improves/worsens/neutral/unclear]: ").strip().lower()
    directionality = dir_input if dir_input in ("improves", "worsens", "neutral", "unclear") else "unclear"

    neg_input = input("  Negation (didn't work)? [y/N]: ").strip().lower()
    negation = neg_input == "y"

    hed_input = input("  Hedging (might/YMMV)? [y/N]: ").strip().lower()
    hedging = hed_input == "y"

    sent_input = input("  Sentiment [positive/negative/mixed]: ").strip().lower()
    sentiment = sent_input if sent_input in ("positive", "negative", "mixed") else None

    cultural = input("  Cultural tag (e.g. Ayurvedic): ").strip() or None

    notes = input("  Notes (optional): ").strip() or None

    return ManualLabel(
        comment_id=comment["id"],
        comment_body=comment["body"],
        has_remedy_claim=True,
        remedy=remedy,
        condition=condition,
        method=method,
        dosage=dosage,
        directionality=directionality,
        negation=negation,
        hedging=hedging,
        sentiment=sentiment,
        cultural_tag=cultural,
        notes=notes,
        labelled_by=os.getenv("LABEL_USER", "human"),
        labelled_at=datetime.now(timezone.utc).isoformat(),
    )


def collect_labels() -> list[ManualLabel]:
    """Collect manual labels via interactive CLI session."""
    comments = sample_comments(SAMPLE_SIZE)
    existing = load_existing_labels()

    # Filter out already-labelled comments
    to_label = [c for c in comments if c["id"] not in existing]
    already_labelled = [c for c in comments if c["id"] in existing]

    log.info(
        "Sampled %d comments. %d already labelled, %d to label.",
        len(comments), len(already_labelled), len(to_label),
    )

    labels: dict[str, ManualLabel] = dict(existing)

    for i, comment in enumerate(to_label, 1):
        print(f"\n[{i}/{len(to_label)}] Remaining: {len(to_label) - i}")
        label = interactive_label(comment, existing.get(comment["id"]))
        if label is not None:
            labels[label.comment_id] = label
            save_labels(labels)
            log.info("Saved label for %s", label.comment_id)

    return list(labels.values())


# ---------------------------------------------------------------------------
# NLP extraction for evaluation
# ---------------------------------------------------------------------------

EXTRACT_PROMPT = """\
Extract natural remedy claim information from these social media comments.

For each comment, output a JSON array. If no remedy claim is present, output [].

Fields per claim:
- remedy: the natural remedy name (exact text from comment)
- condition: the health condition it treats (exact text from comment)
- method: how it was used (tea, tincture, capsule, topical, raw, decoction, poultice, oil, syrup, supplement, other)
- dosage: amount if mentioned, else null
- directionality: "improves" | "worsens" | "neutral" | "unclear"
- negation: true if the comment says the remedy DID NOT work
- hedging: true if the comment uses uncertain language ("might", "seems", "probably", "YMMV")
- sentiment: "positive" | "negative" | "mixed"
- cultural_tag: cultural origin if mentioned, else null

Comments:
{comments}

Output only the JSON array. No explanation.\
"""


def call_llm(prompt: str) -> list[dict]:
    """Call GPT-4o-mini. Returns parsed JSON."""
    response = openai.chat.completions.create(
        model=config.OPENAI_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
        max_tokens=4096,
    )
    raw = response.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    return json.loads(raw)


def extract_batch(comments: list[dict]) -> dict[str, Nlpextract]:
    """
    Run NLP extraction on a batch of comments.
    Returns {comment_id: Nlpextract}.
    """
    lines = []
    for i, c in enumerate(comments):
        lines.append(f"[Comment {i+1}] ID: {c['id']}")
        lines.append(f"Text: {c['body']}")

    prompt = EXTRACT_PROMPT.format(comments="\n".join(lines))

    try:
        raw_results = call_llm(prompt)
    except Exception as exc:
        log.error("LLM call failed: %s", exc)
        return {}

    results: dict[str, Nlpextract] = {}

    for i, c in enumerate(comments):
        if i < len(raw_results):
            r = raw_results[i]
            has_claim = isinstance(r, dict) and r.get("remedy") and r.get("confidence", 0) > 0
            results[c["id"]] = Nlpextract(
                comment_id=c["id"],
                has_remedy_claim=has_claim,
                remedy=r.get("remedy") if isinstance(r, dict) else None,
                condition=r.get("condition") if isinstance(r, dict) else None,
                method=r.get("method") if isinstance(r, dict) else None,
                dosage=r.get("dosage") if isinstance(r, dict) else None,
                directionality=r.get("directionality") if isinstance(r, dict) else None,
                negation=r.get("negation", False) if isinstance(r, dict) else None,
                hedging=r.get("hedging", False) if isinstance(r, dict) else None,
                sentiment=r.get("sentiment") if isinstance(r, dict) else None,
                cultural_tag=r.get("cultural_tag") if isinstance(r, dict) else None,
            )
        else:
            results[c["id"]] = Nlpextract(comment_id=c["id"], has_remedy_claim=False)

    return results


# ---------------------------------------------------------------------------
# Evaluation metrics
# ---------------------------------------------------------------------------

BOOL_FIELDS = ["has_remedy_claim", "negation", "hedging"]
STR_FIELDS = ["remedy", "condition", "method", "directionality", "sentiment", "cultural_tag"]


def evaluate_field(
    manual_labels: list[ManualLabel],
    nlp_results: dict[str, Nlpextract],
    field: str,
) -> EvaluationResult:
    """Compute precision/recall for a single field."""
    result = EvaluationResult(field=field)
    bool_fields = BOOL_FIELDS

    for label in manual_labels:
        nlp = nlp_results.get(label.comment_id)
        if nlp is None:
            continue

        manual_val = getattr(label, field)
        nlp_val = getattr(nlp, field)

        if field in bool_fields:
            manual_bool = bool(manual_val)
            nlp_bool = bool(nlp_val)
            if nlp_bool and manual_bool:
                result.true_positives += 1
            elif nlp_bool and not manual_bool:
                result.false_positives += 1
            elif not nlp_bool and manual_bool:
                result.false_negatives += 1
        else:
            # String field — true positive if both non-None and match
            if nlp_val and manual_val:
                if str(nlp_val).strip().lower() == str(manual_val).strip().lower():
                    result.true_positives += 1
                else:
                    result.false_positives += 1
                    result.false_negatives += 1
            elif nlp_val and not manual_val:
                result.false_positives += 1
            elif not nlp_val and manual_val:
                result.false_negatives += 1

    result.compute()
    return result


def run_evaluation() -> list[EvaluationResult]:
    """Full evaluation pipeline."""
    log.info("Starting evaluation")

    labels = load_existing_labels()
    if not labels:
        log.error("No manual labels found. Run --collect first.")
        sys.exit(1)

    label_list = list(labels.values())
    labelled_comments = [
        {"id": l.comment_id, "body": l.comment_body}
        for l in label_list if l.comment_body
    ]

    # Batch NLP extraction
    batch_size = config.NLP_BATCH_SIZE
    nlp_results: dict[str, Nlpextract] = {}

    for i in range(0, len(labelled_comments), batch_size):
        batch = labelled_comments[i:i + batch_size]
        log.info("Extracting batch %d/%d", i // batch_size + 1,
                 (len(labelled_comments) + batch_size - 1) // batch_size)
        nlp_results.update(extract_batch(batch))
        time.sleep(1.0)

    # Evaluate each field
    all_fields = BOOL_FIELDS + STR_FIELDS
    results: list[EvaluationResult] = []

    for field in all_fields:
        field_result = evaluate_field(label_list, nlp_results, field)
        results.append(field_result)
        log.info(
            "  %-20s  P=%-6s R=%-6s F1=%-6s  TP=%-4d FP=%-4d FN=%-4d",
            field,
            field_result.precision,
            field_result.recall,
            field_result.f1,
            field_result.true_positives,
            field_result.false_positives,
            field_result.false_negatives,
        )

    # Summary
    non_none = [r for r in results if r.true_positives + r.false_positives + r.false_negatives > 0]
    if non_none:
        avg_f1 = sum(r.f1 for r in non_none) / len(non_none)
        log.info("Average F1 across fields: %.4f", avg_f1)

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def print_report(results: list[EvaluationResult], output_file: str | None = None):
    """Print a formatted evaluation report."""
    report_lines = [
        "=" * 80,
        "NLP Extraction Evaluation Report",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        f"Model: {config.OPENAI_MODEL}",
        f"Sample size: {sum(r.true_positives + r.false_positives + r.false_negatives for r in results)}",
        "=" * 80,
        f"{'Field':<22} {'Precision':>10} {'Recall':>10} {'F1':>10} {'TP':>6} {'FP':>6} {'FN':>6}",
        "-" * 80,
    ]

    for r in results:
        report_lines.append(
            f"{r.field:<22} {r.precision:>10.4f} {r.recall:>10.4f} "
            f"{r.f1:>10.4f} {r.true_positives:>6} {r.false_positives:>6} {r.false_negatives:>6}"
        )

    report_lines.append("=" * 80)

    report = "\n".join(report_lines)
    print(report)

    if output_file:
        os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as fh:
            fh.write(report + "\n")
        log.info("Report saved to %s", output_file)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_collect():
    """Collect manual labels interactively."""
    labels = collect_labels()
    log.info("Labelling session complete. %d labels saved to %s", len(labels), LABEL_FILE)


def cmd_evaluate():
    """Run evaluation against existing manual labels."""
    results = run_evaluation()
    print_report(results, "./state/evaluation_report.txt")


def cmd_full():
    """Collect labels then evaluate."""
    cmd_collect()
    cmd_evaluate()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NLP Extraction Evaluation Harness")
    parser.add_argument(
        "command",
        choices=["collect", "evaluate", "full"],
        help="collect: interactive labelling | evaluate: run metrics | full: both",
    )
    args = parser.parse_args()

    if args.command == "collect":
        cmd_collect()
    elif args.command == "evaluate":
        cmd_evaluate()
    else:
        cmd_full()
