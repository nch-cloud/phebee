"""
Performance evaluation module for PheBee (manuscript Tables 3 & 4).

What this test does
- Generates synthetic, provenance-bearing phenotype evidence as NDJSON (JSONL) and ingests it through the
  PheBee bulk ingestion Step Function (end-to-end).
- Exercises representative interactive API workloads through API Gateway and reports p50/p95/p99 latencies.
- Emits manuscript-friendly artifacts (CSV + JSON) to help populate Tables 3 (ingestion) and 4 (latency).

Key design choices (defensible in the manuscript)
- Uses *real* HPO IRIs sampled from a precomputed term list JSON (PHEBEE_EVAL_TERMS_JSON_PATH).
- Generates subject term-sets with a mixed frequency distribution (common + rare) to avoid unrealistically
  sparse usage while still exercising diversity.
- Injects a small number of "anchor" terms (e.g., cardiovascular/neuro system terms) to ensure stable,
  interpretable cohort queries across runs.
- Separates "TermLink identity" (subject, term, qualifier context) from multiple evidence items per term,
  matching the PheBee model (idempotent assertion; evidence accumulates).

Required harness fixtures (same expectations as your existing integration/perf tests)
- api_base_url: str
- sigv4_auth: requests-compatible SigV4 auth object
- physical_resources: dict with at least:
    - PheBeeBucket
    - BulkImportStateMachine
- test_project_id: fixture that yields a project_id string (or substitute with your equivalent)

Environment variables (all optional; defaults are manuscript-friendly)
- PHEBEE_EVAL_SCALE=1                       # must be set to run (otherwise skipped)
- PHEBEE_EVAL_TERMS_JSON_PATH=/path/to.json # REQUIRED to run
- PHEBEE_EVAL_SCALE_SUBJECTS=10000
- PHEBEE_EVAL_SCALE_MIN_TERMS=5
- PHEBEE_EVAL_SCALE_MAX_TERMS=50
- PHEBEE_EVAL_SCALE_MIN_EVIDENCE=1
- PHEBEE_EVAL_SCALE_MAX_EVIDENCE=25
- PHEBEE_EVAL_BATCH_SIZE=10000
- PHEBEE_EVAL_INGEST_TIMEOUT_S=7200
- PHEBEE_EVAL_LATENCY_N=500
- PHEBEE_EVAL_CONCURRENCY=25
- PHEBEE_EVAL_NOTE_DATE_START=2023-01-01    # for synthetic note timestamps
- PHEBEE_EVAL_NOTE_DATE_END=2024-12-31
- PHEBEE_EVAL_QUAL_NEGATED_PCT=0.15
- PHEBEE_EVAL_QUAL_FAMILY_PCT=0.08
- PHEBEE_EVAL_QUAL_HYPOTHETICAL_PCT=0.05
- PHEBEE_EVAL_COMMON_TERM_PCT=0.70          # share of non-anchor terms sampled from "common" pool
- PHEBEE_EVAL_ANCHOR_TERM_PCT=0.60          # share of subjects that get an anchor (cardio/neuro) term
- PHEBEE_EVAL_WRITE_ARTIFACTS=1             # write CSV/JSON artifacts to /tmp and print paths (default 1)

Notes
- This test does not require direct Neptune access; all validation is done through the public API + Athena (optional).
- If your BulkImport SFN input schema differs, adjust mk_record()/mk_evidence_item() accordingly.
"""

from __future__ import annotations

import concurrent.futures
import csv
import json
import os
import random
import statistics
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import boto3
import pytest
import requests

pytestmark = [pytest.mark.integration, pytest.mark.perf]


# -----------------------------
# Config helpers
# -----------------------------

def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    return default if v is None else int(v)


def _env_float(name: str, default: float) -> float:
    v = os.environ.get(name)
    return default if v is None else float(v)


def _env_str(name: str, default: str) -> str:
    v = os.environ.get(name)
    return default if v is None else str(v)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _pctl(values: List[float], p: float) -> float:
    """Percentile with linear interpolation (p in [0,100]). Input values are floats."""
    if not values:
        raise ValueError("Cannot compute percentile on empty list")
    xs = sorted(values)
    if p <= 0:
        return xs[0]
    if p >= 100:
        return xs[-1]
    k = (len(xs) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    if f == c:
        return xs[f]
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def _assert_status(resp: requests.Response, expected: Iterable[int]) -> None:
    if resp.status_code not in set(expected):
        raise AssertionError(
            f"Unexpected status={resp.status_code}, expected={list(expected)}; body={resp.text}"
        )


def _json(resp: requests.Response) -> Dict[str, Any]:
    try:
        return resp.json()
    except Exception as e:
        raise AssertionError(f"Response was not valid JSON: {e}; body={resp.text}")


# -----------------------------
# Term universe (from JSON)
# -----------------------------

@dataclass(frozen=True)
class TermUniverse:
    all_terms: List[str]
    internal_terms: List[str]
    leaf_terms: List[str]
    common_terms: List[str]
    rare_terms: List[str]
    metadata: Dict[str, Any]

    def pick_common(self, rng: random.Random) -> str:
        return rng.choice(self.common_terms or self.all_terms)

    def pick_rare(self, rng: random.Random) -> str:
        return rng.choice(self.rare_terms or self.leaf_terms or self.all_terms)

    def pick_any(self, rng: random.Random) -> str:
        return rng.choice(self.all_terms)


def _load_terms_json(path: str) -> TermUniverse:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"PHEBEE_EVAL_TERMS_JSON_PATH not found: {path}")

    data = json.loads(p.read_text())

    # Flexible parsing: support a few reasonable shapes.
    metadata = data.get("metadata", {}) if isinstance(data, dict) else {}
    if isinstance(data, list):
        all_terms = [str(x) for x in data]
        internal_terms: List[str] = []
        leaf_terms: List[str] = []
        common_terms: List[str] = []
        rare_terms: List[str] = []
    else:
        # Common patterns we might have generated
        terms_obj = data.get("terms", data)

        def _get_list(*keys: str) -> List[str]:
            for k in keys:
                v = terms_obj.get(k)
                if isinstance(v, list) and v:
                    return [str(x) for x in v]
            return []

        all_terms = _get_list("all_terms", "all", "terms", "allHpoTerms")
        internal_terms = _get_list("internal_terms", "internal")
        leaf_terms = _get_list("leaf_terms", "leaf", "leaves")
        common_terms = _get_list("common_terms", "common")
        rare_terms = _get_list("rare_terms", "rare")

        # If the file only contains one list, treat it as all_terms.
        if not all_terms:
            # Try to detect a dict-of-lists and pick the longest list as "all_terms".
            candidate_lists = [v for v in terms_obj.values() if isinstance(v, list)]
            if candidate_lists:
                all_terms = [str(x) for x in max(candidate_lists, key=len)]

    if not all_terms:
        raise ValueError(f"Could not find any term lists in terms JSON: {path}")

    # Derive fallbacks if pools are missing (keeps the generator robust).
    # Internal ~= non-leaf; leaf ~= provided or last-resort: all_terms.
    internal_terms = internal_terms or all_terms
    leaf_terms = leaf_terms or all_terms

    # Derive "common" and "rare" pools if missing: take stable slices.
    # Common: first 2,000 internal terms (or less). Rare: first 5,000 leaf terms (or less).
    if not common_terms:
        common_terms = internal_terms[: min(2000, len(internal_terms))]
    if not rare_terms:
        rare_terms = leaf_terms[: min(5000, len(leaf_terms))]

    # Deduplicate while preserving order.
    def _dedupe(xs: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in xs:
            if x not in seen:
                out.append(x)
                seen.add(x)
        return out

    return TermUniverse(
        all_terms=_dedupe(all_terms),
        internal_terms=_dedupe(internal_terms),
        leaf_terms=_dedupe(leaf_terms),
        common_terms=_dedupe(common_terms),
        rare_terms=_dedupe(rare_terms),
        metadata=metadata,
    )


# -----------------------------
# Synthetic data generation
# -----------------------------

@dataclass(frozen=True)
class QualifierContext:
    negated: bool = False
    family: bool = False
    hypothetical: bool = False

    def as_contexts(self) -> Dict[str, float]:
        return {
            "negated": 1.0 if self.negated else 0.0,
            "family": 1.0 if self.family else 0.0,
            "hypothetical": 1.0 if self.hypothetical else 0.0,
        }

    def as_qualifier_list(self) -> List[str]:
        # Only used for some endpoints; keep stable strings.
        quals: List[str] = []
        if self.negated:
            quals.append("negated")
        if self.family:
            quals.append("family_history")
        if self.hypothetical:
            quals.append("hypothetical")
        return quals


@dataclass(frozen=True)
class TermSource:
    source: str
    version: str
    iri: str


def _mk_term_source_from_terms_json(universe: TermUniverse) -> TermSource:
    """
    Prefer metadata from the terms JSON; fall back to env defaults.
    """
    md = universe.metadata or {}
    source = md.get("ontology", md.get("source", "HPO"))
    version = md.get("release", md.get("version", _env_str("PHEBEE_EVAL_HPO_VERSION", "unknown")))
    iri = md.get(
        "release_iri",
        _env_str("PHEBEE_EVAL_HPO_VERSION_IRI", f"http://purl.obolibrary.org/obo/hp/releases/{version}/hp.owl"),
    )
    return TermSource(source=str(source), version=str(version), iri=str(iri))


def _parse_date(s: str) -> date:
    y, m, d = (int(x) for x in s.split("-"))
    return date(y, m, d)


def _rand_date_iso(rng: random.Random, start: date, end: date) -> str:
    if end < start:
        start, end = end, start
    days = (end - start).days
    offset = rng.randint(0, days if days > 0 else 0)
    return (start + timedelta(days=offset)).isoformat()


def _pick_qualifier_context(
    rng: random.Random,
    negated_pct: float,
    family_pct: float,
    hypothetical_pct: float,
) -> QualifierContext:
    """
    Choose *at most one* qualifier category for a TermLink (simplifies uniqueness + interpretation).
    Probability mass that doesn't land in a qualifier becomes "unqualified".
    """
    x = rng.random()
    if x < negated_pct:
        return QualifierContext(negated=True)
    x -= negated_pct
    if x < family_pct:
        return QualifierContext(family=True)
    x -= family_pct
    if x < hypothetical_pct:
        return QualifierContext(hypothetical=True)
    return QualifierContext()


def _draw_int_uniform(rng: random.Random, lo: int, hi: int) -> int:
    lo2 = min(lo, hi)
    hi2 = max(lo, hi)
    return rng.randint(lo2, hi2)


def _choose_terms_for_subject(
    rng: random.Random,
    universe: TermUniverse,
    k_terms: int,
    *,
    anchor_term_pct: float,
    common_term_pct: float,
    anchor_terms: Sequence[str],
) -> List[str]:
    """
    k_terms unique term IRIs:
    - With probability anchor_term_pct, include one anchor term (cardio/neuro).
    - Remaining terms sampled without replacement from mixture:
        - common_term_pct from common pool
        - remainder from rare pool
    """
    picked: List[str] = []
    seen = set()

    if k_terms <= 0:
        return picked

    if rng.random() < anchor_term_pct and anchor_terms:
        t = rng.choice(list(anchor_terms))
        picked.append(t)
        seen.add(t)

    # Sample remaining terms without replacement. If pools are small, we fall back to all_terms.
    while len(picked) < k_terms:
        use_common = rng.random() < common_term_pct
        cand = universe.pick_common(rng) if use_common else universe.pick_rare(rng)
        if cand in seen:
            continue
        picked.append(cand)
        seen.add(cand)

        # Safety: if we can't find enough unique terms, break.
        if len(seen) > len(universe.all_terms) - 5:
            break

    return picked


def _mk_evidence_item(
    *,
    creator_type: str,
    creator_id: str,
    creator_name: str,
    encounter_id: str,
    clinical_note_id: str,
    note_timestamp: str,
    qualifier_ctx: QualifierContext,
    term_source: TermSource,
    rng: random.Random,
) -> Dict[str, Any]:
    # Keep fields aligned with your existing scale harness + manuscript text.
    span_start = rng.randint(0, 200)
    span_end = span_start + rng.randint(5, 40)

    return {
        "type": "clinical_note",
        "clinical_note_id": clinical_note_id,
        "encounter_id": encounter_id,
        "evidence_creator_id": creator_id,
        "evidence_creator_type": creator_type,  # "automated" | "manual"
        "evidence_creator_name": creator_name,
        "note_timestamp": note_timestamp,
        # Optional but realistic-ish metadata
        "note_type": "progress_note",
        "provider_type": "physician",
        "author_specialty": "pediatrics",
        "span_start": span_start,
        "span_end": span_end,
        "contexts": qualifier_ctx.as_contexts(),
        # Term source metadata (record-level is canonical; duplicating here can be useful for audits)
        "term_source": {"source": term_source.source, "version": term_source.version, "iri": term_source.iri},
    }


def _mk_record(
    *,
    project_id: str,
    project_subject_id: str,
    term_iri: str,
    evidence_items: List[Dict[str, Any]],
    row_num: int,
    batch_id: int,
    qualifier_ctx: QualifierContext,
    term_source: TermSource,
) -> Dict[str, Any]:
    return {
        "project_id": project_id,
        "project_subject_id": project_subject_id,
        "term_iri": term_iri,
        "evidence": evidence_items,
        "row_num": row_num,
        "batch_id": batch_id,
        "qualifiers": qualifier_ctx.as_qualifier_list(),
        "term_source": {"source": term_source.source, "version": term_source.version, "iri": term_source.iri},
    }


def generate_scale_dataset_jsonl(
    *,
    project_id: str,
    universe: TermUniverse,
    n_subjects: int,
    min_terms: int,
    max_terms: int,
    min_evidence: int,
    max_evidence: int,
    rng_seed: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns (records, dataset_stats).

    - records are NDJSON-ready dicts (one record per subject-term assertion; each record has 1..N evidence items)
    - dataset_stats includes parameters and derived counts for the manuscript.
    """
    rng = random.Random(rng_seed)

    note_date_start = _parse_date(_env_str("PHEBEE_EVAL_NOTE_DATE_START", "2023-01-01"))
    note_date_end = _parse_date(_env_str("PHEBEE_EVAL_NOTE_DATE_END", "2024-12-31"))

    negated_pct = _env_float("PHEBEE_EVAL_QUAL_NEGATED_PCT", 0.15)
    family_pct = _env_float("PHEBEE_EVAL_QUAL_FAMILY_PCT", 0.08)
    hypothetical_pct = _env_float("PHEBEE_EVAL_QUAL_HYPOTHETICAL_PCT", 0.05)

    common_term_pct = _env_float("PHEBEE_EVAL_COMMON_TERM_PCT", 0.70)
    anchor_term_pct = _env_float("PHEBEE_EVAL_ANCHOR_TERM_PCT", 0.60)

    term_source = _mk_term_source_from_terms_json(universe)

    # Anchor terms: prefer real, widely used HPO terms that support interpretable cohort queries.
    anchor_terms = [
        "http://purl.obolibrary.org/obo/HP_0001627",  # Abnormality of the cardiovascular system
        "http://purl.obolibrary.org/obo/HP_0000707",  # Abnormality of the nervous system
    ]
    # Parent term used for descendant-expansion style queries (very general).
    parent_term = "http://purl.obolibrary.org/obo/HP_0000118"  # Phenotypic abnormality

    records: List[Dict[str, Any]] = []
    row_num = 0

    # Track distributions for reporting
    terms_per_subject: List[int] = []
    evidence_per_record: List[int] = []
    qualifier_counts = {"negated": 0, "family": 0, "hypothetical": 0, "unqualified": 0}

    for s in range(n_subjects):
        project_subject_id = f"perf-subj-{s:06d}"
        k_terms = _draw_int_uniform(rng, min_terms, max_terms)
        terms_per_subject.append(k_terms)

        subject_terms = _choose_terms_for_subject(
            rng,
            universe,
            k_terms,
            anchor_term_pct=anchor_term_pct,
            common_term_pct=common_term_pct,
            anchor_terms=anchor_terms,
        )

        # Ensure deterministic-but-random encounter ID per subject (helps with "realistic" repeated context).
        encounter_id = f"enc-{s:06d}"

        for t_idx, term_iri in enumerate(subject_terms):
            m_evidence = _draw_int_uniform(rng, min_evidence, max_evidence)
            evidence_per_record.append(m_evidence)

            qualifier_ctx = _pick_qualifier_context(rng, negated_pct, family_pct, hypothetical_pct)
            if qualifier_ctx.negated:
                qualifier_counts["negated"] += 1
            elif qualifier_ctx.family:
                qualifier_counts["family"] += 1
            elif qualifier_ctx.hypothetical:
                qualifier_counts["hypothetical"] += 1
            else:
                qualifier_counts["unqualified"] += 1

            evidence_items: List[Dict[str, Any]] = []
            for e in range(m_evidence):
                clinical_note_id = f"note-{s:06d}-{t_idx:03d}-{e:03d}"
                evidence_items.append(
                    _mk_evidence_item(
                        creator_type="automated",
                        creator_id="ods/phebee-eval:automated-v1",
                        creator_name="PheBee Eval NLP",
                        encounter_id=encounter_id,
                        clinical_note_id=clinical_note_id,
                        note_timestamp=_rand_date_iso(rng, note_date_start, note_date_end),
                        qualifier_ctx=qualifier_ctx,
                        term_source=term_source,
                        rng=rng,
                    )
                )

            # Add one manual evidence item for a single well-known assertion (R5-ish coverage),
            # without changing the TermLink identity (same qualifier context).
            if s == 0 and t_idx == 0:
                evidence_items.append(
                    _mk_evidence_item(
                        creator_type="manual",
                        creator_id="ods/phebee-eval:curator-1",
                        creator_name="PheBee Eval Curator",
                        encounter_id=encounter_id,
                        clinical_note_id=f"note-{s:06d}-{t_idx:03d}-manual",
                        note_timestamp=_rand_date_iso(rng, note_date_start, note_date_end),
                        qualifier_ctx=qualifier_ctx,
                        term_source=term_source,
                        rng=rng,
                    )
                )
                evidence_per_record[-1] += 1  # reflect the manual evidence

            rec = _mk_record(
                project_id=project_id,
                project_subject_id=project_subject_id,
                term_iri=term_iri,
                evidence_items=evidence_items,
                row_num=row_num,
                batch_id=0,  # overwritten when batching for S3
                qualifier_ctx=qualifier_ctx,
                term_source=term_source,
            )
            # Helpful: carry parent term for hierarchy query selection
            if s == 0 and t_idx == 0:
                rec["_eval_parent_term_for_descendant_test"] = parent_term
            row_num += 1
            records.append(rec)

    # Derived totals
    n_records = len(records)
    n_evidence_total = sum(len(r["evidence"]) for r in records)

    def _summary(xs: List[int]) -> Dict[str, Any]:
        if not xs:
            return {"min": None, "max": None, "mean": None, "median": None}
        return {
            "min": int(min(xs)),
            "max": int(max(xs)),
            "mean": float(statistics.fmean(xs)),
            "median": float(statistics.median(xs)),
        }

    dataset_stats: Dict[str, Any] = {
        "n_subjects": n_subjects,
        "n_records": n_records,
        "n_evidence": n_evidence_total,
        "n_unique_terms": len({r["term_iri"] for r in records}),
        "terms_per_subject": _summary(terms_per_subject),
        "evidence_per_record": _summary(evidence_per_record),
        "qualifier_distribution_records": qualifier_counts,
        "term_source": {"source": term_source.source, "version": term_source.version, "iri": term_source.iri},
        "generator_seed": rng_seed,
        "anchor_terms": anchor_terms,
    }

    return records, dataset_stats


def _split_batches(records: List[Dict[str, Any]], batch_size: int) -> List[List[Dict[str, Any]]]:
    return [records[i : i + batch_size] for i in range(0, len(records), batch_size)]


# -----------------------------
# AWS orchestration (S3 + SFN)
# -----------------------------

def _s3_put_jsonl(bucket: str, key: str, records: List[Dict[str, Any]]) -> None:
    s3 = boto3.client("s3")
    body = "\n".join(json.dumps(r) for r in records) + "\n"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )


def _start_sfn_execution(state_machine_arn: str, run_id: str, input_path: str) -> str:
    sfn = boto3.client("stepfunctions")
    execution_name = f"phebee-perf-{run_id}-{uuid.uuid4().hex[:8]}"
    resp = sfn.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps({"run_id": run_id, "input_path": input_path}),
    )
    return resp["executionArn"]


def _wait_for_sfn(execution_arn: str, timeout_s: int = 3600, poll_s: int = 10) -> Dict[str, Any]:
    sfn = boto3.client("stepfunctions")
    t0 = time.time()
    while True:
        desc = sfn.describe_execution(executionArn=execution_arn)
        status = desc["status"]
        if status in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"):
            return desc
        if (time.time() - t0) > timeout_s:
            raise TimeoutError(f"Timed out waiting for Step Function execution: {execution_arn}")
        time.sleep(poll_s)



# -----------------------------
# API wrappers + latency runner
# -----------------------------

def api_post(
    api_base_url: str,
    path: str,
    payload: Dict[str, Any],
    sigv4_auth: Any,
    *,
    call_name: str = "",
    timeout_s: float | None = None,
) -> Dict[str, Any]:
    """POST helper with *fail-fast* diagnostics.

    - No retries.
    - On HTTP != 200, raises with endpoint name + response body.
    - On ReadTimeout, raises with endpoint name + payload summary.

    Set PHEBEE_EVAL_HTTP_TIMEOUT_S to override the default (30s).
    """
    if timeout_s is None:
        timeout_s = float(os.environ.get("PHEBEE_EVAL_HTTP_TIMEOUT_S", "30"))

    try:
        resp = requests.post(
            f"{api_base_url}{path}",
            json=payload,
            auth=sigv4_auth,
            timeout=timeout_s,
        )
    except requests.exceptions.ReadTimeout as e:
        payload_keys = sorted(list(payload.keys()))
        raise AssertionError(
            f"[TIMEOUT] call={call_name or '<unnamed>'} path={path} timeout_s={timeout_s} "
            f"payload_keys={payload_keys} payload_preview={str(payload)[:500]}"
        ) from e

    if resp.status_code != 200:
        raise AssertionError(
            f"[HTTP_ERROR] call={call_name or '<unnamed>'} path={path} status={resp.status_code} body={resp.text}"
        )

    return _json(resp)



def _run_latency_load(
    *,
    api_base_url: str,
    sigv4_auth: Any,
    calls: List[Tuple[str, Dict[str, Any]]],
    n_per_endpoint: int,
    concurrency: int,
    warmup_per_endpoint: int = 5,
) -> List[Dict[str, Any]]:
    """Run a battery of API calls and report p50/p95/p99.

    Each item in calls is (endpoint_name, payload) and the payload must include
    an "_path" key indicating the API path.

    Fail-fast behavior:
    - No retries.
    - Any timeout/HTTP error raises immediately with endpoint + iteration context.
    """
    results: List[Dict[str, Any]] = []
    http_timeout_s = float(os.environ.get("PHEBEE_EVAL_HTTP_TIMEOUT_S", "30"))

    # Warmup (serial): reduces cold-start/caching noise without hiding it entirely.
    for name, payload in calls:
        path = payload.pop("_path")
        for wi in range(max(0, warmup_per_endpoint)):
            try:
                api_post(
                    api_base_url,
                    path,
                    payload,
                    sigv4_auth,
                    call_name=f"warmup:{name}",
                    timeout_s=http_timeout_s,
                )
            except Exception as e:
                raise AssertionError(
                    f"[WARMUP_FAILED] endpoint={name} path={path} warmup_i={wi} timeout_s={http_timeout_s} "
                    f"payload_keys={sorted(list(payload.keys()))} err={e}"
                ) from e
        payload["_path"] = path  # restore

    def _one_call(name: str, path: str, payload: Dict[str, Any], i: int) -> float:
        t0 = time.perf_counter()
        api_post(
            api_base_url,
            path,
            payload,
            sigv4_auth,
            call_name=f"{name}#{i}",
            timeout_s=http_timeout_s,
        )
        return (time.perf_counter() - t0) * 1000.0  # ms

    for name, payload in calls:
        path = payload["_path"]

        latencies: List[float] = []
        payload_copy = dict(payload)
        payload_copy.pop("_path", None)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
            futs = [ex.submit(_one_call, name, path, payload_copy, i) for i in range(n_per_endpoint)]
            for f in concurrent.futures.as_completed(futs):
                # If any worker raised, this will raise and abort the test with context.
                latencies.append(float(f.result()))

        latencies.sort()
        results.append(
            {
                "endpoint": name,
                "n": len(latencies),
                "min_ms": min(latencies) if latencies else None,
                "max_ms": max(latencies) if latencies else None,
                "avg_ms": float(statistics.fmean(latencies)) if latencies else None,
                "p50_ms": _pctl(latencies, 50),
                "p95_ms": _pctl(latencies, 95),
                "p99_ms": _pctl(latencies, 99),
            }
        )

    return results
# -----------------------------
# Artifacts (Tables 3 & 4)
# -----------------------------

def _write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})


# -----------------------------
# Main perf test
# -----------------------------

@pytest.mark.perf
def test_tables_3_and_4_perf_run(
    api_base_url: str,
    sigv4_auth: Any,
    physical_resources: Dict[str, Any],
    test_project_id: str,
):
    """
    Single, parameterized performance run.

    Use multiple runs (different env var settings) to populate a small grid of manuscript results.
    """
    if os.environ.get("PHEBEE_EVAL_SCALE") != "1":
        pytest.skip("Set PHEBEE_EVAL_SCALE=1 to run perf evaluation.")

    terms_path = os.environ.get("PHEBEE_EVAL_TERMS_JSON_PATH")
    if not terms_path:
        pytest.skip("Set PHEBEE_EVAL_TERMS_JSON_PATH to run perf evaluation.")

    # Params (Tables 3/4)
    n_subjects = _env_int("PHEBEE_EVAL_SCALE_SUBJECTS", 10_000)
    min_terms = _env_int("PHEBEE_EVAL_SCALE_MIN_TERMS", 5)
    max_terms = _env_int("PHEBEE_EVAL_SCALE_MAX_TERMS", 50)
    min_evidence = _env_int("PHEBEE_EVAL_SCALE_MIN_EVIDENCE", 1)
    max_evidence = _env_int("PHEBEE_EVAL_SCALE_MAX_EVIDENCE", 25)
    batch_size = _env_int("PHEBEE_EVAL_BATCH_SIZE", 10_000)
    ingest_timeout_s = _env_int("PHEBEE_EVAL_INGEST_TIMEOUT_S", 7200)
    latency_n = _env_int("PHEBEE_EVAL_LATENCY_N", 500)
    concurrency = _env_int("PHEBEE_EVAL_CONCURRENCY", 25)

    # Required physical resources
    bucket = physical_resources.get("PheBeeBucket")
    sm_arn = physical_resources.get("BulkImportStateMachine")
    if not bucket or not sm_arn:
        pytest.skip("Missing required physical resources: PheBeeBucket and/or BulkImportStateMachine")

    # Run identifier
    run_id = f"perf-run-{uuid.uuid4().hex[:10]}"

    # 1) Load terms + generate dataset
    universe = _load_terms_json(terms_path)
    rng_seed = _now_ms()  # you can override by setting PHEBEE_EVAL_SEED if you want strict reproducibility
    if os.environ.get("PHEBEE_EVAL_SEED"):
        rng_seed = int(os.environ["PHEBEE_EVAL_SEED"])

    records, dataset_stats = generate_scale_dataset_jsonl(
        project_id=test_project_id,
        universe=universe,
        n_subjects=n_subjects,
        min_terms=min_terms,
        max_terms=max_terms,
        min_evidence=min_evidence,
        max_evidence=max_evidence,
        rng_seed=rng_seed,
    )

    print(f"[DATASET_STATS] {dataset_stats['n_records']} records, {dataset_stats['n_subjects']} subjects, "
          f"{dataset_stats['n_unique_terms']} unique terms, {dataset_stats['n_evidence']} evidence")

    # 2) Stage to S3 as NDJSON batches
    s3_prefix = f"perf-data/{run_id}/jsonl"
    batches = _split_batches(records, batch_size=max(1, batch_size))
    for i, batch in enumerate(batches):
        # Keep batch_id consistent inside each record (useful for debugging).
        for r in batch:
            r["batch_id"] = i
        key = f"{s3_prefix}/batch-{i:05d}.json"
        _s3_put_jsonl(bucket, key, batch)

    input_path = f"s3://{bucket}/{s3_prefix}"
    print(f"[BULK_IMPORT_START] {input_path}")

    # 3) Start bulk ingest SFN and wait
    t0 = time.time()
    execution_arn = _start_sfn_execution(sm_arn, run_id=run_id, input_path=input_path)
    desc = _wait_for_sfn(execution_arn, timeout_s=ingest_timeout_s)
    t_ingest_s = time.time() - t0

    if desc["status"] != "SUCCEEDED":
        raise AssertionError(f"Bulk import did not succeed: {desc}")

    recs_per_sec = dataset_stats["n_records"] / t_ingest_s if t_ingest_s > 0 else None
    ev_per_sec = dataset_stats["n_evidence"] / t_ingest_s if t_ingest_s > 0 else None

    print(f"[BULK_IMPORT_COMPLETE] {t_ingest_s:.1f}s, {recs_per_sec:.1f} records/sec, {ev_per_sec:.1f} evidence/sec")

    # 4) Pick a small subject sample for latency tests (avoid scanning the whole project)
    # We query by an anchor term that we *know* is frequent.
    anchor_query_term = "http://purl.obolibrary.org/obo/HP_0001627"
    q = api_post(
        api_base_url,
        "/subjects/query",
        {"project_id": test_project_id, "term_iri": anchor_query_term, "include_child_terms": False, "include_qualified": True, "limit": 200},
        sigv4_auth,
    )
    subjects = q.get("body", [])
    if not subjects:
        # fall back to an unfiltered project query
        q2 = api_post(api_base_url, "/subjects/query", {"project_id": test_project_id, "limit": 200}, sigv4_auth)
        subjects = q2.get("body", [])

    if not subjects:
        raise AssertionError("No subjects available for latency testing after ingest.")

    # Pick a stable subset
    sample_subjects = subjects[:100]
    print(f"[API_TEST_PREP] {len(sample_subjects)} subjects available for testing")

    # Helper: build payloads for endpoints that need a subject UUID / project_subject_iri
    def _pick_subject() -> Dict[str, Any]:
        return random.choice(sample_subjects)

    # Discover one subject's term list so we can select a real term for term-info
    subj0 = _pick_subject()
    subj_summary = api_post(api_base_url, "/subject", {"project_subject_iri": subj0["project_subject_iri"]}, sigv4_auth)
    terms0 = subj_summary.get("terms", []) or []
    term_for_term_info = (terms0[0]["term_iri"] if terms0 else anchor_query_term)
    qualifiers_for_term_info = (terms0[0].get("qualifiers", []) if terms0 else [])

    # Parent term for hierarchy query (from injected field if available)
    parent_term = "http://purl.obolibrary.org/obo/HP_0000118"

    # Term source filters (optional; only used if your API supports it)
    ts = dataset_stats.get("term_source", {})
    term_source = ts.get("source")
    term_source_version = ts.get("version")

    # 5) Define representative endpoint calls (Table 4)
    calls: List[Tuple[str, Dict[str, Any]]] = [
        ("basic_subjects_query", {"_path": "/subjects/query", "project_id": test_project_id, "limit": 50}),
        ("individual_subject", {"_path": "/subject", "project_subject_iri": subj0["project_subject_iri"]}),
        ("hierarchy_query", {"_path": "/subjects/query", "project_id": test_project_id, "term_iri": parent_term, "include_child_terms": True, "include_qualified": True, "limit": 50}),
        ("qualified_filtering", {"_path": "/subjects/query", "project_id": test_project_id, "term_iri": anchor_query_term, "include_child_terms": False, "include_qualified": False, "limit": 50}),
        ("specific_phenotype", {"_path": "/subjects/query", "project_id": test_project_id, "term_iri": term_for_term_info, "include_child_terms": False, "include_qualified": True, "limit": 50}),
        ("cardiac_cohort", {"_path": "/subjects/query", "project_id": test_project_id, "term_iri": "http://purl.obolibrary.org/obo/HP_0001627", "include_child_terms": True, "include_qualified": True, "limit": 50}),
        ("neuro_cohort", {"_path": "/subjects/query", "project_id": test_project_id, "term_iri": "http://purl.obolibrary.org/obo/HP_0000707", "include_child_terms": True, "include_qualified": True, "limit": 50}),
        ("paginated_large_cohort", {"_path": "/subjects/query", "project_id": test_project_id, "limit": 50, "cursor": None}),
        ("subject_term_info", {"_path": "/subject/term-info", "subject_id": subj0["subject_iri"].split("/")[-1], "term_iri": term_for_term_info, "qualifiers": qualifiers_for_term_info}),
    ]

    # Optional version-specific query if your API supports it (safe: extra keys typically ignored).
    calls.append(
        (
            "version_specific_query",
            {
                "_path": "/subjects/query",
                "project_id": test_project_id,
                "term_iri": anchor_query_term,
                "include_child_terms": True,
                "include_qualified": True,
                "term_source": term_source,
                "term_source_version": term_source_version,
                "limit": 50,
            },
        )
    )

    print(f"[LATENCY_TEST_START] {latency_n} requests per endpoint, {concurrency} concurrent")
    latency_rows = _run_latency_load(
        api_base_url=api_base_url,
        sigv4_auth=sigv4_auth,
        calls=calls,
        n_per_endpoint=latency_n,
        concurrency=concurrency,
        warmup_per_endpoint=5,
    )

    for r in latency_rows:
        print(f"[RESULT] {r['endpoint']}: p50={r['p50_ms']:.2f}ms, p95={r['p95_ms']:.2f}ms, p99={r['p99_ms']:.2f}ms")

    # 6) Produce manuscript-friendly outputs for Tables 3 & 4
    table3 = {
        "run_id": run_id,
        "project_id": test_project_id,
        "n_subjects": dataset_stats["n_subjects"],
        "n_records": dataset_stats["n_records"],
        "n_evidence": dataset_stats["n_evidence"],
        "n_unique_terms": dataset_stats["n_unique_terms"],
        "terms_per_subject_mean": dataset_stats["terms_per_subject"]["mean"],
        "terms_per_subject_median": dataset_stats["terms_per_subject"]["median"],
        "terms_per_subject_min": dataset_stats["terms_per_subject"]["min"],
        "terms_per_subject_max": dataset_stats["terms_per_subject"]["max"],
        "evidence_per_record_mean": dataset_stats["evidence_per_record"]["mean"],
        "evidence_per_record_median": dataset_stats["evidence_per_record"]["median"],
        "evidence_per_record_min": dataset_stats["evidence_per_record"]["min"],
        "evidence_per_record_max": dataset_stats["evidence_per_record"]["max"],
        "ingest_seconds": round(t_ingest_s, 2),
        "records_per_sec": round(recs_per_sec, 2) if recs_per_sec is not None else None,
        "evidence_per_sec": round(ev_per_sec, 2) if ev_per_sec is not None else None,
        "concurrency": concurrency,
        "requests_per_endpoint": latency_n,
        "sfn_execution_arn": execution_arn,
        "term_source": dataset_stats.get("term_source"),
        "qualifier_distribution_records": dataset_stats.get("qualifier_distribution_records"),
    }

    # Summary helper for the manuscript narrative
    p95s = [r["p95_ms"] for r in latency_rows if r.get("p95_ms") is not None]
    perf_summary = {
        "avg_p95_ms": float(statistics.fmean(p95s)) if p95s else None,
        "fastest_p95_ms": float(min(p95s)) if p95s else None,
        "slowest_p95_ms": float(max(p95s)) if p95s else None,
    }

    out_json = {
        "run_id": run_id,
        "dataset": dataset_stats,
        "ingestion": {
            "seconds": round(t_ingest_s, 2),
            "records_per_sec": round(recs_per_sec, 2) if recs_per_sec is not None else None,
            "evidence_per_sec": round(ev_per_sec, 2) if ev_per_sec is not None else None,
            "sfn_execution_arn": execution_arn,
        },
        "latency": latency_rows,
        "load_testing": {"concurrency": concurrency, "requests_per_endpoint": latency_n, "total_api_calls": latency_n * len(calls)},
        "performance_summary": perf_summary,
        "table3": table3,
    }

    print("[EVAL_JSON]", json.dumps(out_json, indent=2))

    # 7) Write artifacts (CSV + JSON) for manuscript tables
    if os.environ.get("PHEBEE_EVAL_WRITE_ARTIFACTS", "1") == "1":
        # Use /tmp so it works in CI runners too; print paths so you can copy them out.
        base = Path("/tmp/phebee-eval-artifacts") / run_id
        base.mkdir(parents=True, exist_ok=True)

        table3_path = base / "table3_ingestion.csv"
        table4_path = base / "table4_latency.csv"
        json_path = base / "eval_run.json"

        _write_csv(
            table3_path,
            [table3],
            fieldnames=list(table3.keys()),
        )
        _write_csv(
            table4_path,
            latency_rows,
            fieldnames=["endpoint", "n", "min_ms", "max_ms", "avg_ms", "p50_ms", "p95_ms", "p99_ms"],
        )
        json_path.write_text(json.dumps(out_json, indent=2), encoding="utf-8")

        print(f"[ARTIFACT] Table 3 CSV: {table3_path}")
        print(f"[ARTIFACT] Table 4 CSV: {table4_path}")
        print(f"[ARTIFACT] Full JSON:   {json_path}")
