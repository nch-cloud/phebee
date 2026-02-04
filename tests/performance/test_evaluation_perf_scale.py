"""
Scale/performance evaluation harness for PheBee (R11).

This module is intentionally separated from functional integration tests so it can be run
explicitly against an existing stack (e.g., phebee-dev) without impacting CI.

How to run (example):
  PHEBEE_EVAL_SCALE=1 \
  PHEBEE_EVAL_SCALE_SUBJECTS=10000 \
  PHEBEE_EVAL_SCALE_TERMS_PER_SUBJECT=10 \
  PHEBEE_EVAL_SCALE_EVIDENCE_PER_TERM=2 \
  PHEBEE_EVAL_BATCH_SIZE=10000 \
  PHEBEE_EVAL_INGEST_TIMEOUT_S=7200 \
  PHEBEE_EVAL_LATENCY_N=500 \
  PHEBEE_EVAL_CONCURRENCY=25 \
  pytest -m perf tests/integration/test_evaluation_perf_scale.py

Optional:
  PHEBEE_EVAL_METRICS_S3_URI=s3://<bucket>/<prefix>   # upload metrics JSON to S3
  PHEBEE_EVAL_METRICS_PATH=/tmp/phebee_r11_metrics.json # write metrics JSON locally
  PHEBEE_EVAL_STRICT_LATENCY=1                         # enforce p95<=5s gates
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pytest
import requests


# ---------------------------------------------------------------------
# Module-level gate: don't run perf scale tests unless explicitly enabled
# ---------------------------------------------------------------------
if os.environ.get("PHEBEE_EVAL_SCALE") != "1":
    pytest.skip(
        "Scale perf tests disabled (set PHEBEE_EVAL_SCALE=1).",
        allow_module_level=True,
    )


pytestmark = [pytest.mark.integration, pytest.mark.perf]


# ---------------------------------------------------------------------
# Data model helpers
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class TermSource:
    source: str
    version: str
    iri: str

    def as_dict(self) -> Dict[str, str]:
        return {"source": self.source, "version": self.version, "iri": self.iri}


@pytest.fixture
def term_source_hpo() -> TermSource:
    """
    Default term source used for synthetic data. Adjust in your environment if needed.
    Lowercase 'hpo' matches your ontology update machinery convention.
    """
    return TermSource(source="hpo", version="2024-01-01", iri="http://purl.obolibrary.org/obo/hp.owl")


@pytest.fixture
def scale_run_enabled() -> bool:
    # Kept as a fixture so downstream fixtures can depend on it.
    return True


@pytest.fixture
def evaluation_run_id() -> str:
    return f"perf-run-{uuid.uuid4().hex[:10]}"


# ---------------------------------------------------------------------
# Synthetic dataset generation
# ---------------------------------------------------------------------
def _random_term_iri(i: int) -> str:
    # Use a stable but varied HPO-like ID space; this is synthetic.
    # HP_0000001 .. etc.
    return f"http://purl.obolibrary.org/obo/HP_{i:07d}"


def generate_synthetic_records(
    *,
    project_id: str,
    n_subjects: int,
    terms_per_subject: int,
    evidence_per_term: int,
    term_source: Optional[TermSource] = None,
) -> List[Dict[str, Any]]:
    """
    Generate records matching the JSONL input expected by the BulkImport Step Function
    (see existing test_bulk_import_stepfunction.py).
    """
    records: List[Dict[str, Any]] = []
    row_num = 0

    now_date = "2024-01-16"  # purely synthetic and stable

    for s in range(n_subjects):
        project_subject_id = f"subj-{s:06d}"
        # Make encounter/note IDs deterministic-ish per subject for realism
        encounter_id = f"enc-{s:06d}"
        base_note_id = f"note-{s:06d}"

        for t in range(terms_per_subject):
            term_iri = _random_term_iri((t % 2500) + 1)  # cap distinct terms to keep label cache realistic

            for e in range(evidence_per_term):
                row_num += 1
                note_id = f"{base_note_id}-{e:02d}"

                evidence_obj: Dict[str, Any] = {
                    "type": "clinical_note",
                    "clinical_note_id": note_id,
                    "encounter_id": encounter_id,
                    "evidence_creator_id": "ods/phebee-perfgen:v1",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "PheBee Perf Generator",
                    "note_timestamp": now_date,
                    "note_type": "progress_note",
                    "provider_type": "physician",
                    "author_specialty": "cardiology",
                    "span_start": 10 + (e * 5),
                    "span_end": 20 + (e * 5),
                    # Context/qualifiers in the current bulk import shape
                    "contexts": {"negated": 0.0, "family": 0.0, "hypothetical": 0.0},
                }

                rec: Dict[str, Any] = {
                    "project_id": project_id,
                    "project_subject_id": project_subject_id,
                    "term_iri": term_iri,
                    "evidence": [evidence_obj],
                    "row_num": row_num,
                    "batch_id": 0,
                }

                # Optional: include term_source if your ingest supports it (R1). Safe for perf runs.
                if term_source is not None:
                    rec["term_source"] = term_source.as_dict()

                records.append(rec)

    return records


@pytest.fixture
def evaluation_dataset_scale(
    scale_run_enabled: bool,
    test_project_id: str,
    term_source_hpo: TermSource,
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Returns (project_id, records) for a scale run. Uses the existing `test_project_id` fixture.
    """
    n_subjects = int(os.environ.get("PHEBEE_EVAL_SCALE_SUBJECTS", "10000"))
    terms_per_subject = int(os.environ.get("PHEBEE_EVAL_SCALE_TERMS_PER_SUBJECT", "10"))
    evidence_per_term = int(os.environ.get("PHEBEE_EVAL_SCALE_EVIDENCE_PER_TERM", "2"))

    records = generate_synthetic_records(
        project_id=test_project_id,
        n_subjects=n_subjects,
        terms_per_subject=terms_per_subject,
        evidence_per_term=evidence_per_term,
        term_source=term_source_hpo,
    )
    return test_project_id, records


# ---------------------------------------------------------------------
# Utility: upload JSONL shards to S3
# ---------------------------------------------------------------------
def upload_jsonl_shards(
    *,
    s3_bucket: str,
    s3_prefix: str,
    records: List[Dict[str, Any]],
    shard_size: int,
) -> str:
    """
    Upload records as multiple JSONL files under s3://bucket/<s3_prefix>/jsonl/.
    Returns the s3://.../jsonl prefix.
    """
    s3 = boto3.client("s3")
    jsonl_prefix = f"{s3_prefix.rstrip('/')}/jsonl"
    total = len(records)

    # Ensure at least 1 shard
    shard_size = max(1, shard_size)
    shard_count = (total + shard_size - 1) // shard_size

    for i in range(shard_count):
        chunk = records[i * shard_size : (i + 1) * shard_size]
        body = "\n".join(json.dumps(r) for r in chunk).encode("utf-8")
        key = f"{jsonl_prefix}/shard-{i:05d}.json"
        s3.put_object(
            Bucket=s3_bucket,
            Key=key,
            Body=body,
            ContentType="application/x-ndjson",
        )

    # Preflight: ensure Spark glob ("*.json") will match at least one uploaded shard
    resp = s3.list_objects_v2(Bucket=s3_bucket, Prefix=jsonl_prefix + "/")
    keys = [o.get("Key") for o in resp.get("Contents", [])]
    if not any(k and k.endswith(".json") for k in keys):
        raise RuntimeError(f"No .json shards uploaded under s3://{s3_bucket}/{jsonl_prefix}/")

    return f"s3://{s3_bucket}/{jsonl_prefix}"


# ---------------------------------------------------------------------
# Utility: run BulkImport Step Function and wait
# ---------------------------------------------------------------------
def run_bulk_import_sfn(
    *,
    physical_resources: Dict[str, Any],
    run_id: str,
    input_path_s3: str,
    timeout_s: int,
) -> Dict[str, Any]:
    sfn_arn = physical_resources.get("BulkImportStateMachine")
    if not sfn_arn:
        pytest.skip("BulkImportStateMachine not found in physical resources")

    sfn = boto3.client("stepfunctions")
    exec_name = f"perf-exec-{uuid.uuid4().hex[:10]}"

    start = sfn.start_execution(
        stateMachineArn=sfn_arn,
        name=exec_name,
        input=json.dumps({"run_id": run_id, "input_path": input_path_s3}),
    )
    exec_arn = start["executionArn"]

    start_time = time.time()
    while True:
        desc = sfn.describe_execution(executionArn=exec_arn)
        status = desc["status"]

        if status == "SUCCEEDED":
            return {"executionArn": exec_arn, "describe_execution": desc}
        if status in ("FAILED", "TIMED_OUT", "ABORTED"):
            # Add minimal recent history for debugging
            hist = sfn.get_execution_history(executionArn=exec_arn, reverseOrder=True, maxResults=25)
            return {"executionArn": exec_arn, "describe_execution": desc, "history": hist}

        if time.time() - start_time > timeout_s:
            pytest.fail(f"Bulk import SFN did not complete within {timeout_s} seconds (executionArn={exec_arn})")

        time.sleep(30)


# ---------------------------------------------------------------------
# Utility: API calls + latency stats
# ---------------------------------------------------------------------
def api_post(api_base_url: str, path: str, payload: Dict[str, Any], sigv4_auth) -> requests.Response:
    url = f"{api_base_url}{path}"
    return requests.post(url, json=payload, auth=sigv4_auth, timeout=60)


def pctl(values: List[float], percentile: float) -> float:
    """Return percentile of list of floats (seconds)."""
    if not values:
        return float("nan")
    vals = sorted(values)
    k = (len(vals) - 1) * (percentile / 100.0)
    f = int(k)
    c = min(f + 1, len(vals) - 1)
    if f == c:
        return vals[f]
    return vals[f] + (vals[c] - vals[f]) * (k - f)


def timed_call(fn) -> float:
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


def run_concurrent(fn, n: int, concurrency: int) -> List[float]:
    timings: List[float] = []
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [ex.submit(timed_call, fn) for _ in range(n)]
        for fut in as_completed(futs):
            timings.append(fut.result())
    return timings


def maybe_write_metrics(metrics: Dict[str, Any]) -> None:
    # Always print for logs
    print("[EVAL_R11_JSON]", json.dumps(metrics, indent=2, sort_keys=True))

    # Optional local file write
    out_path = os.environ.get("PHEBEE_EVAL_METRICS_PATH")
    if out_path:
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2, sort_keys=True)
        print(f"[EVAL_METRICS_WRITTEN] {out_path}")

    # Optional S3 upload
    out_s3 = os.environ.get("PHEBEE_EVAL_METRICS_S3_URI")
    if out_s3 and out_s3.startswith("s3://"):
        m = re.match(r"s3://([^/]+)/?(.*)", out_s3)
        if m:
            bucket, prefix = m.group(1), m.group(2)
            key = f"{prefix.rstrip('/')}/r11_metrics_{metrics.get('run_id','unknown')}.json"
            boto3.client("s3").put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(metrics, indent=2, sort_keys=True).encode("utf-8"),
                ContentType="application/json",
            )
            print(f"[EVAL_METRICS_UPLOADED] s3://{bucket}/{key}")


# ---------------------------------------------------------------------
# Main perf test (R11)
# ---------------------------------------------------------------------
@pytest.mark.perf
def test_r11_api_latency_at_scale(
    api_base_url,
    sigv4_auth,
    physical_resources,
    evaluation_run_id: str,
    evaluation_dataset_scale: Tuple[str, List[Dict[str, Any]]],
):
    project_id, records = evaluation_dataset_scale

    # Resolve physical resources
    s3_bucket = physical_resources.get("PheBeeBucket")
    if not s3_bucket:
        pytest.skip("PheBeeBucket not found in physical resources")

    shard_size = int(os.environ.get("PHEBEE_EVAL_BATCH_SIZE", "10000"))
    ingest_timeout = int(os.environ.get("PHEBEE_EVAL_INGEST_TIMEOUT_S", "7200"))

    # Upload data
    input_prefix = f"perf-data/{evaluation_run_id}"
    input_path_s3 = upload_jsonl_shards(
        s3_bucket=s3_bucket,
        s3_prefix=input_prefix,
        records=records,
        shard_size=shard_size,
    )

    # Bulk import
    t0 = time.time()
    ingest_result = run_bulk_import_sfn(
        physical_resources=physical_resources,
        run_id=evaluation_run_id,
        input_path_s3=input_path_s3,
        timeout_s=ingest_timeout,
    )
    ingest_s = time.time() - t0

    status = ingest_result["describe_execution"]["status"]
    if status != "SUCCEEDED":
        # Print history if present
        hist = ingest_result.get("history")
        if hist:
            print("[SFN_HISTORY_TAIL]", json.dumps(hist, indent=2, default=str)[:8000])
        pytest.fail(f"Bulk import did not succeed (status={status})")

    # Fetch a sample of subjects for rotation
    resp = api_post(api_base_url, "/subjects/query", {"project_id": project_id, "limit": 50}, sigv4_auth)
    assert resp.status_code == 200, resp.text
    body = resp.json().get("body") or []
    project_subject_iris = [x.get("project_subject_iri") for x in body if x.get("project_subject_iri")]
    assert project_subject_iris, "No subjects returned after ingest; cannot run latency workload."

    # Warm-up to reduce cold-start skew
    api_post(api_base_url, "/subjects/query", {"project_id": project_id, "limit": 10}, sigv4_auth)
    api_post(api_base_url, "/subject", {"project_subject_iri": project_subject_iris[0]}, sigv4_auth)

    n = int(os.environ.get("PHEBEE_EVAL_LATENCY_N", "500"))
    conc = int(os.environ.get("PHEBEE_EVAL_CONCURRENCY", "25"))

    # Define workload functions
    def call_subjects_query():
        r = api_post(api_base_url, "/subjects/query", {"project_id": project_id, "limit": 10}, sigv4_auth)
        assert r.status_code == 200

    idx = {"i": 0}

    def call_subject():
        i = idx["i"]
        idx["i"] = (i + 1) % len(project_subject_iris)
        r = api_post(api_base_url, "/subject", {"project_subject_iri": project_subject_iris[i]}, sigv4_auth)
        assert r.status_code == 200

    # Run concurrent workloads
    timings_query = run_concurrent(call_subjects_query, n=n, concurrency=conc)
    timings_subject = run_concurrent(call_subject, n=n, concurrency=conc)

    def summarize(name: str, vals: List[float]) -> Dict[str, Any]:
        return {
            "endpoint": name,
            "n": len(vals),
            "p50_ms": round(pctl(vals, 50) * 1000, 2),
            "p95_ms": round(pctl(vals, 95) * 1000, 2),
            "max_ms": round(max(vals) * 1000, 2) if vals else None,
        }

    metrics: Dict[str, Any] = {
        "run_id": evaluation_run_id,
        "project_id": project_id,
        "dataset": {
            "n_records": len(records),
            "n_subjects": int(os.environ.get("PHEBEE_EVAL_SCALE_SUBJECTS", "0")),
            "terms_per_subject": int(os.environ.get("PHEBEE_EVAL_SCALE_TERMS_PER_SUBJECT", "0")),
            "evidence_per_term": int(os.environ.get("PHEBEE_EVAL_SCALE_EVIDENCE_PER_TERM", "0")),
        },
        "ingestion": {
            "seconds": round(ingest_s, 2),
            "records_per_sec": round((len(records) / ingest_s), 2) if ingest_s > 0 else None,
            "sfn_execution_arn": ingest_result.get("executionArn"),
        },
        "load": {"concurrency": conc, "requests_per_endpoint": n},
        "latency": [
            summarize("/subjects/query", timings_query),
            summarize("/subject", timings_subject),
        ],
    }

    maybe_write_metrics(metrics)

    # Optional strict gate for local validation runs
    if os.environ.get("PHEBEE_EVAL_STRICT_LATENCY") == "1":
        for item in metrics["latency"]:
            assert item["p95_ms"] <= 5000, f"{item['endpoint']} p95_ms={item['p95_ms']} > 5000"
