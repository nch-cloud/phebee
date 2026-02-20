"""
PheBee API Performance Test (Manuscript Table 4)

Comprehensive API latency testing with 10 realistic query patterns. Uses synthetic datasets
generated with realistic disease clustering patterns for accurate performance evaluation.

How to run:
  PHEBEE_EVAL_SCALE=1 \
  PHEBEE_EVAL_USE_DISEASE_CLUSTERING=1 \
  PHEBEE_EVAL_TERMS_JSON_PATH=data/hpo_terms.json \
  PHEBEE_EVAL_SCALE_SUBJECTS=10000 \
  PHEBEE_EVAL_LATENCY_N=500 \
  PHEBEE_EVAL_CONCURRENCY=25 \
  pytest -v -s test_evaluation_perf_scale.py

Optional:
  PHEBEE_EVAL_METRICS_S3_URI=s3://<bucket>/<prefix>   # Upload metrics JSON to S3
  PHEBEE_EVAL_METRICS_PATH=/tmp/phebee_api_metrics.json # Write metrics JSON locally
  PHEBEE_EVAL_STRICT_LATENCY=1                         # Enforce p95<=5s gates

Note: Run test_import_performance.py first to populate data, then run this test against the
same project (uses session-scoped test_project_id fixture).
"""

from __future__ import annotations

import json
import os
import random
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple

import boto3
import pytest
import requests

# Import shared conftest utilities (relative import to avoid parent conftest)
from .conftest import GeneratedDataset

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
# Fixtures
# ---------------------------------------------------------------------

@pytest.fixture
def evaluation_run_id() -> str:
    """Generate unique run ID for this evaluation."""
    return f"api-perf-{uuid.uuid4().hex[:10]}"

# ---------------------------------------------------------------------
# Minimal term lists for API query patterns
# ---------------------------------------------------------------------

# Cardiac terms - used for specialty-specific query patterns
CARDIAC_TERMS = [
    "http://purl.obolibrary.org/obo/HP_0001627",  # Abnormal heart morphology
    "http://purl.obolibrary.org/obo/HP_0001635",  # Heart failure
    "http://purl.obolibrary.org/obo/HP_0001644",  # Dilated cardiomyopathy
]

# Neurological terms - used for specialty-specific query patterns
NEURO_TERMS = [
    "http://purl.obolibrary.org/obo/HP_0001250",  # Seizures
    "http://purl.obolibrary.org/obo/HP_0002069",  # Bilateral tonic-clonic seizures
    "http://purl.obolibrary.org/obo/HP_0001298",  # Encephalopathy
]

# ---------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------

def upload_jsonl_shards(
    *,
    s3_bucket: str,
    s3_prefix: str,
    records: List[Dict[str, Any]],
    shard_size: int,
) -> str:
    """Upload records as multiple JSONL files under s3://bucket/<s3_prefix>/jsonl/."""
    s3 = boto3.client("s3")
    jsonl_prefix = f"{s3_prefix.rstrip('/')}/jsonl"
    total = len(records)

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

    # Verify upload
    resp = s3.list_objects_v2(Bucket=s3_bucket, Prefix=jsonl_prefix + "/")
    keys = [o.get("Key") for o in resp.get("Contents", [])]
    if not any(k and k.endswith(".json") for k in keys):
        raise RuntimeError(f"No .json shards uploaded under s3://{s3_bucket}/{jsonl_prefix}/")

    return f"s3://{s3_bucket}/{jsonl_prefix}"

def run_bulk_import_sfn(
    *,
    physical_resources: Dict[str, Any],
    project_id: str,
    run_id: str,
    input_path_s3: str,
    timeout_s: int,
) -> Dict[str, Any]:
    """Run BulkImport Step Function and wait for completion."""
    sfn_arn = physical_resources.get("BulkImportStateMachine")
    if not sfn_arn:
        pytest.skip("BulkImportStateMachine not found in physical resources")

    sfn = boto3.client("stepfunctions")
    exec_name = f"perf-exec-{uuid.uuid4().hex[:10]}"

    start = sfn.start_execution(
        stateMachineArn=sfn_arn,
        name=exec_name,
        input=json.dumps({
            "run_id": run_id,
            "input_path": input_path_s3,
            "project_id": project_id  # Explicitly pass project_id to validation
        }),
    )
    exec_arn = start["executionArn"]

    start_time = time.time()
    while True:
        desc = sfn.describe_execution(executionArn=exec_arn)
        status = desc["status"]

        if status == "SUCCEEDED":
            return {"executionArn": exec_arn, "describe_execution": desc}
        if status in ("FAILED", "TIMED_OUT", "ABORTED"):
            hist = sfn.get_execution_history(executionArn=exec_arn, reverseOrder=True, maxResults=25)
            return {"executionArn": exec_arn, "describe_execution": desc, "history": hist}

        if time.time() - start_time > timeout_s:
            pytest.fail(f"Bulk import SFN did not complete within {timeout_s} seconds (executionArn={exec_arn})")

        time.sleep(30)

def api_post(api_base_url: str, path: str, payload: Dict[str, Any], sigv4_auth) -> requests.Response:
    """Make authenticated API POST request."""
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
    """Time a function call and return duration in seconds."""
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0

def run_concurrent(fn, n: int, concurrency: int) -> List[float]:
    """Run function concurrently and return timing results."""
    timings: List[float] = []
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [ex.submit(timed_call, fn) for _ in range(n)]
        for fut in as_completed(futs):
            timings.append(fut.result())
    return timings

def maybe_write_metrics(metrics: Dict[str, Any]) -> None:
    """Write metrics to console, local file, and/or S3."""
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
# Comprehensive API testing functions
# ---------------------------------------------------------------------

def create_api_test_functions(api_base_url: str, sigv4_auth, project_id: str,
                            project_subject_iris: List[str],
                            dataset_terms: List[str]) -> Dict[str, callable]:
    """Create comprehensive API test functions covering all realistic query patterns."""

    # Rotation index for subject queries
    idx = {"i": 0}

    def call_basic_subjects_query():
        """Basic project subjects query - most common pattern."""
        r = api_post(api_base_url, "/subjects/query", {
            "project_id": project_id,
            "limit": 10
        }, sigv4_auth)
        assert r.status_code == 200

    def call_individual_subject():
        """Individual subject lookup - common for detailed views."""
        i = idx["i"]
        idx["i"] = (i + 1) % len(project_subject_iris)
        r = api_post(api_base_url, "/subject", {
            "project_subject_iri": project_subject_iris[i]
        }, sigv4_auth)
        assert r.status_code == 200

    def call_hierarchy_query():
        """Hierarchy expansion query - research pattern."""
        r = api_post(api_base_url, "/subjects/query", {
            "project_id": project_id,
            "term_iri": "http://purl.obolibrary.org/obo/HP_0000118",  # Phenotypic abnormality (root)
            "include_child_terms": True,     # Test hierarchy expansion
            "limit": 20
        }, sigv4_auth)
        assert r.status_code == 200

    def call_qualified_filtering():
        """Qualifier filtering - exclude negated/family/hypothetical."""
        term = random.choice(dataset_terms) if dataset_terms else CARDIAC_TERMS[0]
        r = api_post(api_base_url, "/subjects/query", {
            "project_id": project_id,
            "term_iri": term,
            "include_qualified": False,      # Exclude qualified findings
            "limit": 15
        }, sigv4_auth)
        assert r.status_code == 200

    def call_specific_phenotype():
        """Specific phenotype query - targeted research."""
        term = random.choice(dataset_terms) if dataset_terms else CARDIAC_TERMS[0]
        r = api_post(api_base_url, "/subjects/query", {
            "project_id": project_id,
            "term_iri": term,
            "include_child_terms": True,
            "limit": 25
        }, sigv4_auth)
        assert r.status_code == 200

    def call_cardiac_cohort():
        """Cardiac phenotype cohort - specialty research pattern."""
        term = random.choice(CARDIAC_TERMS)
        r = api_post(api_base_url, "/subjects/query", {
            "project_id": project_id,
            "term_iri": term,
            "include_child_terms": True,
            "include_qualified": False,
            "limit": 30
        }, sigv4_auth)
        assert r.status_code == 200

    def call_neuro_cohort():
        """Neurological phenotype cohort - specialty research pattern."""
        term = random.choice(NEURO_TERMS)
        r = api_post(api_base_url, "/subjects/query", {
            "project_id": project_id,
            "term_iri": term,
            "include_child_terms": True,
            "limit": 25
        }, sigv4_auth)
        assert r.status_code == 200

    def call_paginated_large_cohort():
        """Large cohort with pagination - stress test."""
        r = api_post(api_base_url, "/subjects/query", {
            "project_id": project_id,
            "term_iri": "http://purl.obolibrary.org/obo/HP_0000118",  # Root - matches most subjects
            "include_child_terms": True,
            "limit": 50               # Force pagination
        }, sigv4_auth)
        assert r.status_code == 200

        # Follow pagination if available
        body = r.json().get("body", {})
        if isinstance(body, dict) and body.get("pagination", {}).get("next_cursor"):
            r2 = api_post(api_base_url, "/subjects/query", {
                "project_id": project_id,
                "cursor": body["pagination"]["next_cursor"],
                "limit": 50
            }, sigv4_auth)
            assert r2.status_code == 200

    def call_subject_term_info():
        """Subject term info - detailed phenotype view."""
        i = idx["i"]
        idx["i"] = (i + 1) % len(project_subject_iris)
        term = random.choice(dataset_terms) if dataset_terms else CARDIAC_TERMS[0]

        # Extract subject_id from project_subject_iri
        subject_iri = project_subject_iris[i]
        subject_id = subject_iri.split("/")[-1]  # Get UUID from IRI

        r = api_post(api_base_url, "/subject/term-info", {
            "subject_id": subject_id,  # Use subject_id instead of project_subject_iri
            "term_iri": term
        }, sigv4_auth)
        # Note: May return 404 if subject doesn't have this term - that's OK for perf testing
        assert r.status_code in [200, 404]

    def call_version_specific_query():
        """Version-specific ontology query."""
        term_source_data = dataset_terms[0] if dataset_terms else None
        r = api_post(api_base_url, "/subjects/query", {
            "project_id": project_id,
            "term_source": "HPO",
            "limit": 20
        }, sigv4_auth)
        assert r.status_code == 200

    return {
        "basic_subjects_query": call_basic_subjects_query,
        "individual_subject": call_individual_subject,
        "hierarchy_query": call_hierarchy_query,
        "qualified_filtering": call_qualified_filtering,
        "specific_phenotype": call_specific_phenotype,
        "cardiac_cohort": call_cardiac_cohort,
        "neuro_cohort": call_neuro_cohort,
        "paginated_large_cohort": call_paginated_large_cohort,
        "subject_term_info": call_subject_term_info,
        "version_specific_query": call_version_specific_query,
    }

# ---------------------------------------------------------------------
# Main enhanced performance test
# ---------------------------------------------------------------------

@pytest.mark.perf
def test_r11_enhanced_api_latency_at_scale(
    api_base_url,
    sigv4_auth,
    physical_resources,
    test_project_id: str,
    evaluation_run_id: str,
    synthetic_dataset: GeneratedDataset,
):
    """
    Enhanced performance test with realistic clinical data patterns and comprehensive API testing.
    Uses shared synthetic_dataset fixture from conftest.py for consistency across tests.
    """
    project_id = test_project_id
    print(f"[DEBUG] test_project_id from fixture: {test_project_id}")
    records = synthetic_dataset.records
    dataset_stats = synthetic_dataset.stats

    # Verify project_id in loaded records
    sample_record_project_ids = set(r.get("project_id") for r in records[:5])
    print(f"[DEBUG] Sample project_ids from loaded records: {sample_record_project_ids}")

    # Resolve physical resources
    s3_bucket = physical_resources.get("PheBeeBucket")
    if not s3_bucket:
        pytest.skip("PheBeeBucket not found in physical resources")

    shard_size = int(os.environ.get("PHEBEE_EVAL_BATCH_SIZE", "10000"))
    ingest_timeout = int(os.environ.get("PHEBEE_EVAL_INGEST_TIMEOUT_S", "7200"))

    # Use statistics from GeneratedDataset
    total_subjects = dataset_stats["n_subjects"]
    total_terms = dataset_stats["n_unique_terms"]
    total_evidence = dataset_stats["n_evidence"]

    # Analyze qualifier distribution
    qualifier_stats = {"negated": 0, "family": 0, "hypothetical": 0, "total": 0}
    for record in records:
        for evidence in record["evidence"]:
            contexts = evidence.get("contexts", {})
            qualifier_stats["total"] += 1
            if contexts.get("negated", 0.0) > 0:
                qualifier_stats["negated"] += 1
            if contexts.get("family", 0.0) > 0:
                qualifier_stats["family"] += 1
            if contexts.get("hypothetical", 0.0) > 0:
                qualifier_stats["hypothetical"] += 1

    print(f"[DATASET_STATS] {len(records)} records, {total_subjects} subjects, {total_terms} unique terms, {total_evidence} evidence")
    print(f"[QUALIFIER_STATS] {qualifier_stats}")
    print(f"[DATASET_METADATA] {dataset_stats}")

    # Check if data already exists (from test_import_performance.py running first)
    # Use retry logic to handle eventual consistency
    print(f"\n[CHECKING] Checking if project already has data...")
    has_data = False
    max_retries = 3
    retry_delay_s = 30

    for attempt in range(max_retries):
        check_resp = api_post(api_base_url, "/subjects/query", {"project_id": project_id, "limit": 1}, sigv4_auth)
        if check_resp.status_code == 200:
            check_body = check_resp.json().get("body") or []
            if len(check_body) > 0:
                has_data = True
                print(f"[DATA_FOUND] Project {project_id} has data (attempt {attempt + 1}/{max_retries})")
                break
            else:
                if attempt < max_retries - 1:
                    print(f"[NO_DATA_YET] No data found (attempt {attempt + 1}/{max_retries}), waiting {retry_delay_s}s...")
                    time.sleep(retry_delay_s)
                else:
                    print(f"[NO_DATA] No data found after {max_retries} attempts")
        else:
            pytest.skip(f"Cannot check project data, API returned {check_resp.status_code}")

    ingest_result = {}  # Initialize in case we skip import
    if has_data:
        print(f"[SKIP_IMPORT] Project {project_id} already has data - skipping import step")
        ingest_s = 0  # No import time since we skipped it
    else:
        print(f"[NO_DATA] Project {project_id} is empty - proceeding with import")
        # Upload data
        input_prefix = f"perf-data/{evaluation_run_id}"
        input_path_s3 = upload_jsonl_shards(
            s3_bucket=s3_bucket,
            s3_prefix=input_prefix,
            records=records,
            shard_size=shard_size,
        )

        # Bulk import
        print(f"[BULK_IMPORT_START] {input_path_s3}")
        print(f"[DEBUG] Importing with project_id={project_id}")
        t0 = time.time()
        ingest_result = run_bulk_import_sfn(
            physical_resources=physical_resources,
            project_id=project_id,
            run_id=evaluation_run_id,
            input_path_s3=input_path_s3,
            timeout_s=ingest_timeout,
        )
        ingest_s = time.time() - t0

        status = ingest_result["describe_execution"]["status"]
        if status != "SUCCEEDED":
            hist = ingest_result.get("history")
            if hist:
                print("[SFN_HISTORY_TAIL]", json.dumps(hist, indent=2, default=str)[:8000])
            pytest.fail(f"Bulk import did not succeed (status={status})")

        print(f"[BULK_IMPORT_COMPLETE] {ingest_s:.1f}s, {len(records)/ingest_s:.1f} records/sec")

    # Fetch subjects for API testing
    print(f"[DEBUG] Querying for subjects with project_id={project_id}")
    resp = api_post(api_base_url, "/subjects/query", {"project_id": project_id, "limit": 100}, sigv4_auth)
    assert resp.status_code == 200, resp.text
    resp_json = resp.json()
    print(f"[DEBUG] API response keys: {list(resp_json.keys())}")
    print(f"[DEBUG] Full response: {json.dumps(resp_json, indent=2)[:2000]}")
    body = resp_json.get("body") or []
    print(f"[DEBUG] Body type: {type(body)}, Body length/keys: {len(body) if isinstance(body, (list, dict)) else 'N/A'}")
    project_subject_iris = [x.get("project_subject_iri") for x in body if x.get("project_subject_iri")]
    assert project_subject_iris, "No subjects returned after ingest; cannot run latency workload."

    print(f"[API_TEST_PREP] {len(project_subject_iris)} subjects available for testing")

    # Extract unique terms from dataset for query patterns
    dataset_terms = list(set(r["term_iri"] for r in records))

    # Create comprehensive API test functions
    api_functions = create_api_test_functions(api_base_url, sigv4_auth, project_id, project_subject_iris, dataset_terms)

    # Warm-up to reduce cold-start skew
    print("[WARMUP_START]")
    for name, func in list(api_functions.items())[:3]:  # Warm up first 3 functions
        try:
            func()
        except Exception as e:
            print(f"[WARMUP_WARNING] {name} failed: {e}")

    # Test parameters
    n = int(os.environ.get("PHEBEE_EVAL_LATENCY_N", "500"))
    conc = int(os.environ.get("PHEBEE_EVAL_CONCURRENCY", "25"))

    print(f"[LATENCY_TEST_START] {n} requests per endpoint, {conc} concurrent")

    # Run comprehensive latency tests
    latency_results = []
    for name, func in api_functions.items():
        print(f"[TESTING] {name}")
        try:
            timings = run_concurrent(func, n=n, concurrency=conc)
            result = {
                "endpoint": name,
                "n": len(timings),
                "p50_ms": round(pctl(timings, 50) * 1000, 2),
                "p95_ms": round(pctl(timings, 95) * 1000, 2),
                "p99_ms": round(pctl(timings, 99) * 1000, 2),
                "max_ms": round(max(timings) * 1000, 2) if timings else None,
                "min_ms": round(min(timings) * 1000, 2) if timings else None,
                "avg_ms": round(sum(timings) / len(timings) * 1000, 2) if timings else None,
            }
            latency_results.append(result)
            print(f"[RESULT] {name}: p50={result['p50_ms']}ms, p95={result['p95_ms']}ms, p99={result['p99_ms']}ms")
        except Exception as e:
            print(f"[ERROR] {name} failed: {e}")
            latency_results.append({
                "endpoint": name,
                "error": str(e),
                "n": 0
            })

    # Compile comprehensive metrics
    metrics: Dict[str, Any] = {
        "run_id": evaluation_run_id,
        "project_id": project_id,
        "dataset": {
            "n_records": len(records),
            "n_subjects": total_subjects,
            "n_unique_terms": total_terms,
            "n_evidence": total_evidence,
            "terms_per_subject": dataset_stats["terms_per_subject"],
            "evidence_per_record": dataset_stats["evidence_per_record"],
            "avg_evidence_per_record": round(total_evidence / len(records), 2) if records else 0,
            "qualifier_distribution": {
                "negated_pct": round(qualifier_stats["negated"] / qualifier_stats["total"] * 100, 1) if qualifier_stats["total"] > 0 else 0,
                "family_pct": round(qualifier_stats["family"] / qualifier_stats["total"] * 100, 1) if qualifier_stats["total"] > 0 else 0,
                "hypothetical_pct": round(qualifier_stats["hypothetical"] / qualifier_stats["total"] * 100, 1) if qualifier_stats["total"] > 0 else 0,
            },
            "disease_clustering_enabled": dataset_stats.get("disease_clustering_enabled", False),
            "cluster_distribution": dataset_stats.get("cluster_distribution", {}),
            "term_source": dataset_stats.get("term_source", {}),
        },
        "ingestion": {
            "seconds": round(ingest_s, 2) if ingest_s > 0 else None,
            "records_per_sec": round((len(records) / ingest_s), 2) if ingest_s > 0 else None,
            "sfn_execution_arn": ingest_result.get("executionArn") if ingest_result else None,
            "skipped": ingest_s == 0,
            "note": "Data already present from test_import_performance.py" if ingest_s == 0 else None,
        },
        "load_testing": {
            "concurrency": conc,
            "requests_per_endpoint": n,
            "total_api_calls": sum(r.get("n", 0) for r in latency_results)
        },
        "latency": latency_results,
        "performance_summary": {
            "fastest_p95_ms": min((r.get("p95_ms", float('inf')) for r in latency_results if "p95_ms" in r), default=None),
            "slowest_p95_ms": max((r.get("p95_ms", 0) for r in latency_results if "p95_ms" in r), default=None),
            "avg_p95_ms": round(sum(r.get("p95_ms", 0) for r in latency_results if "p95_ms" in r) /
                              len([r for r in latency_results if "p95_ms" in r]), 2) if latency_results else None,
        }
    }

    maybe_write_metrics(metrics)

    # Optional strict performance gates
    if os.environ.get("PHEBEE_EVAL_STRICT_LATENCY") == "1":
        for item in latency_results:
            if "p95_ms" in item:
                assert item["p95_ms"] <= 5000, f"{item['endpoint']} p95_ms={item['p95_ms']} > 5000ms"

    print(f"[TEST_COMPLETE] Enhanced performance test completed successfully")
