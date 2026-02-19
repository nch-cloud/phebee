"""
PheBee API Latency Test (Manuscript Table 4)

Evaluates API query response times under load with representative cohort query workloads.
Measures p50/p95/p99 latencies for various query patterns including basic queries,
hierarchy expansion, qualifier filtering, and large cohort retrieval.

Key Metrics (Table 4):
- p50/p95/p99 latency (milliseconds) per endpoint
- Min/max/average latency
- Query characteristics (limit, filtering, hierarchy expansion)

Environment Variables:
- PHEBEE_EVAL_SCALE=1 (required to enable test)
- PHEBEE_EVAL_TERMS_JSON_PATH (required): HPO terms JSON file
- PHEBEE_EVAL_LATENCY_N (default: 500): Number of requests per endpoint
- PHEBEE_EVAL_CONCURRENCY (default: 25): Concurrent request threads
- PHEBEE_EVAL_HTTP_TIMEOUT_S (default: 30): HTTP request timeout
- PHEBEE_EVAL_WRITE_ARTIFACTS (default: 1): Write CSV/JSON artifacts to /tmp

Required Fixtures:
- api_base_url: API Gateway endpoint URL
- sigv4_auth: AWS SigV4 authentication object
- test_project_id: PheBee project identifier
- synthetic_dataset: Generated dataset from conftest.py (for anchor terms)

Prerequisites:
- PheBee project must already have data imported
- Run test_import_performance.py first, or use existing project with sufficient data

Output Artifacts:
- /tmp/phebee-eval-artifacts/{run_id}/table4_latency.csv
- /tmp/phebee-eval-artifacts/{run_id}/latency_run.json
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
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest
import requests

pytestmark = [pytest.mark.integration, pytest.mark.perf]


# -----------------------------
# Helper functions
# -----------------------------


def _env_int(name: str, default: int) -> int:
    """Get integer from environment variable or use default."""
    v = os.environ.get(name)
    return default if v is None else int(v)


def _pctl(values: List[float], p: float) -> float:
    """
    Calculate percentile with linear interpolation.

    Args:
        values: List of numeric values
        p: Percentile (0-100)

    Returns:
        Percentile value
    """
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


def _json(resp: requests.Response) -> Dict[str, Any]:
    """Parse JSON response with error handling."""
    try:
        return resp.json()
    except Exception as e:
        raise AssertionError(f"Response was not valid JSON: {e}; body={resp.text}")


# -----------------------------
# API wrappers
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
    """
    POST request with fail-fast diagnostics.

    - No retries
    - Raises on HTTP != 200
    - Raises on timeout with context

    Args:
        api_base_url: Base API URL
        path: API path (e.g., "/subjects/query")
        payload: Request JSON body
        sigv4_auth: AWS SigV4 auth object
        call_name: Optional name for error messages
        timeout_s: Optional timeout override

    Returns:
        Response JSON body
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
            f"[HTTP_ERROR] call={call_name or '<unnamed>'} path={path} "
            f"status={resp.status_code} body={resp.text}"
        )

    return _json(resp)


# -----------------------------
# Latency testing
# -----------------------------


def _run_latency_load(
    *,
    api_base_url: str,
    sigv4_auth: Any,
    calls: List[Tuple[str, Dict[str, Any]]],
    n_per_endpoint: int,
    concurrency: int,
    warmup_per_endpoint: int = 5,
) -> List[Dict[str, Any]]:
    """
    Run API latency battery with concurrent requests.

    Each call is (endpoint_name, payload) where payload includes "_path" key.

    Fail-fast behavior:
    - No retries
    - Any timeout/HTTP error raises immediately with context

    Args:
        api_base_url: Base API URL
        sigv4_auth: AWS SigV4 auth object
        calls: List of (name, payload) tuples with "_path" key
        n_per_endpoint: Number of requests per endpoint
        concurrency: Number of concurrent threads
        warmup_per_endpoint: Number of warmup requests per endpoint

    Returns:
        List of latency statistics per endpoint
    """
    results: List[Dict[str, Any]] = []
    http_timeout_s = float(os.environ.get("PHEBEE_EVAL_HTTP_TIMEOUT_S", "30"))

    # Warmup phase (serial) - reduces cold-start noise
    print(f"\nWarming up endpoints ({warmup_per_endpoint} requests each)...")
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
                    f"[WARMUP_FAILED] endpoint={name} path={path} warmup_i={wi} "
                    f"timeout_s={http_timeout_s} payload_keys={sorted(list(payload.keys()))} err={e}"
                ) from e
        payload["_path"] = path  # restore

    print("Warmup complete.\n")

    def _one_call(name: str, path: str, payload: Dict[str, Any], i: int) -> float:
        """Execute single timed API call."""
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

    # Execute load test for each endpoint
    for name, payload in calls:
        path = payload["_path"]
        print(f"Testing endpoint: {name} ({n_per_endpoint} requests, {concurrency} concurrent)...")

        latencies: List[float] = []
        payload_copy = dict(payload)
        payload_copy.pop("_path", None)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
            futs = [ex.submit(_one_call, name, path, payload_copy, i) for i in range(n_per_endpoint)]
            for f in concurrent.futures.as_completed(futs):
                # If any worker raised, this will raise and abort
                latencies.append(float(f.result()))

        latencies.sort()
        results.append(
            {
                "endpoint": name,
                "n": len(latencies),
                "min_ms": round(min(latencies), 2) if latencies else None,
                "max_ms": round(max(latencies), 2) if latencies else None,
                "avg_ms": round(float(statistics.fmean(latencies)), 2) if latencies else None,
                "p50_ms": round(_pctl(latencies, 50), 2),
                "p95_ms": round(_pctl(latencies, 95), 2),
                "p99_ms": round(_pctl(latencies, 99), 2),
            }
        )

        r = results[-1]
        print(f"  → p50={r['p50_ms']:.2f}ms, p95={r['p95_ms']:.2f}ms, p99={r['p99_ms']:.2f}ms\n")

    return results


def _write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    """Write CSV artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})


# -----------------------------
# Main API latency test
# -----------------------------


@pytest.mark.perf
def test_api_latency(
    api_base_url: str,
    sigv4_auth: Any,
    test_project_id: str,
    synthetic_dataset,  # GeneratedDataset fixture from conftest.py
):
    """
    Measure API query latency under load for PheBee (Table 4).

    This test:
    1. Discovers subjects in the project for query targets
    2. Constructs representative query workload (basic, hierarchy, qualified, etc.)
    3. Executes concurrent requests with warmup phase
    4. Measures p50/p95/p99 latencies for each endpoint
    5. Outputs CSV and JSON artifacts for manuscript Table 4

    Prerequisites:
    - Project must already have data (run test_import_performance.py first)
    - Sufficient subjects for meaningful queries
    """
    if os.environ.get("PHEBEE_EVAL_SCALE") != "1":
        pytest.skip("Set PHEBEE_EVAL_SCALE=1 to run API latency test.")

    # Configuration
    latency_n = _env_int("PHEBEE_EVAL_LATENCY_N", 500)
    concurrency = _env_int("PHEBEE_EVAL_CONCURRENCY", 25)
    run_id = f"latency-perf-{uuid.uuid4().hex[:10]}"

    print(f"\n{'='*80}")
    print(f"PheBee API Latency Test - Table 4")
    print(f"{'='*80}")
    print(f"Run ID: {run_id}")
    print(f"Project ID: {test_project_id}")
    print(f"Requests per endpoint: {latency_n}")
    print(f"Concurrency: {concurrency}")

    # Get anchor terms from dataset for constructing queries
    anchor_terms = synthetic_dataset.anchor_terms
    parent_term = synthetic_dataset.parent_term

    print(f"\nQuery anchor terms:")
    print(f"  Cardiovascular: {anchor_terms[0]}")
    print(f"  Neurological: {anchor_terms[1]}")
    print(f"  Parent (hierarchy): {parent_term}")

    # Discover subjects for queries (with retry for eventual consistency)
    print(f"\nDiscovering subjects in project...")
    print(f"  Using project_id: {test_project_id}")
    subjects = []
    max_retries = 12  # Increased from 5
    retry_delay_s = 30  # Increased from 10

    for attempt in range(max_retries):
        if attempt > 0:
            print(f"  Retry {attempt}/{max_retries-1} after {retry_delay_s}s delay...")
            time.sleep(retry_delay_s)

        try:
            # Try unfiltered query first (most permissive)
            print(f"  Attempt {attempt+1}: Querying for any subjects in project...")
            q_unfiltered = api_post(
                api_base_url,
                "/subjects/query",
                {"project_id": test_project_id, "limit": 200},
                sigv4_auth,
            )
            subjects = q_unfiltered.get("body", [])

            print(f"  → Unfiltered query returned {len(subjects)} subjects")

            # If unfiltered worked, also try with anchor term to see if it's a term-specific issue
            if subjects:
                print(f"  Testing anchor term query: {anchor_terms[0]}")
                q_anchor = api_post(
                    api_base_url,
                    "/subjects/query",
                    {
                        "project_id": test_project_id,
                        "term_iri": anchor_terms[0],
                        "include_child_terms": False,
                        "include_qualified": True,
                        "limit": 200,
                    },
                    sigv4_auth,
                )
                anchor_subjects = q_anchor.get("body", [])
                print(f"  → Anchor term query returned {len(anchor_subjects)} subjects")
                break

        except Exception as e:
            print(f"  Query failed (attempt {attempt+1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                raise

    if not subjects:
        print(f"\nDEBUG: No subjects found after {max_retries} retries over {max_retries * retry_delay_s}s")
        print(f"DEBUG: project_id = {test_project_id}")
        print(f"DEBUG: api_base_url = {api_base_url}")
        pytest.skip(
            f"No subjects available for latency testing after {max_retries} retries "
            f"({max_retries * retry_delay_s}s total wait time). "
            f"Import data may need more time to be indexed, or there may be an issue with data propagation."
        )

    print(f"  Found {len(subjects)} subjects for testing")

    # Pick sample subject and get details for term-specific queries
    sample_subject = subjects[0]
    print(f"\nSample subject: {sample_subject['project_subject_iri']}")

    subj_summary = api_post(
        api_base_url,
        "/subject",
        {"project_subject_iri": sample_subject["project_subject_iri"]},
        sigv4_auth,
    )
    terms = subj_summary.get("terms", []) or []

    # Pick term and qualifiers for term-specific queries
    term_for_query = terms[0]["term_iri"] if terms else anchor_terms[0]
    qualifiers_for_query = terms[0].get("qualifiers", []) if terms else []
    subject_id = sample_subject["subject_iri"].split("/")[-1]

    print(f"  Term for query: {term_for_query}")
    print(f"  Qualifiers: {qualifiers_for_query or 'none'}")

    # Term source metadata (optional filtering)
    stats = synthetic_dataset.stats
    ts = stats.get("term_source", {})
    term_source = ts.get("source")
    term_source_version = ts.get("version")

    # Define representative query workload (Table 4)
    print(f"\nConstructing query workload...")
    calls: List[Tuple[str, Dict[str, Any]]] = [
        (
            "basic_subjects_query",
            {
                "_path": "/subjects/query",
                "project_id": test_project_id,
                "limit": 50,
            },
        ),
        (
            "individual_subject",
            {
                "_path": "/subject",
                "project_subject_iri": sample_subject["project_subject_iri"],
            },
        ),
        (
            "hierarchy_query",
            {
                "_path": "/subjects/query",
                "project_id": test_project_id,
                "term_iri": parent_term,
                "include_child_terms": True,
                "include_qualified": True,
                "limit": 50,
            },
        ),
        (
            "qualified_filtering",
            {
                "_path": "/subjects/query",
                "project_id": test_project_id,
                "term_iri": anchor_terms[0],
                "include_child_terms": False,
                "include_qualified": False,
                "limit": 50,
            },
        ),
        (
            "specific_phenotype",
            {
                "_path": "/subjects/query",
                "project_id": test_project_id,
                "term_iri": term_for_query,
                "include_child_terms": False,
                "include_qualified": True,
                "limit": 50,
            },
        ),
        (
            "cardiac_cohort",
            {
                "_path": "/subjects/query",
                "project_id": test_project_id,
                "term_iri": anchor_terms[0],
                "include_child_terms": False,
                "include_qualified": True,
                "limit": 50,
            },
        ),
        (
            "neuro_cohort",
            {
                "_path": "/subjects/query",
                "project_id": test_project_id,
                "term_iri": anchor_terms[1],
                "include_child_terms": False,
                "include_qualified": True,
                "limit": 50,
            },
        ),
        (
            "paginated_large_cohort",
            {
                "_path": "/subjects/query",
                "project_id": test_project_id,
                "limit": 50,
                "cursor": None,
            },
        ),
        (
            "subject_term_info",
            {
                "_path": "/subject/term-info",
                "subject_id": subject_id,
                "term_iri": term_for_query,
                "qualifiers": qualifiers_for_query,
            },
        ),
    ]

    # Optional version-specific query (if API supports it)
    if term_source and term_source_version:
        calls.append(
            (
                "version_specific_query",
                {
                    "_path": "/subjects/query",
                    "project_id": test_project_id,
                    "term_iri": anchor_terms[0],
                    "include_child_terms": True,
                    "include_qualified": True,
                    "term_source": term_source,
                    "term_source_version": term_source_version,
                    "limit": 50,
                },
            )
        )

    print(f"  {len(calls)} endpoints to test")

    # Execute latency load test
    print(f"\n{'='*80}")
    print(f"Running Latency Load Test")
    print(f"{'='*80}")

    latency_rows = _run_latency_load(
        api_base_url=api_base_url,
        sigv4_auth=sigv4_auth,
        calls=calls,
        n_per_endpoint=latency_n,
        concurrency=concurrency,
        warmup_per_endpoint=5,
    )

    # Performance summary
    p95s = [r["p95_ms"] for r in latency_rows if r.get("p95_ms") is not None]
    perf_summary = {
        "avg_p95_ms": round(float(statistics.fmean(p95s)), 2) if p95s else None,
        "fastest_p95_ms": round(float(min(p95s)), 2) if p95s else None,
        "slowest_p95_ms": round(float(max(p95s)), 2) if p95s else None,
    }

    print(f"\n{'='*80}")
    print(f"Latency Test Complete!")
    print(f"{'='*80}")
    print(f"Performance Summary:")
    print(f"  Average p95 latency: {perf_summary['avg_p95_ms']:.2f}ms")
    print(f"  Fastest p95: {perf_summary['fastest_p95_ms']:.2f}ms")
    print(f"  Slowest p95: {perf_summary['slowest_p95_ms']:.2f}ms")

    # Full JSON output
    out_json = {
        "run_id": run_id,
        "project_id": test_project_id,
        "load_testing": {
            "concurrency": concurrency,
            "requests_per_endpoint": latency_n,
            "total_api_calls": latency_n * len(calls),
            "n_endpoints": len(calls),
        },
        "performance_summary": perf_summary,
        "latency_results": latency_rows,
    }

    # Write artifacts
    if os.environ.get("PHEBEE_EVAL_WRITE_ARTIFACTS", "1") == "1":
        base = Path("/tmp/phebee-eval-artifacts") / run_id
        base.mkdir(parents=True, exist_ok=True)

        table4_path = base / "table4_latency.csv"
        json_path = base / "latency_run.json"

        _write_csv(
            table4_path,
            latency_rows,
            fieldnames=["endpoint", "n", "min_ms", "max_ms", "avg_ms", "p50_ms", "p95_ms", "p99_ms"],
        )

        json_path.write_text(json.dumps(out_json, indent=2), encoding="utf-8")

        print(f"\n{'='*80}")
        print(f"Artifacts Written")
        print(f"{'='*80}")
        print(f"  Table 4 CSV: {table4_path}")
        print(f"  Full JSON:   {json_path}")
        print()

    # Print JSON summary
    print(f"\n[LATENCY_RESULT_JSON]\n{json.dumps(out_json, indent=2)}\n")
