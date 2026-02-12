"""
Cache vs Neptune Performance Comparison at Scale

Tests cache performance improvements over Neptune queries at scale (1000+ subjects).
Verifies that cache implementation provides significant latency reduction for
cohort queries compared to Neptune graph traversal.

Key Metrics:
- Query latency (p50/p95/p99) for cache vs Neptune
- Speedup factor at various dataset sizes
- Pagination performance
- Term-filtered query performance

Environment Variables:
- PHEBEE_PERF_SCALE_N (default: 1000): Number of subjects to generate
- PHEBEE_PERF_SKIP (default: 0): Set to 1 to skip performance tests

Markers:
- @pytest.mark.perf: Can be run separately from integration tests
- @pytest.mark.integration: Requires deployed stack

Output:
- Console output with timing comparisons and speedup factors
"""

from __future__ import annotations

import base64
import gzip
import json
import os
import random
import statistics
import time
import uuid
from typing import Any, Dict, List

import boto3
import pytest

# Mark all tests in this module
pytestmark = [pytest.mark.integration, pytest.mark.perf]


def _env_int(name: str, default: int) -> int:
    """Get integer from environment variable or use default."""
    v = os.environ.get(name)
    return default if v is None else int(v)


def _pctl(values: List[float], p: float) -> float:
    """Calculate percentile with linear interpolation."""
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


# HPO terms for test data generation
COMMON_HPO_TERMS = [
    "http://purl.obolibrary.org/obo/HP_0000152",  # Abnormality of head or neck
    "http://purl.obolibrary.org/obo/HP_0000234",  # Abnormality of the head
    "http://purl.obolibrary.org/obo/HP_0000707",  # Abnormality of the nervous system
    "http://purl.obolibrary.org/obo/HP_0001507",  # Growth abnormality
    "http://purl.obolibrary.org/obo/HP_0001871",  # Abnormality of blood and blood-forming tissues
    "http://purl.obolibrary.org/obo/HP_0002664",  # Neoplasm
    "http://purl.obolibrary.org/obo/HP_0003011",  # Abnormality of the musculature
    "http://purl.obolibrary.org/obo/HP_0025031",  # Abnormality of the digestive system
]


@pytest.fixture(scope="module")
def perf_project_id(physical_resources):
    """
    Create a project for performance testing that persists for the entire test module.

    Similar to test_project_id but module-scoped so the project and data persist
    across all performance tests.
    """
    lambda_client = boto3.client("lambda")
    create_project_function = physical_resources["CreateProjectFunction"]
    remove_project_function = physical_resources["RemoveProjectFunction"]

    project_id = f"perf_test_{uuid.uuid4().hex[:8]}"

    # Create project
    create_payload = {
        "body": json.dumps({
            "project_id": project_id,
            "project_label": f"Performance Test {project_id}"
        })
    }

    response = lambda_client.invoke(
        FunctionName=create_project_function,
        InvocationType="RequestResponse",
        Payload=json.dumps(create_payload)
    )

    response_payload = json.loads(response["Payload"].read())
    if response_payload.get("statusCode") != 200:
        raise RuntimeError(f"Failed to create project {project_id}: {response_payload}")

    body = json.loads(response_payload["body"])
    if not body.get("project_created"):
        raise RuntimeError(f"Project creation returned false: {body}")

    print(f"Created performance test project: {project_id}")

    yield project_id

    # Cleanup
    print(f"\n=== Cleaning up performance test project {project_id} ===")
    delete_payload = {
        "body": json.dumps({
            "project_id": project_id
        })
    }

    response = lambda_client.invoke(
        FunctionName=remove_project_function,
        InvocationType="RequestResponse",
        Payload=json.dumps(delete_payload)
    )

    response_payload = json.loads(response["Payload"].read())
    if response_payload.get("statusCode") != 200:
        print(f"WARNING: Failed to delete project {project_id}: {response_payload}")
    else:
        print(f"✓ Project {project_id} deleted")


@pytest.fixture(scope="module")
def performance_test_data(physical_resources, perf_project_id):
    """
    Generate large dataset for performance testing.

    Creates N subjects (default 1000) with term links for cache vs Neptune comparison.
    Uses the perf_project_id fixture for project creation/cleanup.
    """
    skip_perf = _env_int("PHEBEE_PERF_SKIP", 0)
    if skip_perf:
        pytest.skip("Performance tests skipped (PHEBEE_PERF_SKIP=1)")

    n_subjects = _env_int("PHEBEE_PERF_SCALE_N", 1000)

    # Get Lambda function names from physical_resources
    create_subject_function = physical_resources["CreateSubjectFunction"]
    create_term_link_function = physical_resources["CreateTermLinkFunction"]

    lambda_client = boto3.client("lambda")

    print(f"\n=== Generating Performance Test Dataset ===")
    print(f"Project: {perf_project_id}")
    print(f"Subjects: {n_subjects}")
    print(f"This may take a few minutes...")

    start_time = time.time()

    # Create subjects with term links
    subject_ids = []
    rng = random.Random(42)  # Fixed seed for reproducibility

    for i in range(n_subjects):
        subject_id = f"perf_subj_{i:06d}"

        # Create subject
        create_subject_payload = {
            "body": json.dumps({
                "project_id": perf_project_id,
                "project_subject_id": subject_id
            })
        }

        response = lambda_client.invoke(
            FunctionName=create_subject_function,
            InvocationType="RequestResponse",
            Payload=json.dumps(create_subject_payload)
        )

        response_payload = json.loads(response["Payload"].read())

        if response_payload.get("statusCode") != 200:
            raise RuntimeError(f"Failed to create subject {subject_id}: {response_payload}")

        body = json.loads(response_payload["body"])
        subject_iri = body["subject"]["iri"]
        subject_ids.append((subject_id, subject_iri))

        # Add 2-5 term links per subject with varied terms
        n_terms = rng.randint(2, 5)
        selected_terms = rng.sample(COMMON_HPO_TERMS, n_terms)

        for term_iri in selected_terms:
            # 20% chance of having a qualifier (negated/hypothetical/family)
            qualifiers = []
            if rng.random() < 0.2:
                qualifier_type = rng.choice(["negated", "hypothetical", "family"])
                qualifiers.append(f"http://ods.nationwidechildrens.org/phebee/qualifier/{qualifier_type}")

            create_term_link_payload = {
                "body": json.dumps({
                    "subject_id": subject_iri.split("/")[-1],
                    "term_iri": term_iri,
                    "creator_id": "perf_test_user",
                    "qualifiers": qualifiers
                })
            }

            response = lambda_client.invoke(
                FunctionName=create_term_link_function,
                InvocationType="RequestResponse",
                Payload=json.dumps(create_term_link_payload)
            )

            response_payload = json.loads(response["Payload"].read())

            if response_payload.get("statusCode") != 200:
                raise RuntimeError(f"Failed to create term link for {subject_id}: {response_payload}")

        # Progress indicator every 100 subjects
        if (i + 1) % 100 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            remaining = (n_subjects - i - 1) / rate if rate > 0 else 0
            print(f"  Created {i + 1}/{n_subjects} subjects ({rate:.1f}/sec, ~{remaining:.0f}s remaining)")

    elapsed = time.time() - start_time
    print(f"✓ Dataset created in {elapsed:.1f}s ({n_subjects/elapsed:.1f} subjects/sec)")

    return {
        "project_id": perf_project_id,
        "subject_ids": subject_ids,
        "n_subjects": n_subjects,
    }
    # Project cleanup is handled by perf_project_id fixture


def query_subjects_with_flag(physical_resources, project_id: str, use_cache: bool, **kwargs) -> tuple[Dict[str, Any], float]:
    """
    Query subjects via Lambda with cache flag, return result and latency.

    Returns:
        (result_dict, latency_seconds)
    """
    get_subjects_function = physical_resources["GetSubjectsPhenotypesFunction"]
    lambda_client = boto3.client("lambda")

    payload = {
        "body": json.dumps({
            "project_id": project_id,
            "_use_cache": use_cache,
            **kwargs
        })
    }

    start = time.time()
    response = lambda_client.invoke(
        FunctionName=get_subjects_function,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload)
    )
    latency = time.time() - start

    response_payload = json.loads(response["Payload"].read())

    if response_payload.get("statusCode") != 200:
        raise RuntimeError(f"Query failed: {response_payload}")

    # Handle base64 + gzip encoded response
    if response_payload.get("isBase64Encoded"):
        compressed_body = base64.b64decode(response_payload["body"])
        body_str = gzip.decompress(compressed_body).decode('utf-8')
        body = json.loads(body_str)
    else:
        body = json.loads(response_payload["body"])

    return body, latency


@pytest.mark.perf
def test_cache_vs_neptune_full_project_query_at_scale(physical_resources, performance_test_data):
    """
    Compare cache vs Neptune for full project query at scale (no term filter).

    This is the baseline query: fetch all subjects in a project.
    Expected: Cache should be 5-10x faster at 1000+ subjects.
    """
    project_id = performance_test_data["project_id"]
    n_subjects = performance_test_data["n_subjects"]

    print(f"\n=== Performance Test: Full Project Query ({n_subjects} subjects) ===")

    # Warmup queries (not timed)
    print("Warming up...")
    query_subjects_with_flag(physical_resources, project_id, use_cache=True, limit=10)
    query_subjects_with_flag(physical_resources, project_id, use_cache=False, limit=10)

    # Run multiple queries to get stable measurements
    n_runs = 5
    neptune_latencies = []
    cache_latencies = []

    print(f"\nRunning {n_runs} queries with each backend...")

    for i in range(n_runs):
        # Neptune query
        print(f"  Run {i+1}/{n_runs}: Neptune...", end=" ", flush=True)
        neptune_result, neptune_latency = query_subjects_with_flag(
            physical_resources,
            project_id,
            use_cache=False,
            limit=n_subjects
        )
        neptune_latencies.append(neptune_latency)
        print(f"{neptune_latency*1000:.0f}ms")

        # Cache query
        print(f"  Run {i+1}/{n_runs}: Cache...", end=" ", flush=True)
        cache_result, cache_latency = query_subjects_with_flag(
            physical_resources,
            project_id,
            use_cache=True,
            limit=n_subjects
        )
        cache_latencies.append(cache_latency)
        print(f"{cache_latency*1000:.0f}ms")

        # Verify same result count
        assert neptune_result["n_subjects"] == cache_result["n_subjects"], \
            f"Different result counts: Neptune={neptune_result['n_subjects']}, Cache={cache_result['n_subjects']}"

    # Calculate statistics
    print(f"\n=== Performance Results ({n_subjects} subjects) ===")
    print(f"Neptune latency:")
    print(f"  p50: {_pctl(neptune_latencies, 50)*1000:.0f}ms")
    print(f"  p95: {_pctl(neptune_latencies, 95)*1000:.0f}ms")
    print(f"  p99: {_pctl(neptune_latencies, 99)*1000:.0f}ms")
    print(f"  avg: {statistics.mean(neptune_latencies)*1000:.0f}ms")
    print(f"  min: {min(neptune_latencies)*1000:.0f}ms")
    print(f"  max: {max(neptune_latencies)*1000:.0f}ms")

    print(f"\nCache latency:")
    print(f"  p50: {_pctl(cache_latencies, 50)*1000:.0f}ms")
    print(f"  p95: {_pctl(cache_latencies, 95)*1000:.0f}ms")
    print(f"  p99: {_pctl(cache_latencies, 99)*1000:.0f}ms")
    print(f"  avg: {statistics.mean(cache_latencies)*1000:.0f}ms")
    print(f"  min: {min(cache_latencies)*1000:.0f}ms")
    print(f"  max: {max(cache_latencies)*1000:.0f}ms")

    speedup = statistics.mean(neptune_latencies) / statistics.mean(cache_latencies)
    print(f"\nSpeedup: {speedup:.1f}x faster with cache")

    # Assert cache is faster (should be significantly faster at scale)
    assert statistics.mean(cache_latencies) < statistics.mean(neptune_latencies), \
        "Cache should be faster than Neptune at scale"

    print(f"✓ Cache is {speedup:.1f}x faster than Neptune for full project query at scale")


@pytest.mark.perf
def test_cache_vs_neptune_term_filtered_query_at_scale(physical_resources, performance_test_data):
    """
    Compare cache vs Neptune for term-filtered query at scale.

    This tests hierarchy expansion: fetch all subjects with term HP:0000152 or descendants.
    Expected: Cache should be 5-10x faster due to optimized ancestor_path filtering.
    """
    project_id = performance_test_data["project_id"]
    n_subjects = performance_test_data["n_subjects"]
    term_iri = "http://purl.obolibrary.org/obo/HP_0000152"  # Abnormality of head or neck

    print(f"\n=== Performance Test: Term-Filtered Query ({n_subjects} subjects) ===")
    print(f"Filter: {term_iri} (with hierarchy)")

    # Warmup
    print("Warming up...")
    query_subjects_with_flag(
        physical_resources, project_id, use_cache=True,
        term_iri=term_iri, include_child_terms=True, limit=10
    )
    query_subjects_with_flag(
        physical_resources, project_id, use_cache=False,
        term_iri=term_iri, include_child_terms=True, limit=10
    )

    # Run multiple queries
    n_runs = 5
    neptune_latencies = []
    cache_latencies = []

    print(f"\nRunning {n_runs} queries with each backend...")

    for i in range(n_runs):
        # Neptune query
        print(f"  Run {i+1}/{n_runs}: Neptune...", end=" ", flush=True)
        neptune_result, neptune_latency = query_subjects_with_flag(
            physical_resources,
            project_id,
            use_cache=False,
            term_iri=term_iri,
            include_child_terms=True,
            limit=n_subjects
        )
        neptune_latencies.append(neptune_latency)
        print(f"{neptune_latency*1000:.0f}ms ({neptune_result['n_subjects']} subjects)")

        # Cache query
        print(f"  Run {i+1}/{n_runs}: Cache...", end=" ", flush=True)
        cache_result, cache_latency = query_subjects_with_flag(
            physical_resources,
            project_id,
            use_cache=True,
            term_iri=term_iri,
            include_child_terms=True,
            limit=n_subjects
        )
        cache_latencies.append(cache_latency)
        print(f"{cache_latency*1000:.0f}ms ({cache_result['n_subjects']} subjects)")

        # Verify same result count
        assert neptune_result["n_subjects"] == cache_result["n_subjects"], \
            f"Different result counts: Neptune={neptune_result['n_subjects']}, Cache={cache_result['n_subjects']}"

    # Calculate statistics
    print(f"\n=== Performance Results (term-filtered, {neptune_result['n_subjects']} matching subjects) ===")
    print(f"Neptune latency:")
    print(f"  p50: {_pctl(neptune_latencies, 50)*1000:.0f}ms")
    print(f"  p95: {_pctl(neptune_latencies, 95)*1000:.0f}ms")
    print(f"  avg: {statistics.mean(neptune_latencies)*1000:.0f}ms")

    print(f"\nCache latency:")
    print(f"  p50: {_pctl(cache_latencies, 50)*1000:.0f}ms")
    print(f"  p95: {_pctl(cache_latencies, 95)*1000:.0f}ms")
    print(f"  avg: {statistics.mean(cache_latencies)*1000:.0f}ms")

    speedup = statistics.mean(neptune_latencies) / statistics.mean(cache_latencies)
    print(f"\nSpeedup: {speedup:.1f}x faster with cache")

    # Assert cache is faster
    assert statistics.mean(cache_latencies) < statistics.mean(neptune_latencies), \
        "Cache should be faster than Neptune for term-filtered queries"

    print(f"✓ Cache is {speedup:.1f}x faster than Neptune for term-filtered query at scale")


@pytest.mark.perf
def test_cache_pagination_performance_at_scale(physical_resources, performance_test_data):
    """
    Test cache pagination performance at scale.

    Verifies that paginating through 1000 subjects is efficient with the cache.
    Tests with small page size (50 subjects) to simulate real-world pagination.
    """
    project_id = performance_test_data["project_id"]
    n_subjects = performance_test_data["n_subjects"]
    page_size = 50

    print(f"\n=== Performance Test: Pagination ({n_subjects} subjects, page_size={page_size}) ===")

    # Fetch all pages and measure total time
    start_time = time.time()

    all_subjects = []
    cursor = None
    page_num = 0
    page_latencies = []

    while True:
        page_num += 1
        page_start = time.time()

        result, _ = query_subjects_with_flag(
            physical_resources,
            project_id,
            use_cache=True,
            limit=page_size,
            cursor=cursor
        )

        page_latency = time.time() - page_start
        page_latencies.append(page_latency)

        subjects = result["body"]
        all_subjects.extend(subjects)

        pagination = result["pagination"]

        if page_num % 5 == 0 or not pagination["has_more"]:
            print(f"  Page {page_num}: {len(subjects)} subjects, {page_latency*1000:.0f}ms")

        if not pagination["has_more"]:
            break

        cursor = pagination["next_cursor"]

    total_time = time.time() - start_time

    print(f"\n=== Pagination Performance Results ===")
    print(f"Total subjects fetched: {len(all_subjects)}")
    print(f"Total pages: {page_num}")
    print(f"Total time: {total_time:.2f}s")
    print(f"Throughput: {len(all_subjects)/total_time:.1f} subjects/sec")

    print(f"\nPer-page latency:")
    print(f"  p50: {_pctl(page_latencies, 50)*1000:.0f}ms")
    print(f"  p95: {_pctl(page_latencies, 95)*1000:.0f}ms")
    print(f"  avg: {statistics.mean(page_latencies)*1000:.0f}ms")
    print(f"  max: {max(page_latencies)*1000:.0f}ms")

    # Verify we got all subjects
    assert len(all_subjects) == n_subjects, \
        f"Expected {n_subjects} subjects, got {len(all_subjects)}"

    # Verify no duplicates
    subject_iris = [s["subject_iri"] for s in all_subjects]
    assert len(subject_iris) == len(set(subject_iris)), \
        "Found duplicate subjects in pagination"

    print(f"✓ Successfully paginated through {n_subjects} subjects with consistent performance")
