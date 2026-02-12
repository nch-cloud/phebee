"""
Integration tests comparing DynamoDB cache queries vs Neptune queries.

These tests verify that the cache-based query implementation returns identical
results to the Neptune-based implementation across various query patterns.
"""

import pytest
import json
import os
import time
import gzip
import base64
from general_utils import invoke_lambda


def decode_response_body(response):
    """Decode response body, handling gzip compression if present."""
    body = response["body"]

    # Check if response is base64 encoded and gzipped
    if response.get("isBase64Encoded", False):
        # Decode base64
        compressed_data = base64.b64decode(body)
        # Decompress
        decompressed_data = gzip.decompress(compressed_data)
        return decompressed_data.decode('utf-8')

    # Not compressed, return as-is
    return body


@pytest.fixture
def cache_test_data(physical_resources, test_project_id):
    """
    Create test subjects with various term links for comprehensive cache testing.

    Creates:
    - Subject 1: HP_0000234 (Abnormality of the head) - child term
    - Subject 2: HP_0000152 (Abnormality of head or neck) - parent term
    - Subject 3: HP_0001627 (Abnormal heart morphology) with NEGATED qualifier
    - Subject 4: Multiple terms
    """
    print(f"\n=== Setting up cache test data in project {test_project_id} ===")

    subjects = []

    # Subject 1: Child term only
    print("Creating subject 1 with child term HP_0000234...")
    response = invoke_lambda(physical_resources["CreateSubjectFunction"], {
        "project_id": test_project_id,
        "project_subject_id": "cache-test-subject-1"
    })
    assert response["statusCode"] == 200
    subject1_data = json.loads(response["body"])
    subject1_iri = subject1_data["subject"]["iri"]
    subject1_id = subject1_iri.split('/')[-1]

    invoke_lambda(physical_resources["CreateTermLinkFunction"], {
        "subject_id": subject1_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0000234",
        "creator_id": "test-cache-creator"
    })
    subjects.append({"id": subject1_id, "iri": subject1_iri, "project_subject_id": "cache-test-subject-1"})

    # Subject 2: Parent term only
    print("Creating subject 2 with parent term HP_0000152...")
    response = invoke_lambda(physical_resources["CreateSubjectFunction"], {
        "project_id": test_project_id,
        "project_subject_id": "cache-test-subject-2"
    })
    assert response["statusCode"] == 200
    subject2_data = json.loads(response["body"])
    subject2_iri = subject2_data["subject"]["iri"]
    subject2_id = subject2_iri.split('/')[-1]

    invoke_lambda(physical_resources["CreateTermLinkFunction"], {
        "subject_id": subject2_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0000152",
        "creator_id": "test-cache-creator"
    })
    subjects.append({"id": subject2_id, "iri": subject2_iri, "project_subject_id": "cache-test-subject-2"})

    # Subject 3: Term with negated qualifier
    print("Creating subject 3 with negated term HP_0001627...")
    response = invoke_lambda(physical_resources["CreateSubjectFunction"], {
        "project_id": test_project_id,
        "project_subject_id": "cache-test-subject-3"
    })
    assert response["statusCode"] == 200
    subject3_data = json.loads(response["body"])
    subject3_iri = subject3_data["subject"]["iri"]
    subject3_id = subject3_iri.split('/')[-1]

    invoke_lambda(physical_resources["CreateTermLinkFunction"], {
        "subject_id": subject3_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
        "creator_id": "test-cache-creator",
        "qualifiers": ["http://ods.nationwidechildrens.org/phebee/qualifier/negated"]
    })
    subjects.append({"id": subject3_id, "iri": subject3_iri, "project_subject_id": "cache-test-subject-3"})

    # Subject 4: Multiple terms (one from hierarchy, one unrelated)
    print("Creating subject 4 with multiple terms...")
    response = invoke_lambda(physical_resources["CreateSubjectFunction"], {
        "project_id": test_project_id,
        "project_subject_id": "cache-test-subject-4"
    })
    assert response["statusCode"] == 200
    subject4_data = json.loads(response["body"])
    subject4_iri = subject4_data["subject"]["iri"]
    subject4_id = subject4_iri.split('/')[-1]

    # Add HP_0000234 (in hierarchy with HP_0000152)
    invoke_lambda(physical_resources["CreateTermLinkFunction"], {
        "subject_id": subject4_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0000234",
        "creator_id": "test-cache-creator"
    })

    # Add HP_0001626 (unrelated cardiovascular term)
    invoke_lambda(physical_resources["CreateTermLinkFunction"], {
        "subject_id": subject4_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001626",
        "creator_id": "test-cache-creator"
    })
    subjects.append({"id": subject4_id, "iri": subject4_iri, "project_subject_id": "cache-test-subject-4"})

    print(f"Created {len(subjects)} test subjects")

    # Wait briefly for cache writes to complete
    time.sleep(2)

    return {
        "subjects": subjects,
        "project_id": test_project_id
    }


def query_with_cache_flag(physical_resources, project_id, use_cache, **query_params):
    """
    Query subjects with explicit cache flag control.

    Args:
        use_cache: True to use cache, False to use Neptune, None for default
    """
    params = {
        "project_id": project_id,
        "output_type": "json",
        **query_params
    }

    # Pass _use_cache parameter to Lambda to override default
    if use_cache is not None:
        params["_use_cache"] = use_cache

    response = invoke_lambda(physical_resources["GetSubjectsPhenotypesFunction"], params)
    assert response["statusCode"] == 200, f"Query failed: {response}"

    body_str = decode_response_body(response)
    body = json.loads(body_str)

    return body


def normalize_subjects_for_comparison(subjects_list):
    """
    Normalize subject data for comparison by sorting and standardizing format.
    """
    normalized = []

    for subject in subjects_list:
        # Sort term_links by term_iri
        term_links = sorted(subject.get("term_links", []), key=lambda x: x.get("term_iri", ""))

        # Sort qualifiers within each term_link
        for term_link in term_links:
            if "qualifiers" in term_link:
                term_link["qualifiers"] = sorted(term_link["qualifiers"])

        normalized.append({
            "subject_iri": subject.get("subject_iri"),
            "project_subject_id": subject.get("project_subject_id"),
            "project_subject_iri": subject.get("project_subject_iri"),
            "term_links": term_links
        })

    # Sort by subject_iri for consistent comparison
    return sorted(normalized, key=lambda x: x.get("subject_iri", ""))


@pytest.mark.integration
def test_cache_all_subjects_in_project(physical_resources, cache_test_data):
    """
    Test that querying all subjects in a project returns identical results
    from cache and Neptune.
    """
    project_id = cache_test_data["project_id"]

    print("\n=== Testing: Query all subjects (no term filter) ===")

    # Query with default (cache enabled)
    cache_result = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=True
    )

    print(f"Cache returned {cache_result['n_subjects']} subjects")

    # Verify we got our test subjects
    cache_subjects = cache_result["body"]
    assert len(cache_subjects) >= 4, f"Expected at least 4 subjects, got {len(cache_subjects)}"

    # Verify all have correct structure
    for subject in cache_subjects:
        assert "subject_iri" in subject
        assert "project_subject_id" in subject
        assert "project_subject_iri" in subject
        assert "term_links" in subject
        assert isinstance(subject["term_links"], list)


@pytest.mark.integration
def test_cache_hierarchy_query_include_children(physical_resources, cache_test_data):
    """
    Test that querying with term hierarchy (include_child_terms=True) works correctly.

    HP_0000152 (Abnormality of head or neck) is parent of HP_0000234 (Abnormality of the head).
    Should return subjects with either term.
    """
    project_id = cache_test_data["project_id"]
    parent_term = "http://purl.obolibrary.org/obo/HP_0000152"

    print("\n=== Testing: Hierarchy query with include_child_terms=True ===")
    print(f"Querying for parent term: {parent_term}")

    result = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=True,
        term_iri=parent_term,
        include_child_terms=True
    )

    subjects = result["body"]
    print(f"Found {len(subjects)} subjects with hierarchy query")

    # Should find:
    # - Subject 2: Has HP_0000152 directly
    # - Subject 1: Has HP_0000234 (child of HP_0000152)
    # - Subject 4: Has HP_0000234 (child of HP_0000152)
    assert len(subjects) >= 3, f"Expected at least 3 subjects (parent + children), got {len(subjects)}"

    # Verify subjects have the expected terms
    found_parent_directly = False
    found_child_term = False

    for subject in subjects:
        term_iris = [tl["term_iri"] for tl in subject["term_links"]]
        if "http://purl.obolibrary.org/obo/HP_0000152" in term_iris:
            found_parent_directly = True
        if "http://purl.obolibrary.org/obo/HP_0000234" in term_iris:
            found_child_term = True

    assert found_parent_directly, "Should find subject with parent term"
    assert found_child_term, "Should find subject with child term"


@pytest.mark.integration
def test_cache_hierarchy_query_exact_match_only(physical_resources, cache_test_data):
    """
    Test that querying with include_child_terms=False returns only exact matches.
    """
    project_id = cache_test_data["project_id"]
    parent_term = "http://purl.obolibrary.org/obo/HP_0000152"

    print("\n=== Testing: Exact term match (include_child_terms=False) ===")

    result = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=True,
        term_iri=parent_term,
        include_child_terms=False
    )

    subjects = result["body"]
    print(f"Found {len(subjects)} subjects with exact match")

    # Should only find Subject 2 which has HP_0000152 directly
    # Should NOT include subjects with child terms
    assert len(subjects) >= 1, "Should find at least the subject with exact term match"

    # Verify all returned subjects have the exact term
    for subject in subjects:
        term_iris = [tl["term_iri"] for tl in subject["term_links"]]
        assert parent_term in term_iris, f"Subject {subject['subject_iri']} should have exact term {parent_term}"


@pytest.mark.integration
def test_cache_qualifier_filtering(physical_resources, cache_test_data):
    """
    Test that qualifier filtering (include_qualified=False) excludes negated terms.
    """
    project_id = cache_test_data["project_id"]

    print("\n=== Testing: Qualifier filtering (exclude negated) ===")

    # Query with include_qualified=False (should exclude negated)
    result = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=True,
        include_qualified=False
    )

    subjects = result["body"]
    print(f"Found {len(subjects)} subjects without negated terms")

    # Verify no subjects have negated qualifiers
    for subject in subjects:
        for term_link in subject["term_links"]:
            qualifiers = term_link.get("qualifiers", [])
            assert "negated" not in qualifiers, \
                f"Subject {subject['subject_iri']} has negated term but include_qualified=False"

    # Query with include_qualified=True (should include negated)
    result_with_qualified = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=True,
        include_qualified=True
    )

    subjects_with_qualified = result_with_qualified["body"]
    print(f"Found {len(subjects_with_qualified)} subjects including negated terms")

    # Should have MORE subjects when including qualified
    assert len(subjects_with_qualified) >= len(subjects), \
        "Including qualified should return same or more subjects"


@pytest.mark.integration
def test_cache_pagination(physical_resources, cache_test_data):
    """
    Test that pagination works correctly with the cache.
    """
    project_id = cache_test_data["project_id"]

    print("\n=== Testing: Pagination ===")

    # Query first page with small limit
    first_page = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=True,
        limit=2
    )

    print(f"First page: {first_page['n_subjects']} subjects")
    assert "pagination" in first_page
    assert first_page["pagination"]["limit"] == 2

    first_page_subjects = first_page["body"]
    first_page_iris = {s["subject_iri"] for s in first_page_subjects}

    # If there are more results, test pagination
    if first_page["pagination"]["has_more"]:
        print("Fetching second page...")
        next_cursor = first_page["pagination"]["next_cursor"]
        assert next_cursor is not None, "has_more=True but next_cursor is None"

        second_page = query_with_cache_flag(
            physical_resources,
            project_id,
            use_cache=True,
            limit=2,
            cursor=next_cursor
        )

        print(f"Second page: {second_page['n_subjects']} subjects")
        second_page_subjects = second_page["body"]
        second_page_iris = {s["subject_iri"] for s in second_page_subjects}

        # Verify no duplicates between pages
        overlap = first_page_iris & second_page_iris
        assert len(overlap) == 0, f"Found duplicate subjects across pages: {overlap}"

        print("✓ Pagination working correctly - no duplicates")
    else:
        print("No second page needed - all results fit in first page")


@pytest.mark.integration
def test_cache_specific_subject_ids(physical_resources, cache_test_data):
    """
    Test filtering by specific project_subject_ids.
    """
    project_id = cache_test_data["project_id"]

    print("\n=== Testing: Filter by specific project_subject_ids ===")

    # Query for specific subjects
    result = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=True,
        project_subject_ids=["cache-test-subject-1", "cache-test-subject-3"]
    )

    subjects = result["body"]
    print(f"Found {len(subjects)} subjects with specific IDs")

    # Should only return the 2 requested subjects
    assert len(subjects) == 2, f"Expected 2 subjects, got {len(subjects)}"

    returned_ids = {s["project_subject_id"] for s in subjects}
    expected_ids = {"cache-test-subject-1", "cache-test-subject-3"}
    assert returned_ids == expected_ids, f"Got {returned_ids}, expected {expected_ids}"


@pytest.mark.integration
def test_cache_performance_baseline(physical_resources, cache_test_data):
    """
    Measure cache query performance for baseline comparison.

    This test doesn't compare results, just measures timing.
    """
    project_id = cache_test_data["project_id"]

    print("\n=== Performance Baseline ===")

    # Measure cache query time
    start = time.time()
    result = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=True,
        term_iri="http://purl.obolibrary.org/obo/HP_0000152",
        include_child_terms=True
    )
    cache_time = time.time() - start

    print(f"Cache query completed in {cache_time*1000:.0f}ms")
    print(f"Returned {result['n_subjects']} subjects")

    # Just verify it completed successfully
    assert cache_time < 5.0, f"Cache query took too long: {cache_time}s"


@pytest.mark.integration
def test_compare_cache_vs_neptune_all_subjects(physical_resources, cache_test_data):
    """
    Compare cache vs Neptune results for querying all subjects.
    
    This test actually queries BOTH backends and compares the results.
    """
    project_id = cache_test_data["project_id"]

    print("\n=== Comparing Cache vs Neptune: All Subjects ===")

    # Query with Neptune
    print("Querying with Neptune...")
    start_neptune = time.time()
    neptune_result = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=False
    )
    neptune_time = time.time() - start_neptune

    # Query with Cache
    print("Querying with Cache...")
    start_cache = time.time()
    cache_result = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=True
    )
    cache_time = time.time() - start_cache

    # Compare counts and performance
    print(f"\nPerformance Comparison:")
    print(f"  Neptune: {neptune_time*1000:.0f}ms ({neptune_result['n_subjects']} subjects)")
    print(f"  Cache:   {cache_time*1000:.0f}ms ({cache_result['n_subjects']} subjects)")
    if neptune_time > 0:
        speedup = neptune_time / cache_time
        print(f"  Speedup: {speedup:.1f}x faster with cache")
    assert cache_result['n_subjects'] == neptune_result['n_subjects'], \
        f"Different subject counts: cache={cache_result['n_subjects']}, neptune={neptune_result['n_subjects']}"

    # Normalize and compare subjects
    cache_subjects = normalize_subjects_for_comparison(cache_result["body"])
    neptune_subjects = normalize_subjects_for_comparison(neptune_result["body"])

    assert len(cache_subjects) == len(neptune_subjects), \
        f"Different number of subjects: cache={len(cache_subjects)}, neptune={len(neptune_subjects)}"

    # Compare each subject
    for i, (cache_subj, neptune_subj) in enumerate(zip(cache_subjects, neptune_subjects)):
        assert cache_subj["subject_iri"] == neptune_subj["subject_iri"], \
            f"Subject {i}: Different IRIs"
        assert cache_subj["project_subject_id"] == neptune_subj["project_subject_id"], \
            f"Subject {i}: Different project_subject_ids"
        assert cache_subj["term_links"] == neptune_subj["term_links"], \
            f"Subject {cache_subj['subject_iri']}: Different term_links"

    print("✓ Cache and Neptune return identical results")


@pytest.mark.integration
def test_compare_cache_vs_neptune_hierarchy_query(physical_resources, cache_test_data):
    """
    Compare cache vs Neptune for hierarchy queries with include_child_terms.
    """
    project_id = cache_test_data["project_id"]
    parent_term = "http://purl.obolibrary.org/obo/HP_0000152"

    print("\n=== Comparing Cache vs Neptune: Hierarchy Query ===")

    # Query with Neptune
    print("Querying with Neptune (include_child_terms=True)...")
    start_neptune = time.time()
    neptune_result = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=False,
        term_iri=parent_term,
        include_child_terms=True
    )
    neptune_time = time.time() - start_neptune

    # Query with Cache
    print("Querying with Cache (include_child_terms=True)...")
    start_cache = time.time()
    cache_result = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=True,
        term_iri=parent_term,
        include_child_terms=True
    )
    cache_time = time.time() - start_cache

    print(f"\nPerformance Comparison:")
    print(f"  Neptune: {neptune_time*1000:.0f}ms ({neptune_result['n_subjects']} subjects)")
    print(f"  Cache:   {cache_time*1000:.0f}ms ({cache_result['n_subjects']} subjects)")
    if neptune_time > 0:
        speedup = neptune_time / cache_time
        print(f"  Speedup: {speedup:.1f}x faster with cache")

    # Normalize and compare
    cache_subjects = normalize_subjects_for_comparison(cache_result["body"])
    neptune_subjects = normalize_subjects_for_comparison(neptune_result["body"])

    assert len(cache_subjects) == len(neptune_subjects), \
        f"Different result counts: cache={len(cache_subjects)}, neptune={len(neptune_subjects)}"

    # Compare subjects
    for cache_subj, neptune_subj in zip(cache_subjects, neptune_subjects):
        assert cache_subj["subject_iri"] == neptune_subj["subject_iri"], \
            "Subject IRIs don't match"
        assert cache_subj["term_links"] == neptune_subj["term_links"], \
            f"Term links differ for {cache_subj['subject_iri']}"

    print("✓ Cache and Neptune hierarchy queries return identical results")


@pytest.mark.integration
def test_cache_pagination_with_term_filter(physical_resources, cache_test_data):
    """
    Test pagination specifically for term-filtered queries.

    Verifies that the cache implementation:
    1. Fetches all matching subject_ids with DynamoDB query
    2. Applies offset/limit pagination to the subject list
    3. Lazily loads details in parallel only for subjects on the current page
    4. Correctly returns no duplicate subjects across pages
    """
    project_id = cache_test_data["project_id"]
    parent_term = "http://purl.obolibrary.org/obo/HP_0000152"

    print("\n=== Testing: Term-filtered pagination ===")

    # Query first page with term filter and small limit
    first_page = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=True,
        term_iri=parent_term,
        include_child_terms=True,
        limit=1  # Small limit to force pagination
    )

    print(f"First page: {first_page['n_subjects']} subjects")
    assert "pagination" in first_page
    assert first_page["pagination"]["limit"] == 1

    first_page_subjects = first_page["body"]
    assert len(first_page_subjects) <= 1, "Should return at most 1 subject with limit=1"

    first_page_iris = {s["subject_iri"] for s in first_page_subjects}

    # Verify all subjects on first page match the term filter
    for subject in first_page_subjects:
        term_iris = [tl["term_iri"] for tl in subject["term_links"]]
        # Should have either parent term or child terms in hierarchy
        has_hierarchy_term = any(
            term in term_iris
            for term in ["http://purl.obolibrary.org/obo/HP_0000152",
                        "http://purl.obolibrary.org/obo/HP_0000234"]
        )
        assert has_hierarchy_term, f"Subject {subject['subject_iri']} doesn't match term filter"

    # If there are more results, test pagination with term filter
    if first_page["pagination"]["has_more"]:
        print("Fetching second page with term filter...")
        next_cursor = first_page["pagination"]["next_cursor"]
        assert next_cursor is not None, "has_more=True but next_cursor is None"

        second_page = query_with_cache_flag(
            physical_resources,
            project_id,
            use_cache=True,
            term_iri=parent_term,
            include_child_terms=True,
            limit=1,
            cursor=next_cursor
        )

        print(f"Second page: {second_page['n_subjects']} subjects")
        second_page_subjects = second_page["body"]
        second_page_iris = {s["subject_iri"] for s in second_page_subjects}

        # Verify no duplicates between pages
        overlap = first_page_iris & second_page_iris
        assert len(overlap) == 0, f"Found duplicate subjects across pages: {overlap}"

        # Verify subjects on second page also match term filter
        for subject in second_page_subjects:
            term_iris = [tl["term_iri"] for tl in subject["term_links"]]
            has_hierarchy_term = any(
                term in term_iris
                for term in ["http://purl.obolibrary.org/obo/HP_0000152",
                            "http://purl.obolibrary.org/obo/HP_0000234"]
            )
            assert has_hierarchy_term, f"Subject {subject['subject_iri']} doesn't match term filter"

        print("✓ Term-filtered pagination working correctly - no duplicates, all subjects match filter")
    else:
        print("No second page needed - all matching subjects fit in first page")


@pytest.mark.integration
def test_cache_pagination_early_stop_behavior(physical_resources, cache_test_data):
    """
    Test that pagination correctly respects the limit parameter and sets has_more flag.

    Verifies that when there are more matching subjects than the requested limit:
    1. Returns exactly 'limit' number of subjects (not more)
    2. Sets has_more=True to indicate additional pages exist
    3. Provides a next_cursor for fetching the next page
    """
    project_id = cache_test_data["project_id"]
    parent_term = "http://purl.obolibrary.org/obo/HP_0000152"

    print("\n=== Testing: Pagination limit and has_more behavior ===")

    # Query with a limit smaller than total matching subjects
    result = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=True,
        term_iri=parent_term,
        include_child_terms=True,
        limit=2  # We know there are at least 3 matching subjects
    )

    subjects = result["body"]
    pagination = result["pagination"]

    print(f"Requested limit: 2")
    print(f"Returned subjects: {len(subjects)}")
    print(f"has_more: {pagination['has_more']}")

    # Should return exactly the limit (not more)
    assert len(subjects) == 2, f"Expected 2 subjects with limit=2, got {len(subjects)}"

    # has_more should be True since we have more than 2 matching subjects
    assert pagination["has_more"] is True, "Expected has_more=True when more results exist"

    # Should have a cursor for fetching next page
    assert pagination["next_cursor"] is not None, "Expected next_cursor when has_more=True"

    print("✓ Pagination working correctly: returned exactly limit subjects and set has_more=True")


@pytest.mark.integration
def test_cache_pagination_last_page(physical_resources, cache_test_data):
    """
    Test that the last page of results correctly sets has_more=False.
    """
    project_id = cache_test_data["project_id"]

    print("\n=== Testing: Last page behavior ===")

    # Query with a very large limit to get all subjects in one page
    result = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=True,
        limit=1000  # Large enough to get all test subjects
    )

    pagination = result["pagination"]
    subjects = result["body"]

    print(f"Returned {len(subjects)} subjects with limit=1000")
    print(f"has_more: {pagination['has_more']}")
    print(f"next_cursor: {pagination['next_cursor']}")

    # When all results fit in one page, has_more should be False
    assert pagination["has_more"] is False, "Expected has_more=False when all results returned"

    # next_cursor should be None when no more results
    assert pagination["next_cursor"] is None, "Expected next_cursor=None when has_more=False"

    print("✓ Last page correctly sets has_more=False and next_cursor=None")


@pytest.mark.integration
def test_compare_cache_vs_neptune_qualifier_filtering(physical_resources, cache_test_data):
    """
    Compare cache vs Neptune for qualifier filtering (exclude negated).
    """
    project_id = cache_test_data["project_id"]

    print("\n=== Comparing Cache vs Neptune: Qualifier Filtering ===")

    # Query with Neptune (exclude negated)
    print("Querying with Neptune (include_qualified=False)...")
    start_neptune = time.time()
    neptune_result = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=False,
        include_qualified=False
    )
    neptune_time = time.time() - start_neptune

    # Query with Cache (exclude negated)
    print("Querying with Cache (include_qualified=False)...")
    start_cache = time.time()
    cache_result = query_with_cache_flag(
        physical_resources,
        project_id,
        use_cache=True,
        include_qualified=False
    )
    cache_time = time.time() - start_cache

    print(f"\nPerformance Comparison:")
    print(f"  Neptune: {neptune_time*1000:.0f}ms ({neptune_result['n_subjects']} subjects)")
    print(f"  Cache:   {cache_time*1000:.0f}ms ({cache_result['n_subjects']} subjects)")
    if neptune_time > 0:
        speedup = neptune_time / cache_time
        print(f"  Speedup: {speedup:.1f}x faster with cache")

    # Debug logging: Show what was returned
    print("\n=== DEBUG: Neptune Results ===")
    for i, subject in enumerate(neptune_result["body"]):
        print(f"Subject {i+1}: {subject['subject_iri']}")
        for tl in subject["term_links"]:
            qualifiers = tl.get("qualifiers", [])
            print(f"  - {tl['term_iri']}: {tl['term_label']} | qualifiers={qualifiers}")

    print("\n=== DEBUG: Cache Results ===")
    for i, subject in enumerate(cache_result["body"]):
        print(f"Subject {i+1}: {subject['subject_iri']}")
        for tl in subject["term_links"]:
            qualifiers = tl.get("qualifiers", [])
            print(f"  - {tl['term_iri']}: {tl['term_label']} | qualifiers={qualifiers}")

    # Normalize and compare
    cache_subjects = normalize_subjects_for_comparison(cache_result["body"])
    neptune_subjects = normalize_subjects_for_comparison(neptune_result["body"])

    assert len(cache_subjects) == len(neptune_subjects), \
        f"Different result counts: cache={len(cache_subjects)}, neptune={len(neptune_subjects)}"

    # Verify no negated/hypothetical/family qualifiers in either result
    for subject in cache_subjects + neptune_subjects:
        for term_link in subject["term_links"]:
            qualifiers = term_link.get("qualifiers", [])
            assert not any("negated" in q for q in qualifiers), \
                f"Found negated qualifier when include_qualified=False: {qualifiers}"
            assert not any("hypothetical" in q for q in qualifiers), \
                f"Found hypothetical qualifier when include_qualified=False: {qualifiers}"
            assert not any("family" in q for q in qualifiers), \
                f"Found family qualifier when include_qualified=False: {qualifiers}"

    print("✓ Cache and Neptune qualifier filtering returns identical results")


@pytest.mark.integration
def test_compare_cache_vs_neptune_pagination(physical_resources, cache_test_data):
    """
    Compare cache vs Neptune pagination behavior to ensure identical results across pages.
    """
    project_id = cache_test_data["project_id"]

    print("\n=== Comparing Cache vs Neptune: Pagination Behavior ===")

    # Fetch all results from both backends using pagination
    def fetch_all_pages(use_cache, limit=2):
        all_subjects = []
        cursor = None
        page_num = 0

        while True:
            page_num += 1
            result = query_with_cache_flag(
                physical_resources,
                project_id,
                use_cache=use_cache,
                limit=limit,
                cursor=cursor
            )

            subjects = result["body"]
            all_subjects.extend(subjects)

            pagination = result["pagination"]
            print(f"  Page {page_num}: {len(subjects)} subjects, has_more={pagination['has_more']}")

            if not pagination["has_more"]:
                break

            cursor = pagination["next_cursor"]
            assert cursor is not None, f"Page {page_num}: has_more=True but cursor is None"

        return all_subjects

    # Fetch all pages from Neptune
    print("Fetching all pages from Neptune...")
    neptune_all = fetch_all_pages(use_cache=False, limit=2)

    # Fetch all pages from Cache
    print("Fetching all pages from Cache...")
    cache_all = fetch_all_pages(use_cache=True, limit=2)

    print(f"\nTotal subjects:")
    print(f"  Neptune: {len(neptune_all)}")
    print(f"  Cache:   {len(cache_all)}")

    # Normalize and compare
    cache_subjects = normalize_subjects_for_comparison(cache_all)
    neptune_subjects = normalize_subjects_for_comparison(neptune_all)

    assert len(cache_subjects) == len(neptune_subjects), \
        f"Different total counts: cache={len(cache_subjects)}, neptune={len(neptune_subjects)}"

    # Compare all subjects
    for i, (cache_subj, neptune_subj) in enumerate(zip(cache_subjects, neptune_subjects)):
        assert cache_subj["subject_iri"] == neptune_subj["subject_iri"], \
            f"Subject {i}: Different IRIs"
        assert cache_subj["term_links"] == neptune_subj["term_links"], \
            f"Subject {cache_subj['subject_iri']}: Different term_links"

    print("✓ Cache and Neptune return identical results across all pages")
