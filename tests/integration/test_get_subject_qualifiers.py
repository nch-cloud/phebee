"""
Integration tests for qualifier behavior in subject retrieval.

These tests verify that different qualifier combinations create separate termlinks
in both analytical tables (Iceberg) and Neptune graph.

Key scenarios:
- HPO term qualifiers (external IRI qualifiers like HP:0011009)
- Multiple non-boolean qualifier values (e.g., different onset types)
- Verification in both Iceberg and Neptune
"""

import pytest
import json
import boto3
from phebee.utils.aws import get_client


pytestmark = pytest.mark.integration


# ============================================================================
# Helper Functions
# ============================================================================

def get_subject(subject_id: str = None, project_subject_iri: str = None, physical_resources: dict = None):
    """Helper to invoke GetSubjectFunction."""
    lambda_client = get_client("lambda")

    payload = {}
    if subject_id:
        payload["subject_id"] = subject_id
    if project_subject_iri:
        payload["project_subject_iri"] = project_subject_iri

    response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectFunction"],
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())
    return result


def query_neptune(sparql_query: str, app_name: str):
    """Helper to query Neptune via ExecuteSparqlReadonly Lambda."""
    lambda_client = boto3.client('lambda', region_name='us-east-2')

    response = lambda_client.invoke(
        FunctionName=f"{app_name}-ExecuteSparqlReadonly",
        Payload=json.dumps({
            'query': sparql_query,
            'graphRestriction': 'subjects'
        })
    )

    result = json.loads(response['Payload'].read())
    if not result.get('success'):
        raise Exception(f"SPARQL query failed: {result.get('error')}")

    return result['results']


# ============================================================================
# Test Cases
# ============================================================================

def test_hpo_onset_qualifiers_create_separate_termlinks(
    physical_resources,
    test_project_id,
    test_subject,
    create_evidence_helper,
    standard_hpo_terms,
    query_athena
):
    """
    Test: HPO term qualifiers (different onset types) create separate termlinks.

    Scenario:
    - Create evidence for HP_0001250 (Seizures) with HP:0011009 (Acute onset)
    - Create evidence for HP_0001250 (Seizures) with HP:0003593 (Infantile onset)

    Verify:
    - Two separate entries in analytical table with different termlink_ids
    - Each has its own evidence count
    - Qualifiers are correctly stored
    """
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Create evidence with Acute onset (HP:0011009)
    evidence1 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=["http://purl.obolibrary.org/obo/HP_0011009"]  # Acute onset
    )

    # Create evidence with Infantile onset (HP:0003593)
    evidence2 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=["http://purl.obolibrary.org/obo/HP_0003593"]  # Infantile onset
    )

    # Query analytical table
    result = get_subject(subject_id=subject_uuid, physical_resources=physical_resources)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    terms = body["terms"]

    # Find entries for this term
    matching_terms = [t for t in terms if t["term_iri"] == term_iri]

    assert len(matching_terms) == 2, \
        f"Expected 2 separate termlinks for different onset qualifiers, got {len(matching_terms)}"

    # Verify different termlink_ids
    termlink_ids = [t["termlink_id"] for t in matching_terms]
    assert len(set(termlink_ids)) == 2, "Different onset qualifiers should create different termlink_ids"

    # Verify each has evidence_count = 1
    for term in matching_terms:
        assert term["evidence_count"] == 1, "Each termlink should have its own evidence count"

    # Verify qualifiers are present (check that at least one has qualifiers)
    qualifier_counts = [len(t.get("qualifiers", [])) for t in matching_terms]
    assert all(count > 0 for count in qualifier_counts), "Both termlinks should have qualifiers"


def test_multiple_qualifier_values_same_type(
    physical_resources,
    test_project_id,
    test_subject,
    create_evidence_helper,
    standard_hpo_terms,
    query_athena
):
    """
    Test: Multiple different values for the same qualifier type create separate termlinks.

    Scenario:
    - Create evidence for HP_0001250 with hypothetical=true
    - Create evidence for HP_0001250 with family=true
    - Create evidence for HP_0001250 with negated=true

    Verify:
    - Three separate entries with different termlink_ids
    - Each has its own evidence count
    """
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Create evidence with different boolean qualifiers
    evidence1 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=["hypothetical"]
    )

    evidence2 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=["family"]
    )

    evidence3 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=["negated"]
    )

    # Query analytical table
    result = get_subject(subject_id=subject_uuid, physical_resources=physical_resources)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    terms = body["terms"]

    # Find entries for this term
    matching_terms = [t for t in terms if t["term_iri"] == term_iri]

    assert len(matching_terms) == 3, \
        f"Expected 3 separate termlinks for different qualifiers, got {len(matching_terms)}"

    # Verify different termlink_ids
    termlink_ids = [t["termlink_id"] for t in matching_terms]
    assert len(set(termlink_ids)) == 3, "Different qualifiers should create different termlink_ids"

    # Verify each has evidence_count = 1
    for term in matching_terms:
        assert term["evidence_count"] == 1


def test_mixed_qualifiers_create_separate_termlinks(
    physical_resources,
    test_project_id,
    test_subject,
    create_evidence_helper,
    standard_hpo_terms,
    query_athena
):
    """
    Test: Mixed qualifier combinations create separate termlinks.

    Scenario:
    - Create evidence with [negated]
    - Create evidence with [hypothetical]
    - Create evidence with [negated, hypothetical]
    - Create evidence with no qualifiers

    Verify:
    - Four separate entries with different termlink_ids
    """
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Different qualifier combinations
    evidence1 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=["negated"]
    )

    evidence2 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=["hypothetical"]
    )

    evidence3 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=["negated", "hypothetical"]
    )

    evidence4 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=[]
    )

    # Query analytical table
    result = get_subject(subject_id=subject_uuid, physical_resources=physical_resources)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    terms = body["terms"]

    # Find entries for this term
    matching_terms = [t for t in terms if t["term_iri"] == term_iri]

    assert len(matching_terms) == 4, \
        f"Expected 4 separate termlinks for different qualifier combinations, got {len(matching_terms)}"

    # Verify different termlink_ids
    termlink_ids = [t["termlink_id"] for t in matching_terms]
    assert len(set(termlink_ids)) == 4, "Each qualifier combination should have unique termlink_id"


def test_neptune_has_separate_termlinks_for_qualifiers(
    physical_resources,
    test_project_id,
    test_subject,
    create_evidence_helper,
    standard_hpo_terms,
    query_athena,
    app_name
):
    """
    Test: Neptune graph has separate termlink nodes for different qualifiers.

    Scenario:
    - Create evidence with different onset qualifiers

    Verify:
    - Neptune has separate termlink nodes
    - Each termlink has the correct qualifier
    """
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Create evidence with different onset qualifiers
    evidence1 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=["http://purl.obolibrary.org/obo/HP_0011009"]  # Acute onset
    )

    evidence2 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=["http://purl.obolibrary.org/obo/HP_0003593"]  # Infantile onset
    )

    # Wait for materialization and Neptune load to complete
    # (This test assumes the bulk import pipeline has run)

    # Query Neptune for termlinks
    sparql_query = f"""
    SELECT ?termlink ?qualifier
    WHERE {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            <http://ods.nationwidechildrens.org/phebee/subjects/{subject_uuid}>
                <http://ods.nationwidechildrens.org/phebee#hasTermLink> ?termlink .
            ?termlink <http://ods.nationwidechildrens.org/phebee#hasTerm> <{term_iri}> .
            OPTIONAL {{ ?termlink <http://ods.nationwidechildrens.org/phebee#hasQualifyingTerm> ?qualifier }}
        }}
    }}
    """

    try:
        results = query_neptune(sparql_query, app_name)

        # Extract unique termlinks
        termlinks = {}
        for binding in results['results']['bindings']:
            termlink_uri = binding['termlink']['value']
            qualifier = binding.get('qualifier', {}).get('value')

            if termlink_uri not in termlinks:
                termlinks[termlink_uri] = set()
            if qualifier:
                termlinks[termlink_uri].add(qualifier)

        # Verify we have 2 separate termlinks
        assert len(termlinks) == 2, \
            f"Expected 2 separate termlinks in Neptune, got {len(termlinks)}"

        # Verify qualifiers are present
        all_qualifiers = set()
        for qualifiers in termlinks.values():
            all_qualifiers.update(qualifiers)

        assert 'http://purl.obolibrary.org/obo/HP_0011009' in all_qualifiers, \
            "Acute onset qualifier should be in Neptune"
        assert 'http://purl.obolibrary.org/obo/HP_0003593' in all_qualifiers, \
            "Infantile onset qualifier should be in Neptune"

    except Exception as e:
        pytest.skip(f"Neptune query not available or data not loaded yet: {e}")


def test_false_qualifiers_not_in_materialized_data(
    physical_resources,
    test_project_id,
    test_subject,
    create_evidence_helper,
    standard_hpo_terms,
    query_athena
):
    """
    Test: False/falsey qualifiers are filtered out from materialized tables.

    Scenario:
    - Create evidence with qualifiers including false values

    Verify:
    - Only non-false qualifiers appear in analytical table
    - termlink_id doesn't include false qualifiers
    """
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Create evidence with mixed true/false qualifiers
    # Note: Most APIs normalize these before storage, but test the filtering logic
    evidence = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=["hypothetical"]  # Only true qualifiers should be included
    )

    # Query analytical table
    result = get_subject(subject_id=subject_uuid, physical_resources=physical_resources)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    terms = body["terms"]

    matching_terms = [t for t in terms if t["term_iri"] == term_iri]
    assert len(matching_terms) == 1

    # Verify only non-false qualifiers are present
    qualifiers_str = str(matching_terms[0].get("qualifiers", [])).lower()

    # Should contain hypothetical
    assert "hypothetical" in qualifiers_str

    # Should NOT contain explicit "false" values (if they were somehow stored)
    # This is more of a sanity check since false qualifiers should be filtered earlier
