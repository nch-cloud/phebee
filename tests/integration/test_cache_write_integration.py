"""
Integration test for DynamoDB cache write functionality.

Tests writing subject-term links to the cache and querying them back.
"""

import pytest
import boto3
from phebee.utils.dynamodb_cache import (
    write_subject_term_links_batch,
    build_project_data_pk,
    build_project_data_sk
)
from phebee.utils.aws import get_client


@pytest.mark.integration
def test_write_and_query_subject_term_links(physical_resources, test_project_id):
    """Test writing subject-term links to cache and querying them back."""

    # Get DynamoDB table
    dynamodb = get_client("dynamodb")
    table_name = physical_resources["DynamoDBTable"]
    table = boto3.resource('dynamodb').Table(table_name)

    # Prepare test data - two subjects with HPO terms including hierarchy
    # HP_0000001 -> HP_0000118 -> HP_0001627 (Abnormal heart morphology)
    # HP_0000001 -> HP_0000152 -> HP_0000234 (Abnormality of the head)
    links = [
        {
            "project_id": test_project_id,
            "subject_id": "test-subject-1",
            "term_id": "HP_0001627",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "term_label": "Abnormal heart morphology",
            "ancestor_paths": [["HP_0000001", "HP_0000118", "HP_0001627"]],
            "qualifiers": {"negated": "false", "family": "false"},
            "termlink_id": "termlink-hash-1"
        },
        {
            "project_id": test_project_id,
            "subject_id": "test-subject-2",
            "term_id": "HP_0000234",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0000234",
            "term_label": "Abnormality of the head",
            "ancestor_paths": [["HP_0000001", "HP_0000152", "HP_0000234"]],
            "qualifiers": {"negated": "false", "family": "true"},
            "termlink_id": "termlink-hash-2"
        },
        {
            "project_id": test_project_id,
            "subject_id": "test-subject-3",
            "term_id": "HP_0001627",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "term_label": "Abnormal heart morphology",
            "ancestor_paths": [
                ["HP_0000001", "HP_0000118", "HP_0001627"],
                ["HP_0000001", "HP_0001626", "HP_0001627"]  # Multiple inheritance
            ],
            "qualifiers": {"negated": "true", "family": "false"},
            "termlink_id": "termlink-hash-3"
        }
    ]

    # Write to cache
    count = write_subject_term_links_batch(links)

    # Verify write count (subject-3 has 2 paths, so 4 total items)
    assert count == 4, f"Expected 4 items written (1 + 1 + 2), got {count}"

    # Test 1: Query all subjects for this project
    pk = build_project_data_pk(test_project_id)
    response = table.query(
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={":pk": pk}
    )

    items = response["Items"]
    print(f"Query all: Found {len(items)} items")
    assert len(items) == 4, f"Expected 4 total items, found {len(items)}"

    # Verify all items have correct structure
    for item in items:
        assert "PK" in item
        assert "SK" in item
        assert "termlink_id" in item
        assert "term_iri" in item
        assert "term_label" in item
        assert "qualifiers" in item
        assert item["PK"] == f"PROJECT#{test_project_id}"

    # Test 2: Query by ancestor path prefix (HP_0000118 - should find subjects 1 and 3)
    response = table.query(
        KeyConditionExpression="PK = :pk AND begins_with(SK, :path)",
        ExpressionAttributeValues={
            ":pk": pk,
            ":path": "HP_0000001|HP_0000118"
        }
    )

    items = response["Items"]
    print(f"Query HP_0000118 path: Found {len(items)} items")
    assert len(items) == 2, f"Expected 2 items with HP_0000118 path, found {len(items)}"

    # Verify we found the right subjects
    subject_ids = [item["SK"].split("||")[1] for item in items]
    assert "test-subject-1" in subject_ids
    assert "test-subject-3" in subject_ids

    # Test 3: Query with qualifier filtering - negated=false
    response = table.query(
        KeyConditionExpression="PK = :pk",
        FilterExpression="qualifiers.negated = :negated",
        ExpressionAttributeValues={
            ":pk": pk,
            ":negated": "false"
        }
    )

    items = response["Items"]
    print(f"Query negated=false: Found {len(items)} items")
    # Should find subject-1 (1 path) and subject-2 (1 path) = 2 items
    assert len(items) == 2, f"Expected 2 items with negated=false, found {len(items)}"

    subject_ids = [item["SK"].split("||")[1] for item in items]
    assert "test-subject-1" in subject_ids
    assert "test-subject-2" in subject_ids

    # Test 4: Query with combined path prefix + qualifier filter
    # Find subjects under HP_0000118 with negated=false
    response = table.query(
        KeyConditionExpression="PK = :pk AND begins_with(SK, :path)",
        FilterExpression="qualifiers.negated = :negated",
        ExpressionAttributeValues={
            ":pk": pk,
            ":path": "HP_0000001|HP_0000118",
            ":negated": "false"
        }
    )

    items = response["Items"]
    print(f"Query HP_0000118 + negated=false: Found {len(items)} items")
    # Should find only subject-1 (subject-3 has negated=true)
    assert len(items) == 1, f"Expected 1 item, found {len(items)}"
    assert items[0]["SK"].split("||")[1] == "test-subject-1"

    # Test 5: Query specific subject
    sk_prefix = "HP_0000001|HP_0000118|HP_0001627||test-subject-1"
    response = table.query(
        KeyConditionExpression="PK = :pk AND SK = :sk",
        ExpressionAttributeValues={
            ":pk": pk,
            ":sk": sk_prefix
        }
    )

    items = response["Items"]
    print(f"Query specific subject: Found {len(items)} items")
    assert len(items) == 1, f"Expected 1 item for specific subject query, found {len(items)}"
    assert items[0]["termlink_id"] == "termlink-hash-1"
    assert items[0]["term_label"] == "Abnormal heart morphology"

    # Test 6: Verify multiple inheritance - subject-3 should have 2 entries
    response = table.query(
        KeyConditionExpression="PK = :pk AND begins_with(SK, :subject)",
        ExpressionAttributeValues={
            ":pk": pk,
            ":subject": "HP_0000001"
        },
        FilterExpression="contains(SK, :subject_id)",
        ExpressionAttributeNames={"#sk": "SK"},
    )

    # Alternative approach: query all and filter in code
    response = table.query(
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={":pk": pk}
    )

    subject_3_items = [item for item in response["Items"] if "test-subject-3" in item["SK"]]
    print(f"Subject-3 items: Found {len(subject_3_items)} items (should be 2 due to multiple inheritance)")
    assert len(subject_3_items) == 2, f"Expected 2 items for subject-3 (multiple inheritance), found {len(subject_3_items)}"

    # Verify both have same termlink_id but different sort keys
    termlink_ids = [item["termlink_id"] for item in subject_3_items]
    assert termlink_ids[0] == termlink_ids[1] == "termlink-hash-3"

    sort_keys = [item["SK"] for item in subject_3_items]
    assert len(set(sort_keys)) == 2, "Multiple inheritance should create different sort keys"
    assert "HP_0000001|HP_0000118|HP_0001627||test-subject-3" in sort_keys
    assert "HP_0000001|HP_0001626|HP_0001627||test-subject-3" in sort_keys

    print("✅ All cache write and query tests passed!")

    # Cleanup - delete test items
    for item in response["Items"]:
        table.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})


@pytest.mark.integration
def test_write_empty_qualifiers(physical_resources, test_project_id):
    """Test writing links with empty qualifiers."""

    # Get DynamoDB table
    table_name = physical_resources["DynamoDBTable"]
    table = boto3.resource('dynamodb').Table(table_name)

    links = [{
        "project_id": test_project_id,
        "subject_id": "test-subject-empty-quals",
        "term_id": "HP_0001627",
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
        "term_label": "Abnormal heart morphology",
        "ancestor_paths": [["HP_0000001", "HP_0000118", "HP_0001627"]],
        "qualifiers": {},
        "termlink_id": "termlink-hash-empty"
    }]

    count = write_subject_term_links_batch(links)
    assert count == 1

    # Query back
    pk = build_project_data_pk(test_project_id)
    response = table.query(
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={":pk": pk}
    )

    items = response["Items"]
    assert len(items) == 1
    assert items[0]["qualifiers"] == {}

    print("✅ Empty qualifiers test passed!")

    # Cleanup
    table.delete_item(Key={"PK": items[0]["PK"], "SK": items[0]["SK"]})
