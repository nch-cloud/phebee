"""
Integration tests for cache deletion operations.

Tests that TermLink deletion and Subject deletion properly clean up
cache entries across all relevant projects.
"""

import pytest
import boto3
import json
from phebee.utils.dynamodb_cache import build_project_data_pk


@pytest.mark.integration
def test_delete_termlink_removes_cache_entries(physical_resources, test_project_id):
    """
    Test that deleting a TermLink removes cache entries from ALL projects
    that contain the subject.
    """
    from phebee.utils.aws import get_client
    from phebee.utils.dynamodb import get_current_term_source_version

    # Get cache table
    cache_table_name = physical_resources.get("DynamoDBCacheTable")
    if not cache_table_name:
        pytest.skip("DynamoDBCacheTable not found")

    dynamodb = boto3.resource('dynamodb')
    cache_table = dynamodb.Table(cache_table_name)

    # Get HPO version
    hpo_version = get_current_term_source_version("hpo")
    if not hpo_version:
        pytest.skip("No HPO version installed")

    # Get Lambda client
    lambda_client = get_client("lambda")
    create_project_fn = physical_resources["CreateProjectFunction"]
    create_subject_fn = physical_resources["CreateSubjectFunction"]
    create_termlink_fn = physical_resources["CreateTermLinkFunction"]
    remove_termlink_fn = physical_resources["RemoveTermLinkFunction"]

    # Create a second test project
    project_2_id = f"{test_project_id}_delete_test"
    create_project_resp = lambda_client.invoke(
        FunctionName=create_project_fn,
        Payload=json.dumps({
            "project_id": project_2_id,
            "project_label": "Test Project for Deletion"
        }).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert create_project_resp["StatusCode"] == 200

    try:
        # Create a subject in Project 1
        create_subject_resp = lambda_client.invoke(
            FunctionName=create_subject_fn,
            Payload=json.dumps({"body": json.dumps({
                "project_id": test_project_id,
                "project_subject_id": "delete-test-subject-001"
            })}).encode("utf-8"),
            InvocationType="RequestResponse",
        )
        assert create_subject_resp["StatusCode"] == 200
        create_subject_body = json.loads(json.loads(create_subject_resp["Payload"].read())["body"])
        subject_iri = create_subject_body["subject"]["iri"]
        subject_id = subject_iri.split("/subjects/")[-1]

        # Link subject to Project 2
        link_subject_resp = lambda_client.invoke(
            FunctionName=create_subject_fn,
            Payload=json.dumps({"body": json.dumps({
                "project_id": project_2_id,
                "project_subject_id": "delete-test-subject-002",
                "known_subject_iri": subject_iri
            })}).encode("utf-8"),
            InvocationType="RequestResponse",
        )
        assert link_subject_resp["StatusCode"] == 200

        print(f"Subject {subject_id} linked to projects: {test_project_id}, {project_2_id}")

        # Create a TermLink
        term_iri = "http://purl.obolibrary.org/obo/HP_0001627"  # Abnormal heart morphology

        create_termlink_resp = lambda_client.invoke(
            FunctionName=create_termlink_fn,
            Payload=json.dumps({"body": json.dumps({
                "subject_id": subject_id,
                "term_iri": term_iri,
                "creator_id": "test-creator",
                "qualifiers": []
            })}).encode("utf-8"),
            InvocationType="RequestResponse",
        )
        assert create_termlink_resp["StatusCode"] == 200
        create_termlink_body = json.loads(json.loads(create_termlink_resp["Payload"].read())["body"])
        termlink_iri = create_termlink_body["termlink_iri"]
        termlink_hash = termlink_iri.split("/term-link/")[-1]

        print(f"Created TermLink: {termlink_hash}")

        # Verify cache entries exist for BOTH projects before deletion
        for project_id in [test_project_id, project_2_id]:
            pk = build_project_data_pk(project_id)
            response = cache_table.query(
                KeyConditionExpression="PK = :pk",
                FilterExpression="termlink_id = :termlink_id",
                ExpressionAttributeValues={
                    ":pk": pk,
                    ":termlink_id": termlink_hash
                }
            )
            items = response["Items"]
            assert len(items) > 0, f"No cache entries found for project {project_id} before deletion"
            print(f"Project {project_id}: Found {len(items)} cache entries before deletion")

        # Delete the TermLink
        print(f"Deleting TermLink: {termlink_iri}")
        remove_termlink_resp = lambda_client.invoke(
            FunctionName=remove_termlink_fn,
            Payload=json.dumps({"body": json.dumps({
                "termlink_iri": termlink_iri
            })}).encode("utf-8"),
            InvocationType="RequestResponse",
        )
        assert remove_termlink_resp["StatusCode"] == 200
        print(f"✅ TermLink deleted successfully")

        # Verify cache entries are DELETED from BOTH projects
        for project_id in [test_project_id, project_2_id]:
            pk = build_project_data_pk(project_id)
            response = cache_table.query(
                KeyConditionExpression="PK = :pk",
                FilterExpression="termlink_id = :termlink_id",
                ExpressionAttributeValues={
                    ":pk": pk,
                    ":termlink_id": termlink_hash
                }
            )
            items = response["Items"]
            assert len(items) == 0, f"Cache entries still exist for project {project_id} after deletion"
            print(f"✅ Project {project_id}: Cache entries deleted")

        print(f"✅ TermLink deletion cleaned up cache entries from all {2} projects")

    finally:
        # Cleanup - delete any remaining cache entries
        for project_id in [test_project_id, project_2_id]:
            pk = build_project_data_pk(project_id)
            response = cache_table.query(
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={":pk": pk}
            )
            for item in response.get("Items", []):
                cache_table.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})


@pytest.mark.integration
def test_delete_subject_removes_all_cache_and_mappings(physical_resources, test_project_id):
    """
    Test that deleting a Subject removes:
    1. All cache entries for that subject across all projects
    2. All DynamoDB subject-project mappings
    """
    from phebee.utils.aws import get_client
    from phebee.utils.dynamodb import get_current_term_source_version

    # Get cache table and main table
    cache_table_name = physical_resources.get("DynamoDBCacheTable")
    dynamodb_table_name = physical_resources.get("DynamoDBTable")
    if not cache_table_name or not dynamodb_table_name:
        pytest.skip("Required DynamoDB tables not found")

    dynamodb = boto3.resource('dynamodb')
    cache_table = dynamodb.Table(cache_table_name)
    main_table = dynamodb.Table(dynamodb_table_name)

    # Get HPO version
    hpo_version = get_current_term_source_version("hpo")
    if not hpo_version:
        pytest.skip("No HPO version installed")

    # Get Lambda client
    lambda_client = get_client("lambda")
    create_project_fn = physical_resources["CreateProjectFunction"]
    create_subject_fn = physical_resources["CreateSubjectFunction"]
    create_termlink_fn = physical_resources["CreateTermLinkFunction"]
    remove_subject_fn = physical_resources["RemoveSubjectFunction"]

    # Create a second test project
    project_2_id = f"{test_project_id}_subject_delete"
    create_project_resp = lambda_client.invoke(
        FunctionName=create_project_fn,
        Payload=json.dumps({
            "project_id": project_2_id,
            "project_label": "Test Project for Subject Deletion"
        }).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert create_project_resp["StatusCode"] == 200

    try:
        # Create a subject in Project 1
        create_subject_resp = lambda_client.invoke(
            FunctionName=create_subject_fn,
            Payload=json.dumps({"body": json.dumps({
                "project_id": test_project_id,
                "project_subject_id": "subject-delete-test-001"
            })}).encode("utf-8"),
            InvocationType="RequestResponse",
        )
        assert create_subject_resp["StatusCode"] == 200
        create_subject_body = json.loads(json.loads(create_subject_resp["Payload"].read())["body"])
        subject_iri = create_subject_body["subject"]["iri"]
        subject_id = subject_iri.split("/subjects/")[-1]
        project_subject_iri_1 = f"http://ods.nationwidechildrens.org/phebee/projects/{test_project_id}/subject-delete-test-001"

        # Link subject to Project 2
        link_subject_resp = lambda_client.invoke(
            FunctionName=create_subject_fn,
            Payload=json.dumps({"body": json.dumps({
                "project_id": project_2_id,
                "project_subject_id": "subject-delete-test-002",
                "known_subject_iri": subject_iri
            })}).encode("utf-8"),
            InvocationType="RequestResponse",
        )
        assert link_subject_resp["StatusCode"] == 200

        print(f"Subject {subject_id} linked to projects: {test_project_id}, {project_2_id}")

        # Create TWO TermLinks for this subject
        term_1_iri = "http://purl.obolibrary.org/obo/HP_0001627"  # Abnormal heart morphology
        term_2_iri = "http://purl.obolibrary.org/obo/HP_0000234"  # Abnormality of the head

        for term_iri in [term_1_iri, term_2_iri]:
            create_termlink_resp = lambda_client.invoke(
                FunctionName=create_termlink_fn,
                Payload=json.dumps({"body": json.dumps({
                    "subject_id": subject_id,
                    "term_iri": term_iri,
                    "creator_id": "test-creator",
                    "qualifiers": []
                })}).encode("utf-8"),
                InvocationType="RequestResponse",
            )
            assert create_termlink_resp["StatusCode"] == 200

        print(f"Created 2 TermLinks for subject {subject_id}")

        # Verify cache entries exist for BOTH projects before deletion
        cache_entries_before = {}
        for project_id in [test_project_id, project_2_id]:
            pk = build_project_data_pk(project_id)
            response = cache_table.query(
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={":pk": pk}
            )
            items = [item for item in response["Items"] if item["SK"].endswith(f"||{subject_id}")]
            cache_entries_before[project_id] = len(items)
            assert len(items) > 0, f"No cache entries found for project {project_id} before deletion"
            print(f"Project {project_id}: Found {len(items)} cache entries before deletion")

        # Verify DynamoDB mappings exist
        # Direction 1: PROJECT -> SUBJECT
        for project_id, project_subject_id in [(test_project_id, "subject-delete-test-001"),
                                                (project_2_id, "subject-delete-test-002")]:
            response = main_table.get_item(Key={
                "PK": f"PROJECT#{project_id}",
                "SK": f"SUBJECT#{project_subject_id}"
            })
            assert "Item" in response, f"Project->Subject mapping not found for {project_id}"

        # Direction 2: SUBJECT -> PROJECTS
        response = main_table.query(
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={":pk": f"SUBJECT#{subject_id}"}
        )
        mappings_before = response["Items"]
        assert len(mappings_before) == 2, f"Expected 2 subject->project mappings, found {len(mappings_before)}"
        print(f"Found {len(mappings_before)} DynamoDB subject-project mappings before deletion")

        # Delete the Subject
        print(f"Deleting Subject via project_subject_iri: {project_subject_iri_1}")
        remove_subject_resp = lambda_client.invoke(
            FunctionName=remove_subject_fn,
            Payload=json.dumps({"body": json.dumps({
                "project_subject_iri": project_subject_iri_1
            })}).encode("utf-8"),
            InvocationType="RequestResponse",
        )
        assert remove_subject_resp["StatusCode"] == 200
        print(f"✅ Subject deleted successfully")

        # Verify cache entries are DELETED from BOTH projects
        for project_id in [test_project_id, project_2_id]:
            pk = build_project_data_pk(project_id)
            response = cache_table.query(
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={":pk": pk}
            )
            items = [item for item in response["Items"] if item["SK"].endswith(f"||{subject_id}")]
            assert len(items) == 0, f"Cache entries still exist for project {project_id} after deletion (found {len(items)})"
            print(f"✅ Project {project_id}: {cache_entries_before[project_id]} cache entries deleted")

        # Verify DynamoDB mappings are DELETED
        # Direction 1: PROJECT -> SUBJECT
        for project_id, project_subject_id in [(test_project_id, "subject-delete-test-001"),
                                                (project_2_id, "subject-delete-test-002")]:
            response = main_table.get_item(Key={
                "PK": f"PROJECT#{project_id}",
                "SK": f"SUBJECT#{project_subject_id}"
            })
            assert "Item" not in response, f"Project->Subject mapping still exists for {project_id}"

        # Direction 2: SUBJECT -> PROJECTS
        response = main_table.query(
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={":pk": f"SUBJECT#{subject_id}"}
        )
        mappings_after = response["Items"]
        assert len(mappings_after) == 0, f"Subject->Project mappings still exist (found {len(mappings_after)})"
        print(f"✅ All {len(mappings_before)} DynamoDB subject-project mappings deleted")

        print(f"✅ Subject deletion cleaned up all cache entries and mappings from {2} projects")

    finally:
        # Cleanup - delete any remaining cache entries and mappings
        for project_id in [test_project_id, project_2_id]:
            pk = build_project_data_pk(project_id)
            response = cache_table.query(
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={":pk": pk}
            )
            for item in response.get("Items", []):
                cache_table.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})

        # Clean up DynamoDB mappings if they still exist
        if 'subject_id' in locals():
            response = main_table.query(
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={":pk": f"SUBJECT#{subject_id}"}
            )
            for item in response.get("Items", []):
                main_table.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})

            for project_id, project_subject_id in [(test_project_id, "subject-delete-test-001"),
                                                    (project_2_id, "subject-delete-test-002")]:
                try:
                    main_table.delete_item(Key={
                        "PK": f"PROJECT#{project_id}",
                        "SK": f"SUBJECT#{project_subject_id}"
                    })
                except:
                    pass
