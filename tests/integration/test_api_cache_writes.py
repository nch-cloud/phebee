"""
Integration tests for cache writes via API operations.

Tests that TermLink creation and subject-to-project linking properly
write cache entries for all relevant projects.
"""

import pytest
import boto3
import json
from phebee.utils.dynamodb_cache import build_project_data_pk


@pytest.mark.integration
def test_create_termlink_writes_to_all_projects(physical_resources, test_project_id):
    """
    Test that creating a TermLink via API writes cache entries for ALL projects
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

    # Create a second test project via Lambda
    project_2_id = f"{test_project_id}_second"
    create_project_resp = lambda_client.invoke(
        FunctionName=create_project_fn,
        Payload=json.dumps({
            "project_id": project_2_id,
            "project_label": "Test Project 2"
        }).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert create_project_resp["StatusCode"] == 200

    try:
        # Create a subject in Project 1 via Lambda
        create_subject_resp = lambda_client.invoke(
            FunctionName=create_subject_fn,
            Payload=json.dumps({"body": json.dumps({
                "project_id": test_project_id,
                "project_subject_id": "multi-project-subject-001"
            })}).encode("utf-8"),
            InvocationType="RequestResponse",
        )
        assert create_subject_resp["StatusCode"] == 200
        create_subject_body = json.loads(json.loads(create_subject_resp["Payload"].read())["body"])
        subject_iri = create_subject_body["subject"]["iri"]
        subject_id = subject_iri.split("/subjects/")[-1]

        # Link subject to Project 2 via Lambda
        link_subject_resp = lambda_client.invoke(
            FunctionName=create_subject_fn,
            Payload=json.dumps({"body": json.dumps({
                "project_id": project_2_id,
                "project_subject_id": "multi-project-subject-002",
                "known_subject_iri": subject_iri
            })}).encode("utf-8"),
            InvocationType="RequestResponse",
        )
        assert link_subject_resp["StatusCode"] == 200

        print(f"Subject {subject_id} linked to projects: {test_project_id}, {project_2_id}")

        # Create a TermLink via Lambda
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
        termlink_hash = create_termlink_body["termlink_iri"].split("/term-link/")[-1]

        print(f"Created TermLink: {termlink_hash}")

        # Verify cache entries exist for BOTH projects
        for project_id in [test_project_id, project_2_id]:
            pk = build_project_data_pk(project_id)
            response = cache_table.query(
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={":pk": pk}
            )

            items = response["Items"]
            print(f"Project {project_id}: Found {len(items)} cache entries")

            # Should have at least 1 entry (could be more due to multiple inheritance)
            assert len(items) > 0, f"No cache entries found for project {project_id}"

            # Verify structure
            found_termlink = False
            for item in items:
                assert item["PK"] == f"PROJECT#{project_id}"
                assert "||" in item["SK"]
                assert item["termlink_id"] == termlink_hash
                assert item["term_iri"] == term_iri
                assert item["term_label"] == "Abnormal heart morphology"

                # Verify subject_id in SK
                subject_in_sk = item["SK"].split("||")[1]
                assert subject_in_sk == subject_id
                found_termlink = True

            assert found_termlink, f"TermLink {termlink_hash} not found in cache for project {project_id}"
            print(f"✅ Verified cache entries for project {project_id}")

        print(f"✅ TermLink creation wrote cache entries to all {2} projects")

    finally:
        # Cleanup - delete cache entries
        for project_id in [test_project_id, project_2_id]:
            pk = build_project_data_pk(project_id)
            response = cache_table.query(
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={":pk": pk}
            )
            for item in response.get("Items", []):
                cache_table.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})


@pytest.mark.integration
def test_link_subject_to_project_duplicates_termlinks(physical_resources, test_project_id):
    """
    Test that linking a subject to a new project duplicates all existing TermLinks
    to the new project's cache partition.
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

    # Create a second test project via Lambda
    project_2_id = f"{test_project_id}_duplication_test"
    create_project_resp = lambda_client.invoke(
        FunctionName=create_project_fn,
        Payload=json.dumps({
            "project_id": project_2_id,
            "project_label": "Test Project for Duplication"
        }).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert create_project_resp["StatusCode"] == 200

    try:
        # Create a subject in first project only via Lambda
        create_subject_resp = lambda_client.invoke(
            FunctionName=create_subject_fn,
            Payload=json.dumps({"body": json.dumps({
                "project_id": test_project_id,
                "project_subject_id": "duplication-test-subject"
            })}).encode("utf-8"),
            InvocationType="RequestResponse",
        )
        assert create_subject_resp["StatusCode"] == 200
        create_subject_body = json.loads(json.loads(create_subject_resp["Payload"].read())["body"])
        subject_iri = create_subject_body["subject"]["iri"]
        subject_id = subject_iri.split("/subjects/")[-1]

        print(f"Created subject {subject_id} in project {test_project_id}")

        # Create TWO TermLinks for this subject via Lambda
        term_1_iri = "http://purl.obolibrary.org/obo/HP_0001627"  # Abnormal heart morphology
        term_2_iri = "http://purl.obolibrary.org/obo/HP_0000234"  # Abnormality of the head

        create_termlink_1_resp = lambda_client.invoke(
            FunctionName=create_termlink_fn,
            Payload=json.dumps({"body": json.dumps({
                "subject_id": subject_id,
                "term_iri": term_1_iri,
                "creator_id": "test-creator",
                "qualifiers": []
            })}).encode("utf-8"),
            InvocationType="RequestResponse",
        )
        assert create_termlink_1_resp["StatusCode"] == 200
        create_termlink_1_body = json.loads(json.loads(create_termlink_1_resp["Payload"].read())["body"])
        termlink_1_hash = create_termlink_1_body["termlink_iri"].split("/term-link/")[-1]

        create_termlink_2_resp = lambda_client.invoke(
            FunctionName=create_termlink_fn,
            Payload=json.dumps({"body": json.dumps({
                "subject_id": subject_id,
                "term_iri": term_2_iri,
                "creator_id": "test-creator",
                "qualifiers": []
            })}).encode("utf-8"),
            InvocationType="RequestResponse",
        )
        assert create_termlink_2_resp["StatusCode"] == 200
        create_termlink_2_body = json.loads(json.loads(create_termlink_2_resp["Payload"].read())["body"])
        termlink_2_hash = create_termlink_2_body["termlink_iri"].split("/term-link/")[-1]

        print(f"Created 2 TermLinks: {termlink_1_hash}, {termlink_2_hash}")

        # Verify cache entries exist for first project
        pk_1 = build_project_data_pk(test_project_id)
        response = cache_table.query(
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={":pk": pk_1}
        )

        items_before = response["Items"]
        termlinks_before = set(item["termlink_id"] for item in items_before)

        print(f"Project {test_project_id} has {len(termlinks_before)} unique TermLinks before linking")
        assert termlink_1_hash in termlinks_before
        assert termlink_2_hash in termlinks_before

        # NOW link the subject to the second project via Lambda
        print(f"Linking subject {subject_id} to project {project_2_id}...")
        link_subject_resp = lambda_client.invoke(
            FunctionName=create_subject_fn,
            Payload=json.dumps({"body": json.dumps({
                "project_id": project_2_id,
                "project_subject_id": "duplication-test-subject-p2",
                "known_subject_iri": subject_iri
            })}).encode("utf-8"),
            InvocationType="RequestResponse",
        )
        assert link_subject_resp["StatusCode"] == 200

        # Verify cache entries were DUPLICATED to second project
        pk_2 = build_project_data_pk(project_2_id)
        response = cache_table.query(
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={":pk": pk_2}
        )

        items_after = response["Items"]
        print(f"Project {project_2_id}: Found {len(items_after)} cache entries after linking")

        # Should have entries for BOTH TermLinks
        assert len(items_after) > 0, f"No cache entries found for project {project_2_id}"

        termlinks_after = set(item["termlink_id"] for item in items_after)
        print(f"Project {project_2_id} has {len(termlinks_after)} unique TermLinks")

        # Both TermLinks should be present
        assert termlink_1_hash in termlinks_after, f"TermLink {termlink_1_hash} not found in project {project_2_id}"
        assert termlink_2_hash in termlinks_after, f"TermLink {termlink_2_hash} not found in project {project_2_id}"

        # Verify structure of duplicated entries
        for item in items_after:
            assert item["PK"] == f"PROJECT#{project_2_id}"
            assert "||" in item["SK"]
            assert item["termlink_id"] in [termlink_1_hash, termlink_2_hash]

            # Verify subject_id in SK
            subject_in_sk = item["SK"].split("||")[1]
            assert subject_in_sk == subject_id

        print(f"✅ All {len(termlinks_after)} TermLinks were duplicated to project {project_2_id}")

    finally:
        # Cleanup - delete cache entries
        for project_id in [test_project_id, project_2_id]:
            pk = build_project_data_pk(project_id)
            response = cache_table.query(
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={":pk": pk}
            )
            for item in response.get("Items", []):
                cache_table.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})


@pytest.mark.integration
def test_create_termlink_with_qualifiers_writes_to_cache(physical_resources, test_project_id):
    """
    Test that creating a TermLink with qualifiers properly stores them in the cache.
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
    create_subject_fn = physical_resources["CreateSubjectFunction"]
    create_termlink_fn = physical_resources["CreateTermLinkFunction"]

    try:
        # Create a subject via Lambda
        create_subject_resp = lambda_client.invoke(
            FunctionName=create_subject_fn,
            Payload=json.dumps({"body": json.dumps({
                "project_id": test_project_id,
                "project_subject_id": "qualifier-test-subject"
            })}).encode("utf-8"),
            InvocationType="RequestResponse",
        )
        assert create_subject_resp["StatusCode"] == 200
        create_subject_body = json.loads(json.loads(create_subject_resp["Payload"].read())["body"])
        subject_id = create_subject_body["subject"]["iri"].split("/subjects/")[-1]

        # Create a TermLink with qualifiers via Lambda
        term_iri = "http://purl.obolibrary.org/obo/HP_0001627"
        qualifiers = [
            "http://ods.nationwidechildrens.org/phebee/qualifier/negated",
            "http://ods.nationwidechildrens.org/phebee/qualifier/family"
        ]

        create_termlink_resp = lambda_client.invoke(
            FunctionName=create_termlink_fn,
            Payload=json.dumps({"body": json.dumps({
                "subject_id": subject_id,
                "term_iri": term_iri,
                "creator_id": "test-creator",
                "qualifiers": qualifiers
            })}).encode("utf-8"),
            InvocationType="RequestResponse",
        )
        assert create_termlink_resp["StatusCode"] == 200
        create_termlink_body = json.loads(json.loads(create_termlink_resp["Payload"].read())["body"])
        termlink_hash = create_termlink_body["termlink_iri"].split("/term-link/")[-1]

        print(f"Created TermLink with qualifiers: {termlink_hash}")

        # Verify cache entry has qualifiers
        pk = build_project_data_pk(test_project_id)
        response = cache_table.query(
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={":pk": pk}
        )

        items = response["Items"]
        assert len(items) > 0, "No cache entries found"

        # Find our termlink
        found = False
        for item in items:
            if item["termlink_id"] == termlink_hash:
                # Verify qualifiers are stored correctly
                qualifiers_dict = item.get("qualifiers", {})
                assert qualifiers_dict.get("negated") == "true", f"Expected negated=true, got {qualifiers_dict.get('negated')}"
                assert qualifiers_dict.get("family") == "true", f"Expected family=true, got {qualifiers_dict.get('family')}"
                found = True
                print(f"✅ Verified qualifiers in cache: {qualifiers_dict}")
                break

        assert found, f"TermLink {termlink_hash} not found in cache"

    finally:
        # Cleanup
        pk = build_project_data_pk(test_project_id)
        response = cache_table.query(
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={":pk": pk}
        )
        for item in response.get("Items", []):
            cache_table.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})
