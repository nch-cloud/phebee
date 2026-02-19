"""
Pytest configuration and fixtures for integration test suite.

This test suite focuses on comprehensive integration testing with Neptune-free
verification using Athena/Iceberg analytical tables.
"""

import pytest
import json
import boto3
import uuid
import time
import os
import subprocess
from typing import Dict, Callable, List
from botocore.exceptions import ClientError
from botocore.config import Config
from requests_aws4auth import AWS4Auth

from phebee.utils.aws import get_client


# ============================================================================
# SESSION-LEVEL FIXTURES
# ============================================================================

@pytest.fixture(scope="session")
def profile_name(request):
    profile_name = request.config.getoption("--profile")
    print(f"Using AWS session profile: {profile_name}")
    return profile_name


@pytest.fixture(scope="session")
def aws_session(request, profile_name):
    """Initialize AWS clients and return session."""
    # Create clients so they are ready to use
    get_client("s3", profile_name)
    get_client("cloudformation", profile_name)
    get_client("stepfunctions", profile_name)
    get_client("dynamodb", profile_name)

    lambda_config = Config(read_timeout=900, connect_timeout=900, retries={"max_attempts": 0})
    get_client("lambda", profile_name, lambda_config)

    session = boto3.Session(profile_name=profile_name)
    return session


@pytest.fixture(scope="session")
def cloudformation_stack(request, aws_session, profile_name):
    """
    Deploys a CloudFormation stack for testing purposes or uses existing stack.

    Stack name resolution order:
    1. --existing-stack command line argument
    2. .phebee-test-stack file in project root (if exists)
    3. Generate new stack name and deploy

    Yields:
        str: The name of the CloudFormation stack.
    """
    cf_client = get_client("cloudformation")

    existing_stack = request.config.getoption("--existing-stack")

    # If --existing-stack not provided, check for config file
    if not existing_stack:
        config_file_path = os.path.join(os.getcwd(), ".phebee-test-stack")
        if os.path.exists(config_file_path):
            with open(config_file_path, "r") as f:
                stack_from_file = f.read().strip()
                if stack_from_file:
                    existing_stack = stack_from_file
                    print(f"Using stack from .phebee-test-stack config file: {existing_stack}")

    config_env = request.config.getoption("--config-env")
    stack_uuid = str(uuid.uuid4())[-3:]
    stack_name = f"phebee-it-{stack_uuid}" if not existing_stack else existing_stack

    template_path = "template.yaml"

    if not existing_stack:
        try:
            # Step 1: Build the SAM application
            subprocess.run(
                ["sam", "build", "--template-file", template_path], check=True
            )

            # Step 2: Deploy the SAM application
            deploy_args = [
                "sam",
                "deploy",
                "--stack-name",
                stack_name,
                "--capabilities",
                "CAPABILITY_NAMED_IAM",
                "CAPABILITY_AUTO_EXPAND",
                "--no-confirm-changeset",
                "--no-fail-on-empty-changeset",
                "--resolve-s3",
                "--config-env",
                config_env,
            ]
            if profile_name:
                deploy_args.extend(["--profile", profile_name])
            subprocess.run(deploy_args, check=True)

            # Step 3: Wait for stack creation
            waiter = cf_client.get_waiter("stack_create_complete")
            waiter.wait(StackName=stack_name)

        except subprocess.CalledProcessError as e:
            print(f"Stack creation failed: {e}")
            pytest.fail(f"Failed to run SAM CLI command: {e}")
    else:
        # Existing stack - optionally reset database
        force_reset = request.config.getoption("--force-database-reset")
        if force_reset:
            print("Resetting database...")
            # Import from reset_database test
            from test_reset_database import reset_database
            reset_database(stack_name)
        else:
            print("[CLEANUP] Skipping database cleanup for existing stack")

    yield stack_name

    if not existing_stack:
        # Tear down the stack after tests
        try:
            teardown_args = ["sam", "delete", "--stack-name", stack_name, "--no-prompts"]
            if profile_name:
                teardown_args.extend(["--profile", profile_name])
            subprocess.run(teardown_args, check=True)
        except subprocess.CalledProcessError as e:
            pytest.fail(f"Failed to delete stack: {e}")


@pytest.fixture(scope="session", autouse=True)
def physical_resources(aws_session, cloudformation_stack):
    """Retrieve stack resources and outputs."""
    cf_client = aws_session.client("cloudformation")
    resource_mapping = {}
    next_token = None

    try:
        # Loop through all pages of resources
        while True:
            if next_token:
                response = cf_client.list_stack_resources(
                    StackName=cloudformation_stack, NextToken=next_token
                )
            else:
                response = cf_client.list_stack_resources(
                    StackName=cloudformation_stack
                )

            # Process current page
            for resource in response["StackResourceSummaries"]:
                logical_id = resource["LogicalResourceId"]
                physical_id = resource["PhysicalResourceId"]
                resource_mapping[logical_id] = physical_id

            next_token = response.get("NextToken")
            if not next_token:
                break

        # Also fetch Stack Outputs
        outputs_response = cf_client.describe_stacks(StackName=cloudformation_stack)
        outputs = outputs_response["Stacks"][0].get("Outputs", [])

        for output in outputs:
            output_key = output["OutputKey"]
            output_value = output["OutputValue"]
            resource_mapping[output_key] = output_value

        yield resource_mapping

    except cf_client.exceptions.ClientError as e:
        print(f"Error retrieving stack resources: {e}")
        return None


@pytest.fixture(scope="session")
def stack_outputs(cloudformation_stack):
    """Get stack outputs as a dict."""
    cf_client = get_client("cloudformation")
    response = cf_client.describe_stacks(StackName=cloudformation_stack)
    outputs = response["Stacks"][0].get("Outputs", [])
    return {o["OutputKey"]: o["OutputValue"] for o in outputs}


@pytest.fixture(scope="session")
def hpo_update_execution(cloudformation_stack):
    """
    Run HPO update once per test session to seed ontology data.

    This ensures HPO source data exists in DynamoDB and ontology hierarchy
    is available in Iceberg tables for tests that depend on it.
    """
    cf_client = get_client("cloudformation")

    # Get UpdateHPOSFNArn from stack outputs
    response = cf_client.describe_stacks(StackName=cloudformation_stack)
    outputs = response['Stacks'][0]['Outputs']

    hpo_sfn_arn = None
    for output in outputs:
        if output['OutputKey'] == 'UpdateHPOSFNArn':
            hpo_sfn_arn = output['OutputValue']
            break

    if not hpo_sfn_arn:
        print("[HPO UPDATE] UpdateHPOSFNArn not found - skipping HPO update")
        return None

    sfn_client = get_client("stepfunctions")

    # Start execution
    execution_name = f"test-session-hpo-update-{int(time.time())}"
    print(f"\n[HPO UPDATE] Starting session-level HPO update: {execution_name}")

    response = sfn_client.start_execution(
        stateMachineArn=hpo_sfn_arn,
        name=execution_name,
        input=json.dumps({"test": True})
    )

    execution_arn = response['executionArn']
    print(f"[HPO UPDATE] Execution ARN: {execution_arn}")

    # Wait for completion
    print("[HPO UPDATE] Waiting for execution to complete (may take 15-45 minutes)...")
    start_time = time.time()

    while True:
        execution = sfn_client.describe_execution(executionArn=execution_arn)
        status = execution['status']

        elapsed = int(time.time() - start_time)
        if elapsed % 60 == 0:  # Print every minute
            print(f"[HPO UPDATE] Status: {status} (elapsed: {elapsed//60}m)")

        if status == 'SUCCEEDED':
            duration = time.time() - start_time
            print(f"[HPO UPDATE] Completed successfully in {duration//60:.0f}m {duration%60:.0f}s")
            return execution_arn
        elif status in ['FAILED', 'TIMED_OUT', 'ABORTED']:
            error = execution.get('cause', 'Unknown error')
            print(f"[HPO UPDATE] Execution {status}: {error}")
            return None

        time.sleep(30)


@pytest.fixture(scope="session", autouse=True)
def set_env_variables_from_stack(cloudformation_stack):
    """
    Set environment variables from CloudFormation stack outputs.
    This fixture runs automatically for all tests in the session.
    """
    cf_client = get_client("cloudformation")

    try:
        response = cf_client.describe_stacks(StackName=cloudformation_stack)
        outputs = response["Stacks"][0]["Outputs"]

        print(outputs)

        # Set environment variables
        for output in outputs:
            key = output["OutputKey"]
            value = output["OutputValue"]

            if key == "DynamoDBTableName":
                os.environ["PheBeeDynamoTable"] = value
            elif key == "Region":
                os.environ["Region"] = value
            elif key == "NeptuneEndpoint":
                os.environ["NeptuneEndpoint"] = value
            elif key == "NeptuneClusterIdentifier":
                os.environ["NeptuneClusterIdentifier"] = value
            elif key == "AthenaDatabase":
                os.environ["ICEBERG_DATABASE"] = value
            elif key == "AthenaOntologyHierarchyTable":
                os.environ["ICEBERG_ONTOLOGY_HIERARCHY_TABLE"] = value
            elif key == "PheBeeBucketName":
                os.environ["PHEBEE_BUCKET_NAME"] = value

        print(f"Environment variables set from stack {cloudformation_stack}.")

    except ClientError as e:
        pytest.fail(f"Failed to retrieve stack outputs for {cloudformation_stack}: {e}")


# ============================================================================
# TEST-LEVEL FIXTURES
# ============================================================================

@pytest.fixture
def test_project_id(cloudformation_stack):
    """Create a test project and return its ID."""
    lambda_client = get_client("lambda")
    project_id = f"test_project_{uuid.uuid4().hex[:8]}"
    payload = {"project_id": project_id, "project_label": "Lambda-Initiated Project"}

    print(f"\n[TEST] Test project id: {project_id}")

    # Create the project
    create_response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-CreateProjectFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({"body": json.dumps(payload)}),
    )

    lambda_response = json.loads(create_response["Payload"].read().decode("utf-8"))
    assert create_response["StatusCode"] == 200, f"Invoke failed: {lambda_response}"

    # Handle both direct response and API Gateway format
    if "statusCode" in lambda_response:
        assert lambda_response["statusCode"] == 200, f"Lambda failed: {lambda_response}"
        body = json.loads(lambda_response["body"])
    else:
        body = lambda_response

    assert body.get("project_created") is True

    yield body.get("project_id")
    # No teardown - cleanup deferred to end of session


@pytest.fixture
def test_subject(physical_resources, test_project_id):
    """Create a test subject and return (subject_uuid, project_subject_iri).

    This fixture creates a single test subject in the test project and returns
    both the UUID and IRI for use in tests.

    Returns:
        tuple: (subject_uuid: str, project_subject_iri: str)
    """
    lambda_client = get_client("lambda")

    project_subject_id = f"test-subj-{uuid.uuid4().hex[:8]}"

    # Create subject
    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateSubjectFunction"],
        Payload=json.dumps({
            "body": json.dumps({
                "project_id": test_project_id,
                "project_subject_id": project_subject_id
            })
        }).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())
    body = json.loads(result["body"])

    # Extract UUID from IRI (e.g., "http://example.org/subject/abc123" -> "abc123")
    subject_iri = body["subject"]["iri"]
    subject_uuid = subject_iri.split("/")[-1]

    # Construct project_subject_iri (not returned by CreateSubject)
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{test_project_id}/{project_subject_id}"

    yield (subject_uuid, project_subject_iri)

    # Cleanup: Remove subject after test
    try:
        lambda_client.invoke(
            FunctionName=physical_resources["RemoveSubjectFunction"],
            Payload=json.dumps({
                "body": json.dumps({
                    "project_subject_iri": project_subject_iri
                })
            }).encode("utf-8")
        )
    except Exception as e:
        print(f"Warning: Failed to cleanup subject {project_subject_iri}: {e}")


@pytest.fixture
def create_evidence_helper(physical_resources):
    """
    Fixture providing a helper function to create evidence for a subject.

    Returns:
        Callable: Function that creates evidence and returns the evidence IRI
    """
    lambda_client = get_client("lambda")
    created_evidence = []

    def _create_evidence(
        subject_id: str,
        term_iri: str,
        qualifiers: List[str] = None,
        evidence_type: str = "clinical_note",
        **kwargs
    ) -> str:
        """
        Create evidence for a subject.

        Args:
            subject_id: Subject UUID or IRI
            term_iri: HPO term IRI
            qualifiers: List of qualifier names (e.g., ["negated", "family"])
            evidence_type: Type of evidence (default: "clinical_note")
            **kwargs: Additional evidence fields

        Returns:
            str: Evidence IRI
        """
        # Build evidence payload
        evidence_data = {
            "subject_id": subject_id,
            "term_iri": term_iri,
            "type": evidence_type,
            "run_id": kwargs.get("run_id"),
            "batch_id": kwargs.get("batch_id"),
            "clinical_note_id": kwargs.get("clinical_note_id", f"note-{uuid.uuid4().hex[:8]}"),
            "encounter_id": kwargs.get("encounter_id", f"enc-{uuid.uuid4().hex[:8]}"),
            "creator_id": kwargs.get("evidence_creator_id", "test-creator"),
            "creator_type": kwargs.get("evidence_creator_type", "automated"),
            "creator_name": kwargs.get("evidence_creator_name", "Test Creator"),
            "note_timestamp": kwargs.get("note_timestamp", "2024-01-01"),
            "note_type": kwargs.get("note_type", "progress_note"),
            "provider_type": kwargs.get("provider_type", "physician"),
            "author_specialty": kwargs.get("author_specialty", "internal_medicine"),
            "span_start": kwargs.get("span_start", 0),
            "span_end": kwargs.get("span_end", 100),
        }

        # Add qualifiers if provided
        if qualifiers:
            # Standard qualifiers use short names to match production data
            standard_qualifiers = {"negated", "family", "hypothetical"}

            if isinstance(qualifiers, dict):
                # Dict format: convert to list
                # Handle {"onset": "HP:0003593"} or {"negated": true}
                qualifier_list = []
                for key, value in qualifiers.items():
                    # Standard qualifiers - use short names
                    if key in standard_qualifiers:
                        qualifier_list.append(key)
                    # HPO term as value - convert to full IRI
                    elif isinstance(value, str) and value.startswith("HP:"):
                        iri = value.replace("HP:", "http://purl.obolibrary.org/obo/HP_")
                        qualifier_list.append(iri)
                    # HPO term as key - convert to full IRI
                    elif key.startswith("HP_"):
                        qualifier_list.append(f"http://purl.obolibrary.org/obo/{key}")
                    elif key.startswith("http://"):
                        qualifier_list.append(key)

                if qualifier_list:
                    evidence_data["qualifiers"] = qualifier_list
            elif isinstance(qualifiers, list):
                # List format - preserve as-is for standard qualifiers, convert HPO terms
                qualifier_list = []
                for qual_name in qualifiers:
                    if qual_name in standard_qualifiers:
                        # Use short name
                        qualifier_list.append(qual_name)
                    elif isinstance(qual_name, str):
                        # Convert HPO terms to full IRIs
                        if qual_name.startswith("http://"):
                            qualifier_list.append(qual_name)
                        elif qual_name.startswith("HP:"):
                            iri = qual_name.replace("HP:", "http://purl.obolibrary.org/obo/HP_")
                            qualifier_list.append(iri)
                        elif qual_name.startswith("HP_"):
                            qualifier_list.append(f"http://purl.obolibrary.org/obo/{qual_name}")

                if qualifier_list:
                    evidence_data["qualifiers"] = qualifier_list

        # Create evidence
        response = lambda_client.invoke(
            FunctionName=physical_resources["CreateEvidenceFunction"],
            Payload=json.dumps({
                "body": json.dumps(evidence_data)
            }).encode("utf-8")
        )

        result = json.loads(response["Payload"].read())

        if result["statusCode"] in [200, 201]:
            body = json.loads(result["body"])
            created_evidence.append(body["evidence_id"])
            return body
        else:
            raise Exception(f"Failed to create evidence: {result}")

    yield _create_evidence

    # Cleanup: Remove all created evidence
    for evidence_id in created_evidence:
        try:
            lambda_client.invoke(
                FunctionName=physical_resources["RemoveEvidenceFunction"],
                Payload=json.dumps({
                    "body": json.dumps({"evidence_id": evidence_id})
                }).encode("utf-8")
            )
        except Exception as e:
            print(f"Warning: Failed to cleanup evidence {evidence_id}: {e}")


@pytest.fixture(scope="session")
def standard_hpo_terms():
    """Standard HPO terms for testing."""
    return {
        "seizure": "http://purl.obolibrary.org/obo/HP_0001250",
        "fever": "http://purl.obolibrary.org/obo/HP_0001945",
        "intellectual_disability": "http://purl.obolibrary.org/obo/HP_0001249",
        "abnormality_of_the_nervous_system": "http://purl.obolibrary.org/obo/HP_0000707",
        "phenotypic_abnormality": "http://purl.obolibrary.org/obo/HP_0000118",
        "abnormal_heart_morphology": "http://purl.obolibrary.org/obo/HP_0001627",
        "abnormality_of_head": "http://purl.obolibrary.org/obo/HP_0000234",
        "neoplasm": "http://purl.obolibrary.org/obo/HP_0002664",
        "disturbance_in_speech": "http://purl.obolibrary.org/obo/HP_0002167",
    }


@pytest.fixture
def query_athena(physical_resources):
    """
    Fixture providing a helper function to query Athena.

    Returns:
        Callable: Function that executes Athena query and returns results
    """
    athena_client = get_client("athena")
    s3_client = get_client("s3")

    def _query(sql: str, wait_timeout: int = 300) -> List[Dict]:
        """
        Execute Athena query and return results.

        Args:
            sql: SQL query string
            wait_timeout: Maximum seconds to wait for query completion

        Returns:
            List[Dict]: Query results as list of dicts
        """
        database = physical_resources["AthenaDatabase"]
        s3_bucket = physical_resources["PheBeeBucket"]
        output_location = f"s3://{s3_bucket}/athena-results/"

        # Start query execution
        response = athena_client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_location}
        )

        query_execution_id = response["QueryExecutionId"]

        # Wait for query to complete
        start_time = time.time()
        while True:
            result = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = result["QueryExecution"]["Status"]["State"]

            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break

            if time.time() - start_time > wait_timeout:
                raise TimeoutError(f"Athena query timeout after {wait_timeout}s")

            time.sleep(2)

        if status != "SUCCEEDED":
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise Exception(f"Athena query {status}: {reason}")

        # Get results
        results_response = athena_client.get_query_results(
            QueryExecutionId=query_execution_id
        )

        # Parse results
        rows = results_response["ResultSet"]["Rows"]
        if len(rows) == 0:
            return []

        # Extract column names from first row
        headers = [col["VarCharValue"] for col in rows[0]["Data"]]

        # Convert remaining rows to dicts
        result_dicts = []
        for row in rows[1:]:
            row_dict = {}
            for i, col in enumerate(row["Data"]):
                row_dict[headers[i]] = col.get("VarCharValue")
            result_dicts.append(row_dict)

        return result_dicts

    return _query


def pytest_collection_modifyitems(session, config, items):
    """Reorder tests to run marked tests first/last."""
    # Separate tests marked with 'run_first' and 'run_last' from other tests
    run_first_tests = [item for item in items if "run_first" in item.keywords]
    run_last_tests = [item for item in items if "run_last" in item.keywords]
    other_tests = [
        item
        for item in items
        if "run_first" not in item.keywords and "run_last" not in item.keywords
    ]

    # Reorder items: run_first -> other_tests -> run_last
    items[:] = run_first_tests + other_tests + run_last_tests


@pytest.fixture(scope="session")
def api_base_url(physical_resources):
    """
    API Gateway base URL from CloudFormation outputs.

    Returns:
        str: Base URL for API Gateway (e.g., https://abc123.execute-api.us-east-1.amazonaws.com/prod)
    """
    api_url = physical_resources.get("ApiGatewayUrl") or physical_resources.get("HttpApiUrl")
    if not api_url:
        pytest.skip("ApiGatewayUrl/HttpApiUrl not found in physical_resources - API Gateway may not be deployed")

    return api_url.rstrip("/")


@pytest.fixture(scope="session")
def sigv4_auth():
    """
    AWS SigV4 authentication for API Gateway requests.

    Creates an AWS4Auth instance configured for execute-api service.
    This is used with the requests library for authenticated API calls.

    Returns:
        AWS4Auth: Authentication handler for requests library
    """
    session = boto3.Session()
    credentials = session.get_credentials().get_frozen_credentials()
    region = session.region_name or "us-east-1"

    return AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        "execute-api",
        session_token=credentials.token,
    )


