import os
import pytest
import subprocess
import boto3
import uuid
import json
from botocore.exceptions import ClientError
from botocore.config import Config
from requests_aws4auth import AWS4Auth

from phebee.utils.aws import get_client

from step_function_utils import start_step_function, wait_for_step_function_completion
from s3_utils import delete_s3_prefix
from constants import PROJECT_CONFIGS


def pytest_addoption(parser):
    parser.addoption(
        "--skip-ontology-updates",
        action="store_true",
        default=False,
        help="Skip slow HPO and MONDO graph updates during tests.",
    )


@pytest.fixture(scope="session")
def profile_name(request):
    profile_name = request.config.getoption("--profile")
    print(f"Using AWS session profile: {profile_name}")
    return profile_name


@pytest.fixture(scope="session")
def aws_session(request, profile_name):
    # Create a client for each service, so that they are ready to use.
    # If any other clients are needed, add them here so they are instantiated as well.
    get_client("s3", profile_name)
    get_client("cloudformation", profile_name)
    get_client("stepfunctions", profile_name)
    get_client("dynamodb", profile_name)

    lambda_config = Config(read_timeout=900, connect_timeout=900, retries={"max_attempts": 0})
    get_client("lambda", profile_name, lambda_config)
    
    session = boto3.Session(profile_name=profile_name)

    return session


@pytest.fixture(scope="session")
def sigv4_auth():
    session = boto3.Session()
    credentials = session.get_credentials().get_frozen_credentials()
    region = session.region_name
    return AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        "execute-api",
        session_token=credentials.token,
    )


@pytest.fixture(scope="session")
def stack_outputs(cloudformation_stack):
    cf_client = get_client("cloudformation")

    response = cf_client.describe_stacks(StackName=cloudformation_stack)
    outputs = response["Stacks"][0].get("Outputs", [])
    return {o["OutputKey"]: o["OutputValue"] for o in outputs}


@pytest.fixture(scope="session")
def api_base_url(stack_outputs):
    return stack_outputs["HttpApiUrl"].rstrip("/")  # Or whatever output name you used


@pytest.fixture(scope="session")
def cloudformation_stack(request, aws_session, profile_name):
    """
    Deploys a CloudFormation stack for testing purposes.
    This fixture sets up the necessary CloudFormation stack before running tests.
    It yields the stack name so that tests can verify the stack's creation and resources.
    After all tests are run, the stack is deleted to clean up the environment.
    Yields:
        str: The name of the CloudFormation stack.
    """

    # Note: keeping the aws_session dependency here so that the AWS clients are initialized correctly before everything else runs.
    cf_client = get_client("cloudformation")

    existing_stack = request.config.getoption("--existing-stack")
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
                "--no-confirm-changeset",  # Skip confirmation prompt
                "--no-fail-on-empty-changeset",  # Prevent failures on empty changesets
                "--resolve-s3",  # Create a bucket for deployment
                "--config-env",
                config_env,
            ]
            if profile_name:
                deploy_args.append("--profile")
                deploy_args.append(profile_name)
            subprocess.run(
                deploy_args,
                check=True,
            )

            # Step 3: Wait for the stack creation to complete using CloudFormation waiter
            waiter = cf_client.get_waiter("stack_create_complete")
            waiter.wait(StackName=stack_name)

        except subprocess.CalledProcessError as e:
            # Capture and display more detailed logs
            print(f"Stack creation failed: {e}")
            try:
                # Run describe-stack-events to get more information
                events = subprocess.run(
                    [
                        "aws",
                        "cloudformation",
                        "describe-stack-events",
                        "--stack-name",
                        stack_name,
                    ],
                    capture_output=True,
                    text=True,
                )
                print(events.stdout)
            except subprocess.CalledProcessError as event_error:
                print(f"Failed to get stack events: {event_error}")
            pytest.fail(f"Failed to run SAM CLI command: {e}")
    else:
        # Existing stack.  We will reset the database to make sure tests have clean space to run.
        force_reset = request.config.getoption("--force-database-reset")
        if force_reset:
            print("Resetting database...")
            from test_reset_database import reset_database
            reset_database(stack_name)

    yield stack_name  # This is where the tests will use the stack

    if not existing_stack:
        # Tear down the stack after tests
        try:
            teardown_args = [
                "sam",
                "delete",
                "--stack-name",
                stack_name,
                "--no-prompts",
            ]
            if profile_name:
                teardown_args.append("--profile")
                teardown_args.append(profile_name)
            subprocess.run(
                teardown_args,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            pytest.fail(f"Failed to delete stack: {e}")


@pytest.fixture(scope="session", autouse=True)
def set_env_variables_from_stack(cloudformation_stack):
    """
    Retrieve CloudFormation stack outputs and set environment variables for all tests.
    This fixture runs automatically for all tests in the session.
    """
    cf_client = get_client("cloudformation")

    try:
        # Fetch stack outputs
        response = cf_client.describe_stacks(StackName=cloudformation_stack)
        outputs = response["Stacks"][0]["Outputs"]

        print(outputs)

        # Loop through the outputs and set environment variables
        for output in outputs:
            if output["OutputKey"] == "DynamoDBTableName":
                os.environ["PheBeeDynamoTable"] = output["OutputValue"]
            if output["OutputKey"] == "Region":
                os.environ["Region"] = output["OutputValue"]
            if output["OutputKey"] == "NeptuneEndpoint":
                os.environ["NeptuneEndpoint"] = output["OutputValue"]
            if output["OutputKey"] == "NeptuneClusterIdentifier":
                os.environ["NeptuneClusterIdentifier"] = output["OutputValue"]

        print(f"Environment variables set from stack {cloudformation_stack}.")

    except ClientError as e:
        pytest.fail(f"Failed to retrieve stack outputs for {cloudformation_stack}: {e}")


@pytest.fixture(scope="session", autouse=True)
def physical_resources(aws_session, cloudformation_stack):
    cf_client = aws_session.client("cloudformation")
    resource_mapping = {}
    next_token = None  # Initialize next_token for pagination

    try:
        # Loop through all pages of results
        while True:
            if next_token:
                # If next_token is present, fetch the next page of results
                response = cf_client.list_stack_resources(
                    StackName=cloudformation_stack, NextToken=next_token
                )
            else:
                # Fetch the first page of results
                response = cf_client.list_stack_resources(
                    StackName=cloudformation_stack
                )

            # Process the current page of results
            for resource in response["StackResourceSummaries"]:
                logical_id = resource["LogicalResourceId"]
                physical_id = resource["PhysicalResourceId"]

                # Store only the physical ID
                resource_mapping[logical_id] = physical_id

            # Check for NextToken to handle pagination
            next_token = response.get("NextToken")
            if not next_token:
                break  # No more pages to retrieve, exit the loop

        # Yield the complete resource mapping
        yield resource_mapping

    except cf_client.exceptions.ClientError as e:
        print(f"Error retrieving stack resources: {e}")
        return None


@pytest.fixture(scope="session")
def update_hpo(request, cloudformation_stack, physical_resources):
    if request.config.getoption("--skip-ontology-updates"):
        print("Skipping HPO update as requested via --skip-ontology-updates parameter")
        yield
    else:
        from update_source_utils import update_source
        test_start_time = update_source(cloudformation_stack, "hpo", "UpdateHPOSFNArn")
        yield test_start_time

        delete_s3_prefix(physical_resources["PheBeeBucket"], "sources/hpo")


@pytest.fixture(scope="session")
def update_mondo(request, cloudformation_stack, physical_resources):
    if request.config.getoption("--skip-ontology-updates"):
        print(
            "Skipping Mondo update as requested via --skip-ontology-updates parameter"
        )
        yield
    else:
        from update_source_utils import update_source
        test_start_time = update_source(
            cloudformation_stack, "mondo", "UpdateMondoSFNArn"
        )
        yield test_start_time

        delete_s3_prefix(physical_resources["PheBeeBucket"], "sources/mondo")


@pytest.fixture(scope="session")
def update_eco(request, cloudformation_stack, physical_resources):
    if request.config.getoption("--skip-ontology-updates"):
        print("Skipping ECO update as requested via --skip-ontology-updates parameter")
        yield
    else:
        from update_source_utils import update_source
        test_start_time = update_source(cloudformation_stack, "eco", "UpdateECOSFNArn")
        yield test_start_time

        delete_s3_prefix(physical_resources["PheBeeBucket"], "sources/eco")


@pytest.fixture
def test_project_id(cloudformation_stack):
    lambda_client = get_client("lambda")
    project_id = f"test-project-{uuid.uuid4().hex[:8]}"
    payload = {"project_id": project_id, "project_label": "Lambda-Initiated Project"}

    print(f"Test project id: {project_id}")

    # Create the project by invoking the deployed Lambda
    create_response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-CreateProjectFunction",  # adjust if you've named it
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
        # Direct Lambda response
        body = lambda_response
    
    assert body.get("project_created") is True

    yield body.get("project_id")

    # Teardown
    delete_response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-RemoveProjectFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({"body": json.dumps({"project_id": project_id})}),
    )

    result = json.loads(delete_response["Payload"].read().decode("utf-8"))
    if delete_response["StatusCode"] != 200:
        print(f"WARNING: Teardown failed: {result}")


@pytest.fixture
def create_test_subject(physical_resources):
    lambda_client = get_client("lambda")
    created_subjects = []
    project_id = f"test-proj-{uuid.uuid4().hex[:6]}"

    # Create project
    project_response = lambda_client.invoke(
        FunctionName=physical_resources["CreateProjectFunction"],
        Payload=json.dumps(
            {
                "body": json.dumps(
                    {"project_id": project_id, "project_label": project_id}
                )
            }
        ).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    raw_response = json.loads(project_response["Payload"].read().decode("utf-8"))
    print(f"Raw project response: {raw_response}")
    project_body = json.loads(raw_response["body"])
    print(project_body)

    def _make_subject():
        project_subject_id = f"test-subj-{uuid.uuid4().hex[:6]}"
        payload = {"project_id": project_id, "project_subject_id": project_subject_id}

        response = lambda_client.invoke(
            FunctionName=physical_resources["CreateSubjectFunction"],
            Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
            InvocationType="RequestResponse",
        )

        body = json.loads(response["Payload"].read())

        print(body)

        subject_data = json.loads(body["body"])["subject"]

        created_subjects.append(
            {"project_id": project_id, "project_subject_id": project_subject_id}
        )

        return subject_data

    yield _make_subject

    # Cleanup
    for subj in created_subjects:
        lambda_client.invoke(
            FunctionName=physical_resources["RemoveSubjectFunction"],
            Payload=json.dumps(
                {
                    "body": json.dumps(
                        {
                            "project_id": subj["project_id"],
                            "project_subject_id": subj["project_subject_id"],
                        }
                    )
                }
            ).encode("utf-8"),
            InvocationType="RequestResponse",
        )

    lambda_client.invoke(
        FunctionName=physical_resources["RemoveProjectFunction"],
        Payload=json.dumps({"body": json.dumps({"project_id": project_id})}).encode(
            "utf-8"
        ),
        InvocationType="RequestResponse",
    )


# Fixture to create the test project
@pytest.fixture(scope="session")
def create_test_project(request, cloudformation_stack, physical_resources):
    config = PROJECT_CONFIGS[request.param]
    resources = physical_resources
    lambda_client = get_client("lambda")
    try:
        response = lambda_client.invoke(
            FunctionName=resources["CreateProjectFunction"],
            Payload=json.dumps(
                {
                    "project_id": config["PROJECT_ID"],
                    "project_label": config["PROJECT_LABEL"],
                }
            ),
        )
        result = json.loads(response["Payload"].read())
        assert result["statusCode"] == 200, (
            f"Failed to create the {config['PROJECT_ID']} project"
        )
        yield
    except ClientError as e:
        pytest.fail(f"Failed to create test project: {e}")

    finally:
        # Teardown: remove project and connected triplets
        response = lambda_client.invoke(
            FunctionName=resources["RemoveProjectFunction"],
            Payload=json.dumps(
                {
                    "project_id": config["PROJECT_ID"],
                    "project_label": config["PROJECT_LABEL"],
                }
            ),
        )
        result = json.loads(response["Payload"].read())
        assert result["statusCode"] == 200, (
            f"Failed to remove the {config['PROJECT_ID']} project"
        )


@pytest.fixture
def test_payload(test_project_id):
    import uuid
    subject_id = f"subj-{uuid.uuid4()}"
    term_iri = "http://purl.obolibrary.org/obo/HP_0004322"
    encounter_id = str(uuid.uuid4())
    
    return [
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": "note-001",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2"
                }
            ]
        }
    ]


@pytest.fixture
def test_payload_with_qualifiers(test_project_id):
    import uuid
    subject_id = f"subj-{uuid.uuid4()}"
    term_iri = "http://purl.obolibrary.org/obo/HP_0004322"
    encounter_id = str(uuid.uuid4())
    
    return [
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": "note-qual-1",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2",
                    "contexts": {
                        "negated": 1
                    }
                }
            ]
        },
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": "note-qual-2",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2",
                    "contexts": {
                        "hypothetical": 1
                    }
                }
            ]
        }
    ]


def pytest_collection_modifyitems(session, config, items):
    # Separate tests marked with 'run_first' from other tests
    run_first_tests = [item for item in items if "run_first" in item.keywords]
    run_end_tests = [item for item in items if "run_last" in item.keywords]
    other_tests = [
        item
        for item in items
        if "run_first" not in item.keywords and "run_last" not in item.keywords
    ]

    # Reorder items: run_first -> other_tests -> run_last
    items[:] = run_first_tests + other_tests + run_end_tests

    # Print info message
    config.pluginmanager.get_plugin("terminalreporter").write_line(
        "\n Reordered tests so that tests marked with 'run_first' run at the start and tests marked with 'run_last' run at the end of the session."
    )
