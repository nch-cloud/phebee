import json
from aws_lambda_powertools import Metrics, Logger, Tracer

from phebee.utils.aws import extract_body
from phebee.utils.eventbridge import SUBJECT_CREATED, SUBJECT_LINKED, fire_event
from phebee.utils.sparql import (
    project_exists,
    create_subject,
    link_subject_to_project,
)
from phebee.utils.neptune import execute_update
from phebee.utils.dynamodb import _get_table_name, get_subject_id
from phebee.utils.iceberg import materialize_subject_terms
import boto3

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def create_error_response(status_code, message):
    return {
        "statusCode": status_code,
        "body": json.dumps({"subject_created": False, "error": message}),
        "headers": {"Content-Type": "application/json"},
    }


def lambda_handler(event, context):
    logger.info(event)
    body = extract_body(event)

    project_id = body.get("project_id")
    project_subject_id = body.get("project_subject_id")

    # Validate required parameters
    if not project_id or not project_subject_id:
        return create_error_response(400, "project_id and project_subject_id are required")

    known_project_id = body.get("known_project_id")
    known_project_subject_id = body.get("known_project_subject_id")
    known_subject_iri = body.get("known_subject_iri")

    # Validate input consistency
    if known_project_id and not known_project_subject_id:
        return create_error_response(
            400,
            "If 'known_project_id' is provided, 'known_project_subject_id' is required.",
        )
    if known_project_id and known_subject_iri:
        return create_error_response(
            400, "'known_project_id' and 'known_subject_iri' cannot both be provided."
        )

    try:
        if not project_exists(project_id):
            return create_error_response(400, f"No project found with ID: {project_id}")
    except Exception as e:
        logger.error(f"Error checking project existence: {e}")
        return create_error_response(500, f"Error validating project: {str(e)}")

    # Determine subject IRI
    if known_subject_iri:
        # Verify subject exists by checking DynamoDB for any project mappings
        # Expected format: http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}
        parts = known_subject_iri.rstrip('/').split('/')
        if len(parts) < 5:
            return create_error_response(
                400, f"Invalid known_subject_iri format - must be a valid subject IRI"
            )
        subject_id = parts[-1]
        if not subject_id:
            return create_error_response(
                400, f"Invalid known_subject_iri format - missing subject identifier"
            )

        table_name = _get_table_name()
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)

        try:
            # Query SUBJECT# partition to see if subject has any project mappings
            response = table.query(
                KeyConditionExpression='PK = :pk',
                ExpressionAttributeValues={':pk': f'SUBJECT#{subject_id}'},
                Limit=1
            )
            if not response.get('Items'):
                return create_error_response(
                    400, f"No subject found with IRI: {known_subject_iri}"
                )
        except Exception as e:
            logger.error(f"Error checking subject existence: {e}")
            return create_error_response(
                500, f"Error verifying subject: {str(e)}"
            )
        subject_iri = known_subject_iri

    elif known_project_id and known_project_subject_id:
        # Look up subject_id from DynamoDB mapping
        table_name = _get_table_name()
        try:
            known_subject_id = get_subject_id(table_name, known_project_id, known_project_subject_id)
        except Exception as e:
            logger.error(f"Error looking up subject mapping: {e}")
            return create_error_response(
                500, f"Error looking up subject mapping: {str(e)}"
            )
        if not known_subject_id:
            return create_error_response(
                400,
                f"No subject found with Project ID: {known_project_id} and Project Subject ID: {known_project_subject_id}",
            )
        subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{known_subject_id}"

    else:
        # Create new subject atomically using conditional DynamoDB write
        # This prevents race conditions where concurrent requests create duplicate subjects

        # Step 1: Create subject in Neptune to get UUID-based subject_iri
        try:
            subject_iri = create_subject(project_id, project_subject_id)
        except Exception as e:
            logger.error(f"Error creating subject in Neptune: {e}")
            return create_error_response(500, f"Error creating subject: {str(e)}")

        project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}"
        subject_id = subject_iri.split("/")[-1]

        table_name = _get_table_name()
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)

        try:
            # Step 2: Atomically write DynamoDB mapping with condition that key doesn't exist
            # This ensures only one request can successfully create the mapping
            # Note: Can't use batch_writer with conditions, must use separate put_item calls
            table.put_item(
                Item={
                    'PK': f'PROJECT#{project_id}',
                    'SK': f'SUBJECT#{project_subject_id}',
                    'subject_id': subject_id
                },
                ConditionExpression='attribute_not_exists(PK)'
            )
            # If first write succeeded, write the reverse mapping
            try:
                table.put_item(
                    Item={
                        'PK': f'SUBJECT#{subject_id}',
                        'SK': f'PROJECT#{project_id}#SUBJECT#{project_subject_id}'
                    },
                    ConditionExpression='attribute_not_exists(PK)'
                )
            except Exception as reverse_error:
                # If reverse mapping fails, clean up the first mapping
                logger.error(f"Failed to write reverse mapping, cleaning up: {reverse_error}")
                try:
                    table.delete_item(Key={'PK': f'PROJECT#{project_id}', 'SK': f'SUBJECT#{project_subject_id}'})
                except Exception as cleanup_error:
                    logger.error(f"Failed to clean up forward mapping: {cleanup_error}")
                raise

            # Step 3: If conditional write succeeded, we won the race - link in Neptune
            try:
                link_subject_to_project(subject_iri, project_id, project_subject_id)
            except Exception as link_error:
                # If linking failed after DynamoDB write, clean up both stores
                logger.error(f"Failed to link subject to project, cleaning up: {link_error}")
                try:
                    # Clean up DynamoDB
                    with table.batch_writer() as batch:
                        batch.delete_item(Key={'PK': f'PROJECT#{project_id}', 'SK': f'SUBJECT#{project_subject_id}'})
                        batch.delete_item(Key={'PK': f'SUBJECT#{subject_id}', 'SK': f'PROJECT#{project_id}#SUBJECT#{project_subject_id}'})
                    # Clean up Neptune
                    execute_update(f"DELETE WHERE {{ <{project_subject_iri}> ?p ?o . }}")
                    execute_update(f"DELETE WHERE {{ ?s ?p <{project_subject_iri}> . }}")
                    execute_update(f"DELETE WHERE {{ <{subject_iri}> ?p ?o . }}")
                    execute_update(f"DELETE WHERE {{ ?s ?p <{subject_iri}> . }}")
                except Exception as cleanup_error:
                    logger.error(f"Failed to clean up after link failure: {cleanup_error}")
                raise

            # Success - fire event and return
            subject_data = {
                "iri": subject_iri,
                "subject_id": subject_id,
                "projects": {project_id: project_subject_id},
            }
            try:
                fire_event(SUBJECT_CREATED, subject_data)
            except Exception as e:
                logger.error(f"Failed to fire SUBJECT_CREATED event (non-critical): {e}")
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "subject_created": True,
                        "subject": subject_data,
                    }
                ),
                "headers": {"Content-Type": "application/json"},
            }

        except Exception as e:
            # Check if conditional write failed due to existing key (race condition)
            if 'ConditionalCheckFailedException' in str(e):
                # Another request won the race - clean up our Neptune subject and return existing
                logger.info(f"Conditional write failed - subject already exists for project {project_id}, subject {project_subject_id}")
                try:
                    execute_update(f"DELETE WHERE {{ <{project_subject_iri}> ?p ?o . }}")
                    execute_update(f"DELETE WHERE {{ ?s ?p <{project_subject_iri}> . }}")
                    execute_update(f"DELETE WHERE {{ <{subject_iri}> ?p ?o . }}")
                    execute_update(f"DELETE WHERE {{ ?s ?p <{subject_iri}> . }}")
                except Exception as cleanup_error:
                    logger.error(f"Failed to clean up Neptune data after race condition: {cleanup_error}")

                # Query for the existing subject that won the race
                try:
                    existing_subject_id = get_subject_id(table_name, project_id, project_subject_id)
                except Exception as lookup_error:
                    logger.error(f"Error looking up existing subject after race condition: {lookup_error}")
                    return create_error_response(500, f"Error looking up existing subject: {str(lookup_error)}")

                if existing_subject_id:
                    existing_subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{existing_subject_id}"
                    return {
                        "statusCode": 200,
                        "body": json.dumps(
                            {
                                "subject_created": False,
                                "subject": {
                                    "iri": existing_subject_iri,
                                    "subject_id": existing_subject_id,
                                    "projects": {project_id: project_subject_id},
                                },
                            }
                        ),
                        "headers": {"Content-Type": "application/json"},
                    }
                else:
                    # Shouldn't happen, but handle gracefully
                    return create_error_response(500, "Race condition detected but existing subject not found")
            else:
                # Other DynamoDB error - clean up Neptune and return error
                logger.error(f"Failed to create DynamoDB mapping: {e}")
                try:
                    execute_update(f"DELETE WHERE {{ <{project_subject_iri}> ?p ?o . }}")
                    execute_update(f"DELETE WHERE {{ ?s ?p <{project_subject_iri}> . }}")
                    execute_update(f"DELETE WHERE {{ <{subject_iri}> ?p ?o . }}")
                    execute_update(f"DELETE WHERE {{ ?s ?p <{subject_iri}> . }}")
                except Exception as cleanup_error:
                    logger.error(f"Failed to clean up Neptune data: {cleanup_error}")
                return create_error_response(500, f"Error creating subject: {str(e)}")

    # Check if this exact mapping already exists before attempting to link
    table_name = _get_table_name()
    try:
        existing_subject_id = get_subject_id(table_name, project_id, project_subject_id)
    except Exception as e:
        logger.error(f"Error checking existing subject mapping: {e}")
        return create_error_response(500, f"Error checking existing subject mapping: {str(e)}")

    if existing_subject_id:
        # Mapping already exists
        subject_id_from_iri = subject_iri.split("/")[-1]
        if existing_subject_id != subject_id_from_iri:
            # Trying to link different subject to same project_subject_id
            return create_error_response(400,
                f"Project subject ID '{project_subject_id}' already linked to a different subject")
        # Already linked, just return it without duplicate operations
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "subject_created": False,
                    "subject": {
                        "iri": subject_iri,
                        "subject_id": existing_subject_id,
                        "projects": {project_id: project_subject_id},
                    },
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }

    # Create DynamoDB mapping FIRST for existing subject linked to new project
    # Use conditional writes to prevent race conditions and duplicate event firing
    subject_id = subject_iri.split("/")[-1]
    table_name = _get_table_name()
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    try:
        # Atomically write DynamoDB mapping with condition that key doesn't exist
        table.put_item(
            Item={
                'PK': f'PROJECT#{project_id}',
                'SK': f'SUBJECT#{project_subject_id}',
                'subject_id': subject_id
            },
            ConditionExpression='attribute_not_exists(PK)'
        )
        # If first write succeeded, write the reverse mapping
        try:
            table.put_item(
                Item={
                    'PK': f'SUBJECT#{subject_id}',
                    'SK': f'PROJECT#{project_id}#SUBJECT#{project_subject_id}'
                },
                ConditionExpression='attribute_not_exists(PK)'
            )
        except Exception as reverse_error:
            # If reverse mapping fails, clean up the first mapping
            logger.error(f"Failed to write reverse mapping, cleaning up: {reverse_error}")
            try:
                table.delete_item(Key={'PK': f'PROJECT#{project_id}', 'SK': f'SUBJECT#{project_subject_id}'})
            except Exception as cleanup_error:
                logger.error(f"Failed to clean up forward mapping: {cleanup_error}")
            raise

        # If conditional write succeeded, we won the race - link in Neptune
        try:
            link_subject_to_project(subject_iri, project_id, project_subject_id)
        except Exception as link_error:
            # If linking failed after DynamoDB write, clean up DynamoDB
            logger.error(f"Failed to link subject to project, cleaning up DynamoDB: {link_error}")
            try:
                # Delete both directions of mapping atomically
                with table.batch_writer() as batch:
                    batch.delete_item(Key={'PK': f'PROJECT#{project_id}', 'SK': f'SUBJECT#{project_subject_id}'})
                    batch.delete_item(Key={'PK': f'SUBJECT#{subject_id}', 'SK': f'PROJECT#{project_id}#SUBJECT#{project_subject_id}'})
            except Exception as cleanup_error:
                logger.error(f"Failed to clean up DynamoDB mapping: {cleanup_error}")
            raise

        # Success - re-materialize subject BEFORE firing event
        # This ensures subject is fully queryable in new project when event consumers are notified
        try:
            logger.info(f"Re-materializing subject {subject_id} after linking to project {project_id}")
            materialize_subject_terms(subject_id)
        except Exception as e:
            logger.error(f"Failed to materialize subject after linking (non-critical): {e}")

        # Fire event after materialization completes
        try:
            fire_event(
                SUBJECT_LINKED,
                {"subject_iri": subject_iri, "projects": {project_id: project_subject_id}},
            )
        except Exception as e:
            logger.error(f"Failed to fire SUBJECT_LINKED event (non-critical): {e}")

    except Exception as e:
        # Check if conditional write failed due to existing key (race condition)
        if 'ConditionalCheckFailedException' in str(e):
            # Another request won the race - just return existing mapping without firing duplicate event
            logger.info(f"Conditional write failed - mapping already exists for project {project_id}, subject {project_subject_id}")
            # Fall through to return existing mapping
        else:
            # Other error - return structured error response
            logger.error(f"Failed to create DynamoDB mapping for existing subject link: {e}")
            return create_error_response(500, f"Error linking subject to project: {str(e)}")

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "subject_created": False,
                "subject": {
                    "iri": subject_iri,
                    "subject_id": subject_id,
                    "projects": {project_id: project_subject_id},
                },
            }
        ),
        "headers": {"Content-Type": "application/json"},
    }
