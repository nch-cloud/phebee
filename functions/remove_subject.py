from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.neptune import execute_update
from phebee.utils.aws import extract_body
from phebee.utils.dynamodb import _get_table_name, get_subject_id
from phebee.utils.iceberg import delete_subject_terms
import json
import boto3

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(event)

    body = extract_body(event)

    project_subject_iri = body.get("project_subject_iri")

    # Validate required parameter
    if not project_subject_iri:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "project_subject_iri is required"}),
            "headers": {"Content-Type": "application/json"},
        }

    # Extract identifiers from IRI
    # Expected format: http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}
    parts = project_subject_iri.rstrip('/').split('/')
    if len(parts) < 6:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Invalid project_subject_iri format - must be a valid project subject IRI"}),
            "headers": {"Content-Type": "application/json"},
        }

    project_subject_id = parts[-1]
    project_id = parts[-2]

    # Validate extracted IDs are non-empty
    if not project_subject_id or not project_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Invalid project_subject_iri format - missing project or subject identifiers"}),
            "headers": {"Content-Type": "application/json"},
        }

    # Look up subject_id from DynamoDB mapping (source of truth)
    table_name = _get_table_name()
    try:
        subject_id = get_subject_id(table_name, project_id, project_subject_id)
    except Exception as e:
        logger.error(f"Error looking up subject mapping: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error looking up subject mapping: {str(e)}"}),
            "headers": {"Content-Type": "application/json"},
        }

    if not subject_id:
        return {
            "statusCode": 404,
            "body": json.dumps({"message": "Subject not found"}),
            "headers": {"Content-Type": "application/json"},
        }

    subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # 1. Remove the specific DynamoDB mappings FIRST (atomically with batch_writer)
    try:
        # Delete both directions of mapping atomically
        with table.batch_writer() as batch:
            batch.delete_item(Key={'PK': f'PROJECT#{project_id}', 'SK': f'SUBJECT#{project_subject_id}'})
            batch.delete_item(Key={'PK': f'SUBJECT#{subject_id}', 'SK': f'PROJECT#{project_id}#SUBJECT#{project_subject_id}'})
        logger.info(f"Removed DynamoDB mappings for subject {subject_id}, project {project_id}")
    except Exception as e:
        logger.error(f"Error removing DynamoDB mappings: {e}")
        # Abort operation if DynamoDB deletion fails to prevent orphaned data
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Failed to remove subject mapping: {str(e)}"}),
            "headers": {"Content-Type": "application/json"},
        }

    # 2. Check if there are any remaining project mappings AFTER deletion
    remaining_mappings = []
    is_last_mapping = False
    try:
        response = table.query(
            KeyConditionExpression='PK = :pk',
            ExpressionAttributeValues={':pk': f'SUBJECT#{subject_id}'}
        )
        remaining_mappings = response.get('Items', [])
        logger.info(f"Subject {subject_id} has {len(remaining_mappings)} remaining project mapping(s) after deletion")
        # 3. Determine if this was the last mapping
        is_last_mapping = len(remaining_mappings) == 0
    except Exception as e:
        logger.error(f"Error querying remaining subject mappings: {e}")
        # If query fails, assume it's NOT the last mapping to avoid data loss
        # This leaves by_subject table and subject_iri intact for other potential projects
        is_last_mapping = False

    # 4. If this is the last mapping, cascade delete all evidence
    termlink_data_to_delete = []
    if is_last_mapping:
        try:
            from phebee.utils.iceberg import delete_all_evidence_for_subject
            result = delete_all_evidence_for_subject(subject_id)
            termlink_data_to_delete = result.get("termlink_data", [])
            logger.info(f"Cascade deleted {result.get('evidence_deleted', 0)} evidence records for subject {subject_id}")
        except Exception as e:
            logger.error(f"Error cascade deleting evidence: {e}")
            # Continue with deletion even if evidence cascade fails

    # 5. Always remove from Iceberg by_project_term table for this project
    # Only remove from by_subject table if it's the last mapping
    try:
        delete_subject_terms(
            subject_id,
            project_id,
            include_by_subject=is_last_mapping
        )
        if is_last_mapping:
            logger.info(f"Removed all Iceberg data for subject {subject_id} (last mapping)")
        else:
            logger.info(f"Removed Iceberg by_project_term data for subject {subject_id} in project {project_id}")
    except Exception as e:
        logger.error(f"Error removing Iceberg data: {e}")
        # Continue with deletion even if Iceberg cleanup fails

    # 6. Always delete project-specific Neptune triples for this project_subject_iri
    try:
        logger.info(f"Removing Neptune triples for project_subject_iri {project_subject_iri}")
        execute_update(f"DELETE WHERE {{ <{project_subject_iri}> ?p ?o . }}")
        execute_update(f"DELETE WHERE {{ ?s ?p <{project_subject_iri}> . }}")
    except Exception as e:
        logger.error(f"Error removing project-specific Neptune triples: {e}")
        # Continue with deletion even if Neptune cleanup fails

    # 7. If this was the last mapping, delete all term links and subject_iri from Neptune
    if is_last_mapping:
        # 7a. Delete term links that were associated with the evidence
        if termlink_data_to_delete:
            from phebee.utils.sparql import delete_term_link
            logger.info(f"Deleting {len(termlink_data_to_delete)} term links from Neptune")
            for termlink_data in termlink_data_to_delete:
                termlink_id = termlink_data.get("termlink_id")
                if termlink_id:
                    try:
                        termlink_iri = f"{subject_iri}/term-link/{termlink_id}"
                        delete_term_link(termlink_iri)
                        logger.info(f"Deleted term link: {termlink_iri}")
                    except Exception as e:
                        logger.error(f"Error deleting term link {termlink_iri}: {e}")
                        # Continue with other deletions even if one fails

        # 7b. Delete subject_iri from Neptune
        try:
            logger.info(f"This was the last mapping for subject {subject_id}, removing subject_iri from Neptune")
            execute_update(f"DELETE WHERE {{ <{subject_iri}> ?p ?o . }}")
            execute_update(f"DELETE WHERE {{ ?s ?p <{subject_iri}> . }}")
            logger.info(f"Removed Neptune triples for subject {subject_iri}")
        except Exception as e:
            logger.error(f"Error removing subject_iri Neptune triples: {e}")
            # Continue even if Neptune cleanup fails
    else:
        logger.info(f"Subject {subject_id} still has other project mappings, only unlinked from project {project_id}")

    if is_last_mapping:
        message = f"Subject removed: {subject_iri}"
    else:
        message = f"Subject unlinked from project {project_id}: {subject_iri}"

    return {
        "statusCode": 200,
        "body": json.dumps({"message": message}),
        "headers": {"Content-Type": "application/json"},
    }
