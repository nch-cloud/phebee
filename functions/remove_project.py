from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.constants import PHEBEE
from phebee.utils.neptune import execute_update
from phebee.utils.aws import extract_body
from phebee.utils.iceberg import _execute_athena_query, delete_subject_terms, delete_all_evidence_for_subject
from phebee.utils.dynamodb import _get_table_name
from phebee.utils.sparql import delete_term_link
import json
import os
import boto3

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(event)

    body = extract_body(event)

    project_id = body.get("project_id")

    if not project_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "project_id is required"}),
            "headers": {"Content-Type": "application/json"},
        }

    sparql = f"""
        PREFIX phebee: <{PHEBEE}>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        CLEAR GRAPH <{PHEBEE}/projects/{project_id}>
    """

    try:
        # 1. Query DynamoDB for all subjects in this project
        table_name = _get_table_name()
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)

        subjects_to_unlink = []
        try:
            # Query forward mappings: PK='PROJECT#{project_id}', SK='SUBJECT#{project_subject_id}'
            response = table.query(
                KeyConditionExpression='PK = :pk',
                ExpressionAttributeValues={':pk': f'PROJECT#{project_id}'}
            )

            for item in response.get('Items', []):
                # Parse SK: "SUBJECT#{project_subject_id}"
                sk = item.get('SK', '')
                if sk.startswith('SUBJECT#'):
                    project_subject_id = sk.split('#', 1)[1]
                    subject_id = item.get('subject_id', '')
                    if subject_id:
                        subjects_to_unlink.append({
                            "subject_id": subject_id,
                            "project_subject_id": project_subject_id
                        })

            logger.info(f"Found {len(subjects_to_unlink)} subjects to unlink from project {project_id}")
            logger.info(f"Subjects list: {subjects_to_unlink}")
        except Exception as e:
            logger.error(f"Error querying subjects for project {project_id}: {e}")
            # Continue with project deletion even if subject query fails

        # 2. For each subject, unlink it from this project
        for subject_data in subjects_to_unlink:
            subject_id = subject_data["subject_id"]
            project_subject_id = subject_data["project_subject_id"]

            try:
                logger.info(f"Unlinking subject {subject_id} (project_subject_id: {project_subject_id})")

                # 2a. Check if this is the subject's last project mapping BEFORE deleting
                response = table.query(
                    KeyConditionExpression='PK = :pk',
                    ExpressionAttributeValues={':pk': f'SUBJECT#{subject_id}'}
                )
                remaining_items = response.get('Items', [])
                # Subject will be orphaned if only 1 mapping remains (the one we're about to delete)
                is_last_mapping = len(remaining_items) == 1
                logger.info(f"Subject {subject_id}: remaining_mappings={len(remaining_items)}, is_last_mapping={is_last_mapping}")

                # 2b. Delete DynamoDB mappings
                with table.batch_writer() as batch:
                    batch.delete_item(Key={'PK': f'PROJECT#{project_id}', 'SK': f'SUBJECT#{project_subject_id}'})
                    batch.delete_item(Key={'PK': f'SUBJECT#{subject_id}', 'SK': f'PROJECT#{project_id}#SUBJECT#{project_subject_id}'})

                # 2c. If last mapping, cascade delete evidence
                termlink_data_to_delete = []
                if is_last_mapping:
                    try:
                        result = delete_all_evidence_for_subject(subject_id)
                        termlink_data_to_delete = result.get("termlink_data", [])
                        logger.info(f"Cascade deleted {result.get('evidence_deleted', 0)} evidence records for subject {subject_id}")
                    except Exception as e:
                        logger.error(f"Error cascade deleting evidence for subject {subject_id}: {e}")

                # 2d. Delete from analytical tables
                try:
                    delete_subject_terms(
                        subject_id,
                        project_id,
                        include_by_subject=is_last_mapping
                    )
                except Exception as e:
                    logger.error(f"Error removing analytical data for subject {subject_id}: {e}")

                # 2e. Delete project_subject_iri from Neptune
                project_subject_iri = f"{PHEBEE}/projects/{project_id}/{project_subject_id}"
                try:
                    execute_update(f"DELETE WHERE {{ <{project_subject_iri}> ?p ?o . }}")
                    execute_update(f"DELETE WHERE {{ ?s ?p <{project_subject_iri}> . }}")
                except Exception as e:
                    logger.error(f"Error deleting project_subject_iri {project_subject_iri}: {e}")

                # 2f. If last mapping, delete term links and subject_iri from Neptune
                if is_last_mapping:
                    subject_iri = f"{PHEBEE}/subjects/{subject_id}"

                    # Delete term links
                    for termlink_data in termlink_data_to_delete:
                        termlink_id = termlink_data.get("termlink_id")
                        if termlink_id:
                            try:
                                termlink_iri = f"{subject_iri}/term-link/{termlink_id}"
                                delete_term_link(termlink_iri)
                            except Exception as e:
                                logger.error(f"Error deleting term link {termlink_iri}: {e}")

                    # Delete subject_iri
                    try:
                        execute_update(f"DELETE WHERE {{ <{subject_iri}> ?p ?o . }}")
                        execute_update(f"DELETE WHERE {{ ?s ?p <{subject_iri}> . }}")
                    except Exception as e:
                        logger.error(f"Error deleting subject_iri {subject_iri}: {e}")

            except Exception as e:
                logger.error(f"Error unlinking subject {subject_id}: {e}")
                # Continue with other subjects even if one fails

        # 3. Clear Neptune named graph for this project
        execute_update(sparql)
        logger.info(f"Cleared Neptune graph for project {project_id}")

        # 4. Remove project data from Iceberg by_project_term analytical table
        try:
            database_name = os.environ.get('ICEBERG_DATABASE')
            by_project_term_table = os.environ.get('ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE')

            if database_name and by_project_term_table:
                delete_query = f"""
                DELETE FROM {database_name}.{by_project_term_table}
                WHERE project_id = '{project_id}'
                """
                _execute_athena_query(delete_query)
                logger.info(f"Removed Iceberg analytical data for project {project_id}")
            else:
                logger.warning("Iceberg environment variables not set, skipping analytical table cleanup")
        except Exception as iceberg_error:
            # Log but don't fail the overall operation if Iceberg cleanup fails
            logger.error(f"Failed to clean up Iceberg data for project {project_id}: {iceberg_error}")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Project {project_id} successfully removed."}),
            "headers": {"Content-Type": "application/json"},
        }

    except Exception as e:
        logger.exception("Failed to remove project")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Failed to remove project: {str(e)}"}),
            "headers": {"Content-Type": "application/json"},
        }