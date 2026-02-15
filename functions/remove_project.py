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
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def _retry_iceberg_operation(operation_func, max_retries=3, initial_delay=1.0):
    """
    Retry an Iceberg operation with exponential backoff on ICEBERG_COMMIT_ERROR.

    AWS best practice: Retry failed transactions by reading latest metadata
    and attempting the write operation again.

    Args:
        operation_func: Callable that performs the Iceberg operation
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay in seconds before first retry (default: 1.0)

    Returns:
        Result from operation_func

    Raises:
        Exception: If all retries are exhausted or non-retryable error occurs
    """
    delay = initial_delay
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return operation_func()
        except Exception as e:
            error_msg = str(e)
            last_exception = e

            # Only retry on Iceberg commit errors (optimistic concurrency conflicts)
            if "ICEBERG_COMMIT_ERROR" in error_msg and attempt < max_retries:
                logger.warning(
                    f"Iceberg commit conflict (attempt {attempt + 1}/{max_retries + 1}): {error_msg}. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                # Non-retryable error or retries exhausted
                raise

    # Should not reach here, but raise last exception if we do
    raise last_exception


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

        # 2. Process subjects with retry logic for Iceberg conflicts
        # Uses parallel processing with retry logic to handle optimistic concurrency conflicts.
        # Per AWS best practice: retry failed transactions by reading latest metadata.
        termlink_data_by_subject = {}
        neptune_cleanup_tasks = []
        athena_futures = []

        # Process subjects in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=10) as executor:
            for subject_data in subjects_to_unlink:
                subject_id = subject_data["subject_id"]
                project_subject_id = subject_data["project_subject_id"]

                # 2a. Check if this is the subject's last project mapping BEFORE deleting
                response = table.query(
                    KeyConditionExpression='PK = :pk',
                    ExpressionAttributeValues={':pk': f'SUBJECT#{subject_id}'}
                )
                remaining_items = response.get('Items', [])
                is_last_mapping = len(remaining_items) == 1
                logger.info(f"Subject {subject_id}: remaining_mappings={len(remaining_items)}, is_last_mapping={is_last_mapping}")

                # 2b. Delete DynamoDB mappings (fast, no conflicts)
                with table.batch_writer() as batch:
                    batch.delete_item(Key={'PK': f'PROJECT#{project_id}', 'SK': f'SUBJECT#{project_subject_id}'})
                    batch.delete_item(Key={'PK': f'SUBJECT#{subject_id}', 'SK': f'PROJECT#{project_id}#SUBJECT#{project_subject_id}'})

                # 2c. Submit Iceberg delete operations with retry logic
                if is_last_mapping:
                    # Delete evidence - wrapped with retry logic
                    def delete_evidence_with_retry(sid=subject_id):
                        return _retry_iceberg_operation(
                            lambda: delete_all_evidence_for_subject(sid),
                            max_retries=3
                        )

                    future = executor.submit(delete_evidence_with_retry)
                    athena_futures.append((future, subject_id, "delete_evidence", is_last_mapping))

                # Delete from analytical tables - wrapped with retry logic
                def delete_terms_with_retry(sid=subject_id, pid=project_id, is_last=is_last_mapping):
                    return _retry_iceberg_operation(
                        lambda: delete_subject_terms(sid, pid, is_last),
                        max_retries=3
                    )

                future = executor.submit(delete_terms_with_retry)
                athena_futures.append((future, subject_id, "delete_subject_terms", is_last_mapping))

                # 2d. Collect Neptune cleanup task
                project_subject_iri = f"{PHEBEE}/projects/{project_id}/{project_subject_id}"
                neptune_cleanup_tasks.append({
                    "project_subject_iri": project_subject_iri,
                    "subject_id": subject_id,
                    "is_last_mapping": is_last_mapping
                })

            # Wait for all Athena operations to complete and collect results
            failed_operations = []
            for future, subject_id, operation, is_last_mapping in athena_futures:
                try:
                    result = future.result()
                    if operation == "delete_evidence" and result:
                        termlink_data_by_subject[subject_id] = result.get("termlink_data", [])
                        logger.info(f"Cascade deleted {result.get('evidence_deleted', 0)} evidence records for subject {subject_id}")
                except Exception as e:
                    logger.error(f"Error in {operation} for subject {subject_id} after retries: {e}")
                    failed_operations.append(f"{operation} for subject {subject_id}: {str(e)}")

        # Check if any operations failed after all retries
        if failed_operations:
            error_msg = f"Failed to complete project removal for {project_id}. {len(failed_operations)} operation(s) failed: {'; '.join(failed_operations)}"
            logger.error(error_msg)
            raise Exception(error_msg)

        # 3. Perform Neptune cleanup (after Athena completes)
        for task in neptune_cleanup_tasks:
            try:
                # Delete project_subject_iri from Neptune
                project_subject_iri = task["project_subject_iri"]
                execute_update(f"DELETE WHERE {{ <{project_subject_iri}> ?p ?o . }}")
                execute_update(f"DELETE WHERE {{ ?s ?p <{project_subject_iri}> . }}")

                # If last mapping, delete term links and subject_iri
                if task["is_last_mapping"]:
                    subject_id = task["subject_id"]
                    subject_iri = f"{PHEBEE}/subjects/{subject_id}"
                    termlink_data = termlink_data_by_subject.get(subject_id, [])

                    # Delete term links
                    for termlink_datum in termlink_data:
                        termlink_id = termlink_datum.get("termlink_id")
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
                logger.error(f"Error in Neptune cleanup: {e}")

        # 4. Clear Neptune graph and Iceberg project data in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit Neptune CLEAR GRAPH
            neptune_future = executor.submit(execute_update, sparql)

            # Submit Iceberg project cleanup
            iceberg_future = None
            database_name = os.environ.get('ICEBERG_DATABASE')
            by_project_term_table = os.environ.get('ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE')

            if database_name and by_project_term_table:
                delete_query = f"""
                DELETE FROM {database_name}.{by_project_term_table}
                WHERE project_id = '{project_id}'
                """
                iceberg_future = executor.submit(_execute_athena_query, delete_query)
            else:
                logger.warning("Iceberg environment variables not set, skipping analytical table cleanup")

            # Wait for Neptune to complete
            try:
                neptune_future.result()
                logger.info(f"Cleared Neptune graph for project {project_id}")
            except Exception as e:
                logger.error(f"Error clearing Neptune graph: {e}")

            # Wait for Iceberg to complete
            if iceberg_future:
                try:
                    iceberg_future.result()
                    logger.info(f"Removed Iceberg analytical data for project {project_id}")
                except Exception as iceberg_error:
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