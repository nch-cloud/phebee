import json
import os
import time
import boto3
from aws_lambda_powertools import Logger, Tracer
from phebee.utils.aws import extract_body
from phebee.utils.hash import generate_termlink_hash
from phebee.utils.iceberg import parse_athena_struct_array

logger = Logger()
tracer = Tracer()

@logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """Query evidence records by termlink with pagination support."""
    try:
        body = extract_body(event)

        # Extract and validate parameters
        subject_id = body.get("subject_id")
        term_iri = body.get("term_iri")
        qualifiers = body.get("qualifiers", [])
        run_id = body.get("run_id")
        limit = min(body.get("limit", 100), 1000)
        next_token = body.get("next_token")

        # Validate required parameters
        if not subject_id or not term_iri:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": "subject_id and term_iri are required"})
            }

        # Compute termlink_id hash
        # Normalize qualifiers to match bulk import format (name:value pairs)
        # This ensures queries work for both API-created and bulk-imported evidence
        normalized_qualifiers = []
        for qualifier in qualifiers:
            if ":" in qualifier:
                # Already in name:value format - filter out false values
                name, value = qualifier.split(":", 1)
                if value.lower() not in ["false", "0"]:
                    normalized_qualifiers.append(qualifier)
            else:
                # Short name without value - convert to name:true format
                normalized_qualifiers.append(f"{qualifier}:true")

        # Generate hash
        subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
        termlink_id = generate_termlink_hash(subject_iri, term_iri, normalized_qualifiers)

        logger.info(f"Querying evidence for termlink_id={termlink_id}, subject_id={subject_id}, term_iri={term_iri}, qualifiers={qualifiers}")

        # Build Athena query
        database = os.environ['ICEBERG_DATABASE']
        table = os.environ['ICEBERG_EVIDENCE_TABLE']

        query = f"""
        SELECT
            evidence_id,
            run_id,
            batch_id,
            evidence_type,
            assertion_type,
            created_timestamp,
            created_date,
            source_level,
            subject_id,
            encounter_id,
            clinical_note_id,
            termlink_id,
            term_iri,
            note_context,
            creator,
            text_annotation,
            qualifiers,
            term_source
        FROM {database}.{table}
        WHERE subject_id = '{subject_id}'
          AND term_iri = '{term_iri}'
          AND termlink_id = '{termlink_id}'
        """

        # Add optional run_id filter
        if run_id:
            query += f"  AND run_id = '{run_id}'\n"

        query += "ORDER BY created_timestamp"

        # Execute query
        athena_client = boto3.client('athena')
        workgroup = os.environ.get('ATHENA_WORKGROUP', 'primary')

        # Check workgroup configuration
        wg_response = athena_client.get_work_group(WorkGroup=workgroup)
        wg_cfg = wg_response["WorkGroup"]["Configuration"]
        managed = wg_cfg.get("ManagedQueryResultsConfiguration", {}).get("Enabled", False)

        # Build execution parameters
        params = {
            "QueryString": query,
            "WorkGroup": workgroup
        }
        if not managed:
            params["ResultConfiguration"] = {
                "OutputLocation": f"s3://{os.environ['PHEBEE_BUCKET_NAME']}/athena-results/"
            }

        # Execute query
        response = athena_client.start_query_execution(**params)
        query_execution_id = response['QueryExecutionId']

        logger.info(f"Started Athena query execution: {query_execution_id}")

        # Wait for completion
        while True:
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(0.5)

        if status != 'SUCCEEDED':
            error = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            logger.error(f"Query failed: {error}")
            return {
                "statusCode": 500,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": f"Query failed: {error}"})
            }

        # Fetch results with pagination
        # Determine MaxResults (cap at 1000, handle header row on first page)
        if next_token:
            max_results = min(limit, 1000)
        else:
            max_results = min(limit + 1, 1000)

        get_results_params = {
            'QueryExecutionId': query_execution_id,
            'MaxResults': max_results
        }
        if next_token:
            get_results_params['NextToken'] = next_token

        results = athena_client.get_query_results(**get_results_params)

        # Parse results
        evidence_records = []

        if len(results['ResultSet']['Rows']) > 0:
            # Handle header row (only on first page)
            if next_token:
                columns = [
                    'evidence_id', 'run_id', 'batch_id', 'evidence_type', 'assertion_type',
                    'created_timestamp', 'created_date', 'source_level', 'subject_id',
                    'encounter_id', 'clinical_note_id', 'termlink_id', 'term_iri',
                    'note_context', 'creator', 'text_annotation', 'qualifiers', 'term_source'
                ]
                data_rows = results['ResultSet']['Rows']
            else:
                if len(results['ResultSet']['Rows']) > 1:
                    columns = [col['VarCharValue'] for col in results['ResultSet']['Rows'][0]['Data']]
                    data_rows = results['ResultSet']['Rows'][1:]
                else:
                    data_rows = []
                    columns = []

            # Process rows
            for row in data_rows:
                record = {}
                for i, cell in enumerate(row['Data']):
                    if i < len(columns):
                        value = cell.get('VarCharValue', '')

                        # Parse struct fields
                        if columns[i] in ['creator', 'note_context', 'text_annotation', 'term_source']:
                            if value and value.startswith('{'):
                                try:
                                    # Parse Athena ROW format: {key1=value1, key2=value2}
                                    parsed = {}
                                    content = value.strip('{}')
                                    pairs = content.split(', ')
                                    for pair in pairs:
                                        if '=' in pair:
                                            key, val = pair.split('=', 1)
                                            parsed[key.strip()] = val.strip()
                                    record[columns[i]] = parsed
                                except Exception as e:
                                    logger.warning(f"Error parsing struct field {columns[i]}: {e}")
                                    record[columns[i]] = value
                            else:
                                record[columns[i]] = None
                        elif columns[i] == 'qualifiers':
                            # Parse qualifier array
                            if value:
                                try:
                                    record[columns[i]] = parse_athena_struct_array(value)
                                except Exception as e:
                                    logger.warning(f"Error parsing qualifiers: {e}")
                                    record[columns[i]] = []
                            else:
                                record[columns[i]] = []
                        else:
                            record[columns[i]] = value
                evidence_records.append(record)

        # Get pagination info
        response_next_token = results.get('NextToken')
        has_more = response_next_token is not None

        # Get total count (first page only)
        total_count = None
        if not next_token:
            count_query = f"""
            SELECT COUNT(*) as total_count
            FROM {database}.{table}
            WHERE subject_id = '{subject_id}'
              AND term_iri = '{term_iri}'
              AND termlink_id = '{termlink_id}'
            """

            if run_id:
                count_query += f"  AND run_id = '{run_id}'"

            # Execute count query
            count_params = {
                "QueryString": count_query,
                "WorkGroup": workgroup
            }
            if not managed:
                count_params["ResultConfiguration"] = {
                    "OutputLocation": f"s3://{os.environ['PHEBEE_BUCKET_NAME']}/athena-results/"
                }

            count_response = athena_client.start_query_execution(**count_params)
            count_execution_id = count_response['QueryExecutionId']

            # Wait for count query to complete
            while True:
                count_result = athena_client.get_query_execution(QueryExecutionId=count_execution_id)
                count_status = count_result['QueryExecution']['Status']['State']

                if count_status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                time.sleep(0.5)

            if count_status == 'SUCCEEDED':
                count_results = athena_client.get_query_results(QueryExecutionId=count_execution_id)
                if len(count_results['ResultSet']['Rows']) > 1:
                    total_count = int(count_results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
            else:
                logger.warning(f"Count query failed, skipping total_count")

        # Build response
        response_body = {
            "subject_id": subject_id,
            "term_iri": term_iri,
            "qualifiers": qualifiers,
            "termlink_id": termlink_id,
            "evidence_count": len(evidence_records),
            "limit": limit,
            "has_more": has_more,
            "evidence": evidence_records
        }

        if response_next_token:
            response_body["next_token"] = response_next_token
        if total_count is not None:
            response_body["total_count"] = total_count

        logger.info(f"Returning {len(evidence_records)} evidence records, has_more={has_more}")

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(response_body)
        }

    except Exception as e:
        logger.error(f"Error querying evidence: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": f"Error querying evidence: {str(e)}"})
        }
