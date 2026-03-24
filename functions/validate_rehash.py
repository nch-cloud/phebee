"""
Validate Rehash Lambda

Validates evidence table after rehashing by comparing to baseline stats
and verifying hash consistency.
"""

import time
import os
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena = boto3.client('athena')


def execute_athena_query(query, database):
    """Execute Athena query and wait for results."""
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={
            'OutputLocation': f's3://{os.environ["PHEBEE_BUCKET_NAME"]}/athena-results/'
        }
    )

    query_execution_id = response['QueryExecutionId']

    # Wait for completion
    max_wait_time = 300
    poll_interval = 2
    elapsed_time = 0

    while elapsed_time < max_wait_time:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

        time.sleep(poll_interval)
        elapsed_time += poll_interval

    if status != 'SUCCEEDED':
        error_msg = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        raise Exception(f"Athena query failed: {error_msg}")

    results = athena.get_query_results(QueryExecutionId=query_execution_id)
    return results


def lambda_handler(event, context):
    """
    Validate evidence table after rehashing.

    Input:
        {
            "runId": "uuid",
            "icebergDatabase": "database",
            "evidenceTable": "table",
            "region": "aws-region",
            "baseline": {
                "totalCount": 123456,
                "subjectCount": 1234,
                "sampleIds": ["id1", "id2", ...]
            }
        }

    Output:
        {
            "valid": true/false,
            "details": {
                "rowCountMatch": true/false,
                "subjectCountMatch": true/false,
                "nullHashCount": 0,
                "sampleHashVerified": true/false,
                "errors": [...]
            }
        }
    """
    try:
        run_id = event['runId']
        database = event['icebergDatabase']
        table = event['evidenceTable']
        region = event.get('region', 'us-east-1')
        baseline = event['baseline']

        logger.info(f"Validating rehash for {database}.{table}")
        logger.info(f"Baseline - Total: {baseline['totalCount']:,}, Subjects: {baseline['subjectCount']:,}")

        errors = []
        details = {}

        # Validation 1: Check total row count unchanged
        count_query = f"""
        SELECT
            COUNT(*) as total_count,
            COUNT(DISTINCT subject_id) as subject_count
        FROM {database}.{table}
        """

        logger.info("Checking row counts...")
        results = execute_athena_query(count_query, database)

        data_row = results['ResultSet']['Rows'][1]['Data']
        new_total_count = int(data_row[0]['VarCharValue'])
        new_subject_count = int(data_row[1]['VarCharValue'])

        row_count_match = (new_total_count == baseline['totalCount'])
        subject_count_match = (new_subject_count == baseline['subjectCount'])

        details['rowCountMatch'] = row_count_match
        details['subjectCountMatch'] = subject_count_match
        details['newTotalCount'] = new_total_count
        details['newSubjectCount'] = new_subject_count

        if not row_count_match:
            error_msg = f"Row count mismatch! Expected: {baseline['totalCount']:,}, Got: {new_total_count:,}"
            logger.error(error_msg)
            errors.append(error_msg)

        if not subject_count_match:
            error_msg = f"Subject count mismatch! Expected: {baseline['subjectCount']:,}, Got: {new_subject_count:,}"
            logger.error(error_msg)
            errors.append(error_msg)

        # Validation 2: Check for NULL hashes
        null_check_query = f"""
        SELECT COUNT(*) as null_count
        FROM {database}.{table}
        WHERE evidence_id IS NULL OR termlink_id IS NULL
        """

        logger.info("Checking for NULL hashes...")
        results = execute_athena_query(null_check_query, database)

        null_hash_count = int(results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
        details['nullHashCount'] = null_hash_count

        if null_hash_count > 0:
            error_msg = f"Found {null_hash_count:,} records with NULL hashes!"
            logger.error(error_msg)
            errors.append(error_msg)

        # Validation 3: Sample verification - recalculate hashes and verify correctness
        sample_records = baseline.get('sampleRecords', [])[:10]  # Check first 10 samples

        if sample_records:
            logger.info(f"Verifying {len(sample_records)} sampled records by recalculating hashes...")

            # Import hash functions from phebee-utils layer
            from phebee.utils.hash import generate_evidence_hash, generate_termlink_hash

            verified_count = 0
            hash_mismatches = []

            for i, sample in enumerate(sample_records):
                try:
                    # Build query to fetch this specific evidence record
                    # Match on stable identifiers that don't change during rehash
                    conditions = [
                        f"subject_id = '{sample['subject_id']}'",
                        f"clinical_note_id = '{sample['clinical_note_id']}'",
                        f"encounter_id = '{sample['encounter_id']}'",
                        f"term_iri = '{sample['term_iri']}'"
                    ]

                    if sample.get('span_start'):
                        conditions.append(f"text_annotation.span_start = {sample['span_start']}")
                    if sample.get('span_end'):
                        conditions.append(f"text_annotation.span_end = {sample['span_end']}")

                    fetch_query = f"""
                    SELECT
                        evidence_id,
                        termlink_id,
                        subject_id,
                        clinical_note_id,
                        encounter_id,
                        term_iri,
                        text_annotation.span_start as span_start,
                        text_annotation.span_end as span_end,
                        qualifiers,
                        creator.creator_id as creator_id
                    FROM {database}.{table}
                    WHERE {' AND '.join(conditions)}
                    LIMIT 1
                    """

                    results = execute_athena_query(fetch_query, database)

                    if len(results['ResultSet']['Rows']) < 2:  # Header + data row
                        logger.warning(f"Sample {i+1}: Record not found (may have been deleted)")
                        continue

                    row = results['ResultSet']['Rows'][1]['Data']
                    actual_evidence_id = row[0].get('VarCharValue')
                    actual_termlink_id = row[1].get('VarCharValue')
                    subject_id = row[2].get('VarCharValue')
                    clinical_note_id = row[3].get('VarCharValue')
                    encounter_id = row[4].get('VarCharValue')
                    term_iri = row[5].get('VarCharValue')
                    span_start = int(row[6].get('VarCharValue')) if row[6].get('VarCharValue') else None
                    span_end = int(row[7].get('VarCharValue')) if row[7].get('VarCharValue') else None
                    qualifiers_str = row[8].get('VarCharValue', '[]')
                    creator_id = row[9].get('VarCharValue')

                    # Parse qualifiers from Athena array format
                    import json
                    qualifiers = []
                    if qualifiers_str and qualifiers_str != '[]':
                        # Parse array<struct> format: [{qualifier_type=..., qualifier_value=...}]
                        import re
                        struct_pattern = r'\{qualifier_type=([^,]+), qualifier_value=([^}]+)\}'
                        matches = re.findall(struct_pattern, qualifiers_str)
                        for qt, qv in matches:
                            if qv.lower() not in ['false', '0']:  # Skip false qualifiers
                                qualifiers.append(f"{qt}:{qv}")

                    # Recalculate evidence_id hash
                    expected_evidence_id = generate_evidence_hash(
                        clinical_note_id=clinical_note_id,
                        encounter_id=encounter_id,
                        term_iri=term_iri,
                        span_start=span_start,
                        span_end=span_end,
                        qualifiers=qualifiers,
                        subject_id=subject_id,
                        creator_id=creator_id
                    )

                    # Recalculate termlink_id hash
                    subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
                    expected_termlink_id = generate_termlink_hash(
                        source_node_iri=subject_iri,
                        term_iri=term_iri,
                        qualifiers=qualifiers
                    )

                    # Verify hashes match
                    if expected_evidence_id == actual_evidence_id and expected_termlink_id == actual_termlink_id:
                        verified_count += 1
                    else:
                        mismatch = {
                            'sample_index': i,
                            'subject_id': subject_id,
                            'term_iri': term_iri[:50] + '...'
                        }
                        if expected_evidence_id != actual_evidence_id:
                            mismatch['evidence_id_mismatch'] = True
                        if expected_termlink_id != actual_termlink_id:
                            mismatch['termlink_id_mismatch'] = True
                        hash_mismatches.append(mismatch)
                        logger.error(f"Sample {i+1}: Hash mismatch! {mismatch}")

                except Exception as e:
                    logger.error(f"Sample {i+1}: Error verifying hash: {str(e)}")
                    continue

            sample_hash_verified = (verified_count == len(sample_records))

            details['sampleHashVerified'] = sample_hash_verified
            details['verifiedSampleCount'] = verified_count
            details['expectedSampleCount'] = len(sample_records)
            details['hashMismatches'] = hash_mismatches

            if not sample_hash_verified:
                error_msg = f"Sample hash verification failed! Expected {len(sample_records)}, verified {verified_count}"
                logger.error(error_msg)
                errors.append(error_msg)
            else:
                logger.info(f"✓ All {verified_count} sample hashes verified correctly")
        else:
            details['sampleHashVerified'] = True  # No samples to verify
            logger.info("No samples to verify (small dataset)")

        # Overall validation result
        valid = (
            row_count_match and
            subject_count_match and
            null_hash_count == 0 and
            details.get('sampleHashVerified', True)
        )

        details['errors'] = errors

        if valid:
            logger.info("✓ Rehash validation PASSED")
        else:
            logger.error("✗ Rehash validation FAILED")
            for error in errors:
                logger.error(f"  - {error}")

        return {
            "valid": valid,
            "details": details
        }

    except KeyError as e:
        logger.error(f"Missing required parameter: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error validating rehash: {str(e)}", exc_info=True)
        raise
