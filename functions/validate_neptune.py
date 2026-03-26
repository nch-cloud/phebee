"""
Validate Neptune Lambda

Validates Neptune subjects graph after bulk load by comparing to
analytical tables and checking sample consistency.
"""

import time
import os
import logging
import boto3
import random

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
    Validate Neptune subjects graph after bulk load.

    Input:
        {
            "runId": "uuid",
            "icebergDatabase": "database",
            "bySubjectTable": "table",
            "region": "aws-region",
            "baseline": {
                "expectedSubjectCount": 1234,
                "expectedTermlinkCount": 12345
            }
        }

    Output:
        {
            "valid": true/false,
            "details": {
                "subjectCountMatch": true/false,
                "termlinkCountMatch": true/false,
                "sampleVerificationPassed": true/false,
                "errors": [...]
            }
        }
    """
    try:
        from phebee.utils import sparql

        run_id = event['runId']
        database = event['icebergDatabase']
        by_subject_table = event['bySubjectTable']
        baseline = event['baseline']
        region = event.get('region', 'us-east-1')

        logger.info(f"Validating Neptune subjects graph")
        logger.info(f"Expected - Subjects: {baseline['expectedSubjectCount']:,}, Termlinks: {baseline['expectedTermlinkCount']:,}")

        errors = []
        details = {}

        # Validation 1: Check subject count matches
        logger.info("Querying Neptune for subject count...")
        subject_count_query = """
        SELECT (COUNT(DISTINCT ?s) AS ?subjectCount)
        WHERE {
            GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {
                ?s a <http://ods.nationwidechildrens.org/phebee#Subject>
            }
        }
        """

        result = sparql.execute_query(subject_count_query)
        actual_subject_count = 0

        if result and 'results' in result and 'bindings' in result['results']:
            bindings = result['results']['bindings']
            if bindings and 'subjectCount' in bindings[0]:
                actual_subject_count = int(bindings[0]['subjectCount']['value'])

        subject_count_match = (actual_subject_count == baseline['expectedSubjectCount'])
        details['subjectCountMatch'] = subject_count_match
        details['actualSubjectCount'] = actual_subject_count

        if not subject_count_match:
            error_msg = f"Subject count mismatch! Expected: {baseline['expectedSubjectCount']:,}, Got: {actual_subject_count:,}"
            logger.error(error_msg)
            errors.append(error_msg)
        else:
            logger.info(f"✓ Subject count matches: {actual_subject_count:,}")

        # Validation 2: Check termlink count matches
        logger.info("Querying Neptune for termlink count...")
        termlink_count_query = """
        SELECT (COUNT(?tl) AS ?termlinkCount)
        WHERE {
            GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {
                ?tl a <http://ods.nationwidechildrens.org/phebee#TermLink>
            }
        }
        """

        result = sparql.execute_query(termlink_count_query)
        actual_termlink_count = 0

        if result and 'results' in result and 'bindings' in result['results']:
            bindings = result['results']['bindings']
            if bindings and 'termlinkCount' in bindings[0]:
                actual_termlink_count = int(bindings[0]['termlinkCount']['value'])

        termlink_count_match = (actual_termlink_count == baseline['expectedTermlinkCount'])
        details['termlinkCountMatch'] = termlink_count_match
        details['actualTermlinkCount'] = actual_termlink_count

        if not termlink_count_match:
            error_msg = f"Termlink count mismatch! Expected: {baseline['expectedTermlinkCount']:,}, Got: {actual_termlink_count:,}"
            logger.error(error_msg)
            errors.append(error_msg)
        else:
            logger.info(f"✓ Termlink count matches: {actual_termlink_count:,}")

        # Validation 3: Comprehensive sample verification
        logger.info("Sampling subjects for comprehensive verification...")
        sample_query = f"""
        SELECT subject_id, term_iri, termlink_id, qualifiers, evidence_count
        FROM {database}.{by_subject_table}
        TABLESAMPLE BERNOULLI (1)
        LIMIT 10
        """

        results = execute_athena_query(sample_query, database)

        sample_verification_passed = True
        samples_checked = 0
        samples_matched = 0
        termlink_id_mismatches = []
        qualifier_mismatches = []
        evidence_count_warnings = []

        for row in results['ResultSet']['Rows'][1:]:  # Skip header
            if row['Data'] and len(row['Data']) >= 5:
                subject_id = row['Data'][0].get('VarCharValue')
                term_iri = row['Data'][1].get('VarCharValue')
                expected_termlink_id = row['Data'][2].get('VarCharValue')
                qualifiers_str = row['Data'][3].get('VarCharValue', '[]')
                evidence_count = row['Data'][4].get('VarCharValue')

                if subject_id and term_iri and expected_termlink_id:
                    samples_checked += 1
                    sample_passed = True

                    # Build expected termlink URI
                    expected_termlink_uri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}/term-link/{expected_termlink_id}"

                    # Parse qualifiers from Athena result
                    expected_qualifiers = []
                    if qualifiers_str and qualifiers_str != '[]':
                        # Parse array<struct> format: [{qualifier_type=..., qualifier_value=...}]
                        import re
                        struct_pattern = r'\{qualifier_type=([^,]+), qualifier_value=([^}]+)\}'
                        matches = re.findall(struct_pattern, qualifiers_str)
                        for qt, qv in matches:
                            expected_qualifiers.append((qt, qv))

                    # Query Neptune for this specific termlink
                    # Build subject IRI
                    subject_uri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"

                    # Construct query using string format for clarity
                    neptune_query = """
                    SELECT ?term ?qualifier
                    WHERE {{
                        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
                            <{subject_uri}> <http://ods.nationwidechildrens.org/phebee#hasTermLink> <{termlink_uri}> .
                            <{termlink_uri}> <http://ods.nationwidechildrens.org/phebee#hasTerm> ?term .
                            OPTIONAL {{ <{termlink_uri}> <http://ods.nationwidechildrens.org/phebee#hasQualifyingTerm> ?qualifier }}
                        }}
                    }}
                    """.format(subject_uri=subject_uri, termlink_uri=expected_termlink_uri)

                    logger.info(f"Querying Neptune for sample {samples_checked + 1}: {subject_id}")
                    logger.debug(f"SPARQL query: {neptune_query}")

                    try:
                        result = sparql.execute_query(neptune_query)

                        if not result or 'results' not in result or 'bindings' not in result['results']:
                            logger.error(f"Sample {samples_checked}: No Neptune results for termlink {expected_termlink_id}")
                            sample_passed = False
                            continue

                        bindings = result['results']['bindings']

                        # Check 1: Termlink exists with correct URI
                        if not bindings:
                            termlink_id_mismatches.append({
                                'subject_id': subject_id,
                                'term_iri': term_iri,
                                'expected_termlink_id': expected_termlink_id,
                                'error': 'Termlink URI not found in Neptune'
                            })
                            logger.error(f"Sample {samples_checked}: Termlink URI not found: {expected_termlink_uri}")
                            sample_passed = False
                            continue

                        # Check 2: Term IRI matches
                        actual_term_iri = bindings[0].get('term', {}).get('value')
                        if actual_term_iri != term_iri:
                            logger.error(f"Sample {samples_checked}: Term mismatch! Expected: {term_iri}, Got: {actual_term_iri}")
                            sample_passed = False
                            continue

                        # Check 3: Qualifiers match
                        actual_qualifiers = set()
                        for binding in bindings:
                            if 'qualifier' in binding:
                                qualifier_uri = binding['qualifier']['value']
                                actual_qualifiers.add(qualifier_uri)

                        # Build expected qualifier URIs
                        expected_qualifier_uris = set()
                        for qt, qv in expected_qualifiers:
                            if qt.startswith('http://') or qt.startswith('https://'):
                                expected_qualifier_uris.add(qt)
                            else:
                                expected_qualifier_uris.add(f"http://ods.nationwidechildrens.org/phebee#{qt}")

                        if expected_qualifier_uris != actual_qualifiers:
                            qualifier_mismatches.append({
                                'subject_id': subject_id,
                                'term_iri': term_iri,
                                'termlink_id': expected_termlink_id,
                                'expected': list(expected_qualifier_uris),
                                'actual': list(actual_qualifiers)
                            })
                            logger.error(f"Sample {samples_checked}: Qualifier mismatch!")
                            logger.error(f"  Expected: {expected_qualifier_uris}")
                            logger.error(f"  Actual: {actual_qualifiers}")
                            sample_passed = False

                        # Check 4: Log evidence count (informational only, Neptune doesn't store this)
                        if evidence_count:
                            logger.info(f"Sample {samples_checked}: Evidence count in analytical table: {evidence_count}")

                        if sample_passed:
                            samples_matched += 1
                            logger.info(f"✓ Sample {samples_checked}: All checks passed for {subject_id} / {term_iri[:50]}...")

                    except Exception as e:
                        logger.error(f"Error querying Neptune for sample {samples_checked}: {str(e)}", exc_info=True)
                        sample_passed = False

                    # Check all 10 samples, don't stop early
                    if samples_checked >= 10:
                        break

        # Require 100% match rate
        if samples_checked > 0:
            match_ratio = samples_matched / samples_checked
            sample_verification_passed = (match_ratio == 1.0)  # 100% threshold

            details['sampleVerificationPassed'] = sample_verification_passed
            details['samplesChecked'] = samples_checked
            details['samplesMatched'] = samples_matched
            details['termlinkIdMismatches'] = termlink_id_mismatches
            details['qualifierMismatches'] = qualifier_mismatches

            if not sample_verification_passed:
                error_msg = f"Sample verification failed! Only {samples_matched}/{samples_checked} samples matched (requires 100%)"
                logger.error(error_msg)
                errors.append(error_msg)

                if termlink_id_mismatches:
                    logger.error(f"  Termlink ID mismatches: {len(termlink_id_mismatches)}")
                if qualifier_mismatches:
                    logger.error(f"  Qualifier mismatches: {len(qualifier_mismatches)}")
            else:
                logger.info(f"✓ Sample verification passed: {samples_matched}/{samples_checked} matched (100%)")
        else:
            details['sampleVerificationPassed'] = True  # No samples to verify
            logger.info("No samples to verify")

        # Overall validation result
        valid = (
            subject_count_match and
            termlink_count_match and
            sample_verification_passed
        )

        details['errors'] = errors

        if valid:
            logger.info("✓ Neptune validation PASSED")
        else:
            logger.error("✗ Neptune validation FAILED")
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
        logger.error(f"Error validating Neptune: {str(e)}", exc_info=True)
        raise
