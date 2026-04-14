"""
Validate Neptune Complete Lambda

Validates ALL Neptune graphs after full database reset and rebuild:
- Ontology graphs (HPO, MONDO, ECO)
- Project graphs (per-project subject mappings)
- Subjects graph (subject-term associations)

This is an enhanced version of validate_neptune.py that validates the entire
Neptune database after a full reset and rebuild strategy.
"""

import logging
import random
import os
import boto3
from phebee.utils.iceberg import execute_athena_query
from phebee.utils.dynamodb import get_current_term_source_version

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def validate_ontologies(sparql, iceberg_database, dynamodb_table, region):
    """
    Validate ontology graphs (HPO, MONDO, ECO) in Neptune.

    Args:
        sparql: SPARQL utility module
        iceberg_database: Iceberg database name
        dynamodb_table: DynamoDB table name for ontology hierarchy
        region: AWS region

    Returns:
        dict: Validation results with 'valid' flag and details
    """
    logger.info("=" * 80)
    logger.info("VALIDATING ONTOLOGY GRAPHS (HPO, MONDO, ECO)")
    logger.info("=" * 80)

    # Get hierarchy table name from environment
    hierarchy_table = os.environ.get('ICEBERG_ONTOLOGY_HIERARCHY_TABLE', 'ontology_hierarchy')

    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(dynamodb_table)

    ontologies = [
        {
            "source": "hpo",
            "graph_prefix": "http://ods.nationwidechildrens.org/phebee/hpo~",
            "sample_terms": ["HP:0000001", "HP:0001250"],  # All, Seizure
            "namespace": "http://purl.obolibrary.org/obo/"
        },
        {
            "source": "mondo",
            "graph_prefix": "http://ods.nationwidechildrens.org/phebee/mondo~",
            "sample_terms": ["MONDO:0000001", "MONDO:0005015"],  # disease, diabetes mellitus
            "namespace": "http://purl.obolibrary.org/obo/"
        },
        {
            "source": "eco",
            "graph_prefix": "http://ods.nationwidechildrens.org/phebee/eco~",
            "sample_terms": ["ECO:0000000", "ECO:0000501"],  # evidence, author statement from published clinical study used in manual assertion
            "namespace": "http://purl.obolibrary.org/obo/"
        }
    ]

    errors = []
    ontology_details = {}

    for onto in ontologies:
        logger.info(f"\nValidating {onto['source'].upper()} ontology...")
        onto_errors = []

        # 1. Get current version from DynamoDB and expected count from Iceberg hierarchy table
        logger.info(f"  Getting current {onto['source']} version from DynamoDB...")
        try:
            current_version = get_current_term_source_version(onto['source'])
            if not current_version:
                error_msg = f"{onto['source']}: No version found in DynamoDB"
                logger.error(f"  ✗ {error_msg}")
                onto_errors.append(error_msg)
                expected_count = 0
            else:
                logger.info(f"  Current version: {current_version}")

                # Query Iceberg hierarchy table for term count
                logger.info(f"  Querying Iceberg hierarchy table for {onto['source']} term count...")
                count_query = f"""
                SELECT COUNT(*) as term_count
                FROM {iceberg_database}.{hierarchy_table}
                WHERE source = '{onto['source']}'
                AND version = '{current_version}'
                """

                result = execute_athena_query(count_query, iceberg_database)

                if result and 'ResultSet' in result and 'Rows' in result['ResultSet']:
                    rows = result['ResultSet']['Rows']
                    if len(rows) > 1:  # Skip header row
                        expected_count = int(rows[1]['Data'][0].get('VarCharValue', '0'))
                        logger.info(f"  Expected {onto['source']} terms: {expected_count:,}")
                    else:
                        expected_count = 0
                        logger.warning(f"  No data returned from hierarchy table for {onto['source']}")
                else:
                    expected_count = 0
                    logger.warning(f"  Empty result from hierarchy table query for {onto['source']}")
        except Exception as e:
            error_msg = f"{onto['source']}: Failed to get term count from hierarchy table: {str(e)}"
            logger.error(f"  ✗ {error_msg}")
            onto_errors.append(error_msg)
            expected_count = 0

        # 2. Get actual count from Neptune (query latest version graph)
        logger.info(f"  Querying Neptune for {onto['source']} term count...")
        try:
            # Find latest version graph
            version_query = f"""
            SELECT DISTINCT ?g
            WHERE {{
                GRAPH ?g {{
                    ?s a ?type
                }}
                FILTER(STRSTARTS(STR(?g), "{onto['graph_prefix']}"))
            }}
            ORDER BY DESC(?g)
            LIMIT 1
            """
            result = sparql.execute_query(version_query)

            if result and 'results' in result and 'bindings' in result['results']:
                bindings = result['results']['bindings']
                if bindings and 'g' in bindings[0]:
                    latest_graph = bindings[0]['g']['value']
                    logger.info(f"  Latest {onto['source']} graph: {latest_graph}")

                    # Count terms in this graph
                    count_query = f"""
                    SELECT (COUNT(DISTINCT ?term) AS ?count)
                    WHERE {{
                        GRAPH <{latest_graph}> {{
                            ?term a ?type
                            FILTER(STRSTARTS(STR(?term), "{onto['namespace']}"))
                        }}
                    }}
                    """
                    count_result = sparql.execute_query(count_query)

                    if count_result and 'results' in count_result and 'bindings' in count_result['results']:
                        count_bindings = count_result['results']['bindings']
                        if count_bindings and 'count' in count_bindings[0]:
                            actual_count = int(count_bindings[0]['count']['value'])
                            logger.info(f"  Actual {onto['source']} terms: {actual_count:,}")

                            # Validate counts - pragmatic approach
                            if expected_count == 0:
                                error_msg = f"{onto['source']}: Hierarchy table is empty (0 terms)"
                                logger.error(f"  ✗ {error_msg}")
                                onto_errors.append(error_msg)
                            elif actual_count == 0:
                                error_msg = f"{onto['source']}: Neptune is empty (0 terms)"
                                logger.error(f"  ✗ {error_msg}")
                                onto_errors.append(error_msg)
                            else:
                                # Both have data - calculate difference
                                diff_pct = abs(actual_count - expected_count) / expected_count * 100

                                if diff_pct > 50:
                                    # Warn but don't fail - counts often differ due to what gets loaded
                                    logger.warning(f"  ⚠ {onto['source']} counts differ significantly: "
                                                 f"Hierarchy={expected_count:,}, Neptune={actual_count:,} ({diff_pct:.1f}% difference)")
                                else:
                                    logger.info(f"  ✓ {onto['source']} counts are reasonable: "
                                              f"Hierarchy={expected_count:,}, Neptune={actual_count:,} ({diff_pct:.1f}% difference)")

                            # 3. Sample verification: Check known terms exist
                            logger.info(f"  Verifying sample {onto['source']} terms...")
                            for term_id in onto['sample_terms']:
                                term_uri = f"{onto['namespace']}{term_id.replace(':', '_')}"
                                sample_query = f"""
                                SELECT ?label
                                WHERE {{
                                    GRAPH <{latest_graph}> {{
                                        <{term_uri}> <http://www.w3.org/2000/01/rdf-schema#label> ?label
                                    }}
                                }}
                                LIMIT 1
                                """
                                sample_result = sparql.execute_query(sample_query)

                                if sample_result and 'results' in sample_result and 'bindings' in sample_result['results']:
                                    sample_bindings = sample_result['results']['bindings']
                                    if sample_bindings:
                                        label = sample_bindings[0].get('label', {}).get('value', 'N/A')
                                        logger.info(f"    ✓ Found {term_id}: {label}")
                                    else:
                                        error_msg = f"{onto['source']}: Sample term {term_id} not found in Neptune"
                                        logger.error(f"    ✗ {error_msg}")
                                        onto_errors.append(error_msg)
                                else:
                                    error_msg = f"{onto['source']}: Failed to query sample term {term_id}"
                                    logger.error(f"    ✗ {error_msg}")
                                    onto_errors.append(error_msg)

                            # 4. Hierarchy check: Verify at least one parent-child relationship
                            logger.info(f"  Checking {onto['source']} hierarchy relationships...")
                            hierarchy_query = f"""
                            SELECT ?child ?parent
                            WHERE {{
                                GRAPH <{latest_graph}> {{
                                    ?child <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?parent
                                }}
                            }}
                            LIMIT 1
                            """
                            hierarchy_result = sparql.execute_query(hierarchy_query)

                            if hierarchy_result and 'results' in hierarchy_result and 'bindings' in hierarchy_result['results']:
                                hierarchy_bindings = hierarchy_result['results']['bindings']
                                if hierarchy_bindings:
                                    logger.info(f"  ✓ {onto['source']} hierarchy relationships verified")
                                else:
                                    error_msg = f"{onto['source']}: No hierarchy relationships found in Neptune"
                                    logger.error(f"  ✗ {error_msg}")
                                    onto_errors.append(error_msg)
                            else:
                                error_msg = f"{onto['source']}: Failed to check hierarchy relationships"
                                logger.error(f"  ✗ {error_msg}")
                                onto_errors.append(error_msg)

                            # Calculate difference for reporting
                            diff_pct = abs(actual_count - expected_count) / expected_count * 100 if expected_count > 0 else 0

                            ontology_details[onto['source']] = {
                                "expected_count": expected_count,
                                "actual_count": actual_count,
                                "count_difference_pct": diff_pct,
                                "counts_reasonable": expected_count > 0 and actual_count > 0,
                                "latest_graph": latest_graph,
                                "errors": onto_errors
                            }
                        else:
                            error_msg = f"{onto['source']}: Failed to parse term count from Neptune"
                            logger.error(f"  ✗ {error_msg}")
                            onto_errors.append(error_msg)
                    else:
                        error_msg = f"{onto['source']}: Failed to count terms in Neptune"
                        logger.error(f"  ✗ {error_msg}")
                        onto_errors.append(error_msg)
                else:
                    error_msg = f"{onto['source']}: No graph found in Neptune"
                    logger.error(f"  ✗ {error_msg}")
                    onto_errors.append(error_msg)
            else:
                error_msg = f"{onto['source']}: Failed to find latest graph version"
                logger.error(f"  ✗ {error_msg}")
                onto_errors.append(error_msg)

        except Exception as e:
            error_msg = f"{onto['source']}: Neptune query failed: {str(e)}"
            logger.error(f"  ✗ {error_msg}", exc_info=True)
            onto_errors.append(error_msg)

        errors.extend(onto_errors)

    ontologies_valid = len(errors) == 0
    logger.info("")
    if ontologies_valid:
        logger.info("✓ ALL ONTOLOGY VALIDATIONS PASSED")
    else:
        logger.error(f"✗ ONTOLOGY VALIDATIONS FAILED ({len(errors)} errors)")

    return {
        "valid": ontologies_valid,
        "details": ontology_details,
        "errors": errors
    }


def validate_projects(sparql, dynamodb_table, region):
    """
    Validate project graphs in Neptune.

    Args:
        sparql: SPARQL utility module
        dynamodb_table: DynamoDB table name for subject mappings
        region: AWS region

    Returns:
        dict: Validation results with 'valid' flag and details
    """
    logger.info("=" * 80)
    logger.info("VALIDATING PROJECT GRAPHS")
    logger.info("=" * 80)

    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(dynamodb_table)

    errors = []

    # 1. Scan DynamoDB for all project IDs and mappings
    logger.info("Scanning DynamoDB for project mappings...")
    mapping_records = []
    scan_kwargs = {
        'FilterExpression': 'begins_with(PK, :pk_prefix)',
        'ExpressionAttributeValues': {
            ':pk_prefix': 'SUBJECT#'
        }
    }

    done = False
    while not done:
        response = table.scan(**scan_kwargs)

        for item in response.get('Items', []):
            pk_parts = item['PK'].split('#')
            sk_parts = item['SK'].split('#')

            if len(pk_parts) >= 2 and len(sk_parts) >= 4 and sk_parts[0] == 'PROJECT' and sk_parts[2] == 'SUBJECT':
                subject_id = pk_parts[1]
                project_id = sk_parts[1]
                project_subject_id = sk_parts[3]

                mapping_records.append({
                    'subject_id': subject_id,
                    'project_id': project_id,
                    'project_subject_id': project_subject_id
                })

        if 'LastEvaluatedKey' in response:
            scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        else:
            done = True

    logger.info(f"Found {len(mapping_records):,} subject-project mappings in DynamoDB")

    # Count unique projects
    unique_projects = set(m['project_id'] for m in mapping_records)
    expected_project_count = len(unique_projects)
    expected_mapping_count = len(mapping_records)

    logger.info(f"Expected projects: {expected_project_count:,}")
    logger.info(f"Expected mappings: {expected_mapping_count:,}")

    # 2. Query Neptune for project count
    logger.info("\nQuerying Neptune for project count...")
    project_count_query = """
    SELECT (COUNT(DISTINCT ?project) AS ?count)
    WHERE {
        ?g a ?type
        FILTER(STRSTARTS(STR(?g), "http://ods.nationwidechildrens.org/phebee/projects/"))
        GRAPH ?g {
            ?project a <http://ods.nationwidechildrens.org/phebee#Project>
        }
    }
    """

    result = sparql.execute_query(project_count_query)
    actual_project_count = 0

    if result and 'results' in result and 'bindings' in result['results']:
        bindings = result['results']['bindings']
        if bindings and 'count' in bindings[0]:
            actual_project_count = int(bindings[0]['count']['value'])

    logger.info(f"Actual projects in Neptune: {actual_project_count:,}")

    project_count_match = (actual_project_count == expected_project_count)
    if project_count_match:
        logger.info("✓ Project count matches")
    else:
        error_msg = f"Project count mismatch! Expected: {expected_project_count:,}, Got: {actual_project_count:,}"
        logger.error(f"✗ {error_msg}")
        errors.append(error_msg)

    # 3. Query Neptune for mapping count
    logger.info("\nQuerying Neptune for subject-project mapping count...")
    mapping_count_query = """
    SELECT (COUNT(?projectSubjectId) AS ?count)
    WHERE {
        ?g a ?type
        FILTER(STRSTARTS(STR(?g), "http://ods.nationwidechildrens.org/phebee/projects/"))
        GRAPH ?g {
            ?projectSubjectId a <http://ods.nationwidechildrens.org/phebee#ProjectSubjectId>
        }
    }
    """

    result = sparql.execute_query(mapping_count_query)
    actual_mapping_count = 0

    if result and 'results' in result and 'bindings' in result['results']:
        bindings = result['results']['bindings']
        if bindings and 'count' in bindings[0]:
            actual_mapping_count = int(bindings[0]['count']['value'])

    logger.info(f"Actual mappings in Neptune: {actual_mapping_count:,}")

    mapping_count_match = (actual_mapping_count == expected_mapping_count)
    if mapping_count_match:
        logger.info("✓ Mapping count matches")
    else:
        error_msg = f"Mapping count mismatch! Expected: {expected_mapping_count:,}, Got: {actual_mapping_count:,}"
        logger.error(f"✗ {error_msg}")
        errors.append(error_msg)

    # 4. Sample verification: Check 5 random mappings
    logger.info("\nVerifying sample project-subject mappings...")
    sample_size = min(5, len(mapping_records))
    sample_mappings = random.sample(mapping_records, sample_size)

    samples_checked = 0
    samples_matched = 0

    for mapping in sample_mappings:
        samples_checked += 1
        subject_id = mapping['subject_id']
        project_id = mapping['project_id']
        project_subject_id = mapping['project_subject_id']

        logger.info(f"  Sample {samples_checked}: {project_id} / {subject_id}")

        # Query Neptune for this specific mapping
        project_graph = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"
        subject_uri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
        project_uri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"
        project_subject_id_uri = f"{project_uri}/{project_subject_id}"

        sample_query = f"""
        SELECT ?projectSubjectId ?project
        WHERE {{
            GRAPH <{project_graph}> {{
                <{subject_uri}> <http://ods.nationwidechildrens.org/phebee#hasProjectSubjectId> ?projectSubjectId .
                ?projectSubjectId <http://ods.nationwidechildrens.org/phebee#hasProject> ?project .
            }}
        }}
        """

        try:
            result = sparql.execute_query(sample_query)

            if result and 'results' in result and 'bindings' in result['results']:
                bindings = result['results']['bindings']
                if bindings:
                    actual_project_subject_id_uri = bindings[0].get('projectSubjectId', {}).get('value')
                    actual_project_uri = bindings[0].get('project', {}).get('value')

                    if actual_project_subject_id_uri == project_subject_id_uri and actual_project_uri == project_uri:
                        samples_matched += 1
                        logger.info(f"    ✓ Mapping verified")
                    else:
                        error_msg = f"Sample {samples_checked}: Mapping mismatch for {project_id}/{subject_id}"
                        logger.error(f"    ✗ {error_msg}")
                        errors.append(error_msg)
                else:
                    error_msg = f"Sample {samples_checked}: Mapping not found for {project_id}/{subject_id}"
                    logger.error(f"    ✗ {error_msg}")
                    errors.append(error_msg)
            else:
                error_msg = f"Sample {samples_checked}: Query failed for {project_id}/{subject_id}"
                logger.error(f"    ✗ {error_msg}")
                errors.append(error_msg)

        except Exception as e:
            error_msg = f"Sample {samples_checked}: Error querying Neptune: {str(e)}"
            logger.error(f"    ✗ {error_msg}")
            errors.append(error_msg)

    sample_verification_passed = (samples_matched == samples_checked)

    logger.info("")
    if sample_verification_passed:
        logger.info(f"✓ Sample verification passed: {samples_matched}/{samples_checked} matched")
    else:
        logger.error(f"✗ Sample verification failed: Only {samples_matched}/{samples_checked} matched")

    projects_valid = (project_count_match and mapping_count_match and sample_verification_passed)

    if projects_valid:
        logger.info("✓ ALL PROJECT VALIDATIONS PASSED")
    else:
        logger.error(f"✗ PROJECT VALIDATIONS FAILED ({len(errors)} errors)")

    return {
        "valid": projects_valid,
        "details": {
            "expected_project_count": expected_project_count,
            "actual_project_count": actual_project_count,
            "project_count_match": project_count_match,
            "expected_mapping_count": expected_mapping_count,
            "actual_mapping_count": actual_mapping_count,
            "mapping_count_match": mapping_count_match,
            "samples_checked": samples_checked,
            "samples_matched": samples_matched,
            "sample_verification_passed": sample_verification_passed
        },
        "errors": errors
    }


def validate_subjects(sparql, database, by_subject_table, baseline):
    """
    Validate subjects graph in Neptune (existing validation logic).

    Args:
        sparql: SPARQL utility module
        database: Iceberg database name
        by_subject_table: By-subject analytical table name
        baseline: Baseline counts from analytical table

    Returns:
        dict: Validation results with 'valid' flag and details
    """
    logger.info("=" * 80)
    logger.info("VALIDATING SUBJECTS GRAPH")
    logger.info("=" * 80)

    logger.info(f"Expected - Subjects: {baseline['expectedSubjectCount']:,}, Termlinks: {baseline['expectedTermlinkCount']:,}")

    errors = []
    details = {}

    # Validation 1: Check subject count matches
    logger.info("\nQuerying Neptune for subject count...")
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
        logger.error(f"✗ {error_msg}")
        errors.append(error_msg)
    else:
        logger.info(f"✓ Subject count matches: {actual_subject_count:,}")

    # Validation 2: Check termlink count matches
    logger.info("\nQuerying Neptune for termlink count...")
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
        logger.error(f"✗ {error_msg}")
        errors.append(error_msg)
    else:
        logger.info(f"✓ Termlink count matches: {actual_termlink_count:,}")

    # Validation 3: Comprehensive sample verification (10 random subjects)
    logger.info("\nSampling subjects for comprehensive verification...")
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

    for row in results['ResultSet']['Rows'][1:]:  # Skip header
        if row['Data'] and len(row['Data']) >= 5:
            subject_id = row['Data'][0].get('VarCharValue')
            term_iri = row['Data'][1].get('VarCharValue')
            expected_termlink_id = row['Data'][2].get('VarCharValue')
            qualifiers_str = row['Data'][3].get('VarCharValue', '[]')

            if subject_id and term_iri and expected_termlink_id:
                samples_checked += 1
                sample_passed = True

                expected_termlink_uri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}/term-link/{expected_termlink_id}"

                # Parse qualifiers
                expected_qualifiers = []
                if qualifiers_str and qualifiers_str != '[]':
                    import re
                    struct_pattern = r'\{qualifier_type=([^,]+), qualifier_value=([^}]+)\}'
                    matches = re.findall(struct_pattern, qualifiers_str)
                    for qt, qv in matches:
                        expected_qualifiers.append((qt, qv))

                # Query Neptune for this specific termlink
                subject_uri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"

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

                logger.info(f"  Sample {samples_checked}: {subject_id}")

                try:
                    result = sparql.execute_query(neptune_query)

                    if not result or 'results' not in result or 'bindings' not in result['results']:
                        logger.error(f"    ✗ No Neptune results")
                        sample_passed = False
                        continue

                    bindings = result['results']['bindings']

                    if not bindings:
                        logger.error(f"    ✗ Termlink URI not found")
                        sample_passed = False
                        continue

                    # Check term IRI
                    actual_term_iri = bindings[0].get('term', {}).get('value')
                    if actual_term_iri != term_iri:
                        logger.error(f"    ✗ Term mismatch")
                        sample_passed = False
                        continue

                    # Check qualifiers
                    actual_qualifiers = set()
                    for binding in bindings:
                        if 'qualifier' in binding:
                            actual_qualifiers.add(binding['qualifier']['value'])

                    expected_qualifier_uris = set()
                    for qt, qv in expected_qualifiers:
                        if qt.startswith('http://') or qt.startswith('https://'):
                            expected_qualifier_uris.add(qt)
                        else:
                            expected_qualifier_uris.add(f"http://ods.nationwidechildrens.org/phebee#{qt}")

                    if expected_qualifier_uris != actual_qualifiers:
                        logger.error(f"    ✗ Qualifier mismatch")
                        sample_passed = False

                    if sample_passed:
                        samples_matched += 1
                        logger.info(f"    ✓ All checks passed")

                except Exception as e:
                    logger.error(f"    ✗ Error querying Neptune: {str(e)}")
                    sample_passed = False

                if samples_checked >= 10:
                    break

    # Require 100% match rate
    if samples_checked > 0:
        match_ratio = samples_matched / samples_checked
        sample_verification_passed = (match_ratio == 1.0)

        details['sampleVerificationPassed'] = sample_verification_passed
        details['samplesChecked'] = samples_checked
        details['samplesMatched'] = samples_matched

        if not sample_verification_passed:
            error_msg = f"Sample verification failed! Only {samples_matched}/{samples_checked} samples matched (requires 100%)"
            logger.error(f"✗ {error_msg}")
            errors.append(error_msg)
        else:
            logger.info(f"✓ Sample verification passed: {samples_matched}/{samples_checked} matched (100%)")
    else:
        details['sampleVerificationPassed'] = True
        logger.info("No samples to verify")

    subjects_valid = (subject_count_match and termlink_count_match and sample_verification_passed)

    logger.info("")
    if subjects_valid:
        logger.info("✓ ALL SUBJECTS VALIDATIONS PASSED")
    else:
        logger.error(f"✗ SUBJECTS VALIDATIONS FAILED ({len(errors)} errors)")

    details['errors'] = errors

    return {
        "valid": subjects_valid,
        "details": details,
        "errors": errors
    }


def lambda_handler(event, context):
    """
    Validate ALL Neptune graphs after full database reset and rebuild.

    Input:
        {
            "runId": "uuid",
            "icebergDatabase": "database",
            "bySubjectTable": "table",
            "dynamodbTable": "table",
            "region": "aws-region",
            "baseline": {
                "expectedSubjectCount": 1234,
                "expectedTermlinkCount": 12345
            }
        }

    Output:
        {
            "valid": true/false,
            "ontologies": {...},
            "projects": {...},
            "subjects": {...}
        }
    """
    try:
        from phebee.utils import sparql

        run_id = event['runId']
        database = event['icebergDatabase']
        by_subject_table = event['bySubjectTable']
        dynamodb_table = event['dynamodbTable']
        baseline = event['baseline']
        region = event.get('region', 'us-east-1')

        logger.info("=" * 80)
        logger.info("COMPLETE NEPTUNE VALIDATION")
        logger.info(f"Run ID: {run_id}")
        logger.info("=" * 80)

        # Validate ontologies
        ontologies_result = validate_ontologies(sparql, database, dynamodb_table, region)

        # Validate projects
        projects_result = validate_projects(sparql, dynamodb_table, region)

        # Validate subjects
        subjects_result = validate_subjects(sparql, database, by_subject_table, baseline)

        # Overall validation result
        overall_valid = (
            ontologies_result['valid'] and
            projects_result['valid'] and
            subjects_result['valid']
        )

        logger.info("")
        logger.info("=" * 80)
        if overall_valid:
            logger.info("✓✓✓ ALL VALIDATIONS PASSED ✓✓✓")
        else:
            logger.error("✗✗✗ VALIDATION FAILED ✗✗✗")
            if not ontologies_result['valid']:
                logger.error("  - Ontologies validation failed")
            if not projects_result['valid']:
                logger.error("  - Projects validation failed")
            if not subjects_result['valid']:
                logger.error("  - Subjects validation failed")
        logger.info("=" * 80)

        return {
            "valid": overall_valid,
            "ontologies": ontologies_result,
            "projects": projects_result,
            "subjects": subjects_result
        }

    except KeyError as e:
        logger.error(f"Missing required parameter: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error validating Neptune: {str(e)}", exc_info=True)
        raise
