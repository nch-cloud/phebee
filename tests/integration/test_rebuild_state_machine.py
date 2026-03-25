"""
Integration test for RebuildMaterializedDataStateMachine

This test:
1. Creates diverse test data (evidence + DynamoDB subjects)
2. Executes the rebuild state machine with various flag combinations
3. Validates results across all layers (Evidence, Iceberg, Neptune)
4. Cleans up test data afterwards

Usage:
    pytest tests/integration/test_rebuild_state_machine.py -v
    pytest tests/integration/test_rebuild_state_machine.py::test_full_rebuild -v
"""

import pytest
import boto3
import time
import hashlib
import json
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any
from phebee.utils.hash import generate_evidence_hash, generate_termlink_hash
from phebee.utils.qualifier import Qualifier, normalize_qualifiers, qualifiers_to_string_list, normalize_qualifier_type


class TestRebuildStateMachine:
    """Integration tests for rebuild state machine"""

    @pytest.fixture(scope="class")
    def aws_clients(self):
        """Initialize AWS clients"""
        return {
            'athena': boto3.client('athena', region_name='us-east-2'),
            'dynamodb': boto3.resource('dynamodb', region_name='us-east-2'),
            's3': boto3.client('s3', region_name='us-east-2'),
            'stepfunctions': boto3.client('stepfunctions', region_name='us-east-2')
        }

    @pytest.fixture(scope="class")
    def test_config(self, physical_resources):
        """Test configuration from stack resources"""
        return {
            'database': physical_resources['AthenaDatabase'],
            'evidence_table': physical_resources['AthenaEvidenceTable'],
            'by_subject_table': physical_resources['AthenaSubjectTermsBySubjectTable'],
            'by_project_term_table': physical_resources['AthenaSubjectTermsByProjectTermTable'],
            'dynamodb_table': physical_resources['DynamoDBTableName'],
            'state_machine_arn': physical_resources['RebuildMaterializedDataStateMachineArn'],
            'bucket': physical_resources['PheBeeBucketName'],
            'test_run_prefix': 'integration-test',
            'app_name': physical_resources['AppName']
        }

    @pytest.fixture(scope="class")
    def test_data(self):
        """Generate diverse test data"""
        base_date = datetime.now() - timedelta(days=30)

        test_subjects = [
            {'subject_id': f'TEST_SUBJ_{i:03d}', 'project_id': 'test-project', 'project_subject_id': f'PROJ_SUBJ_{i:03d}'}
            for i in range(1, 11)  # 10 test subjects
        ]

        # Diverse test scenarios
        test_evidence = [
            # 1. No qualifiers
            {
                'subject_id': 'TEST_SUBJ_001',
                'clinical_note_id': 'NOTE001',
                'encounter_id': 'ENC001',
                'term_iri': 'http://purl.obolibrary.org/obo/HP_0001263',
                'qualifiers': None,
                'note_date': base_date,
                'span_start': 10,
                'span_end': 25
            },
            # 2. Boolean qualifier (negated)
            {
                'subject_id': 'TEST_SUBJ_002',
                'clinical_note_id': 'NOTE002',
                'encounter_id': 'ENC002',
                'term_iri': 'http://purl.obolibrary.org/obo/HP_0001250',
                'qualifiers': [{'qualifier_type': 'negated', 'qualifier_value': 'true'}],
                'note_date': base_date + timedelta(days=1),
                'span_start': 15,
                'span_end': 30
            },
            # 3. HPO qualifier (HP:0011009 = Acute onset)
            {
                'subject_id': 'TEST_SUBJ_003',
                'clinical_note_id': 'NOTE003',
                'encounter_id': 'ENC003',
                'term_iri': 'http://purl.obolibrary.org/obo/HP_0001250',
                'qualifiers': [{'qualifier_type': 'http://purl.obolibrary.org/obo/HP_0011009', 'qualifier_value': 'true'}],
                'note_date': base_date + timedelta(days=2),
                'span_start': 20,
                'span_end': 40
            },
            # 4. Multiple qualifiers (mixed boolean)
            {
                'subject_id': 'TEST_SUBJ_004',
                'clinical_note_id': 'NOTE004',
                'encounter_id': 'ENC004',
                'term_iri': 'http://purl.obolibrary.org/obo/HP_0002664',
                'qualifiers': [
                    {'qualifier_type': 'negated', 'qualifier_value': 'false'},  # Should be filtered out
                    {'qualifier_type': 'http://purl.obolibrary.org/obo/HP_0003593', 'qualifier_value': 'true'},  # HP:0003593 = Infantile onset
                    {'qualifier_type': 'hypothetical', 'qualifier_value': 'true'}
                ],
                'note_date': base_date + timedelta(days=3),
                'span_start': 5,
                'span_end': 15
            },
            # 5-10. Multiple evidence for same subject/term (test aggregation)
            *[{
                'subject_id': 'TEST_SUBJ_005',
                'clinical_note_id': f'NOTE00{5+i}',
                'encounter_id': f'ENC00{5+i}',
                'term_iri': 'http://purl.obolibrary.org/obo/HP_0001263',
                'qualifiers': None,
                'note_date': base_date + timedelta(days=i),
                'span_start': 10 + i*5,
                'span_end': 25 + i*5
            } for i in range(5)],
            # 11-12. Same term, different HPO qualifiers for same subject (should create separate termlinks)
            {
                'subject_id': 'TEST_SUBJ_006',
                'clinical_note_id': 'NOTE010',
                'encounter_id': 'ENC010',
                'term_iri': 'http://purl.obolibrary.org/obo/HP_0001250',
                'qualifiers': [{'qualifier_type': 'http://purl.obolibrary.org/obo/HP_0011009', 'qualifier_value': 'true'}],  # Acute onset
                'note_date': base_date + timedelta(days=10),
                'span_start': 10,
                'span_end': 25
            },
            {
                'subject_id': 'TEST_SUBJ_006',
                'clinical_note_id': 'NOTE011',
                'encounter_id': 'ENC011',
                'term_iri': 'http://purl.obolibrary.org/obo/HP_0001250',
                'qualifiers': [{'qualifier_type': 'http://purl.obolibrary.org/obo/HP_0003593', 'qualifier_value': 'true'}],  # Infantile onset
                'note_date': base_date + timedelta(days=11),
                'span_start': 30,
                'span_end': 45
            },
            # 16-20. Multiple subjects, same term (cross-subject aggregation)
            *[{
                'subject_id': f'TEST_SUBJ_{7+i:03d}',
                'clinical_note_id': f'NOTE{12+i:03d}',
                'encounter_id': f'ENC{12+i:03d}',
                'term_iri': 'http://purl.obolibrary.org/obo/HP_0002664',
                'qualifiers': [{'qualifier_type': 'negated', 'qualifier_value': 'true'}],
                'note_date': base_date + timedelta(days=12+i),
                'span_start': 5,
                'span_end': 20
            } for i in range(4)]
        ]

        return {
            'subjects': test_subjects,
            'evidence': test_evidence,
            'run_id': str(uuid.uuid4())
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, aws_clients, test_config, test_data):
        """Setup test data before tests, cleanup after"""
        print("\n=== Setting up test data ===")

        # 1. Insert DynamoDB subjects
        dynamodb_table = aws_clients['dynamodb'].Table(test_config['dynamodb_table'])

        for subject in test_data['subjects']:
            # Forward mapping: PROJECT -> SUBJECT
            dynamodb_table.put_item(Item={
                'PK': f"PROJECT#{subject['project_id']}",
                'SK': f"SUBJECT#{subject['project_subject_id']}",
                'subject_id': subject['subject_id']
            })
            # Reverse mapping: SUBJECT -> PROJECT
            dynamodb_table.put_item(Item={
                'PK': f"SUBJECT#{subject['subject_id']}",
                'SK': f"PROJECT#{subject['project_id']}#SUBJECT#{subject['project_subject_id']}",
                'project_id': subject['project_id'],
                'project_subject_id': subject['project_subject_id']
            })

        print(f"✓ Inserted {len(test_data['subjects'])} subjects into DynamoDB")

        # 2. Insert evidence records
        evidence_inserts = []
        for i, evidence in enumerate(test_data['evidence']):
            # Generate IDs using hash functions
            subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{evidence['subject_id']}"

            # Convert to Qualifier objects
            qualifier_list = []
            for q in (evidence['qualifiers'] or []):
                if isinstance(q, dict):
                    qualifier_list.append(Qualifier(
                        type=normalize_qualifier_type(q['qualifier_type']),
                        value=q['qualifier_value']
                    ))

            # Normalize and convert to strings for hash
            qualifiers_normalized = qualifiers_to_string_list(normalize_qualifiers(qualifier_list))

            evidence_id = generate_evidence_hash(
                clinical_note_id=evidence['clinical_note_id'],
                encounter_id=evidence['encounter_id'],
                term_iri=evidence['term_iri'],
                span_start=evidence.get('span_start'),
                span_end=evidence.get('span_end'),
                qualifiers=qualifiers_normalized,
                subject_id=evidence['subject_id'],
                creator_id='integration-test'
            )

            termlink_id = generate_termlink_hash(
                source_node_iri=subject_iri,
                term_iri=evidence['term_iri'],
                qualifiers=qualifiers_normalized
            )

            # Build qualifier array SQL
            qualifiers_sql = "CAST(NULL AS ARRAY(ROW(qualifier_type VARCHAR, qualifier_value VARCHAR)))"
            if evidence['qualifiers']:
                qual_rows = [f"ROW('{q['qualifier_type']}', '{q['qualifier_value']}')" for q in evidence['qualifiers']]
                qualifiers_sql = f"CAST(ARRAY[{', '.join(qual_rows)}] AS ARRAY(ROW(qualifier_type VARCHAR, qualifier_value VARCHAR)))"

            # Format timestamp for Athena (no microseconds)
            timestamp_str = evidence['note_date'].strftime('%Y-%m-%d %H:%M:%S')

            evidence_inserts.append(f"""
                ('{evidence_id}', '{test_data['run_id']}', 'batch-001', 'EvidenceType', 'AssertionType',
                 TIMESTAMP '{timestamp_str}', DATE '{evidence['note_date'].date().isoformat()}',
                 'source-level', '{evidence['subject_id']}', '{evidence['encounter_id']}', '{evidence['clinical_note_id']}',
                 '{termlink_id}', '{evidence['term_iri']}',
                 ROW('HPO', '2024-04-22', 'http://purl.obolibrary.org/obo/hp.owl'),
                 ROW('clinical-note', TIMESTAMP '{timestamp_str}', NULL, NULL),
                 ROW('integration-test', 'automated', 'Integration Test'),
                 ROW({evidence.get('span_start', 'NULL')}, {evidence.get('span_end', 'NULL')}, NULL),
                 {qualifiers_sql})
            """)

        # Execute batch insert
        insert_query = f"""
        INSERT INTO {test_config['database']}.{test_config['evidence_table']} VALUES
        {', '.join(evidence_inserts)}
        """

        response = aws_clients['athena'].start_query_execution(
            QueryString=insert_query,
            QueryExecutionContext={'Database': test_config['database']},
            ResultConfiguration={'OutputLocation': f"s3://{test_config['bucket']}/athena-results/"}
        )

        # Wait for insert to complete
        query_id = response['QueryExecutionId']
        wait_for_athena_query(aws_clients['athena'], query_id)

        print(f"✓ Inserted {len(test_data['evidence'])} evidence records")
        print(f"✓ Test run ID: {test_data['run_id']}")

        yield  # Run tests

        # Cleanup
        print("\n=== Cleaning up test data ===")

        # Delete evidence by run_id
        delete_query = f"""
        DELETE FROM {test_config['database']}.{test_config['evidence_table']}
        WHERE run_id = '{test_data['run_id']}'
        """
        response = aws_clients['athena'].start_query_execution(
            QueryString=delete_query,
            QueryExecutionContext={'Database': test_config['database']},
            ResultConfiguration={'OutputLocation': f"s3://{test_config['bucket']}/athena-results/"}
        )
        wait_for_athena_query(aws_clients['athena'], response['QueryExecutionId'])

        # Delete DynamoDB subjects
        for subject in test_data['subjects']:
            dynamodb_table.delete_item(Key={'PK': f"PROJECT#{subject['project_id']}", 'SK': f"SUBJECT#{subject['project_subject_id']}"})
            dynamodb_table.delete_item(Key={'PK': f"SUBJECT#{subject['subject_id']}", 'SK': f"PROJECT#{subject['project_id']}#SUBJECT#{subject['project_subject_id']}"})

        print("✓ Cleanup complete")

    def test_full_rebuild(self, aws_clients, test_config, test_data):
        """Test full rebuild with all flags enabled"""
        print("\n=== Testing Full Rebuild ===")

        # Execute state machine
        execution_arn = start_state_machine(
            aws_clients['stepfunctions'],
            test_config['state_machine_arn'],
            {
                'rebuildEvidence': True,
                'rebuildIceberg': True,
                'rebuildNeptune': True,
                'runId': test_data['run_id']
            }
        )

        # Wait for completion
        result = wait_for_state_machine(aws_clients['stepfunctions'], execution_arn, timeout_minutes=30)

        assert result['status'] == 'SUCCEEDED', f"State machine failed: {result}"

        # Validation checks
        self._verify_evidence_hashes(aws_clients, test_config, test_data)
        unique_combinations = self._verify_analytical_tables(aws_clients, test_config, test_data)
        self._verify_neptune_graph(aws_clients, test_config, test_data, unique_combinations)

    def _verify_evidence_hashes(self, aws_clients, test_config, test_data):
        """Verify evidence table hashes are correct"""
        print("  Verifying evidence hashes...")

        # Query evidence for our test run
        query = f"""
        SELECT evidence_id, subject_id, clinical_note_id, encounter_id, term_iri,
               text_annotation.span_start, text_annotation.span_end, qualifiers,
               termlink_id, creator.creator_id
        FROM {test_config['database']}.{test_config['evidence_table']}
        WHERE run_id = '{test_data['run_id']}'
        """

        response = aws_clients['athena'].start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': test_config['database']},
            ResultConfiguration={'OutputLocation': f"s3://{test_config['bucket']}/athena-results/"}
        )

        query_id = response['QueryExecutionId']
        wait_for_athena_query(aws_clients['athena'], query_id)

        results = aws_clients['athena'].get_query_results(QueryExecutionId=query_id)

        verified_count = 0
        for row in results['ResultSet']['Rows'][1:]:  # Skip header
            if not row['Data']:
                continue

            # Parse row data
            evidence_id = row['Data'][0].get('VarCharValue')
            subject_id = row['Data'][1].get('VarCharValue')
            clinical_note_id = row['Data'][2].get('VarCharValue')
            encounter_id = row['Data'][3].get('VarCharValue')
            term_iri = row['Data'][4].get('VarCharValue')
            span_start = row['Data'][5].get('VarCharValue')
            span_end = row['Data'][6].get('VarCharValue')
            qualifiers_str = row['Data'][7].get('VarCharValue', '[]')
            termlink_id = row['Data'][8].get('VarCharValue')
            creator_id = row['Data'][9].get('VarCharValue', 'integration-test')

            # Parse qualifiers
            qualifier_list = []
            if qualifiers_str and qualifiers_str != '[]':
                import re
                struct_pattern = r'\{qualifier_type=([^,]+), qualifier_value=([^}]+)\}'
                matches = re.findall(struct_pattern, qualifiers_str)
                qualifier_list = [
                    Qualifier(type=normalize_qualifier_type(qt), value=qv)
                    for qt, qv in matches if qv.lower() not in ['false', '0']
                ]

            # Normalize and convert to strings for hash
            qualifiers_normalized = qualifiers_to_string_list(normalize_qualifiers(qualifier_list))

            # Recalculate expected hashes
            expected_evidence_id = generate_evidence_hash(
                clinical_note_id=clinical_note_id,
                encounter_id=encounter_id,
                term_iri=term_iri,
                span_start=int(span_start) if span_start else None,
                span_end=int(span_end) if span_end else None,
                qualifiers=qualifiers_normalized,
                subject_id=subject_id,
                creator_id=creator_id
            )

            subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
            expected_termlink_id = generate_termlink_hash(
                source_node_iri=subject_iri,
                term_iri=term_iri,
                qualifiers=qualifiers_normalized
            )

            # Verify hashes match
            assert evidence_id == expected_evidence_id, \
                f"Evidence hash mismatch! Expected: {expected_evidence_id}, Got: {evidence_id}"
            assert termlink_id == expected_termlink_id, \
                f"Termlink hash mismatch! Expected: {expected_termlink_id}, Got: {termlink_id}"

            verified_count += 1

        assert verified_count == len(test_data['evidence']), \
            f"Expected to verify {len(test_data['evidence'])} records, but only verified {verified_count}"

        print(f"  ✓ Evidence hashes correct: {verified_count} records verified")

    def _verify_analytical_tables(self, aws_clients, test_config, test_data):
        """Verify analytical table aggregations"""
        print("  Verifying analytical tables...")

        # Calculate expected rows in analytical table
        # Materialization groups by (subject_id, term_iri, termlink_id)
        # This preserves separate termlinks for different qualifier combinations
        unique_combinations = {}
        for evidence in test_data['evidence']:
            # Filter out qualifiers with false/0 values (matches materialization logic)
            filtered_qualifiers = [
                f"{q['qualifier_type']}:{q['qualifier_value']}"
                for q in (evidence['qualifiers'] or [])
                if q['qualifier_value'].lower() not in ['false', '0']
            ]
            qualifiers_key = tuple(sorted(filtered_qualifiers))
            key = (evidence['subject_id'], evidence['term_iri'], qualifiers_key)

            if key not in unique_combinations:
                unique_combinations[key] = {'count': 0, 'dates': []}
            unique_combinations[key]['count'] += 1
            unique_combinations[key]['dates'].append(evidence['note_date'])

        expected_row_count = len(unique_combinations)

        print(f"    Expected {expected_row_count} unique combinations:")
        for key in sorted(unique_combinations.keys()):
            subject_id, term_iri, qualifiers_key = key
            print(f"      {subject_id}, {term_iri.split('/')[-1]}, {qualifiers_key}")

        # Query by_subject table
        query = f"""
        SELECT subject_id, term_iri, qualifiers, evidence_count, termlink_id,
               first_evidence_date, last_evidence_date
        FROM {test_config['database']}.{test_config['by_subject_table']}
        WHERE subject_id LIKE 'TEST_SUBJ_%'
        """

        response = aws_clients['athena'].start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': test_config['database']},
            ResultConfiguration={'OutputLocation': f"s3://{test_config['bucket']}/athena-results/"}
        )

        query_id = response['QueryExecutionId']
        wait_for_athena_query(aws_clients['athena'], query_id)

        results = aws_clients['athena'].get_query_results(QueryExecutionId=query_id)

        actual_row_count = len(results['ResultSet']['Rows']) - 1  # Exclude header

        print(f"    Found {actual_row_count} rows in analytical table:")

        # Parse all rows to show what's in the table
        found_combinations = set()
        for row in results['ResultSet']['Rows'][1:]:  # Skip header
            if not row['Data']:
                continue
            subject_id = row['Data'][0].get('VarCharValue')
            term_iri = row['Data'][1].get('VarCharValue')
            qualifiers_str = row['Data'][2].get('VarCharValue', '[]')

            # Parse qualifiers
            qualifiers_list = []
            if qualifiers_str and qualifiers_str != '[]':
                import re
                struct_pattern = r'\{qualifier_type=([^,]+), qualifier_value=([^}]+)\}'
                matches = re.findall(struct_pattern, qualifiers_str)
                qualifiers_list = [f"{qt}:{qv}" for qt, qv in matches]

            qualifiers_key = tuple(sorted(qualifiers_list))
            key = (subject_id, term_iri, qualifiers_key)
            found_combinations.add(key)
            print(f"      {subject_id}, {term_iri.split('/')[-1]}, {qualifiers_key}")

        # Show missing combinations
        missing = set(unique_combinations.keys()) - found_combinations
        if missing:
            print(f"    Missing combinations:")
            for key in sorted(missing):
                subject_id, term_iri, qualifiers_key = key
                print(f"      {subject_id}, {term_iri.split('/')[-1]}, {qualifiers_key}")

        # Since we're testing with unique TEST_SUBJ_* IDs, row count should match exactly
        assert actual_row_count == expected_row_count, \
            f"Row count mismatch! Expected {expected_row_count}, found {actual_row_count}"
        print(f"    ✓ Row count matches: {actual_row_count}")

        # Now verify each row in detail
        verified_combinations = 0
        for key in found_combinations:
            subject_id, term_iri, qualifiers_key = key

            # Verify this combination exists in our test data
            assert key in unique_combinations, \
                f"Unexpected combination in table: {subject_id}/{term_iri} with qualifiers {qualifiers_key}"

            verified_combinations += 1

            # Get the full row data for additional checks
            for row in results['ResultSet']['Rows'][1:]:
                if not row['Data']:
                    continue
                row_subject_id = row['Data'][0].get('VarCharValue')
                row_term_iri = row['Data'][1].get('VarCharValue')
                qualifiers_str = row['Data'][2].get('VarCharValue', '[]')

                # Parse qualifiers for matching
                qualifier_list = []
                if qualifiers_str and qualifiers_str != '[]':
                    import re
                    struct_pattern = r'\{qualifier_type=([^,]+), qualifier_value=([^}]+)\}'
                    matches = re.findall(struct_pattern, qualifiers_str)
                    qualifier_list = [
                        Qualifier(type=normalize_qualifier_type(qt), value=qv)
                        for qt, qv in matches if qv.lower() not in ['false', '0']
                    ]
                # Convert to strings for matching key
                qualifiers_list = [q.to_string() for q in qualifier_list]
                row_qualifiers_key = tuple(sorted(qualifiers_list))

                if (row_subject_id, row_term_iri, row_qualifiers_key) == key:
                    # Found the matching row, verify details
                    evidence_count = int(row['Data'][3].get('VarCharValue', '0'))
                    termlink_id = row['Data'][4].get('VarCharValue')

                    # Verify no NULL critical fields
                    assert termlink_id, f"NULL termlink_id for {subject_id}/{term_iri}"

                    expected_count = unique_combinations[key]['count']
                    assert evidence_count == expected_count, \
                        f"Evidence count mismatch for {subject_id}/{term_iri}! " \
                        f"Expected {expected_count}, got {evidence_count}"

                    # Verify termlink_id is correct
                    subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
                    # Normalize and convert to strings for hash
                    qualifiers_normalized = qualifiers_to_string_list(normalize_qualifiers(qualifier_list))
                    expected_termlink_id = generate_termlink_hash(
                        source_node_iri=subject_iri,
                        term_iri=term_iri,
                        qualifiers=qualifiers_normalized
                    )
                    assert termlink_id == expected_termlink_id, \
                        f"Termlink hash mismatch! Expected: {expected_termlink_id}, Got: {termlink_id}"
                    break

        assert verified_combinations == expected_row_count, \
            f"Verified {verified_combinations}/{expected_row_count} combinations"

        print(f"    ✓ Verified {verified_combinations} combinations")

        # Query by_project_term table for test project
        query = f"""
        SELECT COUNT(*) as row_count
        FROM {test_config['database']}.{test_config['by_project_term_table']}
        WHERE project_id = 'test-project'
        """

        response = aws_clients['athena'].start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': test_config['database']},
            ResultConfiguration={'OutputLocation': f"s3://{test_config['bucket']}/athena-results/"}
        )

        query_id = response['QueryExecutionId']
        wait_for_athena_query(aws_clients['athena'], query_id)

        results = aws_clients['athena'].get_query_results(QueryExecutionId=query_id)
        by_project_row_count = int(results['ResultSet']['Rows'][1]['Data'][0].get('VarCharValue', '0'))

        print(f"    ✓ By-project table has {by_project_row_count} rows for test-project")
        print("  ✓ Analytical tables correct")

        return unique_combinations

    def _verify_neptune_graph(self, aws_clients, test_config, test_data, unique_combinations):
        """Verify Neptune graph structure"""
        print("  Verifying Neptune graph...")

        lambda_client = boto3.client('lambda', region_name='us-east-2')

        # 1. Verify subject count for test subjects only
        # Build filter for our test subject IRIs (comma-separated for SPARQL IN syntax)
        test_subject_iris = ', '.join([
            f"<http://ods.nationwidechildrens.org/phebee/subjects/{s['subject_id']}>"
            for s in test_data['subjects']
        ])

        subject_count_query = f"""
        SELECT (COUNT(DISTINCT ?s) AS ?subjectCount)
        WHERE {{
            GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
                ?s a <http://ods.nationwidechildrens.org/phebee#Subject> .
                FILTER(?s IN ({test_subject_iris}))
            }}
        }}
        """

        response = lambda_client.invoke(
            FunctionName=f"{test_config['app_name']}-ExecuteSparqlReadonly",
            Payload=json.dumps({
                'query': subject_count_query,
                'graphRestriction': 'subjects'
            })
        )

        result = json.loads(response['Payload'].read())
        assert result['success'], f"SPARQL query failed: {result.get('error')}"

        bindings = result['results']['results']['bindings']
        actual_subject_count = int(bindings[0]['subjectCount']['value'])
        expected_subject_count = len(test_data['subjects'])

        assert actual_subject_count == expected_subject_count, \
            f"Subject count mismatch! Expected {expected_subject_count}, got {actual_subject_count}"

        print(f"    ✓ Subject count matches: {actual_subject_count}")

        # 2. Verify termlink count for test subjects only
        termlink_count_query = f"""
        SELECT (COUNT(?tl) AS ?termlinkCount)
        WHERE {{
            GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
                ?s <http://ods.nationwidechildrens.org/phebee#hasTermLink> ?tl .
                FILTER(?s IN ({test_subject_iris}))
            }}
        }}
        """

        response = lambda_client.invoke(
            FunctionName=f"{test_config['app_name']}-ExecuteSparqlReadonly",
            Payload=json.dumps({
                'query': termlink_count_query,
                'graphRestriction': 'subjects'
            })
        )

        result = json.loads(response['Payload'].read())
        assert result['success'], f"SPARQL query failed: {result.get('error')}"

        bindings = result['results']['results']['bindings']
        actual_termlink_count = int(bindings[0]['termlinkCount']['value'])

        # Expected: Same as analytical table - one termlink per (subject, term, qualifiers) combination
        expected_termlink_count = len(unique_combinations)

        assert actual_termlink_count == expected_termlink_count, \
            f"Termlink count mismatch! Expected {expected_termlink_count}, got {actual_termlink_count}"

        print(f"    ✓ Termlink count matches: {actual_termlink_count}")

        # 3. Sample verification: Check a specific subject's termlinks
        sample_subject = test_data['subjects'][0]['subject_id']
        subject_terms_query = f"""
        SELECT ?term ?qualifier
        WHERE {{
            GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
                <http://ods.nationwidechildrens.org/phebee/subjects/{sample_subject}>
                    <http://ods.nationwidechildrens.org/phebee#hasTermLink> ?tl .
                ?tl <http://ods.nationwidechildrens.org/phebee#hasTerm> ?term .
                OPTIONAL {{ ?tl <http://ods.nationwidechildrens.org/phebee#hasQualifyingTerm> ?qualifier }}
            }}
        }}
        """

        response = lambda_client.invoke(
            FunctionName=f"{test_config['app_name']}-ExecuteSparqlReadonly",
            Payload=json.dumps({
                'query': subject_terms_query,
                'graphRestriction': 'subjects'
            })
        )

        result = json.loads(response['Payload'].read())
        assert result['success'], f"SPARQL query failed: {result.get('error')}"

        bindings = result['results']['results']['bindings']

        # Group by term
        terms_in_neptune = {}
        for binding in bindings:
            term_iri = binding['term']['value']
            qualifier = binding.get('qualifier', {}).get('value')
            if term_iri not in terms_in_neptune:
                terms_in_neptune[term_iri] = set()
            if qualifier:
                terms_in_neptune[term_iri].add(qualifier)

        # Check against test data - should be exact match for test subject
        expected_terms = set(e['term_iri'] for e in test_data['evidence'] if e['subject_id'] == sample_subject)
        actual_terms = set(terms_in_neptune.keys())

        assert expected_terms == actual_terms, \
            f"Term mismatch for {sample_subject}! Expected: {expected_terms}, Got: {actual_terms}"

        print(f"    ✓ Sample subject verification passed: {len(actual_terms)} terms match exactly")
        print("  ✓ Neptune graph correct")


def wait_for_athena_query(athena_client, query_id, timeout_seconds=300):
    """Wait for Athena query to complete"""
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        response = athena_client.get_query_execution(QueryExecutionId=query_id)
        status = response['QueryExecution']['Status']['State']

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            if status != 'SUCCEEDED':
                raise Exception(f"Athena query {query_id} failed: {status}")
            return

        time.sleep(2)

    raise TimeoutError(f"Athena query {query_id} timed out")


def start_state_machine(sfn_client, state_machine_arn, input_data):
    """Start state machine execution"""
    response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        input=json.dumps(input_data)
    )
    return response['executionArn']


def wait_for_state_machine(sfn_client, execution_arn, timeout_minutes=30):
    """Wait for state machine to complete"""
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60

    while time.time() - start_time < timeout_seconds:
        response = sfn_client.describe_execution(executionArn=execution_arn)
        status = response['status']

        if status in ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED']:
            return {
                'status': status,
                'output': json.loads(response.get('output', '{}')) if status == 'SUCCEEDED' else None,
                'error': response.get('error')
            }

        time.sleep(10)

    raise TimeoutError(f"State machine execution timed out after {timeout_minutes} minutes")
