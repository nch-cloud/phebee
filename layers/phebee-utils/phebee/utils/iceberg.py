"""
Iceberg evidence table utilities for PheBee.
"""
import boto3
import hashlib
from .hash import generate_evidence_hash, generate_termlink_hash
import json
import logging
import os
import random
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Dict, Any, Tuple, Set
from rdflib import Graph, URIRef, Literal as RdfLiteral, Namespace
from rdflib.namespace import RDF, DCTERMS, XSD

from phebee.constants import PHEBEE
from phebee.utils.dynamodb import get_term_descendants_from_cache, put_term_descendants_to_cache

logger = logging.getLogger(__name__)

# Cache for Athena workgroup configuration to avoid rate limiting
_WORKGROUP_CONFIG_CACHE = {}


def _get_workgroup_config_cached(athena_client, workgroup="primary"):
    """
    Get Athena workgroup configuration with caching to avoid rate limiting.

    Args:
        athena_client: boto3 Athena client
        workgroup: Workgroup name (default: "primary")

    Returns:
        tuple: (is_managed: bool, config: dict)
    """
    global _WORKGROUP_CONFIG_CACHE

    if workgroup not in _WORKGROUP_CONFIG_CACHE:
        wg_cfg = athena_client.get_work_group(WorkGroup=workgroup)["WorkGroup"]["Configuration"]
        managed_config = wg_cfg.get("ManagedQueryResultsConfiguration", {})
        is_managed = managed_config.get("Enabled", False) if isinstance(managed_config, dict) else False
        _WORKGROUP_CONFIG_CACHE[workgroup] = (is_managed, wg_cfg)

    return _WORKGROUP_CONFIG_CACHE[workgroup]


def parse_athena_row_array(row_str, field_names):
    """
    Parse Athena ROW array format like [{value1, value2, value3}]
    Returns list of dictionaries mapping field_names to values.

    Args:
        row_str: String representation of array of ROWs
        field_names: List of field names in order they appear in the ROW

    Returns:
        List of dictionaries
    """
    if not row_str or row_str == 'null' or row_str == '[]':
        return []

    # Remove outer brackets
    inner = row_str.strip()
    if inner.startswith('['):
        inner = inner[1:-1].strip()

    rows = []
    if inner:
        # Split by }, { to handle multiple ROWs
        if '}, {' in inner:
            row_parts = inner.split('}, {')
            row_parts = [part.strip('{}') for part in row_parts]
        else:
            # Single ROW, remove outer braces
            row_parts = [inner.strip('{}')]

        for part in row_parts:
            if not part.strip():
                continue

            # Split on comma, respecting nested structures
            values = []
            current_value = ""
            paren_depth = 0
            bracket_depth = 0

            for char in part:
                if char == ',' and paren_depth == 0 and bracket_depth == 0:
                    values.append(current_value.strip())
                    current_value = ""
                else:
                    if char in '({':
                        paren_depth += 1
                    elif char in ')}':
                        paren_depth -= 1
                    elif char == '[':
                        bracket_depth += 1
                    elif char == ']':
                        bracket_depth -= 1
                    current_value += char

            if current_value.strip():
                values.append(current_value.strip())

            # Map values to field names
            row_dict = {}
            for i, field_name in enumerate(field_names):
                if i < len(values):
                    value = values[i]
                    # Clean up the value
                    if value == 'null':
                        row_dict[field_name] = None
                    else:
                        row_dict[field_name] = value
                else:
                    row_dict[field_name] = None

            if row_dict:
                rows.append(row_dict)

    return rows


def parse_athena_struct_array(struct_str):
    """
    Parse Athena struct array format like [{qualifier_type=negated, qualifier_value=true}]
    Returns list of dictionaries.
    """
    if not struct_str or struct_str == 'null':
        return []

    # Remove outer brackets
    if struct_str.startswith('[') and struct_str.endswith(']'):
        inner = struct_str[1:-1].strip()
    else:
        inner = struct_str.strip()

    structs = []
    if inner:
        # Split by }, { to handle multiple structs
        if '}, {' in inner:
            struct_parts = inner.split('}, {')
            struct_parts = [part.strip('{}') for part in struct_parts]
        else:
            # Single struct, remove outer braces
            struct_parts = [inner.strip('{}')]

        for part in struct_parts:
            if not part.strip():
                continue

            # Parse key=value pairs
            struct_dict = {}
            # Split on comma, but be careful of commas within values
            pairs = []
            current_pair = ""
            paren_depth = 0

            for char in part:
                if char == ',' and paren_depth == 0:
                    pairs.append(current_pair.strip())
                    current_pair = ""
                else:
                    if char in '({[':
                        paren_depth += 1
                    elif char in ')}]':
                        paren_depth -= 1
                    current_pair += char

            if current_pair.strip():
                pairs.append(current_pair.strip())

            for pair in pairs:
                if '=' in pair:
                    key, value = pair.split('=', 1)
                    struct_dict[key.strip()] = value.strip()

            if struct_dict:
                structs.append(struct_dict)

    return structs


def parse_qualifiers_field(qualifiers_str):
    """
    Parse qualifiers field from Athena struct format.
    Returns list of active qualifier types.
    """
    if not qualifiers_str or qualifiers_str == 'null':
        return []
    
    qualifiers_list = parse_athena_struct_array(qualifiers_str)
    
    # Extract active qualifiers
    return [
        q['qualifier_type'] for q in qualifiers_list 
        if q.get('qualifier_value') in ['true', '1', 1, 1.0, True]
    ]

PHEBEE_NS = Namespace(PHEBEE)
OBO = Namespace("http://purl.obolibrary.org/obo/")


def query_iceberg_evidence(query: str) -> List[Dict[str, Any]]:
    """
    Execute a query against the Iceberg evidence table using Athena.
    
    Args:
        query: SQL query to execute
        
    Returns:
        List of result rows as dictionaries
    """
    logger.info(f"query_iceberg_evidence called with query: {query}")
    
    try:
        athena_client = boto3.client('athena')

        # Check if primary workgroup is managed (cached to avoid rate limiting)
        managed, wg_cfg = _get_workgroup_config_cached(athena_client)

        database_name = os.environ.get('ICEBERG_DATABASE')
        if not database_name:
            raise ValueError("ICEBERG_DATABASE environment variable is required")

        params = {
            "QueryString": query,
            "QueryExecutionContext": {"Database": database_name},
            "WorkGroup": "primary"
        }

        if not managed:
            bucket_name = os.environ.get('PHEBEE_BUCKET_NAME')
            params["ResultConfiguration"] = {"OutputLocation": f"s3://{bucket_name}/athena-results/"}

        # Start query execution
        response = athena_client.start_query_execution(**params)
        
        query_execution_id = response['QueryExecutionId']
        
        # Wait for query completion
        # Poll every 0.5s for fast queries (most queries complete in 1-5 seconds)
        while True:
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']

            if status == 'SUCCEEDED':
                break
            elif status in ['FAILED', 'CANCELLED']:
                error_msg = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                raise Exception(f"Athena query failed: {error_msg}")

            time.sleep(0.5)
        
        # Get query results
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        
        # Parse results into list of dictionaries
        columns = [col['Name'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        rows = []
        
        for row in results['ResultSet']['Rows'][1:]:  # Skip header row
            row_data = {}
            for i, col in enumerate(columns):
                value = row['Data'][i].get('VarCharValue', '')
                row_data[col] = value
            rows.append(row_data)
        
        return rows
        
    except Exception as e:
        logger.error(f"Error querying Iceberg evidence table: {e}")
        raise


def create_evidence_record(
    subject_id: str,
    term_iri: str,
    creator_id: str,
    creator_name: str = None,
    creator_type: str = "human",
    evidence_type: str = "manual_annotation",
    run_id: str = None,
    batch_id: str = None,
    encounter_id: str = None,
    clinical_note_id: str = None,
    span_start: int = None,
    span_end: int = None,
    qualifiers: List[str] = None,
    note_timestamp: str = None,
    provider_type: str = None,
    author_specialty: str = None,
    note_type: str = None,
    term_source: Dict[str, str] = None
) -> str:
    """
    Create a single evidence record in Iceberg.
    
    Returns:
        str: The generated evidence_id
    """
    # Get environment variables before any local imports
    database_name = os.environ['ICEBERG_DATABASE']
    table_name = os.environ['ICEBERG_EVIDENCE_TABLE']
    
    # Generate evidence ID
    evidence_id = generate_evidence_hash(
        clinical_note_id=clinical_note_id,
        encounter_id=encounter_id,
        term_iri=term_iri,
        span_start=span_start,
        span_end=span_end,
        qualifiers=qualifiers or [],
        subject_id=subject_id,
        creator_id=creator_id
    )
    
    # Generate termlink ID using shared function
    subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
    termlink_id = generate_termlink_hash(subject_iri, term_iri, qualifiers or [])
    
    # Set assertion type and source level
    assertion_type = "manual_assertion"
    source_level = "clinical_note" if clinical_note_id else "subject"
    
    # Build evidence record
    record = {
        "evidence_id": evidence_id,
        "run_id": run_id or f"manual-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
        "batch_id": batch_id,
        "evidence_type": evidence_type,
        "assertion_type": "manual_assertion",
        "created_timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "created_date": datetime.utcnow().date().isoformat(),
        "source_level": "clinical_note" if clinical_note_id else "subject",
        "subject_id": subject_id,
        "encounter_id": encounter_id,
        "clinical_note_id": clinical_note_id,
        "termlink_id": termlink_id,
        "term_iri": term_iri,
        "note_context": {
            "note_timestamp": note_timestamp,
            "note_type": note_type,
            "provider_type": provider_type,
            "author_specialty": author_specialty
        } if any([note_timestamp, note_type, provider_type, author_specialty]) else None,
        "creator": {
            "creator_id": creator_id,
            "creator_name": creator_name or creator_id,  # Default name to ID if not provided
            "creator_type": creator_type
        },
        "text_annotation": {
            "span_start": span_start,
            "span_end": span_end,
            "annotation_metadata": "{}"
        },
        "qualifiers": [
            {
                "qualifier_type": q,
                "qualifier_value": "true"
            }
            for q in (qualifiers or [])
        ] if qualifiers else None
    }
    
    # Insert into Iceberg table using Athena
    # Build structured values for complex types
    note_context_value = "CAST(NULL AS ROW(note_type VARCHAR, note_date TIMESTAMP, provider_type VARCHAR, author_specialty VARCHAR))"
    if record.get('note_context'):
        nc = record['note_context']
        timestamp_part = 'NULL'
        if nc.get('note_timestamp'):
            # Parse ISO 8601 timestamp and convert to Athena format (YYYY-MM-DD HH:MM:SS)
            try:
                # Handle both Z suffix and +00:00 timezone formats
                ts_str = nc['note_timestamp'].replace('Z', '+00:00')
                dt = datetime.fromisoformat(ts_str)
                timestamp_part = f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S')}'"
            except Exception:
                # If parsing fails, use NULL
                timestamp_part = 'NULL'
        note_context_value = f"ROW('{nc.get('note_type', '')}', {timestamp_part}, '{nc.get('provider_type', '')}', '{nc.get('author_specialty', '')}')"
    
    creator_value = f"ROW('{record['creator']['creator_id']}', '{record['creator']['creator_type']}', '{record['creator'].get('creator_name', '')}')"
    
    text_annotation_value = "CAST(NULL AS ROW(span_start INTEGER, span_end INTEGER, annotation_metadata VARCHAR))"
    if record.get('text_annotation'):
        ta = record['text_annotation']
        annotation_metadata_json = ta.get('annotation_metadata', '{}')
        if isinstance(annotation_metadata_json, dict):            
            annotation_metadata_json = json.dumps(annotation_metadata_json)
        # Escape single quotes for SQL
        escaped_metadata = annotation_metadata_json.replace("'", "''")
        
        # Handle NULL values properly for SQL
        span_start_sql = ta.get('span_start') if ta.get('span_start') is not None else 'NULL'
        span_end_sql = ta.get('span_end') if ta.get('span_end') is not None else 'NULL'
        
        text_annotation_value = f"ROW({span_start_sql}, {span_end_sql}, '{escaped_metadata}')"
    
    qualifiers_value = "CAST(NULL AS ARRAY(ROW(qualifier_type VARCHAR, qualifier_value VARCHAR)))"
    if record.get('qualifiers'):
        qual_rows = [f"ROW('{q['qualifier_type']}', '{q['qualifier_value']}')" for q in record['qualifiers']]
        qualifiers_value = f"ARRAY[{', '.join(qual_rows)}]"
    
    # Build term_source value
    term_source_value = "CAST(NULL AS ROW(source VARCHAR, version VARCHAR, iri VARCHAR))"
    if term_source:
        source = term_source.get('source', '')
        version = term_source.get('version', '')
        iri = term_source.get('iri', '')
        term_source_value = f"ROW('{source}', '{version}', '{iri}')"
    
    insert_query = f"""
    INSERT INTO {database_name}.{table_name} (
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
        term_source,
        note_context,
        creator,
        text_annotation,
        qualifiers
    ) VALUES (
        '{evidence_id}',
        '{run_id or f"manual-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"}',
        '{batch_id or ''}',
        '{evidence_type}',
        '{assertion_type}',
        TIMESTAMP '{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}',
        DATE '{datetime.utcnow().date().isoformat()}',
        '{source_level}',
        '{subject_id}',
        {f"'{encounter_id}'" if encounter_id else 'NULL'},
        {f"'{clinical_note_id}'" if clinical_note_id else 'NULL'},
        '{termlink_id}',
        '{term_iri}',
        {term_source_value},
        {note_context_value},
        {creator_value},
        {text_annotation_value},
        {qualifiers_value}
    )
    """
    
    # Retry logic with exponential backoff for Iceberg commit conflicts
    max_retries = 5
    base_delay = 0.5  # 500ms base delay

    for attempt in range(max_retries):
        try:
            # Execute INSERT using Athena

            athena_client = boto3.client('athena')

            # Check if primary workgroup is managed (cached to avoid rate limiting)
            managed, wg_cfg = _get_workgroup_config_cached(athena_client)

            database_name = os.environ.get('ICEBERG_DATABASE')
            if not database_name:
                raise ValueError("ICEBERG_DATABASE environment variable is required")

            params = {
                "QueryString": insert_query,
                "QueryExecutionContext": {"Database": database_name},
                "WorkGroup": "primary"
            }

            if not managed:
                bucket_name = os.environ.get('PHEBEE_BUCKET_NAME')
                params["ResultConfiguration"] = {"OutputLocation": f"s3://{bucket_name}/athena-results/"}

            response = athena_client.start_query_execution(**params)

            query_execution_id = response['QueryExecutionId']

            # Wait for query completion
            while True:
                result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = result['QueryExecution']['Status']['State']

                if status == 'SUCCEEDED':
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    error_msg = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')

                    # Check if this is a retryable Iceberg commit error
                    if 'ICEBERG_COMMIT_ERROR' in error_msg and attempt < max_retries - 1:
                        # Exponential backoff with jitter
                        delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                        logger.warning(f"Iceberg commit conflict on attempt {attempt + 1}/{max_retries}, retrying in {delay:.2f}s: {error_msg}")
                        time.sleep(delay)
                        break  # Break inner loop to retry
                    else:
                        # Non-retryable error or final attempt
                        raise Exception(f"Athena INSERT failed: {error_msg}")

                time.sleep(1)

            # If we succeeded, break out of retry loop
            if status == 'SUCCEEDED':
                logger.info(f"Created evidence record: {evidence_id}")
                break

        except Exception as e:
            # Only retry for Iceberg commit errors
            if 'ICEBERG_COMMIT_ERROR' not in str(e) or attempt >= max_retries - 1:
                logger.error(f"Failed to insert evidence record {evidence_id}: {e}")
                raise
            # Otherwise continue to next retry attempt
    
    return evidence_id


def get_evidence_record(evidence_id: str) -> Dict[str, Any] | None:
    """
    Get a single evidence record by ID from Iceberg.
    """
    database_name = os.environ['ICEBERG_DATABASE']
    table_name = os.environ['ICEBERG_EVIDENCE_TABLE']
    
    query = f"""
    SELECT 
        evidence_id,
        run_id,
        batch_id,
        evidence_type,
        subject_id,
        term_iri,
        termlink_id,
        creator.creator_id as creator_id,
        creator.creator_type as creator_type,
        text_annotation.span_start as span_start,
        text_annotation.span_end as span_end,
        qualifiers,
        created_timestamp,
        term_source.source as term_source_source,
        term_source.version as term_source_version,
        term_source.iri as term_source_iri
    FROM {database_name}.{table_name}
    WHERE evidence_id = '{evidence_id}'
    LIMIT 1
    """
    
    try:
        results = query_iceberg_evidence(query)
        if not results:
            return None
            
        row = results[0]
        
        # Build the properly structured response
        record = {
            "evidence_id": row.get('evidence_id'),
            "run_id": row.get('run_id'),
            "batch_id": row.get('batch_id'),
            "evidence_type": row.get('evidence_type'),
            "subject_id": row.get('subject_id'),
            "term_iri": row.get('term_iri'),
            "termlink_id": row.get('termlink_id'),
            "creator": {
                "creator_id": row.get('creator_id'),
                "creator_type": row.get('creator_type')
            },
            "created_timestamp": row.get('created_timestamp')
        }
        
        # Add text annotation if present
        if row.get('span_start') or row.get('span_end'):
            record["text_annotation"] = {
                "span_start": int(row['span_start']) if row.get('span_start') else None,
                "span_end": int(row['span_end']) if row.get('span_end') else None
            }
        
        # Add term_source if present
        if row.get('term_source_source') or row.get('term_source_version') or row.get('term_source_iri'):
            record["term_source"] = {
                "source": row.get('term_source_source'),
                "version": row.get('term_source_version'),
                "iri": row.get('term_source_iri')
            }
        
        # Add qualifiers if present
        if row.get('qualifiers'):
            # Parse qualifiers array - they come as string representation
            qualifiers_str = row['qualifiers']
            if qualifiers_str and qualifiers_str != '[]':
                # Parse format like [{qualifier_type=negated, qualifier_value=true}]
                qualifiers_dict = {}
                # Use shared parser for Athena struct format
                qualifiers_list = parse_athena_struct_array(qualifiers_str)
                for q in qualifiers_list:
                    if isinstance(q, dict) and 'qualifier_type' in q and 'qualifier_value' in q:
                        qualifiers_dict[q['qualifier_type']] = q['qualifier_value']
                
                record["qualifiers"] = qualifiers_dict
        
        return record
        
    except Exception as e:
        logger.error(f"Error getting evidence record {evidence_id}: {e}")
        return None


def count_evidence_by_termlink(termlink_id: str) -> int:
    """
    Count evidence records for a given termlink_id.
    Used to determine if a term link should be created/deleted.

    Args:
        termlink_id: The termlink ID

    Returns:
        int: Number of evidence records with this termlink_id
    """
    database_name = os.environ['ICEBERG_DATABASE']
    table_name = os.environ['ICEBERG_EVIDENCE_TABLE']

    query = f"""
    SELECT COUNT(*) as evidence_count
    FROM {database_name}.{table_name}
    WHERE termlink_id = '{termlink_id}'
    """

    try:
        results = query_iceberg_evidence(query)
        if results and len(results) > 0:
            count = int(results[0].get('evidence_count', 0))
            logger.info(f"Found {count} evidence records for termlink {termlink_id}")
            return count
        return 0
    except Exception as e:
        logger.error(f"Error counting evidence for termlink {termlink_id}: {e}")
        raise


def update_subject_terms_for_evidence(
    subject_id: str,
    term_iri: str,
    termlink_id: str,
    qualifiers: List[str] = None,
    created_date: str = None
):
    """
    Incrementally update subject-terms analytical tables when evidence is added.
    Uses DELETE + INSERT pattern to upsert rows with incremented evidence_count.

    Since evidence is project-agnostic, this function ALWAYS updates ALL projects that have
    this subject with the same evidence count to maintain consistency across projects.

    Args:
        subject_id: The subject ID
        term_iri: The term IRI
        termlink_id: The termlink ID
        qualifiers: List of active qualifiers
        created_date: Date of evidence creation (YYYY-MM-DD format)
    """
    from phebee.utils.dynamodb import get_projects_for_subject

    database_name = os.environ['ICEBERG_DATABASE']
    by_subject_table = os.environ['ICEBERG_SUBJECT_TERMS_BY_SUBJECT_TABLE']
    by_project_term_table = os.environ['ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE']

    if not created_date:
        from datetime import datetime
        created_date = datetime.utcnow().date().isoformat()

    # Extract term_id from IRI
    term_id = _extract_term_id_from_iri(term_iri)

    # Build qualifiers array SQL
    qualifiers_sql = "ARRAY[]"
    if qualifiers:
        escaped_qualifiers = [q.replace("'", "''") for q in qualifiers]
        qualifier_strings = [f"'{q}'" for q in escaped_qualifiers]
        qualifiers_sql = f"ARRAY[{', '.join(qualifier_strings)}]"

    # Construct IRIs
    subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
    project_subject_iri = subject_iri  # For now, same as subject_iri

    try:
        # Get existing data for by_subject table to preserve counts and dates
        check_query_subject = f"""
        SELECT evidence_count, first_evidence_date, last_evidence_date
        FROM {database_name}.{by_subject_table}
        WHERE subject_id = '{subject_id}' AND termlink_id = '{termlink_id}'
        """

        existing_subject = query_iceberg_evidence(check_query_subject)

        if existing_subject and len(existing_subject) > 0:
            # Row exists - increment count and update last_evidence_date
            old_count = int(existing_subject[0].get('evidence_count', 0))
            old_first_date = existing_subject[0].get('first_evidence_date')
            old_last_date = existing_subject[0].get('last_evidence_date')

            new_count = old_count + 1
            first_date = old_first_date
            last_date = created_date if created_date > old_last_date else old_last_date
        else:
            # New row - set count to 1
            new_count = 1
            first_date = created_date
            last_date = created_date

        # Delete and reinsert for by_subject table
        delete_by_subject = f"""
        DELETE FROM {database_name}.{by_subject_table}
        WHERE subject_id = '{subject_id}' AND termlink_id = '{termlink_id}'
        """

        insert_by_subject = f"""
        INSERT INTO {database_name}.{by_subject_table} (
            subject_id,
            subject_iri,
            term_iri,
            term_id,
            term_label,
            qualifiers,
            evidence_count,
            termlink_id,
            first_evidence_date,
            last_evidence_date
        ) VALUES (
            '{subject_id}',
            '{subject_iri}',
            '{term_iri}',
            '{term_id}',
            CAST(NULL AS VARCHAR),
            {qualifiers_sql},
            {new_count},
            '{termlink_id}',
            DATE '{first_date}',
            DATE '{last_date}'
        )
        """

        # Execute by_subject updates
        _execute_athena_query(delete_by_subject)
        _execute_athena_query(insert_by_subject)

        # Update by_project_term table for ALL projects that have this subject
        # This ensures consistency since evidence is project-agnostic
        from phebee.utils.dynamodb import get_project_subjects, _get_table_name
        table_name = _get_table_name()
        project_subject_pairs = get_project_subjects(table_name, subject_id)
        if not project_subject_pairs:
            raise ValueError(f"Subject {subject_id} has no associated projects in DynamoDB - data consistency error")

        # Update each project with the same evidence count
        for pid, psid in project_subject_pairs:
            # Build correct project_subject_iri for this project
            proj_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{pid}/{psid}"

            # Use DELETE + INSERT pattern (no need to check if row exists first)
            delete_by_project = f"""
            DELETE FROM {database_name}.{by_project_term_table}
            WHERE project_id = '{pid}' AND subject_id = '{subject_id}' AND termlink_id = '{termlink_id}'
            """

            insert_by_project = f"""
            INSERT INTO {database_name}.{by_project_term_table} (
                project_id,
                subject_id,
                project_subject_id,
                subject_iri,
                project_subject_iri,
                term_iri,
                term_id,
                term_label,
                qualifiers,
                evidence_count,
                termlink_id,
                first_evidence_date,
                last_evidence_date
            ) VALUES (
                '{pid}',
                '{subject_id}',
                '{psid}',
                '{subject_iri}',
                '{proj_subject_iri}',
                '{term_iri}',
                '{term_id}',
                CAST(NULL AS VARCHAR),
                {qualifiers_sql},
                {new_count},
                '{termlink_id}',
                DATE '{first_date}',
                DATE '{last_date}'
            )
            """

            _execute_athena_query(delete_by_project)
            _execute_athena_query(insert_by_project)

        logger.info(f"Updated subject-terms analytical tables for termlink {termlink_id} across {len(project_subject_pairs)} project(s)")

    except Exception as e:
        logger.error(f"Error updating subject-terms for evidence: {e}")
        raise


def remove_subject_terms_for_evidence(
    subject_id: str,
    termlink_id: str,
    evidence_count_remaining: int
):
    """
    Incrementally update subject-terms analytical tables when evidence is removed.
    Decrements evidence_count or deletes row if count reaches 0.

    Since evidence is project-agnostic, this function ALWAYS updates ALL projects that have
    this subject with the same evidence count to maintain consistency across projects.

    Args:
        subject_id: The subject ID
        termlink_id: The termlink ID
        evidence_count_remaining: Remaining evidence count after deletion
    """
    from phebee.utils.dynamodb import get_projects_for_subject

    database_name = os.environ['ICEBERG_DATABASE']
    by_subject_table = os.environ['ICEBERG_SUBJECT_TERMS_BY_SUBJECT_TABLE']
    by_project_term_table = os.environ['ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE']

    # Validate that subject has associated projects
    project_ids = get_projects_for_subject(subject_id)
    if not project_ids:
        raise ValueError(f"Subject {subject_id} has no associated projects in DynamoDB - data consistency error")

    try:
        if evidence_count_remaining == 0:
            # Delete rows from by_subject table
            delete_by_subject = f"""
            DELETE FROM {database_name}.{by_subject_table}
            WHERE subject_id = '{subject_id}' AND termlink_id = '{termlink_id}'
            """
            _execute_athena_query(delete_by_subject)
            logger.info(f"Deleted by_subject row for termlink {termlink_id} (evidence_count=0)")

            # Delete rows from by_project_term table for ALL projects
            delete_by_project = f"""
            DELETE FROM {database_name}.{by_project_term_table}
            WHERE subject_id = '{subject_id}'
              AND termlink_id = '{termlink_id}'
            """
            _execute_athena_query(delete_by_project)
            logger.info(f"Deleted by_project_term rows for termlink {termlink_id} (all projects)")

        else:
            # Decrement evidence_count and update last_evidence_date using DELETE + INSERT
            # Use same pattern as update_subject_terms_for_evidence for consistency

            # Query for the latest evidence date from remaining evidence
            evidence_table = os.environ['ICEBERG_EVIDENCE_TABLE']
            latest_date_query = f"""
            SELECT MAX(DATE(created_timestamp)) as latest_date
            FROM {database_name}.{evidence_table}
            WHERE termlink_id = '{termlink_id}'
            """
            latest_result = query_iceberg_evidence(latest_date_query)

            # Get the latest date
            if latest_result and latest_result[0].get('latest_date'):
                last_date = latest_result[0]['latest_date']
            else:
                # If no evidence found, use today's date
                from datetime import datetime
                last_date = datetime.utcnow().date().isoformat()

            # Get existing data for by_subject table to preserve other fields
            check_query_subject = f"""
            SELECT subject_iri, term_iri, term_id, term_label, qualifiers, first_evidence_date
            FROM {database_name}.{by_subject_table}
            WHERE subject_id = '{subject_id}' AND termlink_id = '{termlink_id}'
            """
            existing_subject = query_iceberg_evidence(check_query_subject)

            if existing_subject and len(existing_subject) > 0:
                row = existing_subject[0]
                subject_iri = row.get('subject_iri')
                term_iri = row.get('term_iri')
                term_id = row.get('term_id')
                term_label = row.get('term_label')
                first_date = row.get('first_evidence_date')

                # Parse qualifiers array
                qualifiers_str = row.get('qualifiers')
                qualifiers_sql = "ARRAY[]"
                if qualifiers_str and qualifiers_str != '[]' and qualifiers_str != 'null':
                    # Parse array format like ['negated', 'severity_moderate']
                    qualifiers_str = qualifiers_str.strip('[]')
                    if qualifiers_str:
                        qualifiers_list = [q.strip().strip("'\"") for q in qualifiers_str.split(',')]
                        escaped_qualifiers = [q.replace("'", "''") for q in qualifiers_list]
                        qualifier_strings = [f"'{q}'" for q in escaped_qualifiers]
                        qualifiers_sql = f"ARRAY[{', '.join(qualifier_strings)}]"

                # Delete and reinsert for by_subject table
                delete_by_subject = f"""
                DELETE FROM {database_name}.{by_subject_table}
                WHERE subject_id = '{subject_id}' AND termlink_id = '{termlink_id}'
                """

                insert_by_subject = f"""
                INSERT INTO {database_name}.{by_subject_table} (
                    subject_id,
                    subject_iri,
                    term_iri,
                    term_id,
                    term_label,
                    qualifiers,
                    evidence_count,
                    termlink_id,
                    first_evidence_date,
                    last_evidence_date
                ) VALUES (
                    '{subject_id}',
                    '{subject_iri}',
                    '{term_iri}',
                    '{term_id}',
                    {f"'{term_label}'" if term_label else 'CAST(NULL AS VARCHAR)'},
                    {qualifiers_sql},
                    {evidence_count_remaining},
                    '{termlink_id}',
                    DATE '{first_date}',
                    DATE '{last_date}'
                )
                """

                _execute_athena_query(delete_by_subject)
                _execute_athena_query(insert_by_subject)
                logger.info(f"Decremented by_subject evidence_count to {evidence_count_remaining} for termlink {termlink_id}")

            # Update by_project_term table for ALL projects using DELETE + INSERT
            from phebee.utils.dynamodb import get_project_subjects, _get_table_name
            table_name = _get_table_name()
            project_subject_pairs = get_project_subjects(table_name, subject_id)

            for pid, psid in project_subject_pairs:
                # Get existing data for this project
                check_query_project = f"""
                SELECT project_subject_iri, subject_iri, term_iri, term_id, term_label, qualifiers, first_evidence_date
                FROM {database_name}.{by_project_term_table}
                WHERE project_id = '{pid}' AND subject_id = '{subject_id}' AND termlink_id = '{termlink_id}'
                """
                existing_project = query_iceberg_evidence(check_query_project)

                if existing_project and len(existing_project) > 0:
                    proj_row = existing_project[0]
                    proj_subject_iri_stored = proj_row.get('project_subject_iri')
                    proj_subject_iri = proj_subject_iri_stored if proj_subject_iri_stored else f"http://ods.nationwidechildrens.org/phebee/projects/{pid}/{psid}"
                    proj_term_iri = proj_row.get('term_iri')
                    proj_term_id = proj_row.get('term_id')
                    proj_term_label = proj_row.get('term_label')
                    proj_first_date = proj_row.get('first_evidence_date')
                    proj_subject_iri_base = proj_row.get('subject_iri')

                    # Parse qualifiers
                    proj_qualifiers_str = proj_row.get('qualifiers')
                    proj_qualifiers_sql = "ARRAY[]"
                    if proj_qualifiers_str and proj_qualifiers_str != '[]' and proj_qualifiers_str != 'null':
                        proj_qualifiers_str = proj_qualifiers_str.strip('[]')
                        if proj_qualifiers_str:
                            proj_qualifiers_list = [q.strip().strip("'\"") for q in proj_qualifiers_str.split(',')]
                            proj_escaped_qualifiers = [q.replace("'", "''") for q in proj_qualifiers_list]
                            proj_qualifier_strings = [f"'{q}'" for q in proj_escaped_qualifiers]
                            proj_qualifiers_sql = f"ARRAY[{', '.join(proj_qualifier_strings)}]"

                    # Delete and reinsert for by_project_term table
                    delete_by_project = f"""
                    DELETE FROM {database_name}.{by_project_term_table}
                    WHERE project_id = '{pid}' AND subject_id = '{subject_id}' AND termlink_id = '{termlink_id}'
                    """

                    insert_by_project = f"""
                    INSERT INTO {database_name}.{by_project_term_table} (
                        project_id,
                        subject_id,
                        project_subject_id,
                        subject_iri,
                        project_subject_iri,
                        term_iri,
                        term_id,
                        term_label,
                        qualifiers,
                        evidence_count,
                        termlink_id,
                        first_evidence_date,
                        last_evidence_date
                    ) VALUES (
                        '{pid}',
                        '{subject_id}',
                        '{psid}',
                        '{proj_subject_iri_base}',
                        '{proj_subject_iri}',
                        '{proj_term_iri}',
                        '{proj_term_id}',
                        {f"'{proj_term_label}'" if proj_term_label else 'CAST(NULL AS VARCHAR)'},
                        {proj_qualifiers_sql},
                        {evidence_count_remaining},
                        '{termlink_id}',
                        DATE '{proj_first_date}',
                        DATE '{last_date}'
                    )
                    """

                    _execute_athena_query(delete_by_project)
                    _execute_athena_query(insert_by_project)

            logger.info(f"Decremented by_project_term evidence_count to {evidence_count_remaining} across {len(project_subject_pairs)} project(s)")

    except Exception as e:
        logger.error(f"Error removing subject-terms for evidence: {e}")
        raise


def delete_evidence_record(evidence_id: str) -> bool:
    """
    Delete a single evidence record by ID from Iceberg.

    Returns:
        bool: True if deleted, False if not found
    """
    # First check if record exists
    existing = get_evidence_record(evidence_id)
    if not existing:
        return False

    # Execute DELETE query using Athena
    database_name = os.environ['ICEBERG_DATABASE']
    table_name = os.environ['ICEBERG_EVIDENCE_TABLE']

    delete_query = f"""
    DELETE FROM {database_name}.{table_name}
    WHERE evidence_id = '{evidence_id}'
    """

    try:

        athena_client = boto3.client('athena')

        # Check if primary workgroup is managed (cached to avoid rate limiting)
        managed, wg_cfg = _get_workgroup_config_cached(athena_client)

        database_name = os.environ.get('ICEBERG_DATABASE')
        if not database_name:
            raise ValueError("ICEBERG_DATABASE environment variable is required")

        params = {
            "QueryString": delete_query,
            "QueryExecutionContext": {"Database": database_name},
            "WorkGroup": "primary"
        }

        if not managed:
            bucket_name = os.environ.get('PHEBEE_BUCKET_NAME')
            params["ResultConfiguration"] = {"OutputLocation": f"s3://{bucket_name}/athena-results/"}

        response = athena_client.start_query_execution(**params)
        query_execution_id = response['QueryExecutionId']

        # Wait for query completion
        while True:
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']

            if status == 'SUCCEEDED':
                logger.info(f"Successfully deleted evidence record: {evidence_id}")
                return True
            elif status in ['FAILED', 'CANCELLED']:
                error_msg = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                logger.error(f"Athena DELETE failed: {error_msg}")
                return False

            time.sleep(1)

    except Exception as e:
        logger.error(f"Failed to delete evidence record {evidence_id}: {e}")
        return False


def get_evidence_for_termlink(
    subject_id: str,
    term_iri: str,
    qualifiers: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Get evidence data for a specific term link from Iceberg.

    Args:
        subject_id: The subject UUID
        term_iri: The term IRI
        qualifiers: List of qualifier names (short names like 'negated', not IRIs)

    Returns:
        List of evidence records for the specific termlink
    """
    logger.info(f"Getting evidence for subject_id={subject_id}, term_iri={term_iri}, qualifiers={qualifiers}")

    # Normalize qualifiers for hash generation
    # Qualifiers provided as list of short names - convert to name:true format for hash
    qualifier_list = []
    if qualifiers:
        # Convert short names like ['negated'] to ['negated:true'] format expected by hash function
        qualifier_list = [f"{q}:true" if ':' not in q else q for q in qualifiers]

    # Compute the termlink_id to filter by specific termlink
    subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
    from phebee.utils.hash import generate_termlink_hash
    termlink_hash = generate_termlink_hash(subject_iri, term_iri, qualifier_list)
    # Note: termlink_id is stored as just the hash in the evidence table, not the full IRI
    termlink_id = termlink_hash

    logger.info(f"Computed termlink_id: {termlink_id}")

    database_name = os.environ['ICEBERG_DATABASE']
    table_name = os.environ['ICEBERG_EVIDENCE_TABLE']

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
    FROM {database_name}.{table_name}
    WHERE subject_id = '{subject_id}'
      AND term_iri = '{term_iri}'
      AND termlink_id = '{termlink_id}'
    ORDER BY created_timestamp
    """
    
    logger.info(f"Evidence query: {query}")
    
    try:
        results = query_iceberg_evidence(query)
        logger.info(f"Evidence query returned {len(results)} rows")
        
        # Format evidence records - mirror database schema exactly
        evidence = []
        for row in results:
            # Parse struct fields that may come as strings from Athena
            def parse_struct_field(field_value):
                if field_value is None:
                    return None
                if isinstance(field_value, str):
                    try:
                        # Try to parse as JSON first
                        return json.loads(field_value)
                    except (json.JSONDecodeError, ValueError):
                        # If not JSON, try to parse struct format like {key=value, key=value}
                        if field_value.startswith('{') and field_value.endswith('}'):
                            # Remove braces and split by commas
                            content = field_value[1:-1]
                            pairs = content.split(', ')
                            result = {}
                            for pair in pairs:
                                if '=' in pair:
                                    key, value = pair.split('=', 1)
                                    # Remove any extra whitespace and handle null values
                                    key = key.strip()
                                    value = value.strip()
                                    if value.lower() == 'null':
                                        result[key] = None
                                    else:
                                        # Convert numeric fields to appropriate types
                                        if key in ['span_start', 'span_end'] and value.isdigit():
                                            result[key] = int(value)
                                        elif key == 'annotation_metadata':
                                            # Parse JSON metadata if it's a string
                                            if isinstance(value, str):
                                                try:
                                                    result[key] = json.loads(value)
                                                except json.JSONDecodeError:
                                                    result[key] = value
                                            else:
                                                result[key] = value
                                        else:
                                            result[key] = value
                            return result
                        return field_value
                return field_value
            
            record = {
                "evidence_id": row.get('evidence_id'),
                "run_id": row.get('run_id'),
                "batch_id": row.get('batch_id'),
                "evidence_type": row.get('evidence_type'),
                "assertion_type": row.get('assertion_type'),
                "created_timestamp": row.get('created_timestamp'),
                "created_date": row.get('created_date'),
                "source_level": row.get('source_level'),
                "subject_id": row.get('subject_id'),
                "encounter_id": row.get('encounter_id'),
                "clinical_note_id": row.get('clinical_note_id'),
                "termlink_id": row.get('termlink_id'),  # Include termlink_id
                # Parse struct fields
                "note_context": parse_struct_field(row.get('note_context')),
                "creator": parse_struct_field(row.get('creator')),
                "text_annotation": parse_struct_field(row.get('text_annotation')),
                "qualifiers": row.get('qualifiers'),  # Include qualifiers field
                "term_source": parse_struct_field(row.get('term_source'))  # Add term_source field
            }
            
            evidence.append(record)
        
        logger.info(f"Returning {len(evidence)} evidence records")
        return evidence
        
    except Exception as e:
        logger.error(f"Error getting evidence for termlink: {e}")
        return []


def get_subject_term_info(
    subject_id: str,
    term_iri: str,
    qualifiers: List[str] = None
) -> Dict[str, Any] | None:
    """
    Get detailed term link information for a specific term on a subject from Iceberg.

    Args:
        subject_id: The subject ID (not IRI)
        term_iri: The specific term IRI
        qualifiers: List of qualifier names (short names like 'negated', not IRIs)

    Returns:
        Dict with term details and term_links array, or None if not found
    """
    logger.info(f"get_subject_term_info called with subject_id={subject_id}, term_iri={term_iri}, qualifiers={qualifiers}")
    
    try:
        evidence_data = get_evidence_for_termlink(subject_id, term_iri, qualifiers)
        logger.info(f"get_evidence_for_termlink returned {len(evidence_data) if evidence_data else 0} records")
    except Exception as e:
        logger.error(f"Exception in get_evidence_for_termlink: {e}", exc_info=True)
        return None
    
    if not evidence_data:
        logger.info("No evidence data found, returning None")
        return None

    # Use the termlink_id from the evidence records (already stored correctly)
    # Note: All evidence in evidence_data should have the same termlink_id since we filtered by it
    subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
    termlink_hash = evidence_data[0].get('termlink_id')

    # The termlink_id in evidence is stored as just the hash, construct full IRI
    termlink_iri = f"{subject_iri}/term-link/{termlink_hash}"
    
    # Create single termlink with all evidence
    source_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
    
    term_links = [{
        "termlink_iri": termlink_iri,
        "evidence_count": len(evidence_data),
        "source_iri": source_iri,
        "source_type": evidence_data[0].get('evidence_type', 'http://purl.obolibrary.org/obo/ECO_0006162') if evidence_data else None
    }]
    
    # Extract short qualifier names from the evidence data
    active_qualifiers = []
    if evidence_data and evidence_data[0].get('qualifiers'):
        qualifiers_from_evidence = evidence_data[0]['qualifiers']
        if isinstance(qualifiers_from_evidence, str):
            # Parse if it's a string (might be JSON or struct format)
            active_qualifiers = parse_qualifiers_field(qualifiers_from_evidence)
        elif isinstance(qualifiers_from_evidence, list):
            # Extract active qualifiers from list format
            for q in qualifiers_from_evidence:
                if isinstance(q, dict) and q.get('qualifier_value') in ['true', '1', 1, 1.0, True]:
                    qualifier_type = q.get('qualifier_type', '')
                    # Extract short name from full IRI if needed
                    # NOTE: This assumes all qualifiers are in the same namespace and can be
                    # shortened by taking the last path segment. If we need to support
                    # qualifiers from multiple namespaces in the future, this logic should
                    # be moved to a dedicated function that handles namespace mapping.
                    if qualifier_type.startswith('http://') or qualifier_type.startswith('https://'):
                        qualifier_type = qualifier_type.split('/')[-1]
                    active_qualifiers.append(qualifier_type)
    
    return {
        "term_iri": term_iri,
        "qualifiers": sorted(active_qualifiers),  # Return short names of active qualifiers
        "evidence_count": len(evidence_data),
        "term_links": term_links
    }


# =============================================================================
# Subject-Terms Analytical Table Query Functions
# =============================================================================

def query_subject_by_id(subject_id: str) -> List[Dict[str, Any]]:
    """
    Query all terms for a specific subject using the subject_terms_by_subject table.
    Optimized for individual subject lookups.

    Args:
        subject_id: The subject ID

    Returns:
        List of term records for the subject
    """
    database_name = os.environ.get('ICEBERG_DATABASE')
    table_name = os.environ.get('ICEBERG_SUBJECT_TERMS_BY_SUBJECT_TABLE')

    if not database_name or not table_name:
        raise ValueError("ICEBERG_DATABASE and ICEBERG_SUBJECT_TERMS_BY_SUBJECT_TABLE environment variables are required")

    query = f"""
    SELECT
        subject_id,
        subject_iri,
        term_iri,
        term_id,
        term_label,
        qualifiers,
        evidence_count,
        termlink_id,
        first_evidence_date,
        last_evidence_date
    FROM {database_name}.{table_name}
    WHERE subject_id = '{subject_id}'
    ORDER BY term_id
    """

    try:
        results = query_iceberg_evidence(query)

        # Parse results
        terms = []
        for row in results:
            term = {
                "subject_id": row.get('subject_id'),
                "subject_iri": row.get('subject_iri'),
                "term_iri": row.get('term_iri'),
                "term_id": row.get('term_id'),
                "term_label": row.get('term_label'),
                "evidence_count": int(row.get('evidence_count', 0)) if row.get('evidence_count') else 0,
                "termlink_id": row.get('termlink_id'),
                "first_evidence_date": row.get('first_evidence_date'),
                "last_evidence_date": row.get('last_evidence_date')
            }

            # Parse qualifiers array
            qualifiers_str = row.get('qualifiers')
            if qualifiers_str and qualifiers_str != '[]' and qualifiers_str != 'null':
                # Parse array format like ['negated', 'severity_moderate']
                qualifiers_str = qualifiers_str.strip('[]')
                if qualifiers_str:
                    term["qualifiers"] = [q.strip().strip("'\"") for q in qualifiers_str.split(',')]
                else:
                    term["qualifiers"] = []
            else:
                term["qualifiers"] = []

            terms.append(term)

        logger.info(f"Found {len(terms)} terms for subject {subject_id}")
        return terms

    except Exception as e:
        logger.error(f"Error querying subject by ID {subject_id}: {e}")
        raise


def query_subjects_by_project(
    project_id: str,
    term_id: str = None,
    term_source: str = None,
    term_source_version: str = None,
    include_child_terms: bool = True,
    include_qualified: bool = True,
    project_subject_ids: list = None,
    limit: int = 50,
    offset: int = 0
) -> Dict[str, Any]:
    """
    Query subjects in a project with optional term filtering and pagination.
    Uses the subject_terms_by_project_term table for efficient queries.

    Supports hierarchy expansion via ontology_hierarchy table when include_child_terms=True.

    Args:
        project_id: The project ID
        term_id: Optional term ID to filter by (e.g., "HP:0001234")
        term_source: Ontology source ("hpo" or "mondo") - inferred from term_id if not provided
        term_source_version: Optional ontology version - if not provided, uses latest version
        include_child_terms: If True, expand term_id to include all descendants (default True)
        include_qualified: If False, exclude terms with negated/hypothetical/family qualifiers (default True)
        project_subject_ids: Optional list of specific project_subject_ids to filter by
        limit: Number of subjects to return (default 50)
        offset: Offset for pagination (default 0)

    Returns:
        Dict with:
            - subjects: List of subject records with phenotypes in API format
            - pagination: Pagination metadata (limit, cursor, next_cursor, has_more, total_count)
    """
    from phebee.utils.hash import generate_termlink_hash

    database_name = os.environ.get('ICEBERG_DATABASE')
    table_name = os.environ.get('ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE')

    if not database_name or not table_name:
        raise ValueError("ICEBERG_DATABASE and ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE environment variables are required")

    # Infer ontology source from term_id if not provided
    if term_id and not term_source:
        if term_id.startswith('HP:'):
            term_source = 'hpo'
        elif term_id.startswith('MONDO:'):
            term_source = 'mondo'

    # Build WHERE clauses
    where_clauses = [f"project_id = '{project_id}'"]

    # Build term filter with hierarchy expansion
    if term_id:
        if include_child_terms:
            # Expand to include all descendant terms using hierarchy table
            try:
                logger.info(f"Querying descendants for {term_id}, ontology_source={term_source}, version={term_source_version}")
                descendant_ids = query_term_descendants(term_id, ontology_source=term_source, version=term_source_version)
                if descendant_ids:
                    # Extract just the term_id from results
                    term_ids = [d['term_id'] for d in descendant_ids]
                    # Add the query term itself if not already included
                    if term_id not in term_ids:
                        term_ids.append(term_id)
                    # Build IN clause
                    term_list = "', '".join(term_ids)
                    where_clauses.append(f"term_id IN ('{term_list}')")
                    logger.info(f"Expanded {term_id} to {len(term_ids)} descendant terms: {term_ids[:5]}..." if len(term_ids) > 5 else f"Expanded {term_id} to {len(term_ids)} descendant terms: {term_ids}")
                else:
                    # No descendants found, just use exact match
                    logger.warning(f"No descendants found for {term_id} (ontology_source={term_source}, version={term_source_version}), falling back to exact match")
                    where_clauses.append(f"term_id = '{term_id}'")
            except Exception as e:
                logger.warning(f"Error querying term descendants for {term_id} (ontology_source={term_source}, version={term_source_version}): {e}")
                # Fall back to exact match
                where_clauses.append(f"term_id = '{term_id}'")
        else:
            # Exact match only
            where_clauses.append(f"term_id = '{term_id}'")

    # Build qualifier filter
    if not include_qualified:
        # Exclude terms with negated, hypothetical, or family qualifiers
        # Check both short names (legacy) and full IRIs (current) for backward compatibility
        where_clauses.append("""
            NOT (
                CONTAINS(qualifiers, 'negated') OR
                CONTAINS(qualifiers, 'http://ods.nationwidechildrens.org/phebee/qualifier/negated') OR
                CONTAINS(qualifiers, 'hypothetical') OR
                CONTAINS(qualifiers, 'http://ods.nationwidechildrens.org/phebee/qualifier/hypothetical') OR
                CONTAINS(qualifiers, 'family') OR
                CONTAINS(qualifiers, 'http://ods.nationwidechildrens.org/phebee/qualifier/family')
            )
        """)

    # Build project_subject_ids filter
    if project_subject_ids:
        psid_list = "', '".join(project_subject_ids)
        where_clauses.append(f"project_subject_id IN ('{psid_list}')")

    where_clause = " AND ".join(where_clauses)

    # Query to get subjects with aggregated term data
    # In Athena with Iceberg, OFFSET must come before LIMIT
    # Only include OFFSET if > 0 to avoid potential issues with OFFSET 0
    if offset > 0:
        pagination_clause = f"OFFSET {offset} LIMIT {limit}"
    else:
        pagination_clause = f"LIMIT {limit}"

    query = f"""
    SELECT
        subject_id,
        project_subject_id,
        subject_iri,
        project_subject_iri,
        ARRAY_AGG(
            ROW(term_id, term_iri, term_label, qualifiers, evidence_count, termlink_id, first_evidence_date, last_evidence_date)
        ) as terms
    FROM {database_name}.{table_name}
    WHERE {where_clause}
    GROUP BY subject_id, project_subject_id, subject_iri, project_subject_iri
    ORDER BY subject_id
    {pagination_clause}
    """

    # Count query for total
    count_query = f"""
    SELECT COUNT(DISTINCT subject_id) as total
    FROM {database_name}.{table_name}
    WHERE {where_clause}
    """

    try:
        # Execute queries in parallel to reduce latency
        # Both queries are independent and can run concurrently
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit both queries
            data_future = executor.submit(query_iceberg_evidence, query)
            count_future = executor.submit(query_iceberg_evidence, count_query)

            # Wait for both to complete
            results = data_future.result()
            count_results = count_future.result()

        total_count = int(count_results[0]['total']) if count_results else 0

        # Parse subjects and convert to API format
        subjects = []
        for row in results:
            # Parse terms array - this comes as a string representation of ROW structures
            terms_str = row.get('terms')
            phenotypes = []

            if terms_str and terms_str != '[]' and terms_str != 'null':
                # Parse the array of ROW structures
                # ROW format: (term_id, term_iri, term_label, qualifiers, evidence_count, termlink_id, first_evidence_date, last_evidence_date)
                field_names = ['term_id', 'term_iri', 'term_label', 'qualifiers', 'evidence_count', 'termlink_id', 'first_evidence_date', 'last_evidence_date']
                terms_list = parse_athena_row_array(terms_str, field_names)
                for term_dict in terms_list:
                    # Parse qualifiers
                    qualifiers_str = term_dict.get('qualifiers')
                    qualifiers = []
                    if qualifiers_str and qualifiers_str != '[]' and qualifiers_str != 'null':
                        qualifiers_str = qualifiers_str.strip('[]')
                        if qualifiers_str:
                            qualifiers = [q.strip().strip("'\"") for q in qualifiers_str.split(',')]

                    # Build phenotype in API format
                    phenotype = {
                        "term": {
                            "iri": term_dict.get('term_iri'),
                            "id": term_dict.get('term_id'),
                            "label": term_dict.get('term_label') or term_dict.get('term_id')
                        },
                        "qualifiers": qualifiers,
                        "termlink_id": term_dict.get('termlink_id'),
                        "evidence_count": int(term_dict.get('evidence_count', 0)) if term_dict.get('evidence_count') else 0,
                        "first_evidence_date": term_dict.get('first_evidence_date'),
                        "last_evidence_date": term_dict.get('last_evidence_date')
                    }
                    phenotypes.append(phenotype)

            # Build subject in API format
            subject_data = {
                "subject_iri": row.get('subject_iri'),
                "project_subject_iri": row.get('project_subject_iri'),
                "project_subject_id": row.get('project_subject_id'),
                "phenotypes": phenotypes
            }
            subjects.append(subject_data)

        has_more = (offset + len(subjects)) < total_count

        logger.info(f"Found {len(subjects)} subjects for project {project_id} (total: {total_count}, offset: {offset})")

        # Build pagination in API format
        next_cursor = str(offset + len(subjects)) if has_more else None
        pagination = {
            "limit": limit,
            "cursor": str(offset) if offset > 0 else None,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "total_count": total_count
        }

        return {
            "subjects": subjects,
            "pagination": pagination
        }

    except Exception as e:
        logger.error(f"Error querying subjects by project {project_id}: {e}")
        raise


def query_subject_with_hierarchy(
    subject_id: str,
    ontology_source: str = "hpo",
    ontology_version: str = None
) -> Dict[str, Any]:
    """
    Query a subject's terms with ontology hierarchy information.
    Joins subject_terms_by_subject with ontology_hierarchy table.

    Args:
        subject_id: The subject ID
        ontology_source: Ontology source (default "HPO")
        ontology_version: Optional specific version (uses latest if not specified)

    Returns:
        Dict with subject info and terms enriched with hierarchy data
    """
    database_name = os.environ.get('ICEBERG_DATABASE')
    subject_terms_table = os.environ.get('ICEBERG_SUBJECT_TERMS_BY_SUBJECT_TABLE')
    hierarchy_table = os.environ.get('ICEBERG_ONTOLOGY_HIERARCHY_TABLE')

    if not database_name or not subject_terms_table or not hierarchy_table:
        raise ValueError("ICEBERG_DATABASE, ICEBERG_SUBJECT_TERMS_BY_SUBJECT_TABLE, and ICEBERG_ONTOLOGY_HIERARCHY_TABLE environment variables are required")

    # Build version filter
    version_filter = f"AND h.version = '{ontology_version}'" if ontology_version else ""

    # If no version specified, get latest version first
    version_query = ""
    if not ontology_version:
        version_query = f"""
        SELECT version
        FROM {database_name}.{hierarchy_table}
        WHERE ontology_source = '{ontology_source}'
        ORDER BY version DESC
        LIMIT 1
        """
        try:
            version_results = query_iceberg_evidence(version_query)
            if version_results:
                ontology_version = version_results[0]['version']
                version_filter = f"AND h.version = '{ontology_version}'"
        except:
            pass  # Continue without version filter if query fails

    # Join query
    query = f"""
    SELECT
        st.subject_id,
        st.subject_iri,
        st.term_iri,
        st.term_id,
        st.term_label as current_term_label,
        st.qualifiers,
        st.evidence_count,
        st.termlink_id,
        st.first_evidence_date,
        st.last_evidence_date,
        h.term_label as hierarchy_term_label,
        h.ancestor_term_ids,
        h.version as ontology_version
    FROM {database_name}.{subject_terms_table} st
    LEFT JOIN {database_name}.{hierarchy_table} h
        ON st.term_id = h.term_id
        AND h.ontology_source = '{ontology_source}'
        {version_filter}
    WHERE st.subject_id = '{subject_id}'
    ORDER BY st.term_id
    """

    try:
        results = query_iceberg_evidence(query)

        if not results:
            return None

        # Build subject record
        first_row = results[0]
        subject = {
            "subject_id": first_row.get('subject_id'),
            "subject_iri": first_row.get('subject_iri'),
            "ontology_source": ontology_source,
            "ontology_version": first_row.get('ontology_version'),
            "terms": []
        }

        # Parse each term with hierarchy info
        for row in results:
            term = {
                "term_iri": row.get('term_iri'),
                "term_id": row.get('term_id'),
                "term_label": row.get('hierarchy_term_label') or row.get('current_term_label'),  # Prefer hierarchy label
                "evidence_count": int(row.get('evidence_count', 0)) if row.get('evidence_count') else 0,
                "termlink_id": row.get('termlink_id'),
                "first_evidence_date": row.get('first_evidence_date'),
                "last_evidence_date": row.get('last_evidence_date')
            }

            # Parse qualifiers
            qualifiers_str = row.get('qualifiers')
            if qualifiers_str and qualifiers_str != '[]' and qualifiers_str != 'null':
                qualifiers_str = qualifiers_str.strip('[]')
                if qualifiers_str:
                    term["qualifiers"] = [q.strip().strip("'\"") for q in qualifiers_str.split(',')]
                else:
                    term["qualifiers"] = []
            else:
                term["qualifiers"] = []

            # Parse ancestor_term_ids
            ancestors_str = row.get('ancestor_term_ids')
            if ancestors_str and ancestors_str != '[]' and ancestors_str != 'null':
                ancestors_str = ancestors_str.strip('[]')
                if ancestors_str:
                    term["ancestor_term_ids"] = [a.strip().strip("'\"") for a in ancestors_str.split(',')]
                else:
                    term["ancestor_term_ids"] = []
            else:
                term["ancestor_term_ids"] = []

            subject["terms"].append(term)

        logger.info(f"Found {len(subject['terms'])} terms with hierarchy for subject {subject_id}")
        return subject

    except Exception as e:
        logger.error(f"Error querying subject with hierarchy {subject_id}: {e}")
        raise


# =============================================================================
# Subject-Terms Materialization Functions (Write Operations)
# =============================================================================

def _execute_athena_query(query: str, wait_for_completion: bool = True) -> str:
    """
    Helper function to execute an Athena query.

    Args:
        query: SQL query to execute
        wait_for_completion: Whether to wait for query to complete

    Returns:
        Query execution ID
    """
    athena_client = boto3.client('athena')
    database_name = os.environ.get('ICEBERG_DATABASE')
    bucket_name = os.environ.get('PHEBEE_BUCKET_NAME')

    if not database_name:
        raise ValueError("ICEBERG_DATABASE environment variable is required")

    # Check if primary workgroup is managed (cached to avoid rate limiting)
    managed, wg_cfg = _get_workgroup_config_cached(athena_client)

    params = {
        "QueryString": query,
        "QueryExecutionContext": {"Database": database_name},
        "WorkGroup": "primary"
    }

    if not managed:
        params["ResultConfiguration"] = {"OutputLocation": f"s3://{bucket_name}/athena-results/"}

    response = athena_client.start_query_execution(**params)
    query_execution_id = response['QueryExecutionId']

    if wait_for_completion:
        # Wait for query completion
        # Poll every 0.5s for fast queries (most queries complete in 1-5 seconds)
        while True:
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']

            if status == 'SUCCEEDED':
                break
            elif status in ['FAILED', 'CANCELLED']:
                error_msg = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                raise Exception(f"Athena query failed: {error_msg}")

            time.sleep(0.5)

    return query_execution_id


def _extract_term_id_from_iri(term_iri: str) -> str:
    """
    Extract term ID from IRI (e.g., HP:0001234 from http://purl.obolibrary.org/obo/HP_0001234).

    Args:
        term_iri: The term IRI

    Returns:
        The term ID (e.g., "HP:0001234")
    """
    if not term_iri:
        return None

    # Handle OBO format: http://purl.obolibrary.org/obo/HP_0001234 -> HP:0001234
    if '/obo/' in term_iri:
        term_part = term_iri.split('/obo/')[-1]
        return term_part.replace('_', ':')

    # Handle other formats - just take the last part
    return term_iri.split('/')[-1]


def materialize_project(project_id: str, batch_size: int = 100) -> Dict[str, int]:
    """
    Materialize all subject-term associations for an entire project.
    More efficient than individual subject materialization for bulk operations.

    Since evidence is project-agnostic, this function:
    1. Queries DynamoDB to get all subjects in this project
    2. Materializes by_subject_table rows (project-agnostic) for those subjects
    3. Materializes by_project_term_table rows (project-specific) for this project

    Args:
        project_id: The project ID
        batch_size: Number of subjects to process per batch to avoid ICEBERG_TOO_MANY_OPEN_PARTITIONS error

    Returns:
        Dict with statistics: subjects_processed, terms_materialized
    """
    from phebee.utils.dynamodb import _get_table_name
    import boto3

    database_name = os.environ['ICEBERG_DATABASE']
    evidence_table = os.environ['ICEBERG_EVIDENCE_TABLE']
    by_subject_table = os.environ['ICEBERG_SUBJECT_TERMS_BY_SUBJECT_TABLE']
    by_project_term_table = os.environ['ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE']

    logger.info(f"Materializing project {project_id} with batch_size={batch_size}")

    # Step 1: Get all subjects in this project from DynamoDB
    table_name = _get_table_name()
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table(table_name)

    subject_ids = []
    project_subject_map = {}  # {subject_id: project_subject_id}

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
                    subject_ids.append(subject_id)
                    project_subject_map[subject_id] = project_subject_id

        logger.info(f"Found {len(subject_ids)} subjects in project {project_id}")

    except Exception as e:
        logger.error(f"Error querying DynamoDB for project subjects: {e}")
        raise

    if not subject_ids:
        logger.warning(f"No subjects found for project {project_id}")
        return {"subjects_processed": 0, "terms_materialized": 0}

    # Step 2: Delete existing records for this project (do once, not per batch)
    # Delete from by_project_term_table first (project-specific)
    delete_by_project = f"""
    DELETE FROM {database_name}.{by_project_term_table}
    WHERE project_id = '{project_id}'
    """

    try:
        _execute_athena_query(delete_by_project)
        logger.info(f"Deleted existing by_project_term records for project {project_id}")
    except Exception as e:
        logger.warning(f"Error deleting from by_project_term: {e}")

    # Delete from by_subject_table (all subjects at once - this is project-agnostic)
    subject_id_list_all = "', '".join(subject_ids)
    subject_filter_all = f"WHERE subject_id IN ('{subject_id_list_all}')"

    delete_by_subject = f"""
    DELETE FROM {database_name}.{by_subject_table}
    {subject_filter_all}
    """

    try:
        _execute_athena_query(delete_by_subject)
        logger.info(f"Deleted existing by_subject records for {len(subject_ids)} subjects")
    except Exception as e:
        logger.warning(f"Error deleting from by_subject: {e}")

    # Step 3: Process subjects in batches to avoid TOO_MANY_OPEN_PARTITIONS error
    num_batches = (len(subject_ids) + batch_size - 1) // batch_size
    logger.info(f"Processing {len(subject_ids)} subjects in {num_batches} batches of size {batch_size}")

    try:
        for batch_num in range(num_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(subject_ids))
            batch_subject_ids = subject_ids[start_idx:end_idx]

            logger.info(f"Processing batch {batch_num + 1}/{num_batches} ({len(batch_subject_ids)} subjects)")

            # Build WHERE clause for this batch
            batch_subject_id_list = "', '".join(batch_subject_ids)
            batch_subject_filter = f"WHERE subject_id IN ('{batch_subject_id_list}')"

            # Build aggregation query for by_subject_table (project-agnostic)
            aggregate_by_subject = f"""
            WITH aggregated AS (
                SELECT
                    subject_id,
                    CONCAT('http://ods.nationwidechildrens.org/phebee/subjects/', subject_id) as subject_iri,
                    term_iri,
                    COUNT(*) as evidence_count,
                    MIN(CAST(note_context.note_date AS DATE)) as first_evidence_date,
                    MAX(CAST(note_context.note_date AS DATE)) as last_evidence_date,
                    ARBITRARY(termlink_id) as termlink_id,
                    ARRAY_AGG(DISTINCT
                        CASE
                            WHEN q.qualifier_value IN ('true', '1') THEN q.qualifier_type
                            ELSE NULL
                        END
                    ) as active_qualifiers
                FROM {database_name}.{evidence_table}
                LEFT JOIN UNNEST(COALESCE(qualifiers, ARRAY[])) AS t(q) ON TRUE
                {batch_subject_filter}
                GROUP BY subject_id, term_iri
            )
            SELECT
                subject_id,
                subject_iri,
                term_iri,
                REGEXP_REPLACE(
                    REGEXP_REPLACE(term_iri, '.*/obo/', ''),
                    '_', ':'
                ) as term_id,
                CAST(NULL AS VARCHAR) as term_label,
                FILTER(active_qualifiers, x -> x IS NOT NULL) as qualifiers,
                evidence_count,
                termlink_id,
                first_evidence_date,
                last_evidence_date
            FROM aggregated
            """

            # Build aggregation query for by_project_term_table (project-specific)
            # Create mapping CTE for this batch only
            batch_mapping_values = []
            for subject_id in batch_subject_ids:
                project_subject_id = project_subject_map[subject_id]
                batch_mapping_values.append(f"('{subject_id}', '{project_subject_id}')")

            mapping_cte = f"""
            subject_mapping AS (
                SELECT subject_id, project_subject_id
                FROM (VALUES {', '.join(batch_mapping_values)}) AS t(subject_id, project_subject_id)
            ),
            """

            aggregate_by_project = f"""
            WITH {mapping_cte}
            aggregated AS (
                SELECT
                    '{project_id}' as project_id,
                    e.subject_id,
                    m.project_subject_id,
                    CONCAT('http://ods.nationwidechildrens.org/phebee/subjects/', e.subject_id) as subject_iri,
                    CONCAT('http://ods.nationwidechildrens.org/phebee/projects/{project_id}/', m.project_subject_id) as project_subject_iri,
                    e.term_iri,
                    COUNT(*) as evidence_count,
                    MIN(CAST(e.note_context.note_date AS DATE)) as first_evidence_date,
                    MAX(CAST(e.note_context.note_date AS DATE)) as last_evidence_date,
                    ARBITRARY(e.termlink_id) as termlink_id,
                    ARRAY_AGG(DISTINCT
                        CASE
                            WHEN q.qualifier_value IN ('true', '1') THEN q.qualifier_type
                            ELSE NULL
                        END
                    ) as active_qualifiers
                FROM {database_name}.{evidence_table} e
                JOIN subject_mapping m ON e.subject_id = m.subject_id
                LEFT JOIN UNNEST(COALESCE(e.qualifiers, ARRAY[])) AS t(q) ON TRUE
                GROUP BY e.subject_id, m.project_subject_id, e.term_iri
            )
            SELECT
                project_id,
                subject_id,
                project_subject_id,
                subject_iri,
                project_subject_iri,
                term_iri,
                REGEXP_REPLACE(
                    REGEXP_REPLACE(term_iri, '.*/obo/', ''),
                    '_', ':'
                ) as term_id,
                CAST(NULL AS VARCHAR) as term_label,
                FILTER(active_qualifiers, x -> x IS NOT NULL) as qualifiers,
                evidence_count,
                termlink_id,
                first_evidence_date,
                last_evidence_date
            FROM aggregated
            """

            # Insert into by_subject_table for this batch
            insert_by_subject_query = f"""
            INSERT INTO {database_name}.{by_subject_table} (
                subject_id,
                subject_iri,
                term_iri,
                term_id,
                term_label,
                qualifiers,
                evidence_count,
                termlink_id,
                first_evidence_date,
                last_evidence_date
            )
            {aggregate_by_subject}
            """
            _execute_athena_query(insert_by_subject_query)
            logger.info(f"Batch {batch_num + 1}/{num_batches}: Inserted by_subject records")

            # Insert into by_project_term_table for this batch
            insert_by_project_query = f"""
            INSERT INTO {database_name}.{by_project_term_table} (
                project_id,
                subject_id,
                project_subject_id,
                subject_iri,
                project_subject_iri,
                term_iri,
                term_id,
                term_label,
                qualifiers,
                evidence_count,
                termlink_id,
                first_evidence_date,
                last_evidence_date
            )
            {aggregate_by_project}
            """
            _execute_athena_query(insert_by_project_query)
            logger.info(f"Batch {batch_num + 1}/{num_batches}: Inserted by_project_term records")

        # Step 4: Get final statistics
        stats_query = f"""
        SELECT
            COUNT(DISTINCT subject_id) as subjects,
            COUNT(*) as terms
        FROM {database_name}.{by_project_term_table}
        WHERE project_id = '{project_id}'
        """

        stats_results = query_iceberg_evidence(stats_query)
        stats = {
            "subjects_processed": int(stats_results[0]['subjects']) if stats_results else 0,
            "terms_materialized": int(stats_results[0]['terms']) if stats_results else 0
        }

        logger.info(f"Materialized project {project_id}: {stats}")
        return stats

    except Exception as e:
        logger.error(f"Error materializing project {project_id}: {e}")
        raise


def materialize_subject_terms(subject_id: str) -> Dict[str, int]:
    """
    Materialize subject-term associations for a single subject across all projects.

    This function is called when:
    1. A subject is linked to a new project (need to make existing evidence visible in new project)
    2. Evidence is added/removed for a subject (need to update materialized views)

    Since evidence is project-agnostic, this function:
    1. Queries DynamoDB to get all projects this subject belongs to
    2. Deletes existing materialized records for this subject
    3. Re-materializes by_subject_table (project-agnostic) for this subject
    4. Re-materializes by_project_term_table for all projects this subject belongs to

    Args:
        subject_id: The subject UUID

    Returns:
        Dict with statistics: projects_affected, terms_materialized
    """
    from phebee.utils.dynamodb import _get_table_name
    import boto3

    database_name = os.environ['ICEBERG_DATABASE']
    evidence_table = os.environ['ICEBERG_EVIDENCE_TABLE']
    by_subject_table = os.environ['ICEBERG_SUBJECT_TERMS_BY_SUBJECT_TABLE']
    by_project_term_table = os.environ['ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE']

    logger.info(f"Materializing subject {subject_id}")

    # Step 1: Get all projects this subject belongs to from DynamoDB
    table_name = _get_table_name()
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table(table_name)

    project_mappings = {}  # {project_id: project_subject_id}

    try:
        # Query reverse mappings: PK='SUBJECT#{subject_id}'
        response = table.query(
            KeyConditionExpression='PK = :pk',
            ExpressionAttributeValues={':pk': f'SUBJECT#{subject_id}'}
        )

        for item in response.get('Items', []):
            # Parse SK: "PROJECT#{project_id}#SUBJECT#{project_subject_id}"
            sk = item.get('SK', '')
            if sk.startswith('PROJECT#'):
                parts = sk.split('#')
                if len(parts) >= 4:
                    project_id = parts[1]
                    project_subject_id = parts[3]
                    project_mappings[project_id] = project_subject_id

        logger.info(f"Subject {subject_id} belongs to {len(project_mappings)} projects: {list(project_mappings.keys())}")

    except Exception as e:
        logger.error(f"Error querying DynamoDB for subject projects: {e}")
        raise

    if not project_mappings:
        logger.warning(f"No project mappings found for subject {subject_id}")
        return {"projects_affected": 0, "terms_materialized": 0}

    # Step 2: Delete existing records for this subject from by_subject_table
    delete_by_subject = f"""
    DELETE FROM {database_name}.{by_subject_table}
    WHERE subject_id = '{subject_id}'
    """

    try:
        _execute_athena_query(delete_by_subject)
        logger.info(f"Deleted existing by_subject records for subject {subject_id}")
    except Exception as e:
        logger.warning(f"Error deleting from by_subject: {e}")

    # Step 3: Delete existing records for this subject from by_project_term_table
    delete_by_project = f"""
    DELETE FROM {database_name}.{by_project_term_table}
    WHERE subject_id = '{subject_id}'
    """

    try:
        _execute_athena_query(delete_by_project)
        logger.info(f"Deleted existing by_project_term records for subject {subject_id}")
    except Exception as e:
        logger.warning(f"Error deleting from by_project_term: {e}")

    # Step 4: Build aggregation query for by_subject_table (project-agnostic)
    aggregate_by_subject = f"""
    WITH aggregated AS (
        SELECT
            subject_id,
            CONCAT('http://ods.nationwidechildrens.org/phebee/subjects/', subject_id) as subject_iri,
            term_iri,
            COUNT(*) as evidence_count,
            MIN(CAST(note_context.note_date AS DATE)) as first_evidence_date,
            MAX(CAST(note_context.note_date AS DATE)) as last_evidence_date,
            ARBITRARY(termlink_id) as termlink_id,
            ARRAY_AGG(DISTINCT
                CASE
                    WHEN q.qualifier_value IN ('true', '1') THEN q.qualifier_type
                    ELSE NULL
                END
            ) as active_qualifiers
        FROM {database_name}.{evidence_table}
        LEFT JOIN UNNEST(COALESCE(qualifiers, ARRAY[])) AS t(q) ON TRUE
        WHERE subject_id = '{subject_id}'
        GROUP BY subject_id, term_iri
    )
    SELECT
        subject_id,
        subject_iri,
        term_iri,
        REGEXP_REPLACE(
            REGEXP_REPLACE(term_iri, '.*/obo/', ''),
            '_', ':'
        ) as term_id,
        CAST(NULL AS VARCHAR) as term_label,
        FILTER(active_qualifiers, x -> x IS NOT NULL) as qualifiers,
        evidence_count,
        termlink_id,
        first_evidence_date,
        last_evidence_date
    FROM aggregated
    """

    # Step 5: Build aggregation query for by_project_term_table (project-specific)
    # Create mapping CTE for all projects this subject belongs to
    mapping_values = []
    for project_id, project_subject_id in project_mappings.items():
        mapping_values.append(f"('{project_id}', '{subject_id}', '{project_subject_id}')")

    mapping_cte = f"""
    subject_mapping AS (
        SELECT project_id, subject_id, project_subject_id
        FROM (VALUES {', '.join(mapping_values)}) AS t(project_id, subject_id, project_subject_id)
    ),
    """

    aggregate_by_project = f"""
    WITH {mapping_cte}
    aggregated AS (
        SELECT
            m.project_id,
            e.subject_id,
            m.project_subject_id,
            CONCAT('http://ods.nationwidechildrens.org/phebee/subjects/', e.subject_id) as subject_iri,
            CONCAT('http://ods.nationwidechildrens.org/phebee/projects/', m.project_id, '/', m.project_subject_id) as project_subject_iri,
            e.term_iri,
            COUNT(*) as evidence_count,
            MIN(CAST(e.note_context.note_date AS DATE)) as first_evidence_date,
            MAX(CAST(e.note_context.note_date AS DATE)) as last_evidence_date,
            ARBITRARY(e.termlink_id) as termlink_id,
            ARRAY_AGG(DISTINCT
                CASE
                    WHEN q.qualifier_value IN ('true', '1') THEN q.qualifier_type
                    ELSE NULL
                END
            ) as active_qualifiers
        FROM {database_name}.{evidence_table} e
        JOIN subject_mapping m ON e.subject_id = m.subject_id
        LEFT JOIN UNNEST(COALESCE(e.qualifiers, ARRAY[])) AS t(q) ON TRUE
        GROUP BY m.project_id, e.subject_id, m.project_subject_id, e.term_iri
    )
    SELECT
        project_id,
        subject_id,
        project_subject_id,
        subject_iri,
        project_subject_iri,
        term_iri,
        REGEXP_REPLACE(
            REGEXP_REPLACE(term_iri, '.*/obo/', ''),
            '_', ':'
        ) as term_id,
        CAST(NULL AS VARCHAR) as term_label,
        FILTER(active_qualifiers, x -> x IS NOT NULL) as qualifiers,
        evidence_count,
        termlink_id,
        first_evidence_date,
        last_evidence_date
    FROM aggregated
    """

    try:
        # Step 6: Insert into by_subject_table (project-agnostic)
        insert_by_subject_query = f"""
        INSERT INTO {database_name}.{by_subject_table} (
            subject_id,
            subject_iri,
            term_iri,
            term_id,
            term_label,
            qualifiers,
            evidence_count,
            termlink_id,
            first_evidence_date,
            last_evidence_date
        )
        {aggregate_by_subject}
        """
        _execute_athena_query(insert_by_subject_query)
        logger.info(f"Inserted by_subject records for subject {subject_id}")

        # Step 7: Insert into by_project_term_table (project-specific for all projects)
        insert_by_project_query = f"""
        INSERT INTO {database_name}.{by_project_term_table} (
            project_id,
            subject_id,
            project_subject_id,
            subject_iri,
            project_subject_iri,
            term_iri,
            term_id,
            term_label,
            qualifiers,
            evidence_count,
            termlink_id,
            first_evidence_date,
            last_evidence_date
        )
        {aggregate_by_project}
        """
        _execute_athena_query(insert_by_project_query)
        logger.info(f"Inserted by_project_term records for subject {subject_id} across {len(project_mappings)} projects")

        # Step 8: Get statistics
        stats_query = f"""
        SELECT
            COUNT(DISTINCT project_id) as projects,
            COUNT(*) as terms
        FROM {database_name}.{by_project_term_table}
        WHERE subject_id = '{subject_id}'
        """

        stats_results = query_iceberg_evidence(stats_query)
        stats = {
            "projects_affected": int(stats_results[0]['projects']) if stats_results else 0,
            "terms_materialized": int(stats_results[0]['terms']) if stats_results else 0
        }

        logger.info(f"Materialized subject {subject_id}: {stats}")
        return stats

    except Exception as e:
        logger.error(f"Error materializing subject {subject_id}: {e}")
        raise


def delete_subject_terms(subject_id: str, project_id: str = None, include_by_subject: bool = True) -> bool:
    """
    Delete subject-term records for a specific subject.
    Called when a subject is removed or unlinked from a project.

    Args:
        subject_id: The subject ID
        project_id: Optional project ID to limit deletion to specific project
        include_by_subject: Whether to delete from by_subject table (default True)
                           Set to False when unlinking from a project but subject still exists

    Returns:
        True if successful
    """
    database_name = os.environ['ICEBERG_DATABASE']
    by_subject_table = os.environ['ICEBERG_SUBJECT_TERMS_BY_SUBJECT_TABLE']
    by_project_term_table = os.environ['ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE']

    logger.info(f"Deleting subject-terms for subject {subject_id}" +
                (f" in project {project_id}" if project_id else "") +
                (f" (including by_subject table)" if include_by_subject else " (by_project_term only)"))

    try:
        # Delete from by_subject table only if this is the last mapping
        if include_by_subject:
            delete_query_1 = f"""
            DELETE FROM {database_name}.{by_subject_table}
            WHERE subject_id = '{subject_id}'
            """
            _execute_athena_query(delete_query_1)
            logger.info(f"Deleted from by_subject table for subject {subject_id}")

        # Always delete from by_project_term table
        if project_id:
            # More efficient with project_id filter (uses partition)
            delete_query_2 = f"""
            DELETE FROM {database_name}.{by_project_term_table}
            WHERE project_id = '{project_id}' AND subject_id = '{subject_id}'
            """
        else:
            # Delete from all projects (less efficient, scans all partitions)
            delete_query_2 = f"""
            DELETE FROM {database_name}.{by_project_term_table}
            WHERE subject_id = '{subject_id}'
            """
        _execute_athena_query(delete_query_2)
        logger.info(f"Deleted from by_project_term table for subject {subject_id}" +
                   (f" in project {project_id}" if project_id else " (all projects)"))

        logger.info(f"Successfully deleted subject-terms for subject {subject_id}")
        return True

    except Exception as e:
        logger.error(f"Error deleting subject-terms for subject {subject_id}: {e}")
        raise


def delete_project_subject_terms(project_id: str) -> bool:
    """
    Delete all subject-term records for a project.
    Called when a project is removed.

    Args:
        project_id: The project ID

    Returns:
        True if successful
    """
    database_name = os.environ['ICEBERG_DATABASE']
    by_project_term_table = os.environ['ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE']

    logger.info(f"Deleting subject-terms for project {project_id}")

    # Delete from by_project_term table (partitioned by project_id, so efficient)
    delete_query = f"""
    DELETE FROM {database_name}.{by_project_term_table}
    WHERE project_id = '{project_id}'
    """

    try:
        _execute_athena_query(delete_query)
        logger.info(f"Successfully deleted subject-terms for project {project_id}")
        return True

    except Exception as e:
        logger.error(f"Error deleting subject-terms for project {project_id}: {e}")
        raise


def delete_all_evidence_for_subject(subject_id: str) -> Dict[str, Any]:
    """
    Delete all evidence records for a subject and return the termlink_ids that had evidence.
    Called when a subject is being fully removed (last project mapping deleted).

    Args:
        subject_id: The subject UUID

    Returns:
        Dict with 'termlink_ids' (list of termlink_ids that had evidence) and 'evidence_deleted' (count)
    """
    database_name = os.environ['ICEBERG_DATABASE']
    evidence_table = os.environ['ICEBERG_EVIDENCE_TABLE']

    logger.info(f"Deleting all evidence for subject {subject_id}")

    # First, query the distinct termlink_ids and term_iris so we can delete corresponding term links from Neptune
    query_termlinks = f"""
    SELECT DISTINCT termlink_id, term_iri
    FROM {database_name}.{evidence_table}
    WHERE subject_id = '{subject_id}'
    """

    termlink_data = []
    try:
        results = query_iceberg_evidence(query_termlinks)
        termlink_data = [
            {"termlink_id": row.get("termlink_id"), "term_iri": row.get("term_iri")}
            for row in results if row.get("termlink_id")
        ]
        logger.info(f"Found {len(termlink_data)} distinct term links for subject {subject_id}")
    except Exception as e:
        logger.error(f"Error querying termlink_ids for subject {subject_id}: {e}")
        raise

    # Delete all evidence for this subject
    delete_query = f"""
    DELETE FROM {database_name}.{evidence_table}
    WHERE subject_id = '{subject_id}'
    """

    try:
        _execute_athena_query(delete_query)
        logger.info(f"Deleted all evidence for subject {subject_id}")
    except Exception as e:
        logger.error(f"Error deleting evidence for subject {subject_id}: {e}")
        raise

    return {
        "termlink_data": termlink_data,
        "evidence_deleted": len(termlink_data)
    }


    """
    Get all evidence data for a subject-term combination from Iceberg.
    
    Args:
        subject_id: The subject UUID
        term_iri: The term IRI
        qualifiers: List of qualifier IRIs (optional filter)
        
    Returns:
        List of evidence records
    """
    # Build qualifier filter if provided
    qualifier_filter = ""
    if qualifiers:
        qualifier_conditions = []
        for qualifier in qualifiers:
            qualifier_conditions.append(f"JSON_EXTRACT_SCALAR(qualifiers, '$[*].qualifier_type') = '{qualifier}'")
        if qualifier_conditions:
            qualifier_filter = f"AND ({' OR '.join(qualifier_conditions)})"
    
    database_name = os.environ['ICEBERG_DATABASE']
    table_name = os.environ['ICEBERG_EVIDENCE_TABLE']
    
    query = f"""
    SELECT 
        evidence_id,
        evidence_type,
        assertion_type,
        created_timestamp,
        creator.name as creator,
        creator.type as creator_type,
        text_annotation.span_start,
        text_annotation.span_end,
        clinical_note_id,
        encounter_id,
        qualifiers
    FROM {database_name}.{table_name}
    WHERE subject_id = '{subject_id}' 
      AND term_iri = '{term_iri}'
      {qualifier_filter}
    ORDER BY created_timestamp
    """
    
    try:
        results = query_iceberg_evidence(query)
        
        # Format evidence records
        evidence = []
        for row in results:
            record = {
                "evidence_id": row['evidence_id'],
                "evidence_type": row.get('evidence_type'),
                "assertion_type": row.get('assertion_type'),
                "created": row.get('created_timestamp'),
                "creator": {
                    "name": row.get('creator'),
                    "type": row.get('creator_type')
                },
                "clinical_note_id": row.get('clinical_note_id'),
                "encounter_id": row.get('encounter_id')
            }
            
            # Add span info if present
            if row.get('span_start') or row.get('span_end'):
                record["text_annotation"] = {
                    "span_start": int(row['span_start']) if row.get('span_start') else None,
                    "span_end": int(row['span_end']) if row.get('span_end') else None
                }
            
            # Parse qualifiers
            if row.get('qualifiers'):
                try:
                    record["qualifiers"] = parse_qualifiers_field(row['qualifiers'])
                except:
                    record["qualifiers"] = []
            
            evidence.append(record)
        
        return evidence

    except Exception as e:
        logger.error(f"Error getting evidence for subject-term: {e}")
        return []


# ==============================================================================
# Phase 4: Ontology Hierarchy Functions
# ==============================================================================

def query_term_ancestors(
    term_id: str,
    ontology_source: str = "hpo",
    version: str = None,
    database_name: str = None,
    table_name: str = None
) -> List[str]:
    """
    Query ancestor term IDs for a given term from the ontology hierarchy table.

    Args:
        term_id: The term ID (e.g., "HP:0000001")
        ontology_source: The ontology source (default: "HPO")
        version: The ontology version. If None, uses the latest version.
        database_name: Optional override for database name
        table_name: Optional override for table name

    Returns:
        List of ancestor term IDs (including the term itself)
    """
    database_name = database_name or os.environ.get('ICEBERG_DATABASE')
    table_name = table_name or os.environ.get('ICEBERG_ONTOLOGY_HIERARCHY_TABLE')

    if not database_name or not table_name:
        raise ValueError("Database and table name must be provided or set in environment")

    # If no version specified, get the latest version
    if not version:
        version_query = f"""
        SELECT version
        FROM {database_name}.{table_name}
        WHERE LOWER(ontology_source) = LOWER('{ontology_source}')
        GROUP BY version
        ORDER BY version DESC
        LIMIT 1
        """

        try:
            result = _execute_athena_query(version_query, wait_for_completion=True)
            version_rows = _get_query_results(result)

            if not version_rows:
                logger.warning(f"No versions found for ontology source: {ontology_source}")
                return []

            version = version_rows[0]['version']
            logger.info(f"Using latest version {version} for {ontology_source}")

        except Exception as e:
            logger.error(f"Failed to get latest version for {ontology_source}: {e}")
            return []

    # Query ancestors for the specific term
    query = f"""
    SELECT ancestor_term_ids
    FROM {database_name}.{table_name}
    WHERE ontology_source = '{ontology_source}'
      AND version = '{version}'
      AND term_id = '{term_id}'
    """

    try:
        query_execution_id = _execute_athena_query(query, wait_for_completion=True)
        results = _get_query_results(query_execution_id)

        if not results:
            logger.warning(f"No hierarchy data found for term {term_id} in {ontology_source} version {version}")
            return []

        # Extract ancestor_term_ids array
        ancestor_ids = results[0].get('ancestor_term_ids', [])

        # Handle different formats (string representation vs actual array)
        if isinstance(ancestor_ids, str):
            # Parse string representation like "[HP:0000001, HP:0000002]"
            import json
            try:
                ancestor_ids = json.loads(ancestor_ids)
            except:
                # Try parsing as comma-separated list
                ancestor_ids = [aid.strip() for aid in ancestor_ids.strip('[]').split(',') if aid.strip()]

        return ancestor_ids

    except Exception as e:
        logger.error(f"Failed to query ancestors for term {term_id}: {e}")
        return []


def query_term_descendants(
    term_id: str,
    ontology_source: str = "hpo",
    version: str = None,
    database_name: str = None,
    table_name: str = None
) -> List[Dict[str, Any]]:
    """
    Query descendant term IDs for a given term from the ontology hierarchy table.

    Returns all terms that have the given term in their ancestor lineage.

    Args:
        term_id: The term ID (e.g., "HP:0000001")
        ontology_source: The ontology source (default: "HPO")
        version: The ontology version. If None, uses the latest version.
        database_name: Optional override for database name
        table_name: Optional override for table name

    Returns:
        List of descendant term dictionaries with term_id, term_label, and depth.
        Ordered by depth (shallow to deep).
    """
    database_name = database_name or os.environ.get('ICEBERG_DATABASE')
    table_name = table_name or os.environ.get('ICEBERG_ONTOLOGY_HIERARCHY_TABLE')

    if not database_name or not table_name:
        raise ValueError("Database and table name must be provided or set in environment")

    # If no version specified, get the latest version
    if not version:
        version_query = f"""
        SELECT version
        FROM {database_name}.{table_name}
        WHERE LOWER(ontology_source) = LOWER('{ontology_source}')
        GROUP BY version
        ORDER BY version DESC
        LIMIT 1
        """

        try:
            result = _execute_athena_query(version_query, wait_for_completion=True)
            version_rows = _get_query_results(result)

            if not version_rows:
                logger.warning(f"No versions found for ontology source: {ontology_source}")
                return []

            version = version_rows[0]['version']
            logger.info(f"Using latest version {version} for {ontology_source}")

        except Exception as e:
            logger.error(f"Failed to get latest version for {ontology_source}: {e}")
            return []

    # Check DynamoDB cache first
    cached_descendants = get_term_descendants_from_cache(term_id, ontology_source, version)
    if cached_descendants is not None:
        logger.info(f"Cache hit: Found {len(cached_descendants)} descendants for {term_id} from DynamoDB")
        # Return in same format as Athena query (list of dicts with term_id)
        # Caller only uses term_id, so we don't need term_label or depth from cache
        return [{'term_id': tid} for tid in cached_descendants]

    # Cache miss - query Athena
    logger.info(f"Cache miss: Querying Athena for descendants of {term_id}")

    # Query descendants - all terms that have this term in their ancestor_term_ids
    # Use case-insensitive comparison for ontology_source (stored as lowercase: hpo, mondo)
    query = f"""
    SELECT term_id, term_label, depth
    FROM {database_name}.{table_name}
    WHERE LOWER(ontology_source) = LOWER('{ontology_source}')
      AND version = '{version}'
      AND CONTAINS(ancestor_term_ids, '{term_id}')
      AND term_id != '{term_id}'
    ORDER BY depth ASC
    """

    logger.info(f"Querying descendants: term_id={term_id}, ontology_source={ontology_source} (searching case-insensitive), version={version}, database={database_name}, table={table_name}")

    try:
        query_execution_id = _execute_athena_query(query, wait_for_completion=True)
        results = _get_query_results(query_execution_id)
        logger.info(f"Found {len(results)} descendants for {term_id}")

        if not results:
            logger.info(f"No descendants found for term {term_id} in {ontology_source} version {version}")
            # Cache empty result to avoid repeated queries
            put_term_descendants_to_cache(term_id, ontology_source, version, [])
            return []

        # Extract term IDs for caching
        term_ids = [d['term_id'] for d in results]

        # Write to cache for future requests
        put_term_descendants_to_cache(term_id, ontology_source, version, term_ids)
        logger.info(f"Cached {len(term_ids)} descendants for {term_id} in DynamoDB")

        return results

    except Exception as e:
        logger.error(f"Failed to query descendants for term {term_id}: {e}")
        return []


def _get_query_results(query_execution_id: str) -> List[Dict[str, Any]]:
    """
    Helper function to retrieve query results from Athena.

    Args:
        query_execution_id: The Athena query execution ID

    Returns:
        List of result rows as dictionaries
    """
    import boto3

    athena = boto3.client('athena')

    try:
        # Get query results
        paginator = athena.get_paginator('get_query_results')
        page_iterator = paginator.paginate(QueryExecutionId=query_execution_id)

        results = []
        for page in page_iterator:
            rows = page['ResultSet']['Rows']

            # Skip header row (first page only)
            if not results and rows:
                rows = rows[1:]

            # Extract column names from metadata
            columns = [col['Name'] for col in page['ResultSet']['ResultSetMetadata']['ColumnInfo']]

            # Parse rows
            for row in rows:
                data = row['Data']
                result_dict = {}
                for i, col_name in enumerate(columns):
                    if i < len(data):
                        result_dict[col_name] = data[i].get('VarCharValue')
                results.append(result_dict)

        return results

    except Exception as e:
        logger.error(f"Failed to get query results: {e}")
        raise


def reset_iceberg_tables() -> bool:
    """
    Delete all data from all Iceberg tables (evidence, subject_terms_by_subject, subject_terms_by_project_term).
    Used by reset_database to completely wipe Iceberg data.

    Returns:
        bool: True if all tables cleared successfully, False otherwise
    """
    database_name = os.environ.get('ICEBERG_DATABASE')
    if not database_name:
        raise ValueError("ICEBERG_DATABASE environment variable is required")

    evidence_table = os.environ.get('ICEBERG_EVIDENCE_TABLE')
    by_subject_table = os.environ.get('ICEBERG_SUBJECT_TERMS_BY_SUBJECT_TABLE')
    by_project_term_table = os.environ.get('ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE')

    if not all([evidence_table, by_subject_table, by_project_term_table]):
        raise ValueError("All Iceberg table environment variables must be set")

    tables = [
        (evidence_table, "evidence"),
        (by_subject_table, "subject_terms_by_subject"),
        (by_project_term_table, "subject_terms_by_project_term")
    ]

    athena_client = boto3.client('athena')

    # Check if primary workgroup is managed (cached to avoid rate limiting)
    managed, wg_cfg = _get_workgroup_config_cached(athena_client)

    bucket_name = os.environ.get('PHEBEE_BUCKET_NAME') if not managed else None

    for table_name, table_desc in tables:
        delete_query = f"DELETE FROM {database_name}.{table_name}"

        logger.info(f"Clearing Iceberg table: {table_desc}")

        try:
            params = {
                "QueryString": delete_query,
                "QueryExecutionContext": {"Database": database_name},
                "WorkGroup": "primary"
            }

            if not managed:
                params["ResultConfiguration"] = {"OutputLocation": f"s3://{bucket_name}/athena-results/"}

            response = athena_client.start_query_execution(**params)
            query_execution_id = response['QueryExecutionId']

            # Wait for query completion
            while True:
                result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = result['QueryExecution']['Status']['State']

                if status == 'SUCCEEDED':
                    logger.info(f"Successfully cleared Iceberg table: {table_desc}")
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    error_msg = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    logger.error(f"Athena DELETE failed for {table_desc}: {error_msg}")
                    return False

                time.sleep(1)

        except Exception as e:
            logger.error(f"Failed to clear Iceberg table {table_desc}: {e}")
            return False

    logger.info("Successfully cleared all Iceberg tables")
    return True
