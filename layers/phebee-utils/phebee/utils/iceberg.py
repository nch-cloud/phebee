"""
Iceberg evidence table utilities for PheBee.
"""
import boto3
import hashlib
from .hash import generate_evidence_hash, generate_termlink_hash
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime
from typing import List, Dict, Any, Tuple, Set
from rdflib import Graph, URIRef, Literal as RdfLiteral, Namespace
from rdflib.namespace import RDF, DCTERMS, XSD

from phebee.constants import PHEBEE

logger = logging.getLogger(__name__)


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
        
        # Check if primary workgroup is managed
        wg_cfg = athena_client.get_work_group(WorkGroup="primary")["WorkGroup"]["Configuration"]
        managed_config = wg_cfg.get("ManagedQueryResultsConfiguration", {})
        managed = managed_config.get("Enabled", False) if isinstance(managed_config, dict) else False

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
        while True:
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                break
            elif status in ['FAILED', 'CANCELLED']:
                error_msg = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                raise Exception(f"Athena query failed: {error_msg}")
            
            time.sleep(2)
        
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
            {"qualifier_type": q, "qualifier_value": "true"} for q in (qualifiers or [])
        ] if qualifiers else None
    }
    
    # Insert into Iceberg table using Athena
    # Build structured values for complex types
    note_context_value = "NULL"
    if record.get('note_context'):
        nc = record['note_context']
        timestamp_part = f"TIMESTAMP '{nc['note_date']}'" if nc.get('note_date') else 'NULL'
        note_context_value = f"ROW('{nc.get('note_id', '')}', '{nc.get('note_type', '')}', {timestamp_part}, '{nc.get('encounter_id', '')}')"
    
    creator_value = f"ROW('{record['creator']['creator_id']}', '{record['creator']['creator_type']}', '{record['creator'].get('creator_name', '')}')"
    
    text_annotation_value = "NULL"
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
    
    qualifiers_value = "NULL"
    if record.get('qualifiers'):
        qual_rows = [f"ROW('{q['qualifier_type']}', '{q['qualifier_value']}')" for q in record['qualifiers']]
        qualifiers_value = f"ARRAY[{', '.join(qual_rows)}]"
    
    # Build term_source value
    term_source_value = "NULL"
    if term_source:
        source = term_source.get('source', '')
        version = term_source.get('version', '')
        iri = term_source.get('iri', '')
        term_source_value = f"ROW('{source}', '{version}', '{iri}')"
    
    insert_query = f"""
    INSERT INTO {database_name}.{table_name} VALUES (
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
        {note_context_value},
        {creator_value},
        {text_annotation_value},
        {qualifiers_value},
        {term_source_value}
    )
    """
    
    try:
        # Execute INSERT using Athena
        
        athena_client = boto3.client('athena')
        
        # Check if primary workgroup is managed
        wg_cfg = athena_client.get_work_group(WorkGroup="primary")["WorkGroup"]["Configuration"]
        managed_config = wg_cfg.get("ManagedQueryResultsConfiguration", {})
        managed = managed_config.get("Enabled", False) if isinstance(managed_config, dict) else False

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
                raise Exception(f"Athena INSERT failed: {error_msg}")
            
            time.sleep(1)
        
        logger.info(f"Created evidence record: {evidence_id}")
    except Exception as e:
        logger.error(f"Failed to insert evidence record {evidence_id}: {e}")
        raise
    
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
        
        # Check if primary workgroup is managed
        wg_cfg = athena_client.get_work_group(WorkGroup="primary")["WorkGroup"]["Configuration"]
        managed_config = wg_cfg.get("ManagedQueryResultsConfiguration", {})
        managed = managed_config.get("Enabled", False) if isinstance(managed_config, dict) else False

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
        qualifiers: List of qualifier IRIs
        
    Returns:
        List of evidence records
    """
    logger.info(f"Getting evidence for subject_id={subject_id}, term_iri={term_iri}, qualifiers={qualifiers}")
    
    # Build qualifier filter - simplified to not filter by qualifiers for now
    # This allows us to get all evidence for the termlink regardless of qualifiers
    qualifier_filter = ""
    
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
      {qualifier_filter}
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
        qualifiers: List of qualifier IRIs (optional)
    
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
        return None
    
    # Create deterministic termlink ID using only true qualifiers
    # Extract qualifier values from evidence data if available
    qualifiers_dict = {}
    if evidence_data and evidence_data[0].get('qualifiers'):
        qualifiers_from_evidence = evidence_data[0]['qualifiers']
        # Convert from array format to dictionary format
        if isinstance(qualifiers_from_evidence, list):
            # Convert array of {qualifier_type, qualifier_value} to dict
            for qualifier in qualifiers_from_evidence:
                if isinstance(qualifier, dict) and 'qualifier_type' in qualifier and 'qualifier_value' in qualifier:
                    # Convert values to boolean - handle both string and numeric formats
                    value = qualifier['qualifier_value']
                    if isinstance(value, str):
                        if value.lower() == 'true':
                            qualifiers_dict[qualifier['qualifier_type']] = True
                        elif value.lower() == 'false':
                            qualifiers_dict[qualifier['qualifier_type']] = False
                    elif isinstance(value, (int, float)):
                        # Handle numeric values: 1.0 = True, 0.0 = False
                        qualifiers_dict[qualifier['qualifier_type']] = bool(value)
        elif isinstance(qualifiers_from_evidence, dict):
            qualifiers_dict = qualifiers_from_evidence
        else:
            logger.warning(f"Expected qualifiers to be list or dict, got {type(qualifiers_from_evidence)}: {qualifiers_from_evidence}")
    
    # Build qualifier list with name:value format for non-falsey qualifiers
    def normalize_qualifier_value(value):
        if value in ["false", "0", 0, 0.0, False]:
            return None  # Exclude falsey values
        elif value in ["true", "1", 1, 1.0, True]:
            return "true"
        else:
            return str(value)  # Keep other values as strings
    
    qualifier_list = []
    for qualifier_type, qualifier_value in qualifiers_dict.items():
        normalized_val = normalize_qualifier_value(qualifier_value)
        if normalized_val:
            qualifier_list.append(f"{qualifier_type}:{normalized_val}")
    
    # Sort for deterministic ordering
    qualifier_list.sort()
    
    # Use shared hash function for consistency
    subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
    termlink_hash = generate_termlink_hash(subject_iri, term_iri, qualifier_list)
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
                    if qualifier_type.startswith('http://'):
                        qualifier_type = qualifier_type.split('/')[-1]
                    active_qualifiers.append(qualifier_type)
    
    return {
        "term_iri": term_iri,
        "qualifiers": sorted(active_qualifiers),  # Return short names of active qualifiers
        "evidence_count": len(evidence_data),
        "term_links": term_links
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
