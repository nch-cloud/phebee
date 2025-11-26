"""
Iceberg evidence table utilities for PheBee.
"""
import boto3
import hashlib
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

PHEBEE_NS = Namespace(PHEBEE)
OBO = Namespace("http://purl.obolibrary.org/obo/")

def generate_evidence_id(
    subject_id: str,
    term_iri: str,
    creator_name: str,
    clinical_note_id: str = None,
    span_start: int = None,
    span_end: int = None,
    qualifiers: List[str] = None
) -> str:
    """Generate deterministic evidence ID from content."""
    content_parts = [
        subject_id,
        term_iri,
        creator_name,
        clinical_note_id or "",
        str(span_start) if span_start is not None else "",
        str(span_end) if span_end is not None else "",
        "|".join(sorted(qualifiers or []))
    ]
    content = "|".join(content_parts)
    return hashlib.sha256(content.encode()).hexdigest()

def extract_evidence_records(
    entries: List[Any],  # TermLinkInput objects
    subject_map: Dict[Tuple[str, str], str],
    run_id: str,
    batch_id: str = None
) -> List[Dict[str, Any]]:
    """
    Extract evidence records from TermLinkInput entries for Iceberg storage.
    
    Args:
        entries: List of TermLinkInput objects
        subject_map: Mapping from (project_id, project_subject_id) to subject_iri
        run_id: Bulk upload run identifier
        batch_id: Optional batch identifier within the run
        
    Returns:
        List of evidence records ready for Iceberg insertion
    """
    evidence_records = []
    
    for entry in entries:
        key = (entry.project_id, entry.project_subject_id)
        subject_iri = subject_map.get(key)
        if not subject_iri:
            raise ValueError(f"Subject resolution missing for {key}")
        
        # Extract subject_id from IRI (e.g., "http://example.com/subjects/123" -> "123")
        subject_id = subject_iri.split("/")[-1]
        
        for evidence in entry.evidence:
            # Build qualifiers list
            qualifiers = []
            if evidence.contexts:
                for qualifier, value in evidence.contexts.items():
                    if value == 1:  # Qualifier is present
                        qualifiers.append(qualifier)
            
            # Generate deterministic evidence ID
            evidence_id = generate_evidence_id(
                subject_id=subject_id,
                term_iri=entry.term_iri,
                creator_name=evidence.evidence_creator_name,
                clinical_note_id=evidence.clinical_note_id if evidence.type == "clinical_note" else None,
                span_start=evidence.span_start,
                span_end=evidence.span_end,
                qualifiers=qualifiers
            )
            
            # Determine source level and IDs
            if evidence.type == "clinical_note":
                source_level = "clinical_note"
                encounter_id = evidence.encounter_id
                clinical_note_id = evidence.clinical_note_id
            else:
                source_level = "subject"
                encounter_id = None
                clinical_note_id = None
            
            # Build evidence record
            record = {
                "evidence_id": evidence_id,
                "run_id": run_id,
                "batch_id": batch_id,
                "evidence_type": f"{evidence.evidence_creator_type}_clinical_note" if evidence.type == "clinical_note" else "manual_annotation",
                "assertion_type": f"{evidence.evidence_creator_type}_assertion",
                "created_timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                "created_date": datetime.utcnow().date().isoformat(),
                "source_level": source_level,
                "subject_id": subject_id,
                "encounter_id": encounter_id,
                "clinical_note_id": clinical_note_id,
                "termlink_id": None,  # Will be set after TTL derivation
                "term_iri": entry.term_iri,
                "note_context": {
                    "note_timestamp": evidence.note_timestamp,
                    "note_type": evidence.note_type,
                    "provider_type": evidence.provider_type,
                    "author_specialty": evidence.author_specialty
                } if evidence.type == "clinical_note" else None,
                "creator": {
                    "name": evidence.evidence_creator_name,
                    "type": evidence.evidence_creator_type
                },
                "text_annotation": {
                    "span_start": evidence.span_start,
                    "span_end": evidence.span_end
                } if evidence.span_start is not None or evidence.span_end is not None else None,
                "qualifiers": qualifiers if qualifiers else None,
                "metadata": {}
            }
            
            evidence_records.append(record)
    
    return evidence_records

def derive_neptune_ttl_from_evidence(evidence_records: List[Dict[str, Any]]) -> Tuple[str, Set[str]]:
    """
    Derive simplified Neptune TTL from evidence records.
    
    Args:
        evidence_records: List of evidence records from Iceberg
        
    Returns:
        Tuple of (TTL string, set of created termlink IRIs)
    """
    g = Graph()
    g.bind("phebee", PHEBEE_NS)
    g.bind("obo", OBO)
    g.bind("dcterms", DCTERMS)
    g.bind("xsd", XSD)
    
    # Group evidence by (subject_id, term_iri, qualifiers) to create TermLinks
    termlink_groups = {}
    created_termlinks = set()
    
    for record in evidence_records:
        subject_id = record["subject_id"]
        term_iri = record["term_iri"]
        qualifiers = tuple(sorted(record.get("qualifiers", [])))
        
        key = (subject_id, term_iri, qualifiers)
        if key not in termlink_groups:
            termlink_groups[key] = {
                "subject_id": subject_id,
                "term_iri": term_iri,
                "qualifiers": list(qualifiers),
                "earliest_timestamp": None,
                "evidence_count": 0
            }
        
        # Track earliest timestamp and evidence count
        group = termlink_groups[key]
        group["evidence_count"] += 1
        
        if record.get("note_context", {}).get("note_timestamp"):
            timestamp = record["note_context"]["note_timestamp"]
            if group["earliest_timestamp"] is None or timestamp < group["earliest_timestamp"]:
                group["earliest_timestamp"] = timestamp
    
    # Generate RDF for each TermLink
    for (subject_id, term_iri, qualifiers), group in termlink_groups.items():
        # Create IRIs
        subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
        
        # Generate termlink hash (simplified version of existing logic)
        qualifier_str = "|".join(sorted(qualifiers)) if qualifiers else ""
        termlink_content = f"{subject_iri}|{term_iri}|{qualifier_str}"
        termlink_hash = hashlib.sha256(termlink_content.encode()).hexdigest()[:16]
        termlink_iri = f"{subject_iri}/term-link/{termlink_hash}"
        
        subject_ref = URIRef(subject_iri)
        term_ref = URIRef(term_iri)
        termlink_ref = URIRef(termlink_iri)
        
        # Subject node
        g.add((subject_ref, RDF.type, PHEBEE_NS.Subject))
        
        # TermLink node
        g.add((termlink_ref, RDF.type, PHEBEE_NS.TermLink))
        g.add((termlink_ref, PHEBEE_NS.sourceNode, subject_ref))
        g.add((termlink_ref, PHEBEE_NS.hasTerm, term_ref))
        
        # Add timestamp if available
        if group["earliest_timestamp"]:
            g.add((termlink_ref, DCTERMS.created, RdfLiteral(group["earliest_timestamp"], datatype=XSD.dateTime)))
        
        # Add qualifiers
        for qualifier in qualifiers:
            qualifier_iri = f"http://ods.nationwidechildrens.org/phebee/qualifiers/{qualifier}"
            g.add((termlink_ref, PHEBEE_NS.hasQualifyingTerm, URIRef(qualifier_iri)))
        
        # Convenience backlink
        g.add((subject_ref, PHEBEE_NS.hasTermLink, termlink_ref))
        
        created_termlinks.add(termlink_iri)
    
    # Serialize to TTL
    ttl = g.serialize(format="turtle", prefixes={
        "phebee": PHEBEE_NS, 
        "obo": OBO, 
        "dcterms": DCTERMS, 
        "xsd": XSD
    })
    
    return ttl, created_termlinks

def write_evidence_to_iceberg(
    evidence_records: List[Dict[str, Any]],
    database: str,
    table: str
) -> None:
    """
    Write evidence records to Iceberg table using MERGE for idempotency.
    
    Args:
        evidence_records: List of evidence records to insert
        database: Iceberg database name
        table: Iceberg table name
    """
    
    if not evidence_records:
        logger.info("No evidence records to write")
        return
    
    # For now, we'll implement a simplified approach
    # TODO: Implement proper MERGE for idempotency using Athena
    
    logger.info(f"Would write {len(evidence_records)} evidence records to {database}.{table}")
    logger.info(f"Sample record: {evidence_records[0] if evidence_records else 'None'}")
    
    # TODO: Implement actual Iceberg write using Athena INSERT/MERGE
    # This is a placeholder for the initial implementation


def query_iceberg_evidence(query: str) -> List[Dict[str, Any]]:
    """
    Execute a query against the Iceberg evidence table using Athena.
    
    Args:
        query: SQL query to execute
        
    Returns:
        List of result rows as dictionaries
    """
    
    athena_client = boto3.client('athena')
    
    # Check if primary workgroup is managed
    wg_cfg = athena_client.get_work_group(WorkGroup="primary")["WorkGroup"]["Configuration"]
    managed_config = wg_cfg.get("ManagedQueryResultsConfiguration", {})
    managed = managed_config.get("Enabled", False) if isinstance(managed_config, dict) else False

    params = {
        "QueryString": query,
        "QueryExecutionContext": {"Database": "phebee"},
        "WorkGroup": "primary"
    }

    if not managed:
        bucket_name = os.environ.get('PHEBEE_BUCKET_NAME')
        params["ResultConfiguration"] = {"OutputLocation": f"s3://{bucket_name}/athena-results/"}

    try:
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


def get_evidence_for_phenopackets(
    subject_id: str,
    encounter_id: str | None = None,
    note_id: str | None = None
) -> List[Dict[str, Any]]:
    """
    Get evidence data from Iceberg formatted for phenopacket export.
    
    Args:
        subject_id: The subject UUID
        encounter_id: Optional encounter filter
        note_id: Optional clinical note filter
        
    Returns:
        List of evidence records formatted for phenopacket conversion
    """
    # Build filter conditions
    filters = [f"subject_id = '{subject_id}'"]
    
    if encounter_id:
        filters.append(f"encounter_id = '{encounter_id}'")
    if note_id:
        filters.append(f"clinical_note_id = '{note_id}'")
    
    where_clause = " AND ".join(filters)
    
    # Query for evidence with all needed phenopacket fields
    database_name = os.environ.get('ICEBERG_DATABASE', 'phebee')
    table_name = os.environ.get('ICEBERG_EVIDENCE_TABLE', 'evidence')
    
    query = f"""
    SELECT 
        subject_id,
        term_iri,
        evidence_type,
        assertion_type,
        created_timestamp,
        creator.name as creator,
        qualifiers,
        note_context.note_type as source,
        text_annotation.span_start,
        text_annotation.span_end
    FROM {database_name}.{table_name}
    WHERE {where_clause}
    ORDER BY term_iri, created_timestamp
    """
    
    try:
        results = query_iceberg_evidence(query)
        
        # Convert to phenopacket format
        phenopacket_data = []
        
        for row in results:
            # Parse qualifiers to determine if excluded
            excluded = False
            try:
                qualifiers_list = json.loads(row.get('qualifiers', '[]')) if row.get('qualifiers') else []
                # Check for negated qualifier
                for q in qualifiers_list:
                    if q.get('qualifier_type') == 'negated' and q.get('qualifier_value') == '1':
                        excluded = True
                        break
            except:
                pass
            
            # Format for phenopacket conversion
            record = {
                "projectSubjectId": f"phebee:subject#{row['subject_id']}",
                "term": row['term_iri'],
                "termLabel": None,  # TODO: Add term label lookup
                "excluded": "true" if excluded else "false",
                "evidence_type": row.get('evidence_type'),
                "assertion_method": row.get('assertion_type'),
                "created": row.get('created_timestamp'),
                "creator": row.get('creator'),
                "source": row.get('source')
            }
            
            phenopacket_data.append(record)
        
        return phenopacket_data
        
    except Exception as e:
        logger.error(f"Error getting evidence for phenopackets: {e}")
        return []


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
    metadata: Dict[str, Any] = None,
    note_timestamp: str = None,
    provider_type: str = None,
    author_specialty: str = None,
    note_type: str = None
) -> str:
    """
    Create a single evidence record in Iceberg.
    
    Returns:
        str: The generated evidence_id
    """
    # Get environment variables before any local imports
    database_name = os.environ.get('ICEBERG_DATABASE', 'phebee')
    table_name = os.environ.get('ICEBERG_EVIDENCE_TABLE', 'evidence')
    
    # Generate evidence ID
    evidence_id = generate_evidence_id(
        subject_id=subject_id,
        term_iri=term_iri,
        creator_name=creator_id,  # Use creator_id for uniqueness
        clinical_note_id=clinical_note_id,
        span_start=span_start,
        span_end=span_end,
        qualifiers=qualifiers or []
    )
    
    # Generate termlink ID (simplified version)
    termlink_content = f"{subject_id}|{term_iri}|{'|'.join(sorted(qualifiers or []))}"
    termlink_id = hashlib.sha256(termlink_content.encode()).hexdigest()[:16]
    
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
            "span_end": span_end
        } if span_start is not None or span_end is not None else None,
        "qualifiers": [
            {"qualifier_type": q, "qualifier_value": "1"} for q in (qualifiers or [])
        ] if qualifiers else None,
        "metadata": metadata or {}
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
        text_annotation_value = f"ROW({ta.get('span_start', 'NULL')}, {ta.get('span_end', 'NULL')}, '{ta.get('text_span', '')}', {ta.get('confidence_score', 'NULL')})"
    
    qualifiers_value = "NULL"
    if record.get('qualifiers'):
        qual_rows = [f"ROW('{q['qualifier_type']}', '{q['qualifier_value']}')" for q in record['qualifiers']]
        qualifiers_value = f"ARRAY[{', '.join(qual_rows)}]"
    
    metadata_value = "MAP()"
    if record.get('metadata') and record['metadata']:
        keys = list(record['metadata'].keys())
        values = list(record['metadata'].values())
        if keys:
            keys_quoted = [f"'{k}'" for k in keys]
            values_quoted = [f"'{v}'" for v in values]
            keys_array = f"ARRAY[{', '.join(keys_quoted)}]"
            values_array = f"ARRAY[{', '.join(values_quoted)}]"
            metadata_value = f"MAP({keys_array}, {values_array})"

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
        {metadata_value}
    )
    """
    
    try:
        # Execute INSERT using Athena
        
        athena_client = boto3.client('athena')
        
        # Check if primary workgroup is managed
        wg_cfg = athena_client.get_work_group(WorkGroup="primary")["WorkGroup"]["Configuration"]
        managed_config = wg_cfg.get("ManagedQueryResultsConfiguration", {})
        managed = managed_config.get("Enabled", False) if isinstance(managed_config, dict) else False

        params = {
            "QueryString": insert_query,
            "QueryExecutionContext": {"Database": "phebee"},
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
    database_name = os.environ.get('ICEBERG_DATABASE', 'phebee')
    table_name = os.environ.get('ICEBERG_EVIDENCE_TABLE', 'evidence')
    
    query = f"""
    SELECT 
        evidence_id,
        run_id,
        batch_id,
        evidence_type,
        subject_id,
        term_iri,
        creator.creator_id as creator_id,
        creator.creator_type as creator_type,
        text_annotation.span_start as span_start,
        text_annotation.span_end as span_end,
        qualifiers,
        metadata,
        created_timestamp
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
        
        # Add qualifiers if present
        if row.get('qualifiers'):
            # Parse qualifiers array - they come as string representation
            qualifiers_str = row['qualifiers']
            if qualifiers_str and qualifiers_str != '[]':
                # Parse format like [{qualifier_type=negated, qualifier_value=1}]
                qualifiers = []
                # Extract qualifier_type values from the struct format
                matches = re.findall(r'qualifier_type=([^,}]+)', qualifiers_str)
                for match in matches:
                    qualifiers.append(match.strip())
                record["qualifiers"] = qualifiers
        
        # Add metadata if present
        if row.get('metadata'):
            # Parse metadata map - they come as string representation
            metadata_str = row['metadata']
            if metadata_str and metadata_str != '{}':
                try:
                    # Try to parse as JSON first
                    record["metadata"] = json.loads(metadata_str)
                except:
                    # If that fails, leave as string
                    record["metadata"] = metadata_str
        
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
    database_name = os.environ.get('ICEBERG_DATABASE', 'phebee')
    table_name = os.environ.get('ICEBERG_EVIDENCE_TABLE', 'evidence')
    
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

        params = {
            "QueryString": delete_query,
            "QueryExecutionContext": {"Database": "phebee"},
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
    # Build qualifier filter
    qualifier_filter = ""
    if qualifiers:
        # Convert qualifiers to JSON array format for matching
        qualifier_conditions = []
        for qualifier in qualifiers:
            qualifier_conditions.append(f"JSON_EXTRACT_SCALAR(qualifiers, '$[*].qualifier_type') = '{qualifier}'")
        if qualifier_conditions:
            qualifier_filter = f"AND ({' OR '.join(qualifier_conditions)})"
    
    database_name = os.environ.get('ICEBERG_DATABASE', 'phebee')
    table_name = os.environ.get('ICEBERG_EVIDENCE_TABLE', 'evidence')
    
    query = f"""
    SELECT 
        evidence_id,
        evidence_type,
        assertion_type,
        created_timestamp,
        creator.creator_id as creator,
        text_annotation.span_start,
        text_annotation.span_end,
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
                "creator": row.get('creator')
            }
            
            # Add span info if present
            if row.get('span_start') or row.get('span_end'):
                record["span_start"] = int(row['span_start']) if row.get('span_start') else None
                record["span_end"] = int(row['span_end']) if row.get('span_end') else None
            
            evidence.append(record)
        
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
    evidence_data = get_evidence_for_termlink(subject_id, term_iri, qualifiers)
    
    if not evidence_data:
        return None
    
    # Generate term links from evidence
    term_links = []
    for evidence in evidence_data:
        # Create deterministic term link IRI
        source_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
        if evidence.get('encounter_id'):
            source_iri += f"/encounters/{evidence['encounter_id']}"
        if evidence.get('clinical_note_id'):
            source_iri += f"/clinical-notes/{evidence['clinical_note_id']}"
            
        termlink_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}/term-link/{evidence['evidence_id']}"
        
        term_links.append({
            "termlink_iri": termlink_iri,
            "evidence_count": 1,
            "source_iri": source_iri,
            "source_type": evidence.get('evidence_type', 'clinical_note')
        })
    
    return {
        "term_iri": term_iri,
        "qualifiers": sorted(qualifiers or []),
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
    
    database_name = os.environ.get('ICEBERG_DATABASE', 'phebee')
    table_name = os.environ.get('ICEBERG_EVIDENCE_TABLE', 'evidence')
    
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
                    qualifiers_list = json.loads(row['qualifiers'])
                    record["qualifiers"] = [
                        q['qualifier_type'] for q in qualifiers_list 
                        if q.get('qualifier_value') == '1'
                    ]
                except:
                    record["qualifiers"] = []
            
            evidence.append(record)
        
        return evidence
        
    except Exception as e:
        logger.error(f"Error getting evidence for subject-term: {e}")
        return []
