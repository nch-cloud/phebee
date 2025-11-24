"""
Iceberg evidence table utilities for PheBee.
"""
import hashlib
import logging
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
    creator_id: str,
    clinical_note_id: str = None,
    span_start: int = None,
    span_end: int = None,
    qualifiers: List[str] = None
) -> str:
    """Generate deterministic evidence ID from content."""
    content_parts = [
        subject_id,
        term_iri,
        creator_id,
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
                creator_id=evidence.evidence_creator_id,
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
                "created_timestamp": datetime.utcnow().isoformat(),
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
                    "creator_id": evidence.evidence_creator_id,
                    "creator_type": evidence.evidence_creator_type,
                    "creator_name": evidence.evidence_creator_name,
                    "creator_version": evidence.evidence_creator_version
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
    import boto3
    
    if not evidence_records:
        logger.info("No evidence records to write")
        return
    
    # For now, we'll implement a simplified approach
    # TODO: Implement proper MERGE for idempotency using Athena
    
    logger.info(f"Would write {len(evidence_records)} evidence records to {database}.{table}")
    logger.info(f"Sample record: {evidence_records[0] if evidence_records else 'None'}")
    
    # TODO: Implement actual Iceberg write using Athena INSERT/MERGE
    # This is a placeholder for the initial implementation
