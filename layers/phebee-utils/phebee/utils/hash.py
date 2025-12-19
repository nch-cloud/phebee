"""
Shared hashing utilities for PheBee.

This module provides centralized hashing functions to ensure consistency
across different components of the system.
"""

import hashlib
from typing import List, Optional


def generate_termlink_hash(source_node_iri: str, term_iri: str, qualifiers: Optional[List[str]] = None) -> str:
    """
    Generate a deterministic hash for a term link based on its components.
    
    Args:
        source_node_iri (str): The IRI of the source node (subject, encounter, or clinical note)
        term_iri (str): The IRI of the term being linked
        qualifiers (list): List of qualifier name:value pairs (e.g., ["negated:true", "severity:mild"])
        
    Returns:
        str: A deterministic hash that can be used as part of the term link IRI
    """
    # Sort qualifiers to ensure consistent ordering
    sorted_qualifiers = sorted(qualifiers) if qualifiers else []
    
    # Join positive qualifiers with commas (matches existing format)
    qualifier_contexts = ",".join(sorted_qualifiers)
    
    # Create hash input: source_node_iri|term_iri|qualifier_contexts
    hash_input = f"{source_node_iri}|{term_iri}|{qualifier_contexts}"
    
    # Generate a deterministic hash
    return hashlib.sha256(hash_input.encode()).hexdigest()


def generate_evidence_hash(
    clinical_note_id: str,
    encounter_id: str,
    term_iri: str,
    span_start: Optional[int] = None,
    span_end: Optional[int] = None,
    qualifiers: Optional[List[str]] = None
) -> str:
    """
    Generate deterministic evidence ID from content.
    
    Args:
        clinical_note_id: Clinical note identifier
        encounter_id: Encounter identifier  
        term_iri: Term IRI
        span_start: Text span start position
        span_end: Text span end position
        qualifiers: List of qualifier strings
        
    Returns:
        str: Deterministic evidence hash
    """
    content_parts = [
        clinical_note_id,
        encounter_id,
        term_iri,
        str(span_start) if span_start is not None else "",
        str(span_end) if span_end is not None else "",
        "|".join(sorted(qualifiers or []))
    ]
    content = "|".join(content_parts)
    return hashlib.sha256(content.encode()).hexdigest()
