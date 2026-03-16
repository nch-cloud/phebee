"""
Shared hashing utilities for PheBee.

This module provides centralized hashing functions to ensure consistency
across different components of the system.
"""

import hashlib
from typing import List, Optional


def normalize_qualifiers(qualifiers: Optional[List[str]]) -> List[str]:
    """
    Normalize qualifiers to consistent name:value format for hash computation.

    Supports hybrid qualifier approach:
    - Internal qualifiers (negated, hypothetical, family): Use short form "name:value"
    - External qualifiers (full IRIs): Preserve full IRI format "http://.../:value"

    Args:
        qualifiers: List of qualifiers in various formats:
            - "name:value" (already normalized)
            - "name" (short name - converts to "name:true")
            - "http://.../:value" (external IRI with value)
            - "http://..." (external IRI - converts to "http://...:true")

    Returns:
        Normalized, sorted list with false values filtered out.

    Examples:
        >>> normalize_qualifiers(["negated"])
        ['negated:true']

        >>> normalize_qualifiers(["negated:true", "family:false"])
        ['negated:true']

        >>> normalize_qualifiers(["http://purl.obolibrary.org/obo/HP_0040283:present", "negated"])
        ['http://purl.obolibrary.org/obo/HP_0040283:present', 'negated:true']
    """
    if not qualifiers:
        return []

    # Define internal qualifier names
    INTERNAL_QUALIFIERS = {'negated', 'hypothetical', 'family'}

    normalized = []
    for qualifier in qualifiers:
        if not qualifier:  # Skip empty strings
            continue

        # Check if it's an external IRI first
        is_external = qualifier.startswith('http://') or qualifier.startswith('https://')

        if is_external:
            # For external IRIs, check if there's a value after the last slash
            # e.g., "http://.../HP_0040283:present" has value, "http://.../HP_0040283" doesn't
            last_slash_idx = qualifier.rfind('/')
            colon_after_slash = qualifier.find(':', last_slash_idx + 1)

            if colon_after_slash != -1:
                # Has a value component - filter false values
                value = qualifier[colon_after_slash + 1:]
                if value.lower() not in ["false", "0"]:
                    normalized.append(qualifier)
            else:
                # No value - add ":true"
                normalized.append(f"{qualifier}:true")
        else:
            # Internal qualifier
            if ":" in qualifier:
                # Already has a value component - filter false values
                name, value = qualifier.split(":", 1)
                if value.lower() not in ["false", "0"]:
                    normalized.append(qualifier)
            else:
                # No value - add ":true"
                normalized.append(f"{qualifier}:true")

    # Sort for deterministic ordering
    return sorted(normalized)


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
    # Normalize qualifiers using centralized function
    normalized_qualifiers = normalize_qualifiers(qualifiers)

    # Join normalized qualifiers with commas (matches existing format)
    qualifier_contexts = ",".join(normalized_qualifiers)

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
    qualifiers: Optional[List[str]] = None,
    subject_id: Optional[str] = None,
    creator_id: Optional[str] = None
) -> str:
    """
    Generate deterministic evidence ID from content.

    Args:
        clinical_note_id: Clinical note identifier
        encounter_id: Encounter identifier
        term_iri: Term IRI
        span_start: Text span start position
        span_end: Text span end position
        qualifiers: List of qualifier name:value pairs (e.g., ["negated:true", "family:false"])
        subject_id: Subject identifier for uniqueness across subjects
        creator_id: Creator identifier to distinguish automated vs manual evidence

    Returns:
        str: Deterministic evidence hash
    """
    # Normalize qualifiers using centralized function
    normalized_qualifiers = normalize_qualifiers(qualifiers)

    content_parts = [
        clinical_note_id or "",
        encounter_id or "",
        term_iri,
        str(span_start) if span_start is not None else "",
        str(span_end) if span_end is not None else "",
        "|".join(normalized_qualifiers),
        subject_id or "",
        creator_id or ""
    ]
    content = "|".join(content_parts)
    return hashlib.sha256(content.encode()).hexdigest()
