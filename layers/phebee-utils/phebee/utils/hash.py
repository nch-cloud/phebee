"""
Shared hashing utilities for PheBee.

This module provides centralized hashing functions to ensure consistency
across different components of the system.
"""

import hashlib
from typing import List, Optional, Union

from phebee.utils.qualifier import (
    Qualifier,
    normalize_qualifiers as normalize_qualifier_objects,
    qualifiers_to_string_list
)


def generate_termlink_hash(
    source_node_iri: str,
    term_iri: str,
    qualifiers: Optional[Union[List[str], List[Qualifier]]] = None
) -> str:
    """
    Generate a deterministic hash for a term link based on its components.

    Args:
        source_node_iri: The IRI of the source node (subject, encounter, or clinical note)
        term_iri: The IRI of the term being linked
        qualifiers: List of qualifiers (Qualifier objects or legacy string format)
                   e.g., [Qualifier(type="negated", value="true")] or ["negated:true"]

    Returns:
        A deterministic hash that can be used as part of the term link IRI
    """
    # Convert to Qualifier objects if needed (backward compatibility)
    if qualifiers and len(qualifiers) > 0:
        if isinstance(qualifiers[0], str):
            # Legacy string format - convert to Qualifier objects, filtering empty strings
            qualifier_objects = [Qualifier.from_string(q) for q in qualifiers if q]
        else:
            qualifier_objects = qualifiers
    else:
        qualifier_objects = []

    # Normalize qualifiers using centralized function
    normalized = normalize_qualifier_objects(qualifier_objects)

    # Convert to string format for hash computation (backward compatibility)
    qualifier_strings = qualifiers_to_string_list(normalized)

    # Join normalized qualifiers with commas (matches existing format)
    qualifier_contexts = ",".join(qualifier_strings)

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
    qualifiers: Optional[Union[List[str], List[Qualifier]]] = None,
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
        qualifiers: List of qualifiers (Qualifier objects or legacy string format)
                   e.g., [Qualifier(type="negated", value="true")] or ["negated:true"]
        subject_id: Subject identifier for uniqueness across subjects
        creator_id: Creator identifier to distinguish automated vs manual evidence

    Returns:
        Deterministic evidence hash
    """
    # Convert to Qualifier objects if needed (backward compatibility)
    if qualifiers and len(qualifiers) > 0:
        if isinstance(qualifiers[0], str):
            # Legacy string format - convert to Qualifier objects, filtering empty strings
            qualifier_objects = [Qualifier.from_string(q) for q in qualifiers if q]
        else:
            qualifier_objects = qualifiers
    else:
        qualifier_objects = []

    # Normalize qualifiers using centralized function
    normalized = normalize_qualifier_objects(qualifier_objects)

    # Convert to string format for hash computation (backward compatibility)
    qualifier_strings = qualifiers_to_string_list(normalized)

    content_parts = [
        clinical_note_id or "",
        encounter_id or "",
        term_iri,
        str(span_start) if span_start is not None else "",
        str(span_end) if span_end is not None else "",
        "|".join(qualifier_strings),
        subject_id or "",
        creator_id or ""
    ]
    content = "|".join(content_parts)
    return hashlib.sha256(content.encode()).hexdigest()
