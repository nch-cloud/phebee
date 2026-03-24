"""
Unit tests for bulk processor hash consistency.

Verifies that the bulk evidence processor's hash functions produce
identical results to the API's Qualifier-based hash functions.

This prevents drift between the two implementations and ensures
bulk-imported evidence has the same termlink_ids as API-created evidence.

Note: This test copies the hash functions from bulk_evidence_processor.py
to avoid the PySpark dependency. Keep these in sync manually!
"""

import pytest
import hashlib
from phebee.utils.hash import generate_termlink_hash, generate_evidence_hash
from phebee.utils.qualifier import Qualifier


# ============================================================================
# Bulk Processor Hash Functions (copied from scripts/bulk_evidence_processor.py)
# IMPORTANT: Keep these in sync with the actual bulk processor!
# ============================================================================

def bulk_normalize_qualifiers(qualifiers):
    """
    Normalize qualifiers for hash computation.
    Copied from bulk_evidence_processor.py - KEEP IN SYNC!
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


def bulk_generate_termlink_hash(source_node_iri: str, term_iri: str, qualifiers: list = None) -> str:
    """
    Generate a deterministic hash for a term link.
    Copied from bulk_evidence_processor.py - KEEP IN SYNC!
    """
    # Normalize qualifiers using centralized function
    normalized_qualifiers = bulk_normalize_qualifiers(qualifiers)

    # Join normalized qualifiers with commas
    qualifier_contexts = ",".join(normalized_qualifiers)

    # Create hash input: source_node_iri|term_iri|qualifier_contexts
    hash_input = f"{source_node_iri}|{term_iri}|{qualifier_contexts}"

    # Generate a deterministic hash
    return hashlib.sha256(hash_input.encode()).hexdigest()


def bulk_generate_evidence_hash(
    clinical_note_id: str,
    encounter_id: str,
    term_iri: str,
    span_start: int = None,
    span_end: int = None,
    qualifiers: list = None,
    subject_id: str = None,
    creator_id: str = None
) -> str:
    """
    Generate deterministic evidence ID from content.
    Copied from bulk_evidence_processor.py - KEEP IN SYNC!
    """
    # Normalize qualifiers using centralized function
    normalized_qualifiers = bulk_normalize_qualifiers(qualifiers)

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


class TestBulkHashConsistency:
    """Verify bulk processor hashes match API hashes."""

    def test_termlink_hash_no_qualifiers(self):
        """Test termlink hash with no qualifiers matches between bulk and API."""
        subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/test-subject-123"
        term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

        # Bulk processor (string-based)
        bulk_hash = bulk_generate_termlink_hash(subject_iri, term_iri, None)

        # API (Qualifier-based)
        api_hash = generate_termlink_hash(subject_iri, term_iri, None)

        assert bulk_hash == api_hash, \
            f"Hash mismatch with no qualifiers: bulk={bulk_hash}, api={api_hash}"

    def test_termlink_hash_internal_qualifiers(self):
        """Test termlink hash with internal qualifiers."""
        subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/test-subject-123"
        term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

        # Bulk processor uses strings
        bulk_qualifiers = ["negated:true", "family:false"]
        bulk_hash = bulk_generate_termlink_hash(subject_iri, term_iri, bulk_qualifiers)

        # API uses Qualifier objects
        api_qualifiers = [
            Qualifier(type="negated", value="true"),
            Qualifier(type="family", value="false")
        ]
        api_hash = generate_termlink_hash(subject_iri, term_iri, api_qualifiers)

        assert bulk_hash == api_hash, \
            f"Hash mismatch with internal qualifiers: bulk={bulk_hash}, api={api_hash}"

    def test_termlink_hash_with_qualifier_values(self):
        """Test termlink hash preserves qualifier values."""
        subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/test-subject-123"
        term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

        # Bulk processor
        bulk_qualifiers = ["onset:HP:0003593", "severity:HP:0012828"]
        bulk_hash = bulk_generate_termlink_hash(subject_iri, term_iri, bulk_qualifiers)

        # API
        api_qualifiers = [
            Qualifier(type="onset", value="HP:0003593"),
            Qualifier(type="severity", value="HP:0012828")
        ]
        api_hash = generate_termlink_hash(subject_iri, term_iri, api_qualifiers)

        assert bulk_hash == api_hash, \
            f"Hash mismatch with qualifier values: bulk={bulk_hash}, api={api_hash}"

    def test_termlink_hash_external_iri_qualifiers(self):
        """Test termlink hash with external IRI qualifiers."""
        subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/test-subject-123"
        term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

        # Bulk processor
        bulk_qualifiers = ["http://purl.obolibrary.org/obo/HP_0040283:present"]
        bulk_hash = bulk_generate_termlink_hash(subject_iri, term_iri, bulk_qualifiers)

        # API
        api_qualifiers = [
            Qualifier(type="http://purl.obolibrary.org/obo/HP_0040283", value="present")
        ]
        api_hash = generate_termlink_hash(subject_iri, term_iri, api_qualifiers)

        assert bulk_hash == api_hash, \
            f"Hash mismatch with external IRI: bulk={bulk_hash}, api={api_hash}"

    def test_termlink_hash_mixed_qualifiers(self):
        """Test termlink hash with mixed internal and external qualifiers."""
        subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/test-subject-123"
        term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

        # Bulk processor
        bulk_qualifiers = [
            "negated:true",
            "http://purl.obolibrary.org/obo/HP_0040283:present"
        ]
        bulk_hash = bulk_generate_termlink_hash(subject_iri, term_iri, bulk_qualifiers)

        # API
        api_qualifiers = [
            Qualifier(type="negated", value="true"),
            Qualifier(type="http://purl.obolibrary.org/obo/HP_0040283", value="present")
        ]
        api_hash = generate_termlink_hash(subject_iri, term_iri, api_qualifiers)

        assert bulk_hash == api_hash, \
            f"Hash mismatch with mixed qualifiers: bulk={bulk_hash}, api={api_hash}"

    def test_termlink_hash_qualifier_order_independence(self):
        """Test that qualifier order doesn't affect hash (both normalize and sort)."""
        subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/test-subject-123"
        term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

        # Bulk processor - order 1
        bulk_hash_1 = bulk_generate_termlink_hash(
            subject_iri, term_iri, ["negated:true", "family:true"]
        )

        # Bulk processor - order 2 (reversed)
        bulk_hash_2 = bulk_generate_termlink_hash(
            subject_iri, term_iri, ["family:true", "negated:true"]
        )

        # API - order 1
        api_hash_1 = generate_termlink_hash(
            subject_iri, term_iri,
            [Qualifier(type="negated", value="true"), Qualifier(type="family", value="true")]
        )

        # API - order 2 (reversed)
        api_hash_2 = generate_termlink_hash(
            subject_iri, term_iri,
            [Qualifier(type="family", value="true"), Qualifier(type="negated", value="true")]
        )

        # All should match
        assert bulk_hash_1 == bulk_hash_2 == api_hash_1 == api_hash_2, \
            f"Hash should be order-independent: bulk1={bulk_hash_1}, bulk2={bulk_hash_2}, " \
            f"api1={api_hash_1}, api2={api_hash_2}"

    def test_evidence_hash_consistency(self):
        """Test evidence hash matches between bulk and API."""
        clinical_note_id = "note-123"
        encounter_id = "encounter-456"
        term_iri = "http://purl.obolibrary.org/obo/HP_0001250"
        span_start = 100
        span_end = 200
        subject_id = "subject-789"
        creator_id = "creator-abc"

        # Bulk processor
        bulk_qualifiers = ["negated:true", "onset:HP:0003593"]
        bulk_hash = bulk_generate_evidence_hash(
            clinical_note_id, encounter_id, term_iri,
            span_start, span_end, bulk_qualifiers,
            subject_id, creator_id
        )

        # API
        api_qualifiers = [
            Qualifier(type="negated", value="true"),
            Qualifier(type="onset", value="HP:0003593")
        ]
        api_hash = generate_evidence_hash(
            clinical_note_id, encounter_id, term_iri,
            span_start, span_end, api_qualifiers,
            subject_id, creator_id
        )

        assert bulk_hash == api_hash, \
            f"Evidence hash mismatch: bulk={bulk_hash}, api={api_hash}"

    def test_normalize_qualifiers_consistency(self):
        """Test that normalization produces same results in bulk and API."""
        # Bulk processor (strings)
        bulk_input = ["negated", "family:false", "onset:HP:0003593"]
        bulk_normalized = bulk_normalize_qualifiers(bulk_input)

        # Expected output after normalization:
        # - "negated" -> "negated:true"
        # - "family:false" -> filtered out
        # - "onset:HP:0003593" -> kept as-is
        # Sorted: ["negated:true", "onset:HP:0003593"]

        assert bulk_normalized == ["negated:true", "onset:HP:0003593"], \
            f"Bulk normalization unexpected: {bulk_normalized}"

    def test_different_values_produce_different_hashes(self):
        """Test that different qualifier values produce different hashes (critical for bug fix)."""
        subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/test-subject-123"
        term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

        # Bulk processor - onset value 1
        bulk_hash_1 = bulk_generate_termlink_hash(
            subject_iri, term_iri, ["onset:HP:0003593"]
        )

        # Bulk processor - onset value 2 (different)
        bulk_hash_2 = bulk_generate_termlink_hash(
            subject_iri, term_iri, ["onset:HP:0011462"]
        )

        # API - onset value 1
        api_hash_1 = generate_termlink_hash(
            subject_iri, term_iri,
            [Qualifier(type="onset", value="HP:0003593")]
        )

        # API - onset value 2 (different)
        api_hash_2 = generate_termlink_hash(
            subject_iri, term_iri,
            [Qualifier(type="onset", value="HP:0011462")]
        )

        # Different values should produce different hashes
        assert bulk_hash_1 != bulk_hash_2, \
            "Bulk: Different qualifier values should produce different hashes"
        assert api_hash_1 != api_hash_2, \
            "API: Different qualifier values should produce different hashes"

        # But corresponding hashes should match
        assert bulk_hash_1 == api_hash_1, \
            f"Hash mismatch for HP:0003593: bulk={bulk_hash_1}, api={api_hash_1}"
        assert bulk_hash_2 == api_hash_2, \
            f"Hash mismatch for HP:0011462: bulk={bulk_hash_2}, api={api_hash_2}"
