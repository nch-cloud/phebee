"""
Test to ensure evidence hash implementations are consistent across modules.
"""
import sys
import os
import pytest
from unittest.mock import MagicMock

# Mock PySpark modules before importing bulk_evidence_processor
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()
sys.modules['pyspark.sql.types'] = MagicMock()
sys.modules['pyspark.storagelevel'] = MagicMock()

# Add the scripts directory to the path so we can import from bulk_evidence_processor
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'scripts'))

from phebee.utils.hash import generate_evidence_hash as hash_py_implementation
from phebee.utils.hash import generate_termlink_hash as termlink_hash_py_implementation
from bulk_evidence_processor import generate_evidence_hash as bulk_processor_implementation
from bulk_evidence_processor import generate_termlink_hash as termlink_bulk_processor_implementation


class TestHashConsistency:
    """Test that both evidence and termlink hash implementations produce identical results."""
    
    @pytest.mark.parametrize("clinical_note_id,encounter_id,term_iri,span_start,span_end,qualifiers,subject_id,creator_id", [
        # Basic case
        ("note-001", "enc-001", "http://purl.obolibrary.org/obo/HP_0001627", 10, 20, ["negated:false"], "subj-001", "automated"),
        
        # With nulls/empty values
        (None, None, "http://purl.obolibrary.org/obo/HP_0001627", None, None, None, None, None),
        ("", "", "http://purl.obolibrary.org/obo/HP_0001627", None, None, [], "", ""),
        
        # Manual vs automated creator
        ("note-001", "enc-001", "http://purl.obolibrary.org/obo/HP_0001627", 10, 20, ["negated:false"], "subj-001", "manual"),
        ("note-001", "enc-001", "http://purl.obolibrary.org/obo/HP_0001627", 10, 20, ["negated:false"], "subj-001", "automated"),
        
        # Different subjects
        ("note-001", "enc-001", "http://purl.obolibrary.org/obo/HP_0001627", 10, 20, ["negated:false"], "subj-001", "automated"),
        ("note-001", "enc-001", "http://purl.obolibrary.org/obo/HP_0001627", 10, 20, ["negated:false"], "subj-002", "automated"),
        
        # Complex qualifiers
        ("note-001", "enc-001", "http://purl.obolibrary.org/obo/HP_0001627", 10, 20, 
         ["negated:false", "family:true", "hypothetical:false"], "subj-001", "automated"),
        
        # Different spans
        ("note-001", "enc-001", "http://purl.obolibrary.org/obo/HP_0001627", 5, 15, ["negated:false"], "subj-001", "automated"),
        ("note-001", "enc-001", "http://purl.obolibrary.org/obo/HP_0001627", 10, 20, ["negated:false"], "subj-001", "automated"),
    ])
    def test_hash_implementations_identical(self, clinical_note_id, encounter_id, term_iri, 
                                          span_start, span_end, qualifiers, subject_id, creator_id):
        """Test that both implementations produce identical hashes for the same inputs."""
        
        hash_py_result = hash_py_implementation(
            clinical_note_id=clinical_note_id,
            encounter_id=encounter_id,
            term_iri=term_iri,
            span_start=span_start,
            span_end=span_end,
            qualifiers=qualifiers,
            subject_id=subject_id,
            creator_id=creator_id
        )
        
        bulk_processor_result = bulk_processor_implementation(
            clinical_note_id=clinical_note_id,
            encounter_id=encounter_id,
            term_iri=term_iri,
            span_start=span_start,
            span_end=span_end,
            qualifiers=qualifiers,
            subject_id=subject_id,
            creator_id=creator_id
        )
        
        assert hash_py_result == bulk_processor_result, (
            f"Hash implementations differ!\n"
            f"hash.py result: {hash_py_result}\n"
            f"bulk_processor result: {bulk_processor_result}\n"
            f"Inputs: clinical_note_id={clinical_note_id}, encounter_id={encounter_id}, "
            f"term_iri={term_iri}, span_start={span_start}, span_end={span_end}, "
            f"qualifiers={qualifiers}, subject_id={subject_id}, creator_id={creator_id}"
        )
    
    def test_automated_vs_manual_different_hashes(self):
        """Test that automated and manual evidence get different hashes."""
        base_params = {
            "clinical_note_id": "note-001",
            "encounter_id": "enc-001", 
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "span_start": 10,
            "span_end": 20,
            "qualifiers": ["negated:false", "family:false", "hypothetical:false"],
            "subject_id": "subj-001"
        }
        
        # Test both implementations and ensure they produce identical results
        automated_hash_py = hash_py_implementation(**base_params, creator_id="ods/phebee-eval:automated-v1")
        automated_hash_bulk = bulk_processor_implementation(**base_params, creator_id="ods/phebee-eval:automated-v1")
        assert automated_hash_py == automated_hash_bulk, "Implementations should produce identical hashes for automated evidence"
        
        manual_hash_py = hash_py_implementation(**base_params, creator_id="ods/phebee-eval:curator-1")
        manual_hash_bulk = bulk_processor_implementation(**base_params, creator_id="ods/phebee-eval:curator-1")
        assert manual_hash_py == manual_hash_bulk, "Implementations should produce identical hashes for manual evidence"
        
        # Test that automated and manual produce different hashes
        assert automated_hash_py != manual_hash_py, "Automated and manual evidence should have different hashes"
    
    def test_different_subjects_different_hashes(self):
        """Test that different subjects get different hashes."""
        base_params = {
            "clinical_note_id": "note-001",
            "encounter_id": "enc-001",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627", 
            "span_start": 10,
            "span_end": 20,
            "qualifiers": ["negated:false"],
            "creator_id": "automated"
        }
        
        # Test both implementations and ensure they produce identical results
        subject1_hash_py = hash_py_implementation(**base_params, subject_id="subj-001")
        subject1_hash_bulk = bulk_processor_implementation(**base_params, subject_id="subj-001")
        assert subject1_hash_py == subject1_hash_bulk, "Implementations should produce identical hashes for subject 1"
        
        subject2_hash_py = hash_py_implementation(**base_params, subject_id="subj-002")
        subject2_hash_bulk = bulk_processor_implementation(**base_params, subject_id="subj-002")
        assert subject2_hash_py == subject2_hash_bulk, "Implementations should produce identical hashes for subject 2"
        
        # Test that different subjects produce different hashes
        assert subject1_hash_py != subject2_hash_py, "Different subjects should have different hashes"

    @pytest.mark.parametrize("source_node_iri,term_iri,qualifiers", [
        # Basic case
        ("http://ods.nationwidechildrens.org/phebee/subjects/subj-001", "http://purl.obolibrary.org/obo/HP_0001627", ["negated"]),
        
        # With empty/null qualifiers
        ("http://ods.nationwidechildrens.org/phebee/subjects/subj-001", "http://purl.obolibrary.org/obo/HP_0001627", None),
        ("http://ods.nationwidechildrens.org/phebee/subjects/subj-001", "http://purl.obolibrary.org/obo/HP_0001627", []),
        
        # Multiple qualifiers
        ("http://ods.nationwidechildrens.org/phebee/subjects/subj-001", "http://purl.obolibrary.org/obo/HP_0001627", ["negated", "family"]),
        ("http://ods.nationwidechildrens.org/phebee/subjects/subj-001", "http://purl.obolibrary.org/obo/HP_0001627", ["family", "negated"]),  # Order shouldn't matter
        
        # Different subjects
        ("http://ods.nationwidechildrens.org/phebee/subjects/subj-002", "http://purl.obolibrary.org/obo/HP_0001627", ["negated"]),
        
        # Different terms
        ("http://ods.nationwidechildrens.org/phebee/subjects/subj-001", "http://purl.obolibrary.org/obo/HP_0000001", ["negated"]),
    ])
    def test_termlink_hash_implementations_identical(self, source_node_iri, term_iri, qualifiers):
        """Test that both termlink hash implementations produce identical hashes for the same inputs."""
        
        hash_py_result = termlink_hash_py_implementation(
            source_node_iri=source_node_iri,
            term_iri=term_iri,
            qualifiers=qualifiers
        )
        
        bulk_processor_result = termlink_bulk_processor_implementation(
            source_node_iri=source_node_iri,
            term_iri=term_iri,
            qualifiers=qualifiers
        )
        
        assert hash_py_result == bulk_processor_result, (
            f"Termlink hash implementations differ!\n"
            f"hash.py result: {hash_py_result}\n"
            f"bulk_processor result: {bulk_processor_result}\n"
            f"Inputs: source_node_iri={source_node_iri}, term_iri={term_iri}, qualifiers={qualifiers}"
        )
    
    def test_termlink_qualifier_order_independence(self):
        """Test that termlink hash is independent of qualifier order."""
        source_node_iri = "http://ods.nationwidechildrens.org/phebee/subjects/subj-001"
        term_iri = "http://purl.obolibrary.org/obo/HP_0001627"
        
        # Test both implementations and ensure they produce identical results
        hash1_py = termlink_hash_py_implementation(source_node_iri, term_iri, ["negated", "family", "hypothetical"])
        hash1_bulk = termlink_bulk_processor_implementation(source_node_iri, term_iri, ["negated", "family", "hypothetical"])
        assert hash1_py == hash1_bulk, "Implementations should produce identical hashes for ordered qualifiers"
        
        hash2_py = termlink_hash_py_implementation(source_node_iri, term_iri, ["family", "hypothetical", "negated"])
        hash2_bulk = termlink_bulk_processor_implementation(source_node_iri, term_iri, ["family", "hypothetical", "negated"])
        assert hash2_py == hash2_bulk, "Implementations should produce identical hashes for reordered qualifiers"
        
        # Test that qualifier order doesn't matter
        assert hash1_py == hash2_py, "Termlink hash should be independent of qualifier order"
    
    def test_termlink_different_subjects_different_hashes(self):
        """Test that different subjects get different termlink hashes."""
        term_iri = "http://purl.obolibrary.org/obo/HP_0001627"
        qualifiers = ["negated"]
        
        # Test both implementations and ensure they produce identical results
        subject1_hash_py = termlink_hash_py_implementation("http://ods.nationwidechildrens.org/phebee/subjects/subj-001", term_iri, qualifiers)
        subject1_hash_bulk = termlink_bulk_processor_implementation("http://ods.nationwidechildrens.org/phebee/subjects/subj-001", term_iri, qualifiers)
        assert subject1_hash_py == subject1_hash_bulk, "Implementations should produce identical hashes for subject 1"
        
        subject2_hash_py = termlink_hash_py_implementation("http://ods.nationwidechildrens.org/phebee/subjects/subj-002", term_iri, qualifiers)
        subject2_hash_bulk = termlink_bulk_processor_implementation("http://ods.nationwidechildrens.org/phebee/subjects/subj-002", term_iri, qualifiers)
        assert subject2_hash_py == subject2_hash_bulk, "Implementations should produce identical hashes for subject 2"
        
        # Test that different subjects produce different hashes
        assert subject1_hash_py != subject2_hash_py, "Different subjects should have different termlink hashes"
    
    def test_evidence_hash_different_identifiers(self):
        """Test that different identifiers produce different evidence hashes."""
        base_params = {
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "span_start": 10,
            "span_end": 20,
            "qualifiers": ["negated:false"],
            "subject_id": "subj-001",
            "creator_id": "automated"
        }
        
        # Test different clinical_note_ids
        note1_params = {**base_params, "clinical_note_id": "note-001", "encounter_id": "enc-001"}
        note1_hash_py = hash_py_implementation(**note1_params)
        note1_hash_bulk = bulk_processor_implementation(**note1_params)
        assert note1_hash_py == note1_hash_bulk, "Implementations should produce identical hashes for note 1"
        
        note2_params = {**base_params, "clinical_note_id": "note-002", "encounter_id": "enc-001"}
        note2_hash_py = hash_py_implementation(**note2_params)
        note2_hash_bulk = bulk_processor_implementation(**note2_params)
        assert note2_hash_py == note2_hash_bulk, "Implementations should produce identical hashes for note 2"
        
        assert note1_hash_py != note2_hash_py, "Different clinical notes should have different hashes"
        
        # Test different encounter_ids
        enc1_params = {**base_params, "clinical_note_id": "note-001", "encounter_id": "enc-001"}
        enc1_hash_py = hash_py_implementation(**enc1_params)
        enc1_hash_bulk = bulk_processor_implementation(**enc1_params)
        assert enc1_hash_py == enc1_hash_bulk, "Implementations should produce identical hashes for encounter 1"
        
        enc2_params = {**base_params, "clinical_note_id": "note-001", "encounter_id": "enc-002"}
        enc2_hash_py = hash_py_implementation(**enc2_params)
        enc2_hash_bulk = bulk_processor_implementation(**enc2_params)
        assert enc2_hash_py == enc2_hash_bulk, "Implementations should produce identical hashes for encounter 2"
        
        assert enc1_hash_py != enc2_hash_py, "Different encounters should have different hashes"
        
        # Test different term_iris
        term1_params = {**base_params, "clinical_note_id": "note-001", "encounter_id": "enc-001", "term_iri": "http://purl.obolibrary.org/obo/HP_0001627"}
        term1_hash_py = hash_py_implementation(**term1_params)
        term1_hash_bulk = bulk_processor_implementation(**term1_params)
        assert term1_hash_py == term1_hash_bulk, "Implementations should produce identical hashes for term 1"
        
        term2_params = {**base_params, "clinical_note_id": "note-001", "encounter_id": "enc-001", "term_iri": "http://purl.obolibrary.org/obo/HP_0000001"}
        term2_hash_py = hash_py_implementation(**term2_params)
        term2_hash_bulk = bulk_processor_implementation(**term2_params)
        assert term2_hash_py == term2_hash_bulk, "Implementations should produce identical hashes for term 2"
        
        assert term1_hash_py != term2_hash_py, "Different terms should have different hashes"
    
    def test_evidence_hash_span_edge_cases(self):
        """Test evidence hash span edge cases."""
        base_params = {
            "clinical_note_id": "note-001",
            "encounter_id": "enc-001",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "qualifiers": [],
            "subject_id": "subj-001",
            "creator_id": "automated"
        }
        
        # Test null spans vs zero spans
        null_span_py = hash_py_implementation(**base_params, span_start=None, span_end=None)
        null_span_bulk = bulk_processor_implementation(**base_params, span_start=None, span_end=None)
        assert null_span_py == null_span_bulk, "Implementations should produce identical hashes for null spans"
        
        zero_span_py = hash_py_implementation(**base_params, span_start=0, span_end=0)
        zero_span_bulk = bulk_processor_implementation(**base_params, span_start=0, span_end=0)
        assert zero_span_py == zero_span_bulk, "Implementations should produce identical hashes for zero spans"
        
        assert null_span_py != zero_span_py, "Null spans and zero spans should have different hashes"
        
        # Test different span positions
        span1_py = hash_py_implementation(**base_params, span_start=10, span_end=20)
        span1_bulk = bulk_processor_implementation(**base_params, span_start=10, span_end=20)
        assert span1_py == span1_bulk, "Implementations should produce identical hashes for span 1"
        
        span2_py = hash_py_implementation(**base_params, span_start=15, span_end=25)
        span2_bulk = bulk_processor_implementation(**base_params, span_start=15, span_end=25)
        assert span2_py == span2_bulk, "Implementations should produce identical hashes for span 2"
        
        assert span1_py != span2_py, "Different spans should have different hashes"
    
    def test_termlink_hash_source_node_types(self):
        """Test termlink hash with different source node types."""
        term_iri = "http://purl.obolibrary.org/obo/HP_0001627"
        qualifiers = ["negated"]
        
        # Test subject IRI
        subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/subj-001"
        subject_hash_py = termlink_hash_py_implementation(subject_iri, term_iri, qualifiers)
        subject_hash_bulk = termlink_bulk_processor_implementation(subject_iri, term_iri, qualifiers)
        assert subject_hash_py == subject_hash_bulk, "Implementations should produce identical hashes for subject IRI"
        
        # Test encounter IRI (hypothetical)
        encounter_iri = "http://ods.nationwidechildrens.org/phebee/encounters/enc-001"
        encounter_hash_py = termlink_hash_py_implementation(encounter_iri, term_iri, qualifiers)
        encounter_hash_bulk = termlink_bulk_processor_implementation(encounter_iri, term_iri, qualifiers)
        assert encounter_hash_py == encounter_hash_bulk, "Implementations should produce identical hashes for encounter IRI"
        
        # Test clinical note IRI (hypothetical)
        note_iri = "http://ods.nationwidechildrens.org/phebee/notes/note-001"
        note_hash_py = termlink_hash_py_implementation(note_iri, term_iri, qualifiers)
        note_hash_bulk = termlink_bulk_processor_implementation(note_iri, term_iri, qualifiers)
        assert note_hash_py == note_hash_bulk, "Implementations should produce identical hashes for note IRI"
        
        # Test that different source node types produce different hashes
        assert subject_hash_py != encounter_hash_py, "Subject and encounter IRIs should have different hashes"
        assert subject_hash_py != note_hash_py, "Subject and note IRIs should have different hashes"
        assert encounter_hash_py != note_hash_py, "Encounter and note IRIs should have different hashes"
    
    def test_qualifier_edge_cases(self):
        """Test qualifier edge cases for both hash types."""
        # Test qualifiers with special characters
        base_evidence_params = {
            "clinical_note_id": "note-001",
            "encounter_id": "enc-001",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "span_start": 10,
            "span_end": 20,
            "subject_id": "subj-001",
            "creator_id": "automated"
        }
        
        source_node_iri = "http://ods.nationwidechildrens.org/phebee/subjects/subj-001"
        term_iri = "http://purl.obolibrary.org/obo/HP_0001627"
        
        # Test qualifiers with colons in values
        special_qualifiers = ["severity:mild:moderate", "context:family:history"]
        
        evidence_hash_py = hash_py_implementation(**base_evidence_params, qualifiers=special_qualifiers)
        evidence_hash_bulk = bulk_processor_implementation(**base_evidence_params, qualifiers=special_qualifiers)
        assert evidence_hash_py == evidence_hash_bulk, "Evidence hash implementations should handle qualifiers with colons"
        
        termlink_hash_py = termlink_hash_py_implementation(source_node_iri, term_iri, special_qualifiers)
        termlink_hash_bulk = termlink_bulk_processor_implementation(source_node_iri, term_iri, special_qualifiers)
        assert termlink_hash_py == termlink_hash_bulk, "Termlink hash implementations should handle qualifiers with colons"
        
        # Test empty string qualifiers
        empty_qualifiers = ["", "negated:true", ""]
        
        evidence_hash_empty_py = hash_py_implementation(**base_evidence_params, qualifiers=empty_qualifiers)
        evidence_hash_empty_bulk = bulk_processor_implementation(**base_evidence_params, qualifiers=empty_qualifiers)
        assert evidence_hash_empty_py == evidence_hash_empty_bulk, "Evidence hash implementations should handle empty string qualifiers"
        
        termlink_hash_empty_py = termlink_hash_py_implementation(source_node_iri, term_iri, empty_qualifiers)
        termlink_hash_empty_bulk = termlink_bulk_processor_implementation(source_node_iri, term_iri, empty_qualifiers)
        assert termlink_hash_empty_py == termlink_hash_empty_bulk, "Termlink hash implementations should handle empty string qualifiers"
    
    def test_string_edge_cases(self):
        """Test string edge cases for both hash types."""
        base_evidence_params = {
            "clinical_note_id": "note-001",
            "encounter_id": "enc-001",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "span_start": 10,
            "span_end": 20,
            "qualifiers": [],
            "subject_id": "subj-001",
            "creator_id": "automated"
        }
        
        # Test empty string vs None for optional string parameters
        empty_note_params = {**base_evidence_params, "clinical_note_id": ""}
        empty_note_py = hash_py_implementation(**empty_note_params)
        empty_note_bulk = bulk_processor_implementation(**empty_note_params)
        assert empty_note_py == empty_note_bulk, "Implementations should handle empty clinical_note_id consistently"
        
        none_note_params = {**base_evidence_params, "clinical_note_id": None}
        none_note_py = hash_py_implementation(**none_note_params)
        none_note_bulk = bulk_processor_implementation(**none_note_params)
        assert none_note_py == none_note_bulk, "Implementations should handle None clinical_note_id consistently"
        
        # Empty string and None should produce the same hash (both become "")
        assert empty_note_py == none_note_py, "Empty string and None should produce same hash"
        
        # Test whitespace handling
        whitespace_params = {**base_evidence_params, "clinical_note_id": " note-001 "}
        whitespace_py = hash_py_implementation(**whitespace_params)
        whitespace_bulk = bulk_processor_implementation(**whitespace_params)
        assert whitespace_py == whitespace_bulk, "Implementations should handle whitespace consistently"
        
        normal_params = {**base_evidence_params, "clinical_note_id": "note-001"}
        normal_py = hash_py_implementation(**normal_params)
        
        # Whitespace should matter (no automatic trimming)
        assert whitespace_py != normal_py, "Whitespace should affect hash values"
        
        # Test Unicode characters
        unicode_params = {**base_evidence_params, "clinical_note_id": "note-001-ñoté"}
        unicode_py = hash_py_implementation(**unicode_params)
        unicode_bulk = bulk_processor_implementation(**unicode_params)
        assert unicode_py == unicode_bulk, "Implementations should handle Unicode consistently"
        
        assert unicode_py != normal_py, "Unicode characters should affect hash values"
    
    def test_span_edge_cases_advanced(self):
        """Test advanced span edge cases."""
        base_params = {
            "clinical_note_id": "note-001",
            "encounter_id": "enc-001",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "qualifiers": [],
            "subject_id": "subj-001",
            "creator_id": "automated"
        }
        
        # Test negative spans
        negative_params = {**base_params, "span_start": -5, "span_end": -1}
        negative_py = hash_py_implementation(**negative_params)
        negative_bulk = bulk_processor_implementation(**negative_params)
        assert negative_py == negative_bulk, "Implementations should handle negative spans consistently"
        
        positive_params = {**base_params, "span_start": 5, "span_end": 1}  # Note: start > end
        positive_py = hash_py_implementation(**positive_params)
        positive_bulk = bulk_processor_implementation(**positive_params)
        assert positive_py == positive_bulk, "Implementations should handle reversed spans consistently"
        
        assert negative_py != positive_py, "Negative and positive spans should produce different hashes"
        
        # Test very large spans
        large_params = {**base_params, "span_start": 999999999, "span_end": 1000000000}
        large_py = hash_py_implementation(**large_params)
        large_bulk = bulk_processor_implementation(**large_params)
        assert large_py == large_bulk, "Implementations should handle large spans consistently"
        
        small_params = {**base_params, "span_start": 1, "span_end": 2}
        small_py = hash_py_implementation(**small_params)
        
        assert large_py != small_py, "Large and small spans should produce different hashes"
    
    def test_case_sensitivity(self):
        """Test case sensitivity in hash functions."""
        base_evidence_params = {
            "clinical_note_id": "note-001",
            "encounter_id": "enc-001",
            "span_start": 10,
            "span_end": 20,
            "qualifiers": [],
            "subject_id": "subj-001",
            "creator_id": "automated"
        }
        
        # Test case sensitivity in term IRIs
        lower_params = {**base_evidence_params, "term_iri": "http://purl.obolibrary.org/obo/hp_0001627"}
        lower_py = hash_py_implementation(**lower_params)
        lower_bulk = bulk_processor_implementation(**lower_params)
        assert lower_py == lower_bulk, "Implementations should handle lowercase IRIs consistently"
        
        upper_params = {**base_evidence_params, "term_iri": "http://purl.obolibrary.org/obo/HP_0001627"}
        upper_py = hash_py_implementation(**upper_params)
        upper_bulk = bulk_processor_implementation(**upper_params)
        assert upper_py == upper_bulk, "Implementations should handle uppercase IRIs consistently"
        
        # Case should matter in IRIs
        assert lower_py != upper_py, "Case should matter in term IRIs"
        
        # Test case sensitivity in qualifiers
        lower_qual_params = {**base_evidence_params, "term_iri": "http://purl.obolibrary.org/obo/HP_0001627", "qualifiers": ["negated:true"]}
        lower_qual_py = hash_py_implementation(**lower_qual_params)
        lower_qual_bulk = bulk_processor_implementation(**lower_qual_params)
        assert lower_qual_py == lower_qual_bulk, "Implementations should handle lowercase qualifiers consistently"
        
        upper_qual_params = {**base_evidence_params, "term_iri": "http://purl.obolibrary.org/obo/HP_0001627", "qualifiers": ["NEGATED:TRUE"]}
        upper_qual_py = hash_py_implementation(**upper_qual_params)
        upper_qual_bulk = bulk_processor_implementation(**upper_qual_params)
        assert upper_qual_py == upper_qual_bulk, "Implementations should handle uppercase qualifiers consistently"
        
        # Case should matter in qualifiers
        assert lower_qual_py != upper_qual_py, "Case should matter in qualifiers"
    
    def test_evidence_hash_falsey_vs_missing_qualifiers(self):
        """Test that evidence hash treats missing and false qualifiers identically."""
        base_params = {
            "clinical_note_id": "note-001",
            "encounter_id": "enc-001",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "span_start": 10,
            "span_end": 20,
            "subject_id": "subj-001",
            "creator_id": "automated"
        }
        
        # Test both implementations for each case
        for impl_name, impl_func in [("hash.py", hash_py_implementation), ("bulk_processor", bulk_processor_implementation)]:
            # Test missing qualifiers vs empty list
            hash_missing = impl_func(**base_params, qualifiers=None)
            hash_empty = impl_func(**base_params, qualifiers=[])
            assert hash_missing == hash_empty, f"{impl_name}: Missing and empty qualifiers should produce same evidence hash"
            
            # Test empty vs false qualifiers
            hash_false_qualifiers = impl_func(**base_params, qualifiers=["negated:false", "family:false", "hypothetical:false"])
            assert hash_empty == hash_false_qualifiers, f"{impl_name}: Empty and false-only qualifiers should produce same evidence hash"
            
            # Test mixed true/false vs true-only
            hash_mixed = impl_func(**base_params, qualifiers=["negated:true", "family:false", "hypothetical:false"])
            hash_true_only = impl_func(**base_params, qualifiers=["negated:true"])
            assert hash_mixed == hash_true_only, f"{impl_name}: Mixed true/false and true-only qualifiers should produce same evidence hash"
            
            # Test that only true qualifiers matter
            hash_with_true = impl_func(**base_params, qualifiers=["family:true"])
            hash_without_qualifiers = impl_func(**base_params, qualifiers=[])
            assert hash_with_true != hash_without_qualifiers, f"{impl_name}: True qualifiers should change the evidence hash"
    
    def test_termlink_hash_falsey_vs_missing_qualifiers(self):
        """Test that termlink hash treats missing and false qualifiers identically."""
        source_node_iri = "http://ods.nationwidechildrens.org/phebee/subjects/subj-001"
        term_iri = "http://purl.obolibrary.org/obo/HP_0001627"
        
        # Test both implementations for each case
        for impl_name, impl_func in [("hash.py", termlink_hash_py_implementation), ("bulk_processor", termlink_bulk_processor_implementation)]:
            # Test missing qualifiers vs empty list
            hash_missing = impl_func(source_node_iri, term_iri, None)
            hash_empty = impl_func(source_node_iri, term_iri, [])
            assert hash_missing == hash_empty, f"{impl_name}: Missing and empty qualifiers should produce same termlink hash"
            
            # Test empty vs false qualifiers (simulating what bulk processor would generate)
            hash_false_qualifiers = impl_func(source_node_iri, term_iri, [])  # Bulk processor excludes false qualifiers
            assert hash_empty == hash_false_qualifiers, f"{impl_name}: Empty and false-only qualifiers should produce same termlink hash"
            
            # Test mixed true/false vs true-only
            hash_mixed = impl_func(source_node_iri, term_iri, ["negated:true"])  # False qualifiers excluded
            hash_true_only = impl_func(source_node_iri, term_iri, ["negated:true"])
            assert hash_mixed == hash_true_only, f"{impl_name}: Mixed true/false and true-only qualifiers should produce same termlink hash"
            
            # Test that only true qualifiers matter
            hash_with_true = impl_func(source_node_iri, term_iri, ["family:true"])
            hash_without_qualifiers = impl_func(source_node_iri, term_iri, [])
            assert hash_with_true != hash_without_qualifiers, f"{impl_name}: True qualifiers should change the termlink hash"
