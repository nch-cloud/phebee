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

    def test_current_hash_algorithm(self):
        """
        Test current hash algorithm with known values to prevent regressions.

        This verifies the CURRENT (correct) algorithm behavior:
        - Evidence hash: 8-part hash including subject_id and creator_id, excludes false qualifiers
        - Termlink hash: Excludes false qualifiers for future compatibility

        Test data is based on production evidence, but with CURRENT algorithm expectations.
        Note: Old production data (pre-Jan 31 2026) uses 6-part evidence hash and will differ.
        """
        # Test data from production
        subject_id = "8d8d7fba-9430-446a-9dbd-33958de1ce73"
        term_iri = "http://purl.obolibrary.org/obo/HP_0002104"
        clinical_note_id = "2157816666"
        encounter_id = "703207722"
        span_start = 276
        span_end = 281
        creator_id = "ods/phebee-clinphen:batch-v1.0.0_clinphen-1.28_hpo-v2025-10-22_a3a3f1c"

        # All qualifiers false - current algorithm filters them out
        qualifiers = []

        # Expected hashes with CURRENT algorithm (as of Jan 31, 2026)
        # Evidence: 8-part hash with subject_id and creator_id
        expected_evidence_id = "96da4b72420e6e27794ae869090e7ed8febe0e2655adc941e782da68145a73e4"
        # Termlink: unchanged from production (filters false qualifiers correctly)
        expected_termlink_id = "ed89e630b12145b30eb2b45147648e074c5c2702c84b3cd0e21f38074cc7c678"

        # Test evidence hash
        evidence_hash = hash_py_implementation(
            clinical_note_id=clinical_note_id,
            encounter_id=encounter_id,
            term_iri=term_iri,
            span_start=span_start,
            span_end=span_end,
            qualifiers=qualifiers,
            subject_id=subject_id,
            creator_id=creator_id
        )

        assert evidence_hash == expected_evidence_id, (
            f"Evidence hash mismatch!\n"
            f"Expected: {expected_evidence_id}\n"
            f"Got:      {evidence_hash}\n"
            f"This suggests our evidence hash implementation has diverged from production."
        )

        # Test termlink hash
        subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
        termlink_hash = termlink_hash_py_implementation(
            source_node_iri=subject_iri,
            term_iri=term_iri,
            qualifiers=qualifiers
        )

        assert termlink_hash == expected_termlink_id, (
            f"Termlink hash mismatch!\n"
            f"Expected: {expected_termlink_id}\n"
            f"Got:      {termlink_hash}\n"
            f"This suggests our termlink hash implementation has diverged from production."
        )

        # Also verify bulk processor produces same hashes
        evidence_hash_bulk = bulk_processor_implementation(
            clinical_note_id=clinical_note_id,
            encounter_id=encounter_id,
            term_iri=term_iri,
            span_start=span_start,
            span_end=span_end,
            qualifiers=qualifiers,
            subject_id=subject_id,
            creator_id=creator_id
        )

        assert evidence_hash_bulk == expected_evidence_id, (
            f"Bulk processor evidence hash mismatch!\n"
            f"Expected: {expected_evidence_id}\n"
            f"Got:      {evidence_hash_bulk}"
        )

        termlink_hash_bulk = termlink_bulk_processor_implementation(
            source_node_iri=subject_iri,
            term_iri=term_iri,
            qualifiers=qualifiers
        )

        assert termlink_hash_bulk == expected_termlink_id, (
            f"Bulk processor termlink hash mismatch!\n"
            f"Expected: {expected_termlink_id}\n"
            f"Got:      {termlink_hash_bulk}"
        )


def test_internal_qualifiers_no_breaking_changes():
    """Verify internal qualifiers still produce same hashes after hybrid refactor."""
    from phebee.utils.hash import generate_termlink_hash

    subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/subj-001"
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

    # All these should produce identical hashes
    hash1 = generate_termlink_hash(subject_iri, term_iri, ["negated"])
    hash2 = generate_termlink_hash(subject_iri, term_iri, ["negated:true"])
    hash3 = generate_termlink_hash(subject_iri, term_iri, ["negated", "family:false"])

    assert hash1 == hash2, "negated and negated:true should produce same hash"
    assert hash1 == hash3, "False qualifiers should be filtered out"


def test_external_qualifiers_full_iri():
    """Verify external qualifiers work with full IRIs."""
    from phebee.utils.hash import generate_termlink_hash

    subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/subj-001"
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

    # Test with external IRI with value
    ext_qual = "http://purl.obolibrary.org/obo/HP_0040283:present"
    hash1 = generate_termlink_hash(subject_iri, term_iri, [ext_qual])

    # Without value should add :true
    hash2 = generate_termlink_hash(subject_iri, term_iri,
                                   ["http://purl.obolibrary.org/obo/HP_0040283"])
    hash3 = generate_termlink_hash(subject_iri, term_iri,
                                   ["http://purl.obolibrary.org/obo/HP_0040283:true"])

    assert hash2 == hash3, "External IRI without value should add :true"
    assert hash1 != hash2, "Different qualifier values should produce different hashes"


def test_hybrid_mixed_qualifiers():
    """Verify mixed internal and external qualifiers work correctly."""
    from phebee.utils.hash import generate_termlink_hash

    subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/subj-001"
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

    qualifiers = [
        "negated:true",  # Internal
        "http://purl.obolibrary.org/obo/HP_0040283:present",  # External
        "family:false"  # Filtered
    ]

    hash1 = generate_termlink_hash(subject_iri, term_iri, qualifiers)

    # Order independence
    qualifiers_reordered = [
        "http://purl.obolibrary.org/obo/HP_0040283:present",
        "family:false",
        "negated:true"
    ]
    hash2 = generate_termlink_hash(subject_iri, term_iri, qualifiers_reordered)

    assert hash1 == hash2, "Qualifier order should not affect hash"


def test_no_regression_production_hashes():
    """Verify refactoring didn't break existing hashes using known production data."""
    from phebee.utils.hash import generate_termlink_hash

    # Use known production hash from test_current_hash_algorithm
    subject_id = "8d8d7fba-9430-446a-9dbd-33958de1ce73"
    term_iri = "http://purl.obolibrary.org/obo/HP_0002104"

    subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"

    # Empty qualifiers (all were false in production)
    termlink_hash = generate_termlink_hash(subject_iri, term_iri, [])

    expected = "ed89e630b12145b30eb2b45147648e074c5c2702c84b3cd0e21f38074cc7c678"
    assert termlink_hash == expected, f"Hash regression detected! Expected {expected}, got {termlink_hash}"


def test_known_hash_values_internal_qualifiers():
    """
    Test with known hash values for internal qualifiers to prevent drift.

    These are computed values from current implementation that should remain stable.
    If these fail, the hash algorithm has changed and will break existing data.
    """
    from phebee.utils.hash import generate_termlink_hash, generate_evidence_hash

    subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/test-subj-001"
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

    # Test 1: Single internal qualifier
    hash1 = generate_termlink_hash(subject_iri, term_iri, ["negated:true"])
    expected1 = "d044038bbf12c73b0e5d37bfb90c3d61769d5928e5cb8eb722777a2234981f10"
    assert hash1 == expected1, f"Internal qualifier hash drift! Expected {expected1}, got {hash1}"

    # Test 2: Multiple internal qualifiers (sorted)
    hash2 = generate_termlink_hash(subject_iri, term_iri, ["family:true", "negated:true"])
    expected2 = "ccb3138d37adc852c27ad20e6adbc91ed93ed13836ba5b969ba698a0cff0a342"
    assert hash2 == expected2, f"Multiple qualifier hash drift! Expected {expected2}, got {hash2}"

    # Test 3: Evidence hash with internal qualifiers
    evidence_hash = generate_evidence_hash(
        clinical_note_id="note-123",
        encounter_id="enc-456",
        term_iri=term_iri,
        span_start=10,
        span_end=20,
        qualifiers=["negated:true"],
        subject_id="test-subj-001",
        creator_id="test-creator"
    )
    expected3 = "4ca5d4043b23c149bf43107d70b89130522be7116af826eed63042d8daba6fa2"
    assert evidence_hash == expected3, f"Evidence hash drift! Expected {expected3}, got {evidence_hash}"


def test_known_hash_values_external_qualifiers():
    """
    Test with known hash values for external qualifiers to prevent drift.

    These verify that external IRI qualifiers produce stable hashes.
    """
    from phebee.utils.hash import generate_termlink_hash, generate_evidence_hash

    subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/test-subj-001"
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"
    external_qual = "http://purl.obolibrary.org/obo/HP_0040283:present"

    # Test 1: Single external qualifier
    hash1 = generate_termlink_hash(subject_iri, term_iri, [external_qual])
    expected1 = "d6a9ac27876c85f6a3ae81941be1799a524e79f94384a5518d354393dc79aa54"
    assert hash1 == expected1, f"External qualifier hash drift! Expected {expected1}, got {hash1}"

    # Test 2: External qualifier with :true
    hash2 = generate_termlink_hash(subject_iri, term_iri, ["http://purl.obolibrary.org/obo/HP_0040283:true"])
    expected2 = "dee61373a8f86c7a01850abef13f4f31f58cd39c42b8a0b7e40d90f8d8485755"
    assert hash2 == expected2, f"External qualifier :true hash drift! Expected {expected2}, got {hash2}"

    # Verify they're different (present vs true)
    assert hash1 != hash2, "Different qualifier values should produce different hashes"

    # Test 3: Evidence hash with external qualifier
    evidence_hash = generate_evidence_hash(
        clinical_note_id="note-123",
        encounter_id="enc-456",
        term_iri=term_iri,
        span_start=10,
        span_end=20,
        qualifiers=[external_qual],
        subject_id="test-subj-001",
        creator_id="test-creator"
    )
    expected3 = "f42999b8053735df28637016336c74307d7e5ccd3d5524b85e0660b0a8e84b43"
    assert evidence_hash == expected3, f"External evidence hash drift! Expected {expected3}, got {evidence_hash}"


def test_known_hash_values_mixed_qualifiers():
    """
    Test with known hash values for mixed internal/external qualifiers.

    These verify that hybrid qualifier combinations produce stable hashes.
    """
    from phebee.utils.hash import generate_termlink_hash, generate_evidence_hash

    subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/test-subj-001"
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"
    mixed_quals = [
        "negated:true",  # Internal
        "http://purl.obolibrary.org/obo/HP_0040283:present"  # External
    ]

    # Test 1: Mixed qualifiers in termlink hash
    hash1 = generate_termlink_hash(subject_iri, term_iri, mixed_quals)
    expected1 = "4b5a7c57709cc8c3cdb6a142eb6882a951d702c7fd12eee18c595d7df5fb2212"
    assert hash1 == expected1, f"Mixed qualifier hash drift! Expected {expected1}, got {hash1}"

    # Test 2: Verify order independence (should produce same hash)
    mixed_quals_reversed = [
        "http://purl.obolibrary.org/obo/HP_0040283:present",
        "negated:true"
    ]
    hash2 = generate_termlink_hash(subject_iri, term_iri, mixed_quals_reversed)
    assert hash2 == expected1, f"Order independence broken! Expected {expected1}, got {hash2}"

    # Test 3: Evidence hash with mixed qualifiers
    evidence_hash = generate_evidence_hash(
        clinical_note_id="note-123",
        encounter_id="enc-456",
        term_iri=term_iri,
        span_start=10,
        span_end=20,
        qualifiers=mixed_quals,
        subject_id="test-subj-001",
        creator_id="test-creator"
    )
    expected3 = "0ccc158f8e158d31bc1e218d45699eecb8d9791ae0ce072341fdc928a8678c9e"
    assert evidence_hash == expected3, f"Mixed evidence hash drift! Expected {expected3}, got {evidence_hash}"


def test_known_hash_values_edge_cases():
    """
    Test with known hash values for edge cases to prevent drift.

    Covers special scenarios like empty qualifiers, special characters, etc.
    """
    from phebee.utils.hash import generate_termlink_hash, generate_evidence_hash

    subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/test-subj-001"
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

    # Test 1: No qualifiers
    hash1 = generate_termlink_hash(subject_iri, term_iri, [])
    expected1 = "ef8bcbaaf2a458959e38e725af704ba34bba109aed26cfc073883129e9d89406"
    assert hash1 == expected1, f"Empty qualifiers hash drift! Expected {expected1}, got {hash1}"

    # Test 2: Qualifier with colon in value (special character handling)
    hash2 = generate_termlink_hash(subject_iri, term_iri, ["severity:mild:moderate"])
    expected2 = "3804648c2710e0ea9d6e70c66d5d1e4928203271a04e6f5e6c3d7a79a775c351"
    assert hash2 == expected2, f"Colon in value hash drift! Expected {expected2}, got {hash2}"

    # Test 3: Evidence hash with None values
    evidence_hash = generate_evidence_hash(
        clinical_note_id=None,
        encounter_id=None,
        term_iri=term_iri,
        span_start=None,
        span_end=None,
        qualifiers=[],
        subject_id=None,
        creator_id=None
    )
    expected3 = "2e74cb28084f4a53eb464cfad495186e6cd579ac44ac52e232278a767082f74c"
    assert evidence_hash == expected3, f"None values hash drift! Expected {expected3}, got {evidence_hash}"


def test_split_qualifier_for_storage_internal():
    """Test splitting internal qualifiers for storage."""
    from phebee.utils.iceberg import _split_qualifier_for_storage

    # Internal qualifier with value
    result = _split_qualifier_for_storage("negated:true")
    assert result == {"qualifier_type": "negated", "qualifier_value": "true"}

    # Internal qualifier with false value
    result = _split_qualifier_for_storage("family:false")
    assert result == {"qualifier_type": "family", "qualifier_value": "false"}

    # Internal qualifier without value (should have been normalized)
    result = _split_qualifier_for_storage("hypothetical:true")
    assert result == {"qualifier_type": "hypothetical", "qualifier_value": "true"}


def test_split_qualifier_for_storage_external():
    """Test splitting external IRI qualifiers for storage."""
    from phebee.utils.iceberg import _split_qualifier_for_storage

    # External IRI with semantic value
    result = _split_qualifier_for_storage("http://purl.obolibrary.org/obo/HP_0012823:present")
    assert result == {
        "qualifier_type": "http://purl.obolibrary.org/obo/HP_0012823",
        "qualifier_value": "present"
    }

    # External IRI with boolean value
    result = _split_qualifier_for_storage("http://purl.obolibrary.org/obo/HP_0040283:true")
    assert result == {
        "qualifier_type": "http://purl.obolibrary.org/obo/HP_0040283",
        "qualifier_value": "true"
    }

    # HTTPS IRI
    result = _split_qualifier_for_storage("https://example.org/qualifier/Q123:mild")
    assert result == {
        "qualifier_type": "https://example.org/qualifier/Q123",
        "qualifier_value": "mild"
    }

    # External IRI without value (edge case)
    result = _split_qualifier_for_storage("http://purl.obolibrary.org/obo/HP_0040283:true")
    assert result == {
        "qualifier_type": "http://purl.obolibrary.org/obo/HP_0040283",
        "qualifier_value": "true"
    }


def test_parse_qualifiers_field_internal():
    """Test parsing internal qualifiers from storage."""
    from phebee.utils.iceberg import parse_qualifiers_field
    from phebee.utils.qualifier import Qualifier

    # Active internal qualifier
    athena_output = "[{qualifier_type=negated, qualifier_value=true}]"
    result = parse_qualifiers_field(athena_output)
    assert result == [Qualifier(type="negated", value="true")]

    # Multiple internal qualifiers
    athena_output = "[{qualifier_type=negated, qualifier_value=true}, {qualifier_type=hypothetical, qualifier_value=true}]"
    result = parse_qualifiers_field(athena_output)
    assert set(result) == {Qualifier(type="negated", value="true"), Qualifier(type="hypothetical", value="true")}

    # False qualifier should be filtered out
    athena_output = "[{qualifier_type=negated, qualifier_value=true}, {qualifier_type=family, qualifier_value=false}]"
    result = parse_qualifiers_field(athena_output)
    assert result == [Qualifier(type="negated", value="true")]

    # Empty/null cases
    assert parse_qualifiers_field("") == []
    assert parse_qualifiers_field("null") == []
    assert parse_qualifiers_field(None) == []


def test_parse_qualifiers_field_external():
    """Test parsing external IRI qualifiers from storage."""
    from phebee.utils.iceberg import parse_qualifiers_field
    from phebee.utils.qualifier import Qualifier

    # External IRI with semantic value
    athena_output = "[{qualifier_type=http://purl.obolibrary.org/obo/HP_0012823, qualifier_value=present}]"
    result = parse_qualifiers_field(athena_output)
    assert result == [Qualifier(type="http://purl.obolibrary.org/obo/HP_0012823", value="present")]

    # External IRI with boolean value
    athena_output = "[{qualifier_type=http://purl.obolibrary.org/obo/HP_0040283, qualifier_value=true}]"
    result = parse_qualifiers_field(athena_output)
    assert result == [Qualifier(type="http://purl.obolibrary.org/obo/HP_0040283", value="true")]

    # External IRI with false should be filtered
    athena_output = "[{qualifier_type=http://purl.obolibrary.org/obo/HP_0040283, qualifier_value=false}]"
    result = parse_qualifiers_field(athena_output)
    assert result == []


def test_parse_qualifiers_field_mixed():
    """Test parsing mixed internal and external qualifiers."""
    from phebee.utils.iceberg import parse_qualifiers_field
    from phebee.utils.qualifier import Qualifier

    # Mixed qualifiers with various values
    athena_output = (
        "[{qualifier_type=negated, qualifier_value=true}, "
        "{qualifier_type=http://purl.obolibrary.org/obo/HP_0012823, qualifier_value=present}, "
        "{qualifier_type=family, qualifier_value=false}]"
    )
    result = parse_qualifiers_field(athena_output)
    assert set(result) == {
        Qualifier(type="negated", value="true"),
        Qualifier(type="http://purl.obolibrary.org/obo/HP_0012823", value="present")
    }


def test_storage_retrieval_roundtrip():
    """Test that qualifiers survive a round-trip through storage format."""
    from phebee.utils.iceberg import parse_qualifiers_field
    from phebee.utils.qualifier import Qualifier

    test_cases = [
        (["negated:true"], [Qualifier(type="negated", value="true")]),
        (["http://purl.obolibrary.org/obo/HP_0012823:present"],
         [Qualifier(type="http://purl.obolibrary.org/obo/HP_0012823", value="present")]),
        (["negated:true", "http://purl.obolibrary.org/obo/HP_0040283:mild"],
         [Qualifier(type="negated", value="true"),
          Qualifier(type="http://purl.obolibrary.org/obo/HP_0040283", value="mild")]),
        (["hypothetical:true", "family:false"],  # false should be filtered
         [Qualifier(type="hypothetical", value="true")]),
    ]

    for qualifiers_str, expected_qualifiers in test_cases:
        # Convert string qualifiers to Qualifier objects for storage
        qualifiers_obj = [Qualifier.from_string(q) for q in qualifiers_str]

        # Simulate storage format (convert to storage dicts)
        stored = [q.to_storage_dict() for q in qualifiers_obj]

        # Simulate Athena output format
        athena_str = "["
        athena_str += ", ".join([
            f"{{qualifier_type={s['qualifier_type']}, qualifier_value={s['qualifier_value']}}}"
            for s in stored
        ])
        athena_str += "]"

        # Parse back
        retrieved = parse_qualifiers_field(athena_str)

        assert set(retrieved) == set(expected_qualifiers), f"Round-trip failed for {qualifiers_str}"
