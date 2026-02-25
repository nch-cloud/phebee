"""
Unit tests for string manipulation utility functions.
"""
import pytest
import re
from phebee.utils.iceberg import _extract_term_id_from_iri
from phebee.utils.sparql import camel_to_snake, split_predicate


class TestExtractTermIdFromIri:
    """Test IRI to term ID extraction."""

    def test_hp_obo_iri(self):
        """Test extracting HP term ID from OBO IRI."""
        iri = "http://purl.obolibrary.org/obo/HP_0001250"
        result = _extract_term_id_from_iri(iri)
        assert result == "HP:0001250"

    def test_mondo_obo_iri(self):
        """Test extracting MONDO term ID from OBO IRI."""
        iri = "http://purl.obolibrary.org/obo/MONDO_0000001"
        result = _extract_term_id_from_iri(iri)
        assert result == "MONDO:0000001"

    def test_eco_obo_iri(self):
        """Test extracting ECO term ID from OBO IRI."""
        iri = "http://purl.obolibrary.org/obo/ECO_0000311"
        result = _extract_term_id_from_iri(iri)
        assert result == "ECO:0000311"

    def test_maxo_obo_iri(self):
        """Test extracting MAXO term ID from OBO IRI."""
        iri = "http://purl.obolibrary.org/obo/MAXO_0000001"
        result = _extract_term_id_from_iri(iri)
        assert result == "MAXO:0000001"

    def test_zero_padded_ids(self):
        """Test with various zero padding patterns."""
        assert _extract_term_id_from_iri("http://purl.obolibrary.org/obo/HP_0000001") == "HP:0000001"
        assert _extract_term_id_from_iri("http://purl.obolibrary.org/obo/HP_0001234") == "HP:0001234"
        assert _extract_term_id_from_iri("http://purl.obolibrary.org/obo/HP_1234567") == "HP:1234567"

    def test_non_obo_iri(self):
        """Test with non-OBO format IRI (no /obo/ in path)."""
        iri = "http://example.org/ontology/terms/HP_0001250"
        result = _extract_term_id_from_iri(iri)
        # Should just take the last part
        assert result == "HP_0001250"

    def test_non_obo_iri_with_slash(self):
        """Test non-OBO IRI extracts last segment."""
        iri = "http://example.org/vocab/hasDisease"
        result = _extract_term_id_from_iri(iri)
        assert result == "hasDisease"

    def test_none_input(self):
        """Test that None input returns None."""
        result = _extract_term_id_from_iri(None)
        assert result is None

    def test_empty_string(self):
        """Test that empty string returns None."""
        result = _extract_term_id_from_iri("")
        assert result is None

    def test_iri_with_multiple_underscores(self):
        """Test IRI with multiple underscores in term ID."""
        iri = "http://purl.obolibrary.org/obo/HP_0001_2_3"
        result = _extract_term_id_from_iri(iri)
        # All underscores should be replaced
        assert result == "HP:0001:2:3"

    def test_iri_without_underscore(self):
        """Test OBO IRI without underscore separator."""
        iri = "http://purl.obolibrary.org/obo/HP-0001250"
        result = _extract_term_id_from_iri(iri)
        # No underscore to replace
        assert result == "HP-0001250"

    def test_trailing_slash(self):
        """Test IRI with trailing slash."""
        iri = "http://purl.obolibrary.org/obo/HP_0001250/"
        result = _extract_term_id_from_iri(iri)
        # Takes portion after /obo/ including trailing slash, then replaces underscores
        assert result == "HP:0001250/"

    def test_obo_in_domain(self):
        """Test IRI with 'obo' in domain but not in path."""
        iri = "http://obo.example.org/terms/HP_0001250"
        result = _extract_term_id_from_iri(iri)
        # Should not match /obo/ pattern, just take last part
        assert result == "HP_0001250"


class TestCamelToSnake:
    """Test camelCase to snake_case conversion."""

    def test_simple_camel_case(self):
        """Test simple camelCase conversion."""
        assert camel_to_snake("camelCase") == "camel_case"

    def test_pascal_case(self):
        """Test PascalCase conversion."""
        assert camel_to_snake("PascalCase") == "pascal_case"

    def test_multiple_words(self):
        """Test multiple word conversion."""
        assert camel_to_snake("thisIsCamelCase") == "this_is_camel_case"

    def test_with_numbers(self):
        """Test conversion with numbers."""
        assert camel_to_snake("projectId1") == "project_id1"
        assert camel_to_snake("subject123Name") == "subject123_name"

    def test_consecutive_capitals(self):
        """Test with consecutive capital letters (acronyms)."""
        assert camel_to_snake("HTTPResponse") == "http_response"
        assert camel_to_snake("XMLParser") == "xml_parser"

    def test_single_word(self):
        """Test single lowercase word."""
        assert camel_to_snake("word") == "word"

    def test_single_capital(self):
        """Test single capital letter."""
        assert camel_to_snake("A") == "a"

    def test_already_snake_case(self):
        """Test string that's already snake_case."""
        assert camel_to_snake("already_snake_case") == "already_snake_case"

    def test_mixed_case_with_underscores(self):
        """Test mixed camelCase with existing underscores."""
        assert camel_to_snake("project_SubjectId") == "project__subject_id"

    def test_real_world_examples(self):
        """Test real-world field names from the codebase."""
        assert camel_to_snake("projectSubjectId") == "project_subject_id"
        assert camel_to_snake("termIri") == "term_iri"
        assert camel_to_snake("evidenceType") == "evidence_type"
        assert camel_to_snake("createdBy") == "created_by"

    def test_empty_string(self):
        """Test empty string."""
        assert camel_to_snake("") == ""

    def test_numbers_at_start(self):
        """Test with numbers at start (unusual but valid)."""
        assert camel_to_snake("123Name") == "123_name"

    def test_all_caps(self):
        """Test all caps string."""
        result = camel_to_snake("ALLCAPS")
        # Depends on implementation, but should be lowercase
        assert result.islower()


class TestSplitPredicate:
    """Test predicate IRI splitting and conversion."""

    def test_hash_separator(self):
        """Test predicate with hash separator."""
        pred = "http://example.org/vocab#hasLabel"
        result = split_predicate(pred)
        assert result == "has_label"

    def test_slash_separator(self):
        """Test predicate with slash separator."""
        pred = "http://example.org/vocab/hasLabel"
        result = split_predicate(pred)
        assert result == "has_label"

    def test_rdfs_label(self):
        """Test common RDFS label predicate."""
        pred = "http://www.w3.org/2000/01/rdf-schema#label"
        result = split_predicate(pred)
        assert result == "label"

    def test_rdf_type(self):
        """Test RDF type predicate."""
        pred = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        result = split_predicate(pred)
        assert result == "type"

    def test_camel_case_predicate(self):
        """Test camelCase predicate conversion."""
        pred = "http://example.org/vocab#projectSubjectId"
        result = split_predicate(pred)
        assert result == "project_subject_id"

    def test_pascal_case_predicate(self):
        """Test PascalCase predicate conversion."""
        pred = "http://example.org/vocab/ProjectSubjectId"
        result = split_predicate(pred)
        assert result == "project_subject_id"

    def test_already_lowercase(self):
        """Test predicate that's already lowercase."""
        pred = "http://example.org/vocab#name"
        result = split_predicate(pred)
        assert result == "name"

    def test_phebee_predicates(self):
        """Test PheBee-specific predicates."""
        assert split_predicate("http://ods.nationwidechildrens.org/phebee#hasEvidence") == "has_evidence"
        assert split_predicate("http://ods.nationwidechildrens.org/phebee#termIri") == "term_iri"

    def test_multiple_hashes(self):
        """Test IRI with multiple hash symbols (takes last part)."""
        pred = "http://example.org/vocab#section#hasLabel"
        result = split_predicate(pred)
        assert result == "has_label"

    def test_multiple_slashes(self):
        """Test IRI with multiple slashes (takes last part)."""
        pred = "http://example.org/vocab/v1/hasLabel"
        result = split_predicate(pred)
        assert result == "has_label"

    def test_hash_and_slash(self):
        """Test IRI with both hash and slash (hash takes precedence)."""
        pred = "http://example.org/vocab/v1#hasLabel"
        result = split_predicate(pred)
        # Hash separator checked first
        assert result == "has_label"

    def test_no_separator(self):
        """Test simple string without IRI structure."""
        pred = "hasLabel"
        result = split_predicate(pred)
        assert result == "has_label"

    def test_empty_string(self):
        """Test empty string."""
        result = split_predicate("")
        assert result == ""

    def test_with_numbers(self):
        """Test predicate with numbers."""
        pred = "http://example.org/vocab#hasValue1"
        result = split_predicate(pred)
        assert result == "has_value1"

    def test_acronyms_in_predicate(self):
        """Test predicate with acronyms."""
        pred = "http://example.org/vocab#hasXMLData"
        result = split_predicate(pred)
        assert result == "has_xml_data"
