"""
Unit tests for phenopacket processing functions.
"""
import pytest
import sys
import os

# Add the functions directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../functions'))

# Mock aws_lambda_powertools before importing the module
from unittest.mock import MagicMock
sys.modules['aws_lambda_powertools'] = MagicMock()

from process_phenopacket import create_iri_map, to_full_iri, create_evidence_payload


class TestCreateIriMap:
    """Test IRI mapping creation from phenopacket metadata."""

    def test_valid_phenopacket_with_hp_mondo(self):
        """Test creating IRI map from valid phenopacket with HP and MONDO resources."""
        phenopacket = {
            "metaData": {
                "resources": [
                    {
                        "namespacePrefix": "HP",
                        "iriPrefix": "http://purl.obolibrary.org/obo/HP_"
                    },
                    {
                        "namespacePrefix": "MONDO",
                        "iriPrefix": "http://purl.obolibrary.org/obo/MONDO_"
                    }
                ]
            }
        }

        result = create_iri_map(phenopacket)
        assert result == {
            "HP": "http://purl.obolibrary.org/obo/HP_",
            "MONDO": "http://purl.obolibrary.org/obo/MONDO_"
        }

    def test_empty_resources(self):
        """Test with empty resources list."""
        phenopacket = {
            "metaData": {
                "resources": []
            }
        }

        result = create_iri_map(phenopacket)
        assert result == {}

    def test_single_resource(self):
        """Test with single resource."""
        phenopacket = {
            "metaData": {
                "resources": [
                    {
                        "namespacePrefix": "HP",
                        "iriPrefix": "http://purl.obolibrary.org/obo/HP_"
                    }
                ]
            }
        }

        result = create_iri_map(phenopacket)
        assert result == {"HP": "http://purl.obolibrary.org/obo/HP_"}

    def test_multiple_ontologies(self):
        """Test with multiple ontology resources."""
        phenopacket = {
            "metaData": {
                "resources": [
                    {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"},
                    {"namespacePrefix": "MONDO", "iriPrefix": "http://purl.obolibrary.org/obo/MONDO_"},
                    {"namespacePrefix": "MAXO", "iriPrefix": "http://purl.obolibrary.org/obo/MAXO_"},
                    {"namespacePrefix": "ECO", "iriPrefix": "http://purl.obolibrary.org/obo/ECO_"}
                ]
            }
        }

        result = create_iri_map(phenopacket)
        assert len(result) == 4
        assert result["HP"] == "http://purl.obolibrary.org/obo/HP_"
        assert result["MONDO"] == "http://purl.obolibrary.org/obo/MONDO_"
        assert result["MAXO"] == "http://purl.obolibrary.org/obo/MAXO_"
        assert result["ECO"] == "http://purl.obolibrary.org/obo/ECO_"


class TestToFullIri:
    """Test CURIE to full IRI conversion."""

    def test_hp_curie_conversion(self):
        """Test converting HP CURIE to full IRI."""
        namespace_map = {
            "HP": "http://purl.obolibrary.org/obo/HP_",
            "MONDO": "http://purl.obolibrary.org/obo/MONDO_"
        }

        result = to_full_iri(namespace_map, "HP:0001250")
        assert result == "http://purl.obolibrary.org/obo/HP_0001250"

    def test_mondo_curie_conversion(self):
        """Test converting MONDO CURIE to full IRI."""
        namespace_map = {
            "HP": "http://purl.obolibrary.org/obo/HP_",
            "MONDO": "http://purl.obolibrary.org/obo/MONDO_"
        }

        result = to_full_iri(namespace_map, "MONDO:0000001")
        assert result == "http://purl.obolibrary.org/obo/MONDO_0000001"

    def test_unknown_namespace(self):
        """Test that unknown namespace returns CURIE unchanged."""
        namespace_map = {
            "HP": "http://purl.obolibrary.org/obo/HP_"
        }

        result = to_full_iri(namespace_map, "UNKNOWN:0001234")
        assert result == "UNKNOWN:0001234"

    def test_already_full_iri(self):
        """Test that full IRI is returned unchanged."""
        namespace_map = {
            "HP": "http://purl.obolibrary.org/obo/HP_"
        }

        full_iri = "http://example.org/ontology/term123"
        result = to_full_iri(namespace_map, full_iri)
        assert result == full_iri

    def test_empty_namespace_map(self):
        """Test with empty namespace map."""
        result = to_full_iri({}, "HP:0001250")
        assert result == "HP:0001250"

    def test_curie_without_colon(self):
        """Test term without namespace separator."""
        namespace_map = {
            "HP": "http://purl.obolibrary.org/obo/HP_"
        }

        result = to_full_iri(namespace_map, "HP0001250")
        # Won't match pattern, returns unchanged
        assert result == "HP0001250"

    def test_maxo_curie_conversion(self):
        """Test converting MAXO CURIE to full IRI."""
        namespace_map = {
            "MAXO": "http://purl.obolibrary.org/obo/MAXO_"
        }

        result = to_full_iri(namespace_map, "MAXO:0000001")
        assert result == "http://purl.obolibrary.org/obo/MAXO_0000001"

    def test_case_sensitivity(self):
        """Test that namespace matching is case-sensitive."""
        namespace_map = {
            "HP": "http://purl.obolibrary.org/obo/HP_"
        }

        # lowercase 'hp' shouldn't match uppercase 'HP'
        result = to_full_iri(namespace_map, "hp:0001250")
        assert result == "hp:0001250"


class TestCreateEvidencePayload:
    """Test evidence payload creation from phenopacket."""

    def test_basic_phenopacket_single_feature(self):
        """Test creating evidence payload from basic phenopacket with single feature."""
        phenopacket = {
            "id": "subject-001",
            "metaData": {
                "createdBy": "test-user",
                "resources": [
                    {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
                ]
            },
            "phenotypicFeatures": [
                {
                    "type": {"id": "HP:0001250"},
                    "excluded": False
                }
            ]
        }

        result = create_evidence_payload("project-1", "subject-001", phenopacket)

        assert len(result) == 1
        assert result[0]["subject_id"] == "project-1#subject-001"
        assert result[0]["term_iri"] == "http://purl.obolibrary.org/obo/HP_0001250"
        assert result[0]["evidence_type"] == "http://purl.obolibrary.org/obo/ECO_0000311"
        assert result[0]["creator_id"] == "test-user"
        assert result[0]["creator_type"] == "system"
        assert result[0]["qualifiers"] == []

    def test_excluded_feature_creates_negated_qualifier(self):
        """Test that excluded feature creates negated qualifier."""
        phenopacket = {
            "id": "subject-001",
            "metaData": {
                "createdBy": "test-user",
                "resources": [
                    {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
                ]
            },
            "phenotypicFeatures": [
                {
                    "type": {"id": "HP:0001250"},
                    "excluded": True
                }
            ]
        }

        result = create_evidence_payload("project-1", "subject-001", phenopacket)

        assert len(result) == 1
        assert result[0]["qualifiers"] == ["negated"]

    def test_missing_excluded_field_no_qualifiers(self):
        """Test that missing excluded field results in no qualifiers."""
        phenopacket = {
            "id": "subject-001",
            "metaData": {
                "createdBy": "test-user",
                "resources": [
                    {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
                ]
            },
            "phenotypicFeatures": [
                {
                    "type": {"id": "HP:0001250"}
                }
            ]
        }

        result = create_evidence_payload("project-1", "subject-001", phenopacket)

        assert len(result) == 1
        assert result[0]["qualifiers"] == []

    def test_multiple_phenotypic_features(self):
        """Test creating evidence for multiple phenotypic features."""
        phenopacket = {
            "id": "subject-001",
            "metaData": {
                "createdBy": "test-user",
                "resources": [
                    {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
                ]
            },
            "phenotypicFeatures": [
                {"type": {"id": "HP:0001250"}, "excluded": False},
                {"type": {"id": "HP:0001627"}, "excluded": True},
                {"type": {"id": "HP:0000001"}, "excluded": False}
            ]
        }

        result = create_evidence_payload("project-1", "subject-001", phenopacket)

        assert len(result) == 3
        assert result[0]["term_iri"] == "http://purl.obolibrary.org/obo/HP_0001250"
        assert result[0]["qualifiers"] == []
        assert result[1]["term_iri"] == "http://purl.obolibrary.org/obo/HP_0001627"
        assert result[1]["qualifiers"] == ["negated"]
        assert result[2]["term_iri"] == "http://purl.obolibrary.org/obo/HP_0000001"
        assert result[2]["qualifiers"] == []

    def test_empty_phenotypic_features(self):
        """Test with empty phenotypicFeatures list."""
        phenopacket = {
            "id": "subject-001",
            "metaData": {
                "createdBy": "test-user",
                "resources": []
            },
            "phenotypicFeatures": []
        }

        result = create_evidence_payload("project-1", "subject-001", phenopacket)
        assert result == []

    def test_missing_created_by_uses_default(self):
        """Test that missing createdBy uses default 'phenopacket-importer'."""
        phenopacket = {
            "id": "subject-001",
            "metaData": {
                "resources": [
                    {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
                ]
            },
            "phenotypicFeatures": [
                {"type": {"id": "HP:0001250"}}
            ]
        }

        result = create_evidence_payload("project-1", "subject-001", phenopacket)

        assert len(result) == 1
        assert result[0]["creator_id"] == "phenopacket-importer"

    def test_empty_metadata_uses_default_creator(self):
        """Test that metaData without createdBy uses default creator."""
        phenopacket = {
            "id": "subject-001",
            "metaData": {
                "resources": []  # Empty resources but still valid structure
            },
            "phenotypicFeatures": [
                {"type": {"id": "http://purl.obolibrary.org/obo/HP_0001250"}}
            ]
        }

        result = create_evidence_payload("project-1", "subject-001", phenopacket)

        assert len(result) == 1
        assert result[0]["creator_id"] == "phenopacket-importer"

    def test_evidence_type_is_eco_0000311(self):
        """Test that evidence type is always ECO:0000311."""
        phenopacket = {
            "id": "subject-001",
            "metaData": {
                "createdBy": "test-user",
                "resources": [
                    {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
                ]
            },
            "phenotypicFeatures": [
                {"type": {"id": "HP:0001250"}}
            ]
        }

        result = create_evidence_payload("project-1", "subject-001", phenopacket)

        assert result[0]["evidence_type"] == "http://purl.obolibrary.org/obo/ECO_0000311"

    def test_creator_type_is_system(self):
        """Test that creator_type is always 'system'."""
        phenopacket = {
            "id": "subject-001",
            "metaData": {
                "createdBy": "test-user",
                "resources": [
                    {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
                ]
            },
            "phenotypicFeatures": [
                {"type": {"id": "HP:0001250"}}
            ]
        }

        result = create_evidence_payload("project-1", "subject-001", phenopacket)

        assert result[0]["creator_type"] == "system"

    def test_temporary_subject_id_format(self):
        """Test that subject_id uses temporary format before UUID replacement."""
        phenopacket = {
            "id": "subject-001",
            "metaData": {
                "resources": [
                    {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
                ]
            },
            "phenotypicFeatures": [
                {"type": {"id": "HP:0001250"}}
            ]
        }

        result = create_evidence_payload("my-project", "my-subject", phenopacket)

        assert result[0]["subject_id"] == "my-project#my-subject"

    def test_mondo_disease_terms(self):
        """Test processing MONDO disease terms."""
        phenopacket = {
            "id": "subject-001",
            "metaData": {
                "createdBy": "test-user",
                "resources": [
                    {"namespacePrefix": "MONDO", "iriPrefix": "http://purl.obolibrary.org/obo/MONDO_"}
                ]
            },
            "phenotypicFeatures": [
                {"type": {"id": "MONDO:0008840"}, "excluded": False}
            ]
        }

        result = create_evidence_payload("project-1", "subject-001", phenopacket)

        assert len(result) == 1
        assert result[0]["term_iri"] == "http://purl.obolibrary.org/obo/MONDO_0008840"

    def test_mixed_hp_and_mondo_terms(self):
        """Test processing phenopacket with both HP and MONDO terms."""
        phenopacket = {
            "id": "subject-001",
            "metaData": {
                "createdBy": "test-user",
                "resources": [
                    {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"},
                    {"namespacePrefix": "MONDO", "iriPrefix": "http://purl.obolibrary.org/obo/MONDO_"}
                ]
            },
            "phenotypicFeatures": [
                {"type": {"id": "HP:0001250"}},
                {"type": {"id": "MONDO:0008840"}}
            ]
        }

        result = create_evidence_payload("project-1", "subject-001", phenopacket)

        assert len(result) == 2
        assert result[0]["term_iri"] == "http://purl.obolibrary.org/obo/HP_0001250"
        assert result[1]["term_iri"] == "http://purl.obolibrary.org/obo/MONDO_0008840"

    def test_special_characters_in_project_subject_id(self):
        """Test handling special characters in project/subject IDs."""
        phenopacket = {
            "id": "subject-001",
            "metaData": {
                "resources": [
                    {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
                ]
            },
            "phenotypicFeatures": [
                {"type": {"id": "HP:0001250"}}
            ]
        }

        result = create_evidence_payload("project_123", "subject-with-dashes", phenopacket)

        assert result[0]["subject_id"] == "project_123#subject-with-dashes"
