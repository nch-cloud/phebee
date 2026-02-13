"""
Unit tests for Phenopacket utility functions.
"""
import pytest
from phebee.utils.phenopackets import _compact_iri, _build_metadata


class TestCompactIri:
    """Test IRI compaction functionality."""

    def test_hp_term_iri(self):
        """Test compacting HP term IRI."""
        iri = "http://purl.obolibrary.org/obo/HP_0001250"
        result = _compact_iri(iri)
        assert result == "HP:0001250"

    def test_mondo_term_iri(self):
        """Test compacting MONDO term IRI."""
        iri = "http://purl.obolibrary.org/obo/MONDO_0000001"
        result = _compact_iri(iri)
        assert result == "MONDO:0000001"

    def test_hp_zero_padded_id(self):
        """Test compacting HP term with various zero padding."""
        iri = "http://purl.obolibrary.org/obo/HP_0000001"
        result = _compact_iri(iri)
        assert result == "HP:0000001"

    def test_mondo_longer_id(self):
        """Test compacting MONDO term with longer ID."""
        iri = "http://purl.obolibrary.org/obo/MONDO_0800477"
        result = _compact_iri(iri)
        assert result == "MONDO:0800477"

    def test_non_obo_iri_unchanged(self):
        """Test that non-OBO IRIs are returned unchanged."""
        iri = "http://example.org/ontology/term123"
        result = _compact_iri(iri)
        assert result == iri

    def test_eco_term_iri(self):
        """Test compacting ECO term IRI."""
        iri = "http://purl.obolibrary.org/obo/ECO_0000311"
        result = _compact_iri(iri)
        assert result == "ECO:0000311"

    def test_maxo_term_iri(self):
        """Test compacting MAXO term IRI."""
        iri = "http://purl.obolibrary.org/obo/MAXO_0000001"
        result = _compact_iri(iri)
        assert result == "MAXO:0000001"

    def test_none_input(self):
        """Test that None input returns None."""
        result = _compact_iri(None)
        assert result is None

    def test_empty_string(self):
        """Test that empty string returns empty string."""
        result = _compact_iri("")
        assert result == ""

    def test_already_compacted(self):
        """Test that already-compacted IRI is returned unchanged."""
        iri = "HP:0001250"
        result = _compact_iri(iri)
        assert result == "HP:0001250"

    def test_iri_without_underscore(self):
        """Test IRI that doesn't follow standard OBO pattern."""
        iri = "http://purl.obolibrary.org/obo/HP-0001250"
        result = _compact_iri(iri)
        # Strips prefix but has no underscore, so dashes remain
        assert result == "HP-0001250"

    def test_multiple_underscores(self):
        """Test compacting IRI with term ID containing underscores."""
        iri = "http://purl.obolibrary.org/obo/HP_0001_2_3"
        result = _compact_iri(iri)
        # All underscores are replaced with colons
        assert result == "HP:0001:2:3"

    def test_http_vs_https(self):
        """Test that http (not https) is required for OBO IRIs."""
        iri = "https://purl.obolibrary.org/obo/HP_0001250"
        result = _compact_iri(iri)
        # https won't match pattern, returns unchanged
        assert result == iri


class TestBuildMetadata:
    """Test phenopacket metadata construction."""

    def test_basic_structure(self):
        """Test that metadata has all required top-level fields."""
        metadata = _build_metadata("2024-01-17", "v2024-01-04")

        assert "created" in metadata
        assert "createdBy" in metadata
        assert "resources" in metadata
        assert "phenopacketSchemaVersion" in metadata

    def test_created_by_value(self):
        """Test that createdBy is set to PheBee."""
        metadata = _build_metadata("2024-01-17", "v2024-01-04")
        assert metadata["createdBy"] == "PheBee"

    def test_phenopacket_schema_version(self):
        """Test that phenopacket schema version is 2.0."""
        metadata = _build_metadata("2024-01-17", "v2024-01-04")
        assert metadata["phenopacketSchemaVersion"] == "2.0"

    def test_created_timestamp_exists(self):
        """Test that created timestamp is present and non-empty."""
        metadata = _build_metadata("2024-01-17", "v2024-01-04")
        assert isinstance(metadata["created"], str)
        assert len(metadata["created"]) > 0

    def test_resources_is_list(self):
        """Test that resources is a list."""
        metadata = _build_metadata("2024-01-17", "v2024-01-04")
        assert isinstance(metadata["resources"], list)

    def test_resources_count(self):
        """Test that there are exactly 2 resources (HP and MONDO)."""
        metadata = _build_metadata("2024-01-17", "v2024-01-04")
        assert len(metadata["resources"]) == 2

    def test_hp_resource_structure(self):
        """Test HP resource has all required fields."""
        metadata = _build_metadata("2024-01-17", "v2024-01-04")
        hp_resource = next(r for r in metadata["resources"] if r["id"] == "hp")

        assert hp_resource["id"] == "hp"
        assert hp_resource["name"] == "Human Phenotype Ontology"
        assert hp_resource["url"] == "http://purl.obolibrary.org/obo/hp.owl"
        assert hp_resource["version"] == "2024-01-17"
        assert hp_resource["namespacePrefix"] == "HP"
        assert hp_resource["iriPrefix"] == "http://purl.obolibrary.org/obo/HP_"

    def test_mondo_resource_structure(self):
        """Test MONDO resource has all required fields."""
        metadata = _build_metadata("2024-01-17", "v2024-01-04")
        mondo_resource = next(r for r in metadata["resources"] if r["id"] == "mondo")

        assert mondo_resource["id"] == "mondo"
        assert mondo_resource["name"] == "MONDO Disease Ontology"
        assert mondo_resource["url"] == "http://purl.obolibrary.org/obo/mondo.owl"
        assert mondo_resource["version"] == "v2024-01-04"
        assert mondo_resource["namespacePrefix"] == "MONDO"
        assert mondo_resource["iriPrefix"] == "http://purl.obolibrary.org/obo/MONDO_"

    def test_version_insertion_hp(self):
        """Test that HPO version is correctly inserted."""
        hpo_version = "2024-12-25"
        metadata = _build_metadata(hpo_version, "v2024-01-04")
        hp_resource = next(r for r in metadata["resources"] if r["id"] == "hp")
        assert hp_resource["version"] == hpo_version

    def test_version_insertion_mondo(self):
        """Test that MONDO version is correctly inserted."""
        mondo_version = "v2024-12-25"
        metadata = _build_metadata("2024-01-17", mondo_version)
        mondo_resource = next(r for r in metadata["resources"] if r["id"] == "mondo")
        assert mondo_resource["version"] == mondo_version

    def test_different_version_formats(self):
        """Test with different version format strings."""
        metadata = _build_metadata("v1.2.3", "release-2024")
        hp_resource = next(r for r in metadata["resources"] if r["id"] == "hp")
        mondo_resource = next(r for r in metadata["resources"] if r["id"] == "mondo")

        assert hp_resource["version"] == "v1.2.3"
        assert mondo_resource["version"] == "release-2024"

    def test_empty_versions(self):
        """Test with empty version strings."""
        metadata = _build_metadata("", "")
        hp_resource = next(r for r in metadata["resources"] if r["id"] == "hp")
        mondo_resource = next(r for r in metadata["resources"] if r["id"] == "mondo")

        assert hp_resource["version"] == ""
        assert mondo_resource["version"] == ""

    def test_metadata_immutability_between_calls(self):
        """Test that multiple calls produce independent metadata objects."""
        metadata1 = _build_metadata("v1", "v1")
        metadata2 = _build_metadata("v2", "v2")

        # Modifying metadata1 shouldn't affect metadata2
        metadata1["resources"][0]["version"] = "modified"
        assert metadata2["resources"][0]["version"] == "v2"
