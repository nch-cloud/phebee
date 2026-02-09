"""
Unit tests for DynamoDB cache functions.

Tests the cache query functions using mocked DynamoDB responses.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from phebee.utils.dynamodb_cache import (
    build_term_path_pk,
    build_term_direct_pk,
    build_ancestor_path_sk,
    parse_term_id_from_iri,
    get_term_ancestor_paths,
    get_term_label,
    get_term_labels_from_cache
)


class TestKeyBuilders:
    """Test partition key and sort key builder functions."""

    def test_build_term_path_pk(self):
        """Test shared PK builder for descendant queries."""
        pk = build_term_path_pk("hpo", "2026-01-08")
        assert pk == "TERM_PATH#hpo|2026-01-08"

    def test_build_term_direct_pk(self):
        """Test unique PK builder for direct term lookups."""
        pk = build_term_direct_pk("hpo", "2026-01-08", "HP_0001627")
        assert pk == "TERM#hpo|2026-01-08|HP_0001627"

    def test_build_ancestor_path_sk_single_term(self):
        """Test SK builder with single term (root)."""
        sk = build_ancestor_path_sk(["HP_0000001"])
        assert sk == "HP_0000001"

    def test_build_ancestor_path_sk_multi_term(self):
        """Test SK builder with multi-term path."""
        sk = build_ancestor_path_sk(["HP_0000001", "HP_0000118", "HP_0001627"])
        assert sk == "HP_0000001|HP_0000118|HP_0001627"

    def test_build_ancestor_path_sk_empty(self):
        """Test SK builder with empty path."""
        sk = build_ancestor_path_sk([])
        assert sk == ""

    def test_parse_term_id_from_iri(self):
        """Test extracting term ID from full IRI."""
        term_id = parse_term_id_from_iri("http://purl.obolibrary.org/obo/HP_0001627")
        assert term_id == "HP_0001627"

    def test_parse_term_id_from_iri_mondo(self):
        """Test extracting MONDO term ID from IRI."""
        term_id = parse_term_id_from_iri("http://purl.obolibrary.org/obo/MONDO_0000001")
        assert term_id == "MONDO_0000001"


class TestGetTermAncestorPaths:
    """Test get_term_ancestor_paths function."""

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_single_path_term(self, mock_get_table):
        """Test getting ancestor paths for a term with single inheritance."""
        # Mock DynamoDB response
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        mock_table.query.return_value = {
            "Items": [
                {
                    "PK": "TERM#hpo|2026-01-08|HP_0000118",
                    "SK": "HP_0000001|HP_0000118",
                    "label": "Phenotypic abnormality",
                    "path_length": 2
                }
            ]
        }

        # Call function
        paths = get_term_ancestor_paths("hpo", "2026-01-08", "HP_0000118")

        # Verify
        assert len(paths) == 1
        assert paths[0] == ["HP_0000001", "HP_0000118"]

        # Verify query was called with correct PK
        mock_table.query.assert_called_once()
        call_args = mock_table.query.call_args
        assert call_args[1]["ExpressionAttributeValues"][":pk"] == "TERM#hpo|2026-01-08|HP_0000118"

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_multiple_paths_term(self, mock_get_table):
        """Test getting ancestor paths for a term with multiple inheritance."""
        # Mock DynamoDB response with multiple paths
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        mock_table.query.return_value = {
            "Items": [
                {
                    "PK": "TERM#hpo|2026-01-08|HP_0001627",
                    "SK": "HP_0000001|HP_0000118|HP_0001627",
                    "label": "Abnormal heart morphology",
                    "path_length": 3
                },
                {
                    "PK": "TERM#hpo|2026-01-08|HP_0001627",
                    "SK": "HP_0000001|HP_0001626|HP_0001627",
                    "label": "Abnormal heart morphology",
                    "path_length": 3
                }
            ]
        }

        # Call function
        paths = get_term_ancestor_paths("hpo", "2026-01-08", "HP_0001627")

        # Verify
        assert len(paths) == 2
        assert ["HP_0000001", "HP_0000118", "HP_0001627"] in paths
        assert ["HP_0000001", "HP_0001626", "HP_0001627"] in paths

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_root_term(self, mock_get_table):
        """Test getting ancestor paths for root term."""
        # Mock DynamoDB response for root
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        mock_table.query.return_value = {
            "Items": [
                {
                    "PK": "TERM#hpo|2026-01-08|HP_0000001",
                    "SK": "HP_0000001",
                    "label": "All",
                    "path_length": 1
                }
            ]
        }

        # Call function
        paths = get_term_ancestor_paths("hpo", "2026-01-08", "HP_0000001")

        # Verify
        assert len(paths) == 1
        assert paths[0] == ["HP_0000001"]

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_term_not_found(self, mock_get_table):
        """Test getting ancestor paths for non-existent term."""
        # Mock empty DynamoDB response
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        mock_table.query.return_value = {
            "Items": []
        }

        # Call function
        paths = get_term_ancestor_paths("hpo", "2026-01-08", "HP_9999999")

        # Verify
        assert paths == []


class TestGetTermLabel:
    """Test get_term_label function."""

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_get_label_success(self, mock_get_table):
        """Test getting label for a term."""
        # Mock DynamoDB response
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        mock_table.query.return_value = {
            "Items": [
                {
                    "PK": "TERM#hpo|2026-01-08|HP_0001627",
                    "SK": "HP_0000001|HP_0000118|HP_0001627",
                    "label": "Abnormal heart morphology",
                    "path_length": 3
                }
            ]
        }

        # Call function
        label = get_term_label("hpo", "2026-01-08", "HP_0001627")

        # Verify
        assert label == "Abnormal heart morphology"

        # Verify query used Limit=1
        call_args = mock_table.query.call_args
        assert call_args[1]["Limit"] == 1

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_get_label_with_multiple_paths(self, mock_get_table):
        """Test that label is returned from first path when multiple paths exist."""
        # Mock DynamoDB response with multiple paths (though Limit=1 would only return first)
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        mock_table.query.return_value = {
            "Items": [
                {
                    "PK": "TERM#hpo|2026-01-08|HP_0001627",
                    "SK": "HP_0000001|HP_0000118|HP_0001627",
                    "label": "Abnormal heart morphology",
                    "path_length": 3
                }
            ]
        }

        # Call function
        label = get_term_label("hpo", "2026-01-08", "HP_0001627")

        # Verify
        assert label == "Abnormal heart morphology"

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_get_label_term_not_found(self, mock_get_table):
        """Test getting label for non-existent term."""
        # Mock empty DynamoDB response
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        mock_table.query.return_value = {
            "Items": []
        }

        # Call function
        label = get_term_label("hpo", "2026-01-08", "HP_9999999")

        # Verify
        assert label is None

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_get_label_missing_label_field(self, mock_get_table):
        """Test getting label when term exists but has no label field."""
        # Mock DynamoDB response without label field
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        mock_table.query.return_value = {
            "Items": [
                {
                    "PK": "TERM#hpo|2026-01-08|HP_0001627",
                    "SK": "HP_0000001|HP_0000118|HP_0001627",
                    "path_length": 3
                    # Note: no "label" field
                }
            ]
        }

        # Call function
        label = get_term_label("hpo", "2026-01-08", "HP_0001627")

        # Verify
        assert label is None


class TestGetTermLabelsFromCache:
    """Tests for batch label lookup from cache."""

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_empty_list(self, mock_get_table):
        """Test with empty input list."""
        from phebee.utils.dynamodb_cache import get_term_labels_from_cache

        result = get_term_labels_from_cache([], "2026-01-08", "v2024-10-01")

        assert result == {}
        mock_get_table.assert_not_called()

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_single_hpo_term(self, mock_get_table):
        """Test with single HPO term."""
        from phebee.utils.dynamodb_cache import get_term_labels_from_cache

        # Mock table
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        # Mock response
        mock_table.query.return_value = {
            "Items": [{
                "PK": "TERM#hpo|2026-01-08|HP_0001627",
                "SK": "HP_0000001|HP_0000118|HP_0001627",
                "label": "Abnormal heart morphology",
                "path_length": 3
            }]
        }

        # Call function
        result = get_term_labels_from_cache(
            ["http://purl.obolibrary.org/obo/HP_0001627"],
            "2026-01-08",
            "v2024-10-01"
        )

        # Verify
        assert len(result) == 1
        assert result["http://purl.obolibrary.org/obo/HP_0001627"] == "Abnormal heart morphology"

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_multiple_hpo_terms(self, mock_get_table):
        """Test with multiple HPO terms."""
        from phebee.utils.dynamodb_cache import get_term_labels_from_cache

        # Mock table
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        # Mock responses for different queries
        def mock_query(**kwargs):
            pk = kwargs["ExpressionAttributeValues"][":pk"]
            if "HP_0001627" in pk:
                return {
                    "Items": [{
                        "label": "Abnormal heart morphology"
                    }]
                }
            elif "HP_0000118" in pk:
                return {
                    "Items": [{
                        "label": "Phenotypic abnormality"
                    }]
                }
            return {"Items": []}

        mock_table.query.side_effect = mock_query

        # Call function
        result = get_term_labels_from_cache(
            [
                "http://purl.obolibrary.org/obo/HP_0001627",
                "http://purl.obolibrary.org/obo/HP_0000118"
            ],
            "2026-01-08",
            "v2024-10-01"
        )

        # Verify
        assert len(result) == 2
        assert result["http://purl.obolibrary.org/obo/HP_0001627"] == "Abnormal heart morphology"
        assert result["http://purl.obolibrary.org/obo/HP_0000118"] == "Phenotypic abnormality"

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_mixed_hpo_mondo_terms(self, mock_get_table):
        """Test with mix of HPO and MONDO terms."""
        from phebee.utils.dynamodb_cache import get_term_labels_from_cache

        # Mock table
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        # Mock responses for different queries
        def mock_query(**kwargs):
            pk = kwargs["ExpressionAttributeValues"][":pk"]
            if "hpo" in pk and "HP_0001627" in pk:
                return {
                    "Items": [{
                        "label": "Abnormal heart morphology"
                    }]
                }
            elif "mondo" in pk and "MONDO_0000001" in pk:
                return {
                    "Items": [{
                        "label": "Disease or disorder"
                    }]
                }
            return {"Items": []}

        mock_table.query.side_effect = mock_query

        # Call function
        result = get_term_labels_from_cache(
            [
                "http://purl.obolibrary.org/obo/HP_0001627",
                "http://purl.obolibrary.org/obo/MONDO_0000001"
            ],
            "2026-01-08",
            "v2024-10-01"
        )

        # Verify
        assert len(result) == 2
        assert result["http://purl.obolibrary.org/obo/HP_0001627"] == "Abnormal heart morphology"
        assert result["http://purl.obolibrary.org/obo/MONDO_0000001"] == "Disease or disorder"

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_term_not_found(self, mock_get_table):
        """Test with term not in cache."""
        from phebee.utils.dynamodb_cache import get_term_labels_from_cache

        # Mock table
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        # Mock empty response
        mock_table.query.return_value = {"Items": []}

        # Call function
        result = get_term_labels_from_cache(
            ["http://purl.obolibrary.org/obo/HP_9999999"],
            "2026-01-08",
            "v2024-10-01"
        )

        # Verify - term not found should not be in result
        assert len(result) == 0

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_partial_results(self, mock_get_table):
        """Test with some terms found and some not found."""
        from phebee.utils.dynamodb_cache import get_term_labels_from_cache

        # Mock table
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        # Mock responses - one found, one not
        def mock_query(**kwargs):
            pk = kwargs["ExpressionAttributeValues"][":pk"]
            if "HP_0001627" in pk:
                return {
                    "Items": [{
                        "label": "Abnormal heart morphology"
                    }]
                }
            return {"Items": []}

        mock_table.query.side_effect = mock_query

        # Call function
        result = get_term_labels_from_cache(
            [
                "http://purl.obolibrary.org/obo/HP_0001627",
                "http://purl.obolibrary.org/obo/HP_9999999"
            ],
            "2026-01-08",
            "v2024-10-01"
        )

        # Verify - only found term should be in result
        assert len(result) == 1
        assert result["http://purl.obolibrary.org/obo/HP_0001627"] == "Abnormal heart morphology"
        assert "http://purl.obolibrary.org/obo/HP_9999999" not in result

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_unknown_ontology_prefix(self, mock_get_table):
        """Test with unknown ontology prefix (should be skipped)."""
        from phebee.utils.dynamodb_cache import get_term_labels_from_cache

        # Mock table
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        # Call function with unknown prefix
        result = get_term_labels_from_cache(
            ["http://purl.obolibrary.org/obo/UNKNOWN_0001234"],
            "2026-01-08",
            "v2024-10-01"
        )

        # Verify - unknown term should be skipped
        assert len(result) == 0
        mock_table.query.assert_not_called()
