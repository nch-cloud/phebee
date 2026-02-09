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
    get_term_label
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
