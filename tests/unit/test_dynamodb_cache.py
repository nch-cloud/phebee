"""
Unit tests for DynamoDB cache functions.

Tests the cache query functions using mocked DynamoDB responses.
"""

import os
import pytest
from unittest.mock import Mock, patch, MagicMock
from phebee.utils.dynamodb_cache import (
    build_term_path_pk,
    build_term_direct_pk,
    build_ancestor_path_sk,
    parse_term_id_from_iri,
    get_term_ancestor_paths,
    get_term_label,
    get_term_labels_from_cache,
    build_project_data_pk,
    build_project_data_sk,
    write_subject_term_links_batch
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


class TestProjectDataKeyBuilders:
    """Test partition and sort key builders for project data (subject-term links)."""

    def test_build_project_data_pk(self):
        """Test building partition key from project_id."""
        pk = build_project_data_pk("project-123")
        assert pk == "PROJECT#project-123"

    def test_build_project_data_pk_uuid(self):
        """Test building partition key with UUID project ID."""
        pk = build_project_data_pk("550e8400-e29b-41d4-a716-446655440000")
        assert pk == "PROJECT#550e8400-e29b-41d4-a716-446655440000"

    def test_build_project_data_sk_single_ancestor(self):
        """Test building sort key with single ancestor in path."""
        sk = build_project_data_sk(["HP_0000001"], "subject-abc")
        assert sk == "HP_0000001||subject-abc"

    def test_build_project_data_sk_multi_ancestor(self):
        """Test building sort key with multiple ancestors in path."""
        sk = build_project_data_sk(["HP_0000001", "HP_0000118", "HP_0001627"], "subject-abc")
        assert sk == "HP_0000001|HP_0000118|HP_0001627||subject-abc"

    def test_build_project_data_sk_empty_path(self):
        """Test building sort key with empty ancestor path (root term)."""
        sk = build_project_data_sk([], "subject-abc")
        assert sk == "||subject-abc"

    def test_build_project_data_sk_uuid_subject(self):
        """Test building sort key with UUID subject ID."""
        sk = build_project_data_sk(
            ["HP_0000001", "HP_0000118"],
            "550e8400-e29b-41d4-a716-446655440000"
        )
        assert sk == "HP_0000001|HP_0000118||550e8400-e29b-41d4-a716-446655440000"


class TestWriteSubjectTermLinksBatch:
    """Test batch writing subject-term links to cache."""

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_write_empty_list(self, mock_get_table):
        """Test writing empty list of links."""
        count = write_subject_term_links_batch([])
        assert count == 0
        mock_get_table.assert_not_called()

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_write_single_link_single_path(self, mock_get_table):
        """Test writing single link with single ancestor path."""
        # Mock table
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        # Create test link
        links = [{
            "project_id": "project-123",
            "subject_id": "subject-abc",
            "term_id": "HP_0001627",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "term_label": "Abnormal heart morphology",
            "ancestor_paths": [["HP_0000001", "HP_0000118", "HP_0001627"]],
            "qualifiers": {"negated": "false", "family": "false"},
            "termlink_id": "termlink-hash-123"
        }]

        # Call function
        count = write_subject_term_links_batch(links)

        # Verify count
        assert count == 1

        # Verify batch_write_item was called
        mock_table.batch_writer.assert_called_once()

        # Get the context manager
        mock_batch_writer = mock_table.batch_writer.return_value.__enter__.return_value

        # Verify put_item was called with correct structure
        mock_batch_writer.put_item.assert_called_once()
        call_args = mock_batch_writer.put_item.call_args[1]
        item = call_args["Item"]

        assert item["PK"] == "PROJECT#project-123"
        assert item["SK"] == "HP_0000001|HP_0000118|HP_0001627||subject-abc"
        assert item["termlink_id"] == "termlink-hash-123"
        assert item["term_iri"] == "http://purl.obolibrary.org/obo/HP_0001627"
        assert item["term_label"] == "Abnormal heart morphology"
        assert item["qualifiers"] == {"negated": "false", "family": "false"}

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_write_single_link_multiple_paths(self, mock_get_table):
        """Test writing single link with multiple inheritance (multiple ancestor paths)."""
        # Mock table
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        # Create test link with multiple paths
        links = [{
            "project_id": "project-123",
            "subject_id": "subject-abc",
            "term_id": "HP_0001627",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "term_label": "Abnormal heart morphology",
            "ancestor_paths": [
                ["HP_0000001", "HP_0000118", "HP_0001627"],
                ["HP_0000001", "HP_0001626", "HP_0001627"]
            ],
            "qualifiers": {"negated": "false"},
            "termlink_id": "termlink-hash-123"
        }]

        # Call function
        count = write_subject_term_links_batch(links)

        # Verify count - should be 2 (one per path)
        assert count == 2

        # Verify batch_write_item was called
        mock_table.batch_writer.assert_called_once()

        # Get the context manager
        mock_batch_writer = mock_table.batch_writer.return_value.__enter__.return_value

        # Verify put_item was called twice (once per path)
        assert mock_batch_writer.put_item.call_count == 2

        # Check both calls had correct sort keys
        call_args_list = mock_batch_writer.put_item.call_args_list
        items = [call[1]["Item"] for call in call_args_list]

        # Both should have same PK and termlink_id
        assert all(item["PK"] == "PROJECT#project-123" for item in items)
        assert all(item["termlink_id"] == "termlink-hash-123" for item in items)

        # But different sort keys (one per ancestor path)
        sort_keys = [item["SK"] for item in items]
        assert "HP_0000001|HP_0000118|HP_0001627||subject-abc" in sort_keys
        assert "HP_0000001|HP_0001626|HP_0001627||subject-abc" in sort_keys

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_write_multiple_links(self, mock_get_table):
        """Test writing multiple links in a batch."""
        # Mock table
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        # Create test links
        links = [
            {
                "project_id": "project-123",
                "subject_id": "subject-abc",
                "term_id": "HP_0001627",
                "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
                "term_label": "Abnormal heart morphology",
                "ancestor_paths": [["HP_0000001", "HP_0000118", "HP_0001627"]],
                "qualifiers": {"negated": "false"},
                "termlink_id": "termlink-hash-1"
            },
            {
                "project_id": "project-123",
                "subject_id": "subject-xyz",
                "term_id": "HP_0000234",
                "term_iri": "http://purl.obolibrary.org/obo/HP_0000234",
                "term_label": "Abnormality of the head",
                "ancestor_paths": [["HP_0000001", "HP_0000152", "HP_0000234"]],
                "qualifiers": {"negated": "false"},
                "termlink_id": "termlink-hash-2"
            }
        ]

        # Call function
        count = write_subject_term_links_batch(links)

        # Verify count
        assert count == 2

        # Verify batch_write_item was called
        mock_table.batch_writer.assert_called_once()

        # Get the context manager
        mock_batch_writer = mock_table.batch_writer.return_value.__enter__.return_value

        # Verify put_item was called twice
        assert mock_batch_writer.put_item.call_count == 2

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_write_with_empty_qualifiers(self, mock_get_table):
        """Test writing link with empty qualifiers dict."""
        # Mock table
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        # Create test link with empty qualifiers
        links = [{
            "project_id": "project-123",
            "subject_id": "subject-abc",
            "term_id": "HP_0001627",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "term_label": "Abnormal heart morphology",
            "ancestor_paths": [["HP_0000001", "HP_0000118", "HP_0001627"]],
            "qualifiers": {},
            "termlink_id": "termlink-hash-123"
        }]

        # Call function
        count = write_subject_term_links_batch(links)

        # Verify count
        assert count == 1

        # Verify qualifiers were written as empty dict
        mock_batch_writer = mock_table.batch_writer.return_value.__enter__.return_value
        call_args = mock_batch_writer.put_item.call_args[1]
        assert call_args["Item"]["qualifiers"] == {}

    @patch('phebee.utils.dynamodb_cache._get_cache_table')
    def test_write_large_batch(self, mock_get_table):
        """Test writing more than 25 links (DynamoDB batch limit)."""
        # Mock table
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        # Create 30 test links
        links = []
        for i in range(30):
            links.append({
                "project_id": "project-123",
                "subject_id": f"subject-{i}",
                "term_id": "HP_0001627",
                "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
                "term_label": "Abnormal heart morphology",
                "ancestor_paths": [["HP_0000001", "HP_0000118", "HP_0001627"]],
                "qualifiers": {"negated": "false"},
                "termlink_id": f"termlink-hash-{i}"
            })

        # Call function
        count = write_subject_term_links_batch(links)

        # Verify count
        assert count == 30

        # Verify batch_write_item was called multiple times due to 25-item limit
        # Should be called at least twice (25 + 5)
        assert mock_table.batch_writer.call_count >= 2


class TestGetProjectsForSubject:
    """Test querying all projects that contain a subject."""

    @patch.dict(os.environ, {'DYNAMODB_TABLE_NAME': 'test-table'})
    @patch('boto3.resource')
    def test_get_projects_single_project(self, mock_boto_resource):
        """Test getting projects for subject that belongs to one project."""
        from phebee.utils.sparql import get_projects_for_subject

        # Mock DynamoDB table
        mock_dynamodb = MagicMock()
        mock_table = MagicMock()
        mock_boto_resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        # Mock query response - subject in one project
        subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/subject-abc-123"
        mock_table.query.return_value = {
            "Items": [
                {
                    "PK": f"SUBJECT#{subject_iri}",
                    "SK": "PROJECT#project-1#SUBJECT#subj-001"
                }
            ]
        }

        # Call function
        project_ids = get_projects_for_subject("subject-abc-123")

        # Verify query was called correctly
        mock_table.query.assert_called_once()
        call_kwargs = mock_table.query.call_args[1]
        assert call_kwargs["KeyConditionExpression"] == "PK = :pk"
        assert call_kwargs["ExpressionAttributeValues"] == {":pk": f"SUBJECT#{subject_iri}"}

        # Verify result
        assert project_ids == ["project-1"]

    @patch.dict(os.environ, {'DYNAMODB_TABLE_NAME': 'test-table'})
    @patch('boto3.resource')
    def test_get_projects_multiple_projects(self, mock_boto_resource):
        """Test getting projects for subject that belongs to multiple projects."""
        from phebee.utils.sparql import get_projects_for_subject

        # Mock DynamoDB table
        mock_dynamodb = MagicMock()
        mock_table = MagicMock()
        mock_boto_resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        # Mock query response - subject in three projects
        subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/subject-abc-123"
        mock_table.query.return_value = {
            "Items": [
                {
                    "PK": f"SUBJECT#{subject_iri}",
                    "SK": "PROJECT#project-1#SUBJECT#subj-001"
                },
                {
                    "PK": f"SUBJECT#{subject_iri}",
                    "SK": "PROJECT#project-2#SUBJECT#subj-042"
                },
                {
                    "PK": f"SUBJECT#{subject_iri}",
                    "SK": "PROJECT#project-3#SUBJECT#subj-999"
                }
            ]
        }

        # Call function
        project_ids = get_projects_for_subject("subject-abc-123")

        # Verify result contains all three projects
        assert len(project_ids) == 3
        assert "project-1" in project_ids
        assert "project-2" in project_ids
        assert "project-3" in project_ids

    @patch.dict(os.environ, {'DYNAMODB_TABLE_NAME': 'test-table'})
    @patch('boto3.resource')
    def test_get_projects_no_projects(self, mock_boto_resource):
        """Test getting projects for subject that doesn't belong to any project."""
        from phebee.utils.sparql import get_projects_for_subject

        # Mock DynamoDB table
        mock_dynamodb = MagicMock()
        mock_table = MagicMock()
        mock_boto_resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        # Mock query response - no items
        mock_table.query.return_value = {"Items": []}

        # Call function
        project_ids = get_projects_for_subject("subject-orphan")

        # Verify result is empty
        assert project_ids == []

    @patch.dict(os.environ, {}, clear=True)
    def test_get_projects_no_table_name_env(self):
        """Test function returns empty list when DYNAMODB_TABLE_NAME not set."""
        from phebee.utils.sparql import get_projects_for_subject

        # Call function without DYNAMODB_TABLE_NAME env var
        project_ids = get_projects_for_subject("subject-abc")

        # Should return empty list and log warning
        assert project_ids == []
