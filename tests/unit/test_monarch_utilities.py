"""
Unit tests for Monarch Knowledge Graph API integration.
"""
import pytest
from unittest.mock import patch, Mock
import json
from phebee.utils.monarch import get_associated_terms


class TestGetAssociatedTerms:
    """Test Monarch API association term fetching."""

    @patch('phebee.utils.monarch.requests.get')
    def test_success_single_page(self, mock_get):
        """Test successful fetch with single page of results."""
        # Mock response with 3 associations
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "total": 3,
            "items": [
                {"object": "HP:0001249"},
                {"object": "HP:0001166"},
                {"object": "HP:0002650"}
            ]
        }
        mock_get.return_value = mock_response

        result = get_associated_terms("MONDO:0007947")

        assert result == ["HP:0001166", "HP:0001249", "HP:0002650"]  # Sorted
        mock_get.assert_called_once()
        # Verify URL parameters
        call_args = mock_get.call_args
        assert call_args[1]['params']['entity'] == "MONDO:0007947"
        assert call_args[1]['params']['object_namespace'] == ["HP", "MONDO"]
        assert call_args[1]['timeout'] == 30

    @patch('phebee.utils.monarch.requests.get')
    def test_success_multiple_pages(self, mock_get):
        """Test successful fetch with pagination."""
        # Mock first request (gets total count)
        mock_response1 = Mock()
        mock_response1.status_code = 200
        mock_response1.json.return_value = {
            "total": 5,  # More than single page (limit=3)
            "items": [
                {"object": "HP:0001249"},
                {"object": "HP:0001166"},
                {"object": "HP:0002650"}
            ]
        }

        # Mock second request (next page)
        mock_response2 = Mock()
        mock_response2.status_code = 200
        mock_response2.json.return_value = {
            "total": 5,
            "items": [
                {"object": "HP:0001519"},
                {"object": "MONDO:0005148"}
            ]
        }

        mock_get.side_effect = [mock_response1, mock_response2]

        result = get_associated_terms("MONDO:0007947", limit=3)

        assert len(result) == 5
        assert "HP:0001249" in result
        assert "MONDO:0005148" in result
        assert mock_get.call_count == 2
        # Verify second call has offset
        second_call_params = mock_get.call_args_list[1][1]['params']
        assert second_call_params['offset'] == 3

    @patch('phebee.utils.monarch.requests.get')
    def test_deduplication(self, mock_get):
        """Test that duplicate terms are deduplicated."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "total": 4,
            "items": [
                {"object": "HP:0001249"},
                {"object": "HP:0001249"},  # Duplicate
                {"object": "HP:0001166"},
                {"object": "HP:0001249"}   # Another duplicate
            ]
        }
        mock_get.return_value = mock_response

        result = get_associated_terms("MONDO:0007947")

        assert len(result) == 2
        assert result == ["HP:0001166", "HP:0001249"]  # Sorted and deduplicated

    @patch('phebee.utils.monarch.requests.get')
    def test_empty_results(self, mock_get):
        """Test handling of empty results."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "total": 0,
            "items": []
        }
        mock_get.return_value = mock_response

        result = get_associated_terms("MONDO:9999999")

        assert result == []

    @patch('phebee.utils.monarch.requests.get')
    def test_missing_object_field(self, mock_get):
        """Test handling of items without object field."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "total": 3,
            "items": [
                {"object": "HP:0001249"},
                {"subject": "MONDO:0007947"},  # No object field
                {"object": "HP:0001166"}
            ]
        }
        mock_get.return_value = mock_response

        result = get_associated_terms("MONDO:0007947")

        # Should only include items with object field
        assert len(result) == 2
        assert result == ["HP:0001166", "HP:0001249"]

    @patch('phebee.utils.monarch.requests.get')
    def test_http_error_status(self, mock_get):
        """Test handling of HTTP error status codes."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_get.return_value = mock_response

        with pytest.raises(Exception, match="Monarch API request failed.*status=500"):
            get_associated_terms("MONDO:0007947")

    @patch('phebee.utils.monarch.requests.get')
    def test_http_404_status(self, mock_get):
        """Test handling of 404 status code."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        mock_get.return_value = mock_response

        with pytest.raises(Exception, match="Monarch API request failed.*status=404"):
            get_associated_terms("MONDO:9999999")

    @patch('phebee.utils.monarch.requests.get')
    def test_timeout(self, mock_get):
        """Test handling of request timeout."""
        from requests.exceptions import Timeout
        mock_get.side_effect = Timeout("Connection timeout")

        with pytest.raises(Exception, match="Monarch API request timed out"):
            get_associated_terms("MONDO:0007947")

    @patch('phebee.utils.monarch.requests.get')
    def test_network_error(self, mock_get):
        """Test handling of network errors."""
        from requests.exceptions import ConnectionError
        mock_get.side_effect = ConnectionError("Network unreachable")

        with pytest.raises(ConnectionError, match="Network unreachable"):
            get_associated_terms("MONDO:0007947")

    @patch('phebee.utils.monarch.requests.get')
    def test_json_decode_error(self, mock_get):
        """Test handling of invalid JSON response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        mock_get.return_value = mock_response

        with pytest.raises(json.JSONDecodeError, match="Failed to parse Monarch API response"):
            get_associated_terms("MONDO:0007947")

    @patch('phebee.utils.monarch.requests.get')
    def test_mixed_ontology_sources(self, mock_get):
        """Test fetching associations from multiple ontology sources."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "total": 4,
            "items": [
                {"object": "HP:0001249"},  # HPO
                {"object": "HP:0001166"},  # HPO
                {"object": "MONDO:0005148"},  # MONDO
                {"object": "MONDO:0007947"}   # MONDO
            ]
        }
        mock_get.return_value = mock_response

        result = get_associated_terms("HP:0000001")

        assert len(result) == 4
        # Should include both HPO and MONDO terms
        hpo_terms = [t for t in result if t.startswith('HP:')]
        mondo_terms = [t for t in result if t.startswith('MONDO:')]
        assert len(hpo_terms) == 2
        assert len(mondo_terms) == 2

    @patch('phebee.utils.monarch.requests.get')
    def test_pagination_error_on_second_page(self, mock_get):
        """Test error handling when pagination fails on subsequent page."""
        # First request succeeds
        mock_response1 = Mock()
        mock_response1.status_code = 200
        mock_response1.json.return_value = {
            "total": 5,
            "items": [
                {"object": "HP:0001249"},
                {"object": "HP:0001166"}
            ]
        }

        # Second request fails
        mock_response2 = Mock()
        mock_response2.status_code = 500
        mock_response2.text = "Internal Server Error"

        mock_get.side_effect = [mock_response1, mock_response2]

        with pytest.raises(Exception, match="Monarch API request failed on page 2"):
            get_associated_terms("MONDO:0007947", limit=2)

    @patch('phebee.utils.monarch.requests.get')
    def test_custom_limit(self, mock_get):
        """Test using custom limit parameter."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "total": 2,
            "items": [
                {"object": "HP:0001249"},
                {"object": "HP:0001166"}
            ]
        }
        mock_get.return_value = mock_response

        result = get_associated_terms("MONDO:0007947", limit=500)

        assert len(result) == 2
        # Verify custom limit was used
        call_params = mock_get.call_args[1]['params']
        assert call_params['limit'] == 500
