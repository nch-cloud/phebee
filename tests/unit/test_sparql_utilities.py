"""
Unit tests for SPARQL utility functions.
"""
import pytest
from phebee.utils.sparql import flatten_response, flatten_sparql_results


class TestFlattenResponse:
    """Test dictionary merging with conflict detection."""

    def test_valid_merge_no_overlap(self):
        """Test merging dicts with no overlapping keys."""
        fixed = {"id": "123", "type": "subject"}
        properties = {"name": "John", "age": 30}

        result = flatten_response(fixed, properties)

        assert result == {
            "id": "123",
            "type": "subject",
            "name": "John",
            "age": 30
        }

    def test_empty_fixed_dict(self):
        """Test with empty fixed dict."""
        fixed = {}
        properties = {"name": "John", "age": 30}

        result = flatten_response(fixed, properties)

        assert result == {"name": "John", "age": 30}

    def test_empty_properties_dict(self):
        """Test with empty properties dict."""
        fixed = {"id": "123", "type": "subject"}
        properties = {}

        result = flatten_response(fixed, properties)

        assert result == {"id": "123", "type": "subject"}

    def test_both_empty(self):
        """Test with both dicts empty."""
        result = flatten_response({}, {})
        assert result == {}

    def test_conflicting_keys_raises_error(self):
        """Test that overlapping keys raise ValueError."""
        fixed = {"id": "123", "name": "Fixed Name"}
        properties = {"name": "Property Name", "age": 30}

        with pytest.raises(ValueError, match="Property keys conflict with fixed keys"):
            flatten_response(fixed, properties)

    def test_multiple_conflicting_keys(self):
        """Test multiple overlapping keys in error message."""
        fixed = {"id": "123", "name": "John", "type": "subject"}
        properties = {"name": "Jane", "type": "patient", "age": 30}

        with pytest.raises(ValueError) as exc_info:
            flatten_response(fixed, properties)

        # Should mention the conflicting keys
        error_msg = str(exc_info.value)
        assert "name" in error_msg or "type" in error_msg

    def test_preserves_order(self):
        """Test that fixed keys come before properties keys."""
        fixed = {"a": 1, "b": 2}
        properties = {"c": 3, "d": 4}

        result = flatten_response(fixed, properties)
        keys = list(result.keys())

        # Fixed keys should appear first
        assert keys[:2] == ["a", "b"]
        assert keys[2:] == ["c", "d"]

    def test_with_none_values(self):
        """Test merging dicts with None values."""
        fixed = {"id": "123", "type": None}
        properties = {"name": None, "age": 30}

        result = flatten_response(fixed, properties)

        assert result == {
            "id": "123",
            "type": None,
            "name": None,
            "age": 30
        }

    def test_with_nested_dicts(self):
        """Test with nested dictionary values."""
        fixed = {"id": "123", "metadata": {"version": "1.0"}}
        properties = {"data": {"value": 42}}

        result = flatten_response(fixed, properties)

        assert result["id"] == "123"
        assert result["metadata"] == {"version": "1.0"}
        assert result["data"] == {"value": 42}

    def test_with_list_values(self):
        """Test with list values."""
        fixed = {"id": "123", "tags": ["a", "b"]}
        properties = {"values": [1, 2, 3]}

        result = flatten_response(fixed, properties)

        assert result["tags"] == ["a", "b"]
        assert result["values"] == [1, 2, 3]


class TestFlattenSparqlResults:
    """Test SPARQL JSON result flattening."""

    def test_simple_results(self):
        """Test flattening simple SPARQL results."""
        sparql_json = {
            "head": {"vars": ["subject", "predicate", "object"]},
            "results": {
                "bindings": [
                    {
                        "subject": {"value": "http://example.org/s1"},
                        "predicate": {"value": "http://example.org/p1"},
                        "object": {"value": "value1"}
                    },
                    {
                        "subject": {"value": "http://example.org/s2"},
                        "predicate": {"value": "http://example.org/p2"},
                        "object": {"value": "value2"}
                    }
                ]
            }
        }

        result = flatten_sparql_results(sparql_json)

        assert len(result) == 2
        assert result[0] == {
            "subject": "http://example.org/s1",
            "predicate": "http://example.org/p1",
            "object": "value1"
        }
        assert result[1]["subject"] == "http://example.org/s2"

    def test_empty_results(self):
        """Test with empty SPARQL results."""
        sparql_json = {
            "head": {"vars": ["subject"]},
            "results": {"bindings": []}
        }

        result = flatten_sparql_results(sparql_json)
        assert result == []

    def test_missing_variables(self):
        """Test when some variables are not bound in results."""
        sparql_json = {
            "head": {"vars": ["subject", "label", "comment"]},
            "results": {
                "bindings": [
                    {
                        "subject": {"value": "http://example.org/s1"},
                        "label": {"value": "Label 1"}
                        # comment is missing
                    }
                ]
            }
        }

        result = flatten_sparql_results(sparql_json)

        assert len(result) == 1
        assert result[0]["subject"] == "http://example.org/s1"
        assert result[0]["label"] == "Label 1"
        assert result[0]["comment"] is None

    def test_with_datatypes(self):
        """Test including datatypes in results."""
        sparql_json = {
            "head": {"vars": ["subject", "age"]},
            "results": {
                "bindings": [
                    {
                        "subject": {"value": "http://example.org/s1"},
                        "age": {
                            "value": "30",
                            "datatype": "http://www.w3.org/2001/XMLSchema#integer"
                        }
                    }
                ]
            }
        }

        result = flatten_sparql_results(sparql_json, include_datatype=True)

        assert len(result) == 1
        assert result[0]["age"] == "30"
        assert result[0]["age_datatype"] == "http://www.w3.org/2001/XMLSchema#integer"

    def test_without_datatypes(self):
        """Test that datatypes are excluded by default."""
        sparql_json = {
            "head": {"vars": ["subject", "age"]},
            "results": {
                "bindings": [
                    {
                        "subject": {"value": "http://example.org/s1"},
                        "age": {
                            "value": "30",
                            "datatype": "http://www.w3.org/2001/XMLSchema#integer"
                        }
                    }
                ]
            }
        }

        result = flatten_sparql_results(sparql_json, include_datatype=False)

        assert "age_datatype" not in result[0]
        assert result[0]["age"] == "30"

    def test_group_by_subject(self):
        """Test grouping results by subject."""
        sparql_json = {
            "head": {"vars": ["subjectIRI", "property", "value"]},
            "results": {
                "bindings": [
                    {
                        "subjectIRI": {"value": "http://example.org/s1"},
                        "property": {"value": "name"},
                        "value": {"value": "John"}
                    },
                    {
                        "subjectIRI": {"value": "http://example.org/s1"},
                        "property": {"value": "age"},
                        "value": {"value": "30"}
                    },
                    {
                        "subjectIRI": {"value": "http://example.org/s2"},
                        "property": {"value": "name"},
                        "value": {"value": "Jane"}
                    }
                ]
            }
        }

        result = flatten_sparql_results(sparql_json, group_subjects=True)

        assert isinstance(result, dict)
        assert "http://example.org/s1" in result
        assert "http://example.org/s2" in result
        assert len(result["http://example.org/s1"]) == 2
        assert len(result["http://example.org/s2"]) == 1

    def test_group_without_subject_iri(self):
        """Test grouping when subjectIRI is not in results."""
        sparql_json = {
            "head": {"vars": ["property", "value"]},
            "results": {
                "bindings": [
                    {
                        "property": {"value": "name"},
                        "value": {"value": "John"}
                    }
                ]
            }
        }

        result = flatten_sparql_results(sparql_json, group_subjects=True)

        # Should group by None if subjectIRI is missing
        assert isinstance(result, dict)
        assert None in result

    def test_no_head_section(self):
        """Test with malformed SPARQL JSON (no head section)."""
        sparql_json = {
            "results": {
                "bindings": [
                    {"subject": {"value": "http://example.org/s1"}}
                ]
            }
        }

        result = flatten_sparql_results(sparql_json)

        # Should handle gracefully with empty vars
        assert result == [{}]

    def test_no_results_section(self):
        """Test with malformed SPARQL JSON (no results section)."""
        sparql_json = {
            "head": {"vars": ["subject"]}
        }

        result = flatten_sparql_results(sparql_json)

        # Should return empty list
        assert result == []

    def test_real_world_example(self):
        """Test with realistic PheBee SPARQL response."""
        sparql_json = {
            "head": {"vars": ["termIRI", "termLabel", "evidenceCount"]},
            "results": {
                "bindings": [
                    {
                        "termIRI": {"value": "http://purl.obolibrary.org/obo/HP_0001250"},
                        "termLabel": {"value": "Seizure"},
                        "evidenceCount": {
                            "value": "5",
                            "datatype": "http://www.w3.org/2001/XMLSchema#integer"
                        }
                    },
                    {
                        "termIRI": {"value": "http://purl.obolibrary.org/obo/HP_0001627"},
                        "termLabel": {"value": "Abnormal heart morphology"},
                        "evidenceCount": {
                            "value": "3",
                            "datatype": "http://www.w3.org/2001/XMLSchema#integer"
                        }
                    }
                ]
            }
        }

        result = flatten_sparql_results(sparql_json)

        assert len(result) == 2
        assert result[0]["termIRI"] == "http://purl.obolibrary.org/obo/HP_0001250"
        assert result[0]["termLabel"] == "Seizure"
        assert result[0]["evidenceCount"] == "5"
        assert result[1]["termLabel"] == "Abnormal heart morphology"

    def test_multiple_datatypes(self):
        """Test with multiple fields having datatypes."""
        sparql_json = {
            "head": {"vars": ["subject", "age", "score"]},
            "results": {
                "bindings": [
                    {
                        "subject": {"value": "http://example.org/s1"},
                        "age": {
                            "value": "30",
                            "datatype": "http://www.w3.org/2001/XMLSchema#integer"
                        },
                        "score": {
                            "value": "95.5",
                            "datatype": "http://www.w3.org/2001/XMLSchema#decimal"
                        }
                    }
                ]
            }
        }

        result = flatten_sparql_results(sparql_json, include_datatype=True)

        assert result[0]["age"] == "30"
        assert result[0]["age_datatype"] == "http://www.w3.org/2001/XMLSchema#integer"
        assert result[0]["score"] == "95.5"
        assert result[0]["score_datatype"] == "http://www.w3.org/2001/XMLSchema#decimal"

    def test_datatype_without_value(self):
        """Test handling of datatype field when no datatype is present."""
        sparql_json = {
            "head": {"vars": ["subject", "label"]},
            "results": {
                "bindings": [
                    {
                        "subject": {"value": "http://example.org/s1"},
                        "label": {"value": "Label without datatype"}
                    }
                ]
            }
        }

        result = flatten_sparql_results(sparql_json, include_datatype=True)

        # Should not include datatype field if not present in source
        assert "label_datatype" not in result[0]
        assert result[0]["label"] == "Label without datatype"
