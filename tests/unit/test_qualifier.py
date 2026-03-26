"""
Unit tests for phebee.utils.qualifier module.

Tests the Qualifier dataclass and related functions for parsing, serialization,
and normalization of qualifiers.
"""

import pytest
from phebee.utils.qualifier import (
    Qualifier,
    normalize_qualifiers,
    qualifiers_to_string_list
)


class TestQualifierBasicOperations:
    """Test basic Qualifier creation and conversion."""

    def test_create_qualifier(self):
        """Test creating a Qualifier object."""
        q = Qualifier(type="negated", value="true")
        assert q.type == "negated"
        assert q.value == "true"

    def test_qualifier_immutable(self):
        """Test that Qualifier is immutable (frozen dataclass)."""
        q = Qualifier(type="negated", value="true")
        with pytest.raises(AttributeError):
            q.type = "hypothetical"

    def test_to_string(self):
        """Test converting Qualifier to string format."""
        q = Qualifier(type="onset", value="HP:0003593")
        assert q.to_string() == "onset:HP:0003593"

    def test_to_string_with_colon_in_value(self):
        """Test that colons in value are preserved."""
        q = Qualifier(type="severity", value="http://example.org/mild")
        assert q.to_string() == "severity:http://example.org/mild"


class TestQualifierFromString:
    """Test parsing Qualifiers from string format."""

    def test_from_string_internal_qualifier(self):
        """Test parsing internal qualifier like 'negated:true'."""
        q = Qualifier.from_string("negated:true")
        assert q.type == "negated"
        assert q.value == "true"

    def test_from_string_with_iri_value(self):
        """Test parsing qualifier with IRI value."""
        q = Qualifier.from_string("onset:HP:0003593")
        assert q.type == "onset"
        assert q.value == "HP:0003593"

    def test_from_string_external_iri_with_value(self):
        """Test parsing external IRI with value after colon."""
        q = Qualifier.from_string("http://purl.obolibrary.org/obo/HP_0012823:present")
        assert q.type == "http://purl.obolibrary.org/obo/HP_0012823"
        assert q.value == "present"

    def test_from_string_external_iri_without_value(self):
        """Test parsing external IRI without value (defaults to 'true')."""
        q = Qualifier.from_string("http://purl.obolibrary.org/obo/HP_0012823")
        assert q.type == "http://purl.obolibrary.org/obo/HP_0012823"
        assert q.value == "true"

    def test_from_string_bare_name(self):
        """Test parsing bare qualifier name (defaults to 'true')."""
        q = Qualifier.from_string("negated")
        assert q.type == "negated"
        assert q.value == "true"

    def test_from_string_empty_raises_error(self):
        """Test that empty string raises ValueError."""
        with pytest.raises(ValueError):
            Qualifier.from_string("")

    def test_from_string_round_trip(self):
        """Test that to_string and from_string are inverses."""
        original = Qualifier(type="onset", value="HP:0003593")
        string_form = original.to_string()
        parsed = Qualifier.from_string(string_form)
        assert parsed == original


class TestQualifierStorageDict:
    """Test conversion to/from storage dict format."""

    def test_to_storage_dict(self):
        """Test converting Qualifier to storage dict."""
        q = Qualifier(type="onset", value="HP:0003593")
        d = q.to_storage_dict()
        assert d == {
            'qualifier_type': 'onset',
            'qualifier_value': 'HP:0003593'
        }

    def test_from_dict(self):
        """Test creating Qualifier from storage dict."""
        d = {'qualifier_type': 'negated', 'qualifier_value': 'true'}
        q = Qualifier.from_dict(d)
        assert q.type == "negated"
        assert q.value == "true"

    def test_storage_dict_round_trip(self):
        """Test that to_storage_dict and from_dict are inverses."""
        original = Qualifier(type="severity", value="mild")
        dict_form = original.to_storage_dict()
        parsed = Qualifier.from_dict(dict_form)
        assert parsed == original


class TestQualifierIsActive:
    """Test the is_active() method for filtering inactive qualifiers."""

    def test_is_active_true_value(self):
        """Test that 'true' value is active."""
        q = Qualifier(type="negated", value="true")
        assert q.is_active() is True

    def test_is_active_false_value(self):
        """Test that 'false' value is inactive."""
        q = Qualifier(type="negated", value="false")
        assert q.is_active() is False

    def test_is_active_false_case_insensitive(self):
        """Test that 'FALSE' and 'False' are also inactive."""
        assert Qualifier(type="negated", value="FALSE").is_active() is False
        assert Qualifier(type="negated", value="False").is_active() is False

    def test_is_active_zero_value(self):
        """Test that '0' value is inactive."""
        q = Qualifier(type="severity", value="0")
        assert q.is_active() is False

    def test_is_active_other_values(self):
        """Test that non-false/0 values are active."""
        assert Qualifier(type="onset", value="HP:0003593").is_active() is True
        assert Qualifier(type="severity", value="mild").is_active() is True
        assert Qualifier(type="negated", value="1").is_active() is True


class TestNormalizeQualifiers:
    """Test normalize_qualifiers function."""

    def test_normalize_empty_list(self):
        """Test normalizing empty list."""
        assert normalize_qualifiers([]) == []

    def test_normalize_none(self):
        """Test normalizing None."""
        assert normalize_qualifiers(None) == []

    def test_normalize_filters_inactive(self):
        """Test that normalize_qualifiers filters out inactive qualifiers."""
        qualifiers = [
            Qualifier(type="negated", value="true"),
            Qualifier(type="family", value="false"),
            Qualifier(type="onset", value="HP:0003593")
        ]
        normalized = normalize_qualifiers(qualifiers)
        assert len(normalized) == 2
        assert Qualifier(type="family", value="false") not in normalized

    def test_normalize_sorts_deterministically(self):
        """Test that qualifiers are sorted for deterministic ordering."""
        qualifiers = [
            Qualifier(type="severity", value="mild"),
            Qualifier(type="negated", value="true"),
            Qualifier(type="onset", value="HP:0003593")
        ]
        normalized = normalize_qualifiers(qualifiers)
        # Should be sorted by string representation
        expected = [
            Qualifier(type="negated", value="true"),
            Qualifier(type="onset", value="HP:0003593"),
            Qualifier(type="severity", value="mild")
        ]
        assert normalized == expected

    def test_normalize_all_inactive(self):
        """Test normalizing list with all inactive qualifiers."""
        qualifiers = [
            Qualifier(type="negated", value="false"),
            Qualifier(type="family", value="0")
        ]
        normalized = normalize_qualifiers(qualifiers)
        assert normalized == []


class TestQualifiersToStringList:
    """Test qualifiers_to_string_list function."""

    def test_qualifiers_to_string_list_empty(self):
        """Test converting empty list."""
        assert qualifiers_to_string_list([]) == []

    def test_qualifiers_to_string_list(self):
        """Test converting qualifiers to string list."""
        qualifiers = [
            Qualifier(type="onset", value="HP:0003593"),
            Qualifier(type="negated", value="true")
        ]
        strings = qualifiers_to_string_list(qualifiers)
        assert strings == ["onset:HP:0003593", "negated:true"]

    def test_qualifiers_to_string_list_preserves_order(self):
        """Test that conversion preserves order."""
        qualifiers = [
            Qualifier(type="z", value="last"),
            Qualifier(type="a", value="first")
        ]
        strings = qualifiers_to_string_list(qualifiers)
        assert strings == ["z:last", "a:first"]


class TestQualifierEquality:
    """Test Qualifier equality and hashing."""

    def test_qualifier_equality(self):
        """Test that Qualifiers with same type/value are equal."""
        q1 = Qualifier(type="onset", value="HP:0003593")
        q2 = Qualifier(type="onset", value="HP:0003593")
        assert q1 == q2

    def test_qualifier_inequality_type(self):
        """Test that Qualifiers with different types are not equal."""
        q1 = Qualifier(type="onset", value="HP:0003593")
        q2 = Qualifier(type="severity", value="HP:0003593")
        assert q1 != q2

    def test_qualifier_inequality_value(self):
        """Test that Qualifiers with different values are not equal."""
        q1 = Qualifier(type="onset", value="HP:0003593")
        q2 = Qualifier(type="onset", value="HP:0011462")
        assert q1 != q2

    def test_qualifier_hashable(self):
        """Test that Qualifiers can be used in sets/dicts (hashable)."""
        q1 = Qualifier(type="onset", value="HP:0003593")
        q2 = Qualifier(type="onset", value="HP:0003593")
        q3 = Qualifier(type="negated", value="true")

        qualifiers_set = {q1, q2, q3}
        assert len(qualifiers_set) == 2  # q1 and q2 are same


class TestQualifierEdgeCases:
    """Test edge cases and error handling."""

    def test_qualifier_with_empty_type(self):
        """Test creating Qualifier with empty type (allowed but unusual)."""
        q = Qualifier(type="", value="HP:0003593")
        assert q.type == ""
        assert q.value == "HP:0003593"

    def test_qualifier_with_empty_value(self):
        """Test creating Qualifier with empty value."""
        q = Qualifier(type="onset", value="")
        assert q.type == "onset"
        assert q.value == ""
        # Empty value should still be "active"
        assert q.is_active() is True

    def test_from_string_with_multiple_colons(self):
        """Test parsing string with multiple colons in value."""
        q = Qualifier.from_string("onset:http://example.org:8080/path")
        assert q.type == "onset"
        assert q.value == "http://example.org:8080/path"

    def test_from_string_https_iri(self):
        """Test parsing HTTPS IRI."""
        q = Qualifier.from_string("https://example.org/qualifier:value")
        assert q.type == "https://example.org/qualifier"
        assert q.value == "value"
