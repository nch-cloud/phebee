"""Unit tests for qualifier type normalization."""

import pytest
from phebee.utils.qualifier import normalize_qualifier_type


def test_normalize_internal_qualifier_short_form():
    """Short form internal qualifiers should stay as-is."""
    assert normalize_qualifier_type("negated") == "negated"
    assert normalize_qualifier_type("onset") == "onset"
    assert normalize_qualifier_type("family") == "family"
    assert normalize_qualifier_type("hypothetical") == "hypothetical"


def test_normalize_internal_qualifier_iri_form():
    """IRI form internal qualifiers should be converted to short form."""
    assert normalize_qualifier_type(
        "http://ods.nationwidechildrens.org/phebee/qualifier/negated"
    ) == "negated"
    assert normalize_qualifier_type(
        "http://ods.nationwidechildrens.org/phebee/qualifier/onset"
    ) == "onset"
    assert normalize_qualifier_type(
        "http://ods.nationwidechildrens.org/phebee/qualifier/family"
    ) == "family"


def test_normalize_external_qualifier():
    """External ontology qualifiers should stay as full IRIs."""
    assert normalize_qualifier_type(
        "http://purl.obolibrary.org/obo/HP_0012823"
    ) == "http://purl.obolibrary.org/obo/HP_0012823"
    assert normalize_qualifier_type(
        "http://purl.obolibrary.org/obo/HP_0003577"
    ) == "http://purl.obolibrary.org/obo/HP_0003577"
    assert normalize_qualifier_type(
        "https://example.org/custom/qualifier"
    ) == "https://example.org/custom/qualifier"
