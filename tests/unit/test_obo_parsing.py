"""
Unit tests for OBO file parsing logic in materialize_ontology_hierarchy.

These tests verify that the OBO parser correctly:
- Strips qualifier annotations from is_a relationships
- Filters out cross-ontology parent relationships
- Computes correct ancestor closure and depth values
"""
import pytest
import sys
import os
from unittest.mock import MagicMock

# Add the functions directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../functions'))

# Mock aws_lambda_powertools before importing the module
sys.modules['aws_lambda_powertools'] = MagicMock()

from materialize_ontology_hierarchy import parse_obo_file, compute_ancestor_closure


class TestQualifierAnnotationStripping:
    """Test that {qualifier} annotations are correctly stripped from is_a relationships."""

    def test_simple_qualifier(self):
        """Test basic qualifier annotation with source attribute."""
        obo_content = """
[Term]
id: MONDO:0000001
name: disease
is_a: BFO:0000016 ! disposition

[Term]
id: MONDO:0700096
name: human disease
is_a: MONDO:0000001 {source="http://orcid.org/0000-0002-4142-7153"} ! disease
"""
        terms = parse_obo_file(obo_content, 'mondo')

        assert len(terms) == 2
        mondo_700096 = next(t for t in terms if t['term_id'] == 'MONDO:0700096')
        assert mondo_700096['parent_ids'] == ['MONDO:0000001'], \
            "Qualifier annotation should be stripped from parent ID"

    def test_multiple_qualifiers(self):
        """Test terms with multiple parent relationships, some with qualifiers."""
        obo_content = """
[Term]
id: MONDO:0001071
name: intellectual disability
is_a: MONDO:0700092 {source="PMID:31613434"} ! neurodevelopmental disorder

[Term]
id: MONDO:0800477
name: SETD2-related disorder
is_a: MONDO:0001071 {source="https://clinicalgenome.org/affiliation/40006/"} ! intellectual disability
is_a: MONDO:0100500 ! Mendelian neurodevelopmental disorder
"""
        terms = parse_obo_file(obo_content, 'mondo')

        mondo_1071 = next(t for t in terms if t['term_id'] == 'MONDO:0001071')
        assert mondo_1071['parent_ids'] == ['MONDO:0700092']

        mondo_800477 = next(t for t in terms if t['term_id'] == 'MONDO:0800477')
        assert set(mondo_800477['parent_ids']) == {'MONDO:0001071', 'MONDO:0100500'}

    def test_qualifier_without_comment(self):
        """Test qualifier annotation without trailing comment."""
        obo_content = """
[Term]
id: MONDO:0005071
name: nervous system disorder
is_a: MONDO:0700096 {source="https://orcid.org/0000-0002-1780-5230"}
"""
        terms = parse_obo_file(obo_content, 'mondo')

        mondo_5071 = next(t for t in terms if t['term_id'] == 'MONDO:0005071')
        assert mondo_5071['parent_ids'] == ['MONDO:0700096']


class TestCrossOntologyFiltering:
    """Test that cross-ontology parent relationships are correctly filtered."""

    def test_bfo_parent_filtered(self):
        """Test that BFO parents are filtered from MONDO terms."""
        obo_content = """
[Term]
id: BFO:0000016
name: disposition

[Term]
id: MONDO:0000001
name: disease
is_a: BFO:0000016 ! disposition
"""
        terms = parse_obo_file(obo_content, 'mondo')

        # Should only include MONDO terms
        assert len(terms) == 1
        assert terms[0]['term_id'] == 'MONDO:0000001'

        # BFO parent should be filtered out
        assert terms[0]['parent_ids'] == [], \
            "BFO parent should be filtered from MONDO term"

    def test_mondo_root_no_parents(self):
        """Test that MONDO:0000001 has no parents after BFO filtering."""
        obo_content = """
[Term]
id: MONDO:0000001
name: disease
is_a: BFO:0000016 {source="MONDO:equivalentTo"} ! disposition
"""
        terms = parse_obo_file(obo_content, 'mondo')

        assert len(terms) == 1
        assert terms[0]['parent_ids'] == []

    def test_hpo_term_filtering(self):
        """Test that HP parents are filtered from MONDO terms."""
        obo_content = """
[Term]
id: MONDO:0008840
name: Joubert syndrome
is_a: HP:0001249 {source="MONDO:0008840"} ! Intellectual disability
is_a: MONDO:0015626 ! ciliopathy
"""
        terms = parse_obo_file(obo_content, 'mondo')

        mondo_8840 = terms[0]
        # HP parent should be filtered, only MONDO parent remains
        assert mondo_8840['parent_ids'] == ['MONDO:0015626']

    def test_only_same_ontology_terms_returned(self):
        """Test that only terms matching the ontology prefix are returned."""
        obo_content = """
[Term]
id: HP:0000001
name: All

[Term]
id: MONDO:0000001
name: disease

[Term]
id: BFO:0000016
name: disposition

[Term]
id: UBERON:0000468
name: multicellular organism
"""
        mondo_terms = parse_obo_file(obo_content, 'mondo')
        assert len(mondo_terms) == 1
        assert mondo_terms[0]['term_id'] == 'MONDO:0000001'

        hpo_terms = parse_obo_file(obo_content, 'hpo')
        assert len(hpo_terms) == 1
        assert hpo_terms[0]['term_id'] == 'HP:0000001'


class TestBasicParsing:
    """Test basic OBO parsing functionality."""

    def test_required_fields_present(self):
        """Test that parsed terms contain all required fields."""
        obo_content = """
[Term]
id: MONDO:0000001
name: disease
def: "A disease is a disposition..." []
"""
        terms = parse_obo_file(obo_content, 'mondo')

        assert len(terms) == 1
        term = terms[0]

        # Check required fields
        assert 'term_id' in term
        assert 'term_iri' in term
        assert 'term_label' in term
        assert 'parent_ids' in term

        assert term['term_id'] == 'MONDO:0000001'
        assert term['term_label'] == 'disease'
        assert term['term_iri'] == 'http://purl.obolibrary.org/obo/MONDO_0000001'

    def test_term_iri_conversion(self):
        """Test term ID to IRI conversion."""
        obo_content = """
[Term]
id: HP:0001278
name: Orthostatic hypotension
"""
        terms = parse_obo_file(obo_content, 'hpo')

        assert terms[0]['term_iri'] == 'http://purl.obolibrary.org/obo/HP_0001278'

    def test_multiple_parents(self):
        """Test parsing terms with multiple parents."""
        obo_content = """
[Term]
id: MONDO:0000001
name: disease

[Term]
id: MONDO:0000002
name: parent1

[Term]
id: MONDO:0000003
name: parent2

[Term]
id: MONDO:0000004
name: child
is_a: MONDO:0000002 ! parent1
is_a: MONDO:0000003 ! parent2
"""
        terms = parse_obo_file(obo_content, 'mondo')

        child = next(t for t in terms if t['term_id'] == 'MONDO:0000004')
        assert set(child['parent_ids']) == {'MONDO:0000002', 'MONDO:0000003'}


class TestAncestorClosure:
    """Test ancestor closure computation and depth calculation."""

    def test_simple_hierarchy_depth(self):
        """Test depth calculation for a simple linear hierarchy."""
        obo_content = """
[Term]
id: MONDO:0000001
name: disease

[Term]
id: MONDO:0700096
name: human disease
is_a: MONDO:0000001

[Term]
id: MONDO:0005071
name: nervous system disorder
is_a: MONDO:0700096
"""
        terms = parse_obo_file(obo_content, 'mondo')
        terms_with_ancestors = compute_ancestor_closure(terms)

        # Build depth map
        depth_map = {t['term_id']: t['depth'] for t in terms_with_ancestors}

        assert depth_map['MONDO:0000001'] == 0, "Root should have depth 0"
        assert depth_map['MONDO:0700096'] == 1, "Direct child should have depth 1"
        assert depth_map['MONDO:0005071'] == 2, "Grandchild should have depth 2"

    def test_ancestor_includes_self(self):
        """Test that ancestor list includes the term itself."""
        obo_content = """
[Term]
id: MONDO:0000001
name: disease

[Term]
id: MONDO:0700096
name: human disease
is_a: MONDO:0000001
"""
        terms = parse_obo_file(obo_content, 'mondo')
        terms_with_ancestors = compute_ancestor_closure(terms)

        mondo_700096 = next(t for t in terms_with_ancestors if t['term_id'] == 'MONDO:0700096')
        assert 'MONDO:0700096' in mondo_700096['ancestor_term_ids'], \
            "Ancestor list should include the term itself"

    def test_ancestor_ordering(self):
        """Test that ancestors are ordered by depth (root to leaf)."""
        obo_content = """
[Term]
id: MONDO:0000001
name: disease

[Term]
id: MONDO:0700096
name: human disease
is_a: MONDO:0000001

[Term]
id: MONDO:0005071
name: nervous system disorder
is_a: MONDO:0700096

[Term]
id: MONDO:0700092
name: neurodevelopmental disorder
is_a: MONDO:0005071
"""
        terms = parse_obo_file(obo_content, 'mondo')
        terms_with_ancestors = compute_ancestor_closure(terms)

        mondo_700092 = next(t for t in terms_with_ancestors if t['term_id'] == 'MONDO:0700092')
        ancestors = mondo_700092['ancestor_term_ids']

        # Check ordering: should be root to leaf
        expected_order = ['MONDO:0000001', 'MONDO:0700096', 'MONDO:0005071', 'MONDO:0700092']
        assert ancestors == expected_order, \
            f"Ancestors should be ordered root to leaf, got {ancestors}"

    def test_multiple_inheritance_depth(self):
        """Test depth calculation with multiple inheritance (diamond pattern)."""
        obo_content = """
[Term]
id: MONDO:0000001
name: disease

[Term]
id: MONDO:0000002
name: disease type A
is_a: MONDO:0000001

[Term]
id: MONDO:0000003
name: disease type B
is_a: MONDO:0000001

[Term]
id: MONDO:0000004
name: specific disease
is_a: MONDO:0000002
is_a: MONDO:0000003
"""
        terms = parse_obo_file(obo_content, 'mondo')
        terms_with_ancestors = compute_ancestor_closure(terms)

        depth_map = {t['term_id']: t['depth'] for t in terms_with_ancestors}

        # Depth should be longest path (max depth)
        assert depth_map['MONDO:0000004'] == 2, \
            "Multiple inheritance should use max depth from any parent"

        # All ancestors should be included
        mondo_4 = next(t for t in terms_with_ancestors if t['term_id'] == 'MONDO:0000004')
        ancestor_ids = set(mondo_4['ancestor_term_ids'])
        assert ancestor_ids >= {'MONDO:0000001', 'MONDO:0000002', 'MONDO:0000003', 'MONDO:0000004'}


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_obo_content(self):
        """Test parsing empty OBO content."""
        terms = parse_obo_file("", 'mondo')
        assert terms == []

    def test_term_without_parents(self):
        """Test that root terms have empty parent_ids list."""
        obo_content = """
[Term]
id: HP:0000001
name: All
"""
        terms = parse_obo_file(obo_content, 'hpo')

        assert terms[0]['parent_ids'] == []

    def test_non_term_stanzas_ignored(self):
        """Test that non-[Term] stanzas are ignored."""
        obo_content = """
[Typedef]
id: part_of
name: part of

[Term]
id: MONDO:0000001
name: disease

[Instance]
id: some_instance
"""
        terms = parse_obo_file(obo_content, 'mondo')

        # Should only parse [Term] stanzas
        assert len(terms) == 1
        assert terms[0]['term_id'] == 'MONDO:0000001'

    def test_invalid_ontology_source(self):
        """Test that invalid ontology source raises ValueError."""
        obo_content = """
[Term]
id: MONDO:0000001
name: disease
"""
        with pytest.raises(ValueError, match="Unknown ontology source"):
            parse_obo_file(obo_content, 'invalid_ontology')

    def test_term_without_required_fields(self):
        """Test that terms without required fields are filtered out."""
        obo_content = """
[Term]
id: MONDO:0000001
name: disease

[Term]
id: MONDO:0000002

[Term]
name: term without id
"""
        terms = parse_obo_file(obo_content, 'mondo')

        # Should only include term with all required fields
        assert len(terms) == 1
        assert terms[0]['term_id'] == 'MONDO:0000001'


class TestRegressionTests:
    """Regression tests for previously discovered bugs."""

    def test_mondo_qualifier_bug_fixed(self):
        """
        Regression test for qualifier annotation bug that caused MONDO hierarchy to break.

        Previously, qualifiers like {source="PMID:31613434"} were not stripped from
        parent IDs, causing terms like "MONDO:0700092 {source=...}" to be used as
        parent IDs instead of "MONDO:0700092". This broke the hierarchy computation,
        resulting in orphaned terms with incorrect depth values.
        """
        obo_content = """
[Term]
id: MONDO:0000001
name: disease
is_a: BFO:0000016 ! disposition

[Term]
id: MONDO:0700096
name: human disease
is_a: MONDO:0000001 {source="http://orcid.org/0000-0002-4142-7153"} ! disease

[Term]
id: MONDO:0005071
name: nervous system disorder
is_a: MONDO:0700096 ! human disease

[Term]
id: MONDO:0700092
name: neurodevelopmental disorder
is_a: MONDO:0005071 {source="https://orcid.org/0000-0002-1780-5230"} ! nervous system disorder

[Term]
id: MONDO:0001071
name: intellectual disability
is_a: MONDO:0700092 {source="PMID:31613434"} ! neurodevelopmental disorder

[Term]
id: MONDO:0800477
name: SETD2-related disorder
is_a: MONDO:0001071 {source="https://clinicalgenome.org/affiliation/40006/"} ! intellectual disability
"""
        terms = parse_obo_file(obo_content, 'mondo')
        terms_with_ancestors = compute_ancestor_closure(terms)

        # Build maps
        depth_map = {t['term_id']: t['depth'] for t in terms_with_ancestors}
        ancestor_map = {t['term_id']: t['ancestor_term_ids'] for t in terms_with_ancestors}

        # Verify hierarchy is correct
        assert depth_map['MONDO:0000001'] == 0, "Root should have depth 0"
        assert depth_map['MONDO:0700096'] == 1
        assert depth_map['MONDO:0005071'] == 2
        assert depth_map['MONDO:0700092'] == 3
        assert depth_map['MONDO:0001071'] == 4
        assert depth_map['MONDO:0800477'] == 5, \
            "Bug: MONDO:0800477 should have depth 5, not 0 (orphaned root)"

        # Verify MONDO:0800477 has correct ancestors including root
        ancestors_800477 = ancestor_map['MONDO:0800477']
        assert 'MONDO:0000001' in ancestors_800477, \
            "Bug: MONDO:0800477 should include root MONDO:0000001 in ancestors"
        assert ancestors_800477[0] == 'MONDO:0000001', \
            "Root should be first ancestor"
        assert ancestors_800477[-1] == 'MONDO:0800477', \
            "Self should be last ancestor"
