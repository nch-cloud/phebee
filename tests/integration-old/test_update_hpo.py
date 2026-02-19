import pytest
import os
from update_source_utils import check_dynamodb_record
from phebee.utils.aws import get_client
from phebee.utils.iceberg import (
    query_term_ancestors,
    query_term_descendants,
    query_iceberg_evidence
)

@pytest.mark.integration
def test_update_hpo(cloudformation_stack, update_hpo):
    test_start_time = update_hpo
    check_dynamodb_record("hpo", test_start_time, dynamodb=get_client("dynamodb"))


@pytest.mark.integration
def test_hpo_hierarchy_table_populated(cloudformation_stack, update_hpo):
    """Test that the ontology_hierarchy table was populated after HPO update."""
    database_name = os.environ.get('ICEBERG_DATABASE')
    table_name = os.environ.get('ICEBERG_ONTOLOGY_HIERARCHY_TABLE')

    # Query the table to verify it has HPO data
    query = f"""
    SELECT COUNT(*) as term_count
    FROM {database_name}.{table_name}
    WHERE ontology_source = 'hpo'
    """

    results = query_iceberg_evidence(query)

    assert len(results) > 0, "No results returned from ontology_hierarchy table"
    term_count = int(results[0]['term_count'])

    # HPO has ~17,000 terms (as of 2024)
    assert term_count > 10000, f"Expected more than 10,000 HPO terms, but found {term_count}"
    print(f"✅ Ontology hierarchy table contains {term_count} HPO terms")


@pytest.mark.integration
def test_query_term_ancestors(cloudformation_stack, update_hpo):
    """Test query_term_ancestors returns correct ancestor lineage."""
    # Test with HP:0000152 (Abnormality of head or neck)
    # Should have ancestors: HP:0000001 (All), HP:0000118 (Phenotypic abnormality), HP:0000152 (self)
    term_id = "HP:0000152"

    ancestors = query_term_ancestors(term_id, ontology_source="hpo")

    assert len(ancestors) > 0, f"No ancestors found for {term_id}"

    # Should include self
    assert term_id in ancestors, f"Term {term_id} should be in its own ancestor list"

    # Should include root
    assert "HP:0000001" in ancestors, "Should include root term HP:0000001 (All)"

    # Should include Phenotypic abnormality
    assert "HP:0000118" in ancestors, "Should include HP:0000118 (Phenotypic abnormality)"

    print(f"✅ Found {len(ancestors)} ancestors for {term_id}: {ancestors[:5]}...")


@pytest.mark.integration
def test_query_term_descendants(cloudformation_stack, update_hpo):
    """Test query_term_descendants returns all terms in subtree."""
    # Test with HP:0000118 (Phenotypic abnormality) - should have many descendants
    term_id = "HP:0000118"

    descendants = query_term_descendants(term_id, ontology_source="hpo")

    assert len(descendants) > 0, f"No descendants found for {term_id}"

    # Phenotypic abnormality should have thousands of descendants
    assert len(descendants) > 5000, f"Expected many descendants for {term_id}, but found {len(descendants)}"

    # Check that results include term_id, term_label, and depth
    first_desc = descendants[0]
    assert 'term_id' in first_desc, "Results should include term_id"
    assert 'term_label' in first_desc, "Results should include term_label"
    assert 'depth' in first_desc, "Results should include depth"

    # Verify HP:0000152 (Abnormality of head or neck) is in descendants
    descendant_ids = [d['term_id'] for d in descendants]
    assert "HP:0000152" in descendant_ids, "Should include HP:0000152 as a descendant"

    print(f"✅ Found {len(descendants)} descendants for {term_id}")


@pytest.mark.integration
def test_ancestors_ordered_by_depth(cloudformation_stack, update_hpo):
    """Test that ancestors are ordered by depth (root to leaf)."""
    # Test with HP:0000152 (Abnormality of head or neck)
    term_id = "HP:0000152"

    ancestors = query_term_ancestors(term_id, ontology_source="hpo")

    # Get the full records to check depth ordering
    database_name = os.environ.get('ICEBERG_DATABASE')
    table_name = os.environ.get('ICEBERG_ONTOLOGY_HIERARCHY_TABLE')

    # Query to get depth for each ancestor
    ancestor_list = "', '".join(ancestors)
    query = f"""
    SELECT term_id, depth
    FROM {database_name}.{table_name}
    WHERE ontology_source = 'hpo'
      AND term_id IN ('{ancestor_list}')
    ORDER BY depth ASC
    """

    results = query_iceberg_evidence(query)

    # Build depth map
    depth_map = {r['term_id']: int(r['depth']) for r in results}

    # Verify ancestors list is ordered by depth
    for i in range(len(ancestors) - 1):
        current_depth = depth_map.get(ancestors[i], 999)
        next_depth = depth_map.get(ancestors[i + 1], 999)

        assert current_depth <= next_depth, (
            f"Ancestors not ordered by depth: {ancestors[i]} (depth {current_depth}) "
            f"should come before {ancestors[i+1]} (depth {next_depth})"
        )

    # First should be root (depth 0)
    root_term = ancestors[0]
    assert depth_map[root_term] == 0, f"First ancestor should be root (depth 0), but {root_term} has depth {depth_map[root_term]}"

    # Last should be the term itself
    assert ancestors[-1] == term_id, f"Last ancestor should be the term itself ({term_id})"

    print(f"✅ Ancestors correctly ordered by depth: {[(t, depth_map[t]) for t in ancestors]}")


@pytest.mark.integration
def test_depth_values_correct(cloudformation_stack, update_hpo):
    """Test that depth values are correctly computed."""
    database_name = os.environ.get('ICEBERG_DATABASE')
    table_name = os.environ.get('ICEBERG_ONTOLOGY_HIERARCHY_TABLE')

    # Get depth for known terms
    query = f"""
    SELECT term_id, term_label, depth
    FROM {database_name}.{table_name}
    WHERE ontology_source = 'hpo'
      AND term_id IN ('HP:0000001', 'HP:0000118', 'HP:0000152')
    """

    results = query_iceberg_evidence(query)
    depth_map = {r['term_id']: int(r['depth']) for r in results}

    # HP:0000001 (All) should be depth 0 (root)
    assert depth_map.get('HP:0000001') == 0, "HP:0000001 (All) should have depth 0"

    # HP:0000118 (Phenotypic abnormality) should be depth 1 (child of root)
    assert depth_map.get('HP:0000118') == 1, "HP:0000118 should have depth 1"

    # HP:0000152 (Abnormality of head or neck) should be depth > 1
    hp_152_depth = depth_map.get('HP:0000152', -1)
    assert hp_152_depth > 1, f"HP:0000152 should have depth > 1, but has depth {hp_152_depth}"

    print(f"✅ Depth values correct: HP:0000001 (depth 0), HP:0000118 (depth 1), HP:0000152 (depth {hp_152_depth})")


@pytest.mark.integration
def test_descendants_ordered_by_depth(cloudformation_stack, update_hpo):
    """Test that descendants are ordered by depth (shallow to deep)."""
    # Test with a term that has multiple levels of descendants
    term_id = "HP:0000118"  # Phenotypic abnormality

    descendants = query_term_descendants(term_id, ontology_source="hpo")

    # Verify ordering - depths should be non-decreasing
    for i in range(len(descendants) - 1):
        current_depth = int(descendants[i]['depth'])
        next_depth = int(descendants[i + 1]['depth'])

        assert current_depth <= next_depth, (
            f"Descendants not ordered by depth: {descendants[i]['term_id']} (depth {current_depth}) "
            f"should come before {descendants[i+1]['term_id']} (depth {next_depth})"
        )

    # First descendants should be direct children (depth 2)
    first_depth = int(descendants[0]['depth'])
    assert first_depth == 2, f"First descendant should be at depth 2, but found depth {first_depth}"

    print(f"✅ Descendants correctly ordered by depth (range: {first_depth} to {int(descendants[-1]['depth'])})")
