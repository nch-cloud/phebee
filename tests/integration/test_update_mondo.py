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
def test_update_mondo(cloudformation_stack, update_mondo):
    test_start_time = update_mondo
    check_dynamodb_record("mondo", test_start_time, dynamodb=get_client("dynamodb"))


@pytest.mark.integration
def test_mondo_hierarchy_table_populated(cloudformation_stack, update_mondo):
    """Test that the ontology_hierarchy table was populated after MONDO update."""
    database_name = os.environ.get('ICEBERG_DATABASE')
    table_name = os.environ.get('ICEBERG_ONTOLOGY_HIERARCHY_TABLE')

    # Query the table to verify it has MONDO data
    query = f"""
    SELECT COUNT(*) as term_count
    FROM {database_name}.{table_name}
    WHERE ontology_source = 'mondo'
    """

    results = query_iceberg_evidence(query)

    assert len(results) > 0, "No results returned from ontology_hierarchy table"
    term_count = int(results[0]['term_count'])

    # MONDO has ~25,000 terms (as of 2024)
    assert term_count > 15000, f"Expected more than 15,000 MONDO terms, but found {term_count}"
    print(f"✅ Ontology hierarchy table contains {term_count} MONDO terms")


@pytest.mark.integration
def test_query_term_ancestors(cloudformation_stack, update_mondo):
    """Test query_term_ancestors returns correct ancestor lineage."""
    # Test with MONDO:0005148 (type 2 diabetes mellitus)
    # Should have ancestors including MONDO:0000001 (disease or disorder)
    term_id = "MONDO:0005148"

    ancestors = query_term_ancestors(term_id, ontology_source="mondo")

    assert len(ancestors) > 0, f"No ancestors found for {term_id}"

    # Should include self
    assert term_id in ancestors, f"Term {term_id} should be in its own ancestor list"

    # Should include root
    assert "MONDO:0000001" in ancestors, "Should include root term MONDO:0000001 (disease or disorder)"

    print(f"✅ Found {len(ancestors)} ancestors for {term_id}: {ancestors[:5]}...")


@pytest.mark.integration
def test_query_term_descendants(cloudformation_stack, update_mondo):
    """Test query_term_descendants returns all terms in subtree."""
    # Test with MONDO:0700096 (human disease) - should have many descendants
    term_id = "MONDO:0700096"

    descendants = query_term_descendants(term_id, ontology_source="mondo")

    assert len(descendants) > 0, f"No descendants found for {term_id}"

    # Human disease should have thousands of descendants
    assert len(descendants) > 1000, f"Expected many descendants for {term_id}, but found {len(descendants)}"

    # Check that results include term_id, term_label, and depth
    first_desc = descendants[0]
    assert 'term_id' in first_desc, "Results should include term_id"
    assert 'term_label' in first_desc, "Results should include term_label"
    assert 'depth' in first_desc, "Results should include depth"

    # Verify MONDO:0005148 (type 2 diabetes mellitus) is in descendants
    descendant_ids = [d['term_id'] for d in descendants]
    assert "MONDO:0005148" in descendant_ids, "Should include MONDO:0005148 as a descendant"

    print(f"✅ Found {len(descendants)} descendants for {term_id}")


@pytest.mark.integration
def test_ancestors_ordered_by_depth(cloudformation_stack, update_mondo):
    """Test that ancestors are ordered by depth (root to leaf)."""
    # Test with MONDO:0005148 (type 2 diabetes mellitus)
    term_id = "MONDO:0005148"

    ancestors = query_term_ancestors(term_id, ontology_source="mondo")

    # Get the full records to check depth ordering
    database_name = os.environ.get('ICEBERG_DATABASE')
    table_name = os.environ.get('ICEBERG_ONTOLOGY_HIERARCHY_TABLE')

    # Query to get depth for each ancestor
    ancestor_list = "', '".join(ancestors)
    query = f"""
    SELECT term_id, depth
    FROM {database_name}.{table_name}
    WHERE ontology_source = 'mondo'
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
def test_depth_values_correct(cloudformation_stack, update_mondo):
    """Test that depth values are correctly computed."""
    database_name = os.environ.get('ICEBERG_DATABASE')
    table_name = os.environ.get('ICEBERG_ONTOLOGY_HIERARCHY_TABLE')

    # Get depth for known terms
    query = f"""
    SELECT term_id, term_label, depth
    FROM {database_name}.{table_name}
    WHERE ontology_source = 'mondo'
      AND term_id IN ('MONDO:0000001', 'MONDO:0700096', 'MONDO:0005148')
    """

    results = query_iceberg_evidence(query)
    depth_map = {r['term_id']: int(r['depth']) for r in results}

    # MONDO:0000001 (disease or disorder) should be depth 0 (root)
    assert depth_map.get('MONDO:0000001') == 0, "MONDO:0000001 (disease or disorder) should have depth 0"

    # MONDO:0700096 (human disease) should be depth 1 (child of root)
    assert depth_map.get('MONDO:0700096') == 1, "MONDO:0700096 should have depth 1"

    # MONDO:0005148 (type 2 diabetes mellitus) should be depth > 1
    mondo_5148_depth = depth_map.get('MONDO:0005148', -1)
    assert mondo_5148_depth > 1, f"MONDO:0005148 should have depth > 1, but has depth {mondo_5148_depth}"

    print(f"✅ Depth values correct: MONDO:0000001 (depth 0), MONDO:0700096 (depth 1), MONDO:0005148 (depth {mondo_5148_depth})")


@pytest.mark.integration
def test_descendants_ordered_by_depth(cloudformation_stack, update_mondo):
    """Test that descendants are ordered by depth (shallow to deep)."""
    # Test with a term that has multiple levels of descendants
    term_id = "MONDO:0700096"  # human disease

    descendants = query_term_descendants(term_id, ontology_source="mondo")

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
