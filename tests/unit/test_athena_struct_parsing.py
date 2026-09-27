"""
Unit tests for Athena struct parsing utilities.
"""
import pytest
from phebee.utils.iceberg import parse_athena_struct_array, parse_athena_row_array, parse_qualifiers_field
from phebee.utils.qualifier import Qualifier


class TestParseAthenaStructArray:
    """Test the parse_athena_struct_array function."""
    
    def test_empty_input(self):
        """Test empty and null inputs."""
        assert parse_athena_struct_array("") == []
        assert parse_athena_struct_array("null") == []
        assert parse_athena_struct_array(None) == []
        assert parse_athena_struct_array("[]") == []
    
    def test_single_struct(self):
        """Test parsing a single struct."""
        input_str = "[{qualifier_type=negated, qualifier_value=true}]"
        expected = [{"qualifier_type": "negated", "qualifier_value": "true"}]
        assert parse_athena_struct_array(input_str) == expected
    
    def test_multiple_structs(self):
        """Test parsing multiple structs."""
        input_str = "[{qualifier_type=negated, qualifier_value=true}, {qualifier_type=family, qualifier_value=false}]"
        expected = [
            {"qualifier_type": "negated", "qualifier_value": "true"},
            {"qualifier_type": "family", "qualifier_value": "false"}
        ]
        assert parse_athena_struct_array(input_str) == expected
    
    def test_struct_without_outer_brackets(self):
        """Test parsing struct without outer brackets."""
        input_str = "{qualifier_type=negated, qualifier_value=true}"
        expected = [{"qualifier_type": "negated", "qualifier_value": "true"}]
        assert parse_athena_struct_array(input_str) == expected
    
    def test_complex_values(self):
        """Test parsing structs with complex values."""
        input_str = "[{creator_id=test-user, creator_type=human, creator_name=Dr. Smith}]"
        expected = [{"creator_id": "test-user", "creator_type": "human", "creator_name": "Dr. Smith"}]
        assert parse_athena_struct_array(input_str) == expected
    
    def test_whitespace_handling(self):
        """Test that whitespace is handled correctly."""
        input_str = "[ { qualifier_type = negated , qualifier_value = true } ]"
        expected = [{"qualifier_type": "negated", "qualifier_value": "true"}]
        assert parse_athena_struct_array(input_str) == expected


class TestParseAthenaRowArray:
    """Test the parse_athena_row_array function."""

    def test_row_with_nested_qualifiers(self):
        """Test parsing ROW containing nested qualifiers array (regression test).

        This tests the fix for a bug where '}, {' inside a nested qualifiers
        array was incorrectly treated as a ROW separator.
        """
        # Simulates: ROW(term_id, term_iri, term_label, qualifiers)
        row_str = "[{HP:0033000, http://purl.obolibrary.org/obo/HP_0033000, Abnormality, [{qualifier_type=family, qualifier_value=true}, {qualifier_type=negated, qualifier_value=false}]}]"
        field_names = ['term_id', 'term_iri', 'term_label', 'qualifiers']
        result = parse_athena_row_array(row_str, field_names)

        assert len(result) == 1
        assert result[0]['term_id'] == 'HP:0033000'
        assert result[0]['term_iri'] == 'http://purl.obolibrary.org/obo/HP_0033000'
        assert result[0]['term_label'] == 'Abnormality'
        # The qualifiers field should be the complete array string
        assert result[0]['qualifiers'] == '[{qualifier_type=family, qualifier_value=true}, {qualifier_type=negated, qualifier_value=false}]'

    def test_empty_input(self):
        """Test empty and null inputs."""
        field_names = ['term_id', 'term_iri']
        assert parse_athena_row_array("", field_names) == []
        assert parse_athena_row_array(None, field_names) == []
        assert parse_athena_row_array("null", field_names) == []
        assert parse_athena_row_array("[]", field_names) == []

    def test_multiple_rows(self):
        """Test that separate ROWs are split apart.

        The test above covers one ROW, so it never exercised the separator
        between two of them. A subject with more than one term was collapsing
        into a single term.
        """
        row_str = ("[{HP:0000001, http://purl.obolibrary.org/obo/HP_0000001, All}, "
                   "{HP:0000002, http://purl.obolibrary.org/obo/HP_0000002, Obsolete}]")
        field_names = ['term_id', 'term_iri', 'term_label']
        result = parse_athena_row_array(row_str, field_names)

        assert [r['term_id'] for r in result] == ['HP:0000001', 'HP:0000002']
        assert [r['term_label'] for r in result] == ['All', 'Obsolete']
        assert result[1]['term_iri'] == 'http://purl.obolibrary.org/obo/HP_0000002'

    def test_multiple_rows_each_with_nested_qualifiers(self):
        """Test the two cases together: a nested '}, {' must not split a ROW,
        and a top-level one must."""
        row_str = ("[{HP:0000001, All, [{qualifier_type=negated, qualifier_value=true}, "
                   "{qualifier_type=family, qualifier_value=false}]}, "
                   "{HP:0000002, Obsolete, [{qualifier_type=negated, qualifier_value=false}]}]")
        field_names = ['term_id', 'term_label', 'qualifiers']
        result = parse_athena_row_array(row_str, field_names)

        assert len(result) == 2
        assert result[0]['qualifiers'] == (
            '[{qualifier_type=negated, qualifier_value=true}, '
            '{qualifier_type=family, qualifier_value=false}]'
        )
        assert result[1]['term_id'] == 'HP:0000002'
        assert result[1]['qualifiers'] == '[{qualifier_type=negated, qualifier_value=false}]'

    def test_full_production_row_shape(self):
        """Test the eight-field ROW that query_subjects_by_project aggregates."""
        field_names = ['term_id', 'term_iri', 'term_label', 'qualifiers', 'evidence_count',
                       'termlink_id', 'first_evidence_date', 'last_evidence_date']
        row_str = ("[{HP:0001880, http://purl.obolibrary.org/obo/HP_0001880, Eosinophilia, "
                   "[{qualifier_type=negated, qualifier_value=true}], 3, tl-1, "
                   "2024-01-01, 2024-06-30}, "
                   "{HP:0001873, http://purl.obolibrary.org/obo/HP_0001873, Thrombocytopenia, "
                   "[], 1, tl-2, 2024-02-02, 2024-02-02}]")
        result = parse_athena_row_array(row_str, field_names)

        assert len(result) == 2
        assert result[0] == {
            'term_id': 'HP:0001880',
            'term_iri': 'http://purl.obolibrary.org/obo/HP_0001880',
            'term_label': 'Eosinophilia',
            'qualifiers': '[{qualifier_type=negated, qualifier_value=true}]',
            'evidence_count': '3',
            'termlink_id': 'tl-1',
            'first_evidence_date': '2024-01-01',
            'last_evidence_date': '2024-06-30',
        }
        assert result[1]['term_id'] == 'HP:0001873'
        assert result[1]['qualifiers'] == '[]'
        assert result[1]['evidence_count'] == '1'

    def test_many_rows_at_benchmark_scale(self):
        """Test a term count typical of the 100k benchmark project.

        Subjects there carry a few hundred terms each; the collapsing bug turned
        468 of them into 1, which no fixture-scale test would have caught.
        """
        field_names = ['term_id', 'term_label', 'qualifiers']
        rows = [
            f"{{HP:{i:07d}, label {i}, [{{qualifier_type=negated, qualifier_value=false}}]}}"
            for i in range(468)
        ]
        row_str = "[" + ", ".join(rows) + "]"
        result = parse_athena_row_array(row_str, field_names)

        assert len(result) == 468
        assert result[0]['term_id'] == 'HP:0000000'
        assert result[467]['term_id'] == 'HP:0000467'
        assert all(r['qualifiers'] == '[{qualifier_type=negated, qualifier_value=false}]'
                   for r in result)

    def test_row_with_null_and_missing_fields(self):
        """Test that 'null' becomes None and short ROWs pad with None."""
        field_names = ['term_id', 'term_label', 'qualifiers']
        row_str = "[{HP:0000001, null}, {HP:0000002, Obsolete, []}]"
        result = parse_athena_row_array(row_str, field_names)

        assert result[0] == {'term_id': 'HP:0000001', 'term_label': None, 'qualifiers': None}
        assert result[1]['qualifiers'] == '[]'


class TestParseQualifiersField:
    """Test the parse_qualifiers_field function."""
    
    def test_empty_input(self):
        """Test empty and null inputs."""
        assert parse_qualifiers_field("") == []
        assert parse_qualifiers_field("null") == []
        assert parse_qualifiers_field(None) == []
        assert parse_qualifiers_field("[]") == []
    
    def test_struct_format(self):
        """Test parsing Athena struct format qualifiers."""
        struct_str = "[{qualifier_type=negated, qualifier_value=true}, {qualifier_type=family, qualifier_value=false}]"
        expected = [Qualifier(type="negated", value="true")]  # Only active qualifiers
        assert parse_qualifiers_field(struct_str) == expected
    
    def test_multiple_active_qualifiers(self):
        """Test multiple active qualifiers."""
        struct_str = "[{qualifier_type=negated, qualifier_value=true}, {qualifier_type=hypothetical, qualifier_value=true}]"
        expected = {
            Qualifier(type="negated", value="true"),
            Qualifier(type="hypothetical", value="true")
        }
        assert set(parse_qualifiers_field(struct_str)) == expected
    
    def test_numeric_values(self):
        """Test numeric qualifier values."""
        struct_str = "[{qualifier_type=negated, qualifier_value=1}, {qualifier_type=family, qualifier_value=0}]"
        expected = [Qualifier(type="negated", value="1")]  # Only value=1 should be active
        assert parse_qualifiers_field(struct_str) == expected

    def test_multiple_qualifiers_in_array(self):
        """Test parsing multiple qualifiers in a single array (regression test).

        This tests the fix for a bug where '}, {' inside a qualifiers array
        was incorrectly treated as a ROW separator, causing truncation.
        """
        struct_str = "[{qualifier_type=family, qualifier_value=true}, {qualifier_type=negated, qualifier_value=false}]"
        expected = [Qualifier(type="family", value="true")]  # Only active qualifier
        assert parse_qualifiers_field(struct_str) == expected
