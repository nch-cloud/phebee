"""
Unit tests for Athena struct parsing utilities.
"""
import pytest
from phebee.utils.iceberg import parse_athena_struct_array, parse_qualifiers_field
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
