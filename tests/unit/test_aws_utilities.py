"""
Unit tests for AWS utility functions.
"""
import pytest
from phebee.utils.aws import parse_s3_path, extract_body, exponential_backoff, escape_string


class TestParseS3Path:
    """Test S3 path parsing functionality."""

    def test_valid_simple_path(self):
        """Test parsing a simple S3 path."""
        bucket, key = parse_s3_path("s3://my-bucket/my-key")
        assert bucket == "my-bucket"
        assert key == "my-key"

    def test_valid_nested_path(self):
        """Test parsing an S3 path with nested keys."""
        bucket, key = parse_s3_path("s3://my-bucket/path/to/object.json")
        assert bucket == "my-bucket"
        assert key == "path/to/object.json"

    def test_valid_deep_nested_path(self):
        """Test parsing a deeply nested S3 path."""
        bucket, key = parse_s3_path("s3://data-bucket/year/2024/month/01/data.parquet")
        assert bucket == "data-bucket"
        assert key == "year/2024/month/01/data.parquet"

    def test_missing_bucket(self):
        """Test that missing bucket raises ValueError."""
        with pytest.raises(ValueError, match="Missing bucket"):
            parse_s3_path("s3:///just-a-key")

    def test_missing_key(self):
        """Test that missing key raises ValueError."""
        with pytest.raises(ValueError, match="Missing key"):
            parse_s3_path("s3://bucket-only/")

    def test_missing_key_no_slash(self):
        """Test that bucket without key raises ValueError."""
        with pytest.raises(ValueError, match="Missing key"):
            parse_s3_path("s3://bucket-only")

    def test_invalid_scheme_http(self):
        """Test that HTTP scheme raises ValueError."""
        with pytest.raises(ValueError, match="Invalid URL scheme"):
            parse_s3_path("http://bucket/key")

    def test_invalid_scheme_https(self):
        """Test that HTTPS scheme raises ValueError."""
        with pytest.raises(ValueError, match="Invalid URL scheme"):
            parse_s3_path("https://bucket/key")

    def test_invalid_scheme_empty(self):
        """Test that missing scheme raises ValueError."""
        with pytest.raises(ValueError, match="Invalid URL scheme"):
            parse_s3_path("bucket/key")

    def test_bucket_with_dashes(self):
        """Test parsing bucket names with dashes."""
        bucket, key = parse_s3_path("s3://my-data-bucket-prod/file.txt")
        assert bucket == "my-data-bucket-prod"
        assert key == "file.txt"

    def test_bucket_with_dots(self):
        """Test parsing bucket names with dots."""
        bucket, key = parse_s3_path("s3://my.bucket.name/file.txt")
        assert bucket == "my.bucket.name"
        assert key == "file.txt"

    def test_key_with_special_characters(self):
        """Test parsing keys with special characters."""
        bucket, key = parse_s3_path("s3://bucket/path/file_name-2024.json")
        assert bucket == "bucket"
        assert key == "path/file_name-2024.json"


class TestExtractBody:
    """Test Lambda event body extraction."""

    def test_api_gateway_with_string_body(self):
        """Test extracting body from API Gateway event with stringified JSON."""
        event = {
            "body": '{"key": "value", "number": 42}'
        }
        result = extract_body(event)
        assert result == {"key": "value", "number": 42}

    def test_api_gateway_with_dict_body(self):
        """Test extracting body from API Gateway event with dict body."""
        event = {
            "body": {"key": "value", "number": 42}
        }
        result = extract_body(event)
        assert result == {"key": "value", "number": 42}

    def test_direct_invocation_no_body_field(self):
        """Test direct Lambda invocation without body wrapper."""
        event = {"key": "value", "number": 42}
        result = extract_body(event)
        assert result == {"key": "value", "number": 42}

    def test_empty_body(self):
        """Test extracting empty body."""
        event = {"body": "{}"}
        result = extract_body(event)
        assert result == {}

    def test_nested_body(self):
        """Test extracting nested JSON structure."""
        event = {
            "body": '{"user": {"name": "John", "age": 30}, "items": [1, 2, 3]}'
        }
        result = extract_body(event)
        assert result == {"user": {"name": "John", "age": 30}, "items": [1, 2, 3]}

    def test_body_with_null_values(self):
        """Test extracting body with null values."""
        event = {
            "body": '{"key": null, "value": "test"}'
        }
        result = extract_body(event)
        assert result == {"key": None, "value": "test"}

    def test_malformed_json_string(self):
        """Test that malformed JSON raises JSONDecodeError."""
        event = {
            "body": '{"key": "value"'  # Missing closing brace
        }
        with pytest.raises(Exception):  # Will raise JSONDecodeError
            extract_body(event)


class TestExponentialBackoff:
    """Test exponential backoff calculation."""

    def test_first_attempt(self):
        """Test that first attempt returns initial delay."""
        result = exponential_backoff(0, initial_delay=1, max_delay=60)
        assert result == 1

    def test_second_attempt(self):
        """Test exponential growth for second attempt."""
        result = exponential_backoff(1, initial_delay=1, max_delay=60)
        assert result == 2

    def test_third_attempt(self):
        """Test exponential growth for third attempt."""
        result = exponential_backoff(2, initial_delay=1, max_delay=60)
        assert result == 4

    def test_fourth_attempt(self):
        """Test exponential growth for fourth attempt."""
        result = exponential_backoff(3, initial_delay=1, max_delay=60)
        assert result == 8

    def test_capping_at_max_delay(self):
        """Test that delay is capped at max_delay."""
        result = exponential_backoff(10, initial_delay=1, max_delay=60)
        assert result == 60

    def test_max_delay_smaller_than_exponential(self):
        """Test capping when max_delay is reached early."""
        result = exponential_backoff(5, initial_delay=1, max_delay=10)
        assert result == 10

    def test_large_initial_delay(self):
        """Test with large initial delay."""
        result = exponential_backoff(0, initial_delay=10, max_delay=100)
        assert result == 10

        result = exponential_backoff(1, initial_delay=10, max_delay=100)
        assert result == 20

    def test_max_delay_equals_initial_delay(self):
        """Test when max_delay equals initial_delay (no growth)."""
        result = exponential_backoff(5, initial_delay=10, max_delay=10)
        assert result == 10


class TestEscapeString:
    """Test URL string escaping."""

    def test_simple_string(self):
        """Test that simple strings without special characters are unchanged."""
        result = escape_string("simple")
        assert result == "simple"

    def test_string_with_spaces(self):
        """Test escaping spaces."""
        result = escape_string("hello world")
        assert result == "hello%20world"

    def test_string_with_slashes(self):
        """Test escaping forward slashes."""
        result = escape_string("path/to/file")
        assert result == "path%2Fto%2Ffile"

    def test_string_with_special_characters(self):
        """Test escaping various special characters."""
        result = escape_string("test@example.com")
        assert result == "test%40example.com"

    def test_empty_string(self):
        """Test escaping empty string."""
        result = escape_string("")
        assert result == ""

    def test_unicode_characters(self):
        """Test escaping unicode characters."""
        result = escape_string("café")
        # Unicode should be percent-encoded
        assert "%" in result

    def test_already_escaped_string(self):
        """Test that already-escaped strings get double-escaped."""
        result = escape_string("hello%20world")
        # The % should be escaped to %25
        assert result == "hello%2520world"

    def test_string_with_ampersand(self):
        """Test escaping ampersand."""
        result = escape_string("key=value&other=value2")
        assert "%" in result
        assert "&" not in result

    def test_string_with_question_mark(self):
        """Test escaping question mark."""
        result = escape_string("query?param=value")
        assert "%" in result
        assert "?" not in result

    def test_string_with_hash(self):
        """Test escaping hash/fragment."""
        result = escape_string("file#section")
        assert "%" in result
        assert "#" not in result
