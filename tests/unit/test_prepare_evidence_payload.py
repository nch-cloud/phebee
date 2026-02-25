"""
Unit tests for prepare_evidence_payload Lambda function.
"""
import pytest
import sys
import os
import json

# Add the functions directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../functions'))

# Mock aws_lambda_powertools before importing the module
from unittest.mock import MagicMock
sys.modules['aws_lambda_powertools'] = MagicMock()

from prepare_evidence_payload import lambda_handler


class TestLambdaHandler:
    """Test the prepare_evidence_payload Lambda handler."""

    def test_valid_single_evidence_record(self):
        """Test replacing subject_id in single evidence record."""
        event = {
            "evidence_payload": [
                {
                    "subject_id": "project-1#subject-001",
                    "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
                    "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000311",
                    "creator_id": "test-user",
                    "creator_type": "system",
                    "qualifiers": []
                }
            ],
            "subject_id": "550e8400-e29b-41d4-a716-446655440000"
        }

        result = lambda_handler(event, None)

        assert result["statusCode"] == 200
        assert len(result["fixed_evidence_payload"]) == 1
        assert result["fixed_evidence_payload"][0]["subject_id"] == "550e8400-e29b-41d4-a716-446655440000"
        assert result["fixed_evidence_payload"][0]["term_iri"] == "http://purl.obolibrary.org/obo/HP_0001250"

    def test_valid_multiple_evidence_records(self):
        """Test replacing subject_id in multiple evidence records."""
        event = {
            "evidence_payload": [
                {
                    "subject_id": "project-1#subject-001",
                    "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
                    "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000311",
                    "creator_id": "test-user",
                    "creator_type": "system",
                    "qualifiers": []
                },
                {
                    "subject_id": "project-1#subject-001",
                    "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
                    "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000311",
                    "creator_id": "test-user",
                    "creator_type": "system",
                    "qualifiers": ["negated"]
                }
            ],
            "subject_id": "550e8400-e29b-41d4-a716-446655440000"
        }

        result = lambda_handler(event, None)

        assert result["statusCode"] == 200
        assert len(result["fixed_evidence_payload"]) == 2
        assert result["fixed_evidence_payload"][0]["subject_id"] == "550e8400-e29b-41d4-a716-446655440000"
        assert result["fixed_evidence_payload"][1]["subject_id"] == "550e8400-e29b-41d4-a716-446655440000"
        assert result["fixed_evidence_payload"][0]["term_iri"] == "http://purl.obolibrary.org/obo/HP_0001250"
        assert result["fixed_evidence_payload"][1]["term_iri"] == "http://purl.obolibrary.org/obo/HP_0001627"

    def test_missing_evidence_payload(self):
        """Test that missing evidence_payload returns 400 error."""
        event = {
            "subject_id": "550e8400-e29b-41d4-a716-446655440000"
        }

        result = lambda_handler(event, None)

        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "Missing required fields" in body["message"]

    def test_missing_subject_id(self):
        """Test that missing subject_id returns 400 error."""
        event = {
            "evidence_payload": [
                {
                    "subject_id": "project-1#subject-001",
                    "term_iri": "http://purl.obolibrary.org/obo/HP_0001250"
                }
            ]
        }

        result = lambda_handler(event, None)

        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "Missing required fields" in body["message"]

    def test_empty_evidence_payload(self):
        """Test that empty evidence_payload returns 400 error."""
        event = {
            "evidence_payload": [],
            "subject_id": "550e8400-e29b-41d4-a716-446655440000"
        }

        result = lambda_handler(event, None)

        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "Missing required fields" in body["message"]

    def test_none_evidence_payload(self):
        """Test that None evidence_payload returns 400 error."""
        event = {
            "evidence_payload": None,
            "subject_id": "550e8400-e29b-41d4-a716-446655440000"
        }

        result = lambda_handler(event, None)

        assert result["statusCode"] == 400

    def test_preserves_all_fields(self):
        """Test that all fields except subject_id are preserved."""
        event = {
            "evidence_payload": [
                {
                    "subject_id": "temporary-id",
                    "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
                    "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000311",
                    "creator_id": "test-user",
                    "creator_type": "system",
                    "qualifiers": ["negated", "family"],
                    "additional_field": "should-be-preserved"
                }
            ],
            "subject_id": "new-uuid"
        }

        result = lambda_handler(event, None)

        assert result["statusCode"] == 200
        fixed_evidence = result["fixed_evidence_payload"][0]
        assert fixed_evidence["subject_id"] == "new-uuid"
        assert fixed_evidence["term_iri"] == "http://purl.obolibrary.org/obo/HP_0001250"
        assert fixed_evidence["evidence_type"] == "http://purl.obolibrary.org/obo/ECO_0000311"
        assert fixed_evidence["creator_id"] == "test-user"
        assert fixed_evidence["creator_type"] == "system"
        assert fixed_evidence["qualifiers"] == ["negated", "family"]
        assert fixed_evidence["additional_field"] == "should-be-preserved"

    def test_preserves_qualifiers_list(self):
        """Test that qualifiers list is preserved correctly."""
        event = {
            "evidence_payload": [
                {
                    "subject_id": "temp",
                    "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
                    "qualifiers": ["negated", "family", "hypothetical"]
                }
            ],
            "subject_id": "uuid"
        }

        result = lambda_handler(event, None)

        assert result["statusCode"] == 200
        assert result["fixed_evidence_payload"][0]["qualifiers"] == ["negated", "family", "hypothetical"]

    def test_empty_qualifiers_list(self):
        """Test that empty qualifiers list is preserved."""
        event = {
            "evidence_payload": [
                {
                    "subject_id": "temp",
                    "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
                    "qualifiers": []
                }
            ],
            "subject_id": "uuid"
        }

        result = lambda_handler(event, None)

        assert result["statusCode"] == 200
        assert result["fixed_evidence_payload"][0]["qualifiers"] == []

    def test_different_temporary_subject_id_formats(self):
        """Test replacement works regardless of temporary subject_id format."""
        event = {
            "evidence_payload": [
                {
                    "subject_id": "project-123#subject-abc",
                    "term_iri": "http://purl.obolibrary.org/obo/HP_0001250"
                },
                {
                    "subject_id": "different-format",
                    "term_iri": "http://purl.obolibrary.org/obo/HP_0001627"
                }
            ],
            "subject_id": "final-uuid"
        }

        result = lambda_handler(event, None)

        assert result["statusCode"] == 200
        assert result["fixed_evidence_payload"][0]["subject_id"] == "final-uuid"
        assert result["fixed_evidence_payload"][1]["subject_id"] == "final-uuid"

    def test_creates_independent_copies(self):
        """Test that each evidence record is an independent copy."""
        event = {
            "evidence_payload": [
                {
                    "subject_id": "temp-1",
                    "term_iri": "http://purl.obolibrary.org/obo/HP_0001250"
                },
                {
                    "subject_id": "temp-2",
                    "term_iri": "http://purl.obolibrary.org/obo/HP_0001627"
                }
            ],
            "subject_id": "uuid"
        }

        result = lambda_handler(event, None)

        # Modifying one shouldn't affect the other
        result["fixed_evidence_payload"][0]["custom_field"] = "test"
        assert "custom_field" not in result["fixed_evidence_payload"][1]

    def test_uuid_format_subject_id(self):
        """Test with realistic UUID format."""
        uuid = "a1b2c3d4-e5f6-4789-a012-b3c4d5e6f7a8"
        event = {
            "evidence_payload": [
                {
                    "subject_id": "project-1#subject-001",
                    "term_iri": "http://purl.obolibrary.org/obo/HP_0001250"
                }
            ],
            "subject_id": uuid
        }

        result = lambda_handler(event, None)

        assert result["statusCode"] == 200
        assert result["fixed_evidence_payload"][0]["subject_id"] == uuid

    def test_mondo_disease_evidence(self):
        """Test with MONDO disease evidence."""
        event = {
            "evidence_payload": [
                {
                    "subject_id": "project-1#subject-001",
                    "term_iri": "http://purl.obolibrary.org/obo/MONDO_0008840",
                    "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000311",
                    "creator_id": "phenopacket-importer",
                    "creator_type": "system",
                    "qualifiers": []
                }
            ],
            "subject_id": "550e8400-e29b-41d4-a716-446655440000"
        }

        result = lambda_handler(event, None)

        assert result["statusCode"] == 200
        assert result["fixed_evidence_payload"][0]["term_iri"] == "http://purl.obolibrary.org/obo/MONDO_0008840"
        assert result["fixed_evidence_payload"][0]["subject_id"] == "550e8400-e29b-41d4-a716-446655440000"

    def test_system_creator_type_preserved(self):
        """Test that creator_type 'system' is preserved."""
        event = {
            "evidence_payload": [
                {
                    "subject_id": "temp",
                    "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
                    "creator_type": "system"
                }
            ],
            "subject_id": "uuid"
        }

        result = lambda_handler(event, None)

        assert result["statusCode"] == 200
        assert result["fixed_evidence_payload"][0]["creator_type"] == "system"
