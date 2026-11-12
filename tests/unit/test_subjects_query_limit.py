"""The page size /subjects/query applies before it reads anything.

A page of benchmark subjects (~320 phenotypes each) passes Lambda's 6 MB
response cap at ~220 subjects, so the handler defaults to 100 and reduces
larger requests to 200. Clients follow next_cursor for the rest.
"""
import json
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.modules.setdefault("aws_lambda_powertools", MagicMock())

with patch("phebee.utils.aws.get_client"):
    import get_subjects_pheno


def _limit_sent(body):
    query = MagicMock(return_value={"subjects": [], "pagination": {}})
    with patch.object(get_subjects_pheno, "query_subjects_by_project", query), \
            patch.object(get_subjects_pheno, "get_current_term_source_version", return_value="v1"):
        get_subjects_pheno.lambda_handler({"body": json.dumps({"project_id": "p1", **body})}, None)
    return query.call_args.kwargs["limit"]


def test_default_page_is_100():
    assert _limit_sent({}) == 100


def test_limit_within_the_maximum_is_kept():
    assert _limit_sent({"limit": 25}) == 25


def test_limit_over_the_maximum_is_reduced_to_200():
    assert _limit_sent({"limit": 1000}) == 200


def _response(body):
    with patch.object(get_subjects_pheno, "query_subjects_by_project", MagicMock()), \
            patch.object(get_subjects_pheno, "get_current_term_source_version", return_value="v1"):
        return get_subjects_pheno.lambda_handler({"body": json.dumps(body)}, None)


def test_bad_limit_is_a_400_not_a_function_error():
    response = _response({"project_id": "p1", "limit": "lots"})
    assert response["statusCode"] == 400
    assert "limit" in json.loads(response["body"])["message"]


def test_missing_project_id_is_a_400():
    response = _response({})
    assert response["statusCode"] == 400
    assert "project_id" in json.loads(response["body"])["message"]


def test_mutually_exclusive_term_filters_are_a_400():
    response = _response({
        "project_id": "p1",
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
        "term_association_source_entity": "MONDO:0007947",
    })
    assert response["statusCode"] == 400
    assert "mutually exclusive" in json.loads(response["body"])["message"]
