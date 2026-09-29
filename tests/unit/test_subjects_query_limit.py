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
