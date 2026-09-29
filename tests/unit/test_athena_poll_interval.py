"""How often the interactive Athena helpers poll for a query's completion.

A query is seen as finished half a poll interval late on average. At 0.5 s that
was ~250 ms added to every Athena round trip of the query endpoints. The
interval is bounded below by the GetQueryExecution quota (500 calls/s per
account, no burst), which the interval constant's comment works through.
"""
import os
from unittest.mock import MagicMock, patch

import pytest

from phebee.utils import iceberg


@pytest.fixture(autouse=True)
def iceberg_env():
    with patch.dict(os.environ, {"ICEBERG_DATABASE": "phebee", "PHEBEE_BUCKET_NAME": "b"}):
        yield


def _athena(states_before_success):
    client = MagicMock()
    client.start_query_execution.return_value = {"QueryExecutionId": "qid"}
    client.get_query_execution.side_effect = [
        {"QueryExecution": {"Status": {"State": s}}}
        for s in states_before_success + ["SUCCEEDED"]
    ]
    client.get_query_results.return_value = {
        "ResultSet": {
            "ResultSetMetadata": {"ColumnInfo": [{"Name": "n"}]},
            "Rows": [{"Data": [{"VarCharValue": "n"}]}, {"Data": [{"VarCharValue": "1"}]}],
        }
    }
    return client


def _sleeps(run):
    client = _athena(["QUEUED", "RUNNING", "RUNNING"])
    with patch("boto3.client", return_value=client), \
            patch.object(iceberg, "get_workgroup_config_cached", return_value=(True, {})), \
            patch.object(iceberg.time, "sleep") as sleep:
        run()
    return [c.args[0] for c in sleep.call_args_list]


def test_the_interval_is_under_the_old_half_second():
    assert iceberg.ATHENA_POLL_INTERVAL < 0.5


def test_query_iceberg_evidence_polls_at_the_interval():
    sleeps = _sleeps(lambda: iceberg.query_iceberg_evidence("SELECT 1"))
    assert sleeps == [iceberg.ATHENA_POLL_INTERVAL] * 3


def test_execute_athena_query_polls_at_the_interval():
    sleeps = _sleeps(lambda: iceberg._execute_athena_query("SELECT 1"))
    assert sleeps == [iceberg.ATHENA_POLL_INTERVAL] * 3
