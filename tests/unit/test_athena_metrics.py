"""
Unit tests for the instrumented Athena helper.

These exist because the measurement they support is expensive. The figures reported for
reviewer comment 3 come from a single session behind a multi-hour bulk import, so a fault in
statistics extraction or cost arithmetic would not be discovered until after that session had
been paid for. Everything here runs against a stubbed Athena client.

The specific regression guarded: the helper this replaced returned only wall-clock elapsed
time and discarded QueryExecution.Statistics, even though its poll loop already held the
response carrying them.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "integration"))

from athena_metrics import (  # noqa: E402
    BYTES_PER_TB,
    MIN_BILLED_BYTES,
    PRICE_PER_TB_USD,
    AthenaQueryResult,
    estimate_cost_usd,
    pricing_basis,
    run_query,
)


QID = "abcd-1234"

STATISTICS = {
    "EngineExecutionTimeInMillis": 3000,
    "TotalExecutionTimeInMillis": 3400,
    "QueryQueueTimeInMillis": 120,
    "QueryPlanningTimeInMillis": 210,
    "ServiceProcessingTimeInMillis": 70,
    "DataScannedInBytes": 2 * BYTES_PER_TB,
}


class FakeAthena:
    """
    Minimal stand-in for the Athena client.

    states: the sequence of states returned by successive get_query_execution calls, so a
    test can exercise the poll loop rather than only the terminal case.
    """

    def __init__(self, states=("SUCCEEDED",), statistics=None, rows=None, reuse=None,
                 state_change_reason=""):
        self.states = list(states)
        self.statistics = dict(STATISTICS if statistics is None else statistics)
        if reuse is not None:
            self.statistics["ResultReuseInformation"] = {"ReusedPreviousResult": reuse}
        self.rows = rows if rows is not None else [["HP:0001250", "42"]]
        self.state_change_reason = state_change_reason
        self.start_kwargs = None
        self.get_query_execution_calls = 0
        self.stopped = False

    def start_query_execution(self, **kwargs):
        self.start_kwargs = kwargs
        return {"QueryExecutionId": QID}

    def get_query_execution(self, QueryExecutionId):
        self.get_query_execution_calls += 1
        state = self.states[min(self.get_query_execution_calls - 1, len(self.states) - 1)]
        return {
            "QueryExecution": {
                "Status": {"State": state, "StateChangeReason": self.state_change_reason},
                "Statistics": self.statistics,
            }
        }

    def get_query_results(self, QueryExecutionId, MaxResults=1000):
        header = {"Data": [{"VarCharValue": "term_iri"}, {"VarCharValue": "n"}]}
        data_rows = [
            {"Data": [{"VarCharValue": v} if v is not None else {} for v in row]}
            for row in self.rows
        ]
        return {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": "term_iri"}, {"Name": "n"}]
                },
                "Rows": [header] + data_rows,
            }
        }

    def stop_query_execution(self, QueryExecutionId):
        self.stopped = True


def execute(client, **kwargs):
    return run_query(
        "SELECT 1",
        database="db",
        output_location="s3://bucket/results/",
        workgroup="primary",
        poll_s=0,
        athena_client=client,
        **kwargs,
    )


class TestStatisticsAreCaptured:
    """The regression: service metrics must survive, not be discarded."""

    def test_all_three_reviewer_requested_metrics_present(self):
        result = execute(FakeAthena())
        assert result.engine_execution_ms == 3000
        assert result.total_execution_ms == 3400
        assert result.data_scanned_bytes == 2 * BYTES_PER_TB

    def test_component_timings_present(self):
        result = execute(FakeAthena())
        assert result.queue_ms == 120
        assert result.planning_ms == 210
        assert result.service_processing_ms == 70

    def test_query_execution_id_retained(self):
        assert execute(FakeAthena()).query_execution_id == QID

    def test_no_extra_get_query_execution_call_for_statistics(self):
        """
        The statistics come from the response the poll loop already holds. A single
        SUCCEEDED state must therefore mean exactly one call.
        """
        client = FakeAthena(states=("SUCCEEDED",))
        execute(client)
        assert client.get_query_execution_calls == 1

    def test_statistics_survive_polling(self):
        client = FakeAthena(states=("QUEUED", "RUNNING", "SUCCEEDED"))
        result = execute(client)
        assert client.get_query_execution_calls == 3
        assert result.engine_execution_ms == 3000

    def test_missing_statistics_yields_none_not_crash(self):
        """An absent Statistics block must be reportable as unknown, not raise."""
        client = FakeAthena()
        client.statistics = {}
        result = execute(client)
        assert result.engine_execution_ms is None
        assert result.data_scanned_bytes is None
        assert result.estimated_cost_usd is None
        assert result.billed_at_minimum is None


class TestResultsAndRows:
    def test_columns_and_rows(self):
        result = execute(FakeAthena(rows=[["HP:0001250", "42"], ["HP:0004322", "7"]]))
        assert result.columns == ["term_iri", "n"]
        assert result.rows == [["HP:0001250", "42"], ["HP:0004322", "7"]]

    def test_header_row_is_not_returned_as_data(self):
        assert len(execute(FakeAthena(rows=[["a", "1"]])).rows) == 1

    def test_null_cell_becomes_none(self):
        result = execute(FakeAthena(rows=[["HP:0001250", None]]))
        assert result.rows == [["HP:0001250", None]]

    def test_empty_result_set(self):
        result = execute(FakeAthena(rows=[]))
        assert result.rows == []
        assert result.columns == ["term_iri", "n"]


class TestResultReuse:
    """
    Reuse must be observed, not asserted. The supplement previously claimed reuse "is not
    enabled" on the strength of a workgroup setting that is not captured anywhere in the
    deployment; the value reported per query is what can actually be defended.
    """

    def test_reuse_not_requested_by_default(self):
        client = FakeAthena()
        result = execute(client)
        assert "ResultReuseConfiguration" not in client.start_kwargs
        assert result.result_reuse_requested is False

    def test_reuse_requested_when_asked(self):
        client = FakeAthena()
        result = execute(client, request_result_reuse=True)
        assert client.start_kwargs["ResultReuseConfiguration"] == {
            "ResultReuseByAgeConfiguration": {"Enabled": True}
        }
        assert result.result_reuse_requested is True

    def test_reported_reuse_is_read_back_from_the_service(self):
        assert execute(FakeAthena(reuse=True)).reused_previous_result is True
        assert execute(FakeAthena(reuse=False)).reused_previous_result is False

    def test_absent_reuse_information_is_none(self):
        """Not the same as False: the service said nothing, so neither do we."""
        assert execute(FakeAthena()).reused_previous_result is None


class TestCostEstimation:
    def test_two_terabytes_at_five_dollars(self):
        assert estimate_cost_usd(2 * BYTES_PER_TB) == pytest.approx(2 * PRICE_PER_TB_USD)

    def test_binary_terabyte_is_the_base(self):
        """
        Using a decimal TB would understate cost by about 10%. Pinned because the difference
        is large enough to matter in a published figure and invisible in the result.
        """
        assert estimate_cost_usd(BYTES_PER_TB) == pytest.approx(PRICE_PER_TB_USD)
        assert estimate_cost_usd(10 ** 12) < PRICE_PER_TB_USD

    def test_small_scan_is_billed_at_the_ten_megabyte_floor(self):
        """The reason 15-record measurements cannot produce a meaningful cost figure."""
        tiny = estimate_cost_usd(1_234)
        floor = estimate_cost_usd(MIN_BILLED_BYTES)
        assert tiny == floor

    def test_floor_flag_distinguishes_the_two_cases(self):
        below = AthenaQueryResult(
            columns=[], rows=[], elapsed_seconds=0.0, query_execution_id=QID,
            engine_execution_ms=1, total_execution_ms=1, queue_ms=0, planning_ms=0,
            service_processing_ms=0, data_scanned_bytes=MIN_BILLED_BYTES - 1,
            reused_previous_result=False, result_reuse_requested=False,
        )
        above = below._replace(data_scanned_bytes=MIN_BILLED_BYTES + 1)
        assert below.billed_at_minimum is True
        assert above.billed_at_minimum is False

    def test_exactly_at_the_floor_is_not_below_it(self):
        at = AthenaQueryResult(
            columns=[], rows=[], elapsed_seconds=0.0, query_execution_id=QID,
            engine_execution_ms=1, total_execution_ms=1, queue_ms=0, planning_ms=0,
            service_processing_ms=0, data_scanned_bytes=MIN_BILLED_BYTES,
            reused_previous_result=False, result_reuse_requested=False,
        )
        assert at.billed_at_minimum is False

    def test_zero_bytes_still_costs_the_floor(self):
        assert estimate_cost_usd(0) == estimate_cost_usd(MIN_BILLED_BYTES)

    def test_none_bytes_gives_none(self):
        assert estimate_cost_usd(None) is None


class TestFailureHandling:
    def test_failed_query_raises_with_reason(self):
        client = FakeAthena(states=("FAILED",), state_change_reason="SYNTAX_ERROR: nope")
        with pytest.raises(RuntimeError, match="SYNTAX_ERROR"):
            execute(client)

    def test_cancelled_query_raises(self):
        with pytest.raises(RuntimeError, match="CANCELLED"):
            execute(FakeAthena(states=("CANCELLED",)))

    def test_timeout_stops_the_query(self):
        """A timed-out query must be stopped, not left running and billed."""
        client = FakeAthena(states=("RUNNING",))
        with pytest.raises(TimeoutError):
            run_query(
                "SELECT 1", database="db", output_location="s3://b/r/",
                timeout_s=-1, poll_s=0, athena_client=client,
            )
        assert client.stopped is True


class TestRequestConstruction:
    def test_workgroup_passed_when_given(self):
        client = FakeAthena()
        execute(client)
        assert client.start_kwargs["WorkGroup"] == "primary"

    def test_workgroup_omitted_when_none(self):
        client = FakeAthena()
        run_query("SELECT 1", database="db", output_location="s3://b/r/",
                  workgroup=None, poll_s=0, athena_client=client)
        assert "WorkGroup" not in client.start_kwargs

    def test_database_and_output_location(self):
        client = FakeAthena()
        execute(client)
        assert client.start_kwargs["QueryExecutionContext"] == {"Database": "db"}
        assert client.start_kwargs["ResultConfiguration"] == {
            "OutputLocation": "s3://bucket/results/"
        }


class TestSerialization:
    def test_to_dict_carries_the_reported_figures(self):
        payload = execute(FakeAthena()).to_dict()
        assert payload["engine_execution_ms"] == 3000
        assert payload["data_scanned_bytes"] == 2 * BYTES_PER_TB
        assert payload["estimated_cost_usd"] == pytest.approx(2 * PRICE_PER_TB_USD)
        assert payload["billed_at_minimum"] is False

    def test_to_dict_excludes_result_rows(self):
        """Artifacts record measurements, not query output."""
        payload = execute(FakeAthena(rows=[["HP:0001250", "42"]])).to_dict()
        assert "rows" not in payload
        assert payload["n_result_rows"] == 1

    def test_to_dict_is_json_serializable(self):
        import json
        json.dumps(execute(FakeAthena()).to_dict())

    def test_wall_clock_is_labelled_as_such(self):
        """It must not be mistakable for a query time in the artifact."""
        payload = execute(FakeAthena()).to_dict()
        assert "wall_clock_seconds" in payload
        assert "elapsed_seconds" not in payload

    def test_summary_mentions_engine_time_and_scan(self):
        summary = execute(FakeAthena()).summary()
        assert "engine=3,000ms" in summary
        assert "scanned=" in summary

    def test_summary_flags_the_billing_floor(self):
        client = FakeAthena(statistics={**STATISTICS, "DataScannedInBytes": 1_000})
        assert "10MB floor" in execute(client).summary()


class TestPricingBasis:
    def test_records_the_inputs_a_published_figure_needs(self):
        basis = pricing_basis()
        assert basis["region"] == "us-east-2"
        assert basis["price_per_tb_usd"] == 5.00
        assert basis["minimum_billed_bytes_per_query"] == MIN_BILLED_BYTES
        assert "binary" in basis["terabyte_definition"]
        assert basis["basis_date"]

    def test_states_what_the_estimate_excludes(self):
        assert pricing_basis()["excludes"]
