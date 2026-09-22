"""
Instrumented Athena query execution, for reporting query time, bytes scanned and cost.

The helper this replaces returned only wall-clock elapsed time. That is not usable as a
reported query time: it spans start_query_execution through get_query_results and includes
up to one full poll interval, so a query whose engine time is 3 s can measure 5 s. Athena
returns the authoritative figures in QueryExecution.Statistics, and the poll loop already
holds that response, so this module keeps them instead of discarding them.

Billing note: DataScannedInBytes is the billable quantity, and Athena rounds each query up
to a 10 MB minimum. A cost derived from a small scan is therefore the per-query floor
rather than a function of the query, which is why cost figures are only meaningful once
the table is large. See estimate_cost_usd.

Result reuse is deliberately not requested. run_query exposes request_result_reuse so the
choice is explicit in the call rather than implied by an omission, and every result carries
reused_previous_result as read back from the service, so the reuse status of a reported
measurement is observed rather than asserted.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, NamedTuple, Optional

import boto3


# --- Pricing basis -----------------------------------------------------------------
# Recorded here so that a reported cost can be traced to the inputs that produced it.
# Update PRICE_BASIS_DATE whenever the rate is re-checked, and cite it wherever a cost
# figure is published.
PRICE_REGION = "us-east-2"
PRICE_PER_TB_USD = 5.00
PRICE_BASIS_DATE = "2026-09-19"

# Athena bills a 10 MB minimum per query.
MIN_BILLED_BYTES = 10 * 1024 * 1024

# AWS publishes the rate as "per TB scanned" and bills on a binary terabyte. Using a
# decimal TB (10**12) instead would understate cost by about 10%, so the base is named
# explicitly rather than left to a reader's assumption.
BYTES_PER_TB = 1024 ** 4


def estimate_cost_usd(data_scanned_bytes: Optional[int]) -> Optional[float]:
    """
    Estimated query cost from bytes scanned, with the 10 MB per-query minimum applied.

    An estimate, not a billed amount: it is derived from the scan figure Athena reports for
    one query, not from a billing record, and it excludes S3 request and storage charges.
    It cannot be reconciled against Cost Explorer when queries run in a shared workgroup.
    """
    if data_scanned_bytes is None:
        return None
    billed = max(data_scanned_bytes, MIN_BILLED_BYTES)
    return billed / BYTES_PER_TB * PRICE_PER_TB_USD


class AthenaQueryResult(NamedTuple):
    """
    One query execution and everything the service reported about it.

    elapsed_seconds is retained for continuity with the earlier helper but should not be
    published as a query time; use engine_execution_ms, or total_execution_ms when the
    figure needs to include queue and planning.
    """

    columns: List[str]
    rows: List[List[Optional[str]]]
    elapsed_seconds: float
    query_execution_id: str
    engine_execution_ms: Optional[int]
    total_execution_ms: Optional[int]
    queue_ms: Optional[int]
    planning_ms: Optional[int]
    service_processing_ms: Optional[int]
    data_scanned_bytes: Optional[int]
    reused_previous_result: Optional[bool]
    result_reuse_requested: bool

    @property
    def estimated_cost_usd(self) -> Optional[float]:
        return estimate_cost_usd(self.data_scanned_bytes)

    @property
    def billed_at_minimum(self) -> Optional[bool]:
        """True when the 10 MB floor, not the scan, set the cost."""
        if self.data_scanned_bytes is None:
            return None
        return self.data_scanned_bytes < MIN_BILLED_BYTES

    def to_dict(self) -> Dict[str, Any]:
        """Flat, JSON-serializable form for the metrics artifact. Excludes result rows."""
        return {
            "query_execution_id": self.query_execution_id,
            "engine_execution_ms": self.engine_execution_ms,
            "total_execution_ms": self.total_execution_ms,
            "queue_ms": self.queue_ms,
            "planning_ms": self.planning_ms,
            "service_processing_ms": self.service_processing_ms,
            "data_scanned_bytes": self.data_scanned_bytes,
            "estimated_cost_usd": self.estimated_cost_usd,
            "billed_at_minimum": self.billed_at_minimum,
            "reused_previous_result": self.reused_previous_result,
            "result_reuse_requested": self.result_reuse_requested,
            "wall_clock_seconds": round(self.elapsed_seconds, 3),
            "n_result_rows": len(self.rows),
        }

    def summary(self) -> str:
        """One-line human-readable form for test output."""
        scanned = "n/a" if self.data_scanned_bytes is None else f"{self.data_scanned_bytes:,}"
        engine = "n/a" if self.engine_execution_ms is None else f"{self.engine_execution_ms:,}"
        cost = self.estimated_cost_usd
        cost_s = "n/a" if cost is None else f"${cost:.6f}"
        floor = " (at 10MB floor)" if self.billed_at_minimum else ""
        return (
            f"engine={engine}ms wall={self.elapsed_seconds:.2f}s "
            f"scanned={scanned}B cost~{cost_s}{floor} "
            f"reuse={self.reused_previous_result} rows={len(self.rows)}"
        )


def _statistics(query_execution: Dict[str, Any]) -> Dict[str, Any]:
    return query_execution.get("Statistics") or {}


def run_query(
    query: str,
    database: str,
    output_location: str,
    workgroup: Optional[str] = None,
    timeout_s: int = 900,
    poll_s: int = 2,
    request_result_reuse: bool = False,
    athena_client: Any = None,
) -> AthenaQueryResult:
    """
    Execute a query to completion and return its results with the service's own metrics.

    timeout_s defaults higher than the previous helper's 300 s because a cohort-wide
    aggregation over a full-scale table can legitimately exceed five minutes, and a
    timeout there would discard a measurement rather than reveal a fault.

    Only the first page of results is read, matching the previous behavior. Every query
    this is used for is an aggregate or a bounded LIMIT, so paging would add a second
    source of wall-clock variation without changing any reported figure.
    """
    athena = athena_client or boto3.client("athena")

    kwargs: Dict[str, Any] = {
        "QueryString": query,
        "QueryExecutionContext": {"Database": database},
        "ResultConfiguration": {"OutputLocation": output_location},
    }
    if workgroup:
        kwargs["WorkGroup"] = workgroup
    if request_result_reuse:
        kwargs["ResultReuseConfiguration"] = {
            "ResultReuseByAgeConfiguration": {"Enabled": True}
        }

    t0 = time.time()
    start = athena.start_query_execution(**kwargs)
    qid = start["QueryExecutionId"]

    while True:
        q = athena.get_query_execution(QueryExecutionId=qid)
        state = q["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if (time.time() - t0) > timeout_s:
            athena.stop_query_execution(QueryExecutionId=qid)
            raise TimeoutError(f"Athena query timed out after {timeout_s}s: {qid}")
        time.sleep(poll_s)

    if state != "SUCCEEDED":
        reason = q["QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Athena query failed: state={state} reason={reason} qid={qid}")

    # The terminal get_query_execution response is already in hand, so the statistics cost
    # no additional API call.
    execution = q["QueryExecution"]
    stats = _statistics(execution)
    reuse_info = stats.get("ResultReuseInformation") or {}

    results = athena.get_query_results(QueryExecutionId=qid, MaxResults=1000)
    cols = [c["Name"] for c in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows: List[List[Optional[str]]] = []
    # First row is the header.
    for r in results["ResultSet"]["Rows"][1:]:
        rows.append([(d.get("VarCharValue") if d else None) for d in r.get("Data", [])])

    elapsed = time.time() - t0

    return AthenaQueryResult(
        columns=cols,
        rows=rows,
        elapsed_seconds=elapsed,
        query_execution_id=qid,
        engine_execution_ms=stats.get("EngineExecutionTimeInMillis"),
        total_execution_ms=stats.get("TotalExecutionTimeInMillis"),
        queue_ms=stats.get("QueryQueueTimeInMillis"),
        planning_ms=stats.get("QueryPlanningTimeInMillis"),
        service_processing_ms=stats.get("ServiceProcessingTimeInMillis"),
        data_scanned_bytes=stats.get("DataScannedInBytes"),
        reused_previous_result=reuse_info.get("ReusedPreviousResult"),
        result_reuse_requested=request_result_reuse,
    )


def athena_config(aws_session, cloudformation_stack: str) -> Dict[str, str]:
    """
    Read the Athena database, evidence table, results location and workgroup from stack
    outputs.

    Note for anyone reporting cost from these queries: AthenaWorkgroup is a CloudFormation
    output whose value is the literal string "primary". There is no AWS::Athena::WorkGroup
    resource in the template, so the workgroup is the account default, its configuration is
    not captured in infrastructure-as-code, and queries share it with anything else in the
    account.
    """
    cf = aws_session.client("cloudformation")
    response = cf.describe_stacks(StackName=cloudformation_stack)
    outputs = {
        o["OutputKey"]: o["OutputValue"]
        for o in response["Stacks"][0].get("Outputs", [])
    }

    config = {
        "database": outputs.get("AthenaDatabase"),
        "evidence_table": outputs.get("AthenaEvidenceTable"),
        "output_location": outputs.get("AthenaResultsLocation"),
        "workgroup": outputs.get("AthenaWorkgroup", "primary"),
    }

    missing = [k for k in ("database", "evidence_table", "output_location") if not config[k]]
    if missing:
        raise AssertionError(
            f"Missing Athena config in stack outputs: {missing}. Found: {config}"
        )
    return config


def pricing_basis() -> Dict[str, Any]:
    """The pricing inputs, for inclusion in the metrics artifact and the table legend."""
    return {
        "region": PRICE_REGION,
        "price_per_tb_usd": PRICE_PER_TB_USD,
        "terabyte_definition": "binary (2**40 bytes)",
        "minimum_billed_bytes_per_query": MIN_BILLED_BYTES,
        "basis_date": PRICE_BASIS_DATE,
        "excludes": ["S3 storage", "S3 request charges", "Glue Data Catalog requests"],
        "note": (
            "Estimated from DataScannedInBytes reported per query, not from a billing "
            "record. Not reconcilable against Cost Explorer because queries run in the "
            "shared default workgroup."
        ),
    }
