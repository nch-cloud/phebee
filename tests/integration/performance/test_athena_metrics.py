"""
Athena query time, bytes scanned and estimated cost at benchmark scale, plus the Iceberg
table footprint.

Why this module exists separately from the evaluation suite: the Athena queries in
tests/integration/evaluation/test_evaluation_end_to_end.py run against the module-scoped
golden_evaluation_import fixture, which is 5 subjects x 3 terms x 2 evidence items. At that
size DataScannedInBytes falls under Athena's 10 MB per-query billing minimum, so a cost
figure is the floor charge regardless of the query and a query time is dominated by
planning. Those tests can show that the queries succeed; they cannot show what the queries
cost. This module runs the same query shapes against an already-ingested benchmark project.

Prerequisite: a completed bulk import, normally
    pytest -v -s tests/integration/performance/test_import_performance.py
with PHEBEE_EVAL_BENCHMARK_DIR pointing at a benchmark dataset. This module does not
ingest; it measures what is already there.

Partitioning and what "pruned" can mean here. The evidence table is
    PARTITIONED BY (bucket(64, subject_id), created_date)
which has three consequences that shape every measurement below:

  1. run_id is not a partition key, so WHERE run_id = '...' cannot prune. Both cohort-wide
     published queries filter only on run_id and therefore scan the table.
  2. created_date is the *import* date, not the clinical note date, so one import writes
     one value (two if it straddles midnight UTC). Adding created_date to a predicate
     therefore prunes to the import — which is nearly the whole table when the table holds
     one large benchmark run. The created_date variants below exist to measure that
     non-effect rather than to assume it.
  3. subject_id bucketing is the only pruning that actually applies, and it applies to
     subject-scoped queries. So the meaningful pruned/unpruned pair is the term_source
     lookup located by subject_id (1 of 64 buckets) against the same row located by
     termlink_id alone (full scan).

The general finding this is set up to quantify: a cohort-wide aggregation cannot be pruned
by this partition spec, because pruning requires restricting subjects and a cohort query is
by definition not restricted to subjects.

First-pass versus repeat, rather than cold versus warm. Each query runs twice. That
isolates per-query effects, but table-level Glue and S3 metadata warmth is established by
whichever query runs first in the session and cannot be re-cooled from inside it, so the
first pass of the first query is the only genuinely cold measurement. Labels are pass_1 and
pass_2 rather than cold and warm for that reason.

Result reuse is not requested; see athena_metrics.run_query. It is therefore false for every
execution here, which makes it a control on the measurement rather than a figure to report:
a repeat pass served from a reused result would have a meaningless time and zero bytes
scanned, and the first-versus-repeat comparison above would collapse. Each artifact row
carries reused_previous_result as the service reported it and the test asserts none is true,
but it is not a column in the published table, where it would be one value repeated.

Environment:
    PHEBEE_EVAL_SCALE=1                      required, as elsewhere in this suite
    PHEBEE_EVAL_ATHENA_RUN_ID                run_id to measure; default: newest import_run.json
    PHEBEE_EVAL_BENCHMARK_DIR                dataset whose metadata.json supplies expected counts
    PHEBEE_EVAL_ATHENA_METRICS_DIR           artifact output; default /tmp/phebee-eval-artifacts/<run_id>
    PHEBEE_EVAL_ATHENA_EXPECTED_EVIDENCE     override expected evidence count
    PHEBEE_EVAL_ATHENA_EXPECTED_RECORDS      override expected termlink count
"""
from __future__ import annotations

import csv
import glob
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

# athena_metrics resolves via pytest.ini's pythonpath (tests/integration).
from athena_metrics import (
    AthenaQueryResult,
    athena_config,
    pricing_basis,
    run_query,
)

pytestmark = [pytest.mark.integration, pytest.mark.perf]

ARTIFACT_ROOT = Path("/tmp/phebee-eval-artifacts")
PASSES = ("pass_1", "pass_2")


# -----------------------------
# Locating the run to measure
# -----------------------------

def _newest_import_run() -> Optional[Dict[str, Any]]:
    """
    The most recent import_run.json under /tmp/phebee-eval-artifacts.

    test_import_performance.py mints a fresh run_id per execution and writes it there. This
    picks the newest so the common case needs no configuration, but the chosen run is always
    printed, because silently measuring a stale run would produce plausible numbers for the
    wrong dataset.
    """
    candidates = glob.glob(str(ARTIFACT_ROOT / "*" / "import_run.json"))
    if not candidates:
        return None
    newest = max(candidates, key=os.path.getmtime)
    with open(newest) as fh:
        payload = json.load(fh)
    payload["_artifact_path"] = newest
    return payload


@pytest.fixture(scope="module")
def measured_run() -> Dict[str, Any]:
    if os.environ.get("PHEBEE_EVAL_SCALE") != "1":
        pytest.skip("Set PHEBEE_EVAL_SCALE=1 to run Athena metric measurement.")

    explicit = os.environ.get("PHEBEE_EVAL_ATHENA_RUN_ID")
    if explicit:
        run = {"run_id": explicit, "_source": "PHEBEE_EVAL_ATHENA_RUN_ID"}
    else:
        found = _newest_import_run()
        if not found:
            pytest.skip(
                "No run to measure. Run test_import_performance.py first, or set "
                "PHEBEE_EVAL_ATHENA_RUN_ID."
            )
        run = {
            "run_id": found["run_id"],
            "project_id": found.get("project_id"),
            "_source": found["_artifact_path"],
        }

    print(f"\n[ATHENA] Measuring run_id={run['run_id']} (from {run['_source']})")
    if run.get("project_id"):
        print(f"[ATHENA] project_id={run['project_id']}")
    return run


@pytest.fixture(scope="module")
def expected_counts() -> Dict[str, Optional[int]]:
    """
    Expected ingested counts, from the benchmark dataset's manifest.

    These are the *input* manifest. Comparing them against what Athena reports is the point:
    test_import_performance.py prints the same figures from the manifest before uploading
    anything, so reading its output back is circular and verifies nothing about what Neptune
    and Iceberg received.
    """
    evidence = os.environ.get("PHEBEE_EVAL_ATHENA_EXPECTED_EVIDENCE")
    records = os.environ.get("PHEBEE_EVAL_ATHENA_EXPECTED_RECORDS")
    if evidence and records:
        return {"n_evidence": int(evidence), "n_records": int(records), "_source": "environment"}

    benchmark_dir = os.environ.get("PHEBEE_EVAL_BENCHMARK_DIR")
    if not benchmark_dir:
        return {"n_evidence": None, "n_records": None, "_source": None}

    manifest = Path(benchmark_dir) / "metadata.json"
    if not manifest.exists():
        return {"n_evidence": None, "n_records": None, "_source": None}

    with open(manifest) as fh:
        stats = json.load(fh)["dataset_statistics"]
    return {
        "n_evidence": stats["n_evidence"],
        "n_records": stats["n_records"],
        "_source": str(manifest),
    }


@pytest.fixture(scope="module")
def athena(aws_session, cloudformation_stack) -> Dict[str, str]:
    if os.environ.get("PHEBEE_EVAL_SCALE") != "1":
        pytest.skip("Set PHEBEE_EVAL_SCALE=1 to run Athena metric measurement.")
    config = athena_config(aws_session, cloudformation_stack)
    print(
        f"[ATHENA] database={config['database']} table={config['evidence_table']} "
        f"workgroup={config['workgroup']}"
    )
    return config


# -----------------------------
# Query execution and recording
# -----------------------------

def _group_by_query(measurements: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Measurements keyed by query name, preserving execution order within each query."""
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for m in measurements:
        grouped.setdefault(m["query"], []).append(m)
    return grouped


class Recorder:
    """Accumulates measurements so every one survives a later assertion failure."""

    def __init__(self, run_id: str, athena: Dict[str, str]):
        self.run_id = run_id
        self.athena = athena
        self.measurements: List[Dict[str, Any]] = []

    def run(
        self,
        name: str,
        sql: str,
        *,
        published: bool,
        prunes: str,
        note: str = "",
        passes: int = 2,
    ) -> List[AthenaQueryResult]:
        """
        Execute one query `passes` times, recording each execution.

        published: True when this is a query shape the manuscript reports on, False when it
                   is a variant included to quantify a partitioning effect.
        prunes:    what the partition spec can prune for this predicate — "none",
                   "subject_bucket", or "import_date".
        """
        results: List[AthenaQueryResult] = []
        for i in range(passes):
            label = PASSES[i] if i < len(PASSES) else f"pass_{i + 1}"
            result = run_query(
                sql,
                database=self.athena["database"],
                output_location=self.athena["output_location"],
                workgroup=self.athena["workgroup"],
            )
            print(f"[ATHENA] {name} {label}: {result.summary()}")
            row: Dict[str, Any] = {
                "query": name,
                "pass": label,
                "published_query": published,
                "prunes": prunes,
                "note": note,
                "sql": " ".join(sql.split()),
            }
            row.update(result.to_dict())
            self.measurements.append(row)
            results.append(result)
        return results

    def write(self, name: str, extra: Dict[str, Any]) -> Path:
        """
        Persist this recorder's measurements under `name`.

        Each test writes its own pair of files. A single shared filename would have the
        later test silently overwrite the earlier one's numbers.
        """
        out_dir = Path(
            os.environ.get("PHEBEE_EVAL_ATHENA_METRICS_DIR", str(ARTIFACT_ROOT / self.run_id))
        )
        out_dir.mkdir(parents=True, exist_ok=True)

        payload = {
            "run_id": self.run_id,
            "athena": {
                "database": self.athena["database"],
                "evidence_table": self.athena["evidence_table"],
                "workgroup": self.athena["workgroup"],
            },
            "pricing_basis": pricing_basis(),
            "partition_spec": "bucket(64, subject_id), created_date",
            "measurements": self.measurements,
            **extra,
        }

        json_path = out_dir / f"{name}.json"
        with open(json_path, "w") as fh:
            json.dump(payload, fh, indent=2, default=str)

        if self.measurements:
            csv_path = out_dir / f"{name}.csv"
            fields = [k for k in self.measurements[0] if k != "sql"]
            with open(csv_path, "w", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(self.measurements)
            print(f"[ATHENA] wrote {csv_path}")

        print(f"[ATHENA] wrote {json_path}")
        return json_path


def _scalar(result: AthenaQueryResult) -> Optional[str]:
    if not result.rows or not result.rows[0]:
        return None
    return result.rows[0][0]


# -----------------------------
# Tests
# -----------------------------

def test_ingested_counts_match_dataset_manifest(measured_run, expected_counts, athena):
    """
    Guard: what Iceberg holds for this run must match the dataset manifest.

    This is the only control on the measurement session. The ingest path has moved
    substantially since the benchmark datasets were generated (qualifier canonicalization,
    typed evidence fields, hash changes), so a match is positive evidence that those changes
    preserved ingestion semantics, and a mismatch is a finding that has to be understood
    before any figure from this run is published.

    Counted via Athena rather than read from the import test's output, because that output
    reports the manifest it was given, not what was stored.
    """
    if expected_counts["n_evidence"] is None:
        pytest.skip(
            "No expected counts available. Set PHEBEE_EVAL_BENCHMARK_DIR, or "
            "PHEBEE_EVAL_ATHENA_EXPECTED_EVIDENCE and PHEBEE_EVAL_ATHENA_EXPECTED_RECORDS."
        )

    db, table = athena["database"], athena["evidence_table"]
    run_id = measured_run["run_id"]
    recorder = Recorder(run_id, athena)

    print(f"[ATHENA] expected counts from {expected_counts['_source']}")

    evidence_result = recorder.run(
        "guard_evidence_count",
        f"SELECT COUNT(*) AS n FROM {db}.{table} WHERE run_id = '{run_id}'",
        published=False,
        prunes="none",
        note="Guard. Also the cleanest full-scan bytes-scanned datapoint for this table.",
        passes=1,
    )[0]

    termlink_result = recorder.run(
        "guard_termlink_count",
        f"SELECT COUNT(DISTINCT termlink_id) AS n FROM {db}.{table} WHERE run_id = '{run_id}'",
        published=False,
        prunes="none",
        note="Guard at assertion grain: one termlink_id per (subject, term, qualifiers).",
        passes=1,
    )[0]

    actual_evidence = _scalar(evidence_result)
    actual_records = _scalar(termlink_result)

    recorder.write(
        "athena_guard_counts",
        {
            "guard": {
                "expected_source": expected_counts["_source"],
                "expected_n_evidence": expected_counts["n_evidence"],
                "expected_n_records": expected_counts["n_records"],
                "actual_n_evidence": actual_evidence,
                "actual_n_records": actual_records,
            }
        }
    )

    print(
        f"[ATHENA] evidence rows: expected {expected_counts['n_evidence']:,} "
        f"actual {actual_evidence}"
    )
    print(
        f"[ATHENA] termlinks:     expected {expected_counts['n_records']:,} "
        f"actual {actual_records}"
    )

    assert actual_evidence is not None, "Evidence count query returned no rows"
    assert int(actual_evidence) == expected_counts["n_evidence"], (
        f"Ingested evidence count {actual_evidence} != manifest "
        f"{expected_counts['n_evidence']}. Do not publish figures from this run until the "
        f"discrepancy is explained."
    )
    assert int(actual_records) == expected_counts["n_records"], (
        f"Ingested termlink count {actual_records} != manifest "
        f"{expected_counts['n_records']}. Do not publish figures from this run until the "
        f"discrepancy is explained."
    )


def test_athena_query_metrics(measured_run, athena):
    """
    Measure the three reported query shapes, plus the variants that quantify pruning.

    Writes athena_metrics.json and athena_metrics.csv before asserting anything, so a failed
    expectation never costs the measurement.
    """
    db, table = athena["database"], athena["evidence_table"]
    run_id = measured_run["run_id"]
    recorder = Recorder(run_id, athena)

    # --- Setup: locate one real row, and the import date, for the subject-scoped queries.
    # Tagged as setup rather than measurement; these are not reported query shapes.
    locator = recorder.run(
        "setup_locate_row",
        f"""
        SELECT subject_id, term_iri, termlink_id, created_date
        FROM {db}.{table}
        WHERE run_id = '{run_id}'
        LIMIT 1
        """,
        published=False,
        prunes="none",
        note="Setup only. Supplies a real subject_id/termlink_id for the lookup queries.",
        passes=1,
    )[0]
    assert locator.rows, f"No evidence rows found for run_id={run_id}"
    subject_id, term_iri, termlink_id, created_date = locator.rows[0][:4]
    print(
        f"[ATHENA] locator subject_id={subject_id} termlink_id={termlink_id} "
        f"created_date={created_date}"
    )

    import_dates = recorder.run(
        "setup_import_dates",
        f"SELECT DISTINCT created_date FROM {db}.{table} WHERE run_id = '{run_id}'",
        published=False,
        prunes="none",
        note="Setup only. created_date is the import date, so a single run yields one or two.",
        passes=1,
    )[0]
    distinct_dates = [r[0] for r in import_dates.rows if r and r[0]]
    print(f"[ATHENA] distinct created_date values for this run: {distinct_dates}")

    # --- Published query 1: cohort-wide term frequency. Cannot be pruned.
    recorder.run(
        "term_distribution",
        f"""
        SELECT term_iri, COUNT(*) AS n
        FROM {db}.{table}
        WHERE run_id = '{run_id}'
        GROUP BY term_iri
        ORDER BY n DESC
        LIMIT 10
        """,
        published=True,
        prunes="none",
        note="Reported as 'term frequency counts'. run_id is not a partition key.",
    )

    # --- Published query 2: per-subject feature aggregation. Cannot be pruned.
    recorder.run(
        "per_subject_terms",
        f"""
        SELECT subject_id, COUNT(DISTINCT term_iri) AS n_terms
        FROM {db}.{table}
        WHERE run_id = '{run_id}'
        GROUP BY subject_id
        ORDER BY n_terms DESC
        LIMIT 10
        """,
        published=True,
        prunes="none",
        note="Reported as 'per-subject feature aggregation'. Aggregates across all subjects.",
    )

    # --- Variant: does adding the partitioned import date help a cohort query?
    # Expected not to, when the run is most of the table. Measured rather than assumed.
    if created_date:
        recorder.run(
            "term_distribution_import_date",
            f"""
            SELECT term_iri, COUNT(*) AS n
            FROM {db}.{table}
            WHERE run_id = '{run_id}'
              AND created_date = DATE '{created_date}'
            GROUP BY term_iri
            ORDER BY n DESC
            LIMIT 10
            """,
            published=False,
            prunes="import_date",
            note=(
                "Variant of term_distribution with the partitioned import date added. "
                "Quantifies how little partition pruning offers a cohort query when the "
                "measured run dominates the table."
            ),
        )

    # --- Published query 3: term_source struct retrieval, located by subject. Bucket-pruned.
    recorder.run(
        "term_source_struct",
        f"""
        SELECT
            term_source.source AS term_source_source,
            term_source.version AS term_source_version,
            term_source.iri AS term_source_iri
        FROM {db}.{table}
        WHERE subject_id = '{subject_id}'
          AND term_iri = '{term_iri}'
          AND termlink_id = '{termlink_id}'
        LIMIT 1
        """,
        published=True,
        prunes="subject_bucket",
        note=(
            "Reported as context/provenance/ontology metadata retrieval. subject_id is the "
            "bucketing column, so this prunes to 1 of 64 buckets."
        ),
    )

    # --- Variant: the same single row, located without the bucketing column.
    # This is the meaningful pruned/unpruned pair: identical result, two scan costs.
    recorder.run(
        "term_source_struct_no_bucket",
        f"""
        SELECT
            term_source.source AS term_source_source,
            term_source.version AS term_source_version,
            term_source.iri AS term_source_iri
        FROM {db}.{table}
        WHERE termlink_id = '{termlink_id}'
        LIMIT 1
        """,
        published=False,
        prunes="none",
        note=(
            "Same row as term_source_struct, located by termlink_id alone. Isolates what "
            "subject_id bucket pruning buys, holding the result constant."
        ),
    )

    metrics_path = recorder.write(
        "athena_metrics",
        {
            "locator": {
                "subject_id": subject_id,
                "term_iri": term_iri,
                "termlink_id": termlink_id,
                "created_date": created_date,
                "distinct_created_dates": distinct_dates,
            }
        }
    )

    # --- Post-conditions. Asserted after the artifact is on disk.
    published = [m for m in recorder.measurements if m["published_query"]]
    assert published, "No published query shapes were measured"

    missing_stats = [
        f"{m['query']}/{m['pass']}"
        for m in recorder.measurements
        if m["data_scanned_bytes"] is None or m["engine_execution_ms"] is None
    ]
    assert not missing_stats, (
        f"Athena returned no Statistics for: {missing_stats}. Without them there is nothing "
        f"to report. Metrics written to {metrics_path}."
    )

    reused = [
        f"{m['query']}/{m['pass']}"
        for m in recorder.measurements
        if m["reused_previous_result"]
    ]
    assert not reused, (
        f"Result reuse occurred for {reused}, which invalidates the bytes-scanned and cost "
        f"figures for those executions. Reuse is not requested, so this indicates workgroup "
        f"configuration outside this repo."
    )

    # Bytes scanned is a function of the query and the data, not of service state, so two
    # executions of the same query against an unchanged table must scan the same amount.
    # This is a stricter control than the reuse flag above and catches more: reuse that the
    # service did not report, Athena-side caching, and concurrent writes moving the table
    # under the measurement. Reported rather than asserted, because the artifact is already
    # on disk and a soft discrepancy should not fail a session that cost hours of ingest --
    # but any difference at all needs explaining before the numbers are published.
    for query, passes in _group_by_query(recorder.measurements).items():
        scans = {m["pass"]: m["data_scanned_bytes"] for m in passes}
        distinct = set(scans.values())
        if len(distinct) > 1:
            lo, hi = min(distinct), max(distinct)
            print(
                f"[ATHENA] WARNING: {query} scanned different amounts across passes: "
                f"{scans}. Spread {hi - lo:,} B ({(hi - lo) / hi:.2%} of the larger). Two "
                f"executions of one query over an unchanged table should scan identically, "
                f"so investigate before publishing: unreported result reuse, Athena-side "
                f"caching, or writes to the table during the session."
            )

    at_floor = [m["query"] for m in published if m["billed_at_minimum"]]
    if at_floor:
        print(
            f"[ATHENA] WARNING: these published queries scanned less than the 10 MB billing "
            f"minimum, so their cost is the floor charge and not a function of the query: "
            f"{sorted(set(at_floor))}. Check that the measured run is the benchmark-scale one."
        )


def test_iceberg_table_footprint(measured_run, athena):
    """
    Iceberg table footprint from the current snapshot summary (W4-07).

    Read from the snapshot summary rather than by listing the S3 prefix, so that orphaned
    files from earlier runs, expired snapshots, and Athena's own query results — which land
    in the same bucket — are excluded. total-files-size is the on-disk size of the data files
    the current snapshot references.

    Note the scope: this is the whole table, not just the measured run. When the table holds
    other data, the figure is not attributable to this run's records alone, and the guard
    counts are what establish how much of it belongs to the run.
    """
    db, table = athena["database"], athena["evidence_table"]
    recorder = Recorder(measured_run["run_id"], athena)

    snapshot = recorder.run(
        "iceberg_snapshot_summary",
        f"""
        SELECT committed_at, snapshot_id, summary
        FROM "{db}"."{table}$snapshots"
        ORDER BY committed_at DESC
        LIMIT 1
        """,
        published=False,
        prunes="none",
        note="Metadata query for the table footprint. Not an evidence-table scan.",
        passes=1,
    )[0]

    assert snapshot.rows, f"No Iceberg snapshots found for {db}.{table}"
    committed_at, snapshot_id, summary = snapshot.rows[0][:3]
    print(f"[ATHENA] snapshot {snapshot_id} committed_at={committed_at}")
    print(f"[ATHENA] summary={summary}")

    partitions = recorder.run(
        "iceberg_partition_count",
        f'SELECT COUNT(*) AS n FROM "{db}"."{table}$partitions"',
        published=False,
        prunes="none",
        note="Partition count, for interpreting how much pruning is available.",
        passes=1,
    )[0]
    print(f"[ATHENA] partitions: {_scalar(partitions)}")

    recorder.write(
        "athena_iceberg_footprint",
        {
            "iceberg_footprint": {
                "committed_at": committed_at,
                "snapshot_id": snapshot_id,
                "summary_raw": summary,
                "n_partitions": _scalar(partitions),
                "scope": "whole table, not only the measured run",
            }
        }
    )
