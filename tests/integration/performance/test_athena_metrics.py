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
     termlink_id alone (every bucket).

Bytes scanned is not always deterministic, which took a real run to discover. A bare LIMIT
over an unpruned table lets Athena schedule splits across all 64 buckets and cancel the rest
as soon as one row satisfies the limit, so the figure depends on which split finished first:
at 1K, term_source_struct_no_bucket scanned 8,080,359 B and then 11,779,084 B. Queries whose
LIMIT sits above an aggregation cannot do this, because the aggregate must complete before
the limit applies, and they were identical to the byte. The pruning ratio is therefore taken
from term_source_count_bucketed/_unbucketed, a COUNT(*) pair that cannot terminate early;
term_source_struct_no_bucket is kept because it is the shape a caller would actually write,
and flagged deterministic=False so the cross-pass check reads it as an effect, not a fault.

Which row gets measured is pinned for the same reason. setup_locate_row originally had no
ORDER BY, so each session measured a different assertion in a different bucket -- across two
1K runs the locator scanned 241,800 B and then 1,943,512 B, and term_source_struct drifted
421,926 B to 426,324 B because buckets are not exactly equal in size. Seed 42 makes the
dataset reproducible, so a reader will reasonably expect re-running to reproduce the numbers;
an ORDER BY makes that true of the lookup rows as well.

*Which* column it orders by turned out to matter more than the ordering itself. ORDER BY
termlink_id pins the row to the minimum hash in the table, and the minimum is the single most
prunable value there is: every other data file's lower bound exceeds it, so file-level min/max
statistics exclude every other file without any partition pruning. At 100K that made the
unpruned lookup 78x cheaper than the termlink_id column it nominally scans, and the measured
value of bucket pruning fell from 31.2x at 1K to 0.96x -- a property of the chosen row, not of
the table. The locator now orders by subject_id, term_iri, which is equally deterministic but
uncorrelated with hash order. The lesson generalises: a locator must be reproducible *and*
representative, and pinning it to an extremum buys the first at the cost of the second.

Because of that confound there are two pruning pairs. subject_scoped_count against
subject_distinct_count restricts subject_id alone and so isolates partition pruning; it is the
figure to quote. term_source_count_bucketed against _unbucketed is retained because the gap
between the two pairs is what shows min/max statistics doing the work.

One more asymmetry worth knowing before reading the artifact: an *unfiltered* COUNT(*) is
answered from Iceberg manifest row counts without opening data files (6,822 B at 1K), while
COUNT(DISTINCT) and filtered counts must read the column.

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

Measuring more than one scale. Each scale must be ingested into an otherwise-empty evidence
table, and the table truncated between scales. This is not fastidiousness: run_id is not a
partition key, so a run-filtered query over a table that also holds another scale scans that
other scale too, and the bytes-scanned figures converge regardless of how much data each run
actually contributed. Measure, save the artifacts, truncate, then ingest the next scale.
Every artifact and every CSV row is stamped with the dataset's n_subjects and n_evidence so
pooled results stay attributable. Leave PHEBEE_EVAL_ATHENA_METRICS_DIR unset when doing
this -- the per-run default keeps scales in separate directories, while a pinned directory
would have each scale overwrite the last.

Environment:
    PHEBEE_EVAL_SCALE=1                      required, as elsewhere in this suite
    PHEBEE_EVAL_ATHENA_RUN_ID                run_id to measure; default: newest import_run.json
    PHEBEE_EVAL_BENCHMARK_DIR                dataset whose metadata.json supplies expected counts
                                             and the scale stamped into every artifact
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


def _manifest_stats() -> Optional[Dict[str, Any]]:
    """
    dataset_statistics from the benchmark directory named by PHEBEE_EVAL_BENCHMARK_DIR.

    Returns None when the directory is unset or holds no manifest, so callers can decide
    whether that is fatal. Shared by expected_counts and dataset_scale so the two cannot
    disagree about which dataset is being measured.
    """
    benchmark_dir = os.environ.get("PHEBEE_EVAL_BENCHMARK_DIR")
    if not benchmark_dir:
        return None
    manifest = Path(benchmark_dir) / "metadata.json"
    if not manifest.exists():
        return None
    with open(manifest) as fh:
        stats = json.load(fh)["dataset_statistics"]
    stats["_manifest_path"] = str(manifest)
    return stats


def dataset_scale() -> Dict[str, Any]:
    """
    Which dataset the measurement is against, for stamping into every artifact.

    Needed because bytes scanned is only interpretable next to the data volume that produced
    it. When several scales are measured in one campaign, each must be ingested into an
    otherwise-empty table -- run_id is not a partition key, so a run-filtered query over a
    table holding other scales scans them too and the figures collapse toward each other.
    That makes the scale a property of the table at measurement time, which nothing else in
    the artifact records.
    """
    stats = _manifest_stats()
    if stats is None:
        return {"n_subjects": None, "n_records": None, "n_evidence": None, "source": None}
    return {
        "n_subjects": stats.get("n_subjects"),
        "n_records": stats.get("n_records"),
        "n_evidence": stats.get("n_evidence"),
        "generator_seed": stats.get("generator_seed"),
        "source": stats.get("_manifest_path"),
    }


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

    stats = _manifest_stats()
    if stats is None:
        return {"n_evidence": None, "n_records": None, "_source": None}
    return {
        "n_evidence": stats["n_evidence"],
        "n_records": stats["n_records"],
        "_source": stats["_manifest_path"],
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
        # Read once: the manifest cannot change mid-test, and stamping it onto every row is
        # what lets measurements from several scales be pooled without losing which is which.
        self.scale = dataset_scale()
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
        deterministic: bool = True,
    ) -> List[AthenaQueryResult]:
        """
        Execute one query `passes` times, recording each execution.

        published:     True when this is a query shape the manuscript reports on, False when
                       it is a variant included to quantify a partitioning effect.
        prunes:        what the partition spec can prune for this predicate — "none",
                       "subject_bucket", or "import_date".
        deterministic: whether bytes scanned is expected to be identical across passes. False
                       for a bare LIMIT over an unpruned table: Athena schedules splits across
                       every bucket and cancels the rest once the limit is satisfied, so the
                       figure depends on which split finished first. Set this honestly -- the
                       cross-pass check below reports an unexplained difference as something to
                       investigate and an expected one as an effect being measured, and a
                       wrongly-flagged query either hides a real anomaly or cries wolf.
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
                # Carried on every row, not only in the JSON header, so the CSV is
                # self-describing and rows from different scales can be concatenated.
                "dataset_n_subjects": self.scale["n_subjects"],
                "dataset_n_evidence": self.scale["n_evidence"],
                "published_query": published,
                "prunes": prunes,
                "expect_deterministic": deterministic,
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
            "dataset_scale": self.scale,
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


def _fmt_count(n: Optional[int]) -> str:
    """Thousands-separated, or "unknown" -- an unset expectation must not raise at a print."""
    return "unknown" if n is None else f"{n:,}"


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

    guard_path = recorder.write(
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
        f"[ATHENA] evidence rows: expected {_fmt_count(expected_counts['n_evidence'])} "
        f"actual {actual_evidence}"
    )
    print(
        f"[ATHENA] termlinks:     expected {_fmt_count(expected_counts['n_records'])} "
        f"actual {actual_records}"
    )

    # Fail rather than skip when there is nothing to compare against. This guard is the only
    # check that the ingest preserved counts, so a silent skip would leave the measurement
    # uncontrolled -- the same failure mode as the circular check this replaced, in a new
    # costume. Raised after the artifact is written, so the observed counts survive.
    if expected_counts["n_evidence"] is None or expected_counts["n_records"] is None:
        pytest.fail(
            "No expected counts to compare against. Set PHEBEE_EVAL_BENCHMARK_DIR to the "
            "benchmark directory that was ingested, or set both "
            "PHEBEE_EVAL_ATHENA_EXPECTED_EVIDENCE and PHEBEE_EVAL_ATHENA_EXPECTED_RECORDS. "
            f"Observed evidence={actual_evidence}, termlinks={actual_records}; artifact "
            f"written to {guard_path}."
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
    Measure the four reported query shapes, plus the variants that quantify pruning.

    Writes athena_metrics.json and athena_metrics.csv before asserting anything, so a failed
    expectation never costs the measurement.
    """
    db, table = athena["database"], athena["evidence_table"]
    run_id = measured_run["run_id"]
    recorder = Recorder(run_id, athena)

    # --- Setup: locate one real row, and the import date, for the subject-scoped queries.
    # Tagged as setup rather than measurement; these are not reported query shapes.
    #
    # The ORDER BY is what makes the lookup measurements reproducible, and it is not
    # decoration. Without it this is a bare LIMIT 1, which returns an arbitrary row: two 1K
    # runs picked different assertions in different buckets, and because buckets are not
    # exactly equal in size the pruning figures moved with them.
    #
    # It orders by subject_id, term_iri and NOT by termlink_id, which is the correction to a
    # fix that went wrong. Ordering by termlink_id also pins the row, but it pins it to the
    # *minimum* hash in the table -- at 100K that was 000000502b266e0f..., six leading zeros,
    # exactly the expected minimum of 32.5M uniform hashes. That value is the single most
    # prunable one in the table: every other data file's lower bound exceeds it, so Iceberg
    # file-level min/max statistics exclude all 71 other files with no partition pruning
    # involved. The unpruned lookup came back at 53,718,385 B instead of the ~4.19 GB the
    # whole termlink_id column costs, and the measured value of bucket pruning collapsed from
    # 31.2x at 1K to 0.96x at 100K -- an artefact of the locator, not a property of the table.
    #
    # subject_id and term_iri are uncorrelated with hash order, so the termlink_id they select
    # sits at an arbitrary point in the hash space and min/max statistics get no free win.
    # Cost is unchanged: a top-N with no full sort, but all four projected columns are read,
    # which measured 4,543,188,498 B (~4.23 GiB, $0.021) at 100K -- the largest single query in
    # the session, and the price of reproducibility.
    locator = recorder.run(
        "setup_locate_row",
        f"""
        SELECT subject_id, term_iri, termlink_id, created_date
        FROM {db}.{table}
        WHERE run_id = '{run_id}'
        ORDER BY subject_id, term_iri
        LIMIT 1
        """,
        published=False,
        prunes="none",
        note=(
            "Setup only. Supplies a real subject_id/termlink_id for the lookup queries. "
            "Ordered by subject_id, term_iri so the same assertion is measured on every run "
            "without selecting the extremal termlink_id, which min/max statistics prune for "
            "free and which therefore biases the unpruned lookup."
        ),
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

    # --- Published query 4: the provenance and context field projection.
    #
    # Added 21 Sep because the Results sentence it backs was still reporting only success.
    # "Batch analytics validation via Athena" claims that "expected context, provenance, and
    # ontology metadata fields were correctly retained and queryable (R1)", and that claim
    # rests on two query shapes, not one: term_source_struct above covers the ontology
    # metadata, and this covers the context and provenance fields. It mirrors the evaluation
    # suite's own query at tests/integration/evaluation/test_evaluation_end_to_end.py:720,
    # including the ORDER BY, so the measurement describes the shape the claim was validated
    # with rather than a tidier stand-in.
    #
    # creator is a struct, so creator.creator_type reads one subfield; Parquet stores struct
    # fields as separate columns, which is why this costs little more than the scalar columns
    # beside it. No LIMIT: the claim is that all evidence for the termlink is retrievable, and
    # a LIMIT would both understate the work and reintroduce the early-termination
    # nondeterminism that the aggregate pair below exists to avoid.
    recorder.run(
        "evidence_provenance_fields",
        f"""
        SELECT
            evidence_id,
            run_id,
            evidence_type,
            assertion_type,
            created_timestamp,
            creator.creator_type AS creator_type
        FROM {db}.{table}
        WHERE subject_id = '{subject_id}'
          AND term_iri = '{term_iri}'
          AND termlink_id = '{termlink_id}'
        ORDER BY created_timestamp
        """,
        published=True,
        prunes="subject_bucket",
        note=(
            "Reported as context/provenance field retention. Mirrors the evaluation suite's "
            "query shape for R1. Bucket-pruned via subject_id."
        ),
    )

    # --- Variant: the same single row, located without the bucketing column.
    # Realistic shape for an unpruned lookup, but NOT a stable measurement -- see the
    # deterministic pair below, which is what the pruning ratio should be computed from.
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
            "Same row as term_source_struct, located by termlink_id alone. Realistic unpruned "
            "lookup, but bytes scanned is nondeterministic: the bare LIMIT lets Athena cancel "
            "splits once one match is found. Kept for the shape; not the pruning denominator."
        ),
        deterministic=False,
    )

    # --- Deterministic pruned/unpruned pair.
    #
    # Measured at 1K on 20 Sep: term_source_struct_no_bucket scanned 8,080,359 B then
    # 11,779,084 B -- a 46% swing in a quantity that is a function of query and data alone.
    # Cause is the bare LIMIT above. An aggregate cannot terminate early, because every
    # matching row must be counted before the result exists, so these two are stable and the
    # ratio between them is the measured value of subject_id bucket pruning.
    #
    # Both sides are COUNT(*) so the projection is comparable: the only differences are the
    # extra predicate column on the bucketed side and the 63/64 of files it does not open.
    # That is the real trade, and it is what the caption should quote.
    #
    # Note these are *filtered* counts. An unfiltered COUNT(*) is answered from Iceberg
    # manifest row counts without opening data files -- guard_evidence_count scanned 6,822 B
    # at 1K -- so it would measure nothing here.
    recorder.run(
        "term_source_count_bucketed",
        f"""
        SELECT COUNT(*) AS n
        FROM {db}.{table}
        WHERE subject_id = '{subject_id}'
          AND termlink_id = '{termlink_id}'
        """,
        published=False,
        prunes="subject_bucket",
        note=(
            "Deterministic counterpart to term_source_struct. Bucket-pruned to 1 of 64. "
            "Numerator of the pruning ratio."
        ),
    )

    recorder.run(
        "term_source_count_unbucketed",
        f"""
        SELECT COUNT(*) AS n
        FROM {db}.{table}
        WHERE termlink_id = '{termlink_id}'
        """,
        published=False,
        prunes="none",
        note=(
            "Same row located by termlink_id alone, as an aggregate so no split can be "
            "cancelled early. Denominator of the pruning ratio."
        ),
    )

    # --- Bucket pruning measured on subject_id alone.
    #
    # Added 21 Sep. The pair above compares a subject+termlink lookup against a termlink-only
    # lookup, which confounds two mechanisms: partition pruning on subject_id, and Iceberg
    # file-level min/max statistics on termlink_id. At 100K both independently narrow 72 data
    # files to 1, so they cost the same and the ratio reads 0.96x -- which says nothing about
    # either mechanism.
    #
    # This pair isolates the one the partition spec exists for. Both sides restrict nothing but
    # subject_id, both project subject_id only, and the sole difference is whether the value is
    # pinned to a single bucket. There is no hash column in the predicate, so min/max
    # statistics on termlink_id cannot contribute, and 64 buckets over 72 files puts the
    # ceiling near 72x. This is the number the supplement should quote for pruning.
    recorder.run(
        "subject_scoped_count",
        f"""
        SELECT COUNT(*) AS n
        FROM {db}.{table}
        WHERE subject_id = '{subject_id}'
        """,
        published=False,
        prunes="subject_bucket",
        note=(
            "Bucket-pruned count for one subject. Numerator of the subject_id pruning ratio, "
            "which is the pruning figure free of the termlink_id min/max confound."
        ),
    )

    recorder.run(
        "subject_distinct_count",
        f"""
        SELECT COUNT(DISTINCT subject_id) AS n
        FROM {db}.{table}
        """,
        published=False,
        prunes="none",
        note=(
            "Full subject_id column, no predicate to prune on. Denominator of the subject_id "
            "pruning ratio. Unlike an unfiltered COUNT(*), a COUNT(DISTINCT) must read the "
            "column and so cannot be answered from Iceberg manifest metadata."
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
    grouped = _group_by_query(recorder.measurements)
    for query, passes in grouped.items():
        scans = {m["pass"]: m["data_scanned_bytes"] for m in passes}
        distinct = set(scans.values())
        if len(distinct) <= 1:
            continue
        lo, hi = min(distinct), max(distinct)
        spread = f"spread {hi - lo:,} B ({(hi - lo) / hi:.2%} of the larger)"
        if passes[0]["expect_deterministic"]:
            print(
                f"[ATHENA] WARNING: {query} scanned different amounts across passes: "
                f"{scans}. {spread}. Two executions of one query over an unchanged table "
                f"should scan identically, so investigate before publishing: unreported "
                f"result reuse, Athena-side caching, or writes to the table during the "
                f"session."
            )
        else:
            # Declared nondeterministic, so this is the effect being measured rather than a
            # fault. Still printed: the magnitude is a finding, and silence would leave a
            # reader of the artifact wondering whether the harness noticed.
            print(
                f"[ATHENA] NOTE: {query} scanned different amounts across passes: {scans}. "
                f"{spread}. Expected -- a bare LIMIT over an unpruned table lets Athena "
                f"cancel splits once a match is found, so this figure is scheduling-dependent "
                f"and must not be quoted as a scan cost."
            )

    # The pruning ratios, off the deterministic pairs only. Printed rather than asserted: the
    # value is the result, and there is no threshold it must clear to be worth reporting.
    #
    # Two pairs, because the first one confounds two mechanisms and the second does not. Quote
    # the subject_id pair in the supplement; the termlink pair is retained because the gap
    # between them is itself the finding.
    ratio_pairs = (
        (
            "subject_id bucket pruning (clean)",
            "subject_scoped_count",
            "subject_distinct_count",
            "predicate is subject_id only, so no hash column can contribute; "
            "64 buckets over the current file count is the ceiling",
        ),
        (
            "subject+termlink vs termlink alone (confounded)",
            "term_source_count_bucketed",
            "term_source_count_unbucketed",
            "the unpruned side can be narrowed by Iceberg min/max statistics on termlink_id, "
            "so a ratio near 1.0 means both sides reached one file, not that pruning failed",
        ),
    )
    for label, pruned_q, unpruned_q, caveat in ratio_pairs:
        if not all(q in grouped for q in (pruned_q, unpruned_q)):
            continue
        pruned = max(m["data_scanned_bytes"] for m in grouped[pruned_q])
        unpruned = max(m["data_scanned_bytes"] for m in grouped[unpruned_q])
        if pruned:
            print(
                f"[ATHENA] {label}: {unpruned:,} B unpruned vs {pruned:,} B pruned "
                f"= {unpruned / pruned:.1f}x reduction -- {caveat}"
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
