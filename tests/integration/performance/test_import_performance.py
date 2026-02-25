"""
PheBee Import Performance Test (Manuscript Table 3)

Evaluates bulk data ingestion throughput at scale. Generates synthetic phenotype data
with realistic term frequency distributions, uploads to S3 as NDJSON batches, triggers
the PheBee bulk import Step Function, and measures end-to-end import performance.

Key Metrics (Table 3):
- Ingest time (seconds)
- Records per second throughput
- Evidence items per second throughput
- Dataset characteristics (subjects, records, terms, evidence)

Environment Variables:
- PHEBEE_EVAL_SCALE=1 (required to enable test)
- PHEBEE_EVAL_TERMS_JSON_PATH (required): HPO terms JSON file
- PHEBEE_EVAL_PREVALENCE_CSV_PATH (optional): Term frequency CSV for realistic distributions
- PHEBEE_EVAL_SCALE_SUBJECTS (default: 10000): Number of subjects to generate
- PHEBEE_EVAL_SCALE_MIN_TERMS (default: 5): Min HPO terms per subject
- PHEBEE_EVAL_SCALE_MAX_TERMS (default: 50): Max HPO terms per subject
- PHEBEE_EVAL_SCALE_MIN_EVIDENCE (default: 1): Min evidence items per term link
- PHEBEE_EVAL_SCALE_MAX_EVIDENCE (default: 25): Max evidence items per term link
- PHEBEE_EVAL_BATCH_SIZE (default: 10000): Records per S3 batch file
- PHEBEE_EVAL_INGEST_TIMEOUT_S (default: 21600): Max seconds to wait for import (6 hours)
- PHEBEE_EVAL_SEED (optional): Random seed for reproducibility
- PHEBEE_EVAL_WRITE_ARTIFACTS (default: 1): Write CSV/JSON artifacts to /tmp

Required Fixtures:
- physical_resources: Dict with PheBeeBucket and BulkImportStateMachine
- test_project_id: PheBee project identifier
- synthetic_dataset: Generated dataset from conftest.py

Output Artifacts:
- /tmp/phebee-eval-artifacts/{run_id}/table3_ingestion.csv
- /tmp/phebee-eval-artifacts/{run_id}/import_run.json
"""

from __future__ import annotations

import csv
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

import boto3
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.perf]


# -----------------------------
# Helper functions
# -----------------------------


def _env_int(name: str, default: int) -> int:
    """Get integer from environment variable or use default."""
    v = os.environ.get(name)
    return default if v is None else int(v)


# -----------------------------
# AWS orchestration (S3 + SFN)
# -----------------------------


def _s3_put_jsonl(bucket: str, key: str, records: List[Dict[str, Any]]) -> None:
    """Upload records to S3 as newline-delimited JSON."""
    s3 = boto3.client("s3")
    body = "\n".join(json.dumps(r) for r in records) + "\n"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )


def _start_sfn_execution(state_machine_arn: str, project_id: str, run_id: str, input_path: str) -> str:
    """Start Step Functions bulk import execution."""
    sfn = boto3.client("stepfunctions")
    execution_name = f"phebee-import-perf-{run_id}-{uuid.uuid4().hex[:8]}"
    resp = sfn.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps({
            "run_id": run_id,
            "input_path": input_path,
            "project_id": project_id  # Explicitly pass project_id to validation
        }),
    )
    return resp["executionArn"]


def _wait_for_sfn(execution_arn: str, timeout_s: int = 7200, poll_s: int = 10) -> Dict[str, Any]:
    """Poll Step Functions execution until completion or timeout."""
    sfn = boto3.client("stepfunctions")
    t0 = time.time()
    while True:
        desc = sfn.describe_execution(executionArn=execution_arn)
        status = desc["status"]
        if status in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"):
            return desc
        if (time.time() - t0) > timeout_s:
            raise TimeoutError(f"Timed out waiting for Step Function execution: {execution_arn}")
        time.sleep(poll_s)


def _split_batches(records: List[Dict[str, Any]], batch_size: int) -> List[List[Dict[str, Any]]]:
    """Split records into batches for S3 upload."""
    return [records[i : i + batch_size] for i in range(0, len(records), batch_size)]


def _write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    """Write CSV artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})


# -----------------------------
# Main import performance test
# -----------------------------


@pytest.mark.perf
def test_import_performance(
    physical_resources: Dict[str, Any],
    test_project_id: str,
    synthetic_dataset,  # GeneratedDataset fixture from conftest.py
):
    """
    Measure bulk import performance for PheBee (Table 3).

    This test:
    1. Uses pre-generated synthetic dataset from conftest fixture
    2. Uploads data to S3 as NDJSON batches
    3. Triggers PheBee bulk import Step Function
    4. Waits for completion (with configurable timeout)
    5. Measures throughput (records/sec, evidence/sec)
    6. Outputs CSV and JSON artifacts for manuscript Table 3
    """
    if os.environ.get("PHEBEE_EVAL_SCALE") != "1":
        pytest.skip("Set PHEBEE_EVAL_SCALE=1 to run import performance test.")

    # Required infrastructure
    bucket = physical_resources.get("PheBeeBucket")
    sm_arn = physical_resources.get("BulkImportStateMachine")
    if not bucket or not sm_arn:
        pytest.skip("Missing required physical resources: PheBeeBucket and/or BulkImportStateMachine")

    # Configuration
    batch_size = _env_int("PHEBEE_EVAL_BATCH_SIZE", 10_000)
    ingest_timeout_s = _env_int("PHEBEE_EVAL_INGEST_TIMEOUT_S", 21600)
    run_id = f"import-perf-{uuid.uuid4().hex[:10]}"

    # Dataset statistics
    stats = synthetic_dataset.stats
    records = synthetic_dataset.records
    is_lazy = records is None

    print(f"\n{'='*80}")
    print(f"PheBee Import Performance Test - Table 3")
    print(f"{'='*80}")
    print(f"Run ID: {run_id}")
    print(f"Project ID: {test_project_id}")
    if is_lazy:
        print(f"[MEMORY_OPTIMIZATION] Using lazy loading - streaming from disk")
    print(f"\nDataset Statistics:")
    print(f"  Subjects: {stats['n_subjects']:,}")
    print(f"  Records (term links): {stats['n_records']:,}")
    print(f"  Evidence items: {stats['n_evidence']:,}")
    print(f"  Unique HPO terms: {stats['n_unique_terms']:,}")
    print(f"  Terms per subject: {stats['terms_per_subject']['min']}-{stats['terms_per_subject']['max']} "
          f"(mean: {stats['terms_per_subject']['mean']:.1f})")
    print(f"  Evidence per record: {stats['evidence_per_record']['min']}-{stats['evidence_per_record']['max']} "
          f"(mean: {stats['evidence_per_record']['mean']:.1f})")
    print(f"\nQualifier Distribution:")
    qual_dist = stats['qualifier_distribution_records']
    total_records = stats['n_records']
    print(f"  Unqualified: {qual_dist['unqualified']:,} ({qual_dist['unqualified']/total_records*100:.1f}%)")
    print(f"  Negated: {qual_dist['negated']:,} ({qual_dist['negated']/total_records*100:.1f}%)")
    print(f"  Family History: {qual_dist['family']:,} ({qual_dist['family']/total_records*100:.1f}%)")
    print(f"  Hypothetical: {qual_dist['hypothetical']:,} ({qual_dist['hypothetical']/total_records*100:.1f}%)")

    # Stage data to S3 as NDJSON batches
    print(f"\nUploading to S3...")
    s3_prefix = f"perf-data/{run_id}/jsonl"

    if is_lazy:
        # Lazy loading: stream from benchmark directory
        print(f"  Streaming from benchmark directory: {synthetic_dataset.benchmark_dir}")
        s3 = boto3.client("s3")
        batches_dir = synthetic_dataset.benchmark_dir / "batches"
        batch_files = sorted(batches_dir.glob("batch-*.json"))

        upload_buffer = []
        upload_idx = 0
        total_uploaded = 0

        for batch_file in batch_files:
            with batch_file.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        record = json.loads(line)
                        # Override project_id and subject_id for test isolation
                        record["project_id"] = test_project_id
                        old_subject_id = record.get("subject_id") or record.get("project_subject_id")
                        if old_subject_id and old_subject_id in synthetic_dataset.subject_id_map:
                            record["subject_id"] = synthetic_dataset.subject_id_map[old_subject_id]

                        upload_buffer.append(record)

                        # Upload when buffer reaches batch_size
                        if len(upload_buffer) >= batch_size:
                            key = f"{s3_prefix}/batch-{upload_idx:05d}.json"
                            _s3_put_jsonl(bucket, key, upload_buffer)
                            total_uploaded += len(upload_buffer)
                            if upload_idx == 0 or (upload_idx + 1) % 10 == 0:
                                print(f"  Uploaded batch {upload_idx+1}: {len(upload_buffer):,} records (total: {total_uploaded:,})")
                            upload_idx += 1
                            upload_buffer = []

        # Upload final partial batch
        if upload_buffer:
            key = f"{s3_prefix}/batch-{upload_idx:05d}.json"
            _s3_put_jsonl(bucket, key, upload_buffer)
            total_uploaded += len(upload_buffer)
            upload_idx += 1

        print(f"  Streaming upload complete: {upload_idx} batches, {total_uploaded:,} records")
        n_batches = upload_idx
    else:
        # In-memory: split and upload
        batches = _split_batches(records, batch_size=max(1, batch_size))

        print(f"  Splitting into {len(batches)} batches of ~{batch_size:,} records each")
        for i, batch in enumerate(batches):
            # Update batch_id in each record
            for r in batch:
                r["batch_id"] = i
            key = f"{s3_prefix}/batch-{i:05d}.json"
            _s3_put_jsonl(bucket, key, batch)
            if i == 0 or (i + 1) % 10 == 0 or i == len(batches) - 1:
                print(f"  Uploaded batch {i+1}/{len(batches)}: {len(batch):,} records")

        n_batches = len(batches)

    input_path = f"s3://{bucket}/{s3_prefix}"
    print(f"\nS3 Input Path: {input_path}")

    # Start bulk import Step Function
    print(f"\nStarting bulk import Step Function...")
    print(f"  Project ID: {test_project_id}")
    print(f"  Timeout: {ingest_timeout_s}s ({ingest_timeout_s/60:.1f} minutes)")
    t0 = time.time()
    execution_arn = _start_sfn_execution(sm_arn, project_id=test_project_id, run_id=run_id, input_path=input_path)
    print(f"  Execution ARN: {execution_arn}")

    # Wait for completion
    print(f"\nWaiting for import to complete...")
    desc = _wait_for_sfn(execution_arn, timeout_s=ingest_timeout_s)
    t_ingest_s = time.time() - t0

    # Check status
    if desc["status"] != "SUCCEEDED":
        raise AssertionError(
            f"Bulk import failed or timed out. Status: {desc['status']}\n"
            f"Execution ARN: {execution_arn}\n"
            f"Check Step Functions console for details."
        )

    print(f"\nStep Functions execution completed successfully.")
    print(f"Waiting additional time for data indexing/propagation...")

    # Wait for data to be indexed and queryable (Neptune/DynamoDB eventual consistency)
    indexing_wait_s = 60
    print(f"  Sleeping {indexing_wait_s}s to allow data to become queryable...")
    time.sleep(indexing_wait_s)

    # Calculate throughput
    recs_per_sec = stats["n_records"] / t_ingest_s if t_ingest_s > 0 else None
    ev_per_sec = stats["n_evidence"] / t_ingest_s if t_ingest_s > 0 else None

    print(f"\n{'='*80}")
    print(f"Import Complete!")
    print(f"{'='*80}")
    print(f"  Total time: {t_ingest_s:.2f}s ({t_ingest_s/60:.1f} minutes)")
    print(f"  Records throughput: {recs_per_sec:.2f} records/sec")
    print(f"  Evidence throughput: {ev_per_sec:.2f} evidence/sec")

    # Build Table 3 output
    table3_row = {
        "run_id": run_id,
        "project_id": test_project_id,
        "n_subjects": stats["n_subjects"],
        "n_records": stats["n_records"],
        "n_evidence": stats["n_evidence"],
        "n_unique_terms": stats["n_unique_terms"],
        "terms_per_subject_mean": round(stats["terms_per_subject"]["mean"], 2),
        "terms_per_subject_median": round(stats["terms_per_subject"]["median"], 2),
        "terms_per_subject_min": stats["terms_per_subject"]["min"],
        "terms_per_subject_max": stats["terms_per_subject"]["max"],
        "evidence_per_record_mean": round(stats["evidence_per_record"]["mean"], 2),
        "evidence_per_record_median": round(stats["evidence_per_record"]["median"], 2),
        "evidence_per_record_min": stats["evidence_per_record"]["min"],
        "evidence_per_record_max": stats["evidence_per_record"]["max"],
        "ingest_seconds": round(t_ingest_s, 2),
        "records_per_sec": round(recs_per_sec, 2) if recs_per_sec is not None else None,
        "evidence_per_sec": round(ev_per_sec, 2) if ev_per_sec is not None else None,
        "batch_size": batch_size,
        "n_batches": n_batches,
        "sfn_execution_arn": execution_arn,
    }

    # Full JSON output
    out_json = {
        "run_id": run_id,
        "project_id": test_project_id,
        "dataset": stats,
        "import_performance": {
            "ingest_seconds": round(t_ingest_s, 2),
            "records_per_sec": round(recs_per_sec, 2) if recs_per_sec is not None else None,
            "evidence_per_sec": round(ev_per_sec, 2) if ev_per_sec is not None else None,
            "batch_size": batch_size,
            "n_batches": n_batches,
            "sfn_execution_arn": execution_arn,
            "sfn_status": desc["status"],
            "s3_input_path": input_path,
        },
        "table3": table3_row,
    }

    # Write artifacts
    if os.environ.get("PHEBEE_EVAL_WRITE_ARTIFACTS", "1") == "1":
        base = Path("/tmp/phebee-eval-artifacts") / run_id
        base.mkdir(parents=True, exist_ok=True)

        table3_path = base / "table3_ingestion.csv"
        json_path = base / "import_run.json"

        _write_csv(
            table3_path,
            [table3_row],
            fieldnames=[
                "run_id",
                "project_id",
                "n_subjects",
                "n_records",
                "n_evidence",
                "n_unique_terms",
                "terms_per_subject_mean",
                "terms_per_subject_median",
                "terms_per_subject_min",
                "terms_per_subject_max",
                "evidence_per_record_mean",
                "evidence_per_record_median",
                "evidence_per_record_min",
                "evidence_per_record_max",
                "ingest_seconds",
                "records_per_sec",
                "evidence_per_sec",
                "batch_size",
                "n_batches",
                "sfn_execution_arn",
            ],
        )

        json_path.write_text(json.dumps(out_json, indent=2), encoding="utf-8")

        # Write subject_id mapping if using benchmark dataset
        if synthetic_dataset.subject_id_map:
            mapping_path = base / "subject_id_mapping.json"
            mapping_data = {
                "description": "Mapping of original benchmark subject_ids to new UUIDs for this test run",
                "test_project_id": test_project_id,
                "n_subjects": len(synthetic_dataset.subject_id_map),
                "mapping": synthetic_dataset.subject_id_map,
            }
            mapping_path.write_text(json.dumps(mapping_data, indent=2), encoding="utf-8")

        print(f"\n{'='*80}")
        print(f"Artifacts Written")
        print(f"{'='*80}")
        print(f"  Table 3 CSV: {table3_path}")
        print(f"  Full JSON:   {json_path}")
        if synthetic_dataset.subject_id_map:
            mapping_path = base / "subject_id_mapping.json"
            print(f"  Subject ID Mapping: {mapping_path}")
        print()

    # Print JSON summary for test output
    print(f"\n[IMPORT_RESULT_JSON]\n{json.dumps(out_json, indent=2)}\n")
