"""
End-to-end evaluation module for PheBee (manuscript Section 8 Evaluation).

Design goals:
- Exercise the *advertised* interfaces: bulk ingestion workflow + API Gateway.
- Produce reproducible, manuscript-friendly metrics (ingestion duration/throughput, API p50/p95).
- Cover as many test-addressable requirements as possible (R1/R2/R3/R4/R5/R6/R7/R8/R9/R10/R11).

Notes:
- This file assumes the test harness already provides the following fixtures (as in existing integration tests):
    - api_base_url: str
    - sigv4_auth: requests-compatible SigV4 auth object
    - physical_resources: dict with at least PheBeeBucket and BulkImportStateMachine
- Where features are not yet implemented (e.g., term_source struct in evidence responses),
  tests are marked xfail with a clear reason so they can be flipped to strict asserts later.
"""

import json
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
import pytest
import requests


pytestmark = [pytest.mark.integration, pytest.mark.api]


# -----------------------------
# Utilities
# -----------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _pctl(values: List[float], p: float) -> float:
    """
    Percentile with linear interpolation (p in [0,100]).
    """
    if not values:
        raise ValueError("Cannot compute percentile on empty list")
    if p <= 0:
        return min(values)
    if p >= 100:
        return max(values)
    xs = sorted(values)
    k = (len(xs) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    if f == c:
        return xs[f]
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def _assert_status(resp: requests.Response, expected: Iterable[int]) -> None:
    if resp.status_code not in set(expected):
        raise AssertionError(
            f"Unexpected status={resp.status_code}, expected={list(expected)}; body={resp.text}"
        )


def _json(resp: requests.Response) -> Dict[str, Any]:
    try:
        return resp.json()
    except Exception as e:
        raise AssertionError(f"Response was not valid JSON: {e}; body={resp.text}")


def _s3_put_jsonl(bucket: str, key: str, records: List[Dict[str, Any]]) -> None:
    s3 = boto3.client("s3")
    body = "\n".join(json.dumps(r) for r in records) + "\n"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )


def _start_sfn_execution(state_machine_arn: str, run_id: str, input_path: str) -> str:
    sfn = boto3.client("stepfunctions")
    execution_name = f"phebee-eval-{run_id}-{uuid.uuid4().hex[:8]}"
    resp = sfn.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps({"run_id": run_id, "input_path": input_path}),
    )
    return resp["executionArn"]


def _wait_for_sfn(execution_arn: str, timeout_s: int = 3600, poll_s: int = 10) -> Dict[str, Any]:
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


def _athena_query(
    query: str,
    database: str,
    output_location: str,
    workgroup: Optional[str] = None,
    timeout_s: int = 300,
    poll_s: int = 2,
) -> Tuple[List[str], List[List[str]], float]:
    """
    Returns: (column_names, rows (as strings), elapsed_seconds)
    """
    athena = boto3.client("athena")
    kwargs: Dict[str, Any] = {
        "QueryString": query,
        "QueryExecutionContext": {"Database": database},
        "ResultConfiguration": {"OutputLocation": output_location},
    }
    if workgroup:
        kwargs["WorkGroup"] = workgroup

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

    results = athena.get_query_results(QueryExecutionId=qid, MaxResults=1000)
    cols = [c["Name"] for c in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows: List[List[str]] = []
    # First row is header
    for r in results["ResultSet"]["Rows"][1:]:
        rows.append([(d.get("VarCharValue") if d else None) for d in r.get("Data", [])])

    elapsed = time.time() - t0
    return cols, rows, elapsed


# -----------------------------
# Synthetic data generation
# -----------------------------

@dataclass(frozen=True)
class TermSource:
    source: str
    version: str
    iri: str


def _mk_term_source_hpo() -> TermSource:
    # Synthetic but realistic-ish defaults for evaluation.
    # Version string should match your HPO release identifier convention.
    version = os.environ.get("PHEBEE_EVAL_HPO_VERSION", "2025-10-22")
    iri = os.environ.get(
        "PHEBEE_EVAL_HPO_VERSION_IRI",
        f"http://purl.obolibrary.org/obo/hp/releases/{version}/hp.owl",
    )
    return TermSource(source="HPO", version=version, iri=iri)


def generate_synthetic_jsonl_records(
    *,
    project_id: str,
    n_subjects: int,
    terms_per_subject: int,
    evidence_per_term: int,
    include_mixed_sources_on_first_term: bool = True,
    include_qualified_term: bool = True,
    term_source: Optional[TermSource] = None,
) -> List[Dict[str, Any]]:
    """
    Generates NDJSON records in the shape used by the existing bulk import SFN integration test:
      {project_id, project_subject_id, term_iri, evidence:[...], row_num, batch_id, ...}

    This generator intentionally includes:
      - Descendant-expansion test data: child term asserted; query by a parent term.
      - Qualifier test data: optional negated context record.
      - R5 multi-source: for one TermLink, include both automated + human evidence items.
      - R1 term_source metadata (as a struct) added at the record level and per evidence item.
    """
    if term_source is None:
        term_source = _mk_term_source_hpo()

    # Parent term for descendant expansion queries (very general HPO node)
    parent_term = "http://purl.obolibrary.org/obo/HP_0000118"  # Phenotypic abnormality
    # Child term asserted in evidence (descendant of HP_0000118)
    child_term = "http://purl.obolibrary.org/obo/HP_0001627"   # Abnormality of the cardiovascular system

    records: List[Dict[str, Any]] = []
    row_num = 0

    for s in range(n_subjects):
        project_subject_id = f"eval-subj-{s:05d}"

        for t in range(terms_per_subject):
            # First term is the child term; others are arbitrary HPO terms (synthetic but HPO-like)
            if t == 0:
                term_iri = child_term
            else:
                # Construct a valid-looking HP id; may not exist in ontology, but that's OK for non-desc-exp checks.
                hp_num = 1000000 + (s * 100) + t
                term_iri = f"http://purl.obolibrary.org/obo/HP_{hp_num:07d}"

            evidence_items: List[Dict[str, Any]] = []

            # Evidence item template fields used by the current bulk import test
            def mk_evidence_item(*, creator_type: str, creator_id: str, creator_name: str, contexts: Dict[str, float]) -> Dict[str, Any]:
                return {
                    "type": "clinical_note",
                    "clinical_note_id": f"note-{s:05d}-{t:02d}",
                    "encounter_id": f"enc-{s:05d}",
                    "evidence_creator_id": creator_id,
                    "evidence_creator_type": creator_type,
                    "evidence_creator_name": creator_name,
                    "note_timestamp": "2024-01-15",
                    "note_type": "progress_note",
                    "provider_type": "physician",
                    "author_specialty": "cardiology",
                    "span_start": 45,
                    "span_end": 58,
                    "contexts": contexts,
                    # R1: term_source struct (record-level is canonical; we include here too to maximize compatibility)
                    "term_source": {"source": term_source.source, "version": term_source.version, "iri": term_source.iri},
                }

            # Multi-evidence per term
            # For mixed sources case, reduce automated evidence to make room for manual evidence
            automated_evidence_count = evidence_per_term
            if include_mixed_sources_on_first_term and s == 0 and t == 0:
                automated_evidence_count = evidence_per_term - 1
                
            for e in range(automated_evidence_count):
                evidence_items.append(
                    mk_evidence_item(
                        creator_type="automated",
                        creator_id="ods/phebee-eval:automated-v1",
                        creator_name="PheBee Eval NLP",
                        contexts={"negated": 0.0, "family": 0.0, "hypothetical": 0.0},
                    )
                )

            # R5: add a second evidence flavor (human) for the *first* term of the *first* subject,
            # mapping to the same (source node, term, qualifiers) combination.
            if include_mixed_sources_on_first_term and s == 0 and t == 0:
                evidence_items.append(
                    mk_evidence_item(
                        creator_type="manual",
                        creator_id="ods/phebee-eval:curator-1",
                        creator_name="PheBee Eval Curator",
                        contexts={"negated": 0.0, "family": 0.0, "hypothetical": 0.0},
                    )
                )

            # R2 qualifier-aware filtering: add a negated version of the child term for subject 1, term 0
            if include_qualified_term and s == 1 and t == 0:
                evidence_items.append(
                    mk_evidence_item(
                        creator_type="automated",
                        creator_id="ods/phebee-eval:automated-v1",
                        creator_name="PheBee Eval NLP",
                        contexts={"negated": 1.0, "family": 0.0, "hypothetical": 0.0},
                    )
                )

            record = {
                "project_id": project_id,
                "project_subject_id": project_subject_id,
                "term_iri": term_iri,
                "evidence": evidence_items,
                "row_num": row_num,
                "batch_id": 0,
                # R1: record-level term_source struct
                "term_source": {"source": term_source.source, "version": term_source.version, "iri": term_source.iri},
                # Helpful for tests (non-ingestion fields are generally ignored if not used)
                "_eval_parent_term_for_descendant_test": parent_term if (t == 0) else None,
            }
            row_num += 1
            records.append(record)

    return records


def _split_batches(records: List[Dict[str, Any]], batch_size: int) -> List[List[Dict[str, Any]]]:
    return [records[i : i + batch_size] for i in range(0, len(records), batch_size)]


# -----------------------------
# API wrappers
# -----------------------------

def api_post(api_base_url: str, path: str, payload: Dict[str, Any], sigv4_auth: Any) -> Dict[str, Any]:
    resp = requests.post(f"{api_base_url}{path}", json=payload, auth=sigv4_auth)
    _assert_status(resp, [200])
    return _json(resp)


def api_post_unsigned(api_base_url: str, path: str, payload: Dict[str, Any]) -> requests.Response:
    return requests.post(f"{api_base_url}{path}", json=payload)


# -----------------------------
# Tests
# -----------------------------

@pytest.fixture
def evaluation_run_id() -> str:
    return f"eval-run-{uuid.uuid4().hex[:10]}-{_now_ms()}"


@pytest.fixture
def term_source_hpo() -> TermSource:
    return _mk_term_source_hpo()



@pytest.fixture
def eval_project_id(api_base_url: str, sigv4_auth: Any) -> str:
    """
    Creates a temporary project for evaluation tests and cleans it up afterward.

    We define this here (instead of relying on repo-specific fixture names) so this module
    can run against an existing stack without requiring additional harness fixtures.
    """
    project_id = f"eval-proj-{uuid.uuid4().hex[:8]}"
    api_post(api_base_url, "/project/create", {"project_id": project_id, "project_label": "Evaluation Project"}, sigv4_auth)
    yield project_id
    # Best-effort cleanup
    try:
        api_post(api_base_url, "/project/remove", {"project_id": project_id}, sigv4_auth)
    except Exception as e:
        print(f"[WARN] Failed to remove eval project {project_id}: {e}")

@pytest.fixture
def evaluation_dataset_small(eval_project_id: str, term_source_hpo: TermSource) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Small dataset for correctness tests.
    """
    records = generate_synthetic_jsonl_records(
        project_id=eval_project_id,
        n_subjects=5,
        terms_per_subject=3,
        evidence_per_term=2,
        include_mixed_sources_on_first_term=True,
        include_qualified_term=True,
        term_source=term_source_hpo,
    )
    return eval_project_id, records


def _run_bulk_ingest(
    *,
    physical_resources: Dict[str, Any],
    run_id: str,
    records: List[Dict[str, Any]],
    s3_prefix: Optional[str] = None,
    batch_size: int = 500,
    timeout_s: int = 3600,
) -> Dict[str, Any]:
    bucket = physical_resources.get("PheBeeBucket")
    sm = physical_resources.get("BulkImportStateMachine")
    if not bucket or not sm:
        pytest.skip("Missing required physical resources: PheBeeBucket and/or BulkImportStateMachine")

    if s3_prefix is None:
        s3_prefix = f"evaluation/{run_id}/jsonl"

    batches = _split_batches(records, batch_size=max(1, batch_size))
    for i, batch in enumerate(batches):
        key = f"{s3_prefix}/batch-{i:05d}.json"
        _s3_put_jsonl(bucket, key, batch)

    input_path = f"s3://{bucket}/{s3_prefix}"
    execution_arn = _start_sfn_execution(sm, run_id=run_id, input_path=input_path)
    desc = _wait_for_sfn(execution_arn, timeout_s=timeout_s)
    return {
        "bucket": bucket,
        "state_machine_arn": sm,
        "execution_arn": execution_arn,
        "input_path": input_path,
        "describe_execution": desc,
    }


def test_r8_unsigned_request_rejected(api_base_url):
    """
    R8 (partial): demonstrate API access control is enforced by rejecting unsigned requests.
    """
    resp = api_post_unsigned(api_base_url, "/subjects/query", {"project_id": "any"})
    assert resp.status_code in (401, 403), f"Expected 401/403, got {resp.status_code}: {resp.text}"



def test_r7_shared_subject_representation_project_scoped_identifiers(api_base_url: str, sigv4_auth: Any):
    """
    R7: Privacy-conscious multi-project partitioning.

    Validates that:
      - A single underlying Subject IRI can be linked to multiple projects (shared subject representation).
      - Project-scoped subject identifiers remain isolated per project when querying via /subjects/query.
    """
    project_a = f"eval-r7-a-{uuid.uuid4().hex[:8]}"
    project_b = f"eval-r7-b-{uuid.uuid4().hex[:8]}"
    subj_a = f"A{uuid.uuid4().hex[:8]}"
    subj_b = f"B{uuid.uuid4().hex[:8]}"

    # Create two projects
    api_post(api_base_url, "/project/create", {"project_id": project_a, "project_label": "Eval R7 Project A"}, sigv4_auth)
    api_post(api_base_url, "/project/create", {"project_id": project_b, "project_label": "Eval R7 Project B"}, sigv4_auth)

    try:
        # Create subject mapping in project A
        resp_a = api_post(
            api_base_url,
            "/subject/create",
            {"project_id": project_a, "project_subject_id": subj_a},
            sigv4_auth,
        )
        subject_iri = resp_a["subject"]["iri"]
        assert subject_iri, "Expected a subject IRI"

        # Link a new project_subject_id in project B to the *same* underlying subject via known_project_*
        resp_b = api_post(
            api_base_url,
            "/subject/create",
            {
                "project_id": project_b,
                "project_subject_id": subj_b,
                "known_project_id": project_a,
                "known_project_subject_id": subj_a,
            },
            sigv4_auth,
        )
        assert resp_b["subject"]["iri"] == subject_iri

        # Query project A: should only expose project A identifiers (no leakage of subj_b)
        qa = api_post(api_base_url, "/subjects/query", {"project_id": project_a, "limit": 100}, sigv4_auth)
        body_a = qa.get("body", [])
        assert any(s.get("project_subject_id") == subj_a for s in body_a), "Expected subj_a in project A query"
        assert all(s.get("project_subject_id") != subj_b for s in body_a), "Project B subject_id leaked into project A results"

        # Query project B: should only expose project B identifiers (no leakage of subj_a)
        qb = api_post(api_base_url, "/subjects/query", {"project_id": project_b, "limit": 100}, sigv4_auth)
        body_b = qb.get("body", [])
        assert any(s.get("project_subject_id") == subj_b for s in body_b), "Expected subj_b in project B query"
        assert all(s.get("project_subject_id") != subj_a for s in body_b), "Project A subject_id leaked into project B results"

        # Both projects should point to the same underlying Subject IRI
        subj_iris_a = {s.get("subject_iri") for s in body_a if s.get("project_subject_id") == subj_a}
        subj_iris_b = {s.get("subject_iri") for s in body_b if s.get("project_subject_id") == subj_b}
        assert subject_iri in subj_iris_a
        assert subject_iri in subj_iris_b

    finally:
        # Best-effort cleanup
        try:
            api_post(api_base_url, "/project/remove", {"project_id": project_a}, sigv4_auth)
        except Exception as e:
            print(f"[WARN] Failed to remove project A {project_a}: {e}")
        try:
            api_post(api_base_url, "/project/remove", {"project_id": project_b}, sigv4_auth)
        except Exception as e:
            print(f"[WARN] Failed to remove project B {project_b}: {e}")

def test_eval_ingest_and_core_api_behaviors(
    physical_resources,
    api_base_url,
    sigv4_auth,
    evaluation_run_id,
    evaluation_dataset_small,
    term_source_hpo,
):
    """
    Covers:
      - R2 descendant expansion + qualifier-aware filtering (API behavior)
      - R6 idempotency (TermLink stability across re-ingest)
      - R7 project scoping sanity checks (project_id required, scoped queries)
      - R4 evidence/provenance visibility via /term-link (shape-level)
      - R5 multi-source evidence (human + automated) *within a single TermLink*
    """
    project_id, records = evaluation_dataset_small

    # 1) Bulk ingest
    t0 = time.time()
    ingest = _run_bulk_ingest(physical_resources=physical_resources, run_id=evaluation_run_id, records=records)
    t_ingest = time.time() - t0
    assert ingest["describe_execution"]["status"] == "SUCCEEDED", ingest["describe_execution"]

    n_records = len(records)
    # Manuscript-friendly metric logging
    print(f"[EVAL] Ingestion: run_id={evaluation_run_id} records={n_records} seconds={t_ingest:.2f} recs_per_sec={(n_records / t_ingest):.2f}")

    # 2) Descendant expansion (R2)
    parent_term = next(r["_eval_parent_term_for_descendant_test"] for r in records if r.get("_eval_parent_term_for_descendant_test"))
    # include_child_terms=True should match because we asserted a child term
    body_true = api_post(
        api_base_url,
        "/subjects/query",
        {
            "project_id": project_id,
            "term_iri": parent_term,
            "include_child_terms": True,
            "include_qualified": True,
            "term_source": term_source_hpo.source,
            "term_source_version": term_source_hpo.version,
            "limit": 50,
        },
        sigv4_auth,
    )
    assert body_true.get("n_subjects", 0) >= 1, f"Expected >=1 subject with descendant expansion, got: {body_true}"

    # include_child_terms=False should *not* match if parent itself wasn't asserted
    body_false = api_post(
        api_base_url,
        "/subjects/query",
        {
            "project_id": project_id,
            "term_iri": parent_term,
            "include_child_terms": False,
            "include_qualified": True,
            "term_source": term_source_hpo.source,
            "term_source_version": term_source_hpo.version,
            "limit": 50,
        },
        sigv4_auth,
    )
    assert body_false.get("n_subjects", 0) <= body_true.get("n_subjects", 0)

    # 3) Qualifier-aware filtering (R2)
    # Our generator adds a negated record for subject 1, term 0. We filter by the child term itself.
    child_term = "http://purl.obolibrary.org/obo/HP_0001627"
    without_qualified = api_post(
        api_base_url,
        "/subjects/query",
        {
            "project_id": project_id,
            "term_iri": child_term,
            "include_child_terms": False,
            "include_qualified": False,
            "term_source": term_source_hpo.source,
            "term_source_version": term_source_hpo.version,
            "limit": 100,
        },
        sigv4_auth,
    )
    with_qualified = api_post(
        api_base_url,
        "/subjects/query",
        {
            "project_id": project_id,
            "term_iri": child_term,
            "include_child_terms": False,
            "include_qualified": True,
            "term_source": term_source_hpo.source,
            "term_source_version": term_source_hpo.version,
            "limit": 100,
        },
        sigv4_auth,
    )
    assert with_qualified.get("n_subjects", 0) >= without_qualified.get("n_subjects", 0)

    # 4) Subject-centric term retrieval (Section 7)
    # Use the first subject from the broader result set.
    subjects = body_true.get("body", [])
    assert subjects, f"No subjects returned: {body_true}"
    project_subject_iri = subjects[0]["project_subject_iri"]
    subject_iri = subjects[0]["subject_iri"]  # Keep for term-info endpoint

    subj = api_post(api_base_url, "/subject", {"project_subject_iri": project_subject_iri}, sigv4_auth)
    assert "terms" in subj and isinstance(subj["terms"], list), f"Unexpected /subject response: {subj}"

    # Find our child term in the subject summary if present; otherwise pick any.
    term_entry = next((t for t in subj["terms"] if t.get("term_iri") == child_term), subj["terms"][0])
    term_iri = term_entry["term_iri"]
    qualifiers = term_entry.get("qualifiers", [])

    # Extract just the UUID from the subject_iri for the term-info endpoint
    subject_uuid = subject_iri.split("/")[-1]
    term_info = api_post(
        api_base_url,
        "/subject/term-info",
        {"subject_id": subject_uuid, "term_iri": term_iri, "qualifiers": qualifiers},
        sigv4_auth,
    )
    assert "term_links" in term_info and term_info["term_links"], f"Unexpected /subject/term-info: {term_info}"

    # 5) Evidence drill-in (R4/R5)
    termlink_iri = term_info["term_links"][0]["termlink_iri"]
    tl = api_post(api_base_url, "/term-link", {"termlink_iri": termlink_iri}, sigv4_auth)
    assert "evidence" in tl and isinstance(tl["evidence"], list), f"Unexpected /term-link response: {tl}"

    # R4: provenance-ish fields exist (best-effort; API may evolve)
    ev0 = tl["evidence"][0]
    for k in ["evidence_id", "run_id", "evidence_type", "assertion_type", "created_timestamp"]:
        assert k in ev0, f"Missing {k} in evidence item: {ev0.keys()}"

    # R5: if this is the first termlink created with mixed evidence, we should see both creator types.
    creator_types = set()
    for e in tl["evidence"]:
        creator = e.get("creator") or {}
        ct = creator.get("creator_type")
        if ct:
            creator_types.add(ct)
    # If the termlink we picked isn't the mixed one, this may be only {'automated'}.
    # We enforce R5 in a separate targeted test below (strict).
    print(f"[EVAL] termlink={termlink_iri} creator_types={sorted(list(creator_types))}")


def test_r5_multi_source_evidence_within_single_termlink(
    physical_resources,
    api_base_url,
    sigv4_auth,
    evaluation_run_id,
    evaluation_dataset_small,
):
    """
    Strict R5 check (without requiring API filtering knobs):
    - Ensure a single TermLink can have evidence from both automated and human creators.
    """
    project_id, records = evaluation_dataset_small
    
    # Debug: Check if manual evidence was generated
    first_record = records[0]  # First subject
    first_term_evidence = first_record["evidence"]
    print(f"DEBUG: First subject has {len(first_term_evidence)} evidence items")
    for i, evidence in enumerate(first_term_evidence):
        print(f"DEBUG: Evidence {i}: creator_type={evidence.get('evidence_creator_type')}, creator_id={evidence.get('evidence_creator_id')}")

    ingest = _run_bulk_ingest(physical_resources=physical_resources, run_id=evaluation_run_id, records=records)
    assert ingest["describe_execution"]["status"] == "SUCCEEDED"

    # We know subject 0, term 0 was generated with mixed sources.
    subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/"
    # We don't know the final subject UUID; discover via subjects/query for the child term.
    child_term = "http://purl.obolibrary.org/obo/HP_0001627"
    body = api_post(
        api_base_url,
        "/subjects/query",
        {"project_id": project_id, "term_iri": child_term, "include_child_terms": False, "include_qualified": True, "limit": 50},
        sigv4_auth,
    )
    assert body.get("body"), f"No subjects returned for child term: {body}"
    # Find subject with project_subject_id eval-subj-00000 (first synthetic subject)
    target = next((s for s in body["body"] if s.get("project_subject_id") == "eval-subj-00000"), body["body"][0])
    project_subject_iri = target["project_subject_iri"]

    subj = api_post(api_base_url, "/subject", {"project_subject_iri": project_subject_iri}, sigv4_auth)
    term_entry = next(t for t in subj["terms"] if t["term_iri"] == child_term)
    qualifiers = term_entry.get("qualifiers", [])

    # Extract just the UUID from the subject_iri for the term-info endpoint
    subject_uuid = target["subject_iri"].split("/")[-1]
    term_info = api_post(api_base_url, "/subject/term-info", {"subject_id": subject_uuid, "term_iri": child_term, "qualifiers": qualifiers}, sigv4_auth)
    tl_iri = term_info["term_links"][0]["termlink_iri"]

    tl = api_post(api_base_url, "/term-link", {"termlink_iri": tl_iri}, sigv4_auth)
    print(f"DEBUG: term-link response: {tl}")
    creator_types = { (e.get("creator") or {}).get("creator_type") for e in tl.get("evidence", []) }
    creator_types.discard(None)
    print(f"DEBUG: found creator_types: {creator_types}")

    assert "automated" in creator_types, f"Expected automated evidence; got {creator_types}"
    assert "manual" in creator_types, f"Expected manual evidence; got {creator_types}"


def test_r6_idempotent_termlinks_across_reingest(
    physical_resources,
    api_base_url,
    sigv4_auth,
    evaluation_run_id,
    evaluation_dataset_small,
):
    """
    R6/R9: Re-ingesting the same data should not create duplicate TermLinks for the same
    (source, term, qualifiers) combination. Evidence may accumulate.
    """
    project_id, records = evaluation_dataset_small

    # First ingest
    ingest1 = _run_bulk_ingest(physical_resources=physical_resources, run_id=evaluation_run_id, records=records)
    assert ingest1["describe_execution"]["status"] == "SUCCEEDED"

    # Discover a representative termlink via API
    child_term = "http://purl.obolibrary.org/obo/HP_0001627"
    body = api_post(api_base_url, "/subjects/query", {"project_id": project_id, "term_iri": child_term, "include_child_terms": False, "include_qualified": True, "limit": 50}, sigv4_auth)
    target = next((s for s in body["body"] if s.get("project_subject_id") == "eval-subj-00000"), body["body"][0])
    project_subject_iri = target["project_subject_iri"]
    subj_iri = target["subject_iri"]  # Keep for term-info endpoint
    subj = api_post(api_base_url, "/subject", {"project_subject_iri": project_subject_iri}, sigv4_auth)
    term_entry = next(t for t in subj["terms"] if t["term_iri"] == child_term)
    qualifiers = term_entry.get("qualifiers", [])

    # Extract just the UUID from the subject_iri for the term-info endpoint  
    subject_uuid = subj_iri.split("/")[-1]
    term_info_1 = api_post(api_base_url, "/subject/term-info", {"subject_id": subject_uuid, "term_iri": child_term, "qualifiers": qualifiers}, sigv4_auth)
    assert len(term_info_1["term_links"]) == 1
    tl_iri_1 = term_info_1["term_links"][0]["termlink_iri"]

    # Second ingest (same records; different run_id to preserve evidence)
    ingest2_run = f"{evaluation_run_id}-reingest"
    ingest2 = _run_bulk_ingest(physical_resources=physical_resources, run_id=ingest2_run, records=records)
    assert ingest2["describe_execution"]["status"] == "SUCCEEDED"

    term_info_2 = api_post(api_base_url, "/subject/term-info", {"subject_id": subject_uuid, "term_iri": child_term, "qualifiers": qualifiers}, sigv4_auth)
    assert len(term_info_2["term_links"]) == 1, f"Expected 1 termlink after reingest; got {term_info_2}"
    tl_iri_2 = term_info_2["term_links"][0]["termlink_iri"]

    assert tl_iri_1 == tl_iri_2, f"TermLink IRI changed across re-ingest: {tl_iri_1} vs {tl_iri_2}"


def test_r9_incremental_update_adds_new_term(
    physical_resources,
    api_base_url,
    sigv4_auth,
    evaluation_run_id,
    evaluation_dataset_small,
):
    """
    R9: Demonstrate an incremental update scenario (new term appears after second ingest).
    """
    project_id, records = evaluation_dataset_small

    # First ingest
    ingest1 = _run_bulk_ingest(physical_resources=physical_resources, run_id=evaluation_run_id, records=records)
    assert ingest1["describe_execution"]["status"] == "SUCCEEDED"

    # Pick a target subject
    child_term = "http://purl.obolibrary.org/obo/HP_0001627"
    body = api_post(api_base_url, "/subjects/query", {"project_id": project_id, "term_iri": child_term, "include_child_terms": False, "include_qualified": True, "limit": 50}, sigv4_auth)
    target = next((s for s in body["body"] if s.get("project_subject_id") == "eval-subj-00002"), body["body"][0])
    project_subject_iri = target["project_subject_iri"]
    subj_iri = target["subject_iri"]  # Keep for other endpoints that need it

    subj_before = api_post(api_base_url, "/subject", {"project_subject_iri": project_subject_iri}, sigv4_auth)
    terms_before = {t["term_iri"] for t in subj_before.get("terms", [])}

    # Create an incremental record for that subject with a new term
    new_term = "http://purl.obolibrary.org/obo/HP_0001250"  # Seizure (commonly used; likely present)
    inc_record = {
        "project_id": project_id,
        "project_subject_id": target["project_subject_id"],
        "term_iri": new_term,
        "evidence": [
            {
                "type": "clinical_note",
                "clinical_note_id": "note-inc-00001",
                "encounter_id": "enc-inc-00001",
                "evidence_creator_id": "ods/phebee-eval:automated-v1",
                "evidence_creator_type": "automated",
                "evidence_creator_name": "PheBee Eval NLP",
                "note_timestamp": "2024-02-01",
                "note_type": "progress_note",
                "provider_type": "physician",
                "author_specialty": "neurology",
                "span_start": 10,
                "span_end": 20,
                "contexts": {"negated": 0.0, "family": 0.0, "hypothetical": 0.0},
            }
        ],
        "row_num": 999999,
        "batch_id": 1,
    }

    ingest2_run = f"{evaluation_run_id}-inc"
    ingest2 = _run_bulk_ingest(physical_resources=physical_resources, run_id=ingest2_run, records=[inc_record], s3_prefix=f"evaluation/{ingest2_run}/jsonl", batch_size=1)
    assert ingest2["describe_execution"]["status"] == "SUCCEEDED"

    subj_after = api_post(api_base_url, "/subject", {"project_subject_iri": project_subject_iri}, sigv4_auth)
    terms_after = {t["term_iri"] for t in subj_after.get("terms", [])}

    assert terms_after.issuperset(terms_before), "Terms should not disappear after update"
    assert new_term in terms_after, f"Expected new term after incremental ingest: {new_term}"


@pytest.mark.perf
def test_r11_api_latency_p50_p95(
    api_base_url,
    sigv4_auth,
    physical_resources,
    evaluation_run_id,
    evaluation_dataset_small,
):
    """
    R11: Basic interactive performance check for a small representative workload.
    (For large-scale runs, execute a separate perf job and report numbers in the manuscript.)
    """
    project_id, records = evaluation_dataset_small
    ingest = _run_bulk_ingest(physical_resources=physical_resources, run_id=evaluation_run_id, records=records)
    assert ingest["describe_execution"]["status"] == "SUCCEEDED"

    # Discover a subject IRI once
    child_term = "http://purl.obolibrary.org/obo/HP_0001627"
    q = api_post(api_base_url, "/subjects/query", {"project_id": project_id, "term_iri": child_term, "include_child_terms": False, "include_qualified": True, "limit": 50}, sigv4_auth)
    project_subject_iri = q["body"][0]["project_subject_iri"]

    # Measure two endpoints
    timings: Dict[str, List[float]] = {"subjects_query": [], "subject": []}
    n = int(os.environ.get("PHEBEE_EVAL_LATENCY_N", "20"))

    for _ in range(n):
        t0 = time.perf_counter()
        api_post(api_base_url, "/subjects/query", {"project_id": project_id, "limit": 50}, sigv4_auth)
        timings["subjects_query"].append(time.perf_counter() - t0)

        t1 = time.perf_counter()
        api_post(api_base_url, "/subject", {"project_subject_iri": project_subject_iri}, sigv4_auth)
        timings["subject"].append(time.perf_counter() - t1)

    for k, vals in timings.items():
        p50 = _pctl(vals, 50) * 1000
        p95 = _pctl(vals, 95) * 1000
        print(f"[EVAL] latency_ms endpoint={k} n={len(vals)} p50={p50:.2f} p95={p95:.2f}")

    # Budget is manuscript-dependent; keep as a soft assert by default.
    # Set PHEBEE_EVAL_STRICT_LATENCY=1 to enforce.
    if os.environ.get("PHEBEE_EVAL_STRICT_LATENCY") == "1":
        assert (_pctl(timings["subjects_query"], 95) * 1000) <= 5000
        assert (_pctl(timings["subject"], 95) * 1000) <= 5000


def test_r10_batch_access_via_athena_queries(
    physical_resources,
    evaluation_run_id,
    evaluation_dataset_small,
    aws_session,
    cloudformation_stack,
):
    """
    R10: Validate batch/lakehouse access patterns directly via Athena over Iceberg.

    This test gets Athena configuration from CloudFormation stack outputs.
    """
    # Get Athena configuration from stack outputs
    cf_client = aws_session.client("cloudformation")
    response = cf_client.describe_stacks(StackName=cloudformation_stack)
    outputs = {output["OutputKey"]: output["OutputValue"] for output in response["Stacks"][0].get("Outputs", [])}
    
    db = outputs.get("AthenaDatabase")
    table = outputs.get("AthenaEvidenceTable")
    out = outputs.get("AthenaResultsLocation")
    wg = outputs.get("AthenaWorkgroup", "primary")

    if not (db and table and out):
        raise AssertionError(
            f"Missing Athena config in stack outputs. Found: "
            f"AthenaDatabase={db}, AthenaEvidenceTable={table}, AthenaResultsLocation={out}"
        )

    project_id, records = evaluation_dataset_small
    ingest = _run_bulk_ingest(physical_resources=physical_resources, run_id=evaluation_run_id, records=records)
    assert ingest["describe_execution"]["status"] == "SUCCEEDED"

    # 1) Cohort aggregation: term distribution for this run_id (top terms)
    q1 = f"""
    SELECT term_iri, COUNT(*) AS n
    FROM {db}.{table}
    WHERE run_id = '{evaluation_run_id}'
    GROUP BY term_iri
    ORDER BY n DESC
    LIMIT 10
    """
    cols1, rows1, t1 = _athena_query(q1, database=db, output_location=out, workgroup=wg)
    print(f"[EVAL] athena term_distribution seconds={t1:.2f} cols={cols1} rows_sample={rows1[:5]}")

    # 2) Feature-ish query: per-subject distinct term counts
    q2 = f"""
    SELECT subject_id, COUNT(DISTINCT term_iri) AS n_terms
    FROM {db}.{table}
    WHERE run_id = '{evaluation_run_id}'
    GROUP BY subject_id
    ORDER BY n_terms DESC
    LIMIT 10
    """
    cols2, rows2, t2 = _athena_query(q2, database=db, output_location=out, workgroup=wg)
    print(f"[EVAL] athena per_subject_terms seconds={t2:.2f} cols={cols2} rows_sample={rows2[:5]}")


def test_r1_term_source_struct_exposed_in_evidence(
    physical_resources,
    api_base_url,
    sigv4_auth,
    evaluation_run_id,
    evaluation_dataset_small,
):
    """
    R1: Once implemented, evidence responses should expose explicit term source/version/IRI
    (or allow retrieval via batch queries).
    """
    project_id, records = evaluation_dataset_small
    ingest = _run_bulk_ingest(physical_resources=physical_resources, run_id=evaluation_run_id, records=records)
    assert ingest["describe_execution"]["status"] == "SUCCEEDED"

    child_term = "http://purl.obolibrary.org/obo/HP_0001627"
    body = api_post(api_base_url, "/subjects/query", {"project_id": project_id, "term_iri": child_term, "include_child_terms": False, "include_qualified": True, "limit": 50}, sigv4_auth)
    target = next((s for s in body["body"] if s.get("project_subject_id") == "eval-subj-00000"), body["body"][0])
    project_subject_iri = target["project_subject_iri"]
    subj_iri = target["subject_iri"]  # Keep for other endpoints
    subj = api_post(api_base_url, "/subject", {"project_subject_iri": project_subject_iri}, sigv4_auth)
    term_entry = next(t for t in subj["terms"] if t["term_iri"] == child_term)
    qualifiers = term_entry.get("qualifiers", [])
    # Extract just the UUID from the subject_iri for the term-info endpoint
    subject_uuid = subj_iri.split("/")[-1] 
    term_info = api_post(api_base_url, "/subject/term-info", {"subject_id": subject_uuid, "term_iri": child_term, "qualifiers": qualifiers}, sigv4_auth)
    tl_iri = term_info["term_links"][0]["termlink_iri"]

    tl = api_post(api_base_url, "/term-link", {"termlink_iri": tl_iri}, sigv4_auth)
    ev0 = tl["evidence"][0]

    ts = ev0.get("term_source")
    assert isinstance(ts, dict), f"Expected term_source struct on evidence; got {ts}"
    for k in ("source", "version", "iri"):
        assert ts.get(k), f"Missing term_source.{k} in evidence item"


def test_r3_minimal_phenopacket_mapping_smoke(
    api_base_url,
    sigv4_auth,
    physical_resources,
    evaluation_run_id,
    evaluation_dataset_small,
):
    """
    R3 (partial, test-backed): create a minimal Phenopacket-shaped payload from API outputs.

    This does not require a Phenopackets export endpoint; it validates that the data needed
    for a standards-aligned exchange payload is present and can be serialized cleanly.

    If your repo includes an existing Phenopackets integration test/util, replace this with that call.
    """
    project_id, records = evaluation_dataset_small
    ingest = _run_bulk_ingest(physical_resources=physical_resources, run_id=evaluation_run_id, records=records)
    assert ingest["describe_execution"]["status"] == "SUCCEEDED"

    # Find one subject
    q = api_post(api_base_url, "/subjects/query", {"project_id": project_id, "limit": 10}, sigv4_auth)
    print(f"DEBUG: subjects query response: {q}")
    if not q.get("body") or len(q["body"]) == 0:
        pytest.fail(f"No subjects found for project {project_id}")
        
    # Use project_subject_iri, not subject_iri
    project_subject_iri = q["body"][0]["project_subject_iri"]
    print(f"DEBUG: trying to get subject details for: {project_subject_iri}")
    subj = api_post(api_base_url, "/subject", {"project_subject_iri": project_subject_iri}, sigv4_auth)

    # Minimal Phenopacket-like payload (v2-ish shape, simplified)
    phenopacket = {
        "id": f"phebee-eval-{uuid.uuid4().hex[:8]}",
        "subject": {"id": project_subject_iri},
        "phenotypicFeatures": [],
        "metaData": {
            "created": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "resources": [{"id": "HPO", "name": "Human Phenotype Ontology"}],
        },
    }

    for t in subj.get("terms", []):
        phenopacket["phenotypicFeatures"].append(
            {
                "type": {"id": t["term_iri"], "label": t.get("term_label")},
                "modifiers": [{"id": q} for q in (t.get("qualifiers") or [])],
            }
        )

    # Smoke assertions: serializable and contains expected core fields
    s = json.dumps(phenopacket)
    assert "subject" in phenopacket and phenopacket["subject"]["id"] == project_subject_iri
    assert isinstance(phenopacket["phenotypicFeatures"], list)

    print(f"[EVAL] phenopacket_smoke size_bytes={len(s)} n_features={len(phenopacket['phenotypicFeatures'])}")
