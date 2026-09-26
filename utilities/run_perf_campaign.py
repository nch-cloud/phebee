#!/usr/bin/env python3
"""Drive a full interactive-performance campaign and archive every artifact.

The campaign is a grid: dataset size x concurrency x replicate. For each
dataset size the deployment has to be returned to a known state first, which
is the part that is easy to get wrong by hand:

    reset the database -> reinstall HPO -> import the dataset -> time the API

Only the last step is repeated per concurrency and replicate; the import is
done once per size and the latency runs query the project it created.

Why this exists rather than a shell loop:

  * The reset lambda reports failure in its *payload* (``success: false`` with
    ``statusCode: 500``), not by raising, so a shell loop that only checks the
    CLI exit status will happily benchmark a half-reset database.
  * ``/tmp/phebee-eval-artifacts/<run_id>/`` is named by a run_id generated
    inside the test, so artifacts have to be located after the fact. Left
    alone they accumulate in one flat directory with no record of which grid
    cell produced them.
  * The installed HPO version is *not* an input the harness controls -- see
    the module note below -- so it has to be read back and recorded per run
    if the campaign is to remain interpretable later.

HPO version is a campaign variable, not a constant
--------------------------------------------------
``UpdateHPOSFN`` installs whatever GitHub currently calls the newest HPO
release: ``download_github_release.find_newest_release`` takes no tag or pin
(functions/download_github_release.py). The reset wipes the DynamoDB
``SOURCE~hpo`` record, so the cache check never short-circuits and the
download always happens. Workload 3 (hierarchical term expansion) sends no
``term_source_version``, so ``get_subjects_pheno`` resolves it to the most
recently installed version and expands HP:0001626 against that version's
hierarchy partition. A campaign run months after another therefore expands to
a different descendant set, matches a different number of subjects, and is
not latency-comparable.

This script cannot fix that, but it records the installed version alongside
every run so the comparison can be made honestly. To pin it, replay the state
machine's own steps against the archived assets for the version you want --
they persist in the bucket under the source/version prefix even though the
reset clears the DynamoDB record and the hierarchy table -- and pass
``--skip-hpo``. See "The installed HPO version is not pinned" in
tests/integration/performance/README.md for the sequence.

Resumability
------------
Each grid cell writes ``campaign_run.json`` into its output directory as the
last step. A cell whose ``campaign_run.json`` already exists is skipped, so a
campaign interrupted after nine hours resumes where it stopped. Delete the
directory of a cell you want to redo.

Usage
-----
    python utilities/run_perf_campaign.py \
        --benchmark-root ~/phebee-benchmarks \
        --out perf-campaign-2026-09-25

    # single scale, already imported, latency only
    python utilities/run_perf_campaign.py \
        --benchmark-root ~/phebee-benchmarks \
        --sizes 10000 --skip-reset --skip-hpo \
        --project-id test_project_ab12cd34 \
        --out perf-campaign-2026-09-25

The benchmark root is the directory holding the extracted Zenodo datasets,
one ``<n>-subjects-seed42/`` per size. Verify their MD5s against the table in
tests/integration/performance/README.md before starting; this script does not
re-verify them.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import boto3
from botocore.config import Config

REPO_ROOT = Path(__file__).resolve().parent.parent
ARTIFACT_ROOT = Path("/tmp/phebee-eval-artifacts")

IMPORT_TEST = "tests/integration/performance/test_import_performance.py"
LATENCY_TEST = "tests/integration/performance/test_evaluation_perf_scale.py"

DEFAULT_SIZES = [1000, 5000, 10000, 50000, 100000]
DEFAULT_CONCURRENCY = [1, 10, 25]
DEFAULT_REPLICATES = 3


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(msg: str) -> None:
    print(f"[campaign {utc_now()}] {msg}", flush=True)


def resolve_stack(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    marker = REPO_ROOT / ".phebee-test-stack"
    if marker.exists():
        name = marker.read_text(encoding="utf-8").strip()
        if name:
            return name
    raise SystemExit(
        "No stack name: pass --stack or write one into .phebee-test-stack"
    )


def stack_outputs(session: boto3.Session, stack: str) -> Dict[str, str]:
    cf = session.client("cloudformation")
    stacks = cf.describe_stacks(StackName=stack)["Stacks"]
    return {
        o["OutputKey"]: o["OutputValue"]
        for o in stacks[0].get("Outputs", [])
    }


def git_head() -> Dict[str, Any]:
    """Commit the campaign ran at, and whether the tree was dirty.

    A benchmark whose provenance is a branch name is not reproducible; record
    the sha and say plainly if there were uncommitted changes.

    "Dirty" means tracked files differ from HEAD. It deliberately does not mean
    `git status --porcelain` is non-empty: that counts untracked files, so a
    working copy holding scratch output alongside a clean checkout reported
    dirty=true, which invites a reader to discount a tag the run actually
    matched. Untracked paths are counted separately instead, because they are
    worth knowing about without being a provenance problem.
    """
    def run(*args: str) -> str:
        return subprocess.run(
            args, cwd=REPO_ROOT, capture_output=True, text=True, check=False
        ).stdout.strip()

    untracked = [
        line for line in run(
            "git", "status", "--porcelain", "--untracked-files=normal"
        ).splitlines() if line.startswith("??")
    ]
    tracked_diff = subprocess.run(
        ["git", "diff", "--quiet", "HEAD"],
        cwd=REPO_ROOT, capture_output=True, text=True, check=False
    ).returncode != 0

    return {
        "commit": run("git", "rev-parse", "HEAD"),
        "branch": run("git", "rev-parse", "--abbrev-ref", "HEAD"),
        # Empty when HEAD is not exactly at a tag; this is what to quote.
        "tag": run("git", "describe", "--tags", "--exact-match") or None,
        "dirty": tracked_diff,
        "untracked_file_count": len(untracked),
    }


# ---------------------------------------------------------------------------
# deployment state: reset, HPO install, version readback
# ---------------------------------------------------------------------------

def reset_database(session: boto3.Session, function_arn: str) -> Dict[str, Any]:
    """Invoke ResetDatabaseFunction and insist it actually succeeded.

    The lambda catches its own exceptions and returns statusCode 500 with
    success=false, so a successful *invocation* says nothing about whether the
    database was reset. Read the payload.
    """
    log(f"resetting database via {function_arn}")
    # The function's own timeout is 300s and a Neptune reset plus three Athena
    # DELETEs can use most of it, so the client must outlast it.
    client = session.client(
        "lambda",
        config=Config(read_timeout=900, connect_timeout=60, retries={"max_attempts": 0}),
    )
    started = time.time()
    response = client.invoke(FunctionName=function_arn, InvocationType="RequestResponse")
    raw = response["Payload"].read().decode("utf-8")

    if "FunctionError" in response:
        raise SystemExit(f"ResetDatabaseFunction failed: {raw}")

    payload = json.loads(raw) if raw else {}
    if not payload.get("success"):
        raise SystemExit(f"ResetDatabaseFunction reported failure: {payload}")

    elapsed = time.time() - started
    log(f"reset complete in {elapsed:.0f}s")
    return {"elapsed_s": round(elapsed, 1), "payload": payload}


def run_state_machine(
    session: boto3.Session,
    state_machine_arn: str,
    payload: Dict[str, Any],
    timeout_s: int,
    poll_s: int = 30,
) -> Dict[str, Any]:
    sfn = session.client("stepfunctions")
    name = f"campaign-{int(time.time())}"
    log(f"starting {state_machine_arn.split(':')[-1]} execution {name}")
    execution_arn = sfn.start_execution(
        stateMachineArn=state_machine_arn,
        name=name,
        input=json.dumps(payload),
    )["executionArn"]

    deadline = time.time() + timeout_s
    while True:
        desc = sfn.describe_execution(executionArn=execution_arn)
        status = desc["status"]
        if status != "RUNNING":
            break
        if time.time() > deadline:
            raise SystemExit(
                f"execution {execution_arn} still RUNNING after {timeout_s}s"
            )
        time.sleep(poll_s)

    if status != "SUCCEEDED":
        raise SystemExit(f"execution {execution_arn} ended {status}: {desc.get('cause')}")

    elapsed = (desc["stopDate"] - desc["startDate"]).total_seconds()
    log(f"execution {name} SUCCEEDED in {elapsed:.0f}s")
    return {"execution_arn": execution_arn, "elapsed_s": round(elapsed, 1)}


def installed_ontology_version(
    session: boto3.Session, table_name: str, source_name: str = "hpo"
) -> Optional[str]:
    """Version the deployment will actually resolve to for hierarchy expansion.

    Mirrors dynamodb.get_current_term_source_version: newest InstallTimestamp
    wins, records without one are ignored.
    """
    ddb = session.client("dynamodb")
    items: List[Dict[str, Any]] = []
    kwargs: Dict[str, Any] = {
        "TableName": table_name,
        "KeyConditionExpression": "PK = :pk",
        "ExpressionAttributeValues": {":pk": {"S": f"SOURCE~{source_name}"}},
    }
    while True:
        response = ddb.query(**kwargs)
        items.extend(response.get("Items", []))
        if "LastEvaluatedKey" not in response:
            break
        kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    installed = [i for i in items if "InstallTimestamp" in i]
    if not installed:
        return None
    installed.sort(key=lambda i: i["InstallTimestamp"]["S"], reverse=True)
    return installed[0]["Version"]["S"]


# ---------------------------------------------------------------------------
# running one pytest and capturing what it produced
# ---------------------------------------------------------------------------

def artifact_dirs() -> Set[Path]:
    if not ARTIFACT_ROOT.exists():
        return set()
    return {p for p in ARTIFACT_ROOT.iterdir() if p.is_dir()}


def run_pytest(
    test_path: str,
    env_overrides: Dict[str, str],
    stack: str,
    profile: Optional[str],
    extra_args: List[str],
    log_path: Path,
) -> int:
    env = os.environ.copy()
    env.update(env_overrides)

    cmd = [sys.executable, "-m", "pytest", "-v", "-s", test_path, "--existing-stack", stack]
    if profile:
        cmd += ["--profile", profile]
    cmd += extra_args

    log(f"pytest {test_path} -> {log_path}")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as handle:
        handle.write(f"# {' '.join(cmd)}\n")
        for key in sorted(env_overrides):
            handle.write(f"# {key}={env_overrides[key]}\n")
        handle.flush()
        proc = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            env=env,
            # stdin must not be the terminal. Redirecting only stdout/stderr left
            # fd 0 pointing at the tty, and pytest performs a terminal ioctl on it
            # at startup; a background process doing that takes SIGTTOU, which
            # stops the whole process group. A campaign launched with `&` then
            # suspends ~seconds into its first test and sits there looking slow
            # rather than failing. DEVNULL removes the tty from the child
            # entirely, so it cannot happen however the driver was launched.
            stdin=subprocess.DEVNULL,
            stdout=handle,
            stderr=subprocess.STDOUT,
        )
    return proc.returncode


def collect_artifacts(before: Set[Path], dest: Path, expect: str) -> Optional[Path]:
    """Copy the artifact directory this run created into the campaign tree.

    Runs are serial, so at most one new directory should appear. If more than
    one does, copy them all rather than guessing -- a wrong guess here silently
    files one grid cell's numbers under another's.
    """
    new_dirs = sorted(d for d in artifact_dirs() - before)
    if not new_dirs:
        log(f"WARNING: no new artifact directory appeared (expected {expect})")
        return None
    if len(new_dirs) > 1:
        log(f"WARNING: {len(new_dirs)} new artifact directories: {[d.name for d in new_dirs]}")

    dest.mkdir(parents=True, exist_ok=True)
    primary = None
    for d in new_dirs:
        target = dest / d.name if len(new_dirs) > 1 else dest / "artifacts"
        shutil.copytree(d, target, dirs_exist_ok=True)
        if (target / expect).exists() and primary is None:
            primary = target
    if primary is None:
        log(f"WARNING: none of the copied directories contains {expect}")
    return primary


# ---------------------------------------------------------------------------
# campaign
# ---------------------------------------------------------------------------

def base_env(
    args: argparse.Namespace,
    dataset_dir: Path,
    n_subjects: int,
    hpo_version: Optional[str],
) -> Dict[str, str]:
    env = {
        "PHEBEE_EVAL_SCALE": "1",
        "PHEBEE_EVAL_BENCHMARK_DIR": str(dataset_dir),
        "PHEBEE_EVAL_SCALE_SUBJECTS": str(n_subjects),
        "PHEBEE_EVAL_TERMS_JSON_PATH": args.terms_json,
        "PHEBEE_EVAL_PREVALENCE_CSV_PATH": args.prevalence_csv,
        "PHEBEE_EVAL_SEED": args.seed,
        "PHEBEE_EVAL_USE_DISEASE_CLUSTERING": "1",
        "PHEBEE_EVAL_WRITE_ARTIFACTS": "1",
    }
    # The test cannot work this out for itself: /subjects/query sends no
    # term_source_version, so expansion resolves to whatever was installed most
    # recently. Pass the version read back after the install so it lands in
    # api_run.json rather than only in this script's preamble.json.
    if hpo_version:
        env["PHEBEE_EVAL_HPO_VERSION"] = hpo_version
    return env


def do_import(
    args: argparse.Namespace,
    session: boto3.Session,
    stack: str,
    dataset_dir: Path,
    n_subjects: int,
    size_dir: Path,
    hpo_version: Optional[str],
) -> str:
    """Import one dataset and return the project id the latency runs must use.

    The project is created by the test's own fixture, not by this script:
    PHEBEE_EVAL_PROJECT_ID makes the fixture *use* an id without creating the
    project, which is right for the latency runs and wrong for the import.
    """
    run_dir = size_dir / "import"
    marker = run_dir / "campaign_run.json"
    if marker.exists():
        recorded = json.loads(marker.read_text(encoding="utf-8"))
        log(f"import for {n_subjects} already done, project {recorded['project_id']}")
        return recorded["project_id"]

    env = base_env(args, dataset_dir, n_subjects, hpo_version)
    env["PHEBEE_EVAL_INGEST_TIMEOUT_S"] = str(args.ingest_timeout)

    before = artifact_dirs()
    started = utc_now()
    code = run_pytest(
        IMPORT_TEST, env, stack, args.profile, args.pytest_args, run_dir / "pytest.log"
    )
    if code != 0:
        raise SystemExit(f"import run for {n_subjects} subjects failed; see {run_dir}/pytest.log")

    artifacts = collect_artifacts(before, run_dir, "import_run.json")
    if artifacts is None:
        raise SystemExit(f"import run for {n_subjects} produced no import_run.json")

    import_run = json.loads((artifacts / "import_run.json").read_text(encoding="utf-8"))
    project_id = import_run.get("project_id") or import_run.get("dataset", {}).get("project_id")
    if not project_id:
        raise SystemExit(f"could not read project_id from {artifacts / 'import_run.json'}")

    marker.write_text(
        json.dumps(
            {
                "kind": "import",
                "n_subjects": n_subjects,
                "project_id": project_id,
                "dataset_dir": str(dataset_dir),
                "started_utc": started,
                "finished_utc": utc_now(),
                "env": env,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    log(f"import for {n_subjects} done, project {project_id}")
    return project_id


def do_latency(
    args: argparse.Namespace,
    stack: str,
    dataset_dir: Path,
    n_subjects: int,
    concurrency: int,
    replicate: int,
    project_id: str,
    size_dir: Path,
    provenance: Dict[str, Any],
) -> None:
    run_dir = size_dir / f"c{concurrency}" / f"r{replicate}"
    marker = run_dir / "campaign_run.json"
    if marker.exists():
        log(f"skip n={n_subjects} c={concurrency} r={replicate} (already recorded)")
        return

    env = base_env(args, dataset_dir, n_subjects, provenance.get("hpo_version"))
    env["PHEBEE_EVAL_PROJECT_ID"] = project_id
    env["PHEBEE_EVAL_CONCURRENCY"] = str(concurrency)
    env["PHEBEE_EVAL_LATENCY_N"] = str(args.latency_n)
    env["PHEBEE_EVAL_TERM_INFO_PROBE_N"] = str(args.term_info_probe_n)
    # Distinct query seed per cell, derived from --seed so the whole campaign
    # is reproducible from one number. Replicates must not share a seed: they
    # would then query an identical term sequence and their spread would show
    # only server variance, understating the contribution of term selectivity.
    env["PHEBEE_EVAL_QUERY_SEED"] = str(
        int(args.seed) + replicate * 1000 + concurrency
    )

    before = artifact_dirs()
    started = utc_now()
    code = run_pytest(
        LATENCY_TEST, env, stack, args.profile, args.pytest_args, run_dir / "pytest.log"
    )
    artifacts = collect_artifacts(before, run_dir, "api_run.json")

    record = {
        "kind": "latency",
        "n_subjects": n_subjects,
        "concurrency": concurrency,
        "replicate": replicate,
        "project_id": project_id,
        "dataset_dir": str(dataset_dir),
        "pytest_returncode": code,
        "started_utc": started,
        "finished_utc": utc_now(),
        "env": env,
        **provenance,
    }

    if code != 0:
        # Write the record even on failure: the subject term detail probe is
        # meant to abort a run whose targets do not resolve, and that failure
        # is evidence, not noise. But do not leave a marker that would make a
        # resume skip the cell.
        (run_dir / "campaign_run_FAILED.json").write_text(
            json.dumps(record, indent=2), encoding="utf-8"
        )
        raise SystemExit(
            f"latency run n={n_subjects} c={concurrency} r={replicate} failed "
            f"(exit {code}); see {run_dir}/pytest.log"
        )

    if artifacts is None:
        raise SystemExit(
            f"latency run n={n_subjects} c={concurrency} r={replicate} produced no api_run.json"
        )

    marker.write_text(json.dumps(record, indent=2), encoding="utf-8")
    log(f"done n={n_subjects} c={concurrency} r={replicate}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--benchmark-root", required=True,
                        help="directory holding <n>-subjects-seed42/ dataset directories")
    parser.add_argument("--out", required=True, help="campaign output directory")
    parser.add_argument("--stack", default=None, help="stack name (default: .phebee-test-stack)")
    parser.add_argument("--profile", default=None, help="AWS profile")
    parser.add_argument("--sizes", default=",".join(str(s) for s in DEFAULT_SIZES))
    parser.add_argument("--concurrency", default=",".join(str(c) for c in DEFAULT_CONCURRENCY))
    parser.add_argument("--replicates", type=int, default=DEFAULT_REPLICATES)
    parser.add_argument("--latency-n", type=int, default=100)
    parser.add_argument("--term-info-probe-n", type=int, default=5)
    # Paths are relative to the repo root, since that is where pytest is run
    # from. The harness docstring's "data/hpo_terms.json" is relative to the
    # performance test directory and does not resolve from the root.
    parser.add_argument(
        "--terms-json",
        default="tests/integration/performance/data/hpo_terms_v2026-01-08.json",
        help="HPO term index; loaded even when a pre-generated dataset is used",
    )
    parser.add_argument(
        "--prevalence-csv",
        default="tests/integration/performance/data/term_frequencies.csv",
        help="term prevalence CSV defining the common/rare term pools",
    )
    parser.add_argument("--seed", default="42",
                        help="PHEBEE_EVAL_SEED; recorded in artifacts, see README caveat")
    parser.add_argument("--ingest-timeout", type=int, default=21600)
    parser.add_argument("--hpo-timeout", type=int, default=7200)
    parser.add_argument("--skip-reset", action="store_true",
                        help="do not reset the database before each size")
    parser.add_argument("--skip-hpo", action="store_true",
                        help="do not run UpdateHPOSFN; use when the ontology is pinned by hand")
    parser.add_argument("--allow-hpo-drift", action="store_true",
                        help="continue if a new HPO release lands mid-campaign (default: abort)")
    parser.add_argument("--skip-import", action="store_true",
                        help="data is already loaded; requires --project-id")
    parser.add_argument("--project-id", default=None,
                        help="existing project for --skip-import (single size only)")
    parser.add_argument("--dry-run", action="store_true", help="print the plan and exit")
    parser.add_argument("pytest_args", nargs="*", help="extra args passed through to pytest")
    args = parser.parse_args()

    sizes = [int(s) for s in args.sizes.split(",") if s.strip()]
    concurrencies = [int(c) for c in args.concurrency.split(",") if c.strip()]
    replicates = list(range(1, args.replicates + 1))

    if args.skip_import:
        if not args.project_id:
            raise SystemExit("--skip-import requires --project-id")
        if len(sizes) > 1:
            raise SystemExit("--skip-import applies to one size; pass a single --sizes value")

    benchmark_root = Path(args.benchmark_root).expanduser().resolve()
    out_root = Path(args.out).expanduser().resolve()
    stack = resolve_stack(args.stack)

    # Pre-flight: a campaign is hours long, so fail on a missing input now
    # rather than after the first reset has already wiped the deployment.
    for label, rel in (("--terms-json", args.terms_json),
                       ("--prevalence-csv", args.prevalence_csv)):
        if not (REPO_ROOT / rel).exists():
            raise SystemExit(f"{label}: no such file relative to repo root: {rel}")

    dataset_dirs: Dict[int, Path] = {}
    for n in sizes:
        d = benchmark_root / f"{n}-subjects-seed42"
        if not (d / "metadata.json").exists():
            raise SystemExit(f"no dataset at {d} (expected metadata.json)")
        dataset_dirs[n] = d

    n_latency = len(sizes) * len(concurrencies) * len(replicates)
    n_import = 0 if args.skip_import else len(sizes)
    log(f"stack={stack} sizes={sizes} concurrency={concurrencies} replicates={replicates}")
    log(f"plan: {n_import} import run(s), {n_latency} latency run(s), out={out_root}")
    if args.dry_run:
        for n in sizes:
            print(f"  {n}: reset={not args.skip_reset} hpo={not args.skip_hpo} "
                  f"import={not args.skip_import} dataset={dataset_dirs[n]}")
            for c in concurrencies:
                for r in replicates:
                    print(f"      {out_root / str(n) / f'c{c}' / f'r{r}'}")
        return 0

    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    outputs = stack_outputs(session, stack)
    reset_arn = outputs.get("ResetDatabaseFunctionArn")
    hpo_arn = outputs.get("UpdateHPOSFNArn")
    table_name = outputs.get("DynamoDBTableName")
    if not args.skip_reset and not reset_arn:
        raise SystemExit(f"stack {stack} has no ResetDatabaseFunctionArn output")
    if not args.skip_hpo and not hpo_arn:
        raise SystemExit(f"stack {stack} has no UpdateHPOSFNArn output")

    out_root.mkdir(parents=True, exist_ok=True)
    campaign_meta = {
        "stack": stack,
        "profile": args.profile,
        "started_utc": utc_now(),
        "sizes": sizes,
        "concurrency": concurrencies,
        "replicates": replicates,
        "latency_n": args.latency_n,
        "benchmark_root": str(benchmark_root),
        "git": git_head(),
    }
    (out_root / "campaign.json").write_text(json.dumps(campaign_meta, indent=2), encoding="utf-8")

    campaign_hpo_version: Optional[str] = None
    for n in sizes:
        size_dir = out_root / str(n)
        size_dir.mkdir(parents=True, exist_ok=True)
        dataset_dir = dataset_dirs[n]
        log(f"=== {n} subjects ===")

        preamble: Dict[str, Any] = {}
        if not args.skip_reset:
            preamble["reset"] = reset_database(session, reset_arn)
        if not args.skip_hpo:
            preamble["hpo_install"] = run_state_machine(
                session, hpo_arn, {"test": False}, args.hpo_timeout
            )

        hpo_version = (
            installed_ontology_version(session, table_name) if table_name else None
        )
        log(f"hierarchy expansion will resolve HPO version: {hpo_version}")

        # A campaign spans days and HPO releases roughly monthly. Because each
        # scale reinstalls whatever is newest, a release landing mid-campaign
        # would silently give later scales a different descendant set for
        # HP:0001626 -- an internal inconsistency far worse than the known
        # difference from an older campaign. Stop instead.
        if campaign_hpo_version is None:
            campaign_hpo_version = hpo_version
        elif hpo_version != campaign_hpo_version and not args.allow_hpo_drift:
            raise SystemExit(
                f"HPO version changed mid-campaign: earlier scales used "
                f"{campaign_hpo_version}, this scale resolved {hpo_version}. "
                f"Workload 3 is no longer comparable across scales. Either pin "
                f"the ontology and rerun with --skip-hpo, or pass "
                f"--allow-hpo-drift if you intend to report the split."
            )
        provenance = {"hpo_version": hpo_version, "preamble": preamble}
        (size_dir / "preamble.json").write_text(
            json.dumps({"n_subjects": n, "recorded_utc": utc_now(), **provenance}, indent=2),
            encoding="utf-8",
        )

        if args.skip_import:
            project_id = args.project_id
            log(f"using existing project {project_id}")
        else:
            project_id = do_import(
                args, session, stack, dataset_dir, n, size_dir, hpo_version
            )

        for c in concurrencies:
            for r in replicates:
                do_latency(
                    args, stack, dataset_dir, n, c, r, project_id, size_dir, provenance
                )

    (out_root / "campaign.json").write_text(
        json.dumps({**campaign_meta, "finished_utc": utc_now()}, indent=2), encoding="utf-8"
    )
    log(f"campaign complete: {out_root}")
    log("next: utilities/generate_performance_matrix.py "
        f"{out_root}/*/c*/r*/artifacts/api_run.json -o figs/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
